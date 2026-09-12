from __future__ import annotations

import hashlib
import sqlite3
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest
import rfc8785

from insider_alerts.execution import shadow_audit
from insider_alerts.execution.shadow_audit import (
    build_report,
    classification,
    publish_report,
    summary,
)


@pytest.mark.parametrize(
    ("day", "recorded", "reason", "expected"),
    [
        (
            "2026-09-03",
            "2026-09-03T13:30:00+00:00",
            "time",
            "invalid_time_exit_before_session_close",
        ),
        ("2026-09-03", "2026-09-03T20:00:00+00:00", "time", "unverified_completed_day_result"),
        ("2026-11-27", "2026-11-27T18:01:00+00:00", "time", "unverified_completed_day_result"),
        (
            "2026-11-27",
            "2026-11-27T17:59:00+00:00",
            "time",
            "invalid_time_exit_before_session_close",
        ),
        ("2026-09-03", "2026-09-03T13:30:00+00:00", "target", "unverified_intraday_barrier_result"),
        ("2026-09-03", "2026-09-03T13:30:00+00:00", "stop", "unverified_intraday_barrier_result"),
        ("2026-09-07", "2026-09-07T20:00:00+00:00", "time", "unverifiable_exit_timestamp"),
        ("2027-01-04", "2027-01-04T20:00:00+00:00", "time", "unverifiable_exit_timestamp"),
        ("2026-09-03", "2026-09-03T20:00:00", "time", "unverifiable_exit_timestamp"),
        ("bad", "bad", "time", "unverifiable_exit_timestamp"),
    ],
)
def test_classification_never_claims_valid_profit(
    day: str,
    recorded: str,
    reason: str,
    expected: str,
) -> None:
    row = {"exit_session": day, "closed_at": recorded, "exit_reason": reason}
    assert classification(row) == expected
    assert summary([row])["portfolio_performance_validated"] is False


def test_report_preserves_database_and_binds_original_rows(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger.db"
    with sqlite3.connect(ledger) as conn:
        conn.execute(
            "CREATE TABLE shadow_trades (packet_id TEXT,exit_session TEXT,"
            "closed_at TEXT,exit_reason TEXT)"
        )
        conn.execute(
            "INSERT INTO shadow_trades VALUES ('p','2026-09-03','2026-09-03T13:30:00+00:00','time')"
        )
    before = ledger.read_bytes()
    report = build_report(ledger, datetime(2026, 9, 12, tzinfo=UTC))
    assert ledger.read_bytes() == before
    annotation = report["annotations"][0]
    assert (
        annotation["original_row_sha256"]
        == hashlib.sha256(rfc8785.dumps(annotation["original_row"])).hexdigest()
    )
    assert annotation["action"] == "preserve_original_no_replay_no_correction"
    assert annotation["classification"] == "invalid_time_exit_before_session_close"
    assert report["summary"]["closed_records"] == 1
    output = tmp_path / "artifacts"
    artifact = publish_report(report, output)
    assert artifact.read_bytes() == rfc8785.dumps(report)
    assert publish_report(report, output) == artifact
    report["observed_at_utc"] = "2026-09-13T00:00:00+00:00"
    assert publish_report(report, output) != artifact
    assert len(list(output.iterdir())) == 2
    assert ledger.read_bytes() == before


def test_audit_refuses_naive_clock_or_missing_ledger(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        build_report(tmp_path / "missing.db", datetime(2026, 9, 12))
    with pytest.raises(sqlite3.OperationalError):
        build_report(tmp_path / "missing.db", datetime(2026, 9, 12, tzinfo=UTC))
    assert not (tmp_path / "missing.db").exists()


def test_publish_refuses_tampered_existing_artifact(tmp_path: Path) -> None:
    report = {"a": 1}
    artifact = publish_report(report, tmp_path)
    artifact.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="content address"):
        publish_report(report, tmp_path)
    assert artifact.read_bytes() == b"tampered"
    assert len(list(tmp_path.iterdir())) == 1


def test_cli_and_failed_publication_are_order_incapable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    ledger = tmp_path / "ledger.db"
    with sqlite3.connect(ledger) as conn:
        conn.execute("CREATE TABLE shadow_trades (packet_id TEXT)")
    output = tmp_path / "audit"
    monkeypatch.setattr(
        "sys.argv", ["shadow_audit", "--ledger", str(ledger), "--output", str(output)]
    )
    shadow_audit.main()
    assert '"closed_records": 0' in capsys.readouterr().out
    assert len(list(output.glob("*.json"))) == 1

    def fail_link(*args: object) -> None:
        raise OSError("filesystem refused publication")

    monkeypatch.setattr(shadow_audit.os, "link", fail_link)
    with pytest.raises(OSError, match="refused"):
        publish_report({"new": "record"}, output)
    assert len(list(output.iterdir())) == 1


def test_standalone_module_does_not_preimport_itself() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-W",
            "error::RuntimeWarning",
            "-m",
            "insider_alerts.execution.shadow_audit",
            "--help",
        ],
        check=True,
        capture_output=True,
        text=True,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    assert "--ledger" in result.stdout
    assert "found in sys.modules" not in result.stderr
