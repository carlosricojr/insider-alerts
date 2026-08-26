from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import rfc8785
from jsonschema import Draft202012Validator, FormatChecker

import insider_alerts.research.capture as capture_module
from insider_alerts.research.capture import (
    CaptureConfig,
    ProcessResult,
    run_capture_once,
    sha256_bytes,
)
from insider_alerts.review.queue import (
    apply_decision,
    enqueue_review_packet,
    ensure_review_tables,
)
from insider_alerts.sec.models import FilingRef
from insider_alerts.sec.store import upsert_filing_refs

ROOT = Path(__file__).resolve().parents[1]


def _approved_job(tmp_path: Path) -> tuple[Path, str, datetime]:
    source_db = tmp_path / "source.db"
    accession = "0000000001-26-000001"
    cik = "0000000001"
    packet_id = f"{accession}|{cik}|4"
    observed = datetime.now(UTC) - timedelta(seconds=5)
    ref = FilingRef(
        source="sec_rss",
        cik=cik,
        accession_number=accession,
        form_type="4",
        filed_at=observed - timedelta(minutes=2),
        filing_detail_url="https://www.sec.gov/Archives/test-index.html",
        primary_doc_url=None,
        raw_rss_entry={},
    )
    upsert_filing_refs(str(source_db), [ref])
    with sqlite3.connect(source_db) as conn:
        conn.execute(
            "UPDATE filings SET form4_xml_url=? WHERE accession_number=?",
            ("https://www.sec.gov/Archives/test.xml", accession),
        )
    assert enqueue_review_packet(
        str(source_db),
        ref,
        {
            "issuer_symbol": "TEST",
            "issuer_cik": cik,
            "reporting_owner_cik": "0000000002",
            "score": 9.0,
            "rationale": {},
        },
    )
    assert apply_decision(
        str(source_db),
        {
            "packet_id": packet_id,
            "decision": "approve",
            "analyst": "fixture",
            "reason": "prospective fixture",
        },
    ) == 1
    with sqlite3.connect(source_db) as conn:
        decision_at = datetime.fromisoformat(
            str(
                conn.execute(
                    "SELECT decision_at_utc FROM research_capture_jobs WHERE packet_id=?",
                    (packet_id,),
                ).fetchone()[0]
            )
        ).astimezone(UTC)
    return source_db, packet_id, decision_at


def _config(tmp_path: Path, source_db: Path) -> CaptureConfig:
    return CaptureConfig(
        source_db=source_db,
        evidence_db=tmp_path / "evidence.db",
        artifact_root=tmp_path / "artifacts",
        alpha_python=tmp_path / "alpha-python.exe",
        alpha_script=tmp_path / "alpha" / "scripts" / "capture.py",
        canary_ledger=tmp_path / "canary.db",
        insider_git_commit="a" * 40,
        policy_path=ROOT / "docs/research/registry/OPP-E07-V1.json",
        evidence_schema_path=ROOT / "docs/research/contracts/evidence-snapshot.schema.json",
        capture_delay_seconds=1,
    )


def test_approval_atomically_enqueues_only_future_approved_signals(tmp_path: Path) -> None:
    source_db, packet_id, _ = _approved_job(tmp_path)

    with sqlite3.connect(source_db) as conn:
        row = conn.execute("SELECT * FROM research_capture_jobs").fetchone()

    assert row is not None
    assert row[0] == f"{packet_id}|insider-evidence-capture-v1"
    assert row[1] == packet_id
    assert row[10] == "pending"
    with sqlite3.connect(source_db) as conn:
        with pytest.raises(sqlite3.IntegrityError, match="identity is immutable"):
            conn.execute("UPDATE research_capture_jobs SET payload_json='{}'")
        with pytest.raises(sqlite3.IntegrityError, match="cannot be deleted"):
            conn.execute("DELETE FROM research_capture_jobs")

    reject_db = tmp_path / "reject.db"
    ensure_review_tables(str(reject_db))
    with sqlite3.connect(reject_db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM research_capture_jobs").fetchone()[0] == 0


def test_terminal_option_failure_is_persisted_as_valid_immutable_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_db, packet_id, decision_at = _approved_job(tmp_path)
    config = _config(tmp_path, source_db)

    def fail_options(*_args: Any, **_kwargs: Any) -> tuple[Any, ...]:
        return (
            None,
            None,
            None,
            "OPTION_ARTIFACT_INVALID",
            "hostile invalid surface",
            False,
        )

    monkeypatch.setattr(capture_module, "_capture_options", fail_options)
    result = run_capture_once(config, now=decision_at + timedelta(seconds=2), worker_id="test")

    assert result.status == "completed"
    assert result.option_status == "OPTION_ARTIFACT_INVALID"
    with sqlite3.connect(config.evidence_db) as conn:
        row = conn.execute(
            "SELECT record_json,record_sha256,stored_bytes_sha256 FROM evidence_snapshots"
        ).fetchone()
        assert row is not None
        record_bytes = bytes(row[0])
        record = json.loads(record_bytes)
        schema = json.loads(
            (ROOT / "docs/research/contracts/evidence-snapshot.schema.json").read_text(
                encoding="utf-8"
            )
        )
        Draft202012Validator(schema, format_checker=FormatChecker()).validate(record)
        unsigned = dict(record)
        unsigned.pop("record_sha256")
        assert row[1] == sha256_bytes(rfc8785.dumps(unsigned))
        assert row[2] == sha256_bytes(record_bytes)
        assert record["payload"]["observations"]["options_surface"]["status"] == "error"
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            conn.execute("UPDATE evidence_snapshots SET job_id='mutated'")

    with sqlite3.connect(source_db) as conn:
        state = conn.execute(
            "SELECT status,attempt_count,record_sha256 FROM research_capture_jobs"
        ).fetchone()
        assert state == ("complete", 1, result.snapshot_sha256)
        assert conn.execute("SELECT COUNT(*) FROM research_capture_attempts").fetchone()[0] == 1
    assert run_capture_once(config, now=decision_at + timedelta(seconds=3)).status == "idle"
    assert packet_id in result.job_id


def test_successful_option_capture_is_content_addressed_and_referenced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_db, packet_id, decision_at = _approved_job(tmp_path)
    config = _config(tmp_path, source_db)

    def successful_process(command: list[str], **_kwargs: Any) -> ProcessResult:
        output = Path(command[command.index("--output") + 1])
        request_id = command[command.index("--request-id") + 1]
        captured = datetime.now(UTC)
        artifact = {
            "schema_version": "insider-evidence-option-surface-v1",
            "artifact_status": "RESEARCH_ONLY",
            "source_id": "ib_gateway:US_OPTIONS:SMART:type1",
            "request_id": request_id,
            "symbol": "TEST",
            "client_id": 48,
            "market_data_type": 1,
            "requested_at_utc": (captured - timedelta(seconds=1)).isoformat(),
            "source_max_ts_utc": captured.isoformat(),
            "captured_at_utc": captured.isoformat(),
            "min_dte_days": 3,
            "max_dte_days": 30,
            "max_expiries": 3,
            "max_contracts_per_expiry": 120,
            "surfaces": [
                {
                    "expiry": "2026-09-18",
                    "underlying_price": 10.0,
                    "underlying_bid": 9.99,
                    "underlying_ask": 10.01,
                    "underlying_source_timestamp_utc": captured.isoformat(),
                    "quotes": [{}],
                }
            ],
        }
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(artifact), encoding="utf-8")
        return ProcessResult(returncode=0, stdout="", stderr="", timed_out=False)

    monkeypatch.setattr(capture_module, "run_hidden_process", successful_process)
    result = run_capture_once(config, now=decision_at + timedelta(seconds=2))

    assert result.status == "completed"
    assert result.option_status == "captured"
    option_files = list((config.artifact_root / "options").glob("*.json"))
    assert len(option_files) == 1
    assert option_files[0].stem == sha256_bytes(option_files[0].read_bytes())
    with sqlite3.connect(config.evidence_db) as conn:
        row = conn.execute("SELECT record_json FROM evidence_snapshots").fetchone()
        record = json.loads(bytes(row[0]))
    observation = record["payload"]["observations"]["options_surface"]
    assert observation["status"] == "captured"
    assert observation["artifact_sha256"] == option_files[0].stem
    assert packet_id in record["payload"]["signal"]["packet_id"]


def test_retryable_option_failure_keeps_job_durable_without_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_db, _, decision_at = _approved_job(tmp_path)
    config = _config(tmp_path, source_db)
    monkeypatch.setattr(
        capture_module,
        "_capture_options",
        lambda *_args, **_kwargs: (
            None,
            None,
            None,
            "OPTION_CAPTURE_TIMEOUT",
            "timed out",
            True,
        ),
    )

    result = run_capture_once(config, now=decision_at + timedelta(seconds=2))

    assert result.status == "retry_scheduled"
    with sqlite3.connect(source_db) as conn:
        assert conn.execute(
            "SELECT status,attempt_count FROM research_capture_jobs"
        ).fetchone() == ("retry", 1)
    with sqlite3.connect(config.evidence_db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM evidence_snapshots").fetchone()[0] == 0


def test_restart_after_snapshot_append_recovers_without_recapture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_db, _, decision_at = _approved_job(tmp_path)
    config = _config(tmp_path, source_db)
    calls = 0

    def fail_options(*_args: Any, **_kwargs: Any) -> tuple[Any, ...]:
        nonlocal calls
        calls += 1
        return None, None, None, "OPTION_ARTIFACT_INVALID", "invalid", False

    monkeypatch.setattr(capture_module, "_capture_options", fail_options)
    first = run_capture_once(config, now=decision_at + timedelta(seconds=2))
    assert first.status == "completed"
    with sqlite3.connect(source_db) as conn:
        conn.execute("DROP TRIGGER research_capture_complete_immutable")
        conn.execute(
            """
            UPDATE research_capture_jobs
            SET status='leased', lease_owner='crashed', lease_expires_at_utc=?, record_sha256=NULL
            """,
            ((decision_at - timedelta(seconds=1)).isoformat().replace("+00:00", "Z"),),
        )
    ensure_review_tables(str(source_db))

    recovered = run_capture_once(config, now=decision_at + timedelta(seconds=3))

    assert recovered.status == "completed"
    assert recovered.option_status == "recovered_existing"
    assert recovered.snapshot_sha256 == first.snapshot_sha256
    assert calls == 1
    with sqlite3.connect(config.evidence_db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM evidence_snapshots").fetchone()[0] == 1


def test_permanent_internal_failure_is_terminal_and_releases_lease(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_db, _, decision_at = _approved_job(tmp_path)
    config = _config(tmp_path, source_db)
    monkeypatch.setattr(
        capture_module,
        "_capture_options",
        lambda *_args, **_kwargs: (None, None, None, "OPTION_INVALID", "invalid", False),
    )
    monkeypatch.setattr(
        capture_module,
        "_append_snapshot",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("invalid envelope")),
    )

    result = run_capture_once(config, now=decision_at + timedelta(seconds=2))

    assert result.status == "failed"
    with sqlite3.connect(source_db) as conn:
        assert conn.execute(
            "SELECT status,lease_owner,lease_expires_at_utc,last_error_kind "
            "FROM research_capture_jobs"
        ).fetchone() == ("failed", None, None, "CAPTURE_INTERNAL_TERMINAL")
        assert conn.execute(
            "SELECT status,retryable FROM research_capture_attempts"
        ).fetchone() == ("failed", 0)


def test_exhausted_unleased_job_is_failed_instead_of_reclaimed(tmp_path: Path) -> None:
    source_db, _, decision_at = _approved_job(tmp_path)
    config = _config(tmp_path, source_db)
    with sqlite3.connect(source_db) as conn:
        conn.execute(
            "UPDATE research_capture_jobs SET status='retry', attempt_count=?",
            (config.max_attempts,),
        )

    assert run_capture_once(config, now=decision_at + timedelta(seconds=2)).status == "idle"
    with sqlite3.connect(source_db) as conn:
        assert conn.execute(
            "SELECT status,last_error_kind FROM research_capture_jobs"
        ).fetchone() == ("failed", "CAPTURE_ATTEMPTS_EXHAUSTED")


def test_worker_entrypoint_is_structurally_order_incapable() -> None:
    worker = (ROOT / "src/insider_alerts/research/worker.py").read_text(encoding="utf-8")
    capture = (ROOT / "src/insider_alerts/research/capture.py").read_text(encoding="utf-8")
    combined = worker + capture

    assert "execution.ibkr" not in combined
    assert "placeOrder" not in combined
    assert "submit_market" not in combined
    assert "cancel_order" not in combined
