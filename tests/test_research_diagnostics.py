from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
import rfc8785
from typer.testing import CliRunner

import insider_alerts.research.diagnostics as diagnostics
from insider_alerts import cli
from insider_alerts.backtest.signal_study import DeliveredSignal
from insider_alerts.execution.canary import CanaryStore
from insider_alerts.research.bar_feed import BarFeedStore
from insider_alerts.research.capture import ensure_evidence_store
from insider_alerts.research.diagnostics import (
    DiagnosticConfig,
    DiagnosticRunResult,
    DiagnosticStore,
    diagnostic_status,
    run_diagnostics_once,
)
from insider_alerts.research.session_feed import ExchangeSession, SessionFeedStore
from insider_alerts.research.trial_runtime import TrialWindow
from insider_alerts.review.queue import ensure_review_tables

ROOT = Path(__file__).resolve().parents[1]
NEW_YORK = ZoneInfo("America/New_York")


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _config(tmp_path: Path) -> DiagnosticConfig:
    return DiagnosticConfig(
        diagnostics_db=tmp_path / "diagnostics.db",
        canary_ledger_db=tmp_path / "canary.db",
        source_db=tmp_path / "source.db",
        evidence_db=tmp_path / "evidence.db",
        bar_feed_db=tmp_path / "bars.db",
        session_feed_db=tmp_path / "sessions.db",
        registry_path=ROOT / "docs/research/registry/OPP-E07-V1.json",
    )


def _weekdays(start: date, count: int) -> list[date]:
    result: list[date] = []
    cursor = start
    while len(result) < count:
        if cursor.weekday() < 5:
            result.append(cursor)
        cursor += timedelta(days=1)
    return result


def _install_schedule(path: Path, days: list[date], observed_at: datetime) -> None:
    sessions = [
        ExchangeSession(
            day,
            datetime.combine(day, time(9, 30), NEW_YORK).astimezone(UTC),
            datetime.combine(day, time(16), NEW_YORK).astimezone(UTC),
        )
        for day in days
    ]
    SessionFeedStore(path).append(sessions, observed_at_utc=observed_at)


def _install_source_job(
    path: Path,
    *,
    packet_id: str,
    source_at: datetime,
    decision_at: datetime,
) -> str:
    ensure_review_tables(str(path))
    job_id = f"{packet_id}|insider-evidence-capture-v1"
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            INSERT INTO research_capture_jobs(
              job_id,packet_id,contract_version,accession_number,issuer_cik,form_type,
              payload_json,decision_json,source_first_observed_at_utc,decision_at_utc,
              created_at_utc,updated_at_utc
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                job_id,
                packet_id,
                "insider-evidence-capture-v1",
                "0000000001-26-000001",
                "0000000001",
                "4",
                "{}",
                "{}",
                _utc_text(source_at),
                _utc_text(decision_at),
                _utc_text(decision_at),
                _utc_text(decision_at),
            ),
        )
    return job_id


def _install_routine_evidence(
    path: Path,
    *,
    job_id: str,
    packet_id: str,
    source_at: datetime,
    decision_at: datetime,
    recorded_at: datetime,
    policy_sha256: str = "a" * 64,
) -> str:
    ensure_evidence_store(path)
    unsigned = {
        "schema_version": 2,
        "snapshot_id": "snapshot-1",
        "hypothesis_id": "OPP-E07-V1",
        "recorded_at_utc": _utc_text(recorded_at),
        "enrollment_state": "pending_entry_selection",
        "confirmatory_enrollment_sequence": None,
        "supersedes_snapshot_id": None,
        "payload": {
            "signal": {
                "packet_id": packet_id,
                "accession_number": "0000000001-26-000001",
                "issuer_symbol": "TEST",
                "reporting_owner_ciks": ["0000000002"],
            },
            "timing": {
                "source_first_observed_at_utc": _utc_text(source_at),
                "decision_at_utc": _utc_text(decision_at),
            },
            "versions": {"policy_sha256": policy_sha256},
            "classification": {
                "state": "routine",
                "transaction_owner_mapping": "exact",
                "owner_cik": "0000000002",
                "history_coverage_complete": True,
            },
        },
    }
    record_sha = hashlib.sha256(rfc8785.dumps(unsigned)).hexdigest()
    record = {**unsigned, "record_sha256": record_sha}
    raw = rfc8785.dumps(record)
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            INSERT INTO evidence_snapshots(
              sequence,snapshot_id,job_id,record_sha256,stored_bytes_sha256,record_json,
              recorded_at_utc,owner_history_status
            ) VALUES(1,?,?,?,?,?,?,?)
            """,
            (
                "snapshot-1",
                job_id,
                record_sha,
                hashlib.sha256(raw).hexdigest(),
                raw,
                _utc_text(recorded_at),
                "captured",
            ),
        )
    return record_sha


def _install_canary(
    path: Path,
    *,
    activated_at: datetime,
    signal_at: datetime,
    entry_session: date,
) -> str:
    store = CanaryStore(str(path))
    store.activation(activated_at)
    store.set_metadata({"runtime_source_fingerprint": "f" * 64}, now=signal_at)
    packet_id = "packet-1"
    store.insert_candidate(
        DeliveredSignal(
            packet_id=packet_id,
            accession_number="0000000001-26-000001",
            cik="0000000001",
            symbol="TEST",
            filed_at=signal_at - timedelta(minutes=5),
            signal_at=signal_at,
            score=0.9,
            rationale={},
        ),
        session=entry_session,
        rank="a" * 64,
        is_eligible=True,
        reason="eligible",
        prior_close=10.0,
        median_dollar_volume=1_000_000.0,
        quantity=20,
        now=signal_at + timedelta(seconds=1),
    )
    store.update(packet_id, shadow_state="capacity_suppressed")
    return packet_id


def test_draft_diagnostics_only_heartbeat_and_do_not_open_inputs(tmp_path: Path) -> None:
    config = _config(tmp_path)

    result = run_diagnostics_once(config)

    assert result.status == "idle_registry_draft"
    assert diagnostic_status(config.diagnostics_db)["health"]["last_result"] == (
        "idle_registry_draft"
    )
    assert not config.canary_ledger_db.exists()
    assert not config.source_db.exists()
    assert not config.bar_feed_db.exists()
    assert not config.session_feed_db.exists()


def test_active_diagnostics_bind_control_routine_state_and_bar_requests(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    now = datetime.now(UTC) + timedelta(minutes=1)
    activated_at = now - timedelta(minutes=30)
    source_at = now - timedelta(minutes=20)
    signal_at = now - timedelta(minutes=19)
    days = _weekdays(now.astimezone(NEW_YORK).date() + timedelta(days=1), 12)
    entry_session = days[0]
    _install_schedule(config.session_feed_db, days, signal_at)
    SessionFeedStore(config.session_feed_db).append(
        [
            ExchangeSession(
                days[9],
                datetime.combine(days[9], time(9, 30), NEW_YORK).astimezone(UTC),
                datetime.combine(days[9], time(15, 59), NEW_YORK).astimezone(UTC),
            )
        ],
        observed_at_utc=signal_at + timedelta(minutes=1),
    )
    packet_id = _install_canary(
        config.canary_ledger_db,
        activated_at=activated_at,
        signal_at=signal_at,
        entry_session=entry_session,
    )
    job_id = _install_source_job(
        config.source_db,
        packet_id=packet_id,
        source_at=source_at,
        decision_at=signal_at,
    )
    evidence_sha = _install_routine_evidence(
        config.evidence_db,
        job_id=job_id,
        packet_id=packet_id,
        source_at=source_at,
        decision_at=signal_at,
        recorded_at=signal_at + timedelta(minutes=2),
    )
    monkeypatch.setattr(
        diagnostics,
        "_validated_trial_window",
        lambda _config: TrialWindow(
            "active", "a" * 64, activated_at, activated_at + timedelta(days=30)
        ),
    )

    result = run_diagnostics_once(config, now=now)

    assert result.status == "collecting"
    assert result.candidates_seen == 1
    assert result.candidates_added == 1
    assert result.evidence_bindings_added == 1
    assert result.state_bindings_added == 1
    assert result.bar_requests_ensured == 2
    status = diagnostic_status(config.diagnostics_db)
    assert status["integrity_status"] == "valid"
    assert status["candidates"] == 1
    assert status["evidence_bindings"] == 1
    assert status["state_bindings"] == 1
    assert status["reconciliations"] == 0
    with sqlite3.connect(config.diagnostics_db) as conn:
        evidence = conn.execute(
            "SELECT routine_eligible,routine_reason,evidence_record_sha256 "
            "FROM diagnostic_evidence_bindings"
        ).fetchone()
        candidate = conn.execute(
            "SELECT final_session,record_json FROM diagnostic_candidates"
        ).fetchone()
    assert evidence == (
        1,
        "routine_exact_single_owner_complete_history_pre_cutoff",
        evidence_sha,
    )
    assert candidate[0] == days[9].isoformat()
    candidate_record = json.loads(candidate[1])
    assert candidate_record["schedule_binding"]["observation_watermark"] == 12
    requests = BarFeedStore(config.bar_feed_db).pending_requests(as_of=days[9])
    assert {request.symbol for request in requests} == {"TEST", "SPY"}
    assert {request.requester for request in requests} == {
        "OPP-E07-V1-diagnostic-completed-bar-input-v1"
    }

    again = run_diagnostics_once(config, now=now + timedelta(minutes=1))
    assert again.candidates_added == 0
    assert again.evidence_bindings_added == 0
    assert again.state_bindings_added == 0
    assert again.bar_requests_ensured == 0
    assert diagnostic_status(config.diagnostics_db)["reconciliations"] == 0
    timestamp_only = json.loads(json.dumps(candidate_record))
    timestamp_only["recorded_at_utc"] = _utc_text(now + timedelta(minutes=2))
    assert DiagnosticStore(config.diagnostics_db).add_candidate(timestamp_only) is False
    divergent = json.loads(json.dumps(timestamp_only))
    divergent["canary_selection"]["symbol"] = "OTHER"
    divergent["canary_selection_sha256"] = hashlib.sha256(
        rfc8785.dumps(divergent["canary_selection"])
    ).hexdigest()
    with pytest.raises(ValueError, match="identity already binds different content"):
        DiagnosticStore(config.diagnostics_db).add_candidate(divergent)


def test_changed_canary_selection_is_append_only_reconciliation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    now = datetime.now(UTC) + timedelta(minutes=1)
    activated_at = now - timedelta(minutes=30)
    source_at = now - timedelta(minutes=20)
    signal_at = now - timedelta(minutes=19)
    days = _weekdays(now.astimezone(NEW_YORK).date() + timedelta(days=1), 12)
    _install_schedule(config.session_feed_db, days, signal_at)
    packet_id = _install_canary(
        config.canary_ledger_db,
        activated_at=activated_at,
        signal_at=signal_at,
        entry_session=days[0],
    )
    _install_source_job(
        config.source_db,
        packet_id=packet_id,
        source_at=source_at,
        decision_at=signal_at,
    )
    monkeypatch.setattr(
        diagnostics,
        "_validated_trial_window",
        lambda _config: TrialWindow(
            "active", "a" * 64, activated_at, activated_at + timedelta(days=30)
        ),
    )
    run_diagnostics_once(config, now=now)
    with sqlite3.connect(config.diagnostics_db) as conn:
        original = conn.execute(
            "SELECT record_sha256 FROM diagnostic_candidates WHERE packet_id=?", (packet_id,)
        ).fetchone()[0]
    CanaryStore(str(config.canary_ledger_db)).update(packet_id, prior_close=11.0, symbol="OTHER")

    drifted = run_diagnostics_once(config, now=now + timedelta(minutes=1))

    with sqlite3.connect(config.diagnostics_db) as conn:
        preserved = conn.execute(
            "SELECT record_sha256 FROM diagnostic_candidates WHERE packet_id=?", (packet_id,)
        ).fetchone()[0]
        categories = {
            row[0] for row in conn.execute("SELECT category FROM diagnostic_reconciliations")
        }
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            conn.execute("DELETE FROM diagnostic_candidates")
    assert preserved == original
    assert drifted.status == "degraded"
    assert drifted.unresolved_candidates == 1
    assert "canary_selection_projection_changed" in categories
    assert {
        request.symbol
        for request in BarFeedStore(config.bar_feed_db).pending_requests(as_of=days[9])
    } == {"TEST", "SPY"}
    DiagnosticStore(config.diagnostics_db).validate_integrity()

    CanaryStore(str(config.canary_ledger_db)).update(packet_id, shadow_state="overlap_suppressed")
    state_drifted = run_diagnostics_once(config, now=now + timedelta(minutes=2))
    with sqlite3.connect(config.diagnostics_db) as conn:
        categories = {
            row[0] for row in conn.execute("SELECT category FROM diagnostic_reconciliations")
        }
    assert state_drifted.status == "degraded"
    assert state_drifted.unresolved_candidates == 1
    assert "canary_final_state_changed" in categories

    with sqlite3.connect(config.canary_ledger_db) as conn:
        conn.execute("DELETE FROM candidates WHERE packet_id=?", (packet_id,))
    missing = run_diagnostics_once(config, now=now + timedelta(minutes=3))
    with sqlite3.connect(config.diagnostics_db) as conn:
        categories = {
            row[0] for row in conn.execute("SELECT category FROM diagnostic_reconciliations")
        }
    assert missing.status == "degraded"
    assert missing.unresolved_candidates == 1
    assert "bound_canary_candidate_missing" in categories


def test_evidence_provenance_mismatch_remains_degraded_on_later_cycles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    now = datetime.now(UTC) + timedelta(minutes=1)
    activated_at = now - timedelta(minutes=30)
    source_at = now - timedelta(minutes=20)
    signal_at = now - timedelta(minutes=19)
    days = _weekdays(now.astimezone(NEW_YORK).date() + timedelta(days=1), 12)
    _install_schedule(config.session_feed_db, days, signal_at)
    packet_id = _install_canary(
        config.canary_ledger_db,
        activated_at=activated_at,
        signal_at=signal_at,
        entry_session=days[0],
    )
    job_id = _install_source_job(
        config.source_db,
        packet_id=packet_id,
        source_at=source_at,
        decision_at=signal_at,
    )
    _install_routine_evidence(
        config.evidence_db,
        job_id=job_id,
        packet_id=packet_id,
        source_at=source_at,
        decision_at=signal_at,
        recorded_at=signal_at + timedelta(minutes=2),
        policy_sha256="b" * 64,
    )
    monkeypatch.setattr(
        diagnostics,
        "_validated_trial_window",
        lambda _config: TrialWindow(
            "active", "a" * 64, activated_at, activated_at + timedelta(days=30)
        ),
    )

    first = run_diagnostics_once(config, now=now)
    later = run_diagnostics_once(config, now=now + timedelta(minutes=1))

    assert first.status == "degraded"
    assert later.status == "degraded"
    assert first.unresolved_candidates == later.unresolved_candidates == 1
    assert diagnostic_status(config.diagnostics_db)["evidence_bindings"] == 1


def test_diagnostics_status_cli_is_blinded(tmp_path: Path) -> None:
    path = tmp_path / "diagnostics.db"
    store = DiagnosticStore(path)
    store.write_health(now=datetime.now(UTC), result=DiagnosticRunResult("idle_registry_draft"))

    result = CliRunner().invoke(
        cli.app, ["ops", "research-diagnostics-status", "--diagnostics-db", str(path)]
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["integrity_status"] == "valid"
    assert payload["operational_status"] == "healthy"
    assert "return" not in result.stdout.lower()


def test_diagnostics_status_fails_closed_for_missing_stale_and_corrupt_store(
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    missing = tmp_path / "missing.db"
    missing_result = runner.invoke(
        cli.app, ["ops", "research-diagnostics-status", "--diagnostics-db", str(missing)]
    )
    assert missing_result.exit_code == 3
    assert json.loads(missing_result.stdout)["operational_status"] == "missing"

    stale = tmp_path / "stale.db"
    stale_store = DiagnosticStore(stale)
    heartbeat = datetime.now(UTC) - timedelta(minutes=10)
    stale_store.write_health(now=heartbeat, result=DiagnosticRunResult("idle_registry_draft"))
    assert stale_store.status(now=heartbeat + timedelta(minutes=4))["operational_status"] == (
        "stale"
    )

    corrupt = tmp_path / "corrupt.db"
    corrupt.write_bytes(b"not sqlite")
    corrupt_result = runner.invoke(
        cli.app, ["ops", "research-diagnostics-status", "--diagnostics-db", str(corrupt)]
    )
    assert corrupt_result.exit_code == 3
    corrupt_payload = json.loads(corrupt_result.stdout)
    assert str(corrupt_payload["integrity_status"]).startswith("invalid:DatabaseError")
    assert corrupt_payload["operational_status"] == "invalid_store"
