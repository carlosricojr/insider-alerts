from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import pytest
import rfc8785

import insider_alerts.research.trial_runtime as runtime
from insider_alerts.research.bar_feed import BarFeedStore
from insider_alerts.research.session_feed import ExchangeSession, SessionFeedStore
from insider_alerts.research.trial_runtime import (
    TrialRuntimeConfig,
    TrialRuntimeInvalid,
    TrialStore,
    TrialWindow,
    planned_entry_session,
    run_trial_once,
    trial_runtime_status,
)

ROOT = Path(__file__).resolve().parents[1]
ACTIVATED_AT = datetime(2026, 8, 27, 12, 0, tzinfo=UTC)
REGISTRY_SHA = "a" * 64


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _sessions(*, first: date = date(2026, 8, 27), count: int = 15) -> list[ExchangeSession]:
    output: list[ExchangeSession] = []
    current = first
    while len(output) < count:
        if current.weekday() < 5:
            output.append(
                ExchangeSession(
                    current,
                    datetime.combine(current, time(13, 30), tzinfo=UTC),
                    datetime.combine(current, time(20, 0), tzinfo=UTC),
                )
            )
        current += timedelta(days=1)
    return output


def _config(tmp_path: Path) -> TrialRuntimeConfig:
    return TrialRuntimeConfig(
        trial_db=tmp_path / "trial.db",
        evidence_db=tmp_path / "evidence.db",
        bar_feed_db=tmp_path / "bars.db",
        session_feed_db=tmp_path / "sessions.db",
        registry_path=ROOT / "docs/research/registry/OPP-E07-V1.json",
    )


def _active_window() -> TrialWindow:
    return TrialWindow(
        "active",
        REGISTRY_SHA,
        ACTIVATED_AT,
        runtime.enrollment_deadline(ACTIVATED_AT),
    )


def _install_schedule(
    config: TrialRuntimeConfig,
    *,
    observed_at: datetime = ACTIVATED_AT - timedelta(hours=1),
) -> None:
    SessionFeedStore(config.session_feed_db).append(
        _sessions(),
        observed_at_utc=observed_at,
    )


def _install_evidence(
    config: TrialRuntimeConfig,
    *,
    observed_at: datetime = ACTIVATED_AT + timedelta(minutes=30),
    recorded_at: datetime | None = None,
    policy_sha256: str = REGISTRY_SHA,
    classification_state: str = "opportunistic",
    symbol: str = "TEST",
    accession_number: str = "0000000001-26-000001",
    source_timestamp_text: str | None = None,
    snapshot_id: str | None = None,
) -> dict[str, object]:
    recorded_at = recorded_at or observed_at + timedelta(seconds=30)
    snapshot_id = snapshot_id or str(
        uuid.uuid5(uuid.NAMESPACE_URL, f"snapshot|{_utc_text(observed_at)}")
    )
    record: dict[str, object] = {
        "schema_version": 2,
        "snapshot_id": snapshot_id,
        "hypothesis_id": runtime.HYPOTHESIS_ID,
        "recorded_at_utc": _utc_text(recorded_at),
        "enrollment_state": "pending_entry_selection",
        "confirmatory_enrollment_sequence": None,
        "supersedes_snapshot_id": None,
        "record_sha256": "",
        "payload": {
            "signal": {
                "packet_id": f"packet|{snapshot_id}",
                "accession_number": accession_number,
                "issuer_symbol": symbol,
            },
            "timing": {
                "source_first_observed_at_utc": source_timestamp_text or _utc_text(observed_at)
            },
            "versions": {"policy_sha256": policy_sha256},
            "classification": {
                "state": classification_state,
                "transaction_owner_mapping": "exact",
                "history_coverage_complete": True,
            },
        },
    }
    unsigned = dict(record)
    unsigned.pop("record_sha256")
    record_sha = hashlib.sha256(rfc8785.dumps(unsigned)).hexdigest()
    record["record_sha256"] = record_sha
    raw = rfc8785.dumps(record)
    with sqlite3.connect(config.evidence_db) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS evidence_snapshots(
              sequence INTEGER,snapshot_id TEXT,record_sha256 TEXT,
              stored_bytes_sha256 TEXT,record_json BLOB
            )
            """
        )
        sequence = int(conn.execute("SELECT COUNT(*)+1 FROM evidence_snapshots").fetchone()[0])
        conn.execute(
            "INSERT INTO evidence_snapshots VALUES(?,?,?,?,?)",
            (sequence, snapshot_id, record_sha, hashlib.sha256(raw).hexdigest(), raw),
        )
    return record


def _disposition_reasons(config: TrialRuntimeConfig) -> list[str]:
    with sqlite3.connect(config.trial_db) as conn:
        return [
            str(row[0])
            for row in conn.execute(
                "SELECT reason FROM trial_evidence_dispositions ORDER BY sequence"
            )
        ]


def test_planned_entry_is_pure_at_cutoff_and_does_not_consult_now() -> None:
    sessions = _sessions()
    before = datetime(2026, 8, 27, 13, 19, 59, tzinfo=UTC)
    at_cutoff = datetime(2026, 8, 27, 13, 20, tzinfo=UTC)

    before_entry, before_final = planned_entry_session(before, sessions)
    cutoff_entry, cutoff_final = planned_entry_session(at_cutoff, sessions)

    assert before_entry.session_date == date(2026, 8, 27)
    assert before_final == date(2026, 9, 9)
    assert cutoff_entry.session_date == date(2026, 8, 28)
    assert cutoff_final == date(2026, 9, 10)


def test_planned_entry_fails_closed_without_ten_session_horizon() -> None:
    with pytest.raises(TrialRuntimeInvalid, match="entry_horizon"):
        planned_entry_session(ACTIVATED_AT, _sessions(count=9))


def test_draft_registry_only_heartbeats_and_creates_no_candidates(tmp_path: Path) -> None:
    config = _config(tmp_path)

    result = run_trial_once(config, now=ACTIVATED_AT)

    assert result.status == "idle"
    status = trial_runtime_status(config.trial_db)
    assert status["candidates"] == 0
    assert status["faults"] == 0
    assert status["health"]["last_result"] == "idle_registry_draft"
    assert not config.bar_feed_db.exists()
    assert not config.session_feed_db.exists()


def test_active_runtime_imports_once_and_ensures_stock_and_spy_requests(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    _install_schedule(config)
    evidence = _install_evidence(config)
    monkeypatch.setattr(runtime, "_validated_trial_window", lambda _config: _active_window())
    now = ACTIVATED_AT + timedelta(hours=2)

    first = run_trial_once(config, now=now)
    second = run_trial_once(config, now=now + timedelta(minutes=1))

    assert first.status == second.status == "collecting"
    assert first.candidates_added == 1
    assert second.candidates_added == 0
    assert TrialStore(config.trial_db).status()["candidates"] == 1
    candidate = TrialStore(config.trial_db).candidates()[0]
    assert candidate.evidence_record_sha256 == evidence["record_sha256"]
    assert candidate.planned_entry_date == date(2026, 8, 27)
    assert candidate.final_session_date == date(2026, 9, 9)
    assert (
        candidate.entry_rank_sha256
        == hashlib.sha256(
            (
                f"{runtime.CAPACITY_RANK_SALT}|2026-08-27|{candidate.packet_id}|"
                f"{candidate.accession_number}|TEST"
            ).encode()
        ).hexdigest()
    )
    with sqlite3.connect(config.bar_feed_db) as conn:
        rows = conn.execute(
            "SELECT symbol,start_date,through_date FROM bar_feed_requests ORDER BY symbol"
        ).fetchall()
    assert rows == [
        ("SPY", "2026-04-29", "2026-09-09"),
        ("TEST", "2026-04-29", "2026-09-09"),
    ]
    BarFeedStore(config.bar_feed_db).validate_integrity()


def test_schedule_observed_after_signal_cannot_retroactively_plan_entry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    observed_at = ACTIVATED_AT + timedelta(minutes=30)
    _install_schedule(config, observed_at=observed_at + timedelta(seconds=1))
    _install_evidence(config, observed_at=observed_at)
    monkeypatch.setattr(runtime, "_validated_trial_window", lambda _config: _active_window())

    result = run_trial_once(config, now=observed_at + timedelta(minutes=1))

    assert result.status == "invalid"
    assert "entry_horizon" in _disposition_reasons(config)[0]
    status = TrialStore(config.trial_db).status()
    assert status["candidates"] == 0
    assert status["faults"] == 0


@pytest.mark.parametrize(
    "observed_at",
    [
        ACTIVATED_AT - timedelta(microseconds=1),
        runtime.enrollment_deadline(ACTIVATED_AT),
    ],
)
def test_active_runtime_rejects_both_outside_window_boundaries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    observed_at: datetime,
) -> None:
    config = _config(tmp_path)
    _install_schedule(config, observed_at=ACTIVATED_AT - timedelta(days=1))
    _install_evidence(config, observed_at=observed_at)
    monkeypatch.setattr(runtime, "_validated_trial_window", lambda _config: _active_window())

    result = run_trial_once(config, now=observed_at + timedelta(minutes=1))

    assert result.status == "collecting"
    assert result.error is None
    status = TrialStore(config.trial_db).status()
    assert status["candidates"] == 0
    assert status["evidence_dispositions"] == 1


def test_active_runtime_rejects_evidence_bound_to_another_registry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    _install_schedule(config)
    _install_evidence(config, policy_sha256="b" * 64)
    monkeypatch.setattr(runtime, "_validated_trial_window", lambda _config: _active_window())

    result = run_trial_once(config, now=ACTIVATED_AT + timedelta(hours=1))

    assert result.status == "invalid"
    assert "registry_digest_mismatch" in _disposition_reasons(config)[0]


def test_excluded_pre_activation_evidence_does_not_block_later_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    _install_schedule(config, observed_at=ACTIVATED_AT - timedelta(days=2))
    _install_evidence(
        config,
        observed_at=ACTIVATED_AT - timedelta(microseconds=1),
        policy_sha256="draft" * 12 + "dead",
    )
    valid = _install_evidence(
        config,
        observed_at=ACTIVATED_AT + timedelta(minutes=1),
        accession_number="0000000001-26-000002",
    )
    monkeypatch.setattr(runtime, "_validated_trial_window", lambda _config: _active_window())

    result = run_trial_once(config, now=ACTIVATED_AT + timedelta(hours=1))

    assert result.status == "collecting"
    assert TrialStore(config.trial_db).disposition_counts() == {"excluded": 1}
    candidates = TrialStore(config.trial_db).candidates()
    assert len(candidates) == 1
    assert candidates[0].evidence_record_sha256 == valid["record_sha256"]


def test_invalid_symbol_is_isolated_before_append_and_later_candidate_imports(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    _install_schedule(config)
    _install_evidence(config, symbol="BRK/B")
    _install_evidence(
        config,
        observed_at=ACTIVATED_AT + timedelta(minutes=31),
        symbol="GOOD",
        accession_number="0000000001-26-000002",
    )
    monkeypatch.setattr(runtime, "_validated_trial_window", lambda _config: _active_window())

    result = run_trial_once(config, now=ACTIVATED_AT + timedelta(hours=1))

    assert result.status == "invalid"
    assert TrialStore(config.trial_db).disposition_counts() == {"invalid": 1}
    assert [candidate.symbol for candidate in TrialStore(config.trial_db).candidates()] == ["GOOD"]
    with sqlite3.connect(config.bar_feed_db) as conn:
        assert (
            conn.execute(
                "SELECT GROUP_CONCAT(symbol,',') FROM "
                "(SELECT symbol FROM bar_feed_requests ORDER BY symbol)"
            ).fetchone()[0]
            == "GOOD,SPY"
        )


def test_duplicate_accession_symbol_isolated_without_blocking_later_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    _install_schedule(config)
    _install_evidence(config)
    _install_evidence(config, observed_at=ACTIVATED_AT + timedelta(minutes=31))
    _install_evidence(
        config,
        observed_at=ACTIVATED_AT + timedelta(minutes=32),
        accession_number="0000000001-26-000003",
        symbol="NEXT",
    )
    monkeypatch.setattr(runtime, "_validated_trial_window", lambda _config: _active_window())

    result = run_trial_once(config, now=ACTIVATED_AT + timedelta(hours=1))

    assert result.status == "invalid"
    assert len(TrialStore(config.trial_db).candidates()) == 2
    assert TrialStore(config.trial_db).disposition_counts() == {"invalid": 1}


def test_active_runtime_treats_missing_evidence_store_as_invalid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    _install_schedule(config)
    monkeypatch.setattr(runtime, "_validated_trial_window", lambda _config: _active_window())

    result = run_trial_once(config, now=ACTIVATED_AT + timedelta(hours=1))

    assert result.status == "invalid"
    assert "active_evidence_store_missing" in str(result.error)


def test_special_session_open_before_signal_fails_closed() -> None:
    sessions = _sessions()
    bad = ExchangeSession(
        sessions[0].session_date,
        ACTIVATED_AT - timedelta(minutes=1),
        sessions[0].closes_at_utc,
    )

    with pytest.raises(TrialRuntimeInvalid, match="entry_open_not_after_signal"):
        planned_entry_session(ACTIVATED_AT, [bad, *sessions[1:]])


def test_malformed_timestamp_is_disposed_without_blocking_later_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    _install_schedule(config)
    _install_evidence(config, source_timestamp_text="not-a-timestamp")
    _install_evidence(
        config,
        observed_at=ACTIVATED_AT + timedelta(minutes=31),
        accession_number="0000000001-26-000002",
    )
    monkeypatch.setattr(runtime, "_validated_trial_window", lambda _config: _active_window())

    result = run_trial_once(config, now=ACTIVATED_AT + timedelta(hours=1))

    assert result.status == "invalid"
    assert TrialStore(config.trial_db).disposition_counts() == {"invalid": 1}
    assert len(TrialStore(config.trial_db).candidates()) == 1


def test_future_recorded_time_remains_unresolved_then_imports_without_disposition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    _install_schedule(config)
    observed_at = ACTIVATED_AT + timedelta(minutes=30)
    recorded_at = observed_at + timedelta(minutes=10)
    _install_evidence(config, observed_at=observed_at, recorded_at=recorded_at)
    monkeypatch.setattr(runtime, "_validated_trial_window", lambda _config: _active_window())

    early = run_trial_once(config, now=recorded_at - timedelta(microseconds=1))
    later = run_trial_once(config, now=recorded_at)

    assert early.status == "collecting"
    assert early.unresolved_evidence == 1
    assert later.status == "collecting"
    assert later.candidates_added == 1
    assert TrialStore(config.trial_db).disposition_counts() == {}


def test_candidate_and_disposition_writers_are_mutually_exclusive(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _install_schedule(config)
    record = _install_evidence(config)
    window = _active_window()
    candidate = runtime._candidate_from_evidence(
        record,
        now=ACTIVATED_AT + timedelta(hours=1),
        window=window,
        session_store=SessionFeedStore(config.session_feed_db, initialize=False),
    )
    candidate_first = TrialStore(tmp_path / "candidate-first.db")
    assert candidate_first.append_candidate(candidate)
    assert not candidate_first.append_evidence_disposition(
        snapshot_id=candidate.evidence_snapshot_id,
        evidence_record_sha256=candidate.evidence_record_sha256,
        state="invalid",
        reason="racing-invalid",
        now=ACTIVATED_AT + timedelta(hours=1),
    )
    disposition_first = TrialStore(tmp_path / "disposition-first.db")
    assert disposition_first.append_evidence_disposition(
        snapshot_id=candidate.evidence_snapshot_id,
        evidence_record_sha256=candidate.evidence_record_sha256,
        state="invalid",
        reason="racing-invalid",
        now=ACTIVATED_AT + timedelta(hours=1),
    )
    assert not disposition_first.append_candidate(candidate)
    candidate_first.validate_integrity()
    disposition_first.validate_integrity()


def test_losing_candidate_append_reuses_persisted_import_time_for_bar_requests(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    _install_schedule(config)
    record = _install_evidence(config)
    store = TrialStore(config.trial_db)
    first = runtime._candidate_from_evidence(
        record,
        now=ACTIVATED_AT + timedelta(hours=1),
        window=_active_window(),
        session_store=SessionFeedStore(config.session_feed_db, initialize=False),
    )
    assert store.append_candidate(first)
    later = replace(first, imported_at_utc=first.imported_at_utc + timedelta(seconds=1))
    assert not store.append_candidate(later)
    persisted = store.candidate_for_evidence(first.evidence_record_sha256)
    assert persisted is not None
    runtime._ensure_bar_requests(persisted, BarFeedStore(config.bar_feed_db))
    runtime._ensure_bar_requests(persisted, BarFeedStore(config.bar_feed_db))
    BarFeedStore(config.bar_feed_db).validate_integrity()


def test_transient_read_only_evidence_open_failure_is_degraded_not_invalid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    _install_schedule(config)
    _install_evidence(config)
    monkeypatch.setattr(runtime, "_validated_trial_window", lambda _config: _active_window())
    real_connect = runtime.sqlite3.connect

    def fail_read_only(*args: object, **kwargs: object) -> sqlite3.Connection:
        if args and "mode=ro" in str(args[0]):
            raise sqlite3.OperationalError("temporary open failure")
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(runtime.sqlite3, "connect", fail_read_only)

    result = run_trial_once(config, now=ACTIVATED_AT + timedelta(hours=1))

    assert result.status == "degraded"
    assert TrialStore(config.trial_db).fault_count() == 0


def test_duplicate_evidence_snapshot_identity_fails_before_any_import(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    _install_schedule(config, observed_at=ACTIVATED_AT - timedelta(days=2))
    duplicated = str(uuid.uuid4())
    _install_evidence(
        config,
        observed_at=ACTIVATED_AT - timedelta(microseconds=1),
        snapshot_id=duplicated,
    )
    _install_evidence(
        config,
        observed_at=ACTIVATED_AT + timedelta(minutes=1),
        accession_number="0000000001-26-000002",
        snapshot_id=duplicated,
    )
    monkeypatch.setattr(runtime, "_validated_trial_window", lambda _config: _active_window())

    result = run_trial_once(config, now=ACTIVATED_AT + timedelta(hours=1))

    assert result.status == "invalid"
    assert "evidence_snapshot_id_duplicated" in str(result.error)
    assert TrialStore(config.trial_db).status()["candidates"] == 0


def test_candidate_store_reports_corrupt_bytes_as_invalid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    _install_schedule(config)
    _install_evidence(config)
    monkeypatch.setattr(runtime, "_validated_trial_window", lambda _config: _active_window())
    assert run_trial_once(config, now=ACTIVATED_AT + timedelta(hours=1)).status == "collecting"
    with sqlite3.connect(config.trial_db) as conn:
        conn.execute("DROP TRIGGER trial_candidates_no_update")
        conn.execute("UPDATE trial_candidates SET record_json=?", (json.dumps({}).encode(),))

    assert TrialStore(config.trial_db).status()["integrity_status"] == "invalid"
