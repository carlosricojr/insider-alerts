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
from typer.testing import CliRunner

import insider_alerts.research.trial_finalizer as finalizer
import insider_alerts.research.trial_outcome_finalizer as outcome_finalizer
import insider_alerts.research.trial_runtime as runtime
import insider_alerts.research.trial_worker as trial_worker
from insider_alerts import cli
from insider_alerts.backtest.models import DailyBar
from insider_alerts.research.bar_feed import BarFeedStore
from insider_alerts.research.session_feed import ExchangeSession, SessionFeedStore
from insider_alerts.research.trial_runtime import (
    EntryCompletionInputs,
    EntryEligibility,
    EntryLapseInputs,
    PriorBookPosition,
    TrialCandidate,
    TrialOutcomeInputs,
    TrialResolution,
    TrialRuntimeConfig,
    TrialRuntimeInvalid,
    TrialRuntimeRetryable,
    TrialStore,
    TrialWindow,
    planned_entry_session,
    resolve_ranked_entry_date,
    run_trial_once,
    trial_runtime_status,
)

ROOT = Path(__file__).resolve().parents[1]
ACTIVATED_AT = datetime(2026, 8, 27, 12, 0, tzinfo=UTC)
REGISTRY_SHA = "a" * 64
ENTRY_OPEN = datetime(2026, 8, 27, 13, 30, tzinfo=UTC)


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


def _trial_candidate(
    candidate_id: str,
    *,
    symbol: str,
    rank: str,
    evidence_recorded_at: datetime,
    imported_at: datetime,
) -> TrialCandidate:
    entry_date = date(2026, 8, 27)
    return TrialCandidate(
        candidate_id=candidate_id,
        evidence_snapshot_id=f"snapshot-{candidate_id}",
        evidence_record_sha256=(candidate_id[-1] * 64),
        packet_id=f"packet-{candidate_id}",
        accession_number=f"accession-{candidate_id}",
        symbol=symbol,
        source_first_observed_at_utc=datetime(2026, 8, 27, 13, 0, tzinfo=UTC),
        evidence_recorded_at_utc=evidence_recorded_at,
        classification_state="opportunistic",
        transaction_owner_mapping="exact",
        history_coverage_complete=True,
        planned_entry_date=entry_date,
        entry_opens_at_utc=datetime(2026, 8, 27, 13, 30, tzinfo=UTC),
        final_session_date=date(2026, 9, 9),
        entry_rank_sha256=rank * 64,
        imported_at_utc=imported_at,
    )


def _completion_inputs(
    *,
    completed_at: datetime = datetime(2026, 8, 27, 13, 20, tzinfo=UTC),
    entry_date: date = date(2026, 8, 27),
    entry_open: datetime = ENTRY_OPEN,
    bar_watermark: int = 0,
    bar_digests: tuple[str, ...] = (),
    poll_digests: tuple[str, ...] = (),
    prior_positions: tuple[PriorBookPosition, ...] = (),
) -> EntryCompletionInputs:
    return EntryCompletionInputs(
        entry_date=entry_date,
        completed_at_utc=completed_at,
        entry_opens_at_utc=entry_open,
        final_session_date=entry_date + timedelta(days=13),
        schedule_observation_watermark=10,
        schedule_record_sha256s=("a" * 64,),
        bar_observation_watermark=bar_watermark,
        bar_poll_receipt_watermark=10,
        bar_record_sha256s=bar_digests,
        bar_poll_receipt_sha256s=poll_digests,
        prior_book_positions=prior_positions,
    )


def _lapse_inputs(
    *,
    entry_date: date = date(2026, 8, 27),
    lapsed_at: datetime = ENTRY_OPEN,
    entry_open: datetime = ENTRY_OPEN,
    reason: str,
) -> EntryLapseInputs:
    return EntryLapseInputs(
        entry_date=entry_date,
        lapsed_at_utc=lapsed_at,
        reason=reason,
        entry_opens_at_utc=entry_open,
        schedule_observation_watermark=10,
        schedule_record_sha256s=("a" * 64,),
    )


def _store_at(path: Path, commit_at: datetime) -> TrialStore:
    return TrialStore(path, clock=lambda: commit_at)


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


def test_ranked_entry_resolution_is_pre_open_gap_free_and_capacity_limited() -> None:
    ready = datetime(2026, 8, 27, 13, 10, tzinfo=UTC)
    cutoff = datetime(2026, 8, 27, 13, 20, tzinfo=UTC)
    candidates = [
        _trial_candidate(
            "z", symbol="NEW", rank="1", evidence_recorded_at=ready, imported_at=ready
        ),
        _trial_candidate(
            "a", symbol="NEW", rank="2", evidence_recorded_at=ready, imported_at=ready
        ),
        _trial_candidate(
            "m", symbol="OLD0", rank="3", evidence_recorded_at=ready, imported_at=ready
        ),
        _trial_candidate(
            "y", symbol="SECOND", rank="4", evidence_recorded_at=ready, imported_at=ready
        ),
        _trial_candidate(
            "c", symbol="NEXT", rank="5", evidence_recorded_at=ready, imported_at=ready
        ),
        _trial_candidate(
            "d", symbol="BAD", rank="6", evidence_recorded_at=ready, imported_at=ready
        ),
        _trial_candidate(
            "e",
            symbol="LATE",
            rank="7",
            evidence_recorded_at=cutoff,
            imported_at=cutoff,
        ),
        _trial_candidate(
            "f",
            symbol="IMPORTLATE",
            rank="8",
            evidence_recorded_at=ready,
            imported_at=cutoff,
        ),
    ]
    eligibility = {
        candidate.candidate_id: EntryEligibility(
            candidate.candidate_id not in {"d", "e"},
            (
                "eligible_E07_F00"
                if candidate.candidate_id not in {"d", "e"}
                else "price_out_of_range"
            ),
        )
        for candidate in candidates
    }
    occupied = frozenset(f"OLD{index}" for index in range(18))

    resolved = resolve_ranked_entry_date(
        candidates,
        eligibility=eligibility,
        occupied_symbols=occupied,
        occupied_slots=18,
        next_enrollment_sequence=7,
        completed_at_utc=cutoff,
        entry_opens_at_utc=ENTRY_OPEN,
    )

    assert [item.enrollment_state for item in resolved] == [
        "enrolled",
        "overlap_suppressed",
        "overlap_suppressed",
        "enrolled",
        "capacity_suppressed",
        "ineligible",
        "missed",
        "missed",
    ]
    assert [item.confirmatory_enrollment_sequence for item in resolved] == [
        7,
        None,
        None,
        8,
        None,
        None,
        None,
        None,
    ]
    assert [item.reason for item in resolved] == [
        "eligible_E07_F00",
        "same_symbol_position_active_at_entry_open",
        "same_symbol_position_active_at_entry_open",
        "eligible_E07_F00",
        "challenger_book_at_20_slot_capacity",
        "price_out_of_range",
        "evidence_not_recorded_before_entry_cutoff",
        "candidate_not_imported_before_entry_cutoff",
    ]


@pytest.mark.parametrize(
    "completed_at",
    [
        datetime(2026, 8, 27, 13, 19, 59, 999999, tzinfo=UTC),
        datetime(2026, 8, 27, 13, 30, tzinfo=UTC),
    ],
)
def test_entry_resolution_rejects_completion_outside_pre_open_window(
    completed_at: datetime,
) -> None:
    ready = datetime(2026, 8, 27, 13, 10, tzinfo=UTC)
    candidate = _trial_candidate(
        "a",
        symbol="TEST",
        rank="1",
        evidence_recorded_at=ready,
        imported_at=ready,
    )

    with pytest.raises(TrialRuntimeInvalid, match="pre_open_window"):
        resolve_ranked_entry_date(
            [candidate],
            eligibility={"a": EntryEligibility(True, "eligible_E07_F00")},
            occupied_symbols=frozenset(),
            occupied_slots=0,
            next_enrollment_sequence=1,
            completed_at_utc=completed_at,
            entry_opens_at_utc=ENTRY_OPEN,
        )


@pytest.mark.parametrize(
    "field",
    [
        "source_first_observed_at_utc",
        "evidence_recorded_at_utc",
        "imported_at_utc",
        "entry_opens_at_utc",
    ],
)
def test_entry_resolution_normalizes_naive_candidate_timestamp_failure(field: str) -> None:
    ready = datetime(2026, 8, 27, 13, 10, tzinfo=UTC)
    candidate = _trial_candidate(
        "a",
        symbol="TEST",
        rank="1",
        evidence_recorded_at=ready,
        imported_at=ready,
    )
    candidate = replace(candidate, **{field: ready.replace(tzinfo=None)})

    with pytest.raises(TrialRuntimeInvalid, match="timestamp_naive"):
        resolve_ranked_entry_date(
            [candidate],
            eligibility={"a": EntryEligibility(True, "eligible_E07_F00")},
            occupied_symbols=frozenset(),
            occupied_slots=0,
            next_enrollment_sequence=1,
            completed_at_utc=datetime(2026, 8, 27, 13, 20, tzinfo=UTC),
            entry_opens_at_utc=ENTRY_OPEN,
        )


def test_same_date_source_at_cutoff_is_an_invalid_planner_invariant() -> None:
    ready = datetime(2026, 8, 27, 13, 10, tzinfo=UTC)
    candidate = _trial_candidate(
        "a",
        symbol="TEST",
        rank="1",
        evidence_recorded_at=ready,
        imported_at=ready,
    )
    candidate = replace(
        candidate,
        source_first_observed_at_utc=datetime(2026, 8, 27, 13, 20, tzinfo=UTC),
    )

    with pytest.raises(TrialRuntimeInvalid, match="source_not_before_cutoff"):
        resolve_ranked_entry_date(
            [candidate],
            eligibility={"a": EntryEligibility(True, "eligible_E07_F00")},
            occupied_symbols=frozenset(),
            occupied_slots=0,
            next_enrollment_sequence=1,
            completed_at_utc=datetime(2026, 8, 27, 13, 20, tzinfo=UTC),
            entry_opens_at_utc=ENTRY_OPEN,
        )


def test_entry_completion_atomically_binds_candidates_resolutions_and_inputs(
    tmp_path: Path,
) -> None:
    ready = datetime(2026, 8, 27, 13, 10, tzinfo=UTC)
    completed_at = datetime(2026, 8, 27, 13, 20, tzinfo=UTC)
    candidates = [
        _trial_candidate(
            "a", symbol="AAA", rank="1", evidence_recorded_at=ready, imported_at=ready
        ),
        _trial_candidate(
            "b", symbol="BBB", rank="2", evidence_recorded_at=ready, imported_at=ready
        ),
        _trial_candidate(
            "c", symbol="CCC", rank="3", evidence_recorded_at=ready, imported_at=ready
        ),
    ]
    store = _store_at(tmp_path / "trial.db", completed_at + timedelta(seconds=1))
    for candidate in candidates:
        assert store.append_candidate(candidate)
    eligibility = {
        "a": EntryEligibility(True, "eligible_E07_F00"),
        "b": EntryEligibility(False, "price_out_of_range"),
        "c": EntryEligibility(True, "eligible_E07_F00"),
    }
    resolutions = resolve_ranked_entry_date(
        candidates,
        eligibility=eligibility,
        occupied_symbols=frozenset(),
        occupied_slots=0,
        next_enrollment_sequence=1,
        completed_at_utc=completed_at,
        entry_opens_at_utc=ENTRY_OPEN,
    )
    inputs = _completion_inputs(
        completed_at=completed_at,
        bar_watermark=42,
        bar_digests=("c" * 64, "b" * 64),
        poll_digests=("d" * 64,),
    )

    completion_sha = store.append_entry_completion(inputs, resolutions)

    assert len(completion_sha) == 64
    assert store.append_entry_completion(inputs, resolutions) == completion_sha
    with pytest.raises(TrialRuntimeInvalid, match="conflicting_replay"):
        store.append_entry_completion(replace(inputs, bar_observation_watermark=43), resolutions)
    assert [item.enrollment_state for item in store.resolutions()] == [
        "enrolled",
        "ineligible",
        "enrolled",
    ]
    assert [item.confirmatory_enrollment_sequence for item in store.resolutions()] == [1, None, 2]
    status = store.status()
    assert status["entry_date_completions"] == 1
    assert status["entry_date_lapses"] == 0
    assert status["integrity_status"] == "valid"
    with sqlite3.connect(store.path) as conn:
        row = conn.execute("SELECT record_json FROM trial_entry_date_completions").fetchone()
    record = json.loads(row[0])
    assert record["bar_record_sha256s"] == ["b" * 64, "c" * 64]
    assert record["bar_poll_receipt_sha256s"] == ["d" * 64]
    assert record["decision_clock_at_utc"] == _utc_text(completed_at + timedelta(seconds=1))
    assert [item["candidate_id"] for item in record["candidate_records"]] == ["a", "b", "c"]

    late = _trial_candidate(
        "d",
        symbol="LATE",
        rank="4",
        evidence_recorded_at=ready,
        imported_at=datetime(2026, 8, 27, 14, 0, tzinfo=UTC),
    )
    assert store.append_candidate(late)
    assert store.resolutions()[-1].reason == "candidate_arrived_after_entry_date_completion"
    behind_cursor = replace(
        _trial_candidate(
            "e",
            symbol="OLDER",
            rank="5",
            evidence_recorded_at=datetime(2026, 8, 26, 13, 10, tzinfo=UTC),
            imported_at=datetime(2026, 8, 27, 14, 1, tzinfo=UTC),
        ),
        source_first_observed_at_utc=datetime(2026, 8, 26, 13, 0, tzinfo=UTC),
        planned_entry_date=date(2026, 8, 26),
        entry_opens_at_utc=datetime(2026, 8, 26, 13, 30, tzinfo=UTC),
    )
    assert store.append_candidate(behind_cursor)
    assert store.resolutions()[-1].reason == "candidate_arrived_behind_entry_date_cursor"
    store.validate_integrity()


def test_entry_completion_candidate_mismatch_rolls_back_everything(tmp_path: Path) -> None:
    ready = datetime(2026, 8, 27, 13, 10, tzinfo=UTC)
    candidate = _trial_candidate(
        "a", symbol="AAA", rank="1", evidence_recorded_at=ready, imported_at=ready
    )
    store = _store_at(tmp_path / "trial.db", datetime(2026, 8, 27, 13, 21, tzinfo=UTC))
    assert store.append_candidate(candidate)
    inputs = _completion_inputs()

    with pytest.raises(TrialRuntimeInvalid, match="completion_timestamp_naive"):
        store.append_entry_completion(
            replace(inputs, completed_at_utc=datetime(2026, 8, 27, 13, 20)),
            [],
        )

    with pytest.raises(TrialRuntimeInvalid, match="watermark_invalid"):
        store.append_entry_completion(
            replace(inputs, bar_observation_watermark=True),  # type: ignore[arg-type]
            [],
        )

    with pytest.raises(TrialRuntimeInvalid, match="candidate_set_mismatch"):
        store.append_entry_completion(inputs, [])

    assert store.status()["entry_date_completions"] == 0
    assert store.resolutions() == []


def test_completion_uses_bound_official_open_and_rejects_backdated_commit(
    tmp_path: Path,
) -> None:
    ready = datetime(2026, 8, 27, 13, 10, tzinfo=UTC)
    first = _trial_candidate(
        "a", symbol="AAA", rank="1", evidence_recorded_at=ready, imported_at=ready
    )
    second = replace(
        _trial_candidate(
            "b", symbol="BBB", rank="2", evidence_recorded_at=ready, imported_at=ready
        ),
        entry_opens_at_utc=ENTRY_OPEN + timedelta(minutes=1),
    )
    completed_at = datetime(2026, 8, 27, 13, 20, tzinfo=UTC)
    official_open = ENTRY_OPEN + timedelta(minutes=2)
    resolutions = resolve_ranked_entry_date(
        [first, second],
        eligibility={
            "a": EntryEligibility(True, "eligible_E07_F00"),
            "b": EntryEligibility(True, "eligible_E07_F00"),
        },
        occupied_symbols=frozenset(),
        occupied_slots=0,
        next_enrollment_sequence=1,
        completed_at_utc=completed_at,
        entry_opens_at_utc=official_open,
    )
    valid = _store_at(tmp_path / "valid.db", completed_at + timedelta(seconds=1))
    for candidate in (first, second):
        assert valid.append_candidate(candidate)
    valid.append_entry_completion(
        _completion_inputs(completed_at=completed_at, entry_open=official_open), resolutions
    )
    valid.validate_integrity()

    late = _store_at(tmp_path / "late.db", official_open)
    for candidate in (first, second):
        assert late.append_candidate(candidate)
    with pytest.raises(TrialRuntimeInvalid, match="decision_clock_reached_official_open"):
        late.append_entry_completion(
            _completion_inputs(completed_at=completed_at, entry_open=official_open), resolutions
        )
    assert late.resolutions() == []


def test_entry_completion_rejects_resolution_that_violates_cutoff_state(
    tmp_path: Path,
) -> None:
    cutoff = datetime(2026, 8, 27, 13, 20, tzinfo=UTC)
    candidate = _trial_candidate(
        "a",
        symbol="AAA",
        rank="1",
        evidence_recorded_at=cutoff,
        imported_at=cutoff,
    )
    store = _store_at(tmp_path / "trial.db", cutoff + timedelta(seconds=1))
    assert store.append_candidate(candidate)
    inputs = _completion_inputs(completed_at=cutoff)
    invalid = TrialResolution(
        candidate_id="a",
        entry_date=inputs.entry_date,
        enrollment_state="enrolled",
        reason="eligible_E07_F00",
        confirmatory_enrollment_sequence=1,
        resolved_at_utc=cutoff,
    )

    with pytest.raises(TrialRuntimeInvalid, match="cutoff_state_mismatch"):
        store.append_entry_completion(inputs, [invalid])

    assert store.resolutions() == []
    assert store.status()["entry_date_completions"] == 0


def test_entry_completion_rejects_false_missed_state_for_timely_candidate(
    tmp_path: Path,
) -> None:
    ready = datetime(2026, 8, 27, 13, 10, tzinfo=UTC)
    completed_at = datetime(2026, 8, 27, 13, 20, tzinfo=UTC)
    candidate = _trial_candidate(
        "a",
        symbol="AAA",
        rank="1",
        evidence_recorded_at=ready,
        imported_at=ready,
    )
    store = _store_at(tmp_path / "trial.db", completed_at + timedelta(seconds=1))
    assert store.append_candidate(candidate)
    inputs = _completion_inputs(completed_at=completed_at)
    invalid = TrialResolution(
        candidate_id="a",
        entry_date=inputs.entry_date,
        enrollment_state="missed",
        reason="evidence_not_recorded_before_entry_cutoff",
        confirmatory_enrollment_sequence=None,
        resolved_at_utc=completed_at,
    )

    with pytest.raises(TrialRuntimeInvalid, match="cutoff_state_mismatch"):
        store.append_entry_completion(inputs, [invalid])

    assert store.resolutions() == []
    assert store.status()["entry_date_completions"] == 0


def test_entry_completion_rejects_source_first_observed_at_cutoff(tmp_path: Path) -> None:
    cutoff = datetime(2026, 8, 27, 13, 20, tzinfo=UTC)
    candidate = replace(
        _trial_candidate(
            "a",
            symbol="AAA",
            rank="1",
            evidence_recorded_at=cutoff,
            imported_at=cutoff,
        ),
        source_first_observed_at_utc=cutoff,
    )
    store = _store_at(tmp_path / "trial.db", cutoff + timedelta(seconds=1))
    assert store.append_candidate(candidate)
    inputs = _completion_inputs(completed_at=cutoff)
    missed = TrialResolution(
        candidate_id="a",
        entry_date=inputs.entry_date,
        enrollment_state="missed",
        reason="evidence_not_recorded_before_entry_cutoff",
        confirmatory_enrollment_sequence=None,
        resolved_at_utc=cutoff,
    )

    with pytest.raises(TrialRuntimeInvalid, match="source_not_before_cutoff"):
        store.append_entry_completion(inputs, [missed])

    assert store.resolutions() == []
    assert store.status()["entry_date_completions"] == 0


def test_entry_completion_rejects_resolution_state_outside_closed_vocabulary(
    tmp_path: Path,
) -> None:
    ready = datetime(2026, 8, 27, 13, 10, tzinfo=UTC)
    candidate = _trial_candidate(
        "a", symbol="AAA", rank="1", evidence_recorded_at=ready, imported_at=ready
    )
    store = _store_at(tmp_path / "trial.db", datetime(2026, 8, 27, 13, 21, tzinfo=UTC))
    assert store.append_candidate(candidate)
    invalid = TrialResolution(
        candidate_id="a",
        entry_date=date(2026, 8, 27),
        enrollment_state="enroled",  # type: ignore[arg-type]
        reason="typo",
        confirmatory_enrollment_sequence=None,
        resolved_at_utc=datetime(2026, 8, 27, 13, 20, tzinfo=UTC),
    )
    inputs = _completion_inputs()

    with pytest.raises(TrialRuntimeInvalid, match="state_invalid"):
        store.append_entry_completion(inputs, [invalid])

    assert store.resolutions() == []


def test_entry_lapse_is_atomic_idempotent_and_advances_late_candidate_cursor(
    tmp_path: Path,
) -> None:
    ready = datetime(2026, 8, 27, 13, 10, tzinfo=UTC)
    store = TrialStore(tmp_path / "trial.db")
    first = _trial_candidate(
        "a", symbol="AAA", rank="1", evidence_recorded_at=ready, imported_at=ready
    )
    assert store.append_candidate(first)
    inputs = _lapse_inputs(reason="bar_feed_not_ready_before_open")

    digest = store.append_entry_lapse(inputs)

    assert store.append_entry_lapse(inputs) == digest
    assert store.resolutions()[0].enrollment_state == "missed"
    assert store.resolutions()[0].reason == (
        "entry_date_completion_lapsed:bar_feed_not_ready_before_open"
    )
    late = _trial_candidate(
        "b",
        symbol="BBB",
        rank="2",
        evidence_recorded_at=ready,
        imported_at=datetime(2026, 8, 27, 14, 0, tzinfo=UTC),
    )
    assert store.append_candidate(late)
    assert store.resolutions()[-1].reason == "candidate_arrived_after_entry_date_lapse"
    assert store.status()["entry_date_lapses"] == 1
    assert store.status()["integrity_status"] == "valid"


def test_lapse_uses_bound_official_open_across_candidate_schedule_revisions(
    tmp_path: Path,
) -> None:
    ready = datetime(2026, 8, 27, 13, 10, tzinfo=UTC)
    store = TrialStore(tmp_path / "trial.db")
    candidates = (
        _trial_candidate(
            "a", symbol="AAA", rank="1", evidence_recorded_at=ready, imported_at=ready
        ),
        replace(
            _trial_candidate(
                "b", symbol="BBB", rank="2", evidence_recorded_at=ready, imported_at=ready
            ),
            entry_opens_at_utc=ENTRY_OPEN + timedelta(minutes=1),
        ),
    )
    for candidate in candidates:
        assert store.append_candidate(candidate)

    store.append_entry_lapse(
        _lapse_inputs(
            entry_open=ENTRY_OPEN + timedelta(minutes=2),
            lapsed_at=ENTRY_OPEN + timedelta(minutes=2),
            reason="official_open_passed",
        )
    )

    assert [resolution.enrollment_state for resolution in store.resolutions()] == [
        "missed",
        "missed",
    ]
    store.validate_integrity()


def test_late_candidate_uses_earliest_covering_seal_across_multiple_dates(
    tmp_path: Path,
) -> None:
    store = TrialStore(tmp_path / "trial.db")
    first_ready = datetime(2026, 8, 27, 13, 10, tzinfo=UTC)
    first = _trial_candidate(
        "a",
        symbol="AAA",
        rank="1",
        evidence_recorded_at=first_ready,
        imported_at=first_ready,
    )
    assert store.append_candidate(first)
    store.append_entry_lapse(_lapse_inputs(reason="day_one_outage"))

    second_ready = datetime(2026, 8, 28, 13, 10, tzinfo=UTC)
    second = replace(
        _trial_candidate(
            "b",
            symbol="BBB",
            rank="2",
            evidence_recorded_at=second_ready,
            imported_at=second_ready,
        ),
        source_first_observed_at_utc=datetime(2026, 8, 28, 13, 0, tzinfo=UTC),
        planned_entry_date=date(2026, 8, 28),
        entry_opens_at_utc=datetime(2026, 8, 28, 13, 30, tzinfo=UTC),
        final_session_date=date(2026, 9, 10),
    )
    assert store.append_candidate(second)
    store.append_entry_lapse(
        _lapse_inputs(
            entry_date=date(2026, 8, 28),
            lapsed_at=datetime(2026, 8, 28, 13, 30, tzinfo=UTC),
            entry_open=datetime(2026, 8, 28, 13, 30, tzinfo=UTC),
            reason="day_two_outage",
        )
    )

    late = _trial_candidate(
        "c",
        symbol="LATE",
        rank="3",
        evidence_recorded_at=first_ready,
        imported_at=datetime(2026, 8, 28, 14, 0, tzinfo=UTC),
    )
    assert store.append_candidate(late)
    assert store.resolutions()[-1].reason == "candidate_arrived_after_entry_date_lapse"
    store.validate_integrity()


def test_candidate_import_timestamp_before_concurrent_seal_is_retryable(
    tmp_path: Path,
) -> None:
    ready = datetime(2026, 8, 27, 13, 10, tzinfo=UTC)
    store = TrialStore(tmp_path / "trial.db")
    first = _trial_candidate(
        "a",
        symbol="AAA",
        rank="1",
        evidence_recorded_at=ready,
        imported_at=ready,
    )
    assert store.append_candidate(first)
    sealed_at = datetime(2026, 8, 27, 13, 30, tzinfo=UTC)
    store.append_entry_lapse(_lapse_inputs(lapsed_at=sealed_at, reason="concurrent_cycle"))
    stale_cycle_candidate = _trial_candidate(
        "b",
        symbol="BBB",
        rank="2",
        evidence_recorded_at=ready,
        imported_at=sealed_at - timedelta(seconds=1),
    )

    with pytest.raises(TrialRuntimeRetryable, match="moved_behind_entry_date_cursor"):
        store.append_candidate(stale_cycle_candidate)

    assert store.candidate_for_evidence(stale_cycle_candidate.evidence_record_sha256) is None
    assert store.disposition_for_evidence(stale_cycle_candidate.evidence_record_sha256) is None
    retried = replace(stale_cycle_candidate, imported_at_utc=sealed_at + timedelta(seconds=1))
    assert store.append_candidate(retried)
    assert store.resolutions()[-1].reason == "candidate_arrived_after_entry_date_lapse"
    store.validate_integrity()


def test_candidate_import_large_clock_regression_against_cursor_is_permanent(
    tmp_path: Path,
) -> None:
    ready = datetime(2026, 8, 27, 13, 10, tzinfo=UTC)
    store = TrialStore(tmp_path / "trial.db")
    first = _trial_candidate(
        "a", symbol="AAA", rank="1", evidence_recorded_at=ready, imported_at=ready
    )
    assert store.append_candidate(first)
    sealed_at = datetime(2026, 8, 27, 13, 30, tzinfo=UTC)
    store.append_entry_lapse(_lapse_inputs(lapsed_at=sealed_at, reason="future_clock"))
    stale = _trial_candidate(
        "b",
        symbol="BBB",
        rank="2",
        evidence_recorded_at=ready,
        imported_at=sealed_at - timedelta(minutes=5, seconds=1),
    )

    with pytest.raises(
        TrialRuntimeInvalid, match="candidate_import_clock_regression_exceeds_limit"
    ):
        store.append_candidate(stale)

    assert store.candidate_for_evidence(stale.evidence_record_sha256) is None
    assert store.disposition_for_evidence(stale.evidence_record_sha256) is None


def test_integrity_ties_lapse_resolution_reason_to_lapse_record(tmp_path: Path) -> None:
    ready = datetime(2026, 8, 27, 13, 10, tzinfo=UTC)
    store = TrialStore(tmp_path / "trial.db")
    candidate = _trial_candidate(
        "a",
        symbol="AAA",
        rank="1",
        evidence_recorded_at=ready,
        imported_at=ready,
    )
    assert store.append_candidate(candidate)
    store.append_entry_lapse(_lapse_inputs(reason="bar_feed_not_ready_before_open"))

    with sqlite3.connect(store.path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("DROP TRIGGER trial_resolutions_no_update")
        conn.execute("DROP TRIGGER trial_entry_date_lapses_no_update")
        resolution_row = conn.execute("SELECT * FROM trial_resolutions").fetchone()
        resolution_record = json.loads(bytes(resolution_row["record_json"]))
        resolution_record["reason"] = "arbitrary_missed_reason"
        resolution_bytes = runtime._canonical(resolution_record)
        resolution_sha = runtime._sha256(resolution_bytes)
        conn.execute(
            """
            UPDATE trial_resolutions
            SET reason=?,record_sha256=?,record_json=?
            """,
            ("arbitrary_missed_reason", resolution_sha, resolution_bytes),
        )
        lapse_row = conn.execute("SELECT * FROM trial_entry_date_lapses").fetchone()
        lapse_record = json.loads(bytes(lapse_row["record_json"]))
        lapse_record["resolution_record_sha256s"] = [resolution_sha]
        lapse_record["resolution_set_sha256"] = runtime._sha256(
            runtime._canonical({"resolution_record_sha256s": [resolution_sha]})
        )
        lapse_bytes = runtime._canonical(lapse_record)
        conn.execute(
            "UPDATE trial_entry_date_lapses SET record_sha256=?,record_json=?",
            (runtime._sha256(lapse_bytes), lapse_bytes),
        )

    with pytest.raises(TrialRuntimeInvalid, match="lapse_resolution_binding_mismatch"):
        store.validate_integrity()


def test_entry_lapse_before_open_rolls_back(
    tmp_path: Path,
) -> None:
    ready = datetime(2026, 8, 27, 13, 10, tzinfo=UTC)
    store = TrialStore(tmp_path / "trial.db")
    candidate = _trial_candidate(
        "a", symbol="AAA", rank="1", evidence_recorded_at=ready, imported_at=ready
    )
    assert store.append_candidate(candidate)
    early = _lapse_inputs(
        lapsed_at=datetime(2026, 8, 27, 13, 29, 59, tzinfo=UTC), reason="too_early"
    )

    with pytest.raises(TrialRuntimeInvalid, match="before_official_open"):
        store.append_entry_lapse(early)

    assert store.resolutions() == []
    assert store.status()["entry_date_lapses"] == 0


def test_small_clock_regressions_before_candidate_import_or_prior_seal_are_retryable(
    tmp_path: Path,
) -> None:
    store = _store_at(tmp_path / "trial.db", datetime(2026, 8, 28, 13, 21, tzinfo=UTC))
    imported_late = _trial_candidate(
        "a",
        symbol="AAA",
        rank="1",
        evidence_recorded_at=datetime(2026, 8, 27, 13, 10, tzinfo=UTC),
        imported_at=datetime(2026, 8, 27, 13, 25, tzinfo=UTC),
    )
    assert store.append_candidate(imported_late)
    completion_inputs = _completion_inputs()
    resolution = TrialResolution(
        candidate_id="a",
        entry_date=date(2026, 8, 27),
        enrollment_state="missed",
        reason="candidate_not_imported_before_entry_cutoff",
        confirmatory_enrollment_sequence=None,
        resolved_at_utc=completion_inputs.completed_at_utc,
    )

    with pytest.raises(TrialRuntimeRetryable, match="moved_behind_candidate_import"):
        store.append_entry_completion(completion_inputs, [resolution])

    late_lapse = _lapse_inputs(
        lapsed_at=datetime(2026, 8, 28, 13, 21, tzinfo=UTC),
        reason="extended_outage",
    )
    assert store.append_entry_lapse(late_lapse)
    next_candidate = replace(
        _trial_candidate(
            "b",
            symbol="BBB",
            rank="2",
            evidence_recorded_at=datetime(2026, 8, 28, 13, 10, tzinfo=UTC),
            imported_at=datetime(2026, 8, 28, 13, 22, tzinfo=UTC),
        ),
        source_first_observed_at_utc=datetime(2026, 8, 28, 13, 0, tzinfo=UTC),
        planned_entry_date=date(2026, 8, 28),
        entry_opens_at_utc=datetime(2026, 8, 28, 13, 30, tzinfo=UTC),
    )
    assert store.append_candidate(next_candidate)
    next_inputs = _completion_inputs(
        entry_date=date(2026, 8, 28),
        completed_at=datetime(2026, 8, 28, 13, 20, tzinfo=UTC),
        entry_open=datetime(2026, 8, 28, 13, 30, tzinfo=UTC),
    )
    next_resolution = TrialResolution(
        candidate_id="b",
        entry_date=date(2026, 8, 28),
        enrollment_state="enrolled",
        reason="eligible_E07_F00",
        confirmatory_enrollment_sequence=1,
        resolved_at_utc=next_inputs.completed_at_utc,
    )

    with pytest.raises(TrialRuntimeRetryable, match="seal_time_moved_backwards"):
        store.append_entry_completion(next_inputs, [next_resolution])


def test_large_clock_regression_against_candidate_import_is_permanent(tmp_path: Path) -> None:
    store = _store_at(tmp_path / "trial.db", datetime(2026, 8, 27, 13, 21, tzinfo=UTC))
    candidate = _trial_candidate(
        "a",
        symbol="AAA",
        rank="1",
        evidence_recorded_at=datetime(2026, 8, 27, 13, 10, tzinfo=UTC),
        imported_at=datetime(2026, 8, 27, 13, 26, tzinfo=UTC),
    )
    assert store.append_candidate(candidate)
    inputs = _completion_inputs()
    resolution = TrialResolution(
        candidate_id="a",
        entry_date=date(2026, 8, 27),
        enrollment_state="missed",
        reason="candidate_not_imported_before_entry_cutoff",
        confirmatory_enrollment_sequence=None,
        resolved_at_utc=inputs.completed_at_utc,
    )

    with pytest.raises(TrialRuntimeInvalid, match="import_clock_regression_exceeds_limit"):
        store.append_entry_completion(inputs, [resolution])

    assert store.resolutions() == []
    assert store.status()["entry_date_completions"] == 0


def test_large_clock_regression_against_prior_seal_is_permanent(tmp_path: Path) -> None:
    store = TrialStore(tmp_path / "trial.db")
    first = _trial_candidate(
        "a",
        symbol="AAA",
        rank="1",
        evidence_recorded_at=datetime(2026, 8, 27, 13, 10, tzinfo=UTC),
        imported_at=datetime(2026, 8, 27, 13, 10, tzinfo=UTC),
    )
    assert store.append_candidate(first)
    store.append_entry_lapse(
        _lapse_inputs(
            lapsed_at=datetime(2026, 8, 28, 13, 26, tzinfo=UTC),
            reason="extended_outage",
        )
    )
    second = replace(
        _trial_candidate(
            "b",
            symbol="BBB",
            rank="2",
            evidence_recorded_at=datetime(2026, 8, 28, 13, 10, tzinfo=UTC),
            imported_at=datetime(2026, 8, 28, 13, 27, tzinfo=UTC),
        ),
        source_first_observed_at_utc=datetime(2026, 8, 28, 13, 0, tzinfo=UTC),
        planned_entry_date=date(2026, 8, 28),
        entry_opens_at_utc=datetime(2026, 8, 28, 13, 30, tzinfo=UTC),
    )
    assert store.append_candidate(second)
    inputs = _completion_inputs(
        entry_date=date(2026, 8, 28),
        completed_at=datetime(2026, 8, 28, 13, 20, tzinfo=UTC),
        entry_open=datetime(2026, 8, 28, 13, 30, tzinfo=UTC),
    )
    resolution = TrialResolution(
        candidate_id="b",
        entry_date=date(2026, 8, 28),
        enrollment_state="enrolled",
        reason="eligible_E07_F00",
        confirmatory_enrollment_sequence=1,
        resolved_at_utc=inputs.completed_at_utc,
    )

    with pytest.raises(TrialRuntimeInvalid, match="seal_clock_regression_exceeds_limit"):
        store.append_entry_completion(inputs, [resolution])

    assert len(store.resolutions()) == 1
    assert store.status()["entry_date_completions"] == 0


def test_small_clock_regression_before_lapse_candidate_import_is_retryable(
    tmp_path: Path,
) -> None:
    imported_at = ENTRY_OPEN + timedelta(seconds=30)
    store = TrialStore(tmp_path / "trial.db")
    candidate = _trial_candidate(
        "a",
        symbol="AAA",
        rank="1",
        evidence_recorded_at=datetime(2026, 8, 27, 13, 10, tzinfo=UTC),
        imported_at=imported_at,
    )
    assert store.append_candidate(candidate)

    with pytest.raises(TrialRuntimeRetryable, match="moved_behind_candidate_import"):
        store.append_entry_lapse(_lapse_inputs(reason="clock_regression"))

    assert store.resolutions() == []
    assert store.status()["entry_date_lapses"] == 0


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


def test_trial_worker_runs_import_then_finalizer_and_draft_is_inert(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    error_log = tmp_path / "worker.err.log"
    exit_code = trial_worker.main(
        [
            "--trial-db",
            str(tmp_path / "trial.db"),
            "--evidence-db",
            str(tmp_path / "evidence.db"),
            "--bar-feed-db",
            str(tmp_path / "bars.db"),
            "--session-feed-db",
            str(tmp_path / "sessions.db"),
            "--registry-path",
            str(ROOT / "docs/research/registry/OPP-E07-V1.json"),
            "--error-log",
            str(error_log),
        ]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["diagnostics"]["status"] == "idle_registry_draft"
    assert payload["diagnostic_outcomes"]["status"] == "idle_registry_draft"
    assert payload["candidate_runtime"]["status"] == "idle"
    assert payload["entry_finalizer"]["status"] == "idle_registry_draft"
    assert payload["outcome_finalizer"]["status"] == "idle_registry_draft"
    assert not error_log.exists()
    assert not (tmp_path / "bars.db").exists()
    assert not (tmp_path / "sessions.db").exists()


def test_trial_worker_isolates_diagnostic_failure_from_confirmatory_phase(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        trial_worker,
        "run_diagnostics_once",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("diagnostic corrupt")),
    )
    diagnostics_db = tmp_path / "diagnostics.db"
    trial_db = tmp_path / "trial.db"
    error_log = tmp_path / "worker.err.log"

    exit_code = trial_worker.main(
        [
            "--trial-db",
            str(trial_db),
            "--diagnostics-db",
            str(diagnostics_db),
            "--evidence-db",
            str(tmp_path / "evidence.db"),
            "--bar-feed-db",
            str(tmp_path / "bars.db"),
            "--session-feed-db",
            str(tmp_path / "sessions.db"),
            "--registry-path",
            str(ROOT / "docs/research/registry/OPP-E07-V1.json"),
            "--error-log",
            str(error_log),
        ]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["diagnostics"]["status"] == "degraded"
    assert payload["candidate_runtime"]["status"] == "idle"
    assert TrialStore(trial_db).status()["health"]["last_result"] == "idle_registry_draft"
    assert trial_worker.DiagnosticStore(diagnostics_db).status()["health"]["last_result"] == (
        "degraded"
    )
    assert "diagnostic phase isolated" in error_log.read_text(encoding="utf-8")


def test_trial_worker_isolates_diagnostic_outcome_failure_from_confirmatory_phase(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        trial_worker,
        "finalize_diagnostic_outcomes",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("outcome corrupt")),
    )
    diagnostics_db = tmp_path / "diagnostics.db"
    trial_db = tmp_path / "trial.db"
    error_log = tmp_path / "worker.err.log"

    exit_code = trial_worker.main(
        [
            "--trial-db",
            str(trial_db),
            "--diagnostics-db",
            str(diagnostics_db),
            "--evidence-db",
            str(tmp_path / "evidence.db"),
            "--bar-feed-db",
            str(tmp_path / "bars.db"),
            "--session-feed-db",
            str(tmp_path / "sessions.db"),
            "--registry-path",
            str(ROOT / "docs/research/registry/OPP-E07-V1.json"),
            "--error-log",
            str(error_log),
        ]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["diagnostic_outcomes"]["status"] == "degraded"
    assert payload["candidate_runtime"]["status"] == "idle"
    assert TrialStore(trial_db).status()["health"]["last_result"] == "idle_registry_draft"
    assert (
        trial_worker.DiagnosticStore(diagnostics_db).status()["outcome_health"]["last_result"]
        == "degraded"
    )
    assert "diagnostic outcome phase isolated" in error_log.read_text(encoding="utf-8")


def test_trial_worker_records_finalizer_failure_in_durable_health(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        trial_worker,
        "finalize_pending_entry_dates",
        lambda _config: (_ for _ in ()).throw(ValueError("corrupt session proof")),
    )
    trial_db = tmp_path / "trial.db"
    error_log = tmp_path / "worker.err.log"

    exit_code = trial_worker.main(
        [
            "--trial-db",
            str(trial_db),
            "--registry-path",
            str(ROOT / "docs/research/registry/OPP-E07-V1.json"),
            "--error-log",
            str(error_log),
        ]
    )

    assert exit_code == 2
    status = TrialStore(trial_db).status()
    assert status["faults"] == 1
    assert status["health"]["last_result"] == "invalid"
    assert "corrupt session proof" in str(status["health"]["last_error"])
    assert "corrupt session proof" in error_log.read_text(encoding="utf-8")


def test_trial_worker_retryable_finalizer_failure_is_degraded_not_poisoned(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        trial_worker,
        "finalize_pending_entry_dates",
        lambda _config: (_ for _ in ()).throw(sqlite3.OperationalError("database is locked")),
    )
    trial_db = tmp_path / "trial.db"
    error_log = tmp_path / "worker.err.log"

    exit_code = trial_worker.main(
        [
            "--trial-db",
            str(trial_db),
            "--registry-path",
            str(ROOT / "docs/research/registry/OPP-E07-V1.json"),
            "--error-log",
            str(error_log),
        ]
    )

    assert exit_code == 2
    status = TrialStore(trial_db).status()
    assert status["faults"] == 0
    assert status["health"]["last_result"] == "degraded"
    assert "database is locked" in error_log.read_text(encoding="utf-8")


def test_trial_worker_clock_regression_is_degraded_not_poisoned(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        trial_worker,
        "finalize_pending_entry_dates",
        lambda _config: (_ for _ in ()).throw(
            TrialRuntimeRetryable("entry_completion_clock_moved_backwards")
        ),
    )
    trial_db = tmp_path / "trial.db"

    exit_code = trial_worker.main(
        [
            "--trial-db",
            str(trial_db),
            "--registry-path",
            str(ROOT / "docs/research/registry/OPP-E07-V1.json"),
            "--error-log",
            str(tmp_path / "worker.err.log"),
        ]
    )

    assert exit_code == 2
    status = TrialStore(trial_db).status()
    assert status["faults"] == 0
    assert status["health"]["last_result"] == "degraded"


def test_trial_worker_logs_when_trial_database_cannot_be_opened(tmp_path: Path) -> None:
    trial_db = tmp_path / "trial.db"
    trial_db.write_bytes(b"not sqlite")
    error_log = tmp_path / "worker.err.log"

    exit_code = trial_worker.main(
        [
            "--trial-db",
            str(trial_db),
            "--registry-path",
            str(ROOT / "docs/research/registry/OPP-E07-V1.json"),
            "--error-log",
            str(error_log),
        ]
    )

    assert exit_code == 2
    assert "DatabaseError" in error_log.read_text(encoding="utf-8")


def test_trial_worker_logs_original_error_when_fault_persistence_also_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        trial_worker,
        "finalize_pending_entry_dates",
        lambda _config: (_ for _ in ()).throw(ValueError("original finalizer error")),
    )
    monkeypatch.setattr(
        trial_worker,
        "TrialStore",
        lambda _path: (_ for _ in ()).throw(sqlite3.OperationalError("still locked")),
    )
    error_log = tmp_path / "worker.err.log"

    exit_code = trial_worker.main(
        [
            "--trial-db",
            str(tmp_path / "trial.db"),
            "--registry-path",
            str(ROOT / "docs/research/registry/OPP-E07-V1.json"),
            "--error-log",
            str(error_log),
        ]
    )

    assert exit_code == 2
    assert "original finalizer error" in error_log.read_text(encoding="utf-8")


def test_trial_status_cli_is_read_only_and_integrity_signaling(tmp_path: Path) -> None:
    valid_store = TrialStore(tmp_path / "valid.db")
    valid = CliRunner().invoke(
        cli.app, ["ops", "research-trial-status", "--trial-db", str(valid_store.path)]
    )
    assert valid.exit_code == 0
    missing_path = tmp_path / "missing.db"
    missing = CliRunner().invoke(
        cli.app, ["ops", "research-trial-status", "--trial-db", str(missing_path)]
    )
    assert missing.exit_code == 3
    assert not missing_path.exists()


def test_windows_trial_task_is_direct_hidden_pythonw() -> None:
    installer = (ROOT / "ops/windows/install-research-trial-task.ps1").read_text(encoding="utf-8")
    assert ".venv\\Scripts\\pythonw.exe" in installer
    action = installer.split("$action =", maxsplit=1)[1].split("$logonTrigger", maxsplit=1)[0]
    assert "-Execute $pythonExe" in action
    assert "powershell" not in action.lower()
    assert "cmd.exe" not in action.lower()
    assert "-Hidden" in installer
    assert "New-TimeSpan -Minutes $IntervalMinutes" in installer
    assert "-MultipleInstances IgnoreNew" in installer


def _install_finalizer_inputs(
    config: TrialRuntimeConfig,
    *,
    final_session_close: time | None = None,
) -> TrialCandidate:
    sessions = _sessions(first=date(2026, 7, 1), count=60)
    if final_session_close is not None:
        sessions = [
            (
                replace(
                    session,
                    closes_at_utc=datetime.combine(
                        session.session_date,
                        final_session_close,
                        tzinfo=UTC,
                    ),
                )
                if session.session_date == date(2026, 9, 9)
                else session
            )
            for session in sessions
        ]
    SessionFeedStore(config.session_feed_db).append(
        sessions,
        observed_at_utc=datetime(2026, 8, 26, 12, 0, tzinfo=UTC),
    )
    ready = datetime(2026, 8, 27, 13, 10, tzinfo=UTC)
    candidate = _trial_candidate(
        "a", symbol="TEST", rank="1", evidence_recorded_at=ready, imported_at=ready
    )
    assert TrialStore(config.trial_db).append_candidate(candidate)
    completed_sessions = [
        session
        for session in sessions
        if session.closes_at_utc < candidate.source_first_observed_at_utc
    ]
    bars = [
        DailyBar("TEST", session.session_date, 10.0, 11.0, 9.0, 10.0, 100_000.0)
        for session in completed_sessions[-20:]
    ]
    observed_at = datetime(2026, 8, 27, 13, 19, tzinfo=UTC)
    bar_store = BarFeedStore(config.bar_feed_db)
    assert bar_store.append_completed(bars, observed_at_utc=observed_at) == (20, 0, 0)
    bar_store.record_successful_poll(
        "TEST",
        local_date=date(2026, 8, 27),
        earliest_start_date=date(2026, 4, 29),
        requested_through_date=candidate.final_session_date,
        completed_through_date=completed_sessions[-1].session_date,
        now=observed_at,
        returned_bar_count=20,
        in_range_bar_count=20,
        source_rejection_count=0,
        validation_rejection_count=0,
    )
    return candidate


def _trial_outcome(candidate: TrialCandidate, completion: dict[str, object]) -> TrialOutcomeInputs:
    entry_price = 10.0
    exit_price = 11.0
    spy_entry = 100.0
    spy_exit = 101.0
    schedule_digests = completion["schedule_record_sha256s"]
    assert isinstance(schedule_digests, list) and schedule_digests
    return TrialOutcomeInputs(
        candidate_id=candidate.candidate_id,
        confirmatory_enrollment_sequence=1,
        evidence_record_sha256=candidate.evidence_record_sha256,
        entry_rank_sha256=candidate.entry_rank_sha256,
        symbol=candidate.symbol,
        entry_date=candidate.planned_entry_date,
        entry_at_utc=ENTRY_OPEN,
        exit_date=candidate.planned_entry_date,
        exit_at_utc=datetime(2026, 8, 27, 20, 0, tzinfo=UTC),
        entry_price=entry_price,
        exit_price=exit_price,
        exit_reason="target",
        gross_return=exit_price / entry_price - 1.0,
        spy_entry_price=spy_entry,
        spy_exit_price=spy_exit,
        spy_return=spy_exit / spy_entry - 1.0,
        recorded_at_utc=datetime(2026, 8, 27, 20, 1, tzinfo=UTC),
        schedule_observation_watermark=int(completion["schedule_observation_watermark"]),
        schedule_record_sha256s=(str(schedule_digests[0]),),
        bar_observation_watermark=20,
        bar_poll_receipt_watermark=2,
        bar_record_sha256s=("b" * 64,),
        bar_poll_receipt_sha256s=("c" * 64,),
    )


def _install_terminal_outcome_bars(
    config: TrialRuntimeConfig,
    candidate: TrialCandidate,
    *,
    omit_stock_date: date | None = None,
    omit_spy_date: date | None = None,
    receipt_before_bars: bool = False,
    exit_style: str = "target",
) -> datetime:
    assert exit_style in {"target", "time", "gap_stop"}
    sessions = [
        session
        for session in SessionFeedStore(config.session_feed_db, initialize=False).latest_schedule()
        if candidate.planned_entry_date <= session.session_date <= candidate.final_session_date
    ]
    assert len(sessions) == 10
    observed_at = sessions[-1].closes_at_utc + timedelta(minutes=30)
    store = BarFeedStore(config.bar_feed_db)

    def record_receipts(now: datetime) -> None:
        for symbol in (candidate.symbol, "SPY"):
            omitted = omit_stock_date if symbol == candidate.symbol else omit_spy_date
            returned = 9 if omitted is not None else 10
            store.record_successful_poll(
                symbol,
                local_date=now.astimezone(runtime.NEW_YORK).date(),
                earliest_start_date=candidate.planned_entry_date - timedelta(days=120),
                requested_through_date=candidate.final_session_date,
                completed_through_date=candidate.final_session_date,
                now=now,
                returned_bar_count=returned,
                in_range_bar_count=returned,
                source_rejection_count=0,
                validation_rejection_count=0,
            )

    if receipt_before_bars:
        record_receipts(observed_at - timedelta(minutes=1))
    stock_bars: list[DailyBar] = []
    for index, session in enumerate(sessions):
        if session.session_date == omit_stock_date:
            continue
        open_price, high, low, close = 10.0, 10.5, 9.5, 10.25
        if exit_style == "target":
            high = 11.0
        elif exit_style == "gap_stop" and index == 1:
            open_price, high, low, close = 8.5, 9.0, 8.0, 8.7
        stock_bars.append(
            DailyBar(
                candidate.symbol,
                session.session_date,
                open_price,
                high,
                low,
                close,
                100_000.0,
            )
        )
    spy_bars = [
        DailyBar("SPY", session.session_date, 100.0, 102.0, 99.0, 101.0, 1_000_000.0)
        for session in sessions
        if session.session_date != omit_spy_date
    ]
    store.append_completed(
        [*stock_bars, *spy_bars],
        observed_at_utc=observed_at,
        completed_through_date=candidate.final_session_date,
    )
    if not receipt_before_bars:
        record_receipts(observed_at + timedelta(minutes=1))
    return observed_at + timedelta(minutes=2)


def test_finalizer_seals_point_in_time_inputs_without_outcome_reads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    _install_finalizer_inputs(config)
    monkeypatch.setattr(finalizer, "_validated_trial_window", lambda _config: _active_window())
    original_validate = TrialStore.validate_integrity
    include_outcomes_seen: list[bool] = []

    def traced_validate(store: TrialStore, *, include_outcomes: bool = True) -> None:
        include_outcomes_seen.append(include_outcomes)
        original_validate(store, include_outcomes=include_outcomes)

    monkeypatch.setattr(TrialStore, "validate_integrity", traced_validate)
    decision_at = datetime(2026, 8, 27, 13, 21, tzinfo=UTC)

    result = finalizer.finalize_pending_entry_dates(config, clock=lambda: decision_at)

    assert result == finalizer.FinalizationResult("complete", dates_completed=1)
    assert include_outcomes_seen == [False]
    store = TrialStore(config.trial_db)
    assert store.resolutions()[0].enrollment_state == "enrolled"
    completion = store.entry_completion_records()[0]
    assert completion["decision_clock_at_utc"] == _utc_text(decision_at)
    assert completion["schedule_observation_watermark"] > 0
    assert len(completion["schedule_record_sha256s"]) >= 10
    assert completion["bar_observation_watermark"] == 20
    assert completion["bar_poll_receipt_watermark"] == 1
    assert len(completion["bar_record_sha256s"]) == 20
    assert len(completion["bar_poll_receipt_sha256s"]) == 1
    assert completion["prior_book_positions"] == []


def test_trial_outcome_is_append_only_bound_and_status_blinded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    candidate = _install_finalizer_inputs(config)
    monkeypatch.setattr(finalizer, "_validated_trial_window", lambda _config: _active_window())
    finalizer.finalize_pending_entry_dates(
        config, clock=lambda: datetime(2026, 8, 27, 13, 21, tzinfo=UTC)
    )
    store = TrialStore(config.trial_db)
    completion = store.entry_completion_records()[0]
    outcome = _trial_outcome(candidate, completion)

    with pytest.raises(TrialRuntimeInvalid, match="return_price_mismatch"):
        store.append_outcome(replace(outcome, gross_return=0.5))
    assert store.outcome_candidate_ids() == frozenset()

    digest = store.append_outcome(outcome)

    assert store.append_outcome(outcome) == digest
    conflicting = replace(outcome, exit_price=12.0, gross_return=0.2)
    with pytest.raises(TrialRuntimeInvalid, match="trial_outcome_conflicting_replay"):
        store.append_outcome(conflicting)
    assert store.outcome_candidate_ids() == frozenset({candidate.candidate_id})
    assert store.outcomes() == [outcome]
    store.validate_integrity()
    status = store.status()
    assert status["outcomes"] == 1
    assert "gross_return" not in json.dumps(status, sort_keys=True)

    with sqlite3.connect(store.path) as conn:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            conn.execute("UPDATE trial_outcomes SET recorded_at_utc='2026-01-01T00:00:00Z'")
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            conn.execute("DELETE FROM trial_outcomes")


def test_outcome_finalizer_waits_for_terminal_proof_then_materializes_without_aggregate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    candidate = _install_finalizer_inputs(config)
    monkeypatch.setattr(finalizer, "_validated_trial_window", lambda _config: _active_window())
    monkeypatch.setattr(
        outcome_finalizer, "_validated_trial_window", lambda _config: _active_window()
    )
    finalizer.finalize_pending_entry_dates(
        config, clock=lambda: datetime(2026, 8, 27, 13, 21, tzinfo=UTC)
    )

    before_terminal = outcome_finalizer.finalize_trial_outcomes(
        config, clock=lambda: datetime(2026, 8, 28, 20, 30, tzinfo=UTC)
    )
    assert before_terminal.status == "waiting"
    assert TrialStore(config.trial_db).outcome_candidate_ids() == frozenset()

    horizon = [
        session
        for session in SessionFeedStore(config.session_feed_db, initialize=False).latest_schedule()
        if candidate.planned_entry_date <= session.session_date <= candidate.final_session_date
    ]
    materialized_at = _install_terminal_outcome_bars(
        config,
        candidate,
        omit_stock_date=horizon[4].session_date,
    )
    result = outcome_finalizer.finalize_trial_outcomes(config, clock=lambda: materialized_at)

    assert result == outcome_finalizer.OutcomeFinalizationResult("complete", outcomes_added=1)
    outcome = TrialStore(config.trial_db).outcomes()[0]
    assert outcome.candidate_id == candidate.candidate_id
    assert outcome.exit_reason == "target"
    assert outcome.exit_date == candidate.planned_entry_date
    assert len(outcome.schedule_record_sha256s) == 10
    assert len(outcome.bar_record_sha256s) == 2
    assert len(outcome.bar_poll_receipt_sha256s) == 2


def test_outcome_finalizer_waits_for_receipt_that_observed_terminal_bars(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    candidate = _install_finalizer_inputs(config)
    monkeypatch.setattr(finalizer, "_validated_trial_window", lambda _config: _active_window())
    monkeypatch.setattr(
        outcome_finalizer, "_validated_trial_window", lambda _config: _active_window()
    )
    finalizer.finalize_pending_entry_dates(
        config, clock=lambda: datetime(2026, 8, 27, 13, 21, tzinfo=UTC)
    )
    now = _install_terminal_outcome_bars(config, candidate, receipt_before_bars=True)

    result = outcome_finalizer.finalize_trial_outcomes(config, clock=lambda: now)

    assert result.status == "waiting"
    assert result.reason == "terminal_bar_or_receipt_proof_unavailable"
    assert TrialStore(config.trial_db).outcome_candidate_ids() == frozenset()


def test_outcome_finalizer_rejects_terminal_healthy_poll_with_missing_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    candidate = _install_finalizer_inputs(config)
    monkeypatch.setattr(finalizer, "_validated_trial_window", lambda _config: _active_window())
    monkeypatch.setattr(
        outcome_finalizer, "_validated_trial_window", lambda _config: _active_window()
    )
    finalizer.finalize_pending_entry_dates(
        config, clock=lambda: datetime(2026, 8, 27, 13, 21, tzinfo=UTC)
    )
    sessions = [
        session
        for session in SessionFeedStore(config.session_feed_db, initialize=False).latest_schedule()
        if candidate.planned_entry_date <= session.session_date <= candidate.final_session_date
    ]
    now = _install_terminal_outcome_bars(
        config,
        candidate,
        omit_stock_date=sessions[4].session_date,
        exit_style="time",
    )

    with pytest.raises(TrialRuntimeInvalid, match="terminal_stock_path_incomplete"):
        outcome_finalizer.finalize_trial_outcomes(config, clock=lambda: now)

    assert TrialStore(config.trial_db).outcome_candidate_ids() == frozenset()


def test_outcome_finalizer_rejects_terminal_healthy_poll_with_missing_spy_endpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    candidate = _install_finalizer_inputs(config)
    monkeypatch.setattr(finalizer, "_validated_trial_window", lambda _config: _active_window())
    monkeypatch.setattr(
        outcome_finalizer, "_validated_trial_window", lambda _config: _active_window()
    )
    finalizer.finalize_pending_entry_dates(
        config, clock=lambda: datetime(2026, 8, 27, 13, 21, tzinfo=UTC)
    )
    now = _install_terminal_outcome_bars(
        config,
        candidate,
        omit_spy_date=candidate.planned_entry_date,
    )

    with pytest.raises(TrialRuntimeInvalid, match="outcome_terminal_spy_benchmark_incomplete"):
        outcome_finalizer.finalize_trial_outcomes(config, clock=lambda: now)


def test_outcome_finalizer_materializes_time_exit_at_frozen_early_close(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    candidate = _install_finalizer_inputs(config, final_session_close=time(17, 0))
    monkeypatch.setattr(finalizer, "_validated_trial_window", lambda _config: _active_window())
    monkeypatch.setattr(
        outcome_finalizer, "_validated_trial_window", lambda _config: _active_window()
    )
    finalizer.finalize_pending_entry_dates(
        config, clock=lambda: datetime(2026, 8, 27, 13, 21, tzinfo=UTC)
    )
    now = _install_terminal_outcome_bars(config, candidate, exit_style="time")

    result = outcome_finalizer.finalize_trial_outcomes(config, clock=lambda: now)

    assert result == outcome_finalizer.OutcomeFinalizationResult("complete", outcomes_added=1)
    outcome = TrialStore(config.trial_db).outcomes()[0]
    assert outcome.exit_reason == "time"
    assert outcome.exit_date == candidate.final_session_date
    assert outcome.exit_at_utc == datetime(2026, 9, 9, 17, 0, tzinfo=UTC)
    assert outcome.exit_price == 10.25


def test_outcome_finalizer_materializes_gap_down_stop_price(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    candidate = _install_finalizer_inputs(config)
    monkeypatch.setattr(finalizer, "_validated_trial_window", lambda _config: _active_window())
    monkeypatch.setattr(
        outcome_finalizer, "_validated_trial_window", lambda _config: _active_window()
    )
    finalizer.finalize_pending_entry_dates(
        config, clock=lambda: datetime(2026, 8, 27, 13, 21, tzinfo=UTC)
    )
    now = _install_terminal_outcome_bars(config, candidate, exit_style="gap_stop")

    result = outcome_finalizer.finalize_trial_outcomes(config, clock=lambda: now)

    assert result == outcome_finalizer.OutcomeFinalizationResult("complete", outcomes_added=1)
    outcome = TrialStore(config.trial_db).outcomes()[0]
    assert outcome.exit_reason == "stop"
    assert outcome.exit_price == 8.5
    assert outcome.gross_return == pytest.approx(-0.15)


def test_outcome_finalizer_rejects_corrupt_frozen_schedule_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    candidate = _install_finalizer_inputs(config)
    monkeypatch.setattr(finalizer, "_validated_trial_window", lambda _config: _active_window())
    finalizer.finalize_pending_entry_dates(
        config, clock=lambda: datetime(2026, 8, 27, 13, 21, tzinfo=UTC)
    )
    store = TrialStore(config.trial_db)
    completion = store.entry_completion_records()[0]
    completion["schedule_record_sha256s"] = ["0" * 64]

    with pytest.raises(TrialRuntimeInvalid, match="outcome_frozen_schedule_binding_invalid"):
        outcome_finalizer._bound_horizon(
            SessionFeedStore(config.session_feed_db, initialize=False),
            completion,
            candidate,
        )


def test_waiting_candidate_does_not_block_later_ready_outcome(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    first = _install_finalizer_inputs(config)
    ready = datetime(2026, 8, 27, 13, 10, tzinfo=UTC)
    second = _trial_candidate(
        "d",
        symbol="READY",
        rank="2",
        evidence_recorded_at=ready,
        imported_at=ready,
    )
    trial_store = TrialStore(config.trial_db)
    assert trial_store.append_candidate(second)
    sessions = SessionFeedStore(config.session_feed_db, initialize=False).latest_schedule()
    completed = [
        session
        for session in sessions
        if session.closes_at_utc < second.source_first_observed_at_utc
    ][-20:]
    bar_store = BarFeedStore(config.bar_feed_db, initialize=False)
    assert bar_store.append_completed(
        [
            DailyBar("READY", session.session_date, 10.0, 11.0, 9.0, 10.0, 100_000.0)
            for session in completed
        ],
        observed_at_utc=datetime(2026, 8, 27, 13, 19, tzinfo=UTC),
    ) == (20, 0, 0)
    bar_store.record_successful_poll(
        "READY",
        local_date=date(2026, 8, 27),
        earliest_start_date=date(2026, 4, 29),
        requested_through_date=second.final_session_date,
        completed_through_date=completed[-1].session_date,
        now=datetime(2026, 8, 27, 13, 19, tzinfo=UTC),
        returned_bar_count=20,
        in_range_bar_count=20,
        source_rejection_count=0,
        validation_rejection_count=0,
    )
    monkeypatch.setattr(finalizer, "_validated_trial_window", lambda _config: _active_window())
    monkeypatch.setattr(
        outcome_finalizer, "_validated_trial_window", lambda _config: _active_window()
    )
    sealed = finalizer.finalize_pending_entry_dates(
        config, clock=lambda: datetime(2026, 8, 27, 13, 21, tzinfo=UTC)
    )
    assert sealed.dates_completed == 1
    assert [item.enrollment_state for item in trial_store.resolutions()] == [
        "enrolled",
        "enrolled",
    ]
    now = _install_terminal_outcome_bars(config, second)

    result = outcome_finalizer.finalize_trial_outcomes(config, clock=lambda: now)

    assert result == outcome_finalizer.OutcomeFinalizationResult(
        "waiting",
        outcomes_added=1,
        outcomes_waiting=1,
        reason="terminal_bar_or_receipt_proof_unavailable",
    )
    assert trial_store.outcome_candidate_ids() == frozenset({second.candidate_id})
    assert first.candidate_id not in trial_store.outcome_candidate_ids()


def test_finalizer_waits_for_healthy_poll_then_lapses_at_official_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    candidate = _install_finalizer_inputs(config)
    with sqlite3.connect(config.bar_feed_db) as conn:
        conn.execute("DROP TRIGGER bar_poll_receipts_no_delete")
        conn.execute("DELETE FROM bar_poll_receipts")
    monkeypatch.setattr(finalizer, "_validated_trial_window", lambda _config: _active_window())

    waiting = finalizer.finalize_pending_entry_dates(
        config,
        clock=lambda: datetime(2026, 8, 27, 13, 21, tzinfo=UTC),
    )
    assert waiting.status == "waiting"
    assert waiting.pending_date == candidate.planned_entry_date
    assert waiting.reason == "healthy_bar_poll_proof_unavailable"
    assert TrialStore(config.trial_db).resolutions() == []

    lapsed = finalizer.finalize_pending_entry_dates(
        config,
        clock=lambda: ENTRY_OPEN,
    )
    assert lapsed == finalizer.FinalizationResult("complete", dates_lapsed=1)
    assert TrialStore(config.trial_db).resolutions()[0].enrollment_state == "missed"


def test_finalizer_waits_when_receipt_proves_observations_beyond_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    _install_finalizer_inputs(config)
    monkeypatch.setattr(finalizer, "_validated_trial_window", lambda _config: _active_window())
    monkeypatch.setattr(finalizer.BarFeedStore, "observation_watermark", lambda _store: 19)

    result = finalizer.finalize_pending_entry_dates(
        config,
        clock=lambda: datetime(2026, 8, 27, 13, 21, tzinfo=UTC),
    )

    assert result.status == "waiting"
    assert result.reason == "healthy_bar_poll_proof_unavailable"
    assert TrialStore(config.trial_db).resolutions() == []


def test_finalizer_rolls_boundary_race_into_lapse_without_permanent_fault(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    _install_finalizer_inputs(config)
    monkeypatch.setattr(finalizer, "_validated_trial_window", lambda _config: _active_window())
    moments = iter(
        (
            datetime(2026, 8, 27, 13, 21, tzinfo=UTC),
            ENTRY_OPEN,
            ENTRY_OPEN,
        )
    )

    result = finalizer.finalize_pending_entry_dates(config, clock=lambda: next(moments))

    assert result == finalizer.FinalizationResult("complete", dates_lapsed=1)
    store = TrialStore(config.trial_db)
    assert store.status()["faults"] == 0
    assert store.resolutions()[0].reason == (
        "entry_date_completion_lapsed:decision_clock_reached_official_open_before_seal"
    )


def test_finalizer_clock_regression_is_retryable_and_rolls_back(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    _install_finalizer_inputs(config)
    monkeypatch.setattr(finalizer, "_validated_trial_window", lambda _config: _active_window())
    decision_at = datetime(2026, 8, 27, 13, 21, tzinfo=UTC)
    moments = iter((decision_at, decision_at - timedelta(seconds=1)))

    with pytest.raises(TrialRuntimeRetryable, match="clock_moved_backwards"):
        finalizer.finalize_pending_entry_dates(config, clock=lambda: next(moments))

    store = TrialStore(config.trial_db)
    assert store.resolutions() == []
    assert store.status()["entry_date_completions"] == 0


def test_finalizer_clock_regression_across_open_is_retryable_and_rolls_back(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    _install_finalizer_inputs(config)
    monkeypatch.setattr(finalizer, "_validated_trial_window", lambda _config: _active_window())
    moments = iter(
        (
            datetime(2026, 8, 27, 13, 21, tzinfo=UTC),
            ENTRY_OPEN,
            ENTRY_OPEN - timedelta(seconds=1),
        )
    )

    with pytest.raises(TrialRuntimeRetryable, match="moved_backwards_across_open"):
        finalizer.finalize_pending_entry_dates(config, clock=lambda: next(moments))

    store = TrialStore(config.trial_db)
    assert store.resolutions() == []
    assert store.status()["entry_date_completions"] == 0
    assert store.status()["entry_date_lapses"] == 0


def test_finalizer_large_clock_regression_across_open_is_permanent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    _install_finalizer_inputs(config)
    monkeypatch.setattr(finalizer, "_validated_trial_window", lambda _config: _active_window())
    moments = iter(
        (
            datetime(2026, 8, 27, 13, 21, tzinfo=UTC),
            ENTRY_OPEN + timedelta(minutes=6),
            ENTRY_OPEN - timedelta(seconds=1),
        )
    )

    with pytest.raises(TrialRuntimeInvalid, match="clock_regression_exceeds_limit"):
        finalizer.finalize_pending_entry_dates(config, clock=lambda: next(moments))

    store = TrialStore(config.trial_db)
    assert store.resolutions() == []
    assert store.status()["entry_date_completions"] == 0
    assert store.status()["entry_date_lapses"] == 0


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


@pytest.mark.parametrize(
    "classification_state",
    ["routine", "unpartitionable", "ambiguous_multi_owner"],
)
def test_non_opportunistic_evidence_is_excluded_before_candidate_or_capacity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    classification_state: str,
) -> None:
    config = _config(tmp_path)
    _install_schedule(config)
    _install_evidence(config, classification_state=classification_state)
    monkeypatch.setattr(runtime, "_validated_trial_window", lambda _config: _active_window())

    result = run_trial_once(config, now=ACTIVATED_AT + timedelta(hours=2))

    assert result.status == "collecting"
    store = TrialStore(config.trial_db)
    assert store.candidates() == []
    assert store.disposition_counts() == {"excluded": 1}
    assert _disposition_reasons(config) == [f"classification_state_excluded:{classification_state}"]
    assert BarFeedStore(config.bar_feed_db).status()["request_count"] == 0


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


def test_runtime_candidate_import_clock_regression_is_degraded_without_fault(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    _install_schedule(config)
    _install_evidence(config)
    monkeypatch.setattr(runtime, "_validated_trial_window", lambda _config: _active_window())
    monkeypatch.setattr(
        TrialStore,
        "append_candidate",
        lambda _store, _candidate: (_ for _ in ()).throw(
            TrialRuntimeRetryable("candidate_import_time_moved_behind_entry_date_cursor")
        ),
    )

    result = run_trial_once(config, now=ACTIVATED_AT + timedelta(hours=1))

    assert result.status == "degraded"
    status = TrialStore(config.trial_db).status()
    assert status["faults"] == 0
    assert status["health"]["last_result"] == "degraded"
    assert "candidate_import_time_moved_behind" in str(status["health"]["last_error"])


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
