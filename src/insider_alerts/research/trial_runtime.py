"""Order-incapable prospective runtime for the OPP-E07-V1 shadow trial."""

from __future__ import annotations

import contextlib
import hashlib
import json
import sqlite3
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Literal
from zoneinfo import ZoneInfo

import rfc8785

from insider_alerts.research.bar_feed import BarFeedStore, BarRequest
from insider_alerts.research.inference import (
    CAPACITY_RANK_SALT,
    HYPOTHESIS_ID,
    _validate_registry,
    enrollment_deadline,
)
from insider_alerts.research.session_feed import ExchangeSession, SessionFeedStore

NEW_YORK = ZoneInfo("America/New_York")
TRIAL_CONTRACT_VERSION = "opp-e07-trial-runtime-v1"
BAR_REQUESTER = "OPP-E07-V1-completed-bar-input-v1"
SIGNAL_CUTOFF = time(9, 20)
MAX_SESSIONS = 10
BAR_LOOKBACK_CALENDAR_DAYS = 120
MAX_CHALLENGER_SLOTS = 20
MAX_TRANSIENT_CLOCK_REGRESSION = timedelta(minutes=5)
ENTRY_STATES = frozenset(
    {"enrolled", "ineligible", "overlap_suppressed", "capacity_suppressed", "missed"}
)


class TrialRuntimeInvalid(RuntimeError):
    """A fail-closed violation that can invalidate prospective trial operation."""


class TrialRuntimeRetryable(RuntimeError):
    """A transient runtime condition that must not poison prospective state."""


class EntryCompletionDecisionAfterOpen(TrialRuntimeInvalid):
    """The transactional decision clock crossed the official entry open."""

    def __init__(self, decision_clock_at_utc: datetime) -> None:
        self.decision_clock_at_utc = decision_clock_at_utc
        super().__init__("entry_completion_decision_clock_reached_official_open")


class EvidenceExcluded(RuntimeError):
    """An expected evidence row that is outside this registry's candidate universe."""


class EvidenceNotReady(RuntimeError):
    """A valid-looking evidence row that must remain unresolved for a later cycle."""


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("trial-runtime timestamp cannot be naive")
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("persisted trial-runtime timestamp is naive")
    return parsed.astimezone(UTC)


def _raise_clock_regression(
    *,
    earlier: datetime,
    later: datetime,
    retryable_reason: str,
    invalid_reason: str,
) -> None:
    """Retry small wall-clock steps but fault when timestamp trust is materially broken."""

    regression = later - earlier
    if timedelta(0) < regression <= MAX_TRANSIENT_CLOCK_REGRESSION:
        raise TrialRuntimeRetryable(retryable_reason)
    raise TrialRuntimeInvalid(invalid_reason)


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical(value: dict[str, Any]) -> bytes:
    return rfc8785.dumps(value)


def _require_sha256(value: str, context: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise TrialRuntimeInvalid(f"{context}_not_sha256")
    return value


@dataclass(frozen=True, slots=True)
class TrialWindow:
    status: Literal["draft", "active"]
    registry_sha256: str
    activated_at_utc: datetime | None = None
    enrollment_deadline_utc: datetime | None = None


@dataclass(frozen=True, slots=True)
class TrialCandidate:
    candidate_id: str
    evidence_snapshot_id: str
    evidence_record_sha256: str
    packet_id: str
    accession_number: str
    symbol: str
    source_first_observed_at_utc: datetime
    evidence_recorded_at_utc: datetime
    classification_state: str
    transaction_owner_mapping: str
    history_coverage_complete: bool
    planned_entry_date: date
    entry_opens_at_utc: datetime
    final_session_date: date
    entry_rank_sha256: str
    imported_at_utc: datetime


@dataclass(frozen=True, slots=True)
class TrialRuntimeConfig:
    trial_db: Path
    evidence_db: Path
    bar_feed_db: Path
    session_feed_db: Path
    registry_path: Path


@dataclass(frozen=True, slots=True)
class TrialRuntimeResult:
    status: Literal["idle", "collecting", "degraded", "invalid"]
    evidence_seen: int = 0
    candidates_added: int = 0
    bar_requests_ensured: int = 0
    unresolved_evidence: int = 0
    error: str | None = None


@dataclass(frozen=True, slots=True)
class EntryEligibility:
    eligible: bool
    reason: str


@dataclass(frozen=True, slots=True)
class TrialResolution:
    candidate_id: str
    entry_date: date
    enrollment_state: Literal[
        "enrolled", "ineligible", "overlap_suppressed", "capacity_suppressed", "missed"
    ]
    reason: str
    confirmatory_enrollment_sequence: int | None
    resolved_at_utc: datetime


@dataclass(frozen=True, slots=True)
class PriorBookPosition:
    candidate_id: str
    symbol: str
    occupied_through_date: date
    basis: Literal["bars_no_exit_before_entry_open", "missing_bars_conservative"]
    bar_record_sha256s: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class EntryCompletionInputs:
    entry_date: date
    completed_at_utc: datetime
    entry_opens_at_utc: datetime
    final_session_date: date
    schedule_observation_watermark: int
    schedule_record_sha256s: tuple[str, ...]
    bar_observation_watermark: int
    bar_poll_receipt_watermark: int
    bar_record_sha256s: tuple[str, ...]
    bar_poll_receipt_sha256s: tuple[str, ...]
    prior_book_positions: tuple[PriorBookPosition, ...]


@dataclass(frozen=True, slots=True)
class EntryLapseInputs:
    entry_date: date
    lapsed_at_utc: datetime
    reason: str
    entry_opens_at_utc: datetime
    schedule_observation_watermark: int
    schedule_record_sha256s: tuple[str, ...]


def _validated_trial_window(config: TrialRuntimeConfig) -> TrialWindow:
    try:
        registry_bytes = config.registry_path.read_bytes()
        registry = json.loads(registry_bytes)
    except (OSError, json.JSONDecodeError) as exc:
        raise TrialRuntimeInvalid("prospective_registry_unreadable") from exc
    if not isinstance(registry, dict):
        raise TrialRuntimeInvalid("prospective_registry_not_object")
    status = registry.get("status")
    try:
        if status == "draft":
            _validate_registry(registry, allow_draft=True)
            return TrialWindow("draft", _sha256(registry_bytes))
        if status != "active":
            raise TrialRuntimeInvalid("prospective_registry_not_draft_or_active")
        _validate_registry(registry, allow_draft=False)
    except ValueError as exc:
        raise TrialRuntimeInvalid(f"prospective_registry_invalid:{exc}") from exc
    activation = registry.get("activation")
    if not isinstance(activation, dict):
        raise TrialRuntimeInvalid("active_registry_missing_activation")
    activated_at = _parse_utc(str(activation.get("activated_at_utc", "")))
    return TrialWindow(
        "active",
        _sha256(registry_bytes),
        activated_at,
        enrollment_deadline(activated_at),
    )


def planned_entry_session(
    signal_at_utc: datetime,
    schedule_as_known: list[ExchangeSession],
) -> tuple[ExchangeSession, date]:
    """Freeze an entry and ten-session horizon without consulting wall-clock now."""

    if signal_at_utc.tzinfo is None:
        raise ValueError("signal timestamp cannot be naive")
    signal_local = signal_at_utc.astimezone(NEW_YORK)
    ordered = sorted(schedule_as_known, key=lambda item: item.session_date)
    eligible = [item for item in ordered if item.session_date >= signal_local.date()]
    if (
        eligible
        and eligible[0].session_date == signal_local.date()
        and signal_local.time() >= SIGNAL_CUTOFF
    ):
        eligible = eligible[1:]
    if len(eligible) < MAX_SESSIONS:
        raise TrialRuntimeInvalid("schedule_as_known_does_not_cover_entry_horizon")
    if eligible[0].opens_at_utc <= signal_at_utc:
        raise TrialRuntimeInvalid("planned_entry_open_not_after_signal")
    return eligible[0], eligible[MAX_SESSIONS - 1].session_date


def _normalized_symbol(symbol: str) -> str:
    normalized = symbol.strip().upper()
    if not normalized or len(normalized) > 32 or not normalized.isascii():
        raise TrialRuntimeInvalid("candidate_symbol_not_canonical")
    if any(character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-" for character in normalized):
        raise TrialRuntimeInvalid("candidate_symbol_not_supported_by_bar_feed")
    return normalized


def _entry_rank(
    *,
    entry_date: date,
    packet_id: str,
    accession_number: str,
    symbol: str,
) -> str:
    material = (
        f"{CAPACITY_RANK_SALT}|{entry_date.isoformat()}|{packet_id}|{accession_number}|{symbol}"
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def resolve_ranked_entry_date(
    candidates: Sequence[TrialCandidate],
    *,
    eligibility: Mapping[str, EntryEligibility],
    occupied_symbols: frozenset[str],
    occupied_slots: int,
    next_enrollment_sequence: int,
    completed_at_utc: datetime,
    entry_opens_at_utc: datetime,
) -> list[TrialResolution]:
    """Resolve one complete entry date from only pre-open, preregistered inputs."""

    if not candidates:
        raise TrialRuntimeInvalid("entry_completion_has_no_candidates")
    if completed_at_utc.tzinfo is None:
        raise ValueError("entry completion timestamp cannot be naive")
    if occupied_slots != len(occupied_symbols) or not 0 <= occupied_slots <= MAX_CHALLENGER_SLOTS:
        raise TrialRuntimeInvalid("prior_book_occupancy_invalid")
    if next_enrollment_sequence < 1:
        raise TrialRuntimeInvalid("next_enrollment_sequence_invalid")
    if len({candidate.candidate_id for candidate in candidates}) != len(candidates):
        raise TrialRuntimeInvalid("entry_completion_candidate_duplicate")
    if len({candidate.entry_rank_sha256 for candidate in candidates}) != len(candidates):
        raise TrialRuntimeInvalid("entry_completion_rank_duplicate")
    for candidate in candidates:
        if any(
            value.tzinfo is None
            for value in (
                candidate.source_first_observed_at_utc,
                candidate.evidence_recorded_at_utc,
                candidate.imported_at_utc,
                candidate.entry_opens_at_utc,
            )
        ):
            raise TrialRuntimeInvalid("entry_completion_candidate_timestamp_naive")
    entry_dates = {candidate.planned_entry_date for candidate in candidates}
    if entry_opens_at_utc.tzinfo is None:
        raise TrialRuntimeInvalid("entry_completion_official_open_naive")
    if len(entry_dates) != 1:
        raise TrialRuntimeInvalid("entry_completion_candidate_session_mismatch")
    entry_date = next(iter(entry_dates))
    entry_open = entry_opens_at_utc.astimezone(UTC)
    if entry_open.astimezone(NEW_YORK).date() != entry_date:
        raise TrialRuntimeInvalid("entry_completion_official_open_date_mismatch")
    cutoff_local = datetime.combine(entry_date, SIGNAL_CUTOFF, tzinfo=NEW_YORK)
    cutoff_utc = cutoff_local.astimezone(UTC)
    if (
        completed_at_utc.astimezone(NEW_YORK).date() != entry_date
        or not cutoff_utc <= completed_at_utc < entry_open
    ):
        raise TrialRuntimeInvalid("entry_completion_outside_pre_open_window")
    if set(eligibility) != {candidate.candidate_id for candidate in candidates}:
        raise TrialRuntimeInvalid("entry_completion_eligibility_set_mismatch")

    active_symbols = set(occupied_symbols)
    active_slots = occupied_slots
    sequence = next_enrollment_sequence
    resolutions: list[TrialResolution] = []
    for candidate in sorted(
        candidates,
        key=lambda item: (item.entry_rank_sha256, item.candidate_id),
    ):
        state: Literal[
            "enrolled", "ineligible", "overlap_suppressed", "capacity_suppressed", "missed"
        ]
        reason: str
        enrollment_sequence: int | None = None
        if candidate.source_first_observed_at_utc >= cutoff_utc:
            raise TrialRuntimeInvalid("same_date_candidate_source_not_before_cutoff")
        if candidate.evidence_recorded_at_utc >= cutoff_utc:
            state, reason = "missed", "evidence_not_recorded_before_entry_cutoff"
        elif candidate.imported_at_utc >= cutoff_utc:
            state, reason = "missed", "candidate_not_imported_before_entry_cutoff"
        elif not eligibility[candidate.candidate_id].eligible:
            state, reason = "ineligible", eligibility[candidate.candidate_id].reason
        elif candidate.symbol in active_symbols:
            state, reason = "overlap_suppressed", "same_symbol_position_active_at_entry_open"
        elif active_slots >= MAX_CHALLENGER_SLOTS:
            state, reason = "capacity_suppressed", "challenger_book_at_20_slot_capacity"
        else:
            state, reason = "enrolled", eligibility[candidate.candidate_id].reason
            enrollment_sequence = sequence
            sequence += 1
            active_slots += 1
            active_symbols.add(candidate.symbol)
        resolutions.append(
            TrialResolution(
                candidate_id=candidate.candidate_id,
                entry_date=entry_date,
                enrollment_state=state,
                reason=reason,
                confirmatory_enrollment_sequence=enrollment_sequence,
                resolved_at_utc=completed_at_utc,
            )
        )
    return resolutions


def _validate_cutoff_resolution_states(
    candidates: Sequence[TrialCandidate],
    resolutions: Sequence[TrialResolution],
    *,
    cutoff_utc: datetime,
) -> None:
    """Re-enforce the timestamp-derived states at the persistence boundary."""
    for candidate, resolution in zip(candidates, resolutions, strict=True):
        if candidate.source_first_observed_at_utc >= cutoff_utc:
            raise TrialRuntimeInvalid("same_date_candidate_source_not_before_cutoff")
        expected_reason: str | None = None
        if candidate.evidence_recorded_at_utc >= cutoff_utc:
            expected_reason = "evidence_not_recorded_before_entry_cutoff"
        elif candidate.imported_at_utc >= cutoff_utc:
            expected_reason = "candidate_not_imported_before_entry_cutoff"
        if expected_reason is not None and (
            resolution.enrollment_state != "missed"
            or resolution.reason != expected_reason
            or resolution.confirmatory_enrollment_sequence is not None
        ):
            raise TrialRuntimeInvalid("completion_candidate_cutoff_state_mismatch")
        if expected_reason is None and resolution.enrollment_state == "missed":
            raise TrialRuntimeInvalid("completion_candidate_cutoff_state_mismatch")


class TrialStore:
    """Append-only candidate universe plus mutable operational health."""

    def __init__(
        self,
        path: Path | str,
        *,
        initialize: bool = True,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.path = Path(path)
        self._clock = clock or (lambda: datetime.now(UTC))
        if initialize:
            self._initialize()

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _initialize(self) -> None:
        with contextlib.closing(self._connect()) as conn, conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=FULL;
                CREATE TABLE IF NOT EXISTS trial_candidates (
                    sequence INTEGER NOT NULL UNIQUE,
                    candidate_id TEXT PRIMARY KEY,
                    evidence_snapshot_id TEXT NOT NULL UNIQUE,
                    evidence_record_sha256 TEXT NOT NULL UNIQUE,
                    packet_id TEXT NOT NULL UNIQUE,
                    accession_number TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    source_first_observed_at_utc TEXT NOT NULL,
                    evidence_recorded_at_utc TEXT NOT NULL,
                    classification_state TEXT NOT NULL,
                    transaction_owner_mapping TEXT NOT NULL,
                    history_coverage_complete INTEGER NOT NULL
                      CHECK(history_coverage_complete IN(0,1)),
                    planned_entry_date TEXT NOT NULL,
                    entry_opens_at_utc TEXT NOT NULL,
                    final_session_date TEXT NOT NULL,
                    entry_rank_sha256 TEXT NOT NULL UNIQUE,
                    imported_at_utc TEXT NOT NULL,
                    record_sha256 TEXT NOT NULL UNIQUE,
                    record_json BLOB NOT NULL
                );
                CREATE INDEX IF NOT EXISTS trial_candidates_entry_rank
                ON trial_candidates(planned_entry_date,entry_rank_sha256,candidate_id);
                CREATE UNIQUE INDEX IF NOT EXISTS trial_candidates_accession_symbol
                ON trial_candidates(accession_number,symbol);
                CREATE TRIGGER IF NOT EXISTS trial_candidates_sequence
                BEFORE INSERT ON trial_candidates
                WHEN NEW.sequence<>(SELECT COALESCE(MAX(sequence),0)+1 FROM trial_candidates)
                BEGIN SELECT RAISE(ABORT, 'trial candidate sequence must be gap-free'); END;
                CREATE TRIGGER IF NOT EXISTS trial_candidates_no_update
                BEFORE UPDATE ON trial_candidates
                BEGIN SELECT RAISE(ABORT, 'trial candidates are immutable'); END;

                CREATE TABLE IF NOT EXISTS trial_evidence_dispositions (
                    sequence INTEGER NOT NULL UNIQUE,
                    disposition_id TEXT PRIMARY KEY,
                    evidence_snapshot_id TEXT NOT NULL UNIQUE,
                    evidence_record_sha256 TEXT NOT NULL UNIQUE,
                    state TEXT NOT NULL CHECK(state IN('excluded','invalid')),
                    reason TEXT NOT NULL,
                    recorded_at_utc TEXT NOT NULL,
                    record_sha256 TEXT NOT NULL UNIQUE,
                    record_json BLOB NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS trial_evidence_dispositions_sequence
                BEFORE INSERT ON trial_evidence_dispositions
                WHEN NEW.sequence<>(
                  SELECT COALESCE(MAX(sequence),0)+1 FROM trial_evidence_dispositions
                )
                BEGIN SELECT RAISE(ABORT, 'evidence disposition sequence must be gap-free'); END;
                CREATE TRIGGER IF NOT EXISTS trial_evidence_dispositions_no_update
                BEFORE UPDATE ON trial_evidence_dispositions
                BEGIN SELECT RAISE(ABORT, 'evidence dispositions are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS trial_evidence_dispositions_no_delete
                BEFORE DELETE ON trial_evidence_dispositions
                BEGIN SELECT RAISE(ABORT, 'evidence dispositions are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS trial_candidates_no_delete
                BEFORE DELETE ON trial_candidates
                BEGIN SELECT RAISE(ABORT, 'trial candidates are immutable'); END;

                CREATE TABLE IF NOT EXISTS trial_resolutions (
                    sequence INTEGER NOT NULL UNIQUE,
                    resolution_id TEXT PRIMARY KEY,
                    candidate_id TEXT NOT NULL UNIQUE,
                    entry_date TEXT NOT NULL,
                    enrollment_state TEXT NOT NULL CHECK(enrollment_state IN(
                      'enrolled','ineligible','overlap_suppressed','capacity_suppressed','missed'
                    )),
                    reason TEXT NOT NULL,
                    confirmatory_enrollment_sequence INTEGER UNIQUE,
                    resolved_at_utc TEXT NOT NULL,
                    record_sha256 TEXT NOT NULL UNIQUE,
                    record_json BLOB NOT NULL,
                    FOREIGN KEY(candidate_id) REFERENCES trial_candidates(candidate_id)
                );
                CREATE TRIGGER IF NOT EXISTS trial_resolutions_sequence
                BEFORE INSERT ON trial_resolutions
                WHEN NEW.sequence<>(SELECT COALESCE(MAX(sequence),0)+1 FROM trial_resolutions)
                BEGIN SELECT RAISE(ABORT, 'trial resolution sequence must be gap-free'); END;
                CREATE TRIGGER IF NOT EXISTS trial_resolutions_no_update
                BEFORE UPDATE ON trial_resolutions
                BEGIN SELECT RAISE(ABORT, 'trial resolutions are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS trial_resolutions_no_delete
                BEFORE DELETE ON trial_resolutions
                BEGIN SELECT RAISE(ABORT, 'trial resolutions are immutable'); END;

                CREATE TABLE IF NOT EXISTS trial_entry_date_completions (
                    sequence INTEGER NOT NULL UNIQUE,
                    entry_date TEXT PRIMARY KEY,
                    completed_at_utc TEXT NOT NULL,
                    decision_clock_at_utc TEXT NOT NULL,
                    record_sha256 TEXT NOT NULL UNIQUE,
                    record_json BLOB NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS trial_entry_date_completions_sequence
                BEFORE INSERT ON trial_entry_date_completions
                WHEN NEW.sequence<>(
                  SELECT COALESCE(MAX(sequence),0)+1 FROM trial_entry_date_completions
                )
                BEGIN SELECT RAISE(ABORT, 'entry-date completion sequence must be gap-free'); END;
                CREATE TRIGGER IF NOT EXISTS trial_entry_date_completions_no_update
                BEFORE UPDATE ON trial_entry_date_completions
                BEGIN SELECT RAISE(ABORT, 'entry-date completions are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS trial_entry_date_completions_no_delete
                BEFORE DELETE ON trial_entry_date_completions
                BEGIN SELECT RAISE(ABORT, 'entry-date completions are immutable'); END;

                CREATE TABLE IF NOT EXISTS trial_entry_date_lapses (
                    sequence INTEGER NOT NULL UNIQUE,
                    entry_date TEXT PRIMARY KEY,
                    lapsed_at_utc TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    record_sha256 TEXT NOT NULL UNIQUE,
                    record_json BLOB NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS trial_entry_date_lapses_sequence
                BEFORE INSERT ON trial_entry_date_lapses
                WHEN NEW.sequence<>(
                  SELECT COALESCE(MAX(sequence),0)+1 FROM trial_entry_date_lapses
                )
                BEGIN SELECT RAISE(ABORT, 'entry-date lapse sequence must be gap-free'); END;
                CREATE TRIGGER IF NOT EXISTS trial_entry_date_lapses_no_update
                BEFORE UPDATE ON trial_entry_date_lapses
                BEGIN SELECT RAISE(ABORT, 'entry-date lapses are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS trial_entry_date_lapses_no_delete
                BEFORE DELETE ON trial_entry_date_lapses
                BEGIN SELECT RAISE(ABORT, 'entry-date lapses are immutable'); END;

                CREATE TABLE IF NOT EXISTS trial_outcomes (
                    sequence INTEGER NOT NULL UNIQUE,
                    outcome_id TEXT PRIMARY KEY,
                    candidate_id TEXT NOT NULL UNIQUE,
                    recorded_at_utc TEXT NOT NULL,
                    record_sha256 TEXT NOT NULL UNIQUE,
                    record_json BLOB NOT NULL,
                    FOREIGN KEY(candidate_id) REFERENCES trial_candidates(candidate_id)
                );
                CREATE TRIGGER IF NOT EXISTS trial_outcomes_sequence
                BEFORE INSERT ON trial_outcomes
                WHEN NEW.sequence<>(SELECT COALESCE(MAX(sequence),0)+1 FROM trial_outcomes)
                BEGIN SELECT RAISE(ABORT, 'trial outcome sequence must be gap-free'); END;
                CREATE TRIGGER IF NOT EXISTS trial_outcomes_no_update
                BEFORE UPDATE ON trial_outcomes
                BEGIN SELECT RAISE(ABORT, 'trial outcomes are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS trial_outcomes_no_delete
                BEFORE DELETE ON trial_outcomes
                BEGIN SELECT RAISE(ABORT, 'trial outcomes are immutable'); END;

                CREATE TABLE IF NOT EXISTS trial_faults (
                    sequence INTEGER NOT NULL UNIQUE,
                    fault_id TEXT PRIMARY KEY,
                    occurred_at_utc TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    record_sha256 TEXT NOT NULL UNIQUE,
                    record_json BLOB NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS trial_faults_sequence
                BEFORE INSERT ON trial_faults
                WHEN NEW.sequence<>(SELECT COALESCE(MAX(sequence),0)+1 FROM trial_faults)
                BEGIN SELECT RAISE(ABORT, 'trial fault sequence must be gap-free'); END;
                CREATE TRIGGER IF NOT EXISTS trial_faults_no_update
                BEFORE UPDATE ON trial_faults
                BEGIN SELECT RAISE(ABORT, 'trial faults are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS trial_faults_no_delete
                BEFORE DELETE ON trial_faults
                BEGIN SELECT RAISE(ABORT, 'trial faults are immutable'); END;

                CREATE TABLE IF NOT EXISTS trial_health (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    last_worker_heartbeat_utc TEXT NOT NULL,
                    last_result TEXT NOT NULL,
                    last_error TEXT,
                    evidence_seen INTEGER NOT NULL,
                    candidates_total INTEGER NOT NULL,
                    unresolved_evidence INTEGER NOT NULL
                );
                """
            )

    @staticmethod
    def _candidate_record(candidate: TrialCandidate) -> dict[str, Any]:
        return {
            "contract_version": TRIAL_CONTRACT_VERSION,
            "candidate_id": candidate.candidate_id,
            "evidence_snapshot_id": candidate.evidence_snapshot_id,
            "evidence_record_sha256": candidate.evidence_record_sha256,
            "packet_id": candidate.packet_id,
            "accession_number": candidate.accession_number,
            "symbol": candidate.symbol,
            "source_first_observed_at_utc": _utc_text(candidate.source_first_observed_at_utc),
            "evidence_recorded_at_utc": _utc_text(candidate.evidence_recorded_at_utc),
            "classification_state": candidate.classification_state,
            "transaction_owner_mapping": candidate.transaction_owner_mapping,
            "history_coverage_complete": candidate.history_coverage_complete,
            "planned_entry_date": candidate.planned_entry_date.isoformat(),
            "entry_opens_at_utc": _utc_text(candidate.entry_opens_at_utc),
            "final_session_date": candidate.final_session_date.isoformat(),
            "entry_rank_sha256": candidate.entry_rank_sha256,
            "imported_at_utc": _utc_text(candidate.imported_at_utc),
        }

    def candidate_for_evidence(self, evidence_record_sha256: str) -> TrialCandidate | None:
        with contextlib.closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM trial_candidates WHERE evidence_record_sha256=?",
                (evidence_record_sha256,),
            ).fetchone()
        return None if row is None else self._verify_candidate_row(row)

    def append_candidate(self, candidate: TrialCandidate) -> bool:
        record = self._candidate_record(candidate)
        encoded = _canonical(record)
        digest = _sha256(encoded)
        with contextlib.closing(self._connect()) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            if conn.execute(
                """
                SELECT 1 FROM trial_evidence_dispositions
                WHERE evidence_record_sha256=?
                """,
                (candidate.evidence_record_sha256,),
            ).fetchone():
                return False
            existing = conn.execute(
                "SELECT * FROM trial_candidates WHERE candidate_id=?",
                (candidate.candidate_id,),
            ).fetchone()
            if existing is not None:
                persisted = self._verify_candidate_row(existing)
                expected_fields = asdict(candidate)
                persisted_fields = asdict(persisted)
                expected_fields.pop("imported_at_utc")
                persisted_fields.pop("imported_at_utc")
                if expected_fields != persisted_fields:
                    raise TrialRuntimeInvalid("candidate_identity_reused_with_different_content")
                return False
            latest_seal = conn.execute(
                """
                SELECT sealed_at_utc FROM (
                  SELECT entry_date,decision_clock_at_utc sealed_at_utc
                  FROM trial_entry_date_completions
                  UNION ALL
                  SELECT entry_date,lapsed_at_utc sealed_at_utc FROM trial_entry_date_lapses
                ) ORDER BY entry_date DESC LIMIT 1
                """
            ).fetchone()
            if latest_seal is not None:
                latest_sealed_at = _parse_utc(str(latest_seal["sealed_at_utc"]))
                if candidate.imported_at_utc < latest_sealed_at:
                    _raise_clock_regression(
                        earlier=candidate.imported_at_utc,
                        later=latest_sealed_at,
                        retryable_reason="candidate_import_time_moved_behind_entry_date_cursor",
                        invalid_reason="candidate_import_clock_regression_exceeds_limit",
                    )
            sequence = int(
                conn.execute("SELECT COALESCE(MAX(sequence),0)+1 FROM trial_candidates").fetchone()[
                    0
                ]
            )
            conn.execute(
                """
                INSERT INTO trial_candidates(
                  sequence,candidate_id,evidence_snapshot_id,evidence_record_sha256,packet_id,
                  accession_number,symbol,source_first_observed_at_utc,evidence_recorded_at_utc,
                  classification_state,transaction_owner_mapping,history_coverage_complete,
                  planned_entry_date,entry_opens_at_utc,final_session_date,entry_rank_sha256,
                  imported_at_utc,record_sha256,record_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    sequence,
                    candidate.candidate_id,
                    candidate.evidence_snapshot_id,
                    candidate.evidence_record_sha256,
                    candidate.packet_id,
                    candidate.accession_number,
                    candidate.symbol,
                    _utc_text(candidate.source_first_observed_at_utc),
                    _utc_text(candidate.evidence_recorded_at_utc),
                    candidate.classification_state,
                    candidate.transaction_owner_mapping,
                    int(candidate.history_coverage_complete),
                    candidate.planned_entry_date.isoformat(),
                    _utc_text(candidate.entry_opens_at_utc),
                    candidate.final_session_date.isoformat(),
                    candidate.entry_rank_sha256,
                    _utc_text(candidate.imported_at_utc),
                    digest,
                    encoded,
                ),
            )
            sealed = conn.execute(
                """
                SELECT kind,entry_date,sealed_at_utc FROM (
                  SELECT 'completion' kind,entry_date,decision_clock_at_utc sealed_at_utc
                  FROM trial_entry_date_completions
                  UNION ALL
                  SELECT 'lapse' kind,entry_date,lapsed_at_utc sealed_at_utc
                  FROM trial_entry_date_lapses
                ) WHERE entry_date>=? ORDER BY entry_date ASC LIMIT 1
                """,
                (candidate.planned_entry_date.isoformat(),),
            ).fetchone()
            if sealed is not None:
                sealed_at = _parse_utc(str(sealed["sealed_at_utc"]))
                if candidate.imported_at_utc < sealed_at:
                    raise EvidenceNotReady("candidate_import_predates_existing_entry_date_seal")
                reason = (
                    f"candidate_arrived_after_entry_date_{sealed['kind']}"
                    if str(sealed["entry_date"]) == candidate.planned_entry_date.isoformat()
                    else "candidate_arrived_behind_entry_date_cursor"
                )
                self._insert_resolution(
                    conn,
                    TrialResolution(
                        candidate_id=candidate.candidate_id,
                        entry_date=candidate.planned_entry_date,
                        enrollment_state="missed",
                        reason=reason,
                        confirmatory_enrollment_sequence=None,
                        resolved_at_utc=candidate.imported_at_utc,
                    ),
                )
        return True

    @classmethod
    def _verify_candidate_row(cls, row: sqlite3.Row) -> TrialCandidate:
        raw = bytes(row["record_json"])
        if _sha256(raw) != str(row["record_sha256"]):
            raise TrialRuntimeInvalid("candidate_stored_bytes_digest_mismatch")
        record = json.loads(raw)
        if not isinstance(record, dict) or _canonical(record) != raw:
            raise TrialRuntimeInvalid("candidate_record_not_canonical")
        expected = cls._candidate_record(
            TrialCandidate(
                candidate_id=str(row["candidate_id"]),
                evidence_snapshot_id=str(row["evidence_snapshot_id"]),
                evidence_record_sha256=str(row["evidence_record_sha256"]),
                packet_id=str(row["packet_id"]),
                accession_number=str(row["accession_number"]),
                symbol=str(row["symbol"]),
                source_first_observed_at_utc=_parse_utc(str(row["source_first_observed_at_utc"])),
                evidence_recorded_at_utc=_parse_utc(str(row["evidence_recorded_at_utc"])),
                classification_state=str(row["classification_state"]),
                transaction_owner_mapping=str(row["transaction_owner_mapping"]),
                history_coverage_complete=bool(row["history_coverage_complete"]),
                planned_entry_date=date.fromisoformat(str(row["planned_entry_date"])),
                entry_opens_at_utc=_parse_utc(str(row["entry_opens_at_utc"])),
                final_session_date=date.fromisoformat(str(row["final_session_date"])),
                entry_rank_sha256=str(row["entry_rank_sha256"]),
                imported_at_utc=_parse_utc(str(row["imported_at_utc"])),
            )
        )
        if record != expected:
            raise TrialRuntimeInvalid("candidate_columns_do_not_match_record")
        return TrialCandidate(
            candidate_id=str(record["candidate_id"]),
            evidence_snapshot_id=str(record["evidence_snapshot_id"]),
            evidence_record_sha256=str(record["evidence_record_sha256"]),
            packet_id=str(record["packet_id"]),
            accession_number=str(record["accession_number"]),
            symbol=str(record["symbol"]),
            source_first_observed_at_utc=_parse_utc(str(record["source_first_observed_at_utc"])),
            evidence_recorded_at_utc=_parse_utc(str(record["evidence_recorded_at_utc"])),
            classification_state=str(record["classification_state"]),
            transaction_owner_mapping=str(record["transaction_owner_mapping"]),
            history_coverage_complete=bool(record["history_coverage_complete"]),
            planned_entry_date=date.fromisoformat(str(record["planned_entry_date"])),
            entry_opens_at_utc=_parse_utc(str(record["entry_opens_at_utc"])),
            final_session_date=date.fromisoformat(str(record["final_session_date"])),
            entry_rank_sha256=str(record["entry_rank_sha256"]),
            imported_at_utc=_parse_utc(str(record["imported_at_utc"])),
        )

    def candidates(self) -> list[TrialCandidate]:
        with contextlib.closing(self._connect()) as conn:
            rows = conn.execute("SELECT * FROM trial_candidates ORDER BY sequence").fetchall()
        return [self._verify_candidate_row(row) for row in rows]

    @staticmethod
    def _resolution_record(resolution: TrialResolution) -> dict[str, Any]:
        return {
            "contract_version": TRIAL_CONTRACT_VERSION,
            "candidate_id": resolution.candidate_id,
            "entry_date": resolution.entry_date.isoformat(),
            "enrollment_state": resolution.enrollment_state,
            "reason": resolution.reason,
            "confirmatory_enrollment_sequence": resolution.confirmatory_enrollment_sequence,
            "resolved_at_utc": _utc_text(resolution.resolved_at_utc),
        }

    @classmethod
    def _insert_resolution(cls, conn: sqlite3.Connection, resolution: TrialResolution) -> str:
        if not resolution.reason.strip():
            raise TrialRuntimeInvalid("trial_resolution_reason_empty")
        if resolution.enrollment_state not in ENTRY_STATES:
            raise TrialRuntimeInvalid("trial_resolution_state_invalid")
        if (resolution.enrollment_state == "enrolled") != (
            resolution.confirmatory_enrollment_sequence is not None
        ):
            raise TrialRuntimeInvalid("trial_resolution_sequence_state_mismatch")
        record = cls._resolution_record(resolution)
        encoded = _canonical(record)
        digest = _sha256(encoded)
        sequence = int(
            conn.execute("SELECT COALESCE(MAX(sequence),0)+1 FROM trial_resolutions").fetchone()[0]
        )
        conn.execute(
            """
            INSERT INTO trial_resolutions(
              sequence,resolution_id,candidate_id,entry_date,enrollment_state,reason,
              confirmatory_enrollment_sequence,resolved_at_utc,record_sha256,record_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                sequence,
                str(uuid.uuid5(uuid.NAMESPACE_URL, f"{HYPOTHESIS_ID}|resolution|{digest}")),
                resolution.candidate_id,
                resolution.entry_date.isoformat(),
                resolution.enrollment_state,
                resolution.reason,
                resolution.confirmatory_enrollment_sequence,
                _utc_text(resolution.resolved_at_utc),
                digest,
                encoded,
            ),
        )
        return digest

    @classmethod
    def _verify_resolution_row(cls, row: sqlite3.Row) -> TrialResolution:
        raw = bytes(row["record_json"])
        if _sha256(raw) != str(row["record_sha256"]):
            raise TrialRuntimeInvalid("trial_resolution_digest_mismatch")
        record = json.loads(raw)
        if not isinstance(record, dict) or _canonical(record) != raw:
            raise TrialRuntimeInvalid("trial_resolution_not_canonical")
        resolution = TrialResolution(
            candidate_id=str(row["candidate_id"]),
            entry_date=date.fromisoformat(str(row["entry_date"])),
            enrollment_state=str(row["enrollment_state"]),  # type: ignore[arg-type]
            reason=str(row["reason"]),
            confirmatory_enrollment_sequence=(
                None
                if row["confirmatory_enrollment_sequence"] is None
                else int(row["confirmatory_enrollment_sequence"])
            ),
            resolved_at_utc=_parse_utc(str(row["resolved_at_utc"])),
        )
        if resolution.enrollment_state not in ENTRY_STATES:
            raise TrialRuntimeInvalid("trial_resolution_state_invalid")
        if record != cls._resolution_record(resolution):
            raise TrialRuntimeInvalid("trial_resolution_columns_mismatch")
        if (resolution.enrollment_state == "enrolled") != (
            resolution.confirmatory_enrollment_sequence is not None
        ):
            raise TrialRuntimeInvalid("trial_resolution_sequence_state_mismatch")
        return resolution

    def resolutions(self) -> list[TrialResolution]:
        with contextlib.closing(self._connect()) as conn:
            rows = conn.execute("SELECT * FROM trial_resolutions ORDER BY sequence").fetchall()
        return [self._verify_resolution_row(row) for row in rows]

    def entry_completion_records(self) -> list[dict[str, Any]]:
        with contextlib.closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT * FROM trial_entry_date_completions ORDER BY sequence"
            ).fetchall()
        return [self._verify_completion_row(row) for row in rows]

    @staticmethod
    def _prior_book_record(position: PriorBookPosition) -> dict[str, Any]:
        symbol = _normalized_symbol(position.symbol)
        if not position.candidate_id.strip():
            raise TrialRuntimeInvalid("prior_book_candidate_id_empty")
        if position.basis not in {
            "bars_no_exit_before_entry_open",
            "missing_bars_conservative",
        }:
            raise TrialRuntimeInvalid("prior_book_basis_invalid")
        digests = tuple(sorted(set(position.bar_record_sha256s)))
        if len(digests) != len(position.bar_record_sha256s):
            raise TrialRuntimeInvalid("prior_book_bar_digest_duplicate")
        for digest in digests:
            _require_sha256(digest, "prior_book_bar_record")
        return {
            "candidate_id": position.candidate_id,
            "symbol": symbol,
            "occupied_through_date": position.occupied_through_date.isoformat(),
            "basis": position.basis,
            "bar_record_sha256s": list(digests),
        }

    @staticmethod
    def _verify_completion_row(row: sqlite3.Row) -> dict[str, Any]:
        raw = bytes(row["record_json"])
        if _sha256(raw) != str(row["record_sha256"]):
            raise TrialRuntimeInvalid("entry_completion_digest_mismatch")
        record = json.loads(raw)
        if not isinstance(record, dict) or _canonical(record) != raw:
            raise TrialRuntimeInvalid("entry_completion_not_canonical")
        required = {
            "contract_version",
            "entry_date",
            "completed_at_utc",
            "decision_clock_at_utc",
            "entry_opens_at_utc",
            "final_session_date",
            "schedule_observation_watermark",
            "schedule_record_sha256s",
            "bar_observation_watermark",
            "bar_poll_receipt_watermark",
            "bar_record_sha256s",
            "bar_poll_receipt_sha256s",
            "candidate_records",
            "candidate_set_sha256",
            "prior_book_positions",
            "prior_book_sha256",
            "resolution_record_sha256s",
            "resolution_set_sha256",
        }
        if set(record) != required or record["contract_version"] != TRIAL_CONTRACT_VERSION:
            raise TrialRuntimeInvalid("entry_completion_record_shape_invalid")
        if (
            record["entry_date"] != row["entry_date"]
            or record["completed_at_utc"] != row["completed_at_utc"]
            or record["decision_clock_at_utc"] != row["decision_clock_at_utc"]
        ):
            raise TrialRuntimeInvalid("entry_completion_columns_mismatch")
        entry_date = date.fromisoformat(str(record["entry_date"]))
        completed_at = _parse_utc(str(record["completed_at_utc"]))
        decision_clock_at = _parse_utc(str(record["decision_clock_at_utc"]))
        entry_open = _parse_utc(str(record["entry_opens_at_utc"]))
        final_session_date = date.fromisoformat(str(record["final_session_date"]))
        cutoff_utc = datetime.combine(entry_date, SIGNAL_CUTOFF, tzinfo=NEW_YORK).astimezone(UTC)
        if (
            completed_at.astimezone(NEW_YORK).date() != entry_date
            or entry_open.astimezone(NEW_YORK).date() != entry_date
            or final_session_date < entry_date
            or not cutoff_utc <= completed_at <= decision_clock_at < entry_open
        ):
            raise TrialRuntimeInvalid("entry_completion_local_date_mismatch")
        for name in (
            "schedule_observation_watermark",
            "bar_observation_watermark",
            "bar_poll_receipt_watermark",
        ):
            if (
                isinstance(record[name], bool)
                or not isinstance(record[name], int)
                or record[name] < 0
            ):
                raise TrialRuntimeInvalid(f"completion_{name}_invalid")
        schedule_digests = record["schedule_record_sha256s"]
        bar_digests = record["bar_record_sha256s"]
        poll_digests = record["bar_poll_receipt_sha256s"]
        resolution_digests = record["resolution_record_sha256s"]
        candidate_records = record["candidate_records"]
        prior_book_positions = record["prior_book_positions"]
        if not all(
            isinstance(value, list)
            for value in (
                schedule_digests,
                bar_digests,
                poll_digests,
                resolution_digests,
                candidate_records,
                prior_book_positions,
            )
        ):
            raise TrialRuntimeInvalid("entry_completion_digest_lists_invalid")
        for name, digests in (
            ("schedule", schedule_digests),
            ("bar", bar_digests),
            ("poll", poll_digests),
        ):
            if digests != sorted(set(digests)):
                raise TrialRuntimeInvalid(f"entry_completion_{name}_digests_not_canonical")
            for digest in digests:
                _require_sha256(str(digest), f"entry_completion_{name}_record")
        if not schedule_digests:
            raise TrialRuntimeInvalid("entry_completion_schedule_binding_empty")
        if (
            not resolution_digests
            or len(resolution_digests) != len(set(resolution_digests))
            or not candidate_records
            or len(candidate_records) != len(resolution_digests)
        ):
            raise TrialRuntimeInvalid("entry_completion_bound_sets_invalid")
        for digest in resolution_digests:
            _require_sha256(str(digest), "entry_completion_bound_record")
        expected_prior_positions: list[dict[str, Any]] = []
        for position in prior_book_positions:
            if not isinstance(position, dict):
                raise TrialRuntimeInvalid("prior_book_position_invalid")
            try:
                expected_prior_positions.append(
                    TrialStore._prior_book_record(
                        PriorBookPosition(
                            candidate_id=str(position["candidate_id"]),
                            symbol=str(position["symbol"]),
                            occupied_through_date=date.fromisoformat(
                                str(position["occupied_through_date"])
                            ),
                            basis=str(position["basis"]),  # type: ignore[arg-type]
                            bar_record_sha256s=tuple(position["bar_record_sha256s"]),
                        )
                    )
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise TrialRuntimeInvalid("prior_book_position_invalid") from exc
        expected_prior_positions.sort(key=lambda item: (item["symbol"], item["candidate_id"]))
        if expected_prior_positions != prior_book_positions:
            raise TrialRuntimeInvalid("prior_book_positions_not_canonical")
        if len({item["candidate_id"] for item in prior_book_positions}) != len(
            prior_book_positions
        ) or len({item["symbol"] for item in prior_book_positions}) != len(prior_book_positions):
            raise TrialRuntimeInvalid("prior_book_positions_duplicate")
        if _sha256(_canonical({"positions": prior_book_positions})) != record["prior_book_sha256"]:
            raise TrialRuntimeInvalid("completion_prior_book_digest_mismatch")
        expected_candidate_records: list[dict[str, str]] = []
        for candidate in candidate_records:
            if not isinstance(candidate, dict) or set(candidate) != {
                "candidate_id",
                "record_sha256",
            }:
                raise TrialRuntimeInvalid("entry_completion_candidate_binding_invalid")
            expected_candidate_records.append(
                {
                    "candidate_id": str(candidate["candidate_id"]),
                    "record_sha256": _require_sha256(
                        str(candidate["record_sha256"]), "completion_candidate_record"
                    ),
                }
            )
        candidate_ids = [candidate["candidate_id"] for candidate in expected_candidate_records]
        candidate_digests = [candidate["record_sha256"] for candidate in expected_candidate_records]
        if len(candidate_ids) != len(set(candidate_ids)) or len(candidate_digests) != len(
            set(candidate_digests)
        ):
            raise TrialRuntimeInvalid("entry_completion_candidate_bindings_duplicate")
        if expected_candidate_records != candidate_records:
            raise TrialRuntimeInvalid("entry_completion_candidate_binding_not_canonical")
        if _sha256(_canonical({"candidates": candidate_records})) != record["candidate_set_sha256"]:
            raise TrialRuntimeInvalid("entry_completion_candidate_set_digest_mismatch")
        if (
            _sha256(_canonical({"resolution_record_sha256s": resolution_digests}))
            != record["resolution_set_sha256"]
        ):
            raise TrialRuntimeInvalid("entry_completion_resolution_set_digest_mismatch")
        return record

    def append_entry_completion(
        self,
        inputs: EntryCompletionInputs,
        resolutions: Sequence[TrialResolution],
    ) -> str:
        if inputs.completed_at_utc.tzinfo is None or inputs.entry_opens_at_utc.tzinfo is None:
            raise TrialRuntimeInvalid("entry_completion_timestamp_naive")
        for name, watermark in (
            ("schedule", inputs.schedule_observation_watermark),
            ("bar", inputs.bar_observation_watermark),
            ("bar_poll_receipt", inputs.bar_poll_receipt_watermark),
        ):
            if isinstance(watermark, bool) or not isinstance(watermark, int) or watermark < 0:
                raise TrialRuntimeInvalid(f"completion_{name}_watermark_invalid")
        schedule_digests = tuple(sorted(set(inputs.schedule_record_sha256s)))
        bar_digests = tuple(sorted(set(inputs.bar_record_sha256s)))
        poll_digests = tuple(sorted(set(inputs.bar_poll_receipt_sha256s)))
        for name, supplied, canonical in (
            ("schedule", inputs.schedule_record_sha256s, schedule_digests),
            ("bar", inputs.bar_record_sha256s, bar_digests),
            ("poll", inputs.bar_poll_receipt_sha256s, poll_digests),
        ):
            if len(canonical) != len(supplied):
                raise TrialRuntimeInvalid(f"completion_{name}_digest_duplicate")
            for digest in canonical:
                _require_sha256(digest, f"completion_{name}_record")
        if not schedule_digests:
            raise TrialRuntimeInvalid("completion_schedule_binding_empty")
        prior_positions = sorted(
            (self._prior_book_record(position) for position in inputs.prior_book_positions),
            key=lambda item: (item["symbol"], item["candidate_id"]),
        )
        if len({item["candidate_id"] for item in prior_positions}) != len(prior_positions) or len(
            {item["symbol"] for item in prior_positions}
        ) != len(prior_positions):
            raise TrialRuntimeInvalid("prior_book_positions_duplicate")
        prior_book_sha = _sha256(_canonical({"positions": prior_positions}))
        replay_resolution_digests = [
            _sha256(_canonical(self._resolution_record(resolution))) for resolution in resolutions
        ]
        with contextlib.closing(self._connect()) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            if conn.execute(
                "SELECT 1 FROM trial_entry_date_lapses WHERE entry_date=?",
                (inputs.entry_date.isoformat(),),
            ).fetchone():
                raise TrialRuntimeInvalid("completion_conflicts_with_entry_date_lapse")
            existing = conn.execute(
                "SELECT * FROM trial_entry_date_completions WHERE entry_date=?",
                (inputs.entry_date.isoformat(),),
            ).fetchone()
            if existing is not None:
                existing_record = self._verify_completion_row(existing)
                replay_matches = (
                    existing_record["completed_at_utc"] == _utc_text(inputs.completed_at_utc)
                    and existing_record["entry_opens_at_utc"]
                    == _utc_text(inputs.entry_opens_at_utc)
                    and existing_record["final_session_date"]
                    == inputs.final_session_date.isoformat()
                    and existing_record["schedule_observation_watermark"]
                    == inputs.schedule_observation_watermark
                    and existing_record["schedule_record_sha256s"] == list(schedule_digests)
                    and existing_record["bar_observation_watermark"]
                    == inputs.bar_observation_watermark
                    and existing_record["bar_poll_receipt_watermark"]
                    == inputs.bar_poll_receipt_watermark
                    and existing_record["bar_record_sha256s"] == list(bar_digests)
                    and existing_record["bar_poll_receipt_sha256s"] == list(poll_digests)
                    and existing_record["prior_book_sha256"] == prior_book_sha
                    and existing_record["resolution_record_sha256s"] == replay_resolution_digests
                    and [
                        str(candidate["candidate_id"])
                        for candidate in existing_record["candidate_records"]
                    ]
                    == [resolution.candidate_id for resolution in resolutions]
                )
                if replay_matches:
                    return str(existing["record_sha256"])
                raise TrialRuntimeInvalid("entry_date_completion_conflicting_replay")
            latest = conn.execute(
                """
                SELECT entry_date,sealed_at_utc FROM (
                  SELECT entry_date,decision_clock_at_utc sealed_at_utc
                  FROM trial_entry_date_completions
                  UNION ALL
                  SELECT entry_date,lapsed_at_utc sealed_at_utc FROM trial_entry_date_lapses
                ) ORDER BY entry_date DESC LIMIT 1
                """
            ).fetchone()
            if latest is not None:
                if date.fromisoformat(str(latest["entry_date"])) >= inputs.entry_date:
                    raise TrialRuntimeInvalid("entry_dates_not_strictly_ordered")
                latest_sealed_at = _parse_utc(str(latest["sealed_at_utc"]))
                if inputs.completed_at_utc < latest_sealed_at:
                    _raise_clock_regression(
                        earlier=inputs.completed_at_utc,
                        later=latest_sealed_at,
                        retryable_reason="entry_seal_time_moved_backwards",
                        invalid_reason="entry_seal_clock_regression_exceeds_limit",
                    )
            earlier_pending = conn.execute(
                """
                SELECT 1 FROM trial_candidates candidate
                WHERE candidate.planned_entry_date<? AND NOT EXISTS(
                  SELECT 1 FROM trial_resolutions resolution
                  WHERE resolution.candidate_id=candidate.candidate_id
                ) LIMIT 1
                """,
                (inputs.entry_date.isoformat(),),
            ).fetchone()
            if earlier_pending is not None:
                raise TrialRuntimeInvalid("completion_skips_earlier_pending_candidate")
            candidate_rows = conn.execute(
                """
                SELECT * FROM trial_candidates candidate
                WHERE candidate.planned_entry_date=? AND NOT EXISTS(
                  SELECT 1 FROM trial_resolutions resolution
                  WHERE resolution.candidate_id=candidate.candidate_id
                ) ORDER BY candidate.entry_rank_sha256,candidate.candidate_id
                """,
                (inputs.entry_date.isoformat(),),
            ).fetchall()
            if not candidate_rows:
                raise TrialRuntimeInvalid("entry_completion_has_no_pending_candidates")
            candidates = [self._verify_candidate_row(row) for row in candidate_rows]
            latest_import = max(candidate.imported_at_utc for candidate in candidates)
            if inputs.completed_at_utc < latest_import:
                _raise_clock_regression(
                    earlier=inputs.completed_at_utc,
                    later=latest_import,
                    retryable_reason="entry_completion_clock_moved_behind_candidate_import",
                    invalid_reason="entry_completion_import_clock_regression_exceeds_limit",
                )
            cutoff_utc = datetime.combine(
                inputs.entry_date,
                SIGNAL_CUTOFF,
                tzinfo=NEW_YORK,
            ).astimezone(UTC)
            entry_open = inputs.entry_opens_at_utc.astimezone(UTC)
            if (
                inputs.completed_at_utc.astimezone(NEW_YORK).date() != inputs.entry_date
                or entry_open.astimezone(NEW_YORK).date() != inputs.entry_date
                or not cutoff_utc <= inputs.completed_at_utc < entry_open
            ):
                raise TrialRuntimeInvalid("entry_completion_outside_pre_open_window")
            expected_ids = [candidate.candidate_id for candidate in candidates]
            if [resolution.candidate_id for resolution in resolutions] != expected_ids:
                raise TrialRuntimeInvalid("completion_resolution_candidate_set_mismatch")
            if any(
                resolution.entry_date != inputs.entry_date
                or resolution.resolved_at_utc != inputs.completed_at_utc
                for resolution in resolutions
            ):
                raise TrialRuntimeInvalid("completion_resolution_time_or_date_mismatch")
            _validate_cutoff_resolution_states(
                candidates,
                resolutions,
                cutoff_utc=cutoff_utc,
            )
            expected_next = int(
                conn.execute(
                    "SELECT COALESCE(MAX(confirmatory_enrollment_sequence),0)+1 "
                    "FROM trial_resolutions"
                ).fetchone()[0]
            )
            enrolled_sequences = [
                resolution.confirmatory_enrollment_sequence
                for resolution in resolutions
                if resolution.enrollment_state == "enrolled"
            ]
            if enrolled_sequences != list(
                range(expected_next, expected_next + len(enrolled_sequences))
            ):
                raise TrialRuntimeInvalid("completion_enrollment_sequence_not_gap_free")
            candidate_material = {
                "candidates": [
                    {
                        "candidate_id": str(row["candidate_id"]),
                        "record_sha256": str(row["record_sha256"]),
                    }
                    for row in candidate_rows
                ]
            }
            candidate_set_sha = _sha256(_canonical(candidate_material))
            resolution_digests = [
                self._insert_resolution(conn, resolution) for resolution in resolutions
            ]
            resolution_set_sha = _sha256(
                _canonical({"resolution_record_sha256s": resolution_digests})
            )
            decision_clock_at = self._clock()
            if decision_clock_at.tzinfo is None:
                raise TrialRuntimeInvalid("entry_completion_decision_clock_naive")
            decision_clock_at = decision_clock_at.astimezone(UTC)
            if decision_clock_at < inputs.completed_at_utc:
                _raise_clock_regression(
                    earlier=decision_clock_at,
                    later=inputs.completed_at_utc,
                    retryable_reason="entry_completion_clock_moved_backwards",
                    invalid_reason="entry_completion_clock_regression_exceeds_limit",
                )
            if decision_clock_at >= entry_open:
                raise EntryCompletionDecisionAfterOpen(decision_clock_at)
            record: dict[str, Any] = {
                "contract_version": TRIAL_CONTRACT_VERSION,
                "entry_date": inputs.entry_date.isoformat(),
                "completed_at_utc": _utc_text(inputs.completed_at_utc),
                "decision_clock_at_utc": _utc_text(decision_clock_at),
                "entry_opens_at_utc": _utc_text(entry_open),
                "final_session_date": inputs.final_session_date.isoformat(),
                "schedule_observation_watermark": inputs.schedule_observation_watermark,
                "schedule_record_sha256s": list(schedule_digests),
                "bar_observation_watermark": inputs.bar_observation_watermark,
                "bar_poll_receipt_watermark": inputs.bar_poll_receipt_watermark,
                "bar_record_sha256s": list(bar_digests),
                "bar_poll_receipt_sha256s": list(poll_digests),
                "candidate_records": candidate_material["candidates"],
                "candidate_set_sha256": candidate_set_sha,
                "prior_book_positions": prior_positions,
                "prior_book_sha256": prior_book_sha,
                "resolution_record_sha256s": resolution_digests,
                "resolution_set_sha256": resolution_set_sha,
            }
            encoded = _canonical(record)
            digest = _sha256(encoded)
            sequence = int(
                conn.execute(
                    "SELECT COALESCE(MAX(sequence),0)+1 FROM trial_entry_date_completions"
                ).fetchone()[0]
            )
            conn.execute(
                "INSERT INTO trial_entry_date_completions VALUES(?,?,?,?,?,?)",
                (
                    sequence,
                    inputs.entry_date.isoformat(),
                    _utc_text(inputs.completed_at_utc),
                    _utc_text(decision_clock_at),
                    digest,
                    encoded,
                ),
            )
        return digest

    @staticmethod
    def _verify_lapse_row(row: sqlite3.Row) -> dict[str, Any]:
        raw = bytes(row["record_json"])
        if _sha256(raw) != str(row["record_sha256"]):
            raise TrialRuntimeInvalid("entry_lapse_digest_mismatch")
        record = json.loads(raw)
        if not isinstance(record, dict) or _canonical(record) != raw:
            raise TrialRuntimeInvalid("entry_lapse_not_canonical")
        required = {
            "contract_version",
            "entry_date",
            "lapsed_at_utc",
            "reason",
            "entry_opens_at_utc",
            "schedule_observation_watermark",
            "schedule_record_sha256s",
            "candidate_records",
            "candidate_set_sha256",
            "resolution_record_sha256s",
            "resolution_set_sha256",
        }
        if set(record) != required or record["contract_version"] != TRIAL_CONTRACT_VERSION:
            raise TrialRuntimeInvalid("entry_lapse_record_shape_invalid")
        if (
            record["entry_date"] != row["entry_date"]
            or record["lapsed_at_utc"] != row["lapsed_at_utc"]
            or record["reason"] != row["reason"]
        ):
            raise TrialRuntimeInvalid("entry_lapse_columns_mismatch")
        if not isinstance(record["reason"], str) or not record["reason"].strip():
            raise TrialRuntimeInvalid("entry_lapse_reason_empty")
        entry_open = _parse_utc(str(record["entry_opens_at_utc"]))
        lapsed_at = _parse_utc(str(record["lapsed_at_utc"]))
        if (
            entry_open.astimezone(NEW_YORK).date() != date.fromisoformat(str(record["entry_date"]))
            or lapsed_at < entry_open
        ):
            raise TrialRuntimeInvalid("entry_lapse_official_open_date_mismatch")
        if (
            isinstance(record["schedule_observation_watermark"], bool)
            or not isinstance(record["schedule_observation_watermark"], int)
            or record["schedule_observation_watermark"] < 0
        ):
            raise TrialRuntimeInvalid("entry_lapse_schedule_watermark_invalid")
        schedule_digests = record["schedule_record_sha256s"]
        if (
            not isinstance(schedule_digests, list)
            or not schedule_digests
            or schedule_digests != sorted(set(schedule_digests))
        ):
            raise TrialRuntimeInvalid("entry_lapse_schedule_digests_invalid")
        for digest in schedule_digests:
            _require_sha256(str(digest), "lapse_schedule_record")
        candidate_records = record["candidate_records"]
        resolution_digests = record["resolution_record_sha256s"]
        if (
            not isinstance(candidate_records, list)
            or not isinstance(resolution_digests, list)
            or not candidate_records
            or len(candidate_records) != len(resolution_digests)
            or len(resolution_digests) != len(set(resolution_digests))
        ):
            raise TrialRuntimeInvalid("entry_lapse_bound_sets_invalid")
        candidate_ids: list[str] = []
        for candidate in candidate_records:
            if not isinstance(candidate, dict) or set(candidate) != {
                "candidate_id",
                "record_sha256",
            }:
                raise TrialRuntimeInvalid("entry_lapse_candidate_binding_invalid")
            candidate_ids.append(str(candidate["candidate_id"]))
            _require_sha256(str(candidate["record_sha256"]), "lapse_candidate_record")
        if len(candidate_ids) != len(set(candidate_ids)):
            raise TrialRuntimeInvalid("entry_lapse_candidate_binding_duplicate")
        for digest in resolution_digests:
            _require_sha256(str(digest), "lapse_resolution_record")
        if _sha256(_canonical({"candidates": candidate_records})) != record["candidate_set_sha256"]:
            raise TrialRuntimeInvalid("entry_lapse_candidate_set_digest_mismatch")
        if (
            _sha256(_canonical({"resolution_record_sha256s": resolution_digests}))
            != record["resolution_set_sha256"]
        ):
            raise TrialRuntimeInvalid("entry_lapse_resolution_set_digest_mismatch")
        return record

    def append_entry_lapse(self, inputs: EntryLapseInputs) -> str:
        if inputs.lapsed_at_utc.tzinfo is None or inputs.entry_opens_at_utc.tzinfo is None:
            raise ValueError("entry lapse timestamp cannot be naive")
        if not inputs.reason.strip():
            raise TrialRuntimeInvalid("entry_lapse_reason_empty")
        if (
            isinstance(inputs.schedule_observation_watermark, bool)
            or not isinstance(inputs.schedule_observation_watermark, int)
            or inputs.schedule_observation_watermark < 0
        ):
            raise TrialRuntimeInvalid("entry_lapse_schedule_watermark_invalid")
        schedule_digests = tuple(sorted(set(inputs.schedule_record_sha256s)))
        if not schedule_digests or len(schedule_digests) != len(inputs.schedule_record_sha256s):
            raise TrialRuntimeInvalid("entry_lapse_schedule_digests_invalid")
        for digest in schedule_digests:
            _require_sha256(digest, "lapse_schedule_record")
        with contextlib.closing(self._connect()) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            if conn.execute(
                "SELECT 1 FROM trial_entry_date_completions WHERE entry_date=?",
                (inputs.entry_date.isoformat(),),
            ).fetchone():
                raise TrialRuntimeInvalid("lapse_conflicts_with_entry_date_completion")
            existing = conn.execute(
                "SELECT * FROM trial_entry_date_lapses WHERE entry_date=?",
                (inputs.entry_date.isoformat(),),
            ).fetchone()
            if existing is not None:
                existing_lapse = self._verify_lapse_row(existing)
                if (
                    existing_lapse["lapsed_at_utc"] == _utc_text(inputs.lapsed_at_utc)
                    and existing_lapse["reason"] == inputs.reason
                    and existing_lapse["entry_opens_at_utc"] == _utc_text(inputs.entry_opens_at_utc)
                    and existing_lapse["schedule_observation_watermark"]
                    == inputs.schedule_observation_watermark
                    and existing_lapse["schedule_record_sha256s"] == list(schedule_digests)
                ):
                    return str(existing["record_sha256"])
                raise TrialRuntimeInvalid("entry_date_lapse_conflicting_replay")
            latest = conn.execute(
                """
                SELECT entry_date,sealed_at_utc FROM (
                  SELECT entry_date,decision_clock_at_utc sealed_at_utc
                  FROM trial_entry_date_completions
                  UNION ALL
                  SELECT entry_date,lapsed_at_utc sealed_at_utc FROM trial_entry_date_lapses
                ) ORDER BY entry_date DESC LIMIT 1
                """
            ).fetchone()
            if latest is not None:
                if date.fromisoformat(str(latest["entry_date"])) >= inputs.entry_date:
                    raise TrialRuntimeInvalid("entry_dates_not_strictly_ordered")
                latest_sealed_at = _parse_utc(str(latest["sealed_at_utc"]))
                if inputs.lapsed_at_utc < latest_sealed_at:
                    _raise_clock_regression(
                        earlier=inputs.lapsed_at_utc,
                        later=latest_sealed_at,
                        retryable_reason="entry_seal_time_moved_backwards",
                        invalid_reason="entry_seal_clock_regression_exceeds_limit",
                    )
            earlier_pending = conn.execute(
                """
                SELECT 1 FROM trial_candidates candidate
                WHERE candidate.planned_entry_date<? AND NOT EXISTS(
                  SELECT 1 FROM trial_resolutions resolution
                  WHERE resolution.candidate_id=candidate.candidate_id
                ) LIMIT 1
                """,
                (inputs.entry_date.isoformat(),),
            ).fetchone()
            if earlier_pending is not None:
                raise TrialRuntimeInvalid("lapse_skips_earlier_pending_candidate")
            candidate_rows = conn.execute(
                """
                SELECT * FROM trial_candidates candidate
                WHERE candidate.planned_entry_date=? AND NOT EXISTS(
                  SELECT 1 FROM trial_resolutions resolution
                  WHERE resolution.candidate_id=candidate.candidate_id
                ) ORDER BY candidate.entry_rank_sha256,candidate.candidate_id
                """,
                (inputs.entry_date.isoformat(),),
            ).fetchall()
            if not candidate_rows:
                raise TrialRuntimeInvalid("entry_lapse_has_no_pending_candidates")
            candidates = [self._verify_candidate_row(row) for row in candidate_rows]
            latest_import = max(candidate.imported_at_utc for candidate in candidates)
            if inputs.lapsed_at_utc < latest_import:
                _raise_clock_regression(
                    earlier=inputs.lapsed_at_utc,
                    later=latest_import,
                    retryable_reason="entry_lapse_clock_moved_behind_candidate_import",
                    invalid_reason="entry_lapse_import_clock_regression_exceeds_limit",
                )
            entry_open = inputs.entry_opens_at_utc.astimezone(UTC)
            if (
                entry_open.astimezone(NEW_YORK).date() != inputs.entry_date
                or inputs.lapsed_at_utc < entry_open
            ):
                raise TrialRuntimeInvalid("entry_lapse_before_official_open")
            resolutions = [
                TrialResolution(
                    candidate_id=candidate.candidate_id,
                    entry_date=inputs.entry_date,
                    enrollment_state="missed",
                    reason=f"entry_date_completion_lapsed:{inputs.reason}",
                    confirmatory_enrollment_sequence=None,
                    resolved_at_utc=inputs.lapsed_at_utc,
                )
                for candidate in candidates
            ]
            candidate_records = [
                {
                    "candidate_id": str(row["candidate_id"]),
                    "record_sha256": str(row["record_sha256"]),
                }
                for row in candidate_rows
            ]
            resolution_digests = [
                self._insert_resolution(conn, resolution) for resolution in resolutions
            ]
            lapse_record: dict[str, Any] = {
                "contract_version": TRIAL_CONTRACT_VERSION,
                "entry_date": inputs.entry_date.isoformat(),
                "lapsed_at_utc": _utc_text(inputs.lapsed_at_utc),
                "reason": inputs.reason,
                "entry_opens_at_utc": _utc_text(entry_open),
                "schedule_observation_watermark": inputs.schedule_observation_watermark,
                "schedule_record_sha256s": list(schedule_digests),
                "candidate_records": candidate_records,
                "candidate_set_sha256": _sha256(_canonical({"candidates": candidate_records})),
                "resolution_record_sha256s": resolution_digests,
                "resolution_set_sha256": _sha256(
                    _canonical({"resolution_record_sha256s": resolution_digests})
                ),
            }
            encoded = _canonical(lapse_record)
            digest = _sha256(encoded)
            sequence = int(
                conn.execute(
                    "SELECT COALESCE(MAX(sequence),0)+1 FROM trial_entry_date_lapses"
                ).fetchone()[0]
            )
            conn.execute(
                "INSERT INTO trial_entry_date_lapses VALUES(?,?,?,?,?,?)",
                (
                    sequence,
                    inputs.entry_date.isoformat(),
                    _utc_text(inputs.lapsed_at_utc),
                    inputs.reason,
                    digest,
                    encoded,
                ),
            )
        return digest

    def disposition_for_evidence(self, evidence_record_sha256: str) -> str | None:
        with contextlib.closing(self._connect()) as conn:
            row = conn.execute(
                """
                SELECT state FROM trial_evidence_dispositions
                WHERE evidence_record_sha256=?
                """,
                (evidence_record_sha256,),
            ).fetchone()
        return None if row is None else str(row["state"])

    def disposition_counts(self) -> dict[str, int]:
        with contextlib.closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT state,COUNT(*) AS count FROM trial_evidence_dispositions GROUP BY state"
            ).fetchall()
        return {str(row["state"]): int(row["count"]) for row in rows}

    def fault_count(self) -> int:
        with contextlib.closing(self._connect()) as conn:
            return int(conn.execute("SELECT COUNT(*) FROM trial_faults").fetchone()[0])

    def append_evidence_disposition(
        self,
        *,
        snapshot_id: str,
        evidence_record_sha256: str,
        state: Literal["excluded", "invalid"],
        reason: str,
        now: datetime,
    ) -> bool:
        record: dict[str, Any] = {
            "contract_version": TRIAL_CONTRACT_VERSION,
            "evidence_snapshot_id": snapshot_id,
            "evidence_record_sha256": evidence_record_sha256,
            "state": state,
            "reason": reason,
            "recorded_at_utc": _utc_text(now),
        }
        encoded = _canonical(record)
        digest = _sha256(encoded)
        with contextlib.closing(self._connect()) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            if conn.execute(
                "SELECT 1 FROM trial_candidates WHERE evidence_record_sha256=?",
                (evidence_record_sha256,),
            ).fetchone():
                return False
            existing = conn.execute(
                """
                SELECT state,reason FROM trial_evidence_dispositions
                WHERE evidence_record_sha256=?
                """,
                (evidence_record_sha256,),
            ).fetchone()
            if existing is not None:
                if str(existing["state"]) != state or str(existing["reason"]) != reason:
                    raise TrialRuntimeInvalid("evidence_disposition_conflict")
                return False
            sequence = int(
                conn.execute(
                    "SELECT COALESCE(MAX(sequence),0)+1 FROM trial_evidence_dispositions"
                ).fetchone()[0]
            )
            conn.execute(
                """
                INSERT INTO trial_evidence_dispositions(
                  sequence,disposition_id,evidence_snapshot_id,evidence_record_sha256,
                  state,reason,recorded_at_utc,record_sha256,record_json
                ) VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (
                    sequence,
                    str(uuid.uuid5(uuid.NAMESPACE_URL, evidence_record_sha256)),
                    snapshot_id,
                    evidence_record_sha256,
                    state,
                    reason,
                    _utc_text(now),
                    digest,
                    encoded,
                ),
            )
        return True

    @staticmethod
    def _verify_disposition_row(row: sqlite3.Row) -> None:
        raw = bytes(row["record_json"])
        if _sha256(raw) != str(row["record_sha256"]):
            raise TrialRuntimeInvalid("evidence_disposition_digest_mismatch")
        record = json.loads(raw)
        if not isinstance(record, dict) or _canonical(record) != raw:
            raise TrialRuntimeInvalid("evidence_disposition_not_canonical")
        if (
            record.get("contract_version") != TRIAL_CONTRACT_VERSION
            or record.get("evidence_snapshot_id") != row["evidence_snapshot_id"]
            or record.get("evidence_record_sha256") != row["evidence_record_sha256"]
            or record.get("state") != row["state"]
            or record.get("reason") != row["reason"]
            or record.get("recorded_at_utc") != row["recorded_at_utc"]
        ):
            raise TrialRuntimeInvalid("evidence_disposition_columns_mismatch")

    def record_fault(self, *, now: datetime, kind: str, detail: str) -> None:
        record: dict[str, Any] = {
            "contract_version": TRIAL_CONTRACT_VERSION,
            "occurred_at_utc": _utc_text(now),
            "kind": kind,
            "detail": detail[:2000],
        }
        encoded = _canonical(record)
        digest = _sha256(encoded)
        with contextlib.closing(self._connect()) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            latest = conn.execute(
                "SELECT kind,detail FROM trial_faults ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
            if latest is not None and (
                str(latest["kind"]) == kind and str(latest["detail"]) == detail[:2000]
            ):
                return
            sequence = int(
                conn.execute("SELECT COALESCE(MAX(sequence),0)+1 FROM trial_faults").fetchone()[0]
            )
            conn.execute(
                "INSERT INTO trial_faults VALUES(?,?,?,?,?,?,?)",
                (
                    sequence,
                    str(uuid.uuid5(uuid.NAMESPACE_URL, digest)),
                    _utc_text(now),
                    kind,
                    detail[:2000],
                    digest,
                    encoded,
                ),
            )

    @staticmethod
    def _verify_fault_row(row: sqlite3.Row) -> None:
        raw = bytes(row["record_json"])
        if _sha256(raw) != str(row["record_sha256"]):
            raise TrialRuntimeInvalid("trial_fault_digest_mismatch")
        record = json.loads(raw)
        if not isinstance(record, dict) or _canonical(record) != raw:
            raise TrialRuntimeInvalid("trial_fault_not_canonical")
        if (
            record.get("contract_version") != TRIAL_CONTRACT_VERSION
            or record.get("occurred_at_utc") != row["occurred_at_utc"]
            or record.get("kind") != row["kind"]
            or record.get("detail") != row["detail"]
        ):
            raise TrialRuntimeInvalid("trial_fault_columns_mismatch")

    def write_health(
        self,
        *,
        now: datetime,
        result: str,
        error: str | None,
        evidence_seen: int,
        unresolved_evidence: int,
    ) -> None:
        with contextlib.closing(self._connect()) as conn, conn:
            candidates_total = int(
                conn.execute("SELECT COUNT(*) FROM trial_candidates").fetchone()[0]
            )
            conn.execute(
                """
                INSERT INTO trial_health VALUES(1,?,?,?,?,?,?)
                ON CONFLICT(singleton) DO UPDATE SET
                  last_worker_heartbeat_utc=excluded.last_worker_heartbeat_utc,
                  last_result=excluded.last_result,last_error=excluded.last_error,
                  evidence_seen=excluded.evidence_seen,candidates_total=excluded.candidates_total,
                  unresolved_evidence=excluded.unresolved_evidence
                """,
                (
                    _utc_text(now),
                    result,
                    error,
                    evidence_seen,
                    candidates_total,
                    unresolved_evidence,
                ),
            )

    def validate_integrity(self, *, include_outcomes: bool = True) -> None:
        with contextlib.closing(self._connect()) as conn:
            rows = conn.execute("SELECT * FROM trial_candidates ORDER BY sequence").fetchall()
            candidate_count = len(rows)
            if [int(row["sequence"]) for row in rows] != list(range(1, candidate_count + 1)):
                raise TrialRuntimeInvalid("candidate_sequence_not_gap_free")
            for row in rows:
                self._verify_candidate_row(row)
            tables = [
                "trial_evidence_dispositions",
                "trial_resolutions",
                "trial_entry_date_completions",
                "trial_entry_date_lapses",
                "trial_faults",
            ]
            if include_outcomes:
                tables.append("trial_outcomes")
            for table in tables:
                sequences = [
                    int(row[0])
                    for row in conn.execute(f"SELECT sequence FROM {table} ORDER BY sequence")
                ]
                if sequences != list(range(1, len(sequences) + 1)):
                    raise TrialRuntimeInvalid(f"{table}_sequence_not_gap_free")
            disposition_rows = conn.execute(
                "SELECT * FROM trial_evidence_dispositions ORDER BY sequence"
            ).fetchall()
            for row in disposition_rows:
                self._verify_disposition_row(row)
            candidate_evidence = {str(row["evidence_record_sha256"]) for row in rows}
            disposition_evidence = {str(row["evidence_record_sha256"]) for row in disposition_rows}
            if candidate_evidence & disposition_evidence:
                raise TrialRuntimeInvalid("evidence_has_candidate_and_disposition")
            candidate_snapshots = {str(row["evidence_snapshot_id"]) for row in rows}
            disposition_snapshots = {str(row["evidence_snapshot_id"]) for row in disposition_rows}
            if candidate_snapshots & disposition_snapshots:
                raise TrialRuntimeInvalid("snapshot_has_candidate_and_disposition")
            resolution_rows = conn.execute(
                "SELECT * FROM trial_resolutions ORDER BY sequence"
            ).fetchall()
            resolutions = [self._verify_resolution_row(row) for row in resolution_rows]
            enrollment_sequences = [
                resolution.confirmatory_enrollment_sequence
                for resolution in resolutions
                if resolution.enrollment_state == "enrolled"
            ]
            if enrollment_sequences != list(range(1, len(enrollment_sequences) + 1)):
                raise TrialRuntimeInvalid("confirmatory_enrollment_sequence_not_gap_free")
            candidates_by_id = {str(row["candidate_id"]): row for row in rows}
            for resolution in resolutions:
                candidate_row = candidates_by_id.get(resolution.candidate_id)
                if candidate_row is None:
                    raise TrialRuntimeInvalid("resolution_candidate_missing")
                if str(candidate_row["planned_entry_date"]) != resolution.entry_date.isoformat():
                    raise TrialRuntimeInvalid("resolution_candidate_entry_date_mismatch")
            completion_rows = conn.execute(
                "SELECT * FROM trial_entry_date_completions ORDER BY sequence"
            ).fetchall()
            completion_dates: list[date] = []
            bound_resolution_sha256s: set[str] = set()
            resolution_by_sha256 = {
                str(row["record_sha256"]): resolution
                for row, resolution in zip(resolution_rows, resolutions, strict=True)
            }
            for completion_row in completion_rows:
                completion = self._verify_completion_row(completion_row)
                entry_date = date.fromisoformat(str(completion["entry_date"]))
                completed_at = _parse_utc(str(completion["completed_at_utc"]))
                completion_dates.append(entry_date)
                completion_candidate_ids: list[str] = []
                for candidate_binding in completion["candidate_records"]:
                    candidate_id = str(candidate_binding["candidate_id"])
                    completion_candidate_ids.append(candidate_id)
                    candidate_row = candidates_by_id.get(candidate_id)
                    if (
                        candidate_row is None
                        or str(candidate_row["planned_entry_date"]) != entry_date.isoformat()
                        or str(candidate_row["record_sha256"])
                        != str(candidate_binding["record_sha256"])
                        or _parse_utc(str(candidate_row["imported_at_utc"])) > completed_at
                    ):
                        raise TrialRuntimeInvalid("completion_candidate_binding_mismatch")
                try:
                    bound_resolutions = [
                        resolution_by_sha256[str(digest)]
                        for digest in completion["resolution_record_sha256s"]
                    ]
                except KeyError as exc:
                    raise TrialRuntimeInvalid("completion_resolution_binding_missing") from exc
                bound_resolution_sha256s.update(
                    str(digest) for digest in completion["resolution_record_sha256s"]
                )
                if [
                    resolution.candidate_id for resolution in bound_resolutions
                ] != completion_candidate_ids or any(
                    resolution.entry_date != entry_date
                    or resolution.resolved_at_utc != completed_at
                    for resolution in bound_resolutions
                ):
                    raise TrialRuntimeInvalid("completion_resolution_binding_mismatch")
                cutoff = datetime.combine(entry_date, SIGNAL_CUTOFF, tzinfo=NEW_YORK).astimezone(
                    UTC
                )
                decision_clock_at = _parse_utc(str(completion["decision_clock_at_utc"]))
                entry_open = _parse_utc(str(completion["entry_opens_at_utc"]))
                if not cutoff <= completed_at <= decision_clock_at < entry_open:
                    raise TrialRuntimeInvalid("entry_completion_outside_pre_open_window")
                _validate_cutoff_resolution_states(
                    [
                        self._verify_candidate_row(candidates_by_id[candidate_id])
                        for candidate_id in completion_candidate_ids
                    ],
                    bound_resolutions,
                    cutoff_utc=cutoff,
                )
            if completion_dates != sorted(set(completion_dates)):
                raise TrialRuntimeInvalid("entry_completion_dates_not_strictly_ordered")
            lapse_rows = conn.execute(
                "SELECT * FROM trial_entry_date_lapses ORDER BY sequence"
            ).fetchall()
            for lapse_row in lapse_rows:
                lapse = self._verify_lapse_row(lapse_row)
                entry_date = date.fromisoformat(str(lapse["entry_date"]))
                lapsed_at = _parse_utc(str(lapse["lapsed_at_utc"]))
                lapse_candidate_ids: list[str] = []
                for candidate_binding in lapse["candidate_records"]:
                    candidate_id = str(candidate_binding["candidate_id"])
                    lapse_candidate_ids.append(candidate_id)
                    candidate_row = candidates_by_id.get(candidate_id)
                    if (
                        candidate_row is None
                        or str(candidate_row["planned_entry_date"]) != entry_date.isoformat()
                        or str(candidate_row["record_sha256"])
                        != str(candidate_binding["record_sha256"])
                        or _parse_utc(str(candidate_row["imported_at_utc"])) > lapsed_at
                    ):
                        raise TrialRuntimeInvalid("lapse_candidate_binding_mismatch")
                try:
                    lapse_resolutions = [
                        resolution_by_sha256[str(digest)]
                        for digest in lapse["resolution_record_sha256s"]
                    ]
                except KeyError as exc:
                    raise TrialRuntimeInvalid("lapse_resolution_binding_missing") from exc
                bound_resolution_sha256s.update(
                    str(digest) for digest in lapse["resolution_record_sha256s"]
                )
                if [
                    resolution.candidate_id for resolution in lapse_resolutions
                ] != lapse_candidate_ids or any(
                    resolution.entry_date != entry_date
                    or resolution.resolved_at_utc != lapsed_at
                    or resolution.enrollment_state != "missed"
                    or resolution.reason != f"entry_date_completion_lapsed:{lapse['reason']}"
                    for resolution in lapse_resolutions
                ):
                    raise TrialRuntimeInvalid("lapse_resolution_binding_mismatch")
                if lapsed_at < _parse_utc(str(lapse["entry_opens_at_utc"])):
                    raise TrialRuntimeInvalid("entry_lapse_before_official_open")
            sealed_rows = conn.execute(
                """
                SELECT kind,entry_date,sealed_at_utc FROM (
                  SELECT 'completion' kind,entry_date,decision_clock_at_utc sealed_at_utc
                  FROM trial_entry_date_completions
                  UNION ALL
                  SELECT 'lapse' kind,entry_date,lapsed_at_utc sealed_at_utc
                  FROM trial_entry_date_lapses
                ) ORDER BY entry_date
                """
            ).fetchall()
            sealed_dates = [date.fromisoformat(str(row["entry_date"])) for row in sealed_rows]
            sealed_times = [_parse_utc(str(row["sealed_at_utc"])) for row in sealed_rows]
            if sealed_dates != sorted(set(sealed_dates)):
                raise TrialRuntimeInvalid("sealed_entry_dates_not_strictly_ordered")
            if sealed_times != sorted(sealed_times):
                raise TrialRuntimeInvalid("entry_seal_time_moved_backwards")
            for resolution_row, resolution in zip(
                resolution_rows,
                resolutions,
                strict=True,
            ):
                if str(resolution_row["record_sha256"]) in bound_resolution_sha256s:
                    continue
                candidate_row = candidates_by_id[resolution.candidate_id]
                candidate_date = date.fromisoformat(str(candidate_row["planned_entry_date"]))
                covering_seal = next(
                    (
                        row
                        for row in sealed_rows
                        if date.fromisoformat(str(row["entry_date"])) >= candidate_date
                    ),
                    None,
                )
                if covering_seal is None:
                    raise TrialRuntimeInvalid("unbound_resolution_has_no_covering_entry_seal")
                covering_date = date.fromisoformat(str(covering_seal["entry_date"]))
                covering_at = _parse_utc(str(covering_seal["sealed_at_utc"]))
                imported_at = _parse_utc(str(candidate_row["imported_at_utc"]))
                expected_reason = (
                    f"candidate_arrived_after_entry_date_{covering_seal['kind']}"
                    if covering_date == candidate_date
                    else "candidate_arrived_behind_entry_date_cursor"
                )
                if (
                    resolution.enrollment_state != "missed"
                    or resolution.confirmatory_enrollment_sequence is not None
                    or resolution.reason != expected_reason
                    or imported_at < covering_at
                    or resolution.resolved_at_utc != imported_at
                ):
                    raise TrialRuntimeInvalid("unbound_resolution_not_late_arrival_missed")
            if sealed_dates:
                unresolved_behind_cursor = conn.execute(
                    """
                    SELECT 1 FROM trial_candidates candidate
                    WHERE candidate.planned_entry_date<=? AND NOT EXISTS(
                      SELECT 1 FROM trial_resolutions resolution
                      WHERE resolution.candidate_id=candidate.candidate_id
                    ) LIMIT 1
                    """,
                    (max(sealed_dates).isoformat(),),
                ).fetchone()
                if unresolved_behind_cursor is not None:
                    raise TrialRuntimeInvalid("candidate_unresolved_behind_entry_date_cursor")
            fault_rows = conn.execute("SELECT * FROM trial_faults ORDER BY sequence").fetchall()
            for row in fault_rows:
                self._verify_fault_row(row)

    def status(self) -> dict[str, Any]:
        with contextlib.closing(self._connect()) as conn:
            counts = {
                "candidates": int(
                    conn.execute("SELECT COUNT(*) FROM trial_candidates").fetchone()[0]
                ),
                "evidence_dispositions": int(
                    conn.execute("SELECT COUNT(*) FROM trial_evidence_dispositions").fetchone()[0]
                ),
                "resolutions": int(
                    conn.execute("SELECT COUNT(*) FROM trial_resolutions").fetchone()[0]
                ),
                "entry_date_completions": int(
                    conn.execute("SELECT COUNT(*) FROM trial_entry_date_completions").fetchone()[0]
                ),
                "entry_date_lapses": int(
                    conn.execute("SELECT COUNT(*) FROM trial_entry_date_lapses").fetchone()[0]
                ),
                "outcomes": int(conn.execute("SELECT COUNT(*) FROM trial_outcomes").fetchone()[0]),
                "faults": int(conn.execute("SELECT COUNT(*) FROM trial_faults").fetchone()[0]),
            }
            health = conn.execute("SELECT * FROM trial_health WHERE singleton=1").fetchone()
        try:
            self.validate_integrity()
        except (
            KeyError,
            TypeError,
            ValueError,
            TrialRuntimeInvalid,
            json.JSONDecodeError,
            sqlite3.DatabaseError,
        ):
            integrity = "invalid"
        else:
            integrity = "valid"
        return {
            **counts,
            "integrity_status": integrity,
            "health": dict(health) if health is not None else None,
        }


def _verified_evidence(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        raise TrialRuntimeInvalid("active_evidence_store_missing")
    try:
        conn = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True, timeout=30)
    except sqlite3.OperationalError:
        raise
    except sqlite3.DatabaseError as exc:
        raise TrialRuntimeInvalid("evidence_store_unreadable") from exc
    conn.row_factory = sqlite3.Row
    with contextlib.closing(conn):
        rows = conn.execute("SELECT * FROM evidence_snapshots ORDER BY sequence").fetchall()
    output: list[dict[str, Any]] = []
    seen_snapshot_ids: set[str] = set()
    for expected_sequence, row in enumerate(rows, start=1):
        if int(row["sequence"]) != expected_sequence:
            raise TrialRuntimeInvalid("evidence_sequence_not_gap_free")
        raw = bytes(row["record_json"])
        if _sha256(raw) != str(row["stored_bytes_sha256"]):
            raise TrialRuntimeInvalid("evidence_stored_bytes_digest_mismatch")
        try:
            record = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise TrialRuntimeInvalid("evidence_record_json_invalid") from exc
        if not isinstance(record, dict) or _canonical(record) != raw:
            raise TrialRuntimeInvalid("evidence_record_not_canonical")
        unsigned = dict(record)
        record_sha = str(unsigned.pop("record_sha256", ""))
        if _sha256(_canonical(unsigned)) != record_sha or record_sha != str(row["record_sha256"]):
            raise TrialRuntimeInvalid("evidence_record_digest_mismatch")
        if str(record.get("snapshot_id")) != str(row["snapshot_id"]):
            raise TrialRuntimeInvalid("evidence_snapshot_identity_mismatch")
        snapshot_id = str(record["snapshot_id"])
        if snapshot_id in seen_snapshot_ids:
            raise TrialRuntimeInvalid("evidence_snapshot_id_duplicated")
        seen_snapshot_ids.add(snapshot_id)
        output.append(record)
    return output


def _candidate_from_evidence(
    record: dict[str, Any],
    *,
    now: datetime,
    window: TrialWindow,
    session_store: SessionFeedStore,
) -> TrialCandidate:
    if window.status != "active" or window.activated_at_utc is None:
        raise TrialRuntimeInvalid("candidate_import_requires_active_registry")
    payload = record.get("payload")
    if not isinstance(payload, dict):
        raise TrialRuntimeInvalid("evidence_payload_missing")
    signal = payload.get("signal")
    timing = payload.get("timing")
    versions = payload.get("versions")
    classification = payload.get("classification")
    if not all(isinstance(value, dict) for value in (signal, timing, versions, classification)):
        raise TrialRuntimeInvalid("evidence_candidate_fields_missing")
    assert isinstance(signal, dict)
    assert isinstance(timing, dict)
    assert isinstance(versions, dict)
    assert isinstance(classification, dict)
    if (
        record.get("hypothesis_id") != HYPOTHESIS_ID
        or record.get("enrollment_state") != "pending_entry_selection"
        or record.get("confirmatory_enrollment_sequence") is not None
    ):
        raise TrialRuntimeInvalid("evidence_not_pending_trial_candidate")
    observed_at = _parse_utc(str(timing.get("source_first_observed_at_utc", "")))
    deadline = window.enrollment_deadline_utc
    if deadline is None or not window.activated_at_utc <= observed_at < deadline:
        raise EvidenceExcluded("evidence_outside_activation_window")
    if versions.get("policy_sha256") != window.registry_sha256:
        raise TrialRuntimeInvalid("evidence_registry_digest_mismatch")
    recorded_at = _parse_utc(str(record.get("recorded_at_utc", "")))
    if recorded_at < observed_at:
        raise TrialRuntimeInvalid("evidence_import_clock_order_invalid")
    if now < recorded_at:
        raise EvidenceNotReady("evidence_recorded_after_runtime_clock")
    symbol = _normalized_symbol(str(signal.get("issuer_symbol", "")))
    packet_id = str(signal.get("packet_id", ""))
    accession = str(signal.get("accession_number", ""))
    snapshot_id = str(record.get("snapshot_id", ""))
    record_sha = str(record.get("record_sha256", ""))
    if not all((symbol, packet_id, accession, snapshot_id, record_sha)):
        raise TrialRuntimeInvalid("evidence_candidate_identity_missing")
    classification_state = classification.get("state")
    owner_mapping = classification.get("transaction_owner_mapping")
    history_complete = classification.get("history_coverage_complete")
    if classification_state not in {
        "routine",
        "opportunistic",
        "unpartitionable",
        "ambiguous_multi_owner",
    }:
        raise TrialRuntimeInvalid("evidence_classification_state_invalid")
    if owner_mapping not in {"exact", "ambiguous", "missing"}:
        raise TrialRuntimeInvalid("evidence_owner_mapping_invalid")
    if not isinstance(history_complete, bool):
        raise TrialRuntimeInvalid("evidence_history_coverage_not_boolean")
    if classification_state != "opportunistic":
        raise EvidenceExcluded(f"classification_state_excluded:{classification_state}")
    if owner_mapping != "exact":
        raise EvidenceExcluded(f"transaction_owner_mapping_excluded:{owner_mapping}")
    if history_complete is not True:
        raise EvidenceExcluded("classification_history_coverage_incomplete")
    schedule = session_store.schedule_as_known_at(observed_at)
    entry, final_date = planned_entry_session(observed_at, schedule)
    return TrialCandidate(
        candidate_id=str(
            uuid.uuid5(uuid.NAMESPACE_URL, f"{HYPOTHESIS_ID}|candidate|{snapshot_id}")
        ),
        evidence_snapshot_id=snapshot_id,
        evidence_record_sha256=record_sha,
        packet_id=packet_id,
        accession_number=accession,
        symbol=symbol,
        source_first_observed_at_utc=observed_at,
        evidence_recorded_at_utc=recorded_at,
        classification_state=classification_state,
        transaction_owner_mapping=owner_mapping,
        history_coverage_complete=history_complete,
        planned_entry_date=entry.session_date,
        entry_opens_at_utc=entry.opens_at_utc,
        final_session_date=final_date,
        entry_rank_sha256=_entry_rank(
            entry_date=entry.session_date,
            packet_id=packet_id,
            accession_number=accession,
            symbol=symbol,
        ),
        imported_at_utc=now,
    )


def _ensure_bar_requests(
    candidate: TrialCandidate,
    bar_store: BarFeedStore,
) -> int:
    added = 0
    for symbol in (candidate.symbol, "SPY"):
        request = BarRequest(
            request_id=f"{candidate.candidate_id}|{symbol}|daily-v1",
            symbol=symbol,
            start_date=candidate.source_first_observed_at_utc.astimezone(NEW_YORK).date()
            - timedelta(days=BAR_LOOKBACK_CALENDAR_DAYS),
            through_date=candidate.final_session_date,
            requested_at_utc=candidate.imported_at_utc,
            requester=BAR_REQUESTER,
        )
        bar_store.request(request)
        added += 1
    return added


def run_trial_once(
    config: TrialRuntimeConfig,
    *,
    now: datetime | None = None,
) -> TrialRuntimeResult:
    now = (now or datetime.now(UTC)).astimezone(UTC)
    store = TrialStore(config.trial_db)
    try:
        window = _validated_trial_window(config)
        if window.status == "draft":
            store.write_health(
                now=now,
                result="idle_registry_draft",
                error=None,
                evidence_seen=0,
                unresolved_evidence=0,
            )
            return TrialRuntimeResult("idle")
        evidence = _verified_evidence(config.evidence_db)
        session_store = SessionFeedStore(config.session_feed_db, initialize=False)
        session_store.validate_integrity()
        bar_store = BarFeedStore(config.bar_feed_db)
        candidates_added = requests_ensured = 0
        for record in evidence:
            record_sha = str(record.get("record_sha256", ""))
            snapshot_id = str(record.get("snapshot_id", ""))
            if not record_sha or not snapshot_id:
                raise TrialRuntimeInvalid("evidence_disposition_identity_missing")
            candidate = store.candidate_for_evidence(record_sha)
            if candidate is not None:
                requests_ensured += _ensure_bar_requests(candidate, bar_store)
                continue
            if store.disposition_for_evidence(record_sha) is not None:
                continue
            try:
                candidate = _candidate_from_evidence(
                    record,
                    now=now,
                    window=window,
                    session_store=session_store,
                )
                candidates_added += int(store.append_candidate(candidate))
                persisted_candidate = store.candidate_for_evidence(record_sha)
                if persisted_candidate is None:
                    continue
                candidate = persisted_candidate
            except EvidenceNotReady:
                continue
            except EvidenceExcluded as exc:
                store.append_evidence_disposition(
                    snapshot_id=snapshot_id,
                    evidence_record_sha256=record_sha,
                    state="excluded",
                    reason=str(exc),
                    now=now,
                )
            except (TrialRuntimeInvalid, ValueError, sqlite3.IntegrityError) as exc:
                store.append_evidence_disposition(
                    snapshot_id=snapshot_id,
                    evidence_record_sha256=record_sha,
                    state="invalid",
                    reason=f"{type(exc).__name__}: {exc}"[:2000],
                    now=now,
                )
            else:
                requests_ensured += _ensure_bar_requests(candidate, bar_store)
        store.validate_integrity()
        disposition_counts = store.disposition_counts()
        unresolved = len(evidence) - len(store.candidates()) - sum(disposition_counts.values())
        invalid_count = disposition_counts.get("invalid", 0)
        fault_count = store.fault_count()
        invalid_total = invalid_count + fault_count
        runtime_status: Literal["collecting", "invalid"] = (
            "invalid" if invalid_total else "collecting"
        )
        invalid_error = (
            f"invalid_evidence_dispositions={invalid_count};faults={fault_count}"
            if invalid_total
            else None
        )
        result = TrialRuntimeResult(
            runtime_status,
            evidence_seen=len(evidence),
            candidates_added=candidates_added,
            bar_requests_ensured=requests_ensured,
            unresolved_evidence=unresolved,
            error=invalid_error,
        )
        store.write_health(
            now=now,
            result=runtime_status,
            error=invalid_error,
            evidence_seen=len(evidence),
            unresolved_evidence=unresolved,
        )
        return result
    except (TrialRuntimeRetryable, sqlite3.OperationalError, OSError) as exc:
        error = f"{type(exc).__name__}: {exc}"[:2000]
        store.write_health(
            now=now,
            result="degraded",
            error=error,
            evidence_seen=0,
            unresolved_evidence=0,
        )
        return TrialRuntimeResult("degraded", error=error)
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"[:2000]
        store.record_fault(now=now, kind="TRIAL_RUNTIME_INVALID", detail=error)
        store.write_health(
            now=now,
            result="invalid",
            error=error,
            evidence_seen=0,
            unresolved_evidence=0,
        )
        return TrialRuntimeResult("invalid", error=error)


def trial_runtime_status(path: Path | str) -> dict[str, Any]:
    selected = Path(path)
    if not selected.is_file():
        return {"exists": False, "path": str(selected), "integrity_status": "missing"}
    try:
        status = TrialStore(selected, initialize=False).status()
    except sqlite3.DatabaseError:
        status = {"integrity_status": "invalid"}
    return {"exists": True, "path": str(selected), **status}


def result_json(result: TrialRuntimeResult) -> dict[str, object]:
    return asdict(result)
