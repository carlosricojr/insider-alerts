"""Order-incapable prospective runtime for the OPP-E07-V1 shadow trial."""

from __future__ import annotations

import contextlib
import hashlib
import json
import sqlite3
import uuid
from collections.abc import Mapping, Sequence
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


class TrialRuntimeInvalid(RuntimeError):
    """A fail-closed violation that can invalidate prospective trial operation."""


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


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical(value: dict[str, Any]) -> bytes:
    return rfc8785.dumps(value)


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
    entry_opens = {candidate.entry_opens_at_utc for candidate in candidates}
    if len(entry_dates) != 1 or len(entry_opens) != 1:
        raise TrialRuntimeInvalid("entry_completion_candidate_session_mismatch")
    entry_date = next(iter(entry_dates))
    entry_open = next(iter(entry_opens))
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


class TrialStore:
    """Append-only candidate universe plus mutable operational health."""

    def __init__(self, path: Path | str, *, initialize: bool = True) -> None:
        self.path = Path(path)
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

    def validate_integrity(self) -> None:
        with contextlib.closing(self._connect()) as conn:
            rows = conn.execute("SELECT * FROM trial_candidates ORDER BY sequence").fetchall()
            candidate_count = len(rows)
            if [int(row["sequence"]) for row in rows] != list(range(1, candidate_count + 1)):
                raise TrialRuntimeInvalid("candidate_sequence_not_gap_free")
            for row in rows:
                self._verify_candidate_row(row)
            for table in (
                "trial_evidence_dispositions",
                "trial_resolutions",
                "trial_entry_date_completions",
                "trial_outcomes",
                "trial_faults",
            ):
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
    schedule = session_store.schedule_as_known_at(observed_at)
    entry, final_date = planned_entry_session(observed_at, schedule)
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
    except (sqlite3.OperationalError, OSError) as exc:
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
