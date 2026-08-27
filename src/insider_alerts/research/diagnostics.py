"""Prospective, order-incapable diagnostic bindings for OPP-E07-V1."""

from __future__ import annotations

import contextlib
import hashlib
import json
import math
import sqlite3
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Literal
from zoneinfo import ZoneInfo

import rfc8785

from insider_alerts.research.bar_feed import BarFeedStore, BarRequest
from insider_alerts.research.inference import HYPOTHESIS_ID
from insider_alerts.research.session_feed import SessionFeedStore
from insider_alerts.research.trial_runtime import (
    BAR_LOOKBACK_CALENDAR_DAYS,
    MAX_SESSIONS,
    TrialRuntimeConfig,
    TrialRuntimeInvalid,
    _validated_trial_window,
)

DIAGNOSTIC_CONTRACT_VERSION = "opp-e07-prospective-diagnostics-v1"
DIAGNOSTIC_BAR_REQUESTER = "OPP-E07-V1-diagnostic-completed-bar-input-v1"
NEW_YORK = ZoneInfo("America/New_York")
SIGNAL_CUTOFF = time(9, 20)
FINAL_SHADOW_STATES = frozenset({"rejected", "overlap_suppressed", "capacity_suppressed", "closed"})
HEARTBEAT_STALE_AFTER = timedelta(minutes=3)


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("diagnostic timestamp cannot be naive")
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("persisted diagnostic timestamp is naive")
    return parsed.astimezone(UTC)


def _canonical(value: dict[str, Any]) -> bytes:
    return rfc8785.dumps(value)


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _record_id(kind: str, digest: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{HYPOTHESIS_ID}|diagnostic|{kind}|{digest}"))


def _diagnostic_trade_id(packet_id: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{HYPOTHESIS_ID}|diagnostic-trade|{packet_id}"))


@dataclass(frozen=True, slots=True)
class DiagnosticConfig:
    diagnostics_db: Path
    canary_ledger_db: Path
    source_db: Path
    evidence_db: Path
    bar_feed_db: Path
    session_feed_db: Path
    registry_path: Path


@dataclass(frozen=True, slots=True)
class DiagnosticRunResult:
    status: Literal["idle_registry_draft", "collecting", "degraded"]
    candidates_seen: int = 0
    candidates_added: int = 0
    evidence_bindings_added: int = 0
    state_bindings_added: int = 0
    bar_requests_ensured: int = 0
    reconciliations_added: int = 0
    unresolved_candidates: int = 0
    error: str | None = None


class DiagnosticStore:
    """Append-only diagnostic provenance plus mutable operational health."""

    def __init__(self, path: Path | str, *, initialize: bool = True) -> None:
        self.path = Path(path)
        if initialize:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._initialize()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA synchronous=FULL")
        return conn

    def _initialize(self) -> None:
        with contextlib.closing(self._connect()) as conn, conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=FULL;
                CREATE TABLE IF NOT EXISTS diagnostic_candidates (
                    sequence INTEGER NOT NULL UNIQUE,
                    candidate_id TEXT PRIMARY KEY,
                    packet_id TEXT NOT NULL UNIQUE,
                    signal_at_utc TEXT NOT NULL,
                    source_first_observed_at_utc TEXT NOT NULL,
                    entry_session TEXT,
                    final_session TEXT,
                    canary_selection_sha256 TEXT NOT NULL,
                    record_sha256 TEXT NOT NULL UNIQUE,
                    record_json BLOB NOT NULL,
                    recorded_at_utc TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS diagnostic_evidence_bindings (
                    sequence INTEGER NOT NULL UNIQUE,
                    binding_id TEXT PRIMARY KEY,
                    packet_id TEXT NOT NULL UNIQUE,
                    evidence_record_sha256 TEXT NOT NULL UNIQUE,
                    routine_eligible INTEGER NOT NULL,
                    routine_reason TEXT NOT NULL,
                    record_sha256 TEXT NOT NULL UNIQUE,
                    record_json BLOB NOT NULL,
                    recorded_at_utc TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS diagnostic_state_bindings (
                    sequence INTEGER NOT NULL UNIQUE,
                    binding_id TEXT PRIMARY KEY,
                    packet_id TEXT NOT NULL UNIQUE,
                    shadow_state TEXT NOT NULL,
                    canary_state_sha256 TEXT NOT NULL,
                    shadow_trade_sha256 TEXT,
                    record_sha256 TEXT NOT NULL UNIQUE,
                    record_json BLOB NOT NULL,
                    recorded_at_utc TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS diagnostic_reconciliations (
                    sequence INTEGER NOT NULL UNIQUE,
                    reconciliation_id TEXT PRIMARY KEY,
                    packet_id TEXT,
                    category TEXT NOT NULL,
                    record_sha256 TEXT NOT NULL UNIQUE,
                    record_json BLOB NOT NULL,
                    recorded_at_utc TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS diagnostic_outcomes (
                    sequence INTEGER NOT NULL UNIQUE,
                    outcome_id TEXT PRIMARY KEY,
                    candidate_id TEXT NOT NULL UNIQUE,
                    packet_id TEXT NOT NULL UNIQUE,
                    trade_id TEXT NOT NULL UNIQUE,
                    record_sha256 TEXT NOT NULL UNIQUE,
                    record_json BLOB NOT NULL,
                    recorded_at_utc TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS diagnostic_outcome_receipts (
                    sequence INTEGER NOT NULL UNIQUE,
                    receipt_id TEXT PRIMARY KEY,
                    candidate_id TEXT NOT NULL UNIQUE,
                    packet_id TEXT NOT NULL UNIQUE,
                    disposition TEXT NOT NULL CHECK(
                      disposition IN ('available','not_traded','unavailable')
                    ),
                    reason TEXT NOT NULL,
                    outcome_record_sha256 TEXT UNIQUE,
                    record_sha256 TEXT NOT NULL UNIQUE,
                    record_json BLOB NOT NULL,
                    recorded_at_utc TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS diagnostic_health (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    last_worker_heartbeat_utc TEXT NOT NULL,
                    last_result TEXT NOT NULL,
                    last_error TEXT,
                    candidates_seen INTEGER NOT NULL,
                    unresolved_candidates INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS diagnostic_outcome_health (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    last_worker_heartbeat_utc TEXT NOT NULL,
                    last_result TEXT NOT NULL,
                    last_error TEXT,
                    candidates_seen INTEGER NOT NULL,
                    outcomes_waiting INTEGER NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS diagnostic_candidates_sequence
                BEFORE INSERT ON diagnostic_candidates
                WHEN NEW.sequence<>(SELECT COALESCE(MAX(sequence),0)+1 FROM diagnostic_candidates)
                BEGIN SELECT RAISE(ABORT, 'diagnostic candidate sequence must be gap-free'); END;
                CREATE TRIGGER IF NOT EXISTS diagnostic_evidence_sequence
                BEFORE INSERT ON diagnostic_evidence_bindings
                WHEN NEW.sequence<>(
                  SELECT COALESCE(MAX(sequence),0)+1 FROM diagnostic_evidence_bindings
                )
                BEGIN SELECT RAISE(ABORT, 'diagnostic evidence sequence must be gap-free'); END;
                CREATE TRIGGER IF NOT EXISTS diagnostic_state_sequence
                BEFORE INSERT ON diagnostic_state_bindings
                WHEN NEW.sequence<>(
                  SELECT COALESCE(MAX(sequence),0)+1 FROM diagnostic_state_bindings
                )
                BEGIN SELECT RAISE(ABORT, 'diagnostic state sequence must be gap-free'); END;
                CREATE TRIGGER IF NOT EXISTS diagnostic_reconciliation_sequence
                BEFORE INSERT ON diagnostic_reconciliations
                WHEN NEW.sequence<>(
                  SELECT COALESCE(MAX(sequence),0)+1 FROM diagnostic_reconciliations
                )
                BEGIN
                  SELECT RAISE(ABORT, 'diagnostic reconciliation sequence must be gap-free');
                END;
                CREATE TRIGGER IF NOT EXISTS diagnostic_outcomes_sequence
                BEFORE INSERT ON diagnostic_outcomes
                WHEN NEW.sequence<>(SELECT COALESCE(MAX(sequence),0)+1 FROM diagnostic_outcomes)
                BEGIN SELECT RAISE(ABORT, 'diagnostic outcome sequence must be gap-free'); END;
                CREATE TRIGGER IF NOT EXISTS diagnostic_outcome_receipts_sequence
                BEFORE INSERT ON diagnostic_outcome_receipts
                WHEN NEW.sequence<>(
                  SELECT COALESCE(MAX(sequence),0)+1 FROM diagnostic_outcome_receipts
                )
                BEGIN
                  SELECT RAISE(ABORT, 'diagnostic outcome receipt sequence must be gap-free');
                END;
                CREATE TRIGGER IF NOT EXISTS diagnostic_candidates_no_update
                BEFORE UPDATE ON diagnostic_candidates
                BEGIN SELECT RAISE(ABORT, 'diagnostic candidates are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS diagnostic_candidates_no_delete
                BEFORE DELETE ON diagnostic_candidates
                BEGIN SELECT RAISE(ABORT, 'diagnostic candidates are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS diagnostic_evidence_no_update
                BEFORE UPDATE ON diagnostic_evidence_bindings
                BEGIN SELECT RAISE(ABORT, 'diagnostic evidence bindings are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS diagnostic_evidence_no_delete
                BEFORE DELETE ON diagnostic_evidence_bindings
                BEGIN SELECT RAISE(ABORT, 'diagnostic evidence bindings are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS diagnostic_state_no_update
                BEFORE UPDATE ON diagnostic_state_bindings
                BEGIN SELECT RAISE(ABORT, 'diagnostic state bindings are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS diagnostic_state_no_delete
                BEFORE DELETE ON diagnostic_state_bindings
                BEGIN SELECT RAISE(ABORT, 'diagnostic state bindings are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS diagnostic_reconciliation_no_update
                BEFORE UPDATE ON diagnostic_reconciliations
                BEGIN SELECT RAISE(ABORT, 'diagnostic reconciliations are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS diagnostic_reconciliation_no_delete
                BEFORE DELETE ON diagnostic_reconciliations
                BEGIN SELECT RAISE(ABORT, 'diagnostic reconciliations are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS diagnostic_outcomes_no_update
                BEFORE UPDATE ON diagnostic_outcomes
                BEGIN SELECT RAISE(ABORT, 'diagnostic outcomes are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS diagnostic_outcomes_no_delete
                BEFORE DELETE ON diagnostic_outcomes
                BEGIN SELECT RAISE(ABORT, 'diagnostic outcomes are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS diagnostic_outcome_receipts_no_update
                BEFORE UPDATE ON diagnostic_outcome_receipts
                BEGIN SELECT RAISE(ABORT, 'diagnostic outcome receipts are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS diagnostic_outcome_receipts_no_delete
                BEFORE DELETE ON diagnostic_outcome_receipts
                BEGIN SELECT RAISE(ABORT, 'diagnostic outcome receipts are immutable'); END;
                """
            )

    def _append(self, table: str, columns: dict[str, object], record: dict[str, Any]) -> bool:
        encoded = _canonical(record)
        digest = _sha256(encoded)
        identifier = _record_id(table, digest)
        id_column = {
            "diagnostic_candidates": "candidate_id",
            "diagnostic_evidence_bindings": "binding_id",
            "diagnostic_state_bindings": "binding_id",
            "diagnostic_reconciliations": "reconciliation_id",
        }[table]
        identity = columns.get(id_column)
        if identity is not None and identity != identifier:
            raise ValueError(f"{table} identity does not match record digest")
        columns[id_column] = identifier
        columns["record_sha256"] = digest
        columns["record_json"] = encoded
        with contextlib.closing(self._connect()) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            unique_column = "packet_id" if table != "diagnostic_reconciliations" else id_column
            unique_value = columns[unique_column]
            existing = conn.execute(
                f"SELECT record_sha256,record_json FROM {table} WHERE {unique_column}=?",
                (unique_value,),
            ).fetchone()
            if existing is not None:
                if str(existing["record_sha256"]) != digest:
                    existing_record = json.loads(bytes(existing["record_json"]))
                    replay_record = dict(record)
                    if not isinstance(existing_record, dict):
                        raise ValueError(f"{table} existing record is not an object")
                    existing_record.pop("recorded_at_utc", None)
                    replay_record.pop("recorded_at_utc", None)
                    if _canonical(existing_record) != _canonical(replay_record):
                        raise ValueError(f"{table} identity already binds different content")
                return False
            columns["sequence"] = int(
                conn.execute(f"SELECT COALESCE(MAX(sequence),0)+1 FROM {table}").fetchone()[0]
            )
            names = ",".join(columns)
            placeholders = ",".join("?" for _ in columns)
            conn.execute(
                f"INSERT INTO {table}({names}) VALUES({placeholders})", tuple(columns.values())
            )
        return True

    def candidate(self, packet_id: str) -> sqlite3.Row | None:
        with contextlib.closing(self._connect()) as conn:
            row: sqlite3.Row | None = conn.execute(
                "SELECT * FROM diagnostic_candidates WHERE packet_id=?", (packet_id,)
            ).fetchone()
        return row

    def candidate_packet_ids(self) -> set[str]:
        with contextlib.closing(self._connect()) as conn:
            return {
                str(row[0]) for row in conn.execute("SELECT packet_id FROM diagnostic_candidates")
            }

    def candidates(self) -> list[sqlite3.Row]:
        with contextlib.closing(self._connect()) as conn:
            return conn.execute("SELECT * FROM diagnostic_candidates ORDER BY sequence").fetchall()

    def add_candidate(self, record: dict[str, Any]) -> bool:
        return self._append(
            "diagnostic_candidates",
            {
                "packet_id": record["packet_id"],
                "signal_at_utc": record["canary_selection"]["signal_at_utc"],
                "source_first_observed_at_utc": record["source"]["source_first_observed_at_utc"],
                "entry_session": record["canary_selection"]["entry_session"],
                "final_session": record["schedule_binding"]["final_session"],
                "canary_selection_sha256": record["canary_selection_sha256"],
                "recorded_at_utc": record["recorded_at_utc"],
            },
            record,
        )

    def evidence_binding(self, packet_id: str) -> sqlite3.Row | None:
        with contextlib.closing(self._connect()) as conn:
            row: sqlite3.Row | None = conn.execute(
                "SELECT * FROM diagnostic_evidence_bindings WHERE packet_id=?", (packet_id,)
            ).fetchone()
        return row

    def add_evidence_binding(self, record: dict[str, Any]) -> bool:
        return self._append(
            "diagnostic_evidence_bindings",
            {
                "packet_id": record["packet_id"],
                "evidence_record_sha256": record["evidence_record_sha256"],
                "routine_eligible": int(record["routine_eligible"]),
                "routine_reason": record["routine_reason"],
                "recorded_at_utc": record["recorded_at_utc"],
            },
            record,
        )

    def state_binding(self, packet_id: str) -> sqlite3.Row | None:
        with contextlib.closing(self._connect()) as conn:
            row: sqlite3.Row | None = conn.execute(
                "SELECT * FROM diagnostic_state_bindings WHERE packet_id=?", (packet_id,)
            ).fetchone()
        return row

    def add_state_binding(self, record: dict[str, Any]) -> bool:
        return self._append(
            "diagnostic_state_bindings",
            {
                "packet_id": record["packet_id"],
                "shadow_state": record["shadow_state"],
                "canary_state_sha256": record["canary_state_sha256"],
                "shadow_trade_sha256": record["shadow_trade_sha256"],
                "recorded_at_utc": record["recorded_at_utc"],
            },
            record,
        )

    def outcome_receipt(self, packet_id: str) -> sqlite3.Row | None:
        with contextlib.closing(self._connect()) as conn:
            row: sqlite3.Row | None = conn.execute(
                "SELECT * FROM diagnostic_outcome_receipts WHERE packet_id=?", (packet_id,)
            ).fetchone()
        return row

    def outcome_disposition_counts(self) -> dict[str, int]:
        with contextlib.closing(self._connect()) as conn:
            return {
                str(row["disposition"]): int(row["count"])
                for row in conn.execute(
                    "SELECT disposition,COUNT(*) count FROM diagnostic_outcome_receipts "
                    "GROUP BY disposition"
                )
            }

    @staticmethod
    def _scientific_record(record: dict[str, Any], *, receipt: bool = False) -> bytes:
        stable = dict(record)
        stable.pop("recorded_at_utc", None)
        if receipt:
            stable.pop("outcome_record_sha256", None)
        return _canonical(stable)

    def append_outcome_receipt(
        self,
        *,
        outcome_record: dict[str, Any] | None,
        receipt_record: dict[str, Any],
    ) -> tuple[bool, bool]:
        """Atomically append a terminal disposition and its optional economic outcome."""

        disposition = str(receipt_record.get("disposition"))
        if disposition == "available" and outcome_record is None:
            raise ValueError("available diagnostic disposition must own an outcome")
        if disposition == "not_traded" and outcome_record is not None:
            raise ValueError("not-traded diagnostic disposition cannot own an outcome")
        packet_id = str(receipt_record.get("packet_id"))
        candidate_id = str(receipt_record.get("candidate_id"))
        outcome_encoded: bytes | None = None
        outcome_digest: str | None = None
        outcome_id: str | None = None
        if outcome_record is not None:
            if (
                str(outcome_record.get("packet_id")) != packet_id
                or str(outcome_record.get("candidate_id")) != candidate_id
            ):
                raise ValueError("diagnostic outcome and receipt identity mismatch")
            outcome_encoded = _canonical(outcome_record)
            outcome_digest = _sha256(outcome_encoded)
            outcome_id = _record_id("diagnostic_outcomes", outcome_digest)
        receipt_record = {**receipt_record, "outcome_record_sha256": outcome_digest}
        receipt_encoded = _canonical(receipt_record)
        receipt_digest = _sha256(receipt_encoded)
        receipt_id = _record_id("diagnostic_outcome_receipts", receipt_digest)
        if outcome_record is not None:
            assert outcome_digest is not None
            assert outcome_id is not None
            self._validate_row(
                "diagnostic_outcomes",
                {
                    "outcome_id": outcome_id,
                    "candidate_id": candidate_id,
                    "packet_id": packet_id,
                    "trade_id": outcome_record.get("trade_id"),
                    "record_sha256": outcome_digest,
                    "recorded_at_utc": outcome_record.get("recorded_at_utc"),
                },
                outcome_record,
                outcome_digest,
            )
        self._validate_row(
            "diagnostic_outcome_receipts",
            {
                "receipt_id": receipt_id,
                "candidate_id": candidate_id,
                "packet_id": packet_id,
                "disposition": disposition,
                "reason": receipt_record.get("reason"),
                "outcome_record_sha256": outcome_digest,
                "record_sha256": receipt_digest,
                "recorded_at_utc": receipt_record.get("recorded_at_utc"),
            },
            receipt_record,
            receipt_digest,
        )
        with contextlib.closing(self._connect()) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            candidate = conn.execute(
                "SELECT candidate_id,record_sha256 FROM diagnostic_candidates WHERE packet_id=?",
                (packet_id,),
            ).fetchone()
            if candidate is None or str(candidate["candidate_id"]) != candidate_id:
                raise ValueError("diagnostic outcome receipt has no owning candidate")
            if receipt_record.get("candidate_record_sha256") != candidate["record_sha256"]:
                raise ValueError("diagnostic outcome receipt candidate digest mismatch")
            state = conn.execute(
                "SELECT record_sha256 FROM diagnostic_state_bindings WHERE packet_id=?",
                (packet_id,),
            ).fetchone()
            if (
                state is None
                or receipt_record.get("state_binding_record_sha256") != state["record_sha256"]
            ):
                raise ValueError("diagnostic outcome receipt state digest mismatch")
            evidence_sha = receipt_record.get("evidence_binding_record_sha256")
            if evidence_sha is not None:
                evidence = conn.execute(
                    "SELECT record_sha256 FROM diagnostic_evidence_bindings WHERE packet_id=?",
                    (packet_id,),
                ).fetchone()
                if evidence is None or evidence_sha != evidence["record_sha256"]:
                    raise ValueError("diagnostic outcome receipt evidence digest mismatch")
            if outcome_record is not None and (
                outcome_record.get("candidate_record_sha256") != candidate["record_sha256"]
                or outcome_record.get("state_binding_record_sha256") != state["record_sha256"]
            ):
                raise ValueError("diagnostic outcome provenance digest mismatch")
            existing_receipt = conn.execute(
                "SELECT * FROM diagnostic_outcome_receipts WHERE packet_id=?", (packet_id,)
            ).fetchone()
            existing_outcome = conn.execute(
                "SELECT * FROM diagnostic_outcomes WHERE packet_id=?", (packet_id,)
            ).fetchone()
            if existing_receipt is not None:
                persisted_receipt = json.loads(bytes(existing_receipt["record_json"]))
                if not isinstance(persisted_receipt, dict) or self._scientific_record(
                    persisted_receipt, receipt=True
                ) != self._scientific_record(receipt_record, receipt=True):
                    raise ValueError("diagnostic outcome receipt conflicts with persisted content")
                if outcome_record is None:
                    if existing_outcome is not None:
                        raise ValueError("diagnostic receipt unexpectedly owns outcome")
                else:
                    if existing_outcome is None:
                        raise ValueError("diagnostic receipt is missing its outcome")
                    persisted_outcome = json.loads(bytes(existing_outcome["record_json"]))
                    if not isinstance(persisted_outcome, dict) or self._scientific_record(
                        persisted_outcome
                    ) != self._scientific_record(outcome_record):
                        raise ValueError("diagnostic outcome conflicts with persisted content")
                    if (
                        existing_receipt["outcome_record_sha256"]
                        != existing_outcome["record_sha256"]
                    ):
                        raise ValueError("diagnostic outcome receipt digest link is corrupt")
                return False, False
            if existing_outcome is not None:
                raise ValueError("orphan diagnostic outcome exists without a receipt")
            outcome_added = False
            if outcome_record is not None:
                assert outcome_encoded is not None
                assert outcome_digest is not None
                assert outcome_id is not None
                outcome_sequence = int(
                    conn.execute(
                        "SELECT COALESCE(MAX(sequence),0)+1 FROM diagnostic_outcomes"
                    ).fetchone()[0]
                )
                conn.execute(
                    """
                    INSERT INTO diagnostic_outcomes(
                      sequence,outcome_id,candidate_id,packet_id,trade_id,record_sha256,
                      record_json,recorded_at_utc
                    ) VALUES(?,?,?,?,?,?,?,?)
                    """,
                    (
                        outcome_sequence,
                        outcome_id,
                        candidate_id,
                        packet_id,
                        outcome_record["trade_id"],
                        outcome_digest,
                        outcome_encoded,
                        outcome_record["recorded_at_utc"],
                    ),
                )
                outcome_added = True
            receipt_sequence = int(
                conn.execute(
                    "SELECT COALESCE(MAX(sequence),0)+1 FROM diagnostic_outcome_receipts"
                ).fetchone()[0]
            )
            conn.execute(
                """
                INSERT INTO diagnostic_outcome_receipts(
                  sequence,receipt_id,candidate_id,packet_id,disposition,reason,
                  outcome_record_sha256,record_sha256,record_json,recorded_at_utc
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    receipt_sequence,
                    receipt_id,
                    candidate_id,
                    packet_id,
                    disposition,
                    receipt_record["reason"],
                    outcome_digest,
                    receipt_digest,
                    receipt_encoded,
                    receipt_record["recorded_at_utc"],
                ),
            )
        return outcome_added, True

    def add_reconciliation(
        self,
        *,
        packet_id: str | None,
        category: str,
        detail: dict[str, Any],
        now: datetime,
    ) -> bool:
        stable = {
            "contract_version": DIAGNOSTIC_CONTRACT_VERSION,
            "hypothesis_id": HYPOTHESIS_ID,
            "packet_id": packet_id,
            "category": category,
            "detail": detail,
        }
        stable_digest = _sha256(_canonical(stable))
        record = {**stable, "stable_issue_sha256": stable_digest, "recorded_at_utc": _utc_text(now)}
        encoded = _canonical(record)
        digest = _sha256(encoded)
        reconciliation_id = str(
            uuid.uuid5(uuid.NAMESPACE_URL, f"{HYPOTHESIS_ID}|diagnostic-issue|{stable_digest}")
        )
        with contextlib.closing(self._connect()) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                "SELECT 1 FROM diagnostic_reconciliations WHERE reconciliation_id=?",
                (reconciliation_id,),
            ).fetchone()
            if existing is not None:
                return False
            sequence = int(
                conn.execute(
                    "SELECT COALESCE(MAX(sequence),0)+1 FROM diagnostic_reconciliations"
                ).fetchone()[0]
            )
            conn.execute(
                """
                INSERT INTO diagnostic_reconciliations(
                  sequence,reconciliation_id,packet_id,category,record_sha256,record_json,
                  recorded_at_utc
                ) VALUES(?,?,?,?,?,?,?)
                """,
                (
                    sequence,
                    reconciliation_id,
                    packet_id,
                    category,
                    digest,
                    encoded,
                    _utc_text(now),
                ),
            )
        return True

    def write_health(self, *, now: datetime, result: DiagnosticRunResult) -> None:
        with contextlib.closing(self._connect()) as conn, conn:
            conn.execute(
                """
                INSERT INTO diagnostic_health VALUES(1,?,?,?,?,?)
                ON CONFLICT(singleton) DO UPDATE SET
                  last_worker_heartbeat_utc=excluded.last_worker_heartbeat_utc,
                  last_result=excluded.last_result,
                  last_error=excluded.last_error,
                  candidates_seen=excluded.candidates_seen,
                  unresolved_candidates=excluded.unresolved_candidates
                WHERE excluded.last_worker_heartbeat_utc>=
                      diagnostic_health.last_worker_heartbeat_utc
                """,
                (
                    _utc_text(now),
                    result.status,
                    result.error,
                    result.candidates_seen,
                    result.unresolved_candidates,
                ),
            )

    def write_outcome_health(
        self,
        *,
        now: datetime,
        status: str,
        error: str | None,
        candidates_seen: int,
        outcomes_waiting: int,
    ) -> None:
        with contextlib.closing(self._connect()) as conn, conn:
            conn.execute(
                """
                INSERT INTO diagnostic_outcome_health VALUES(1,?,?,?,?,?)
                ON CONFLICT(singleton) DO UPDATE SET
                  last_worker_heartbeat_utc=excluded.last_worker_heartbeat_utc,
                  last_result=excluded.last_result,
                  last_error=excluded.last_error,
                  candidates_seen=excluded.candidates_seen,
                  outcomes_waiting=excluded.outcomes_waiting
                WHERE excluded.last_worker_heartbeat_utc>=
                      diagnostic_outcome_health.last_worker_heartbeat_utc
                """,
                (
                    _utc_text(now),
                    status,
                    error,
                    candidates_seen,
                    outcomes_waiting,
                ),
            )

    def validate_integrity(self) -> None:
        tables = (
            "diagnostic_candidates",
            "diagnostic_evidence_bindings",
            "diagnostic_state_bindings",
            "diagnostic_reconciliations",
            "diagnostic_outcomes",
            "diagnostic_outcome_receipts",
        )
        with contextlib.closing(self._connect()) as conn:
            for table in tables:
                rows = conn.execute(f"SELECT * FROM {table} ORDER BY sequence").fetchall()
                if [int(row["sequence"]) for row in rows] != list(range(1, len(rows) + 1)):
                    raise ValueError(f"{table} sequence is not gap-free")
                for row in rows:
                    raw = bytes(row["record_json"])
                    digest = _sha256(raw)
                    if digest != str(row["record_sha256"]):
                        raise ValueError(f"{table} stored bytes failed integrity check")
                    value = json.loads(raw)
                    if not isinstance(value, dict) or _canonical(value) != raw:
                        raise ValueError(f"{table} record is not canonical JSON")
                    self._validate_row(table, row, value, digest)
            candidates = {
                str(row["packet_id"]): row
                for row in conn.execute("SELECT * FROM diagnostic_candidates")
            }
            for table in (
                "diagnostic_evidence_bindings",
                "diagnostic_state_bindings",
                "diagnostic_outcomes",
                "diagnostic_outcome_receipts",
            ):
                for row in conn.execute(f"SELECT * FROM {table}"):
                    candidate = candidates.get(str(row["packet_id"]))
                    if candidate is None:
                        raise ValueError(f"{table} has no owning diagnostic candidate")
                    if (
                        table in {"diagnostic_outcomes", "diagnostic_outcome_receipts"}
                        and row["candidate_id"] != candidate["candidate_id"]
                    ):
                        raise ValueError(f"{table} candidate identity mismatch")
            evidence = {
                str(row["packet_id"]): str(row["record_sha256"])
                for row in conn.execute("SELECT * FROM diagnostic_evidence_bindings")
            }
            states = {
                str(row["packet_id"]): str(row["record_sha256"])
                for row in conn.execute("SELECT * FROM diagnostic_state_bindings")
            }
            outcomes = {
                str(row["packet_id"]): row
                for row in conn.execute("SELECT * FROM diagnostic_outcomes")
            }
            receipts = {
                str(row["packet_id"]): row
                for row in conn.execute("SELECT * FROM diagnostic_outcome_receipts")
            }
            if set(outcomes) != {
                packet_id
                for packet_id, row in receipts.items()
                if row["outcome_record_sha256"] is not None
            }:
                raise ValueError("diagnostic outcomes and receipt links do not match")
            for packet_id, outcome in outcomes.items():
                if receipts[packet_id]["outcome_record_sha256"] != outcome["record_sha256"]:
                    raise ValueError("diagnostic outcome receipt digest link mismatch")
                record = json.loads(bytes(outcome["record_json"]))
                candidate = candidates[packet_id]
                candidate_record = json.loads(bytes(candidate["record_json"]))
                selection = candidate_record.get("canary_selection")
                if (
                    record.get("candidate_record_sha256") != candidate["record_sha256"]
                    or record.get("state_binding_record_sha256") != states.get(packet_id)
                    or not isinstance(selection, dict)
                    or record.get("symbol") != selection.get("symbol")
                ):
                    raise ValueError("diagnostic outcome provenance link mismatch")
            for packet_id, row in receipts.items():
                record = json.loads(bytes(row["record_json"]))
                candidate = candidates[packet_id]
                if (
                    record.get("candidate_record_sha256") != candidate["record_sha256"]
                    or record.get("state_binding_record_sha256") != states.get(packet_id)
                    or (
                        record.get("evidence_binding_record_sha256") is not None
                        and record.get("evidence_binding_record_sha256") != evidence.get(packet_id)
                    )
                ):
                    raise ValueError("diagnostic outcome receipt provenance link mismatch")

    @staticmethod
    def _validate_row(
        table: str,
        row: Mapping[str, Any] | sqlite3.Row,
        record: dict[str, Any],
        digest: str,
    ) -> None:
        if record.get("contract_version") != DIAGNOSTIC_CONTRACT_VERSION:
            raise ValueError(f"{table} contract version mismatch")
        if record.get("hypothesis_id") != HYPOTHESIS_ID:
            raise ValueError(f"{table} hypothesis mismatch")
        if str(record.get("recorded_at_utc")) != str(row["recorded_at_utc"]):
            raise ValueError(f"{table} recorded timestamp mismatch")
        if table == "diagnostic_reconciliations":
            stable = dict(record)
            stable.pop("recorded_at_utc", None)
            stable_digest = str(stable.pop("stable_issue_sha256", ""))
            if _sha256(_canonical(stable)) != stable_digest:
                raise ValueError("diagnostic reconciliation stable digest mismatch")
            expected_id = str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"{HYPOTHESIS_ID}|diagnostic-issue|{stable_digest}",
                )
            )
            if (
                str(row["reconciliation_id"]) != expected_id
                or record.get("packet_id") != row["packet_id"]
                or str(record.get("category")) != str(row["category"])
            ):
                raise ValueError("diagnostic reconciliation columns mismatch")
            return
        id_column = {
            "diagnostic_candidates": "candidate_id",
            "diagnostic_evidence_bindings": "binding_id",
            "diagnostic_state_bindings": "binding_id",
            "diagnostic_outcomes": "outcome_id",
            "diagnostic_outcome_receipts": "receipt_id",
        }[table]
        if str(row[id_column]) != _record_id(table, digest):
            raise ValueError(f"{table} identity mismatch")
        if str(record.get("packet_id")) != str(row["packet_id"]):
            raise ValueError(f"{table} packet mismatch")
        if table == "diagnostic_candidates":
            selection = record.get("canary_selection")
            schedule = record.get("schedule_binding")
            source = record.get("source")
            if not all(isinstance(value, dict) for value in (selection, schedule, source)):
                raise ValueError("diagnostic candidate nested record missing")
            assert isinstance(selection, dict)
            assert isinstance(schedule, dict)
            assert isinstance(source, dict)
            if (
                str(selection.get("signal_at_utc")) != str(row["signal_at_utc"])
                or str(source.get("source_first_observed_at_utc"))
                != str(row["source_first_observed_at_utc"])
                or selection.get("entry_session") != row["entry_session"]
                or schedule.get("final_session") != row["final_session"]
                or str(record.get("canary_selection_sha256")) != str(row["canary_selection_sha256"])
                or _sha256(_canonical(selection)) != str(row["canary_selection_sha256"])
            ):
                raise ValueError("diagnostic candidate columns mismatch")
        elif table == "diagnostic_evidence_bindings":
            if (
                str(record.get("evidence_record_sha256")) != str(row["evidence_record_sha256"])
                or int(bool(record.get("routine_eligible"))) != int(row["routine_eligible"])
                or str(record.get("routine_reason")) != str(row["routine_reason"])
            ):
                raise ValueError("diagnostic evidence columns mismatch")
        elif table == "diagnostic_state_bindings":
            state = record.get("canary_state")
            if not isinstance(state, dict):
                raise ValueError("diagnostic canary state is missing")
            trade = state.get("shadow_trade")
            trade_sha = _sha256(_canonical(trade)) if isinstance(trade, dict) else None
            if (
                str(record.get("shadow_state")) != str(row["shadow_state"])
                or state.get("shadow_state") != row["shadow_state"]
                or _sha256(_canonical(state)) != str(row["canary_state_sha256"])
                or str(record.get("canary_state_sha256")) != str(row["canary_state_sha256"])
                or trade_sha != row["shadow_trade_sha256"]
                or record.get("shadow_trade_sha256") != row["shadow_trade_sha256"]
            ):
                raise ValueError("diagnostic state columns mismatch")
        elif table == "diagnostic_outcomes":
            numeric = (
                "entry_price",
                "exit_price",
                "gross_return",
                "spy_entry_price",
                "spy_exit_price",
                "spy_return",
            )
            try:
                values = {name: float(record[name]) for name in numeric}
                entry_at = _parse_utc(str(record["entry_at_utc"]))
                exit_at = _parse_utc(str(record["exit_at_utc"]))
                entry_date = date.fromisoformat(str(record["entry_date"]))
                exit_date = date.fromisoformat(str(record["exit_date"]))
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError("diagnostic outcome value is invalid") from exc
            if (
                str(record.get("candidate_id")) != str(row["candidate_id"])
                or str(record.get("trade_id")) != str(row["trade_id"])
                or str(row["trade_id"]) != _diagnostic_trade_id(str(row["packet_id"]))
                or not all(math.isfinite(value) for value in values.values())
                or values["entry_price"] <= 0
                or values["exit_price"] <= 0
                or values["spy_entry_price"] <= 0
                or values["spy_exit_price"] <= 0
                or exit_at <= entry_at
                or entry_at.astimezone(NEW_YORK).date() != entry_date
                or exit_at.astimezone(NEW_YORK).date() != exit_date
                or str(record.get("symbol")).upper() != record.get("symbol")
                or record.get("exit_reason")
                not in {"stop", "target", "time", "stop_and_target_same_day_stop_assumed"}
                or values["gross_return"] < -1.0
                or values["spy_return"] < -1.0
                or not math.isclose(
                    values["gross_return"],
                    values["exit_price"] / values["entry_price"] - 1.0,
                    rel_tol=1e-12,
                    abs_tol=1e-12,
                )
                or not math.isclose(
                    values["spy_return"],
                    values["spy_exit_price"] / values["spy_entry_price"] - 1.0,
                    rel_tol=1e-12,
                    abs_tol=1e-12,
                )
            ):
                raise ValueError("diagnostic outcome columns or arithmetic mismatch")
        elif table == "diagnostic_outcome_receipts":
            disposition = str(record.get("disposition"))
            outcome_digest = record.get("outcome_record_sha256")
            if (
                str(record.get("candidate_id")) != str(row["candidate_id"])
                or disposition != str(row["disposition"])
                or str(record.get("reason")) != str(row["reason"])
                or outcome_digest != row["outcome_record_sha256"]
                or (disposition == "available" and outcome_digest is None)
                or (disposition == "not_traded" and outcome_digest is not None)
            ):
                raise ValueError("diagnostic outcome receipt columns mismatch")

    def status(self, *, now: datetime | None = None) -> dict[str, object]:
        checked_at = (now or datetime.now(UTC)).astimezone(UTC)
        try:
            self.validate_integrity()
            with contextlib.closing(self._connect()) as conn:
                counts = {
                    "candidates": int(
                        conn.execute("SELECT COUNT(*) FROM diagnostic_candidates").fetchone()[0]
                    ),
                    "evidence_bindings": int(
                        conn.execute(
                            "SELECT COUNT(*) FROM diagnostic_evidence_bindings"
                        ).fetchone()[0]
                    ),
                    "state_bindings": int(
                        conn.execute("SELECT COUNT(*) FROM diagnostic_state_bindings").fetchone()[0]
                    ),
                    "reconciliations": int(
                        conn.execute("SELECT COUNT(*) FROM diagnostic_reconciliations").fetchone()[
                            0
                        ]
                    ),
                    "outcomes": int(
                        conn.execute("SELECT COUNT(*) FROM diagnostic_outcomes").fetchone()[0]
                    ),
                    "outcome_receipts": int(
                        conn.execute("SELECT COUNT(*) FROM diagnostic_outcome_receipts").fetchone()[
                            0
                        ]
                    ),
                    "outcome_dispositions": {
                        str(row["disposition"]): int(row["count"])
                        for row in conn.execute(
                            "SELECT disposition,COUNT(*) count "
                            "FROM diagnostic_outcome_receipts GROUP BY disposition"
                        )
                    },
                }
                health = conn.execute(
                    "SELECT * FROM diagnostic_health WHERE singleton=1"
                ).fetchone()
                outcome_health = conn.execute(
                    "SELECT * FROM diagnostic_outcome_health WHERE singleton=1"
                ).fetchone()
        except (OSError, ValueError, sqlite3.DatabaseError) as exc:
            return {
                "path": str(self.path),
                "exists": self.path.is_file(),
                "integrity_status": f"invalid:{type(exc).__name__}:{exc}",
                "operational_status": "invalid_store",
                "candidates": None,
                "evidence_bindings": None,
                "state_bindings": None,
                "reconciliations": None,
                "outcomes": None,
                "outcome_receipts": None,
                "outcome_dispositions": None,
                "health": None,
                "outcome_health": None,
            }
        health_dict = dict(health) if health is not None else None
        outcome_health_dict = dict(outcome_health) if outcome_health is not None else None

        def health_state(value: dict[str, object] | None) -> str:
            if value is None:
                return "never_ran"
            try:
                heartbeat = _parse_utc(str(value["last_worker_heartbeat_utc"]))
            except (KeyError, ValueError):
                return "invalid_health"
            age = checked_at - heartbeat
            if age < -timedelta(minutes=5):
                return "heartbeat_in_future"
            if age > HEARTBEAT_STALE_AFTER:
                return "stale"
            if value.get("last_result") not in {
                "idle_registry_draft",
                "collecting",
                "degraded",
            }:
                return "invalid_health"
            if value.get("last_result") == "degraded":
                return "degraded"
            return "healthy"

        states = (health_state(health_dict), health_state(outcome_health_dict))
        operational = next((state for state in states if state != "healthy"), "healthy")
        return {
            "path": str(self.path),
            "exists": self.path.is_file(),
            "integrity_status": "valid",
            "operational_status": operational,
            **counts,
            "health": health_dict,
            "outcome_health": outcome_health_dict,
        }


def _selection_projection(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "packet_id": str(row["packet_id"]),
        "accession_number": str(row["accession_number"]),
        "cik": str(row["cik"]),
        "symbol": str(row["symbol"]),
        "signal_at_utc": _utc_text(_parse_utc(str(row["signal_at"]))),
        "score": float(row["score"]),
        "entry_session": str(row["entry_session"]) if row["entry_session"] is not None else None,
        "lottery_rank": str(row["lottery_rank"]) if row["lottery_rank"] is not None else None,
        "eligible": bool(row["eligible"]),
        "eligibility_reason": str(row["eligibility_reason"]),
        "prior_close": float(row["prior_close"]) if row["prior_close"] is not None else None,
        "median_dollar_volume_20d": (
            float(row["median_dollar_volume_20d"])
            if row["median_dollar_volume_20d"] is not None
            else None
        ),
        "planned_quantity": (
            int(row["planned_quantity"]) if row["planned_quantity"] is not None else None
        ),
        "canary_created_at_utc": _utc_text(_parse_utc(str(row["created_at"]))),
    }


def _state_projection(row: sqlite3.Row, shadow_trade: sqlite3.Row | None) -> dict[str, Any]:
    trade = None
    if shadow_trade is not None:
        trade = dict(zip(shadow_trade.keys(), tuple(shadow_trade), strict=True))
    return {
        "packet_id": str(row["packet_id"]),
        "shadow_state": str(row["shadow_state"]),
        "shadow_trade": trade,
    }


def _read_canary(
    path: Path, activated_at: datetime
) -> tuple[list[sqlite3.Row], dict[str, str], dict[str, sqlite3.Row]]:
    uri = f"file:{path.resolve().as_posix()}?mode=ro"
    with contextlib.closing(sqlite3.connect(uri, uri=True, timeout=30)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("BEGIN")
        rows = conn.execute(
            "SELECT * FROM candidates WHERE signal_at>=? ORDER BY signal_at,packet_id",
            (activated_at.astimezone(UTC).isoformat(),),
        ).fetchall()
        metadata = {
            str(row["key"]): str(row["value"])
            for row in conn.execute(
                "SELECT key,value FROM metadata WHERE key IN "
                "('activation_utc','runtime_source_fingerprint')"
            )
        }
        trades = {
            str(row["packet_id"]): row
            for row in conn.execute("SELECT * FROM shadow_trades").fetchall()
        }
    return rows, metadata, trades


def _source_job(path: Path, packet_id: str) -> sqlite3.Row | None:
    uri = f"file:{path.resolve().as_posix()}?mode=ro"
    with contextlib.closing(sqlite3.connect(uri, uri=True, timeout=30)) as conn:
        conn.row_factory = sqlite3.Row
        row: sqlite3.Row | None = conn.execute(
            """
            SELECT job_id,packet_id,source_first_observed_at_utc,decision_at_utc
            FROM research_capture_jobs WHERE packet_id=?
            """,
            (packet_id,),
        ).fetchone()
    return row


def _evidence(path: Path, job_id: str) -> tuple[str, dict[str, Any]] | None:
    if not path.is_file():
        return None
    uri = f"file:{path.resolve().as_posix()}?mode=ro"
    with contextlib.closing(sqlite3.connect(uri, uri=True, timeout=30)) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT snapshot_id,record_sha256,stored_bytes_sha256,record_json "
            "FROM evidence_snapshots WHERE job_id=?",
            (job_id,),
        ).fetchone()
    if row is None:
        return None
    raw = bytes(row["record_json"])
    digest = str(row["record_sha256"])
    if _sha256(raw) != str(row["stored_bytes_sha256"]):
        raise ValueError("diagnostic evidence bytes failed integrity check")
    value = json.loads(raw)
    if not isinstance(value, dict) or _canonical(value) != raw:
        raise ValueError("diagnostic evidence is not canonical JSON")
    unsigned = dict(value)
    embedded_digest = str(unsigned.pop("record_sha256", ""))
    if _sha256(_canonical(unsigned)) != digest or embedded_digest != digest:
        raise ValueError("diagnostic evidence record digest mismatch")
    if str(value.get("snapshot_id")) != str(row["snapshot_id"]):
        raise ValueError("diagnostic evidence snapshot identity mismatch")
    return digest, value


def _schedule_binding(
    store: SessionFeedStore, entry_session: date | None, *, as_of_utc: datetime
) -> dict[str, Any]:
    records = store.schedule_records_as_known_at(as_of_utc)
    watermark = max((record.sequence for record in records), default=0)
    if entry_session is None:
        return {
            "observation_watermark": watermark,
            "record_sha256s": [],
            "final_session": None,
        }
    eligible = [record for record in records if record.session.session_date >= entry_session]
    if (
        not eligible
        or eligible[0].session.session_date != entry_session
        or len(eligible) < MAX_SESSIONS
    ):
        raise TrialRuntimeInvalid("diagnostic_schedule_does_not_cover_frozen_horizon")
    selected = eligible[:MAX_SESSIONS]
    return {
        "observation_watermark": watermark,
        "record_sha256s": [record.record_sha256 for record in selected],
        "final_session": selected[-1].session.session_date.isoformat(),
    }


def _routine_binding(
    evidence_sha: str,
    evidence: dict[str, Any],
    *,
    packet_id: str,
    selection: dict[str, Any],
    expected_source_at: datetime,
    expected_decision_at: datetime,
    registry_sha256: str,
    entry_session: date | None,
    now: datetime,
) -> dict[str, Any]:
    payload = evidence.get("payload")
    if not isinstance(payload, dict):
        raise ValueError("diagnostic evidence payload is missing")
    timing = payload.get("timing")
    classification = payload.get("classification")
    versions = payload.get("versions")
    signal = payload.get("signal")
    if not all(isinstance(value, dict) for value in (timing, classification, versions)):
        raise ValueError("diagnostic evidence timing or classification is missing")
    assert isinstance(timing, dict)
    assert isinstance(classification, dict)
    assert isinstance(versions, dict)
    if not isinstance(signal, dict):
        raise ValueError("diagnostic evidence signal is missing")
    if str(signal.get("packet_id")) != packet_id:
        raise ValueError("diagnostic evidence packet mismatch")
    recorded_at = _parse_utc(str(evidence.get("recorded_at_utc", "")))
    cutoff = (
        datetime.combine(entry_session, SIGNAL_CUTOFF, NEW_YORK).astimezone(UTC)
        if entry_session is not None
        else None
    )
    state = str(classification.get("state"))
    mapping = str(classification.get("transaction_owner_mapping"))
    owner_cik = classification.get("owner_cik")
    reporting_owner_ciks = signal.get("reporting_owner_ciks")
    history_complete = classification.get("history_coverage_complete") is True
    evidence_source_at = _parse_utc(str(timing.get("source_first_observed_at_utc", "")))
    evidence_decision_at = _parse_utc(str(timing.get("decision_at_utc", "")))
    provenance_valid = (
        evidence.get("hypothesis_id") == HYPOTHESIS_ID
        and evidence_source_at == expected_source_at
        and evidence_decision_at == expected_decision_at
        and versions.get("policy_sha256") == registry_sha256
        and str(signal.get("packet_id")) == packet_id
        and str(signal.get("accession_number")) == selection["accession_number"]
        and str(signal.get("issuer_symbol")).upper() == selection["symbol"]
    )
    if not provenance_valid:
        reason = "evidence_provenance_mismatch"
    elif cutoff is None:
        reason = "control_has_no_entry_session"
    elif recorded_at >= cutoff:
        reason = "evidence_not_recorded_before_entry_cutoff"
    elif state != "routine":
        reason = f"classification_{state}"
    elif mapping != "exact":
        reason = f"owner_mapping_{mapping}"
    elif (
        not isinstance(owner_cik, str)
        or not owner_cik.strip()
        or reporting_owner_ciks != [owner_cik]
    ):
        reason = "single_owner_cik_missing"
    elif not history_complete:
        reason = "history_coverage_incomplete"
    else:
        reason = "routine_exact_single_owner_complete_history_pre_cutoff"
    return {
        "contract_version": DIAGNOSTIC_CONTRACT_VERSION,
        "hypothesis_id": HYPOTHESIS_ID,
        "packet_id": packet_id,
        "evidence_record_sha256": evidence_sha,
        "evidence_recorded_at_utc": _utc_text(recorded_at),
        "evidence_source_first_observed_at_utc": _utc_text(evidence_source_at),
        "evidence_decision_at_utc": _utc_text(evidence_decision_at),
        "provenance_valid": provenance_valid,
        "classification_state": state,
        "transaction_owner_mapping": mapping,
        "owner_cik": owner_cik,
        "history_coverage_complete": history_complete,
        "entry_cutoff_at_utc": _utc_text(cutoff) if cutoff is not None else None,
        "routine_eligible": reason == "routine_exact_single_owner_complete_history_pre_cutoff",
        "routine_reason": reason,
        "recorded_at_utc": _utc_text(now),
    }


def run_diagnostics_once(
    config: DiagnosticConfig, *, now: datetime | None = None
) -> DiagnosticRunResult:
    """Bind prospective control evidence without changing canary or confirmatory state."""

    now = (now or datetime.now(UTC)).astimezone(UTC)
    store = DiagnosticStore(config.diagnostics_db)
    trial_config = TrialRuntimeConfig(
        trial_db=Path("unused-diagnostic-trial.db"),
        evidence_db=config.evidence_db,
        bar_feed_db=config.bar_feed_db,
        session_feed_db=config.session_feed_db,
        registry_path=config.registry_path,
    )
    window = _validated_trial_window(trial_config)
    if window.status == "draft":
        result = DiagnosticRunResult("idle_registry_draft")
        store.write_health(now=now, result=result)
        return result
    if window.activated_at_utc is None:
        raise TrialRuntimeInvalid("active_diagnostic_window_missing_activation")

    session_store = SessionFeedStore(config.session_feed_db, initialize=False)
    session_store.validate_integrity()
    bar_store = BarFeedStore(config.bar_feed_db)
    bar_store.validate_integrity()
    bound_packet_ids = store.candidate_packet_ids()
    rows, metadata, trades = _read_canary(config.canary_ledger_db, window.activated_at_utc)
    observed_packet_ids = {str(row["packet_id"]) for row in rows}
    canary_activation = metadata.get("activation_utc")
    if canary_activation is None:
        raise ValueError("canary activation metadata is missing")

    added = evidence_added = states_added = requests = reconciliations = 0
    unresolved_packets: set[str] = set()
    for row in rows:
        packet_id = str(row["packet_id"])
        selection = _selection_projection(row)
        selection_sha = _sha256(_canonical(selection))
        job = _source_job(config.source_db, packet_id)
        if job is None:
            unresolved_packets.add(packet_id)
            reconciliations += int(
                store.add_reconciliation(
                    packet_id=packet_id,
                    category="source_capture_job_missing",
                    detail={"canary_selection_sha256": selection_sha},
                    now=now,
                )
            )
            continue
        source_at = _parse_utc(str(job["source_first_observed_at_utc"]))
        decision_at = _parse_utc(str(job["decision_at_utc"]))
        signal_at = _parse_utc(str(row["signal_at"]))
        if source_at < window.activated_at_utc:
            continue
        if source_at > signal_at:
            reconciliations += int(
                store.add_reconciliation(
                    packet_id=packet_id,
                    category="source_after_canary_signal",
                    detail={
                        "source_at_utc": _utc_text(source_at),
                        "signal_at_utc": _utc_text(signal_at),
                    },
                    now=now,
                )
            )
            unresolved_packets.add(packet_id)
            continue
        if decision_at != signal_at:
            unresolved_packets.add(packet_id)
            reconciliations += int(
                store.add_reconciliation(
                    packet_id=packet_id,
                    category="approval_timestamp_mismatch",
                    detail={
                        "capture_decision_at_utc": _utc_text(decision_at),
                        "canary_signal_at_utc": _utc_text(signal_at),
                    },
                    now=now,
                )
            )

        existing = store.candidate(packet_id)
        if existing is None:
            entry = date.fromisoformat(str(row["entry_session"])) if row["entry_session"] else None
            try:
                schedule = _schedule_binding(session_store, entry, as_of_utc=signal_at)
            except TrialRuntimeInvalid as exc:
                unresolved_packets.add(packet_id)
                reconciliations += int(
                    store.add_reconciliation(
                        packet_id=packet_id,
                        category="schedule_horizon_unavailable",
                        detail={
                            "reason": str(exc),
                            "entry_session": entry.isoformat() if entry else None,
                        },
                        now=now,
                    )
                )
                continue
            candidate_record = {
                "contract_version": DIAGNOSTIC_CONTRACT_VERSION,
                "hypothesis_id": HYPOTHESIS_ID,
                "packet_id": packet_id,
                "registry_sha256": window.registry_sha256,
                "canary_activation_utc": _utc_text(_parse_utc(canary_activation)),
                "canary_runtime_source_fingerprint": metadata.get("runtime_source_fingerprint"),
                "canary_selection": selection,
                "canary_selection_sha256": selection_sha,
                "source": {
                    "job_id": str(job["job_id"]),
                    "source_first_observed_at_utc": _utc_text(source_at),
                    "decision_at_utc": _utc_text(decision_at),
                },
                "schedule_binding": schedule,
                "recorded_at_utc": _utc_text(now),
            }
            added += int(store.add_candidate(candidate_record))
        elif str(existing["canary_selection_sha256"]) != selection_sha:
            unresolved_packets.add(packet_id)
            reconciliations += int(
                store.add_reconciliation(
                    packet_id=packet_id,
                    category="canary_selection_projection_changed",
                    detail={
                        "bound_sha256": str(existing["canary_selection_sha256"]),
                        "observed_sha256": selection_sha,
                    },
                    now=now,
                )
            )

        bound = store.candidate(packet_id)
        if bound is None:
            unresolved_packets.add(packet_id)
            continue
        bound_record = json.loads(bytes(bound["record_json"]))
        if not isinstance(bound_record, dict):
            raise ValueError("bound diagnostic candidate is not an object")
        bound_selection = bound_record.get("canary_selection")
        bound_source = bound_record.get("source")
        if not isinstance(bound_selection, dict) or not isinstance(bound_source, dict):
            raise ValueError("bound diagnostic candidate provenance is missing")
        bound_entry = (
            date.fromisoformat(str(bound["entry_session"]))
            if bound["entry_session"] is not None
            else None
        )
        bound_source_at = _parse_utc(str(bound_source["source_first_observed_at_utc"]))
        bound_decision_at = _parse_utc(str(bound_source["decision_at_utc"]))
        if bound_entry is not None and bound["final_session"] is not None:
            for symbol in (str(bound_selection["symbol"]), "SPY"):
                request = BarRequest(
                    request_id=f"{bound['candidate_id']}|{symbol}|daily-v1",
                    symbol=symbol,
                    start_date=bound_source_at.astimezone(NEW_YORK).date()
                    - timedelta(days=BAR_LOOKBACK_CALENDAR_DAYS),
                    through_date=date.fromisoformat(str(bound["final_session"])),
                    requested_at_utc=_parse_utc(str(bound["recorded_at_utc"])),
                    requester=DIAGNOSTIC_BAR_REQUESTER,
                )
                try:
                    request_existed = bar_store.has_request(request.request_id)
                    bar_store.request(request)
                except ValueError as exc:
                    unresolved_packets.add(packet_id)
                    reconciliations += int(
                        store.add_reconciliation(
                            packet_id=packet_id,
                            category="bar_request_identity_conflict",
                            detail={
                                "request_id": request.request_id,
                                "reason": f"{type(exc).__name__}:{exc}"[:1000],
                            },
                            now=now,
                        )
                    )
                else:
                    requests += int(not request_existed)

        bound_evidence = store.evidence_binding(packet_id)
        if bound_evidence is None:
            try:
                evidence = _evidence(config.evidence_db, str(job["job_id"]))
                if evidence is not None:
                    evidence_record = _routine_binding(
                        evidence[0],
                        evidence[1],
                        packet_id=packet_id,
                        selection=bound_selection,
                        expected_source_at=bound_source_at,
                        expected_decision_at=bound_decision_at,
                        registry_sha256=window.registry_sha256,
                        entry_session=bound_entry,
                        now=now,
                    )
                    evidence_added += int(store.add_evidence_binding(evidence_record))
                    if evidence_record["provenance_valid"] is not True:
                        unresolved_packets.add(packet_id)
                        reconciliations += int(
                            store.add_reconciliation(
                                packet_id=packet_id,
                                category="evidence_provenance_mismatch",
                                detail={
                                    "evidence_record_sha256": evidence[0],
                                    "routine_reason": evidence_record["routine_reason"],
                                },
                                now=now,
                            )
                        )
            except (KeyError, TypeError, ValueError, sqlite3.DatabaseError) as exc:
                unresolved_packets.add(packet_id)
                reconciliations += int(
                    store.add_reconciliation(
                        packet_id=packet_id,
                        category="evidence_binding_unavailable",
                        detail={"reason": f"{type(exc).__name__}:{exc}"[:1000]},
                        now=now,
                    )
                )
        else:
            evidence_record = json.loads(bytes(bound_evidence["record_json"]))
            if not isinstance(evidence_record, dict):
                raise ValueError("bound diagnostic evidence is not an object")
            if evidence_record.get("provenance_valid") is not True:
                unresolved_packets.add(packet_id)

        shadow_state = str(row["shadow_state"])
        if shadow_state in FINAL_SHADOW_STATES:
            shadow_trade = trades.get(packet_id)
            state_mismatch = (shadow_state == "closed" and shadow_trade is None) or (
                shadow_state != "closed" and shadow_trade is not None
            )
            if state_mismatch:
                unresolved_packets.add(packet_id)
                reconciliations += int(
                    store.add_reconciliation(
                        packet_id=packet_id,
                        category="canary_shadow_state_trade_mismatch",
                        detail={
                            "shadow_state": shadow_state,
                            "shadow_trade_present": shadow_trade is not None,
                        },
                        now=now,
                    )
                )
                continue
            state_projection = _state_projection(row, shadow_trade)
            state_sha = _sha256(_canonical(state_projection))
            trade = state_projection["shadow_trade"]
            trade_sha = _sha256(_canonical(trade)) if isinstance(trade, dict) else None
            existing_state = store.state_binding(packet_id)
            if existing_state is None:
                state_record = {
                    "contract_version": DIAGNOSTIC_CONTRACT_VERSION,
                    "hypothesis_id": HYPOTHESIS_ID,
                    "packet_id": packet_id,
                    "shadow_state": shadow_state,
                    "canary_state": state_projection,
                    "canary_state_sha256": state_sha,
                    "shadow_trade_sha256": trade_sha,
                    "recorded_at_utc": _utc_text(now),
                }
                states_added += int(store.add_state_binding(state_record))
            elif str(existing_state["canary_state_sha256"]) != state_sha:
                unresolved_packets.add(packet_id)
                reconciliations += int(
                    store.add_reconciliation(
                        packet_id=packet_id,
                        category="canary_final_state_changed",
                        detail={
                            "bound_sha256": str(existing_state["canary_state_sha256"]),
                            "observed_sha256": state_sha,
                        },
                        now=now,
                    )
                )

    for missing_packet_id in sorted(bound_packet_ids - observed_packet_ids):
        unresolved_packets.add(missing_packet_id)
        reconciliations += int(
            store.add_reconciliation(
                packet_id=missing_packet_id,
                category="bound_canary_candidate_missing",
                detail={"canary_ledger_path": str(config.canary_ledger_db.resolve())},
                now=now,
            )
        )

    store.validate_integrity()
    result = DiagnosticRunResult(
        "degraded" if unresolved_packets else "collecting",
        candidates_seen=len(rows),
        candidates_added=added,
        evidence_bindings_added=evidence_added,
        state_bindings_added=states_added,
        bar_requests_ensured=requests,
        reconciliations_added=reconciliations,
        unresolved_candidates=len(unresolved_packets),
        error="diagnostic_candidates_unresolved" if unresolved_packets else None,
    )
    store.write_health(now=now, result=result)
    return result


def diagnostic_status(path: Path | str) -> dict[str, object]:
    selected = Path(path)
    if not selected.is_file():
        return {
            "path": str(selected),
            "exists": False,
            "integrity_status": "missing",
            "operational_status": "missing",
            "candidates": 0,
            "evidence_bindings": 0,
            "state_bindings": 0,
            "reconciliations": 0,
            "outcomes": 0,
            "outcome_receipts": 0,
            "outcome_dispositions": {},
            "health": None,
            "outcome_health": None,
        }
    return DiagnosticStore(selected, initialize=False).status()
