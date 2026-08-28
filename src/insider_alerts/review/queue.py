from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime

from insider_alerts.sec.models import FilingRef

VALID_DECISIONS = {"approve", "reject", "escalate", "deadletter"}
PACKET_ID_RE = re.compile(r"^\d{10}-\d{2}-\d{6}\|\d{10}\|4(?:/A)?$")


class DecisionValidationError(ValueError):
    """Raised when decision payload is invalid."""


def ensure_review_tables(db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS review_packets (
                packet_id TEXT PRIMARY KEY,
                accession_number TEXT NOT NULL,
                cik TEXT NOT NULL,
                form_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                decision_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(review_packets)").fetchall()
        }
        if "notification_sent_at" not in columns:
            try:
                conn.execute("ALTER TABLE review_packets ADD COLUMN notification_sent_at TEXT")
            except sqlite3.OperationalError as exc:
                # A second service can finish this additive migration after our PRAGMA.
                if "duplicate column name" not in str(exc).lower():
                    raise
        if "notification_required" not in columns:
            try:
                conn.execute(
                    "ALTER TABLE review_packets ADD COLUMN notification_required "
                    "INTEGER NOT NULL DEFAULT 0 CHECK(notification_required IN (0,1))"
                )
            except sqlite3.OperationalError as exc:
                if "duplicate column name" not in str(exc).lower():
                    raise
        if "notification_suppressed_at" not in columns:
            try:
                conn.execute(
                    "ALTER TABLE review_packets ADD COLUMN notification_suppressed_at TEXT"
                )
            except sqlite3.OperationalError as exc:
                if "duplicate column name" not in str(exc).lower():
                    raise
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS deadletter_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                packet_id TEXT NOT NULL,
                reason TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_review_packets_accession_form_type
            ON review_packets (accession_number, form_type)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_review_packets_status_created_at
            ON review_packets (status, created_at DESC)
            """
        )
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS research_capture_jobs (
                job_id TEXT PRIMARY KEY,
                packet_id TEXT NOT NULL,
                contract_version TEXT NOT NULL,
                accession_number TEXT NOT NULL,
                issuer_cik TEXT NOT NULL,
                form_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                decision_json TEXT NOT NULL,
                source_first_observed_at_utc TEXT NOT NULL,
                decision_at_utc TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending'
                    CHECK(status IN ('pending','leased','retry','complete','failed')),
                attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
                lease_owner TEXT,
                lease_expires_at_utc TEXT,
                last_error_kind TEXT,
                last_error_message TEXT,
                record_sha256 TEXT,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL,
                UNIQUE(packet_id, contract_version)
            );
            CREATE INDEX IF NOT EXISTS idx_research_capture_jobs_claim
                ON research_capture_jobs(status, decision_at_utc, job_id);
            CREATE TABLE IF NOT EXISTS research_capture_attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                attempt_number INTEGER NOT NULL CHECK(attempt_number > 0),
                started_at_utc TEXT NOT NULL,
                finished_at_utc TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('retry','completed','failed')),
                error_kind TEXT,
                error_message TEXT,
                retryable INTEGER NOT NULL CHECK(retryable IN (0,1)),
                UNIQUE(job_id, attempt_number)
            );
            CREATE TRIGGER IF NOT EXISTS research_capture_attempts_no_update
            BEFORE UPDATE ON research_capture_attempts
            BEGIN SELECT RAISE(ABORT, 'capture attempts are append-only'); END;
            CREATE TRIGGER IF NOT EXISTS research_capture_attempts_no_delete
            BEFORE DELETE ON research_capture_attempts
            BEGIN SELECT RAISE(ABORT, 'capture attempts are append-only'); END;
            CREATE TRIGGER IF NOT EXISTS research_capture_complete_immutable
            BEFORE UPDATE ON research_capture_jobs
            WHEN OLD.status IN ('complete','failed')
            BEGIN SELECT RAISE(ABORT, 'final capture jobs are immutable'); END;
            CREATE TRIGGER IF NOT EXISTS research_capture_job_identity_immutable
            BEFORE UPDATE OF
                job_id, packet_id, contract_version, accession_number, issuer_cik, form_type,
                payload_json, decision_json, source_first_observed_at_utc, decision_at_utc,
                created_at_utc
            ON research_capture_jobs
            BEGIN SELECT RAISE(ABORT, 'capture job identity is immutable'); END;
            CREATE TRIGGER IF NOT EXISTS research_capture_jobs_no_delete
            BEFORE DELETE ON research_capture_jobs
            BEGIN SELECT RAISE(ABORT, 'capture jobs cannot be deleted'); END;
            CREATE TRIGGER IF NOT EXISTS enqueue_research_capture_after_approval
            AFTER UPDATE OF status ON review_packets
            WHEN NEW.status = 'approve' AND OLD.status <> 'approve'
            BEGIN
                INSERT OR IGNORE INTO research_capture_jobs(
                    job_id, packet_id, contract_version, accession_number, issuer_cik,
                    form_type, payload_json, decision_json, source_first_observed_at_utc,
                    decision_at_utc, created_at_utc, updated_at_utc
                ) VALUES(
                    NEW.packet_id || '|insider-evidence-capture-v1',
                    NEW.packet_id, 'insider-evidence-capture-v1', NEW.accession_number,
                    NEW.cik, NEW.form_type, NEW.payload_json, NEW.decision_json,
                    NEW.created_at, NEW.updated_at, NEW.updated_at, NEW.updated_at
                );
            END;
            """
        )
        conn.commit()


def packet_id_for_ref(ref: FilingRef) -> str:
    return f"{ref.accession_number}|{ref.cik}|{ref.form_type}"


def enqueue_review_packet(db_path: str, ref: FilingRef, packet: Mapping[str, object]) -> bool:
    ensure_review_tables(db_path)
    packet_id = packet_id_for_ref(ref)
    now = datetime.now(tz=UTC).isoformat()

    with sqlite3.connect(db_path) as conn:
        existing = conn.execute(
            """
            SELECT packet_id
            FROM review_packets
            WHERE accession_number = ? AND form_type = ?
            LIMIT 1
            """,
            (ref.accession_number, ref.form_type),
        ).fetchone()
        if existing is not None:
            return False

        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO review_packets (
                packet_id, accession_number, cik, form_type, payload_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                packet_id,
                ref.accession_number,
                ref.cik,
                ref.form_type,
                json.dumps(packet, separators=(",", ":"), sort_keys=True),
                now,
                now,
            ),
        )
        conn.commit()
    return cursor.rowcount == 1


def enqueue_review_packets_batch(
    db_path: str,
    packets: Sequence[tuple[FilingRef, Mapping[str, object]]],
) -> int:
    ensure_review_tables(db_path)
    if not packets:
        return 0

    now = datetime.now(tz=UTC).isoformat()
    rows: list[tuple[str, str, str, str, str, str, str]] = []
    for ref, packet in packets:
        rows.append(
            (
                packet_id_for_ref(ref),
                ref.accession_number,
                ref.cik,
                ref.form_type,
                json.dumps(packet, separators=(",", ":"), sort_keys=True),
                now,
                now,
            )
        )

    with sqlite3.connect(db_path) as conn:
        before = conn.total_changes
        conn.execute("BEGIN IMMEDIATE")
        for row in rows:
            conn.execute(
                """
                INSERT INTO review_packets (
                    packet_id, accession_number, cik, form_type,
                    payload_json, created_at, updated_at
                )
                SELECT ?, ?, ?, ?, ?, ?, ?
                WHERE NOT EXISTS (
                    SELECT 1 FROM review_packets
                    WHERE accession_number = ? AND form_type = ?
                )
                """,
                (*row, row[1], row[3]),
            )
        conn.commit()
        inserted = conn.total_changes - before
    return int(inserted)


def _validate_decision_payload(payload: Mapping[str, object]) -> None:
    required = {"packet_id", "decision", "analyst", "reason"}
    missing = sorted(required.difference(payload.keys()))
    if missing:
        raise DecisionValidationError(f"missing required keys: {', '.join(missing)}")

    packet_id = payload["packet_id"]
    if not isinstance(packet_id, str) or PACKET_ID_RE.fullmatch(packet_id.strip()) is None:
        raise DecisionValidationError("invalid packet_id format")

    decision = payload["decision"]
    if not isinstance(decision, str) or decision not in VALID_DECISIONS:
        raise DecisionValidationError(f"invalid decision: {decision}")

    analyst = payload["analyst"]
    if not isinstance(analyst, str) or not analyst.strip():
        raise DecisionValidationError("invalid analyst")

    reason = payload["reason"]
    if not isinstance(reason, str) or not reason.strip():
        raise DecisionValidationError("invalid reason")


def apply_decision(
    db_path: str,
    payload: Mapping[str, object],
    *,
    notification_required: bool = False,
    notification_suppressed: bool = False,
) -> int:
    ensure_review_tables(db_path)
    _validate_decision_payload(payload)
    if notification_suppressed and not notification_required:
        raise ValueError("notification suppression requires a delivery intent")

    packet_id = str(payload["packet_id"])
    decision = str(payload["decision"])
    now = datetime.now(tz=UTC).isoformat()
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    suppressed_at = now if notification_suppressed else None

    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            UPDATE review_packets
            SET status = ?, decision_json = ?, updated_at = ?, notification_required = ?,
                notification_sent_at = NULL, notification_suppressed_at = ?
            WHERE packet_id = ? AND status = 'pending'
            """,
            (
                decision,
                encoded,
                now,
                int(notification_required),
                suppressed_at,
                packet_id,
            ),
        )

        if decision == "deadletter" and cursor.rowcount == 1:
            conn.execute(
                """
                INSERT INTO deadletter_events (packet_id, reason, payload_json, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (packet_id, str(payload["reason"]), encoded, now),
            )

        conn.commit()
    return int(cursor.rowcount)


def mark_notification_delivered(
    db_path: str,
    packet_id: str,
    decision: Mapping[str, object],
) -> int:
    """CAS-acknowledge the exact decision only after provider success."""

    ensure_review_tables(db_path)
    delivered_at = datetime.now(tz=UTC).isoformat()
    encoded = json.dumps(decision, separators=(",", ":"), sort_keys=True)
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            UPDATE review_packets
            SET notification_sent_at = ?
            WHERE packet_id = ? AND decision_json = ?
              AND notification_required = 1
              AND notification_sent_at IS NULL
              AND notification_suppressed_at IS NULL
            """,
            (delivered_at, packet_id, encoded),
        )
        conn.commit()
    return int(cursor.rowcount)


def mark_notification_suppressed(
    db_path: str,
    packet_id: str,
    decision: Mapping[str, object],
) -> int:
    """CAS-acknowledge a co-filing only after its economic event was delivered elsewhere."""

    ensure_review_tables(db_path)
    suppressed_at = datetime.now(tz=UTC).isoformat()
    encoded = json.dumps(decision, separators=(",", ":"), sort_keys=True)
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            UPDATE review_packets
            SET notification_suppressed_at = ?
            WHERE packet_id = ? AND decision_json = ?
              AND notification_required = 1
              AND notification_sent_at IS NULL
              AND notification_suppressed_at IS NULL
            """,
            (suppressed_at, packet_id, encoded),
        )
        conn.commit()
    return int(cursor.rowcount)


def list_notification_outbox(db_path: str, *, limit: int) -> list[dict[str, object]]:
    """Return decisions durably marked for delivery but not yet acknowledged locally."""

    ensure_review_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT packet_id, accession_number, cik, form_type, payload_json, status,
                   decision_json, created_at, updated_at
            FROM review_packets
            WHERE notification_required = 1
              AND notification_sent_at IS NULL
              AND notification_suppressed_at IS NULL
              AND decision_json IS NOT NULL
            ORDER BY updated_at ASC, packet_id ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [
        {
            "packet_id": str(row["packet_id"]),
            "accession_number": str(row["accession_number"]),
            "cik": str(row["cik"]),
            "form_type": str(row["form_type"]),
            "payload": json.loads(str(row["payload_json"])),
            "status": str(row["status"]),
            "decision": json.loads(str(row["decision_json"])),
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
        }
        for row in rows
    ]


def list_deadletters(db_path: str) -> list[dict[str, str]]:
    ensure_review_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT packet_id, reason, payload_json AS decision_json, created_at
            FROM deadletter_events
            ORDER BY id DESC
            """
        ).fetchall()
    return [dict(row) for row in rows]


def list_pending_review_packets(db_path: str, *, limit: int) -> list[dict[str, object]]:
    ensure_review_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT packet_id, accession_number, cik, form_type, payload_json, status,
                   created_at, updated_at
            FROM review_packets
            WHERE status = 'pending'
            -- Oldest first prevents retryable judge outages from stranding earlier signals once
            -- the pending queue grows beyond the caller's bounded decision limit.
            ORDER BY created_at ASC, packet_id ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    packets: list[dict[str, object]] = []
    for row in rows:
        packets.append(
            {
                "packet_id": str(row["packet_id"]),
                "accession_number": str(row["accession_number"]),
                "cik": str(row["cik"]),
                "form_type": str(row["form_type"]),
                "payload": json.loads(str(row["payload_json"])),
                "status": str(row["status"]),
                "created_at": str(row["created_at"]),
                "updated_at": str(row["updated_at"]),
            }
        )
    return packets


def get_review_packet(db_path: str, packet_id: str) -> dict[str, object] | None:
    ensure_review_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT packet_id, accession_number, cik, form_type, payload_json, status,
                   decision_json, created_at, updated_at
            FROM review_packets
            WHERE packet_id = ?
            LIMIT 1
            """,
            (packet_id,),
        ).fetchone()

    if row is None:
        return None

    decision_json = str(row["decision_json"]) if row["decision_json"] is not None else None
    return {
        "packet_id": str(row["packet_id"]),
        "accession_number": str(row["accession_number"]),
        "cik": str(row["cik"]),
        "form_type": str(row["form_type"]),
        "payload": json.loads(str(row["payload_json"])),
        "status": str(row["status"]),
        "decision_json": json.loads(decision_json) if decision_json else None,
        "created_at": str(row["created_at"]),
        "updated_at": str(row["updated_at"]),
    }


def replay_deadletter(db_path: str, packet_id: str) -> int:
    ensure_review_tables(db_path)
    now = datetime.now(tz=UTC).isoformat()
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            UPDATE review_packets
            SET status = 'pending', decision_json = NULL, updated_at = ?,
                notification_required = 0, notification_sent_at = NULL,
                notification_suppressed_at = NULL
            WHERE packet_id = ? AND status = 'deadletter'
            """,
            (now, packet_id),
        )
        conn.commit()
    return int(cursor.rowcount)
