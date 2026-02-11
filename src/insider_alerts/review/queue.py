from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from datetime import UTC, datetime

from insider_alerts.sec.models import FilingRef

VALID_DECISIONS = {"approve", "reject", "escalate", "deadletter"}


class DecisionValidationError(ValueError):
    """Raised when decision payload is invalid."""


def ensure_review_tables(db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
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
        conn.commit()


def packet_id_for_ref(ref: FilingRef) -> str:
    return f"{ref.accession_number}|{ref.cik}|{ref.form_type}"


def enqueue_review_packet(db_path: str, ref: FilingRef, packet: Mapping[str, object]) -> bool:
    ensure_review_tables(db_path)
    packet_id = packet_id_for_ref(ref)
    now = datetime.now(tz=UTC).isoformat()

    with sqlite3.connect(db_path) as conn:
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


def _validate_decision_payload(payload: Mapping[str, object]) -> None:
    required = {"packet_id", "decision", "analyst", "reason"}
    missing = sorted(required.difference(payload.keys()))
    if missing:
        raise DecisionValidationError(f"missing required keys: {', '.join(missing)}")

    decision = payload["decision"]
    if not isinstance(decision, str) or decision not in VALID_DECISIONS:
        raise DecisionValidationError(f"invalid decision: {decision}")


def apply_decision(db_path: str, payload: Mapping[str, object]) -> int:
    ensure_review_tables(db_path)
    _validate_decision_payload(payload)

    packet_id = str(payload["packet_id"])
    decision = str(payload["decision"])
    now = datetime.now(tz=UTC).isoformat()
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True)

    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            UPDATE review_packets
            SET status = ?, decision_json = ?, updated_at = ?
            WHERE packet_id = ? AND status = 'pending'
            """,
            (decision, encoded, now, packet_id),
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


def replay_deadletter(db_path: str, packet_id: str) -> int:
    ensure_review_tables(db_path)
    now = datetime.now(tz=UTC).isoformat()
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            UPDATE review_packets
            SET status = 'pending', decision_json = NULL, updated_at = ?
            WHERE packet_id = ? AND status = 'deadletter'
            """,
            (now, packet_id),
        )
        conn.commit()
    return int(cursor.rowcount)
