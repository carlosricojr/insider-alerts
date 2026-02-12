from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Mapping
from datetime import UTC, datetime

from insider_alerts.sec.models import FilingRef

VALID_DECISIONS = {"approve", "reject", "escalate", "deadletter"}
PACKET_ID_RE = re.compile(r"^\d{10}-\d{2}-\d{6}\|\d{10}\|4(?:/A)?$")


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
            ORDER BY created_at DESC
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
            SET status = 'pending', decision_json = NULL, updated_at = ?
            WHERE packet_id = ? AND status = 'deadletter'
            """,
            (now, packet_id),
        )
        conn.commit()
    return int(cursor.rowcount)
