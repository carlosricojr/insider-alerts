"""Append-only, capture-only journal of ntfy server transport attempts."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat
from collections.abc import Mapping
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import rfc8785

from insider_alerts.notify.ntfy import NtfyTransportEvent

JOURNAL_VERSION = "ntfy-transport-journal-v1"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_PROVIDER_ID = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
_PACKET_ID = re.compile(r"^[A-Za-z0-9|_.:-]{1,256}$")
_EXCEPTION_CLASS = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]{0,127}$")
_PHASES = {"request_started", "response_received", "transport_failed"}
_EXPECTED_POLICY: dict[str, Any] = {
    "schema_version": 1,
    "contract_id": JOURNAL_VERSION,
    "purpose": "capture_only_future_research",
    "current_trial_decision_use": "prohibited",
    "activation_boundary": "request_started_at_utc_gte_journal_activation",
    "semantics": {
        "request_started": "local_http_attempt_start",
        "response_received": "ntfy_server_http_response_not_device_delivery",
        "transport_failed": "local_transport_exception_without_message",
        "client_received": "unavailable_without_subscriber_instrumentation",
        "unmatched_start": "outcome_unknown_after_crash_or_observer_failure",
    },
    "delivery_behavior": {
        "adds_retries": False,
        "changes_headers": False,
        "changes_payload": False,
        "changes_provider_idempotency": False,
    },
    "stored_fields": [
        "packet_id",
        "transport_id",
        "attempt_number",
        "phase",
        "occurred_at_utc",
        "request_body_sha256",
        "route_sha256",
        "http_status",
        "response_body_sha256",
        "provider_message_id",
        "provider_message_time",
        "exception_class",
        "runtime_git_commit",
        "policy_sha256",
    ],
    "prohibited_plaintext": [
        "url",
        "topic",
        "authorization",
        "token",
        "message_body",
        "title",
        "tags",
        "exception_message",
        "raw_response",
    ],
    "isolation": {
        "modifies_active_evidence_snapshot": False,
        "reads_trial_outcomes": False,
        "affects_enrollment": False,
        "affects_live_orders": False,
        "blocks_notification_on_capture_failure": False,
    },
}


class NotificationJournalError(RuntimeError):
    """The notification transport journal cannot prove a safe append."""


class NotificationJournalNotActive(NotificationJournalError):
    """The journal has not been explicitly activated."""


@dataclass(frozen=True, slots=True)
class NotificationJournalConfig:
    database: Path
    research_root: Path
    policy_path: Path
    runtime_git_commit: str
    write_timeout_ms: int = 100

    def __post_init__(self) -> None:
        if len(self.runtime_git_commit) != 40 or any(
            char not in "0123456789abcdef" for char in self.runtime_git_commit
        ):
            raise ValueError("runtime_git_commit must be a lowercase 40-character SHA-1")
        if not 1 <= self.write_timeout_ms <= 1_000:
            raise ValueError("write_timeout_ms must be between 1 and 1000")


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("notification journal timestamp cannot be naive")
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"notification journal timestamp is naive: {value}")
    return parsed.astimezone(UTC)


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical(value: Mapping[str, Any]) -> bytes:
    return rfc8785.dumps(dict(value))


def _is_reparse_point(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except OSError:
        return False
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    attributes = getattr(metadata, "st_file_attributes", 0)
    return path.is_symlink() or bool(reparse_flag and attributes & reparse_flag)


def _confined_database(config: NotificationJournalConfig) -> Path:
    root = Path(os.path.abspath(config.research_root))
    database = Path(os.path.abspath(config.database))
    if database == root or not database.is_relative_to(root):
        raise NotificationJournalError("notification journal escaped data/research")
    if not root.is_dir() or _is_reparse_point(root):
        raise NotificationJournalError("data/research is unavailable or a reparse point")
    cursor = root
    for part in database.relative_to(root).parts[:-1]:
        cursor /= part
        if cursor.exists() and _is_reparse_point(cursor):
            raise NotificationJournalError("notification journal parent is a reparse point")
    if database.exists() and _is_reparse_point(database):
        raise NotificationJournalError("notification journal is a reparse point")
    return database


def _load_policy(path: Path) -> tuple[dict[str, Any], bytes, str]:
    raw = path.read_bytes()
    try:
        policy = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NotificationJournalError("notification policy is not valid UTF-8 JSON") from exc
    if not isinstance(policy, dict):
        raise NotificationJournalError("notification policy must be an object")
    if policy != _EXPECTED_POLICY:
        raise NotificationJournalError("notification policy does not match the reviewed contract")
    return policy, raw, _sha256(_canonical(policy))


def _connect(path: Path, *, timeout_ms: int, initialize: bool = False) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=timeout_ms / 1000)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout={timeout_ms}")
    conn.execute("PRAGMA synchronous=FULL")
    if initialize:
        conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _validate_event(
    *, packet_id: str, transport_id: str, event: NtfyTransportEvent
) -> None:
    if not _PACKET_ID.fullmatch(packet_id):
        raise NotificationJournalError("packet_id is invalid")
    if not _SHA256.fullmatch(transport_id):
        raise NotificationJournalError("transport_id must be a SHA-256")
    if (
        event.phase not in _PHASES
        or isinstance(event.attempt_number, bool)
        or event.attempt_number < 1
    ):
        raise NotificationJournalError("notification transport event identity is invalid")
    _utc_text(event.occurred_at_utc)
    if not _SHA256.fullmatch(event.request_body_sha256) or not _SHA256.fullmatch(
        event.route_sha256
    ):
        raise NotificationJournalError("notification request digest is invalid")
    if event.response_body_sha256 is not None and not _SHA256.fullmatch(
        event.response_body_sha256
    ):
        raise NotificationJournalError("notification response digest is invalid")
    if event.provider_message_id is not None and not _PROVIDER_ID.fullmatch(
        event.provider_message_id
    ):
        raise NotificationJournalError("provider message id is invalid")
    if event.provider_message_time is not None and (
        isinstance(event.provider_message_time, bool) or event.provider_message_time < 0
    ):
        raise NotificationJournalError("provider message time is invalid")
    if event.http_status is not None and (
        isinstance(event.http_status, bool) or not 100 <= event.http_status <= 599
    ):
        raise NotificationJournalError("HTTP status is invalid")
    if event.exception_class is not None and not _EXCEPTION_CLASS.fullmatch(
        event.exception_class
    ):
        raise NotificationJournalError("exception class is invalid")
    NotificationTransportJournal._validate_phase(event)


def _ensure_schema(config: NotificationJournalConfig) -> None:
    database = _confined_database(config)
    with closing(_connect(database, timeout_ms=30_000, initialize=True)) as conn, conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS notification_journal_configuration (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                activated_at_utc TEXT NOT NULL,
                policy_sha256 TEXT NOT NULL,
                policy_bytes_sha256 TEXT NOT NULL,
                initial_runtime_git_commit TEXT NOT NULL,
                record_sha256 TEXT NOT NULL UNIQUE,
                record_json BLOB NOT NULL
            );
            CREATE TRIGGER IF NOT EXISTS notification_journal_configuration_no_update
            BEFORE UPDATE ON notification_journal_configuration
            BEGIN SELECT RAISE(ABORT, 'notification journal configuration is immutable'); END;
            CREATE TRIGGER IF NOT EXISTS notification_journal_configuration_no_delete
            BEFORE DELETE ON notification_journal_configuration
            BEGIN SELECT RAISE(ABORT, 'notification journal configuration is immutable'); END;

            CREATE TABLE IF NOT EXISTS notification_transport_events (
                sequence INTEGER PRIMARY KEY,
                event_id TEXT NOT NULL UNIQUE,
                packet_id TEXT NOT NULL,
                transport_id TEXT NOT NULL,
                attempt_number INTEGER NOT NULL CHECK(attempt_number>0),
                phase TEXT NOT NULL
                  CHECK(phase IN ('request_started','response_received','transport_failed')),
                occurred_at_utc TEXT NOT NULL,
                record_sha256 TEXT NOT NULL UNIQUE,
                record_json BLOB NOT NULL,
                UNIQUE(transport_id,attempt_number,phase)
            );
            CREATE INDEX IF NOT EXISTS notification_transport_events_attempt
            ON notification_transport_events(transport_id,attempt_number,sequence);
            CREATE TRIGGER IF NOT EXISTS notification_transport_events_sequence
            BEFORE INSERT ON notification_transport_events
            WHEN NEW.sequence<>(
              SELECT COALESCE(MAX(sequence),0)+1 FROM notification_transport_events
            )
            BEGIN SELECT RAISE(ABORT, 'notification event sequence must be gap-free'); END;
            CREATE TRIGGER IF NOT EXISTS notification_transport_events_one_terminal
            BEFORE INSERT ON notification_transport_events
            WHEN NEW.phase IN ('response_received','transport_failed') AND EXISTS(
              SELECT 1 FROM notification_transport_events
              WHERE transport_id=NEW.transport_id AND attempt_number=NEW.attempt_number
                AND phase IN ('response_received','transport_failed')
            )
            BEGIN SELECT RAISE(ABORT, 'notification attempt already has a terminal event'); END;
            CREATE TRIGGER IF NOT EXISTS notification_transport_events_terminal_requires_start
            BEFORE INSERT ON notification_transport_events
            WHEN NEW.phase IN ('response_received','transport_failed') AND NOT EXISTS(
              SELECT 1 FROM notification_transport_events
              WHERE transport_id=NEW.transport_id AND attempt_number=NEW.attempt_number
                AND phase='request_started'
            )
            BEGIN SELECT RAISE(ABORT, 'notification terminal event requires a start'); END;
            CREATE TRIGGER IF NOT EXISTS notification_transport_events_no_update
            BEFORE UPDATE ON notification_transport_events
            BEGIN SELECT RAISE(ABORT, 'notification transport events are immutable'); END;
            CREATE TRIGGER IF NOT EXISTS notification_transport_events_no_delete
            BEFORE DELETE ON notification_transport_events
            BEGIN SELECT RAISE(ABORT, 'notification transport events are immutable'); END;

            CREATE TABLE IF NOT EXISTS notification_journal_health (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                last_event_at_utc TEXT NOT NULL,
                last_packet_id TEXT NOT NULL,
                last_phase TEXT NOT NULL,
                last_error TEXT
            );
            """
        )


def activate_notification_journal(
    config: NotificationJournalConfig, *, activated_at_utc: datetime
) -> dict[str, str]:
    """Seal the one-time capture boundary without sending a notification."""

    database = _confined_database(config)
    _ensure_schema(config)
    _, policy_bytes, policy_sha = _load_policy(config.policy_path)
    activation = _utc_text(activated_at_utc)
    record = {
        "contract_version": JOURNAL_VERSION,
        "activated_at_utc": activation,
        "policy_sha256": policy_sha,
        "policy_bytes_sha256": _sha256(policy_bytes),
        "initial_runtime_git_commit": config.runtime_git_commit,
    }
    encoded = _canonical(record)
    digest = _sha256(encoded)
    with closing(_connect(database, timeout_ms=30_000)) as conn, conn:
        existing = conn.execute(
            "SELECT * FROM notification_journal_configuration WHERE singleton=1"
        ).fetchone()
        if existing is None:
            conn.execute(
                "INSERT INTO notification_journal_configuration VALUES(1,?,?,?,?,?,?)",
                (
                    activation,
                    policy_sha,
                    _sha256(policy_bytes),
                    config.runtime_git_commit,
                    digest,
                    encoded,
                ),
            )
        else:
            persisted = bytes(existing["record_json"])
            if (
                str(existing["activated_at_utc"]) != activation
                or str(existing["policy_sha256"]) != policy_sha
                or str(existing["policy_bytes_sha256"]) != _sha256(policy_bytes)
                or str(existing["initial_runtime_git_commit"])
                != config.runtime_git_commit
                or str(existing["record_sha256"]) != digest
                or persisted != encoded
            ):
                raise NotificationJournalError("notification journal activation is immutable")
    return {
        "activated_at_utc": activation,
        "policy_sha256": policy_sha,
        "contract_version": JOURNAL_VERSION,
    }


class NotificationTransportJournal:
    def __init__(self, config: NotificationJournalConfig) -> None:
        self.config = config
        _confined_database(config)

    def append(
        self,
        *,
        packet_id: str,
        transport_id: str,
        event: NtfyTransportEvent,
    ) -> bool:
        _validate_event(packet_id=packet_id, transport_id=transport_id, event=event)
        _, policy_bytes, policy_sha = _load_policy(self.config.policy_path)
        database = _confined_database(self.config)
        if not database.is_file():
            raise NotificationJournalNotActive("notification journal is not activated")
        with closing(
            _connect(database, timeout_ms=self.config.write_timeout_ms)
        ) as conn:
            conn.execute("BEGIN IMMEDIATE")
            configuration = conn.execute(
                "SELECT * FROM notification_journal_configuration WHERE singleton=1"
            ).fetchone()
            if configuration is None:
                raise NotificationJournalNotActive("notification journal is not activated")
            if (
                str(configuration["policy_sha256"]) != policy_sha
                or str(configuration["policy_bytes_sha256"]) != _sha256(policy_bytes)
            ):
                raise NotificationJournalError("notification journal policy changed")
            occurred = _utc_text(event.occurred_at_utc)
            if event.occurred_at_utc.astimezone(UTC) < _parse_utc(
                str(configuration["activated_at_utc"])
            ):
                conn.rollback()
                return False
            event_id = _sha256(
                f"{transport_id}|{event.attempt_number}|{event.phase}".encode()
            )
            record = {
                "schema_version": 1,
                "contract_version": JOURNAL_VERSION,
                "event_id": event_id,
                "packet_id": packet_id,
                "transport_id": transport_id,
                "attempt_number": event.attempt_number,
                "phase": event.phase,
                "occurred_at_utc": occurred,
                "request_body_sha256": event.request_body_sha256,
                "route_sha256": event.route_sha256,
                "http_status": event.http_status,
                "response_body_sha256": event.response_body_sha256,
                "provider_message_id": event.provider_message_id,
                "provider_message_time": event.provider_message_time,
                "exception_class": event.exception_class,
                "runtime_git_commit": self.config.runtime_git_commit,
                "policy_sha256": policy_sha,
            }
            encoded = _canonical(record)
            digest = _sha256(encoded)
            existing = conn.execute(
                "SELECT record_sha256 FROM notification_transport_events WHERE event_id=?",
                (event_id,),
            ).fetchone()
            if existing is not None:
                if str(existing["record_sha256"]) != digest:
                    raise NotificationJournalError("notification event identity collision")
                conn.rollback()
                return False
            sequence = int(
                conn.execute(
                    "SELECT COALESCE(MAX(sequence),0)+1 FROM notification_transport_events"
                ).fetchone()[0]
            )
            conn.execute(
                "INSERT INTO notification_transport_events VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    sequence,
                    event_id,
                    packet_id,
                    transport_id,
                    event.attempt_number,
                    event.phase,
                    occurred,
                    digest,
                    encoded,
                ),
            )
            conn.execute(
                """
                INSERT INTO notification_journal_health VALUES(1,?,?,?,NULL)
                ON CONFLICT(singleton) DO UPDATE SET
                  last_event_at_utc=excluded.last_event_at_utc,
                  last_packet_id=excluded.last_packet_id,
                  last_phase=excluded.last_phase,
                  last_error=NULL
                """,
                (occurred, packet_id, event.phase),
            )
            conn.commit()
        return True

    @staticmethod
    def _validate_phase(event: NtfyTransportEvent) -> None:
        if event.phase == "request_started":
            optional = (
                event.http_status,
                event.response_body_sha256,
                event.provider_message_id,
                event.provider_message_time,
                event.exception_class,
            )
            if any(value is not None for value in optional):
                raise NotificationJournalError("request start contains terminal fields")
        elif event.phase == "response_received":
            if (
                event.http_status is None
                or event.response_body_sha256 is None
                or event.exception_class is not None
            ):
                raise NotificationJournalError("response event fields are incomplete")
        elif (
            event.exception_class is None
            or event.http_status is not None
            or event.response_body_sha256 is not None
            or event.provider_message_id is not None
            or event.provider_message_time is not None
        ):
            raise NotificationJournalError("transport failure fields are invalid")


def notification_transport_id(packet_id: str, dispatch_nonce: str) -> str:
    """Bind one notification invocation while grouping all of its retry attempts."""

    if not packet_id or not dispatch_nonce:
        raise ValueError("packet_id and dispatch_nonce are required")
    return _sha256(f"{JOURNAL_VERSION}|{packet_id}|{dispatch_nonce}".encode())


def notification_journal_status(
    config: NotificationJournalConfig,
) -> dict[str, Any]:
    database = _confined_database(config)
    if not database.is_file():
        return {"valid": False, "reason": "notification_journal_missing"}
    integrity_errors: list[str] = []
    with closing(_connect(database, timeout_ms=30_000)) as conn:
        conn.execute("BEGIN")
        integrity_result = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
        if integrity_result != "ok":
            return {
                "valid": False,
                "reason": "notification_journal_integrity_check_failed",
                "integrity_errors": [integrity_result],
            }
        objects = {
            (str(row[0]), str(row[1]))
            for row in conn.execute(
                "SELECT type,name FROM sqlite_master WHERE type IN ('table','trigger')"
            )
        }
        required_objects = {
            ("table", "notification_journal_configuration"),
            ("table", "notification_transport_events"),
            ("table", "notification_journal_health"),
            ("trigger", "notification_journal_configuration_no_update"),
            ("trigger", "notification_journal_configuration_no_delete"),
            ("trigger", "notification_transport_events_sequence"),
            ("trigger", "notification_transport_events_one_terminal"),
            ("trigger", "notification_transport_events_terminal_requires_start"),
            ("trigger", "notification_transport_events_no_update"),
            ("trigger", "notification_transport_events_no_delete"),
        }
        missing_objects = sorted(required_objects - objects)
        if missing_objects:
            return {
                "valid": False,
                "reason": "notification_journal_schema_incomplete",
                "integrity_errors": [
                    f"missing_{object_type}:{name}"
                    for object_type, name in missing_objects
                ],
            }
        schema = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='notification_journal_configuration'"
        ).fetchone()
        if schema is None:
            return {"valid": False, "reason": "notification_journal_uninitialized"}
        configuration = conn.execute(
            "SELECT * FROM notification_journal_configuration WHERE singleton=1"
        ).fetchone()
        rows = conn.execute(
            "SELECT * FROM notification_transport_events ORDER BY sequence"
        ).fetchall()
        health = conn.execute(
            "SELECT * FROM notification_journal_health WHERE singleton=1"
        ).fetchone()
    if configuration is None:
        integrity_errors.append("activation_missing")
        activation = None
    else:
        activation = str(configuration["activated_at_utc"])
        config_bytes = bytes(configuration["record_json"])
        if _sha256(config_bytes) != str(configuration["record_sha256"]):
            integrity_errors.append("activation_digest_mismatch")
        try:
            config_record = json.loads(config_bytes)
        except json.JSONDecodeError:
            integrity_errors.append("activation_invalid_json")
        else:
            if not isinstance(config_record, dict):
                integrity_errors.append("activation_not_object")
            else:
                expected_config_keys = {
                    "contract_version",
                    "activated_at_utc",
                    "policy_sha256",
                    "policy_bytes_sha256",
                    "initial_runtime_git_commit",
                }
                if set(config_record) != expected_config_keys:
                    integrity_errors.append("activation_fields_invalid")
                try:
                    config_is_canonical = _canonical(config_record) == config_bytes
                except (TypeError, ValueError):
                    config_is_canonical = False
                if not config_is_canonical:
                    integrity_errors.append("activation_not_canonical")
                if any(
                    config_record.get(field) != configuration[field]
                    for field in (
                        "activated_at_utc",
                        "policy_sha256",
                        "policy_bytes_sha256",
                        "initial_runtime_git_commit",
                    )
                ):
                    integrity_errors.append("activation_row_mismatch")
                if (
                    config_record.get("contract_version") != JOURNAL_VERSION
                    or not _SHA256.fullmatch(str(config_record.get("policy_sha256")))
                    or not _SHA256.fullmatch(
                        str(config_record.get("policy_bytes_sha256"))
                    )
                    or not re.fullmatch(
                        r"[0-9a-f]{40}",
                        str(config_record.get("initial_runtime_git_commit")),
                    )
                ):
                    integrity_errors.append("activation_provenance_invalid")
                try:
                    _parse_utc(str(config_record.get("activated_at_utc")))
                except ValueError:
                    integrity_errors.append("activation_timestamp_invalid")
    try:
        _, current_policy_bytes, current_policy_sha = _load_policy(config.policy_path)
    except (OSError, NotificationJournalError):
        integrity_errors.append("policy_unavailable_or_invalid")
        current_policy_sha = None
        current_policy_bytes = b""
    if configuration is not None and (
        current_policy_sha != str(configuration["policy_sha256"])
        or _sha256(current_policy_bytes) != str(configuration["policy_bytes_sha256"])
    ):
        integrity_errors.append("policy_digest_mismatch")
    starts: dict[tuple[str, int], datetime] = {}
    terminals: dict[tuple[str, int], datetime] = {}
    phases: dict[str, int] = {phase: 0 for phase in sorted(_PHASES)}
    for expected_sequence, row in enumerate(rows, start=1):
        if int(row["sequence"]) != expected_sequence:
            integrity_errors.append("event_sequence_gap")
        encoded = bytes(row["record_json"])
        digest = str(row["record_sha256"])
        if _sha256(encoded) != digest:
            integrity_errors.append(f"event_digest_mismatch:{row['event_id']}")
            continue
        try:
            record = json.loads(encoded)
        except json.JSONDecodeError:
            integrity_errors.append(f"event_invalid_json:{row['event_id']}")
            continue
        if not isinstance(record, dict):
            integrity_errors.append(f"event_not_object:{row['event_id']}")
            continue
        expected_keys = {
            "schema_version",
            "contract_version",
            "event_id",
            "packet_id",
            "transport_id",
            "attempt_number",
            "phase",
            "occurred_at_utc",
            "request_body_sha256",
            "route_sha256",
            "http_status",
            "response_body_sha256",
            "provider_message_id",
            "provider_message_time",
            "exception_class",
            "runtime_git_commit",
            "policy_sha256",
        }
        if set(record) != expected_keys:
            integrity_errors.append(f"event_fields_invalid:{row['event_id']}")
            continue
        try:
            event_is_canonical = _canonical(record) == encoded
        except (TypeError, ValueError):
            event_is_canonical = False
        if not event_is_canonical:
            integrity_errors.append(f"event_not_canonical:{row['event_id']}")
            continue
        row_bindings = {
            "event_id": str(row["event_id"]),
            "packet_id": str(row["packet_id"]),
            "transport_id": str(row["transport_id"]),
            "attempt_number": int(row["attempt_number"]),
            "phase": str(row["phase"]),
            "occurred_at_utc": str(row["occurred_at_utc"]),
        }
        if any(record.get(field) != value for field, value in row_bindings.items()):
            integrity_errors.append(f"event_row_mismatch:{row['event_id']}")
            continue
        if (
            record.get("contract_version") != JOURNAL_VERSION
            or record.get("schema_version") != 1
            or not isinstance(record.get("runtime_git_commit"), str)
            or not re.fullmatch(r"[0-9a-f]{40}", str(record.get("runtime_git_commit")))
            or record.get("policy_sha256")
            != (str(configuration["policy_sha256"]) if configuration is not None else None)
        ):
            integrity_errors.append(f"event_provenance_invalid:{row['event_id']}")
            continue
        expected_event_id = _sha256(
            f"{record['transport_id']}|{record['attempt_number']}|{record['phase']}".encode()
        )
        if record["event_id"] != expected_event_id:
            integrity_errors.append(f"event_identity_invalid:{row['event_id']}")
            continue
        try:
            validated_event = NtfyTransportEvent(
                attempt_number=record["attempt_number"],
                phase=record["phase"],
                occurred_at_utc=_parse_utc(record["occurred_at_utc"]),
                request_body_sha256=record["request_body_sha256"],
                route_sha256=record["route_sha256"],
                http_status=record["http_status"],
                response_body_sha256=record["response_body_sha256"],
                provider_message_id=record["provider_message_id"],
                provider_message_time=record["provider_message_time"],
                exception_class=record["exception_class"],
            )
            _validate_event(
                packet_id=record["packet_id"],
                transport_id=record["transport_id"],
                event=validated_event,
            )
        except (AttributeError, TypeError, ValueError, NotificationJournalError):
            integrity_errors.append(f"event_semantics_invalid:{row['event_id']}")
            continue
        phase = str(row["phase"])
        phases[phase] = phases.get(phase, 0) + 1
        key = (str(row["transport_id"]), int(row["attempt_number"]))
        occurred = _parse_utc(str(row["occurred_at_utc"]))
        if phase == "request_started":
            starts[key] = occurred
        else:
            if key in terminals:
                integrity_errors.append(f"multiple_terminal_events:{row['event_id']}")
            terminals[key] = occurred
    for key, occurred in terminals.items():
        if key not in starts:
            integrity_errors.append(f"orphan_terminal:{key[0]}:{key[1]}")
        elif occurred < starts[key]:
            integrity_errors.append(f"terminal_before_start:{key[0]}:{key[1]}")
    unmatched = len(set(starts) - set(terminals))
    return {
        "valid": not integrity_errors,
        "activation_at_utc": activation,
        "events": len(rows),
        "phases": phases,
        "unmatched_starts": unmatched,
        "integrity_errors": integrity_errors,
        "health": dict(health) if health is not None else None,
    }
