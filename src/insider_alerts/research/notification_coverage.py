"""Blinded reconciliation of operational notification acknowledgements to transport custody."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

import rfc8785

from insider_alerts.research.notification_transport import (
    NotificationJournalConfig,
    notification_journal_status,
)
from insider_alerts.review.queue import (
    DELIVERY_ACK_VERSION,
    PACKET_ID_RE,
    NotificationDeliverySchemaError,
    validate_notification_delivery_schema,
)

COVERAGE_VERSION = "notification-coverage-v1"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_EXPECTED_POLICY: dict[str, Any] = {
    "schema_version": 1,
    "contract_id": COVERAGE_VERSION,
    "purpose": "operational_capture_completeness_only",
    "activation_boundary": "sealed_source_snapshot_and_delivery_ack_sequence_watermark",
    "source_snapshot_order": "source_fully_materialized_and_closed_before_journal_open",
    "baseline": {
        "membership": "visible_nonsuppressed_notification_sent_rows",
        "covered": "exact_atomic_acknowledgement_and_matching_request_2xx_attempt",
        "missingness": "immutable_and_never_backfilled",
        "historical_completeness_claim": "prohibited",
    },
    "post_activation": {
        "membership": "append_only_notification_delivery_ack_sequence_after_watermark",
        "identity": "exact_transport_id_and_attempt_number",
        "success": "matching_request_and_2xx_response_before_operational_acknowledgement",
        "observer_failure": "durable_ack_with_missing_transport_identity",
        "gap_observation_time": "after_snapshot_and_not_before_linked_delivery_evidence",
    },
    "failure_semantics": {
        "source_or_journal_unreadable": "degraded_not_missing",
        "rollback_or_path_drift": "degraded_not_missing",
        "deterministic_uncovered_acknowledgement": "append_only_gap",
        "accepted_baseline_missingness_affects_future_health": False,
    },
    "freshness_seconds": 180,
    "isolation": {
        "reads_trial_outcomes": False,
        "affects_trial_enrollment": False,
        "affects_live_orders": False,
        "affects_notification_delivery": False,
        "enters_active_evidence_snapshot": False,
        "supports_pre_activation_transport_claims": False,
    },
}


class NotificationCoverageError(RuntimeError):
    """Coverage cannot prove a structurally valid snapshot."""


class NotificationCoverageNotActive(NotificationCoverageError):
    """The post-fix coverage boundary has not been sealed."""


@dataclass(frozen=True, slots=True)
class NotificationCoverageConfig:
    source_db: Path
    source_root: Path
    coverage_db: Path
    research_root: Path
    policy_path: Path
    policy_root: Path
    journal: NotificationJournalConfig
    runtime_git_commit: str

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[0-9a-f]{40}", self.runtime_git_commit):
            raise ValueError("runtime_git_commit must be a lowercase SHA-1")


@dataclass(frozen=True, slots=True)
class _SourceItem:
    packet_id: str
    decision_sha256: str
    notification_sent_at_utc: str
    source_record_sha256: str


@dataclass(frozen=True, slots=True)
class _Ack:
    sequence: int
    ack_id: str
    packet_id: str
    decision_sha256: str
    notification_sent_at_utc: str
    transport_id: str | None
    attempt_number: int
    responded_at_utc: str
    request_body_sha256: str
    route_sha256: str
    http_status: int
    record_sha256: str


@dataclass(frozen=True, slots=True)
class _SourceSnapshot:
    items: tuple[_SourceItem, ...]
    acknowledgements: tuple[_Ack, ...]
    schema_sha256: str
    completed_at_utc: str


@dataclass(frozen=True, slots=True)
class _JournalSnapshot:
    events: tuple[dict[str, Any], ...]
    activation_record_sha256: str
    activation_at_utc: str
    snapshot_sha256: str
    max_sequence: int


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical(value: dict[str, Any]) -> bytes:
    return rfc8785.dumps(value)


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("coverage timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("coverage timestamp must include an offset")
    return parsed.astimezone(UTC)


def _is_reparse_point(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except OSError:
        return False
    flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    attributes = getattr(metadata, "st_file_attributes", 0)
    return path.is_symlink() or bool(flag and attributes & flag)


def _confined(path: Path, *, root: Path, kind: str) -> Path:
    confined_root = Path(os.path.abspath(root))
    target = Path(os.path.abspath(path))
    if target == confined_root or not target.is_relative_to(confined_root):
        raise NotificationCoverageError(f"{kind} escaped its trusted root")
    if not confined_root.is_dir() or _is_reparse_point(confined_root):
        raise NotificationCoverageError(f"{kind} trusted root is unavailable or unsafe")
    cursor = confined_root
    for part in target.relative_to(confined_root).parts:
        cursor /= part
        if _is_reparse_point(cursor):
            raise NotificationCoverageError(f"{kind} traverses a reparse point")
    return target


def _paths(config: NotificationCoverageConfig) -> tuple[Path, Path, Path, Path]:
    source = _confined(config.source_db, root=config.source_root, kind="coverage source")
    coverage = _confined(config.coverage_db, root=config.research_root, kind="coverage database")
    journal = _confined(
        config.journal.database, root=config.research_root, kind="transport journal"
    )
    policy = _confined(config.policy_path, root=config.policy_root, kind="coverage policy")
    return source, coverage, journal, policy


def _load_policy(config: NotificationCoverageConfig) -> tuple[bytes, str]:
    _, _, _, policy_path = _paths(config)
    try:
        raw = policy_path.read_bytes()
        policy = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NotificationCoverageError("coverage policy is unavailable or invalid") from exc
    if policy != _EXPECTED_POLICY:
        raise NotificationCoverageError("coverage policy does not match the reviewed contract")
    return raw, _sha256(_canonical(policy))


def _connect_readonly(path: Path, *, timeout_ms: int = 1_000) -> sqlite3.Connection:
    conn = sqlite3.connect(
        f"file:{path.resolve().as_posix()}?mode=ro",
        uri=True,
        timeout=timeout_ms / 1000,
    )
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout={timeout_ms}")
    conn.execute("PRAGMA query_only=ON")
    conn.execute("PRAGMA trusted_schema=OFF")
    return conn


def _connect_coverage(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=5)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA synchronous=FULL")
    return conn


def _create_coverage_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS notification_coverage_configuration (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                activated_at_utc TEXT NOT NULL,
                source_ack_sequence_watermark INTEGER NOT NULL
                  CHECK(source_ack_sequence_watermark>=0),
                journal_sequence_watermark INTEGER NOT NULL CHECK(journal_sequence_watermark>=0),
                baseline_sent_count INTEGER NOT NULL CHECK(baseline_sent_count>=0),
                baseline_covered_count INTEGER NOT NULL CHECK(baseline_covered_count>=0),
                baseline_missing_count INTEGER NOT NULL CHECK(baseline_missing_count>=0),
                policy_sha256 TEXT NOT NULL,
                source_schema_sha256 TEXT NOT NULL,
                initial_runtime_git_commit TEXT NOT NULL,
                record_sha256 TEXT NOT NULL UNIQUE,
                record_json BLOB NOT NULL
            );
            CREATE TRIGGER IF NOT EXISTS notification_coverage_configuration_no_update
            BEFORE UPDATE ON notification_coverage_configuration
            BEGIN SELECT RAISE(ABORT, 'notification coverage configuration is immutable'); END;
            CREATE TRIGGER IF NOT EXISTS notification_coverage_configuration_no_delete
            BEFORE DELETE ON notification_coverage_configuration
            BEGIN SELECT RAISE(ABORT, 'notification coverage configuration is immutable'); END;
            CREATE TABLE IF NOT EXISTS notification_coverage_baseline (
                sequence INTEGER PRIMARY KEY,
                source_record_sha256 TEXT NOT NULL UNIQUE,
                packet_id_sha256 TEXT NOT NULL,
                classification TEXT NOT NULL CHECK(classification IN ('covered','missing')),
                request_record_sha256 TEXT,
                response_record_sha256 TEXT,
                record_sha256 TEXT NOT NULL UNIQUE,
                record_json BLOB NOT NULL
            );
            CREATE TRIGGER IF NOT EXISTS notification_coverage_baseline_sequence
            BEFORE INSERT ON notification_coverage_baseline
            WHEN NEW.sequence<>(
              SELECT COALESCE(MAX(sequence),0)+1 FROM notification_coverage_baseline
            )
            BEGIN
              SELECT RAISE(ABORT, 'notification coverage baseline sequence must be gap-free');
            END;
            CREATE TRIGGER IF NOT EXISTS notification_coverage_baseline_no_update
            BEFORE UPDATE ON notification_coverage_baseline
            BEGIN SELECT RAISE(ABORT, 'notification coverage baseline is immutable'); END;
            CREATE TRIGGER IF NOT EXISTS notification_coverage_baseline_no_delete
            BEFORE DELETE ON notification_coverage_baseline
            BEGIN SELECT RAISE(ABORT, 'notification coverage baseline is immutable'); END;
            CREATE TABLE IF NOT EXISTS notification_coverage_gaps (
                sequence INTEGER PRIMARY KEY,
                gap_id TEXT NOT NULL UNIQUE,
                reason TEXT NOT NULL,
                source_record_sha256 TEXT NOT NULL,
                ack_id TEXT,
                evidence_not_before_at_utc TEXT NOT NULL,
                first_observed_at_utc TEXT NOT NULL,
                record_sha256 TEXT NOT NULL UNIQUE,
                record_json BLOB NOT NULL
            );
            CREATE TRIGGER IF NOT EXISTS notification_coverage_gaps_sequence
            BEFORE INSERT ON notification_coverage_gaps
            WHEN NEW.sequence<>(SELECT COALESCE(MAX(sequence),0)+1 FROM notification_coverage_gaps)
            BEGIN SELECT RAISE(ABORT, 'notification coverage gap sequence must be gap-free'); END;
            CREATE TRIGGER IF NOT EXISTS notification_coverage_gaps_no_update
            BEFORE UPDATE ON notification_coverage_gaps
            BEGIN SELECT RAISE(ABORT, 'notification coverage gaps are immutable'); END;
            CREATE TRIGGER IF NOT EXISTS notification_coverage_gaps_no_delete
            BEFORE DELETE ON notification_coverage_gaps
            BEGIN SELECT RAISE(ABORT, 'notification coverage gaps are immutable'); END;
            CREATE TABLE IF NOT EXISTS notification_coverage_health (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                last_started_at_utc TEXT NOT NULL,
                last_success_at_utc TEXT,
                last_error_kind TEXT,
                last_error_message TEXT,
                post_activation_ack_count INTEGER NOT NULL CHECK(post_activation_ack_count>=0),
                current_gap_count INTEGER NOT NULL CHECK(current_gap_count>=0),
                last_source_ack_sequence INTEGER NOT NULL CHECK(last_source_ack_sequence>=0),
                last_journal_sequence INTEGER NOT NULL CHECK(last_journal_sequence>=0),
                last_source_prefix_sha256 TEXT NOT NULL,
                last_journal_prefix_sha256 TEXT NOT NULL
            );
            """
    )


def _coverage_schema_definitions(conn: sqlite3.Connection) -> dict[tuple[str, str], str]:
    return {
        (str(row["type"]), str(row["name"])): " ".join(str(row["sql"]).split())
        for row in conn.execute(
            "SELECT type,name,sql FROM sqlite_master "
            "WHERE name LIKE 'notification_coverage_%' AND sql IS NOT NULL"
        ).fetchall()
    }


def _expected_coverage_schema() -> dict[tuple[str, str], str]:
    with sqlite3.connect(":memory:") as expected:
        expected.row_factory = sqlite3.Row
        _create_coverage_schema(expected)
        return _coverage_schema_definitions(expected)


_EXPECTED_COVERAGE_SCHEMA = _expected_coverage_schema()


def _ensure_store(path: Path) -> None:
    with closing(_connect_coverage(path)) as conn, conn:
        conn.execute("PRAGMA journal_mode=WAL")
        _create_coverage_schema(conn)
        mismatched = _coverage_schema_definition_mismatches(conn)
        if mismatched:
            raise NotificationCoverageError(
                f"notification coverage schema definition mismatch: {mismatched}"
            )


def _source_schema_descriptor(conn: sqlite3.Connection) -> dict[str, Any]:
    try:
        validate_notification_delivery_schema(conn)
    except NotificationDeliverySchemaError as exc:
        raise NotificationCoverageError(str(exc)) from exc
    required_objects = {
        ("table", "review_packets"),
        ("table", "notification_delivery_acks"),
        ("trigger", "notification_delivery_acks_sequence"),
        ("trigger", "notification_delivery_acks_no_update"),
        ("trigger", "notification_delivery_acks_no_delete"),
    }
    objects = {
        (str(row["type"]), str(row["name"])): str(row["sql"] or "")
        for row in conn.execute(
            "SELECT type,name,sql FROM sqlite_master WHERE type IN ('table','trigger')"
        ).fetchall()
    }
    missing = sorted(required_objects - set(objects))
    if missing:
        raise NotificationCoverageError(f"coverage source schema missing objects: {missing}")
    columns: dict[str, list[dict[str, Any]]] = {}
    for table in ("review_packets", "notification_delivery_acks"):
        columns[table] = [
            {
                "name": str(row["name"]),
                "type": str(row["type"]),
                "notnull": int(row["notnull"]),
                "default": row["dflt_value"],
                "pk": int(row["pk"]),
            }
            for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
        ]
    required_columns = {
        "review_packets": {
            "packet_id",
            "decision_json",
            "notification_required",
            "notification_sent_at",
            "notification_suppressed_at",
        },
        "notification_delivery_acks": {
            "sequence",
            "ack_id",
            "packet_id",
            "decision_sha256",
            "notification_sent_at_utc",
            "transport_id",
            "attempt_number",
            "responded_at_utc",
            "request_body_sha256",
            "route_sha256",
            "http_status",
            "record_sha256",
            "record_json",
        },
    }
    for table, expected in required_columns.items():
        actual = {str(column["name"]) for column in columns[table]}
        if not expected.issubset(actual):
            raise NotificationCoverageError(f"coverage source {table} columns are incomplete")
    return {
        "columns": columns,
        "objects": [
            {"type": kind, "name": name, "sql_sha256": _sha256(sql.encode())}
            for (kind, name), sql in sorted(objects.items())
            if (kind, name) in required_objects
        ],
    }


def _source_item(row: sqlite3.Row) -> _SourceItem:
    packet_id = str(row["packet_id"])
    if PACKET_ID_RE.fullmatch(packet_id) is None:
        raise NotificationCoverageError("coverage source packet identity is invalid")
    decision_json = str(row["decision_json"])
    decision_sha = _sha256(decision_json.encode("utf-8"))
    sent_at = _utc_text(_parse_utc(str(row["notification_sent_at"])))
    record = {
        "schema_version": 1,
        "packet_id": packet_id,
        "decision_sha256": decision_sha,
        "notification_sent_at_utc": sent_at,
        "notification_required": int(row["notification_required"]),
        "notification_suppressed_at": row["notification_suppressed_at"],
    }
    return _SourceItem(packet_id, decision_sha, sent_at, _sha256(_canonical(record)))


def _ack(row: sqlite3.Row, *, expected_sequence: int) -> _Ack:
    if int(row["sequence"]) != expected_sequence:
        raise NotificationCoverageError("notification delivery acknowledgement sequence is invalid")
    encoded = bytes(row["record_json"])
    record_sha = str(row["record_sha256"])
    if not _SHA256.fullmatch(record_sha) or _sha256(encoded) != record_sha:
        raise NotificationCoverageError("notification delivery acknowledgement digest is invalid")
    try:
        record = json.loads(encoded)
    except json.JSONDecodeError as exc:
        raise NotificationCoverageError(
            "notification delivery acknowledgement is invalid JSON"
        ) from exc
    expected_keys = {
        "schema_version",
        "contract_version",
        "packet_id",
        "decision_sha256",
        "notification_sent_at_utc",
        "transport_id",
        "attempt_number",
        "responded_at_utc",
        "request_body_sha256",
        "route_sha256",
        "http_status",
    }
    if (
        not isinstance(record, dict)
        or set(record) != expected_keys
        or _canonical(record) != encoded
    ):
        raise NotificationCoverageError("notification delivery acknowledgement envelope is invalid")
    bindings = {
        "packet_id": str(row["packet_id"]),
        "decision_sha256": str(row["decision_sha256"]),
        "notification_sent_at_utc": str(row["notification_sent_at_utc"]),
        "transport_id": row["transport_id"],
        "attempt_number": int(row["attempt_number"]),
        "responded_at_utc": str(row["responded_at_utc"]),
        "request_body_sha256": str(row["request_body_sha256"]),
        "route_sha256": str(row["route_sha256"]),
        "http_status": int(row["http_status"]),
    }
    if any(record.get(key) != value for key, value in bindings.items()):
        raise NotificationCoverageError("notification delivery acknowledgement row mismatch")
    packet_id = bindings["packet_id"]
    if (
        record.get("schema_version") != 1
        or record.get("contract_version") != DELIVERY_ACK_VERSION
        or not isinstance(packet_id, str)
        or PACKET_ID_RE.fullmatch(packet_id) is None
        or not _SHA256.fullmatch(str(bindings["decision_sha256"]))
        or not _SHA256.fullmatch(str(bindings["request_body_sha256"]))
        or not _SHA256.fullmatch(str(bindings["route_sha256"]))
        or (
            bindings["transport_id"] is not None
            and not _SHA256.fullmatch(str(bindings["transport_id"]))
        )
        or int(bindings["attempt_number"]) < 1
        or not 200 <= int(bindings["http_status"]) <= 299
    ):
        raise NotificationCoverageError(
            "notification delivery acknowledgement semantics are invalid"
        )
    sent_at = _utc_text(_parse_utc(str(bindings["notification_sent_at_utc"])))
    responded_at = _utc_text(_parse_utc(str(bindings["responded_at_utc"])))
    if _parse_utc(responded_at) > _parse_utc(sent_at):
        raise NotificationCoverageError("notification acknowledgement precedes provider response")
    ack_id = _sha256(
        f"{DELIVERY_ACK_VERSION}|{packet_id}|{bindings['decision_sha256']}|{sent_at}".encode()
    )
    if str(row["ack_id"]) != ack_id:
        raise NotificationCoverageError("notification delivery acknowledgement identity is invalid")
    return _Ack(
        expected_sequence,
        ack_id,
        packet_id,
        str(bindings["decision_sha256"]),
        sent_at,
        str(bindings["transport_id"]) if bindings["transport_id"] is not None else None,
        int(bindings["attempt_number"]),
        responded_at,
        str(bindings["request_body_sha256"]),
        str(bindings["route_sha256"]),
        int(bindings["http_status"]),
        record_sha,
    )


def _read_source(config: NotificationCoverageConfig, *, now_fn: Any) -> _SourceSnapshot:
    source, _, _, _ = _paths(config)
    if not source.is_file():
        raise NotificationCoverageError("coverage source database is missing")
    try:
        with closing(_connect_readonly(source)) as conn:
            conn.execute("BEGIN")
            descriptor = _source_schema_descriptor(conn)
            item_rows = conn.execute(
                """
                SELECT packet_id,decision_json,notification_required,
                       notification_sent_at,notification_suppressed_at
                FROM review_packets
                WHERE notification_required=1 AND notification_sent_at IS NOT NULL
                  AND notification_suppressed_at IS NULL
                ORDER BY packet_id,notification_sent_at
                """
            ).fetchall()
            ack_rows = conn.execute(
                "SELECT * FROM notification_delivery_acks ORDER BY sequence"
            ).fetchall()
    except sqlite3.Error as exc:
        raise NotificationCoverageError("coverage source snapshot is unavailable") from exc
    items = tuple(_source_item(row) for row in item_rows)
    acknowledgements = tuple(
        _ack(row, expected_sequence=sequence) for sequence, row in enumerate(ack_rows, start=1)
    )
    return _SourceSnapshot(
        items,
        acknowledgements,
        _sha256(_canonical(descriptor)),
        _utc_text(now_fn()),
    )


def _read_journal(config: NotificationCoverageConfig) -> _JournalSnapshot:
    report = notification_journal_status(config.journal, _include_validated_events=True)
    if not report.get("valid"):
        raise NotificationCoverageError("transport journal snapshot is invalid")
    raw_events = report.get("_validated_events")
    if not isinstance(raw_events, list):
        raise NotificationCoverageError("transport journal returned no validated events")
    events = tuple(dict(event) for event in raw_events)
    record_digests = [str(event["record_sha256"]) for event in events]
    return _JournalSnapshot(
        events=events,
        activation_record_sha256=str(report["_configuration_record_sha256"]),
        activation_at_utc=str(report["activation_at_utc"]),
        snapshot_sha256=_sha256(_canonical({"record_sha256": record_digests})),
        max_sequence=max((int(event["sequence"]) for event in events), default=0),
    )


def confined_notification_coverage_source(config: NotificationCoverageConfig) -> Path:
    """Resolve the configured source only after applying the monitor's confinement rules."""

    source, _, _, _ = _paths(config)
    if not source.is_file() or _is_reparse_point(source):
        raise NotificationCoverageError(
            "coverage source database must be an existing regular non-reparse file"
        )
    return source


def _attempts(journal: _JournalSnapshot) -> dict[tuple[str, int], dict[str, dict[str, Any]]]:
    attempts: dict[tuple[str, int], dict[str, dict[str, Any]]] = {}
    for event in journal.events:
        key = (str(event["transport_id"]), int(event["attempt_number"]))
        attempts.setdefault(key, {})[str(event["phase"])] = event
    return attempts


def _ack_gap_reason(ack: _Ack, journal: _JournalSnapshot) -> str | None:
    if ack.transport_id is None:
        return "transport_identity_missing"
    attempt = _attempts(journal).get((ack.transport_id, ack.attempt_number), {})
    start = attempt.get("request_started")
    response = attempt.get("response_received")
    if start is None:
        return "request_start_missing"
    if response is None:
        return "successful_response_missing"
    if str(start["packet_id"]) != ack.packet_id or str(response["packet_id"]) != ack.packet_id:
        return "packet_binding_mismatch"
    for field, expected in (
        ("request_body_sha256", ack.request_body_sha256),
        ("route_sha256", ack.route_sha256),
    ):
        if str(start[field]) != expected or str(response[field]) != expected:
            return "request_binding_mismatch"
    if int(response["http_status"]) != ack.http_status or not 200 <= ack.http_status <= 299:
        return "successful_status_mismatch"
    if _utc_text(_parse_utc(str(response["occurred_at_utc"]))) != ack.responded_at_utc:
        return "response_timestamp_mismatch"
    if int(start["sequence"]) >= int(response["sequence"]):
        return "attempt_sequence_invalid"
    if _parse_utc(str(start["occurred_at_utc"])) > _parse_utc(ack.responded_at_utc):
        return "attempt_timestamp_invalid"
    if _parse_utc(ack.responded_at_utc) > _parse_utc(ack.notification_sent_at_utc):
        return "acknowledgement_timestamp_invalid"
    return None


def _baseline_attempt(
    item: _SourceItem,
    acknowledgements: tuple[_Ack, ...],
    journal: _JournalSnapshot,
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    exact_acks = [
        ack
        for ack in acknowledgements
        if (
            ack.packet_id,
            ack.decision_sha256,
            ack.notification_sent_at_utc,
        )
        == (item.packet_id, item.decision_sha256, item.notification_sent_at_utc)
    ]
    if len(exact_acks) != 1:
        return None
    ack = exact_acks[0]
    if _ack_gap_reason(ack, journal) is not None or ack.transport_id is None:
        return None
    attempt = _attempts(journal).get((ack.transport_id, ack.attempt_number), {})
    start = attempt.get("request_started")
    response = attempt.get("response_received")
    if start is None or response is None:
        return None
    return start, response


def _coverage_schema_definition_mismatches(conn: sqlite3.Connection) -> list[str]:
    actual = _coverage_schema_definitions(conn)
    return sorted(
        f"{kind}:{name}"
        for kind, name in set(_EXPECTED_COVERAGE_SCHEMA) | set(actual)
        if _EXPECTED_COVERAGE_SCHEMA.get((kind, name)) != actual.get((kind, name))
    )


def activate_notification_coverage(
    config: NotificationCoverageConfig,
    *,
    now_fn: Any = lambda: datetime.now(UTC),
) -> dict[str, Any]:
    """Seal the audited pre-fix baseline without modifying either source stream."""

    _, coverage_path, _, _ = _paths(config)
    policy_bytes, policy_sha = _load_policy(config)
    _ensure_store(coverage_path)
    with closing(_connect_readonly(coverage_path)) as conn:
        existing = conn.execute(
            "SELECT * FROM notification_coverage_configuration WHERE singleton=1"
        ).fetchone()
    if existing is not None:
        return {
            "activated_at_utc": str(existing["activated_at_utc"]),
            "record_sha256": str(existing["record_sha256"]),
            "baseline_sent_count": int(existing["baseline_sent_count"]),
            "baseline_covered_count": int(existing["baseline_covered_count"]),
            "baseline_missing_count": int(existing["baseline_missing_count"]),
            "already_active": True,
        }

    source = _read_source(config, now_fn=now_fn)
    journal_started = _utc_text(now_fn())
    journal = _read_journal(config)
    baseline_rows: list[dict[str, Any]] = []
    for item in source.items:
        attempt = _baseline_attempt(item, source.acknowledgements, journal)
        classification: Literal["covered", "missing"] = (
            "covered" if attempt is not None else "missing"
        )
        baseline_rows.append(
            {
                "schema_version": 1,
                "contract_version": COVERAGE_VERSION,
                "source_record_sha256": item.source_record_sha256,
                "packet_id_sha256": _sha256(item.packet_id.encode()),
                "classification": classification,
                "request_record_sha256": (
                    str(attempt[0]["record_sha256"]) if attempt is not None else None
                ),
                "response_record_sha256": (
                    str(attempt[1]["record_sha256"]) if attempt is not None else None
                ),
            }
        )
    covered = [
        row["source_record_sha256"] for row in baseline_rows if row["classification"] == "covered"
    ]
    missing = [
        row["source_record_sha256"] for row in baseline_rows if row["classification"] == "missing"
    ]
    activated_at = _utc_text(now_fn())
    source_root = Path(os.path.abspath(config.source_root))
    research_root = Path(os.path.abspath(config.research_root))
    source_path, _, journal_path, policy_path = _paths(config)
    configuration = {
        "schema_version": 1,
        "contract_version": COVERAGE_VERSION,
        "activated_at_utc": activated_at,
        "source_snapshot_completed_at_utc": source.completed_at_utc,
        "journal_snapshot_started_at_utc": journal_started,
        "source_before_journal": True,
        "source_database_ref": source_path.relative_to(source_root).as_posix(),
        "journal_database_ref": journal_path.relative_to(research_root).as_posix(),
        "policy_ref": policy_path.relative_to(Path(os.path.abspath(config.policy_root))).as_posix(),
        "policy_sha256": policy_sha,
        "policy_bytes_sha256": _sha256(policy_bytes),
        "source_schema_sha256": source.schema_sha256,
        "source_ack_snapshot_sha256": _sha256(
            _canonical({"record_sha256": [ack.record_sha256 for ack in source.acknowledgements]})
        ),
        "source_ack_sequence_watermark": max(
            (ack.sequence for ack in source.acknowledgements), default=0
        ),
        "journal_sequence_watermark": journal.max_sequence,
        "journal_snapshot_sha256": journal.snapshot_sha256,
        "journal_activation_at_utc": journal.activation_at_utc,
        "journal_activation_record_sha256": journal.activation_record_sha256,
        "baseline_sent_count": len(baseline_rows),
        "baseline_covered_count": len(covered),
        "baseline_missing_count": len(missing),
        "baseline_all_sha256": _sha256(
            _canonical({"items": sorted(row["source_record_sha256"] for row in baseline_rows)})
        ),
        "baseline_covered_sha256": _sha256(_canonical({"items": sorted(covered)})),
        "baseline_missing_sha256": _sha256(_canonical({"items": sorted(missing)})),
        "initial_runtime_git_commit": config.runtime_git_commit,
    }
    encoded_config = _canonical(configuration)
    config_sha = _sha256(encoded_config)
    with closing(_connect_coverage(coverage_path)) as conn, conn:
        conn.execute("BEGIN IMMEDIATE")
        if (
            conn.execute(
                "SELECT 1 FROM notification_coverage_configuration WHERE singleton=1"
            ).fetchone()
            is not None
        ):
            raise NotificationCoverageError("notification coverage activation raced another writer")
        conn.execute(
            "INSERT INTO notification_coverage_configuration VALUES(1,?,?,?,?,?,?,?,?,?,?,?)",
            (
                activated_at,
                configuration["source_ack_sequence_watermark"],
                journal.max_sequence,
                len(baseline_rows),
                len(covered),
                len(missing),
                policy_sha,
                source.schema_sha256,
                config.runtime_git_commit,
                config_sha,
                encoded_config,
            ),
        )
        for sequence, record in enumerate(baseline_rows, start=1):
            encoded = _canonical(record)
            conn.execute(
                "INSERT INTO notification_coverage_baseline VALUES(?,?,?,?,?,?,?,?)",
                (
                    sequence,
                    record["source_record_sha256"],
                    record["packet_id_sha256"],
                    record["classification"],
                    record["request_record_sha256"],
                    record["response_record_sha256"],
                    _sha256(encoded),
                    encoded,
                ),
            )
        conn.execute(
            "INSERT INTO notification_coverage_health VALUES(1,?,?,NULL,NULL,0,0,?,?,?,?)",
            (
                activated_at,
                activated_at,
                configuration["source_ack_sequence_watermark"],
                journal.max_sequence,
                configuration["source_ack_snapshot_sha256"],
                journal.snapshot_sha256,
            ),
        )
    return {
        "activated_at_utc": activated_at,
        "record_sha256": config_sha,
        "baseline_sent_count": len(baseline_rows),
        "baseline_covered_count": len(covered),
        "baseline_missing_count": len(missing),
        "already_active": False,
    }


def _read_coverage_state(
    coverage_path: Path,
) -> tuple[sqlite3.Row, list[sqlite3.Row], list[sqlite3.Row], sqlite3.Row | None]:
    if not coverage_path.is_file():
        raise NotificationCoverageNotActive("notification coverage database is missing")
    try:
        with closing(_connect_readonly(coverage_path)) as conn:
            conn.execute("BEGIN")
            mismatched_objects = _coverage_schema_definition_mismatches(conn)
            if mismatched_objects:
                raise NotificationCoverageError(
                    f"notification coverage schema definition mismatch: {mismatched_objects}"
                )
            configuration = conn.execute(
                "SELECT * FROM notification_coverage_configuration WHERE singleton=1"
            ).fetchone()
            if configuration is None:
                raise NotificationCoverageNotActive("notification coverage is not activated")
            baseline = conn.execute(
                "SELECT * FROM notification_coverage_baseline ORDER BY sequence"
            ).fetchall()
            gaps = conn.execute(
                "SELECT * FROM notification_coverage_gaps ORDER BY sequence"
            ).fetchall()
            health = conn.execute(
                "SELECT * FROM notification_coverage_health WHERE singleton=1"
            ).fetchone()
    except sqlite3.Error as exc:
        raise NotificationCoverageError("notification coverage store is unavailable") from exc
    return configuration, baseline, gaps, health


def _validate_coverage_state(
    configuration: sqlite3.Row,
    baseline: list[sqlite3.Row],
    stored_gaps: list[sqlite3.Row],
) -> dict[str, Any]:
    encoded = bytes(configuration["record_json"])
    config_sha = str(configuration["record_sha256"])
    if _sha256(encoded) != config_sha:
        raise NotificationCoverageError("coverage activation digest mismatch")
    try:
        record = json.loads(encoded)
    except json.JSONDecodeError as exc:
        raise NotificationCoverageError("coverage activation is invalid JSON") from exc
    if not isinstance(record, dict) or _canonical(record) != encoded:
        raise NotificationCoverageError("coverage activation is not canonical")
    expected_configuration_keys = {
        "schema_version",
        "contract_version",
        "activated_at_utc",
        "source_snapshot_completed_at_utc",
        "journal_snapshot_started_at_utc",
        "source_before_journal",
        "source_database_ref",
        "journal_database_ref",
        "policy_ref",
        "policy_sha256",
        "policy_bytes_sha256",
        "source_schema_sha256",
        "source_ack_snapshot_sha256",
        "source_ack_sequence_watermark",
        "journal_sequence_watermark",
        "journal_snapshot_sha256",
        "journal_activation_at_utc",
        "journal_activation_record_sha256",
        "baseline_sent_count",
        "baseline_covered_count",
        "baseline_missing_count",
        "baseline_all_sha256",
        "baseline_covered_sha256",
        "baseline_missing_sha256",
        "initial_runtime_git_commit",
    }
    if (
        set(record) != expected_configuration_keys
        or record.get("schema_version") != 1
        or record.get("contract_version") != COVERAGE_VERSION
        or record.get("source_before_journal") is not True
        or record.get("source_ack_sequence_watermark")
        != int(configuration["source_ack_sequence_watermark"])
        or record.get("journal_sequence_watermark")
        != int(configuration["journal_sequence_watermark"])
    ):
        raise NotificationCoverageError("coverage activation semantics are invalid")
    config_bindings = {
        "activated_at_utc": str(configuration["activated_at_utc"]),
        "source_ack_sequence_watermark": int(configuration["source_ack_sequence_watermark"]),
        "journal_sequence_watermark": int(configuration["journal_sequence_watermark"]),
        "baseline_sent_count": int(configuration["baseline_sent_count"]),
        "baseline_covered_count": int(configuration["baseline_covered_count"]),
        "baseline_missing_count": int(configuration["baseline_missing_count"]),
        "policy_sha256": str(configuration["policy_sha256"]),
        "source_schema_sha256": str(configuration["source_schema_sha256"]),
        "initial_runtime_git_commit": str(configuration["initial_runtime_git_commit"]),
    }
    if any(record.get(field) != value for field, value in config_bindings.items()):
        raise NotificationCoverageError("coverage activation row mismatch")
    try:
        source_completed = _parse_utc(str(record["source_snapshot_completed_at_utc"]))
        journal_started = _parse_utc(str(record["journal_snapshot_started_at_utc"]))
        activated_at = _parse_utc(str(record["activated_at_utc"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise NotificationCoverageError("coverage activation timestamps are invalid") from exc
    if source_completed > journal_started or journal_started > activated_at:
        raise NotificationCoverageError("coverage snapshot ordering is invalid")
    if len(baseline) != int(configuration["baseline_sent_count"]):
        raise NotificationCoverageError("coverage baseline count mismatch")
    covered = missing = 0
    baseline_ids: set[str] = set()
    covered_ids: list[str] = []
    missing_ids: list[str] = []
    for sequence, row in enumerate(baseline, start=1):
        if int(row["sequence"]) != sequence:
            raise NotificationCoverageError("coverage baseline sequence is invalid")
        row_bytes = bytes(row["record_json"])
        if _sha256(row_bytes) != str(row["record_sha256"]):
            raise NotificationCoverageError("coverage baseline digest mismatch")
        item = json.loads(row_bytes)
        if not isinstance(item, dict) or _canonical(item) != row_bytes:
            raise NotificationCoverageError("coverage baseline envelope is invalid")
        expected_keys = {
            "schema_version",
            "contract_version",
            "source_record_sha256",
            "packet_id_sha256",
            "classification",
            "request_record_sha256",
            "response_record_sha256",
        }
        if set(item) != expected_keys:
            raise NotificationCoverageError("coverage baseline fields are invalid")
        source_sha = str(row["source_record_sha256"])
        if item.get("source_record_sha256") != source_sha or not _SHA256.fullmatch(source_sha):
            raise NotificationCoverageError("coverage baseline source binding is invalid")
        baseline_ids.add(source_sha)
        row_bindings = {
            "source_record_sha256": source_sha,
            "packet_id_sha256": str(row["packet_id_sha256"]),
            "classification": str(row["classification"]),
            "request_record_sha256": row["request_record_sha256"],
            "response_record_sha256": row["response_record_sha256"],
        }
        if any(item.get(field) != value for field, value in row_bindings.items()):
            raise NotificationCoverageError("coverage baseline row mismatch")
        if not _SHA256.fullmatch(str(row["packet_id_sha256"])):
            raise NotificationCoverageError("coverage baseline packet digest is invalid")
        if str(row["classification"]) == "covered":
            if not _SHA256.fullmatch(str(row["request_record_sha256"])) or not _SHA256.fullmatch(
                str(row["response_record_sha256"])
            ):
                raise NotificationCoverageError("covered baseline evidence is incomplete")
            covered += 1
            covered_ids.append(source_sha)
        elif str(row["classification"]) == "missing":
            if (
                row["request_record_sha256"] is not None
                or row["response_record_sha256"] is not None
            ):
                raise NotificationCoverageError("missing baseline contains invented evidence")
            missing += 1
            missing_ids.append(source_sha)
        else:
            raise NotificationCoverageError("coverage baseline classification is invalid")
    if covered != int(configuration["baseline_covered_count"]) or missing != int(
        configuration["baseline_missing_count"]
    ):
        raise NotificationCoverageError("coverage baseline partition mismatch")
    if (
        record.get("baseline_all_sha256") != _sha256(_canonical({"items": sorted(baseline_ids)}))
        or record.get("baseline_covered_sha256")
        != _sha256(_canonical({"items": sorted(covered_ids)}))
        or record.get("baseline_missing_sha256")
        != _sha256(_canonical({"items": sorted(missing_ids)}))
    ):
        raise NotificationCoverageError("coverage baseline set digest mismatch")
    gap_ids: list[str] = []
    for sequence, row in enumerate(stored_gaps, start=1):
        if int(row["sequence"]) != sequence:
            raise NotificationCoverageError("coverage gap sequence is invalid")
        row_bytes = bytes(row["record_json"])
        if _sha256(row_bytes) != str(row["record_sha256"]):
            raise NotificationCoverageError("coverage gap digest mismatch")
        gap = json.loads(row_bytes)
        if not isinstance(gap, dict) or _canonical(gap) != row_bytes:
            raise NotificationCoverageError("coverage gap envelope is invalid")
        gap_bindings = {
            "gap_id": str(row["gap_id"]),
            "reason": str(row["reason"]),
            "source_record_sha256": str(row["source_record_sha256"]),
            "ack_id": row["ack_id"],
            "evidence_not_before_at_utc": str(row["evidence_not_before_at_utc"]),
            "first_observed_at_utc": str(row["first_observed_at_utc"]),
        }
        if any(gap.get(field) != value for field, value in gap_bindings.items()):
            raise NotificationCoverageError("coverage gap identity mismatch")
        expected_gap_id = _sha256(
            (
                f"{COVERAGE_VERSION}|{gap_bindings['reason']}|"
                f"{gap_bindings['source_record_sha256']}|{gap_bindings['ack_id'] or ''}"
            ).encode()
        )
        if gap_bindings["gap_id"] != expected_gap_id:
            raise NotificationCoverageError("coverage gap digest identity is invalid")
        evidence_not_before = _parse_utc(str(gap_bindings["evidence_not_before_at_utc"]))
        first_observed = _parse_utc(str(gap_bindings["first_observed_at_utc"]))
        if first_observed < evidence_not_before:
            raise NotificationCoverageError("coverage gap predates its linked evidence")
        gap_ids.append(str(row["gap_id"]))
    return {
        "configuration": record,
        "configuration_sha256": config_sha,
        "baseline_source_ids": baseline_ids,
        "stored_gap_ids": gap_ids,
    }


def _live_reconciliation(
    config: NotificationCoverageConfig,
    *,
    now_fn: Any,
) -> dict[str, Any]:
    source_path, coverage_path, journal_path, policy_path = _paths(config)
    configuration, baseline, stored_gaps, health = _read_coverage_state(coverage_path)
    state = _validate_coverage_state(configuration, baseline, stored_gaps)
    policy_bytes, current_policy_sha = _load_policy(config)
    config_record = state["configuration"]
    expected_refs = {
        "source_database_ref": source_path.relative_to(
            Path(os.path.abspath(config.source_root))
        ).as_posix(),
        "journal_database_ref": journal_path.relative_to(
            Path(os.path.abspath(config.research_root))
        ).as_posix(),
        "policy_ref": policy_path.relative_to(Path(os.path.abspath(config.policy_root))).as_posix(),
    }
    if any(config_record.get(field) != value for field, value in expected_refs.items()):
        raise NotificationCoverageError("coverage configured paths changed after activation")
    if current_policy_sha != config_record.get("policy_sha256") or _sha256(
        policy_bytes
    ) != config_record.get("policy_bytes_sha256"):
        raise NotificationCoverageError("coverage policy changed after activation")

    # The source snapshot must be fully materialized and closed before the journal opens.
    source = _read_source(config, now_fn=now_fn)
    if source.schema_sha256 != config_record.get("source_schema_sha256"):
        raise NotificationCoverageError("coverage source schema changed after activation")
    journal = _read_journal(config)
    if journal.activation_record_sha256 != config_record.get("journal_activation_record_sha256"):
        raise NotificationCoverageError("transport journal activation changed")

    watermark = int(configuration["source_ack_sequence_watermark"])
    journal_watermark = int(configuration["journal_sequence_watermark"])
    if len(source.acknowledgements) < watermark or journal.max_sequence < journal_watermark:
        raise NotificationCoverageError("notification evidence sequence rolled back")
    source_prefix_sha = _sha256(
        _canonical(
            {"record_sha256": [ack.record_sha256 for ack in source.acknowledgements[:watermark]]}
        )
    )
    journal_prefix_sha = _sha256(
        _canonical(
            {
                "record_sha256": [
                    str(event["record_sha256"])
                    for event in journal.events
                    if int(event["sequence"]) <= journal_watermark
                ]
            }
        )
    )
    if source_prefix_sha != config_record.get(
        "source_ack_snapshot_sha256"
    ) or journal_prefix_sha != config_record.get("journal_snapshot_sha256"):
        raise NotificationCoverageError("notification evidence prefix changed after activation")
    current_source_prefix_sha = _sha256(
        _canonical({"record_sha256": [ack.record_sha256 for ack in source.acknowledgements]})
    )
    if health is not None:
        try:
            prior_source_sequence = int(health["last_source_ack_sequence"])
            prior_journal_sequence = int(health["last_journal_sequence"])
            prior_source_prefix_sha = str(health["last_source_prefix_sha256"])
            prior_journal_prefix_sha = str(health["last_journal_prefix_sha256"])
        except (KeyError, TypeError, ValueError) as exc:
            raise NotificationCoverageError("coverage health sequence is invalid") from exc
        if not _SHA256.fullmatch(prior_source_prefix_sha) or not _SHA256.fullmatch(
            prior_journal_prefix_sha
        ):
            raise NotificationCoverageError("coverage health prefix is invalid")
        if (
            len(source.acknowledgements) < prior_source_sequence
            or journal.max_sequence < prior_journal_sequence
        ):
            raise NotificationCoverageError("notification evidence rolled back after monitoring")
        observed_source_prefix_sha = _sha256(
            _canonical(
                {
                    "record_sha256": [
                        ack.record_sha256 for ack in source.acknowledgements[:prior_source_sequence]
                    ]
                }
            )
        )
        observed_journal_prefix_sha = _sha256(
            _canonical(
                {
                    "record_sha256": [
                        str(event["record_sha256"])
                        for event in journal.events
                        if int(event["sequence"]) <= prior_journal_sequence
                    ]
                }
            )
        )
        if (
            observed_source_prefix_sha != prior_source_prefix_sha
            or observed_journal_prefix_sha != prior_journal_prefix_sha
        ):
            raise NotificationCoverageError("notification evidence prefix changed after monitoring")
    post_acks = [ack for ack in source.acknowledgements if ack.sequence > watermark]
    gaps: list[dict[str, str | None]] = []
    ack_identities = {
        (ack.packet_id, ack.decision_sha256, ack.notification_sent_at_utc)
        for ack in source.acknowledgements
    }
    for ack in post_acks:
        reason = _ack_gap_reason(ack, journal)
        if reason is not None:
            evidence_not_before = _utc_text(
                max(
                    _parse_utc(ack.notification_sent_at_utc),
                    _parse_utc(ack.responded_at_utc),
                )
            )
            gaps.append(
                {
                    "reason": reason,
                    "source_record_sha256": ack.record_sha256,
                    "ack_id": ack.ack_id,
                    "evidence_not_before_at_utc": evidence_not_before,
                }
            )
    baseline_ids = state["baseline_source_ids"]
    for item in source.items:
        identity = (item.packet_id, item.decision_sha256, item.notification_sent_at_utc)
        if item.source_record_sha256 not in baseline_ids and identity not in ack_identities:
            gaps.append(
                {
                    "reason": "unledgered_current_delivery",
                    "source_record_sha256": item.source_record_sha256,
                    "ack_id": None,
                    "evidence_not_before_at_utc": item.notification_sent_at_utc,
                }
            )
    deduped: dict[str, dict[str, str | None]] = {}
    for gap in gaps:
        gap_identity_text = (
            f"{COVERAGE_VERSION}|{gap['reason']}|{gap['source_record_sha256']}|"
            f"{gap['ack_id'] or ''}"
        )
        gap_id = _sha256(gap_identity_text.encode())
        deduped[gap_id] = {**gap, "gap_id": gap_id}
    return {
        "configuration_sha256": state["configuration_sha256"],
        "activated_at_utc": str(configuration["activated_at_utc"]),
        "baseline": {
            "sent": int(configuration["baseline_sent_count"]),
            "covered": int(configuration["baseline_covered_count"]),
            "missing": int(configuration["baseline_missing_count"]),
            "missing_sha256": config_record["baseline_missing_sha256"],
        },
        "source_ack_sequence": max((ack.sequence for ack in source.acknowledgements), default=0),
        "journal_sequence": journal.max_sequence,
        "source_prefix_sha256": current_source_prefix_sha,
        "journal_prefix_sha256": journal.snapshot_sha256,
        "post_activation_ack_count": len(post_acks),
        "current_gaps": list(deduped.values()),
        "stored_gap_ids": state["stored_gap_ids"],
        "health": dict(health) if health is not None else None,
    }


def _write_health(
    coverage_path: Path,
    *,
    started_at: str,
    success_at: str | None,
    error: BaseException | None,
    post_ack_count: int,
    current_gap_count: int,
    source_sequence: int,
    journal_sequence: int,
    source_prefix_sha256: str | None,
    journal_prefix_sha256: str | None,
) -> None:
    started_at = _utc_text(_parse_utc(started_at))
    if success_at is not None:
        success_at = _utc_text(_parse_utc(success_at))
    with closing(_connect_coverage(coverage_path)) as conn, conn:
        existing = conn.execute(
            "SELECT * FROM notification_coverage_health WHERE singleton=1"
        ).fetchone()
        if source_prefix_sha256 is None or journal_prefix_sha256 is None:
            if existing is None:
                raise NotificationCoverageError(
                    "coverage health cannot preserve a missing checkpoint"
                )
            source_sequence = int(existing["last_source_ack_sequence"])
            journal_sequence = int(existing["last_journal_sequence"])
            source_prefix_sha256 = str(existing["last_source_prefix_sha256"])
            journal_prefix_sha256 = str(existing["last_journal_prefix_sha256"])
        conn.execute(
            """
            INSERT INTO notification_coverage_health VALUES(1,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(singleton) DO UPDATE SET
              last_started_at_utc=CASE
                WHEN excluded.last_started_at_utc>
                     notification_coverage_health.last_started_at_utc
                THEN excluded.last_started_at_utc
                ELSE notification_coverage_health.last_started_at_utc
              END,
              last_success_at_utc=CASE
                WHEN excluded.last_started_at_utc>
                     notification_coverage_health.last_started_at_utc
                THEN COALESCE(
                  excluded.last_success_at_utc,
                  notification_coverage_health.last_success_at_utc
                )
                ELSE notification_coverage_health.last_success_at_utc
              END,
              last_error_kind=CASE
                WHEN excluded.last_started_at_utc>
                     notification_coverage_health.last_started_at_utc
                THEN excluded.last_error_kind
                ELSE notification_coverage_health.last_error_kind
              END,
              last_error_message=CASE
                WHEN excluded.last_started_at_utc>
                     notification_coverage_health.last_started_at_utc
                THEN excluded.last_error_message
                ELSE notification_coverage_health.last_error_message
              END,
              post_activation_ack_count=CASE
                WHEN excluded.last_started_at_utc>
                     notification_coverage_health.last_started_at_utc
                THEN excluded.post_activation_ack_count
                ELSE notification_coverage_health.post_activation_ack_count
              END,
              current_gap_count=CASE
                WHEN excluded.last_started_at_utc>
                     notification_coverage_health.last_started_at_utc
                THEN excluded.current_gap_count
                ELSE notification_coverage_health.current_gap_count
              END,
              last_source_ack_sequence=MAX(
                notification_coverage_health.last_source_ack_sequence,
                excluded.last_source_ack_sequence
              ),
              last_journal_sequence=MAX(
                notification_coverage_health.last_journal_sequence,
                excluded.last_journal_sequence
              ),
              last_source_prefix_sha256=CASE
                WHEN excluded.last_source_ack_sequence>
                     notification_coverage_health.last_source_ack_sequence
                THEN excluded.last_source_prefix_sha256
                ELSE notification_coverage_health.last_source_prefix_sha256
              END,
              last_journal_prefix_sha256=CASE
                WHEN excluded.last_journal_sequence>
                     notification_coverage_health.last_journal_sequence
                THEN excluded.last_journal_prefix_sha256
                ELSE notification_coverage_health.last_journal_prefix_sha256
              END
            """,
            (
                started_at,
                success_at,
                type(error).__name__ if error is not None else None,
                str(error)[:512] if error is not None else None,
                post_ack_count,
                current_gap_count,
                source_sequence,
                journal_sequence,
                source_prefix_sha256,
                journal_prefix_sha256,
            ),
        )


def run_notification_coverage_once(
    config: NotificationCoverageConfig,
    *,
    now_fn: Any = lambda: datetime.now(UTC),
) -> dict[str, Any]:
    """Reconcile once, durably recording deterministic gaps and operational health."""

    _, coverage_path, _, _ = _paths(config)
    started = _utc_text(now_fn())
    try:
        report = _live_reconciliation(config, now_fn=now_fn)
        gaps = report["current_gaps"]
        if not isinstance(gaps, list):
            raise NotificationCoverageError("coverage reconciliation returned invalid gaps")
        observed_at = now_fn()
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("coverage observation clock returned a naive timestamp")
        observed_at = observed_at.astimezone(UTC)
        with closing(_connect_coverage(coverage_path)) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            for gap in gaps:
                gap_id = str(gap["gap_id"])
                if (
                    conn.execute(
                        "SELECT 1 FROM notification_coverage_gaps WHERE gap_id=?", (gap_id,)
                    ).fetchone()
                    is not None
                ):
                    continue
                sequence = int(
                    conn.execute(
                        "SELECT COALESCE(MAX(sequence),0)+1 FROM notification_coverage_gaps"
                    ).fetchone()[0]
                )
                record = {
                    "schema_version": 1,
                    "contract_version": COVERAGE_VERSION,
                    "gap_id": gap_id,
                    "reason": str(gap["reason"]),
                    "source_record_sha256": str(gap["source_record_sha256"]),
                    "ack_id": gap["ack_id"],
                    "evidence_not_before_at_utc": str(gap["evidence_not_before_at_utc"]),
                    "first_observed_at_utc": _utc_text(
                        max(
                            observed_at,
                            _parse_utc(str(gap["evidence_not_before_at_utc"])),
                        )
                    ),
                }
                encoded = _canonical(record)
                conn.execute(
                    "INSERT INTO notification_coverage_gaps VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        sequence,
                        gap_id,
                        record["reason"],
                        record["source_record_sha256"],
                        record["ack_id"],
                        record["evidence_not_before_at_utc"],
                        record["first_observed_at_utc"],
                        _sha256(encoded),
                        encoded,
                    ),
                )
        error = (
            NotificationCoverageError("post-activation notification coverage gap") if gaps else None
        )
        _write_health(
            coverage_path,
            started_at=started,
            success_at=None if error else started,
            error=error,
            post_ack_count=int(report["post_activation_ack_count"]),
            current_gap_count=len(gaps),
            source_sequence=int(report["source_ack_sequence"]),
            journal_sequence=int(report["journal_sequence"]),
            source_prefix_sha256=str(report["source_prefix_sha256"]),
            journal_prefix_sha256=str(report["journal_prefix_sha256"]),
        )
        return {**report, "valid": error is None, "reason": str(error) if error else None}
    except Exception as exc:
        _write_health(
            coverage_path,
            started_at=started,
            success_at=None,
            error=exc,
            post_ack_count=0,
            current_gap_count=0,
            source_sequence=0,
            journal_sequence=0,
            source_prefix_sha256=None,
            journal_prefix_sha256=None,
        )
        raise


def notification_coverage_status(
    config: NotificationCoverageConfig,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return strict, outcome-blind coverage and monitor freshness state."""

    evaluated_at = (now or datetime.now(UTC)).astimezone(UTC)
    try:
        report = _live_reconciliation(config, now_fn=lambda: evaluated_at)
    except NotificationCoverageNotActive as exc:
        return {"valid": False, "reason": "notification_coverage_not_active", "detail": str(exc)}
    except (NotificationCoverageError, OSError, sqlite3.Error, ValueError) as exc:
        return {
            "valid": False,
            "reason": "notification_coverage_degraded",
            "detail": f"{type(exc).__name__}: {exc}",
        }
    health = report.get("health")
    errors: list[str] = []
    if not isinstance(health, dict):
        errors.append("coverage_health_missing")
    else:
        try:
            started = _parse_utc(str(health["last_started_at_utc"]))
            success = _parse_utc(str(health["last_success_at_utc"]))
        except (KeyError, TypeError, ValueError):
            errors.append("coverage_health_timestamp_invalid")
        else:
            if started > evaluated_at + timedelta(seconds=5) or success > evaluated_at + timedelta(
                seconds=5
            ):
                errors.append("coverage_health_from_future")
            if evaluated_at - started > timedelta(seconds=_EXPECTED_POLICY["freshness_seconds"]):
                errors.append("coverage_health_stale")
        if health.get("last_error_kind") is not None:
            errors.append("coverage_worker_error")
        try:
            health_gap_count = int(health.get("current_gap_count", -1))
        except (TypeError, ValueError):
            errors.append("coverage_health_semantics_invalid")
        else:
            if health_gap_count != len(report["current_gaps"]):
                errors.append("coverage_health_gap_count_mismatch")
    if report["current_gaps"]:
        errors.append("post_activation_coverage_gap")
    stored_gap_ids = report["stored_gap_ids"]
    if stored_gap_ids:
        errors.append("historical_post_activation_gap")
    public_report = {
        key: value for key, value in report.items() if key not in {"current_gaps", "stored_gap_ids"}
    }
    current_gap_ids = sorted(str(gap["gap_id"]) for gap in report["current_gaps"])
    return {
        **public_report,
        "valid": not errors,
        "integrity_errors": errors,
        "current_gap_count": len(current_gap_ids),
        "current_gap_sha256": _sha256(_canonical({"gap_ids": current_gap_ids})),
        "stored_gap_count": len(stored_gap_ids),
        "stored_gap_sha256": _sha256(_canonical({"gap_ids": sorted(stored_gap_ids)})),
    }
