from __future__ import annotations

import asyncio
import ctypes
import hashlib
import json
import os
import sqlite3
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import monotonic, sleep
from typing import Literal

import httpx

from insider_alerts.config import Settings
from insider_alerts.execution.errors import IbkrExecutionError
from insider_alerts.notify.ntfy import NtfyDeliveryReceipt, NtfyNotificationError

FailureKind = Literal[
    "ibkr_gateway_unavailable",
    "ibkr_execution_failure",
    "operating_system_failure",
    "sqlite_failure",
    "validation_failure",
]
NotificationPhase = Literal["outage", "recovery", "recovery_indeterminate"]
DispatchStatus = Literal["delivered", "failed", "stale"]

OUTAGE_THRESHOLD_SECONDS = 300
DELIVERY_RETRY_SECONDS = 300
DELIVERY_DEADLINE_SECONDS = 5.0
ALERT_DB_TIMEOUT_SECONDS = 0.1
_COMPONENT = "live_canary"


@dataclass(frozen=True, slots=True)
class OperationalNotificationAction:
    incident_id: str
    phase: NotificationPhase
    failure_kind: FailureKind
    started_at_utc: datetime
    recovered_at_utc: datetime | None
    reserved_at_utc: datetime


@dataclass(frozen=True, slots=True)
class OperationalDispatchResult:
    status: DispatchStatus
    error_kind: str | None = None


def classify_operational_failure(exc: Exception) -> FailureKind:
    """Map a caught canary failure to a bounded, secret-free operational category."""

    if isinstance(exc, IbkrExecutionError):
        detail = str(exc)
        if detail.startswith(
            ("IBKR_GATEWAY_STARTUP_SYNC_FAILED", "IBKR_GATEWAY_HANDSHAKE_TIMEOUT")
        ):
            return "ibkr_gateway_unavailable"
        return "ibkr_execution_failure"
    if isinstance(exc, sqlite3.Error):
        return "sqlite_failure"
    if isinstance(exc, OSError):
        return "operating_system_failure"
    return "validation_failure"


def operational_notification_content(
    action: OperationalNotificationAction,
) -> tuple[str, str, list[str], int]:
    if action.phase == "outage":
        gateway_hint = (
            " Check IB Gateway authentication and localhost port 4001."
            if action.failure_kind == "ibkr_gateway_unavailable"
            else " Check the live-canary status and error log."
        )
        return (
            "Insider canary operational outage",
            (
                f"Incident `{action.incident_id}` has failed continuously for at least "
                f"{OUTAGE_THRESHOLD_SECONDS // 60} minutes. "
                f"Category: `{action.failure_kind}`. Live orders remain fail-closed."
                f"{gateway_hint}"
            ),
            ["warning", "chart_with_downwards_trend"],
            5,
        )

    recovered_at = action.recovered_at_utc
    if recovered_at is None:  # pragma: no cover - guarded by tracker invariants
        raise ValueError("recovery action requires recovered_at_utc")
    downtime_seconds = max(0, int((recovered_at - action.started_at_utc).total_seconds()))
    delivery_note = (
        " The earlier outage delivery was indeterminate, so this message is also the durable "
        "outage notice."
        if action.phase == "recovery_indeterminate"
        else ""
    )
    return (
        "Insider canary recovered",
        (
            f"Incident `{action.incident_id}` recovered after approximately "
            f"{downtime_seconds // 60} minutes.{delivery_note} The successful cycle performed "
            "normal broker reconciliation before any live action."
        ),
        ["white_check_mark", "chart_with_upwards_trend"],
        4,
    )


async def send_operational_notification(
    settings: Settings,
    action: OperationalNotificationAction,
) -> NtfyDeliveryReceipt:
    """Send one packetless notice under a hard end-to-end deadline."""

    title, message, tags, priority = operational_notification_content(action)
    url = f"{str(settings.ntfy_base_url).rstrip('/')}/{settings.ntfy_topic}"
    headers = {
        "Title": title,
        "Markdown": "yes",
        "Tags": ",".join(tags),
        "Priority": str(priority),
    }
    if settings.ntfy_token:
        headers["Authorization"] = f"Bearer {settings.ntfy_token}"
    body = message.encode("utf-8")
    body_sha = hashlib.sha256(body).hexdigest()
    route_sha = hashlib.sha256(url.encode("utf-8")).hexdigest()
    try:
        async with asyncio.timeout(DELIVERY_DEADLINE_SECONDS):
            async with httpx.AsyncClient(timeout=httpx.Timeout(3.0)) as client:
                response = await client.post(url, content=body, headers=headers)
                response.raise_for_status()
    except (TimeoutError, httpx.HTTPError) as exc:
        raise NtfyNotificationError(
            f"operational ntfy delivery failed: {type(exc).__name__}"
        ) from exc
    return NtfyDeliveryReceipt(
        attempt_number=1,
        responded_at_utc=datetime.now(UTC),
        request_body_sha256=body_sha,
        route_sha256=route_sha,
        http_status=response.status_code,
    )


class OperationalIncidentTracker:
    """Durably reserve and reconcile at-least-once operational notifications."""

    def __init__(
        self,
        ledger_path: str | Path,
        *,
        outage_threshold_seconds: int = OUTAGE_THRESHOLD_SECONDS,
        delivery_retry_seconds: int = DELIVERY_RETRY_SECONDS,
        database_timeout_seconds: float = ALERT_DB_TIMEOUT_SECONDS,
    ) -> None:
        if outage_threshold_seconds < 1:
            raise ValueError("outage_threshold_seconds must be positive")
        if delivery_retry_seconds < 1:
            raise ValueError("delivery_retry_seconds must be positive")
        if database_timeout_seconds <= 0:
            raise ValueError("database_timeout_seconds must be positive")
        self.path = Path(ledger_path).resolve(strict=False)
        self.outage_threshold = timedelta(seconds=outage_threshold_seconds)
        self.delivery_retry = timedelta(seconds=delivery_retry_seconds)
        self.database_timeout_seconds = database_timeout_seconds
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=self.database_timeout_seconds)
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout={int(self.database_timeout_seconds * 1000)}")
        return conn

    def _ensure_schema(self) -> None:
        with _operational_mutex(self.path, timeout_seconds=1.0), self._connect() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS operational_incidents (
                    incident_id TEXT PRIMARY KEY,
                    component TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    last_failure_at TEXT NOT NULL,
                    failure_count INTEGER NOT NULL CHECK(failure_count > 0),
                    latest_failure_kind TEXT NOT NULL,
                    outage_last_attempt_at TEXT,
                    outage_notified_at TEXT,
                    outage_receipt_sha256 TEXT,
                    recovered_at TEXT,
                    recovery_last_attempt_at TEXT,
                    recovery_notified_at TEXT,
                    recovery_receipt_sha256 TEXT,
                    recovery_abandoned_at TEXT
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_operational_incidents_one_open
                    ON operational_incidents(component)
                    WHERE recovered_at IS NULL;
                """
            )
            conn.commit()

    def record_failure(
        self,
        failure_kind: FailureKind,
        *,
        now: datetime,
    ) -> OperationalNotificationAction | None:
        failure_kind = _failure_kind(failure_kind)
        observed_at = _as_utc(now)
        stamp = observed_at.isoformat()
        with _operational_mutex(self.path), self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            pending_recoveries = conn.execute(
                """
                SELECT incident_id FROM operational_incidents
                WHERE component=? AND recovered_at IS NOT NULL
                  AND outage_last_attempt_at IS NOT NULL
                  AND recovery_notified_at IS NULL
                  AND recovery_abandoned_at IS NULL
                """,
                (_COMPONENT,),
            ).fetchall()
            if pending_recoveries:
                conn.execute(
                    """
                    UPDATE operational_incidents SET recovery_abandoned_at=?
                    WHERE component=? AND recovered_at IS NOT NULL
                      AND outage_last_attempt_at IS NOT NULL
                      AND recovery_notified_at IS NULL
                      AND recovery_abandoned_at IS NULL
                    """,
                    (stamp, _COMPONENT),
                )
                for pending in pending_recoveries:
                    _event(
                        conn,
                        stamp,
                        "operational_recovery_notification_abandoned",
                        incident_id=str(pending["incident_id"]),
                    )

            row = conn.execute(
                "SELECT * FROM operational_incidents WHERE component=? AND recovered_at IS NULL",
                (_COMPONENT,),
            ).fetchone()
            if row is None:
                incident_sequence = int(
                    conn.execute("SELECT COUNT(*) FROM operational_incidents").fetchone()[0]
                )
                incident_id = hashlib.sha256(
                    f"{_COMPONENT}|{stamp}|{failure_kind}|{incident_sequence}".encode()
                ).hexdigest()[:24]
                conn.execute(
                    """
                    INSERT INTO operational_incidents(
                        incident_id,component,started_at,last_failure_at,failure_count,
                        latest_failure_kind
                    ) VALUES(?,?,?,?,?,?)
                    """,
                    (incident_id, _COMPONENT, stamp, stamp, 1, failure_kind),
                )
                _event(
                    conn,
                    stamp,
                    "operational_outage_started",
                    level="warning",
                    incident_id=incident_id,
                    failure_kind=failure_kind,
                )
                started_at = observed_at
                last_attempt_at = None
                outage_notified_at = None
            else:
                incident_id = str(row["incident_id"])
                started_at = _parse_utc(str(row["started_at"]))
                last_attempt_at = _optional_utc(row["outage_last_attempt_at"])
                outage_notified_at = row["outage_notified_at"]
                conn.execute(
                    """
                    UPDATE operational_incidents
                    SET last_failure_at=?, failure_count=failure_count+1,
                        latest_failure_kind=?
                    WHERE incident_id=?
                    """,
                    (stamp, failure_kind, incident_id),
                )

            due = (
                outage_notified_at is None
                and observed_at - started_at >= self.outage_threshold
                and (
                    last_attempt_at is None
                    or observed_at - last_attempt_at >= self.delivery_retry
                )
            )
            if not due:
                conn.commit()
                return None
            conn.execute(
                "UPDATE operational_incidents SET outage_last_attempt_at=? "
                "WHERE incident_id=? AND outage_notified_at IS NULL",
                (stamp, incident_id),
            )
            _event(
                conn,
                stamp,
                "operational_outage_notification_reserved",
                incident_id=incident_id,
            )
            conn.commit()
        return OperationalNotificationAction(
            incident_id=incident_id,
            phase="outage",
            failure_kind=failure_kind,
            started_at_utc=started_at,
            recovered_at_utc=None,
            reserved_at_utc=observed_at,
        )

    def record_success(self, *, now: datetime) -> OperationalNotificationAction | None:
        observed_at = _as_utc(now)
        stamp = observed_at.isoformat()
        with _operational_mutex(self.path), self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM operational_incidents WHERE component=? AND recovered_at IS NULL",
                (_COMPONENT,),
            ).fetchone()
            if row is not None:
                incident_id = str(row["incident_id"])
                conn.execute(
                    "UPDATE operational_incidents SET recovered_at=? WHERE incident_id=?",
                    (stamp, incident_id),
                )
                _event(
                    conn,
                    stamp,
                    "operational_outage_recovered",
                    incident_id=incident_id,
                )
                if row["outage_last_attempt_at"] is None:
                    conn.commit()
                    return None
                phase: NotificationPhase = (
                    "recovery"
                    if row["outage_notified_at"] is not None
                    else "recovery_indeterminate"
                )
                started_at = _parse_utc(str(row["started_at"]))
                failure_kind = _failure_kind(str(row["latest_failure_kind"]))
                last_attempt_at = None
                recovered_at = observed_at
            else:
                row = conn.execute(
                    """
                    SELECT * FROM operational_incidents
                    WHERE component=? AND recovered_at IS NOT NULL
                      AND outage_last_attempt_at IS NOT NULL
                      AND recovery_notified_at IS NULL
                      AND recovery_abandoned_at IS NULL
                    ORDER BY recovered_at DESC LIMIT 1
                    """,
                    (_COMPONENT,),
                ).fetchone()
                if row is None:
                    conn.commit()
                    return None
                incident_id = str(row["incident_id"])
                started_at = _parse_utc(str(row["started_at"]))
                recovered_at = _parse_utc(str(row["recovered_at"]))
                failure_kind = _failure_kind(str(row["latest_failure_kind"]))
                phase = (
                    "recovery"
                    if row["outage_notified_at"] is not None
                    else "recovery_indeterminate"
                )
                last_attempt_at = _optional_utc(row["recovery_last_attempt_at"])

            if (
                last_attempt_at is not None
                and observed_at - last_attempt_at < self.delivery_retry
            ):
                conn.commit()
                return None
            conn.execute(
                """
                UPDATE operational_incidents SET recovery_last_attempt_at=?
                WHERE incident_id=? AND recovery_notified_at IS NULL
                  AND recovery_abandoned_at IS NULL
                """,
                (stamp, incident_id),
            )
            _event(
                conn,
                stamp,
                "operational_recovery_notification_reserved",
                incident_id=incident_id,
                phase=phase,
            )
            conn.commit()
        return OperationalNotificationAction(
            incident_id=incident_id,
            phase=phase,
            failure_kind=failure_kind,
            started_at_utc=started_at,
            recovered_at_utc=recovered_at,
            reserved_at_utc=observed_at,
        )

    async def dispatch(
        self,
        settings: Settings,
        action: OperationalNotificationAction,
    ) -> OperationalDispatchResult:
        with _operational_mutex(self.path):
            if not self._dispatchable(action):
                return OperationalDispatchResult("stale")
            try:
                receipt = await send_operational_notification(settings, action)
            except Exception as exc:
                self._record_delivery_failure_locked(action, exc, now=datetime.now(UTC))
                return OperationalDispatchResult("failed", type(exc).__name__[:128])
            self._record_delivery_success_locked(action, receipt)
            return OperationalDispatchResult("delivered")

    def _dispatchable(self, action: OperationalNotificationAction) -> bool:
        attempt_column = (
            "outage_last_attempt_at"
            if action.phase == "outage"
            else "recovery_last_attempt_at"
        )
        notified_column = (
            "outage_notified_at" if action.phase == "outage" else "recovery_notified_at"
        )
        lifecycle_guard = (
            "recovered_at IS NULL"
            if action.phase == "outage"
            else "recovered_at IS NOT NULL AND recovery_abandoned_at IS NULL"
        )
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT 1 FROM operational_incidents
                WHERE incident_id=? AND {notified_column} IS NULL
                  AND {attempt_column}=? AND {lifecycle_guard}
                """,
                (action.incident_id, action.reserved_at_utc.isoformat()),
            ).fetchone()
        return row is not None

    def _record_delivery_success_locked(
        self,
        action: OperationalNotificationAction,
        receipt: NtfyDeliveryReceipt,
    ) -> None:
        stamp = _as_utc(receipt.responded_at_utc).isoformat()
        is_outage = action.phase == "outage"
        notified_column = "outage_notified_at" if is_outage else "recovery_notified_at"
        receipt_column = "outage_receipt_sha256" if is_outage else "recovery_receipt_sha256"
        lifecycle_guard = (
            "recovered_at IS NULL"
            if is_outage
            else "recovered_at IS NOT NULL AND recovery_abandoned_at IS NULL"
        )
        event_phase = "outage" if is_outage else "recovery"
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                f"""
                UPDATE operational_incidents
                SET {notified_column}=?, {receipt_column}=?
                WHERE incident_id=? AND {notified_column} IS NULL AND {lifecycle_guard}
                """,
                (stamp, receipt.request_body_sha256, action.incident_id),
            )
            if cursor.rowcount:
                _event(
                    conn,
                    stamp,
                    f"operational_{event_phase}_notification_delivered",
                    incident_id=action.incident_id,
                    http_status=receipt.http_status,
                    request_body_sha256=receipt.request_body_sha256,
                    phase=action.phase,
                )
            conn.commit()

    def _record_delivery_failure_locked(
        self,
        action: OperationalNotificationAction,
        exc: Exception,
        *,
        now: datetime,
    ) -> None:
        stamp = _as_utc(now).isoformat()
        event_phase = "outage" if action.phase == "outage" else "recovery"
        with self._connect() as conn:
            _event(
                conn,
                stamp,
                f"operational_{event_phase}_notification_failed",
                level="warning",
                incident_id=action.incident_id,
                exception_kind=type(exc).__name__[:128],
                phase=action.phase,
            )
            conn.commit()


def operational_incident_status(ledger_path: str | Path) -> dict[str, object]:
    path = Path(ledger_path).resolve(strict=False)
    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(path, timeout=ALERT_DB_TIMEOUT_SECONDS)
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout={int(ALERT_DB_TIMEOUT_SECONDS * 1000)}")
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='operational_incidents'"
        ).fetchone()
        if exists is None:
            return {"available": False, "active": None, "latest": None}
        active = conn.execute(
            """
            SELECT incident_id,started_at,last_failure_at,failure_count,latest_failure_kind,
                   outage_notified_at,outage_last_attempt_at
            FROM operational_incidents
            WHERE component=? AND recovered_at IS NULL
            """,
            (_COMPONENT,),
        ).fetchone()
        latest = conn.execute(
            """
            SELECT incident_id,started_at,last_failure_at,failure_count,latest_failure_kind,
                   outage_notified_at,recovered_at,recovery_notified_at,recovery_abandoned_at
            FROM operational_incidents
            WHERE component=? ORDER BY started_at DESC LIMIT 1
            """,
            (_COMPONENT,),
        ).fetchone()
    except sqlite3.Error as exc:
        return {
            "available": False,
            "active": None,
            "latest": None,
            "error_kind": type(exc).__name__,
        }
    finally:
        if conn is not None:
            conn.close()
    return {
        "available": True,
        "active": dict(active) if active is not None else None,
        "latest": dict(latest) if latest is not None else None,
    }


@contextmanager
def _operational_mutex(
    ledger_path: Path,
    *,
    timeout_seconds: float = ALERT_DB_TIMEOUT_SECONDS,
) -> Iterator[None]:
    if timeout_seconds <= 0:
        raise ValueError("operational mutex timeout must be positive")
    identity = hashlib.sha256(str(ledger_path).casefold().encode()).hexdigest()
    if sys.platform == "win32":
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        create_mutex = kernel32.CreateMutexW
        create_mutex.argtypes = (ctypes.c_void_p, ctypes.c_bool, ctypes.c_wchar_p)
        create_mutex.restype = ctypes.c_void_p
        wait = kernel32.WaitForSingleObject
        wait.argtypes = (ctypes.c_void_p, ctypes.c_uint32)
        wait.restype = ctypes.c_uint32
        release = kernel32.ReleaseMutex
        release.argtypes = (ctypes.c_void_p,)
        release.restype = ctypes.c_bool
        close = kernel32.CloseHandle
        close.argtypes = (ctypes.c_void_p,)
        close.restype = ctypes.c_bool
        handle = create_mutex(None, False, f"Global\\InsiderAlertsCanaryOps-{identity}")
        if not handle:
            raise OSError(ctypes.get_last_error(), "operational mutex creation failed")
        acquired = False
        try:
            result = wait(handle, max(1, int(timeout_seconds * 1000)))
            if result not in (0x00000000, 0x00000080):
                if result == 0x00000102:
                    raise TimeoutError("operational mutex acquisition timed out")
                raise OSError(ctypes.get_last_error(), "operational mutex wait failed")
            acquired = True
            yield
        finally:
            if acquired:
                release(handle)
            close(handle)
        return

    import fcntl  # noqa: PLC0415

    lock_path = ledger_path.with_name(f".{ledger_path.name}.{identity[:12]}.ops.lock")
    descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    acquired = False
    deadline = monotonic() + timeout_seconds
    try:
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
                break
            except BlockingIOError:
                if monotonic() >= deadline:
                    raise TimeoutError("operational mutex acquisition timed out") from None
                sleep(0.01)
        yield
    finally:
        if acquired:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _event(
    conn: sqlite3.Connection,
    occurred_at: str,
    event_type: str,
    *,
    level: str = "info",
    **detail: object,
) -> None:
    conn.execute(
        """
        INSERT INTO events(occurred_at,level,event_type,packet_id,detail_json)
        VALUES(?,?,?,?,?)
        """,
        (
            occurred_at,
            level,
            event_type,
            None,
            json.dumps(detail, sort_keys=True, separators=(",", ":")),
        ),
    )


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("operational incident timestamp must be timezone-aware")
    return value.astimezone(UTC)


def _parse_utc(value: str) -> datetime:
    return _as_utc(datetime.fromisoformat(value))


def _optional_utc(value: object) -> datetime | None:
    return _parse_utc(str(value)) if value is not None else None


def _failure_kind(value: str) -> FailureKind:
    allowed: dict[str, FailureKind] = {
        "ibkr_gateway_unavailable": "ibkr_gateway_unavailable",
        "ibkr_execution_failure": "ibkr_execution_failure",
        "operating_system_failure": "operating_system_failure",
        "sqlite_failure": "sqlite_failure",
        "validation_failure": "validation_failure",
    }
    failure_kind = allowed.get(value)
    if failure_kind is None:
        raise ValueError("invalid operational failure kind in ledger")
    return failure_kind
