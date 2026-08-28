from __future__ import annotations

import csv
import io
import json
import math
import os
import sqlite3
import subprocess
import time
from collections.abc import Callable, Sequence
from contextlib import closing, suppress
from datetime import UTC, datetime
from pathlib import Path

from insider_alerts.config import AUTOPILOT_IB_REQUEST_TIMEOUT_SECONDS, Settings
from insider_alerts.research.option_chain_admission import OPTION_CHAIN_CAPTURE_TIMEOUT_SECONDS

AUTOPILOT_HEALTH_SCHEMA_VERSION = 1
MIN_STALE_SECONDS = 300
STALE_SAFETY_MARGIN_SECONDS = 70
SQLITE_STAGE_BUDGET_SECONDS = 200.0
SQLITE_REVIEW_ITEM_BUDGET_SECONDS = 10.0
OPTION_CHAIN_SQLITE_BUDGET_SECONDS = 90.0
OPTION_CHAIN_CLEANUP_BUDGET_SECONDS = 10.0
REVIEW_DECISION_SQLITE_BUDGET_SECONDS = 40.0
NOTIFICATION_ACK_SQLITE_BUDGET_SECONDS = 40.0
NOTIFICATION_OBSERVER_BUDGET_SECONDS = 15.0
HTTPX_TIMEOUT_PHASES = 4
URLLIB_TIMEOUT_PHASES = 2
SCHEDULER_CONTROL_TIMEOUT_SECONDS = 5.0
SCHEDULED_TASK_RUNNING_STATE = 4
MAX_STAGE_LENGTH = 80
MAX_ERROR_LENGTH = 1000

_HEALTH_COLUMNS = frozenset(
    {
        "singleton",
        "schema_version",
        "runtime_id",
        "runtime_started_utc",
        "source_fingerprint",
        "last_progress_utc",
        "last_progress_stage",
        "last_cycle_started_utc",
        "last_cycle_success_utc",
        "last_error_kind",
        "last_error_message",
    }
)
_REQUIRED_TEXT_COLUMNS = (
    "runtime_id",
    "runtime_started_utc",
    "source_fingerprint",
    "last_progress_utc",
    "last_progress_stage",
)
_OPTIONAL_TEXT_COLUMNS = (
    "last_cycle_started_utc",
    "last_cycle_success_utc",
    "last_error_kind",
    "last_error_message",
)

TaskRunner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]


class RuntimeOwnershipError(RuntimeError):
    """A superseded worker attempted to publish a heartbeat."""


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("heartbeat time must be timezone-aware")
    return value.astimezone(UTC)


def _stamp(value: datetime) -> str:
    return _utc(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _parse_stamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


class AutopilotHealthStore:
    """Small operational store kept separate from scientific and order state."""

    def __init__(self, path: Path | str, *, busy_timeout_ms: int = 250) -> None:
        if busy_timeout_ms < 1:
            raise ValueError("busy_timeout_ms must be positive")
        self.path = Path(path)
        self.busy_timeout_ms = busy_timeout_ms

    def _connect(self, *, write: bool) -> sqlite3.Connection:
        if write:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(self.path, timeout=self.busy_timeout_ms / 1000)
        else:
            conn = sqlite3.connect(
                f"file:{self.path.resolve().as_posix()}?mode=ro",
                uri=True,
                timeout=self.busy_timeout_ms / 1000,
            )
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        if write:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
        return conn

    def initialize(self) -> None:
        with closing(self._connect(write=True)) as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS autopilot_health (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    schema_version INTEGER NOT NULL,
                    runtime_id TEXT NOT NULL,
                    runtime_started_utc TEXT NOT NULL,
                    source_fingerprint TEXT NOT NULL,
                    last_progress_utc TEXT NOT NULL,
                    last_progress_stage TEXT NOT NULL,
                    last_cycle_started_utc TEXT,
                    last_cycle_success_utc TEXT,
                    last_error_kind TEXT,
                    last_error_message TEXT
                );
                """
            )

    def register_runtime(
        self,
        *,
        runtime_id: str,
        source_fingerprint: str,
        now: datetime,
    ) -> None:
        if not runtime_id or not source_fingerprint:
            raise ValueError("runtime identity and source fingerprint are required")
        stamp = _stamp(now)
        self.initialize()
        with closing(self._connect(write=True)) as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT INTO autopilot_health(
                    singleton,schema_version,runtime_id,runtime_started_utc,
                    source_fingerprint,last_progress_utc,last_progress_stage,
                    last_cycle_started_utc,last_cycle_success_utc,last_error_kind,
                    last_error_message
                ) VALUES(1,?,?,?,?,?,'runtime_started',NULL,NULL,NULL,NULL)
                ON CONFLICT(singleton) DO UPDATE SET
                    schema_version=excluded.schema_version,
                    runtime_id=excluded.runtime_id,
                    runtime_started_utc=excluded.runtime_started_utc,
                    source_fingerprint=excluded.source_fingerprint,
                    last_progress_utc=excluded.last_progress_utc,
                    last_progress_stage=excluded.last_progress_stage,
                    last_cycle_started_utc=NULL,
                    last_cycle_success_utc=NULL
                """,
                (
                    AUTOPILOT_HEALTH_SCHEMA_VERSION,
                    runtime_id,
                    stamp,
                    source_fingerprint,
                    stamp,
                ),
            )
            conn.commit()

    def progress(
        self,
        *,
        runtime_id: str,
        stage: str,
        now: datetime,
        cycle_started: bool = False,
        cycle_succeeded: bool = False,
        error: BaseException | None = None,
        advance_progress: bool = True,
    ) -> None:
        stage = stage.strip()
        if not stage or len(stage) > MAX_STAGE_LENGTH:
            raise ValueError("heartbeat stage must be non-empty and bounded")
        stamp = _stamp(now)
        error_kind = type(error).__name__ if error is not None else None
        error_message = str(error)[:MAX_ERROR_LENGTH] if error is not None else None
        with closing(self._connect(write=True)) as conn:
            cursor = conn.execute(
                """
                UPDATE autopilot_health
                SET last_progress_utc=CASE WHEN ? THEN ? ELSE last_progress_utc END,
                    last_progress_stage=CASE WHEN ? THEN ? ELSE last_progress_stage END,
                    last_cycle_started_utc=CASE WHEN ? THEN ? ELSE last_cycle_started_utc END,
                    last_cycle_success_utc=CASE WHEN ? THEN ? ELSE last_cycle_success_utc END,
                    last_error_kind=CASE
                        WHEN ? THEN NULL
                        WHEN ? IS NOT NULL THEN ?
                        ELSE last_error_kind
                    END,
                    last_error_message=CASE
                        WHEN ? THEN NULL
                        WHEN ? IS NOT NULL THEN ?
                        ELSE last_error_message
                    END
                WHERE singleton=1 AND runtime_id=?
                """,
                (
                    int(advance_progress),
                    stamp,
                    int(advance_progress),
                    stage,
                    int(cycle_started),
                    stamp,
                    int(cycle_succeeded),
                    stamp,
                    int(cycle_succeeded),
                    error_kind,
                    error_kind,
                    int(cycle_succeeded),
                    error_message,
                    error_message,
                    runtime_id,
                ),
            )
            conn.commit()
        if cursor.rowcount != 1:
            raise RuntimeOwnershipError("autopilot runtime heartbeat ownership was superseded")

    def read(self) -> dict[str, object]:
        with closing(self._connect(write=False)) as conn:
            row = conn.execute("SELECT * FROM autopilot_health WHERE singleton=1").fetchone()
        if row is None:
            raise sqlite3.DatabaseError("autopilot health row is missing")
        result = dict(row)
        missing_columns = sorted(_HEALTH_COLUMNS.difference(result))
        if missing_columns:
            raise sqlite3.DatabaseError(
                f"autopilot health schema is missing columns: {','.join(missing_columns)}"
            )
        try:
            schema_version = int(result.get("schema_version", -1))
        except (TypeError, ValueError) as exc:
            raise sqlite3.DatabaseError("autopilot health schema version is malformed") from exc
        if schema_version != AUTOPILOT_HEALTH_SCHEMA_VERSION:
            raise sqlite3.DatabaseError("autopilot health schema version is unsupported")
        for column in _REQUIRED_TEXT_COLUMNS:
            value = result[column]
            if not isinstance(value, str) or not value:
                raise sqlite3.DatabaseError(
                    f"autopilot health column {column} must be non-empty text"
                )
        for column in _OPTIONAL_TEXT_COLUMNS:
            value = result[column]
            if value is not None and not isinstance(value, str):
                raise sqlite3.DatabaseError(
                    f"autopilot health column {column} must be text or null"
                )
        return result


def heartbeat_state(
    path: Path | str,
    *,
    now: datetime,
    stale_seconds: int,
) -> tuple[bool, str, bool]:
    """Return stale, reason, corrupt. Locked stores raise so no restart is attempted."""

    if stale_seconds < 1:
        raise ValueError("stale_seconds must be positive")
    store_path = Path(path)
    if Path(f"{store_path.resolve()}.quarantine.json").is_file():
        return True, "heartbeat_quarantine_incomplete", True
    if not store_path.is_file():
        return True, "heartbeat_store_missing", False
    try:
        health = AutopilotHealthStore(store_path).read()
    except sqlite3.OperationalError as exc:
        message = str(exc).casefold()
        if "locked" in message or "busy" in message:
            raise
        return True, f"heartbeat_store_corrupt_{type(exc).__name__}", True
    except sqlite3.DatabaseError as exc:
        return True, f"heartbeat_store_corrupt_{type(exc).__name__}", True
    raw = health.get("last_progress_utc")
    if not isinstance(raw, str) or not raw:
        return True, "heartbeat_missing", False
    try:
        progress = _parse_stamp(raw)
    except (TypeError, ValueError, OverflowError):
        return True, "heartbeat_invalid", False
    checked_at = _utc(now)
    age = (checked_at - progress).total_seconds()
    if age < 0:
        return True, "heartbeat_future_untrusted", False
    if age > stale_seconds:
        return True, f"heartbeat_stale_{int(age)}s", False
    return False, f"heartbeat_fresh_{int(age)}s", False


def _task_is_running(result: subprocess.CompletedProcess[str]) -> bool:
    if result.returncode != 0:
        error = (result.stderr or result.stdout or "unknown schtasks failure").strip()
        raise RuntimeError(f"failed to query scheduled autopilot worker: {error}")
    normalized = result.stdout.strip()
    if normalized.startswith("__TASK_STATE__="):
        try:
            return int(normalized.partition("=")[2]) == SCHEDULED_TASK_RUNNING_STATE
        except ValueError as exc:
            raise RuntimeError("scheduled task state API returned malformed output") from exc
    rows = csv.reader(io.StringIO(result.stdout))
    return any(len(row) > 3 and row[3].strip().casefold() == "running" for row in rows)


def _run_schtasks(arguments: Sequence[str]) -> subprocess.CompletedProcess[str]:
    command = ["schtasks.exe", *arguments]
    environment: dict[str, str] | None = None
    if arguments and arguments[0].casefold() == "/query":
        try:
            task_name = arguments[arguments.index("/TN") + 1]
        except (ValueError, IndexError) as exc:
            raise RuntimeError("scheduled task query requires an exact task name") from exc
        environment = dict(os.environ)
        environment["INSIDER_ALERTS_TASK_QUERY_NAME"] = task_name
        state_script = (
            "$task = Get-ScheduledTask "
            "-TaskPath '\\' -TaskName $env:INSIDER_ALERTS_TASK_QUERY_NAME -ErrorAction Stop; "
            "[Console]::Out.Write('__TASK_STATE__=' + [int]$task.State)"
        )
        command = ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", state_script]
    try:
        return subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=SCHEDULER_CONTROL_TIMEOUT_SECONDS,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            env=environment,
        )
    except subprocess.TimeoutExpired as exc:
        operation = arguments[0] if arguments else "unknown"
        raise RuntimeError(
            f"scheduled task control {operation} timed out after "
            f"{SCHEDULER_CONTROL_TIMEOUT_SECONDS:g} seconds"
        ) from exc


def quarantine_corrupt_health_store(path: Path | str, *, now: datetime) -> list[str]:
    store_path = Path(path).resolve()
    manifest_path = Path(f"{store_path}.quarantine.json")
    suffixes = ("", "-wal", "-shm")
    if manifest_path.is_file():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            members = manifest["members"]
        except (OSError, KeyError, TypeError, json.JSONDecodeError) as exc:
            raise RuntimeError("autopilot health quarantine manifest is unreadable") from exc
        if not isinstance(members, list) or len(members) != len(suffixes):
            raise RuntimeError("autopilot health quarantine manifest is malformed")
    else:
        token = _stamp(now).replace(":", "").replace("-", "").replace(".", "")
        members = [
            {
                "source": str(Path(f"{store_path}{suffix}")),
                "destination": str(
                    Path(f"{store_path}{suffix}").with_name(
                        f"{Path(f'{store_path}{suffix}').name}.corrupt-{token}"
                    )
                ),
                "required": Path(f"{store_path}{suffix}").exists(),
            }
            for suffix in suffixes
        ]
        staging = manifest_path.with_name(f".{manifest_path.name}.{os.getpid()}.tmp")
        encoded = json.dumps({"version": 1, "members": members}, sort_keys=True)
        with staging.open("x", encoding="utf-8") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.replace(staging, manifest_path)
        finally:
            staging.unlink(missing_ok=True)

    moved: list[str] = []
    expected_sources = {str(Path(f"{store_path}{suffix}")) for suffix in suffixes}
    for member in members:
        if not isinstance(member, dict):
            raise RuntimeError("autopilot health quarantine manifest is malformed")
        source_text = member.get("source")
        destination_text = member.get("destination")
        required = member.get("required")
        if (
            not isinstance(source_text, str)
            or source_text not in expected_sources
            or not isinstance(destination_text, str)
            or not isinstance(required, bool)
        ):
            raise RuntimeError("autopilot health quarantine manifest is malformed")
        source = Path(source_text)
        destination = Path(destination_text)
        if destination.parent != store_path.parent or not destination.name.startswith(
            f"{source.name}.corrupt-"
        ):
            raise RuntimeError("autopilot health quarantine manifest escaped its store")
        if destination.exists():
            if source.exists():
                raise RuntimeError("autopilot health quarantine destination collision")
            moved.append(str(destination))
            continue
        if not source.exists():
            if required:
                raise RuntimeError("autopilot health quarantine member disappeared")
            continue
        os.replace(source, destination)
        moved.append(str(destination))
    manifest_path.unlink()
    return moved


def _await_worker_runtime(
    heartbeat_db: Path | str,
    *,
    previous_runtime_id: str | None,
    sleep_fn: Callable[[float], None],
    attempts: int = 10,
) -> bool:
    for attempt in range(attempts):
        try:
            health = AutopilotHealthStore(heartbeat_db).read()
            runtime_id = health.get("runtime_id")
            if isinstance(runtime_id, str) and runtime_id and runtime_id != previous_runtime_id:
                return True
        except (OSError, sqlite3.Error):
            pass
        if attempt + 1 < attempts:
            sleep_fn(0.5)
    return False


def run_autopilot_watchdog(
    *,
    heartbeat_db: Path | str,
    worker_task_name: str,
    stale_seconds: int,
    now: datetime | None = None,
    task_runner: TaskRunner | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
    stop_poll_attempts: int = 4,
) -> dict[str, object]:
    if stop_poll_attempts < 1:
        raise ValueError("stop_poll_attempts must be positive")
    checked_at = _utc(now or datetime.now(UTC))
    stale, reason, corrupt = heartbeat_state(
        heartbeat_db,
        now=checked_at,
        stale_seconds=stale_seconds,
    )
    runner = task_runner or _run_schtasks
    previous_runtime_id: str | None = None
    if not corrupt and Path(heartbeat_db).is_file():
        with suppress(OSError, sqlite3.Error):
            candidate = AutopilotHealthStore(heartbeat_db).read().get("runtime_id")
            if isinstance(candidate, str):
                previous_runtime_id = candidate
    query = ["/Query", "/TN", worker_task_name, "/FO", "CSV", "/NH", "/V"]
    worker_running = _task_is_running(runner(query))
    end_return_code: int | None = None
    should_start = stale or not worker_running
    task_fenced = False
    if should_start:
        disable_result = runner(["/Change", "/TN", worker_task_name, "/Disable"])
        if disable_result.returncode != 0:
            error = (
                disable_result.stderr or disable_result.stdout or "unknown schtasks failure"
            ).strip()
            raise RuntimeError(f"failed to fence scheduled autopilot worker: {error}")
        task_fenced = True
        # Disabling prevents RestartOnFailure from racing the stop/quarantine boundary, but it does
        # not stop an already-running instance. Re-query after the fence before deciding to end it.
        worker_running = _task_is_running(runner(query))

    if worker_running and should_start:
        end_result = runner(["/End", "/TN", worker_task_name])
        end_return_code = end_result.returncode
        if end_result.returncode != 0:
            error = (
                end_result.stderr or end_result.stdout or "unknown schtasks failure"
            ).strip()
            raise RuntimeError(f"failed to stop stale scheduled autopilot worker: {error}")
        for attempt in range(stop_poll_attempts):
            if not _task_is_running(runner(query)):
                worker_running = False
                break
            if attempt + 1 < stop_poll_attempts:
                sleep_fn(0.25)
        else:
            raise RuntimeError("stale scheduled autopilot worker did not stop")

    quarantined: list[str] = []
    if stale and corrupt:
        if worker_running:
            raise RuntimeError("refusing to quarantine health while autopilot worker is running")
        quarantined = quarantine_corrupt_health_store(heartbeat_db, now=checked_at)

    run_return_code: int | None = None
    if should_start:
        if not task_fenced:
            raise RuntimeError("autopilot worker recovery lost its scheduler fence")
        enable_result = runner(["/Change", "/TN", worker_task_name, "/Enable"])
        if enable_result.returncode != 0:
            error = (
                enable_result.stderr or enable_result.stdout or "unknown schtasks failure"
            ).strip()
            raise RuntimeError(f"failed to re-enable scheduled autopilot worker: {error}")
        run_result = runner(["/Run", "/TN", worker_task_name])
        run_return_code = run_result.returncode
        if run_result.returncode != 0:
            error = (
                run_result.stderr or run_result.stdout or "unknown schtasks failure"
            ).strip()
            raise RuntimeError(f"failed to start scheduled autopilot worker: {error}")
        if task_runner is None and not _await_worker_runtime(
            heartbeat_db,
            previous_runtime_id=previous_runtime_id,
            sleep_fn=sleep_fn,
        ):
            raise RuntimeError(
                "scheduled autopilot worker accepted start but did not publish a new runtime"
            )
    if stale:
        action = "restart"
    elif should_start:
        action = "start"
    else:
        action = "already_running"
    return {
        "checked_at_utc": _stamp(checked_at),
        "worker_task_name": worker_task_name,
        "heartbeat_stale": stale,
        "reason": reason,
        "action": action,
        "end_return_code": end_return_code,
        "run_return_code": run_return_code,
        "quarantined": quarantined,
    }


def autopilot_health_status(path: Path | str) -> dict[str, object]:
    store_path = Path(path)
    if not store_path.is_file():
        return {"exists": False, "valid": False, "reason": "heartbeat_store_missing"}
    try:
        health = AutopilotHealthStore(store_path).read()
    except sqlite3.DatabaseError as exc:
        return {
            "exists": True,
            "valid": False,
            "reason": f"heartbeat_store_unreadable_{type(exc).__name__}",
        }
    return {"exists": True, "valid": True, "health": health}


def autopilot_runtime_budget(
    *,
    settings: Settings,
    quant_timeout_seconds: int,
) -> dict[str, float | int]:
    """Return conservative single-stage blocking budgets used by watchdog validation."""

    # Scalar HTTPX timeouts apply independently to pool/connect/write/read phases, while urllib's
    # socket timeout can apply once to connect and again to response reads. Budget those legitimate
    # phases explicitly; the stale watchdog remains the hard wall for slow-drip responses.
    sec_window = settings.sec_retry_attempts * (
        HTTPX_TIMEOUT_PHASES * settings.sec_timeout_seconds
        + 1 / settings.sec_rate_limit_per_second
    ) + (settings.sec_retry_attempts - 1) * settings.sec_retry_max_seconds
    ib_window = settings.market_data_retry_attempts * (
        3 * AUTOPILOT_IB_REQUEST_TIMEOUT_SECONDS
        + 1 / settings.market_data_rate_limit_per_second
    ) + (settings.market_data_retry_attempts - 1) * settings.market_data_retry_max_seconds
    yahoo_window = settings.market_data_retry_attempts * (
        URLLIB_TIMEOUT_PHASES * settings.market_data_timeout_seconds
        + 1 / settings.market_data_rate_limit_per_second
    ) + (settings.market_data_retry_attempts - 1) * settings.market_data_retry_max_seconds
    review_item_window = (
        sec_window + ib_window + yahoo_window + SQLITE_REVIEW_ITEM_BUDGET_SECONDS
    )
    option_chain_window = (
        OPTION_CHAIN_SQLITE_BUDGET_SECONDS
        + OPTION_CHAIN_CAPTURE_TIMEOUT_SECONDS
        + OPTION_CHAIN_CLEANUP_BUDGET_SECONDS
    )
    notification_window = (
        settings.ntfy_retry_attempts
        * HTTPX_TIMEOUT_PHASES
        * settings.ntfy_timeout_seconds
        + (settings.ntfy_retry_attempts - 1) * settings.ntfy_retry_max_seconds
        + NOTIFICATION_OBSERVER_BUDGET_SECONDS
        + NOTIFICATION_ACK_SQLITE_BUDGET_SECONDS
    )
    quant_window = quant_timeout_seconds + 20
    maximum_stage = max(
        sec_window,
        review_item_window,
        option_chain_window,
        REVIEW_DECISION_SQLITE_BUDGET_SECONDS,
        notification_window,
        quant_window,
        SQLITE_STAGE_BUDGET_SECONDS,
    )
    required_stale = max(
        MIN_STALE_SECONDS,
        math.ceil(maximum_stage + STALE_SAFETY_MARGIN_SECONDS),
    )
    return {
        "sec_window_seconds": round(sec_window, 3),
        "review_item_window_seconds": round(review_item_window, 3),
        "option_chain_window_seconds": round(option_chain_window, 3),
        "decision_write_window_seconds": REVIEW_DECISION_SQLITE_BUDGET_SECONDS,
        "notification_window_seconds": round(notification_window, 3),
        "quant_window_seconds": quant_window,
        "sqlite_window_seconds": SQLITE_STAGE_BUDGET_SECONDS,
        "maximum_stage_seconds": round(maximum_stage, 3),
        "required_stale_seconds": required_stale,
    }


def sec_ingestion_runtime_budget(*, settings: Settings) -> dict[str, float | int]:
    """Return the largest legitimate blocking stage for the isolated SEC worker."""

    sec_window = settings.sec_retry_attempts * (
        HTTPX_TIMEOUT_PHASES * settings.sec_timeout_seconds
        + 1 / settings.sec_rate_limit_per_second
    ) + (settings.sec_retry_attempts - 1) * settings.sec_retry_max_seconds
    ib_window = settings.market_data_retry_attempts * (
        3 * AUTOPILOT_IB_REQUEST_TIMEOUT_SECONDS
        + 1 / settings.market_data_rate_limit_per_second
    ) + (settings.market_data_retry_attempts - 1) * settings.market_data_retry_max_seconds
    yahoo_window = settings.market_data_retry_attempts * (
        URLLIB_TIMEOUT_PHASES * settings.market_data_timeout_seconds
        + 1 / settings.market_data_rate_limit_per_second
    ) + (settings.market_data_retry_attempts - 1) * settings.market_data_retry_max_seconds
    review_item_window = (
        sec_window + ib_window + yahoo_window + SQLITE_REVIEW_ITEM_BUDGET_SECONDS
    )
    maximum_stage = max(sec_window, review_item_window, SQLITE_STAGE_BUDGET_SECONDS)
    required_stale = max(
        MIN_STALE_SECONDS,
        math.ceil(maximum_stage + STALE_SAFETY_MARGIN_SECONDS),
    )
    return {
        "sec_window_seconds": round(sec_window, 3),
        "review_item_window_seconds": round(review_item_window, 3),
        "sqlite_window_seconds": SQLITE_STAGE_BUDGET_SECONDS,
        "maximum_stage_seconds": round(maximum_stage, 3),
        "required_stale_seconds": required_stale,
    }


def validate_sec_ingestion_stale_threshold(
    *,
    stale_seconds: int,
    settings: Settings,
) -> None:
    minimum = int(sec_ingestion_runtime_budget(settings=settings)["required_stale_seconds"])
    if stale_seconds < minimum:
        raise ValueError(
            f"SEC ingestion heartbeat stale threshold must be at least {minimum} seconds "
            "for bounded external calls"
        )


def validate_stale_threshold(
    *,
    quant_timeout_seconds: int,
    stale_seconds: int,
    settings: Settings | None = None,
) -> None:
    minimum = (
        int(
            autopilot_runtime_budget(
                settings=settings,
                quant_timeout_seconds=quant_timeout_seconds,
            )["required_stale_seconds"]
        )
        if settings is not None
        else max(MIN_STALE_SECONDS, quant_timeout_seconds + 90)
    )
    if stale_seconds < minimum:
        raise ValueError(
            f"heartbeat stale threshold must be at least {minimum} seconds "
            "for bounded external calls and the configured quant timeout"
        )
