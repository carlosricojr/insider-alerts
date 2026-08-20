from __future__ import annotations

import csv
import io
import json
import subprocess
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path

from insider_alerts.execution.canary import CanaryStore

TaskRunner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]


def _task_is_running(result: subprocess.CompletedProcess[str]) -> bool:
    if result.returncode != 0:
        error = (result.stderr or result.stdout or "unknown schtasks failure").strip()
        raise RuntimeError(f"failed to query scheduled canary worker: {error}")
    rows = csv.reader(io.StringIO(result.stdout))
    return any(len(row) > 3 and row[3].strip().casefold() == "running" for row in rows)


def heartbeat_is_stale(
    metadata: dict[str, str],
    *,
    now: datetime,
    stale_seconds: int,
) -> tuple[bool, str]:
    if stale_seconds < 1:
        raise ValueError("stale_seconds must be positive")
    timestamps: list[tuple[str, datetime]] = []
    for key in ("last_cycle_started_utc", "last_cycle_success_utc"):
        value = metadata.get(key)
        if not value:
            continue
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        timestamps.append((key, parsed.astimezone(UTC)))
    if not timestamps:
        return True, "heartbeat_missing"
    newest_key, newest = max(timestamps, key=lambda item: item[1])
    age = (now.astimezone(UTC) - newest).total_seconds()
    if age > stale_seconds:
        return True, f"{newest_key}_stale_{int(age)}s"
    return False, f"{newest_key}_fresh_{max(0, int(age))}s"


def _run_schtasks(arguments: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["schtasks.exe", *arguments],
        check=False,
        capture_output=True,
        text=True,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )


def run_scheduled_task_watchdog(
    *,
    ledger_db: str,
    worker_task_name: str,
    stale_seconds: int,
    now: datetime | None = None,
    task_runner: TaskRunner | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
    stop_poll_attempts: int = 20,
) -> dict[str, object]:
    if stop_poll_attempts < 1:
        raise ValueError("stop_poll_attempts must be positive")
    store = CanaryStore(ledger_db)
    with store.connect() as conn:
        metadata = {
            str(row["key"]): str(row["value"])
            for row in conn.execute("SELECT key,value FROM metadata")
        }
    checked_at = (now or datetime.now(UTC)).astimezone(UTC)
    stale, reason = heartbeat_is_stale(
        metadata,
        now=checked_at,
        stale_seconds=stale_seconds,
    )
    runner = task_runner or _run_schtasks
    end_return_code: int | None = None
    query_arguments = ["/Query", "/TN", worker_task_name, "/FO", "CSV", "/NH", "/V"]
    worker_running = _task_is_running(runner(query_arguments))
    if stale and worker_running:
        end_result = runner(["/End", "/TN", worker_task_name])
        end_return_code = end_result.returncode
        if end_result.returncode != 0:
            error = (
                end_result.stderr or end_result.stdout or "unknown schtasks failure"
            ).strip()
            raise RuntimeError(f"failed to stop stale scheduled canary worker: {error}")
        for attempt in range(stop_poll_attempts):
            if not _task_is_running(runner(query_arguments)):
                break
            if attempt + 1 < stop_poll_attempts:
                sleep_fn(0.25)
        else:
            raise RuntimeError("stale scheduled canary worker did not stop")
    should_start = stale or not worker_running
    run_return_code: int | None = None
    if should_start:
        run_result = runner(["/Run", "/TN", worker_task_name])
        run_return_code = run_result.returncode
        if run_result.returncode != 0:
            error = (run_result.stderr or run_result.stdout or "unknown schtasks failure").strip()
            raise RuntimeError(f"failed to start scheduled canary worker: {error}")
    if stale:
        action = "restart"
        event_type = "watchdog_worker_restarted"
    elif should_start:
        action = "start"
        event_type = "watchdog_worker_started"
    else:
        action = "already_running"
        event_type = "watchdog_worker_ensured"
    store.event(event_type, level="critical" if stale else "info", reason=reason)
    return {
        "checked_at_utc": checked_at.isoformat(),
        "worker_task_name": worker_task_name,
        "heartbeat_stale": stale,
        "reason": reason,
        "action": action,
        "end_return_code": end_return_code,
        "run_return_code": run_return_code,
    }


def append_watchdog_log(path: Path, result: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(result, sort_keys=True) + "\n")
