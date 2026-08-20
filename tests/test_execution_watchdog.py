import subprocess
from datetime import UTC, datetime, timedelta

import pytest

from insider_alerts.execution.canary import CanaryStore
from insider_alerts.execution.watchdog import (
    heartbeat_is_stale,
    run_scheduled_task_watchdog,
)


def test_heartbeat_staleness_uses_newest_cycle_timestamp() -> None:
    now = datetime(2026, 8, 20, 18, 0, tzinfo=UTC)
    stale, reason = heartbeat_is_stale(
        {
            "last_cycle_success_utc": (now - timedelta(minutes=5)).isoformat(),
            "last_cycle_started_utc": (now - timedelta(seconds=15)).isoformat(),
        },
        now=now,
        stale_seconds=120,
    )

    assert stale is False
    assert reason == "last_cycle_started_utc_fresh_15s"


def test_watchdog_ends_and_restarts_worker_when_heartbeat_is_stale(tmp_path) -> None:
    now = datetime(2026, 8, 20, 18, 0, tzinfo=UTC)
    ledger = tmp_path / "ledger.db"
    store = CanaryStore(str(ledger))
    store.set_metadata(
        {"last_cycle_success_utc": (now - timedelta(minutes=5)).isoformat()},
        now=now,
    )
    calls: list[list[str]] = []
    results = iter(
        [
            subprocess.CompletedProcess([], 0, '"host","task","next","Running"\n', ""),
            subprocess.CompletedProcess([], 0, "SUCCESS", ""),
            subprocess.CompletedProcess([], 0, '"host","task","next","Ready"\n', ""),
            subprocess.CompletedProcess([], 0, "SUCCESS", ""),
        ]
    )

    def _task_runner(arguments):  # type: ignore[no-untyped-def]
        calls.append(list(arguments))
        return next(results)

    result = run_scheduled_task_watchdog(
        ledger_db=str(ledger),
        worker_task_name="Canary Worker",
        stale_seconds=120,
        now=now,
        task_runner=_task_runner,
    )

    assert result["action"] == "restart"
    assert calls == [
        ["/Query", "/TN", "Canary Worker", "/FO", "CSV", "/NH", "/V"],
        ["/End", "/TN", "Canary Worker"],
        ["/Query", "/TN", "Canary Worker", "/FO", "CSV", "/NH", "/V"],
        ["/Run", "/TN", "Canary Worker"],
    ]


def test_watchdog_only_ensures_worker_when_heartbeat_is_fresh(tmp_path) -> None:
    now = datetime(2026, 8, 20, 18, 0, tzinfo=UTC)
    ledger = tmp_path / "ledger.db"
    store = CanaryStore(str(ledger))
    store.set_metadata(
        {"last_cycle_success_utc": (now - timedelta(seconds=15)).isoformat()},
        now=now,
    )
    calls: list[list[str]] = []

    def _task_runner(arguments):  # type: ignore[no-untyped-def]
        calls.append(list(arguments))
        return subprocess.CompletedProcess(
            arguments,
            0,
            '"host","task","next","Running"\n',
            "",
        )

    result = run_scheduled_task_watchdog(
        ledger_db=str(ledger),
        worker_task_name="Canary Worker",
        stale_seconds=120,
        now=now,
        task_runner=_task_runner,
    )

    assert result["action"] == "already_running"
    assert result["run_return_code"] is None
    assert calls == [
        ["/Query", "/TN", "Canary Worker", "/FO", "CSV", "/NH", "/V"]
    ]


def test_watchdog_starts_stopped_worker_when_heartbeat_is_fresh(tmp_path) -> None:
    now = datetime(2026, 8, 20, 18, 0, tzinfo=UTC)
    ledger = tmp_path / "ledger.db"
    CanaryStore(str(ledger)).set_metadata(
        {"last_cycle_success_utc": (now - timedelta(seconds=15)).isoformat()},
        now=now,
    )
    calls: list[list[str]] = []

    def _task_runner(arguments):  # type: ignore[no-untyped-def]
        calls.append(list(arguments))
        stdout = '"host","task","next","Ready"\n' if arguments[0] == "/Query" else "ok"
        return subprocess.CompletedProcess(arguments, 0, stdout, "")

    result = run_scheduled_task_watchdog(
        ledger_db=str(ledger),
        worker_task_name="Canary Worker",
        stale_seconds=120,
        now=now,
        task_runner=_task_runner,
    )

    assert result["action"] == "start"
    assert calls == [
        ["/Query", "/TN", "Canary Worker", "/FO", "CSV", "/NH", "/V"],
        ["/Run", "/TN", "Canary Worker"],
    ]


def test_watchdog_does_not_restart_when_stale_worker_cannot_be_stopped(tmp_path) -> None:
    now = datetime(2026, 8, 20, 18, 0, tzinfo=UTC)
    ledger = tmp_path / "ledger.db"
    CanaryStore(str(ledger)).set_metadata(
        {"last_cycle_success_utc": (now - timedelta(minutes=5)).isoformat()},
        now=now,
    )
    calls: list[list[str]] = []

    def _task_runner(arguments):  # type: ignore[no-untyped-def]
        calls.append(list(arguments))
        if arguments[0] == "/Query":
            return subprocess.CompletedProcess(
                arguments,
                0,
                '"host","task","next","Running"\n',
                "",
            )
        return subprocess.CompletedProcess(arguments, 1, "", "access denied")

    with pytest.raises(RuntimeError, match="failed to stop stale"):
        run_scheduled_task_watchdog(
            ledger_db=str(ledger),
            worker_task_name="Canary Worker",
            stale_seconds=120,
            now=now,
            task_runner=_task_runner,
        )

    assert [call[0] for call in calls] == ["/Query", "/End"]
