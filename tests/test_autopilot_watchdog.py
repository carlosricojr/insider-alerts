from __future__ import annotations

import sqlite3
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from insider_alerts.execution.autopilot_watchdog import (
    AutopilotHealthStore,
    RuntimeOwnershipError,
    autopilot_health_status,
    heartbeat_state,
    run_autopilot_watchdog,
    validate_stale_threshold,
)


def _task_result(state: str) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess([], 0, f'"host","task","next","{state}"\n', "")


def test_health_store_fences_superseded_runtime(tmp_path: Path) -> None:
    now = datetime(2026, 8, 28, 9, 0, tzinfo=UTC)
    store = AutopilotHealthStore(tmp_path / "health.db")
    store.register_runtime(runtime_id="runtime-a", source_fingerprint="a" * 64, now=now)
    store.progress(
        runtime_id="runtime-a",
        stage="cycle_started",
        now=now + timedelta(seconds=1),
        cycle_started=True,
    )
    store.register_runtime(
        runtime_id="runtime-b",
        source_fingerprint="b" * 64,
        now=now + timedelta(seconds=2),
    )

    with pytest.raises(RuntimeOwnershipError, match="superseded"):
        store.progress(
            runtime_id="runtime-a",
            stage="late_old_write",
            now=now + timedelta(seconds=3),
        )

    health = store.read()
    assert health["runtime_id"] == "runtime-b"
    assert health["last_progress_stage"] == "runtime_started"


def test_progress_write_is_bounded_by_short_database_lock_timeout(tmp_path: Path) -> None:
    now = datetime(2026, 8, 28, 9, 0, tzinfo=UTC)
    path = tmp_path / "health.db"
    store = AutopilotHealthStore(path, busy_timeout_ms=25)
    store.register_runtime(runtime_id="runtime", source_fingerprint="a" * 64, now=now)
    locker = sqlite3.connect(path)
    locker.execute("PRAGMA busy_timeout=25")
    locker.execute("BEGIN IMMEDIATE")
    try:
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            store.progress(
                runtime_id="runtime",
                stage="locked_write",
                now=now + timedelta(seconds=1),
            )
    finally:
        locker.rollback()
        locker.close()


@pytest.mark.parametrize(
    ("raw", "reason"),
    [
        ("not-a-time", "heartbeat_invalid"),
        ("2026-08-28T09:00:01.000000Z", "heartbeat_future_untrusted"),
    ],
)
def test_heartbeat_state_fails_closed_for_invalid_or_future_time(
    tmp_path: Path,
    raw: str,
    reason: str,
) -> None:
    now = datetime(2026, 8, 28, 9, 0, tzinfo=UTC)
    path = tmp_path / "health.db"
    store = AutopilotHealthStore(path)
    store.register_runtime(runtime_id="runtime", source_fingerprint="a" * 64, now=now)
    with sqlite3.connect(path) as conn:
        conn.execute("UPDATE autopilot_health SET last_progress_utc=?", (raw,))

    assert heartbeat_state(path, now=now, stale_seconds=300) == (True, reason, False)


def test_heartbeat_state_reports_fresh_stale_and_missing(tmp_path: Path) -> None:
    now = datetime(2026, 8, 28, 9, 0, tzinfo=UTC)
    path = tmp_path / "health.db"
    assert heartbeat_state(path, now=now, stale_seconds=300) == (
        True,
        "heartbeat_store_missing",
        False,
    )
    store = AutopilotHealthStore(path)
    store.register_runtime(
        runtime_id="runtime",
        source_fingerprint="a" * 64,
        now=now - timedelta(seconds=299),
    )
    assert heartbeat_state(path, now=now, stale_seconds=300) == (
        False,
        "heartbeat_fresh_299s",
        False,
    )
    assert heartbeat_state(path, now=now + timedelta(seconds=2), stale_seconds=300) == (
        True,
        "heartbeat_stale_301s",
        False,
    )


def test_watchdog_leaves_fresh_running_worker_alone(tmp_path: Path) -> None:
    now = datetime(2026, 8, 28, 9, 0, tzinfo=UTC)
    path = tmp_path / "health.db"
    AutopilotHealthStore(path).register_runtime(
        runtime_id="runtime",
        source_fingerprint="a" * 64,
        now=now - timedelta(seconds=10),
    )
    calls: list[list[str]] = []

    def runner(arguments):  # type: ignore[no-untyped-def]
        calls.append(list(arguments))
        return _task_result("Running")

    result = run_autopilot_watchdog(
        heartbeat_db=path,
        worker_task_name="Autopilot Worker",
        stale_seconds=300,
        now=now,
        task_runner=runner,
    )

    assert result["action"] == "already_running"
    assert [call[0] for call in calls] == ["/Query"]


def test_watchdog_fully_stops_stale_worker_before_start(tmp_path: Path) -> None:
    now = datetime(2026, 8, 28, 9, 0, tzinfo=UTC)
    path = tmp_path / "health.db"
    AutopilotHealthStore(path).register_runtime(
        runtime_id="runtime",
        source_fingerprint="a" * 64,
        now=now - timedelta(minutes=6),
    )
    calls: list[list[str]] = []
    results = iter(
        [
            _task_result("Running"),
            subprocess.CompletedProcess([], 0, "SUCCESS", ""),
            _task_result("Ready"),
            subprocess.CompletedProcess([], 0, "SUCCESS", ""),
        ]
    )

    def runner(arguments):  # type: ignore[no-untyped-def]
        calls.append(list(arguments))
        return next(results)

    result = run_autopilot_watchdog(
        heartbeat_db=path,
        worker_task_name="Autopilot Worker",
        stale_seconds=300,
        now=now,
        task_runner=runner,
    )

    assert result["action"] == "restart"
    assert [call[0] for call in calls] == ["/Query", "/End", "/Query", "/Run"]


def test_watchdog_never_starts_replacement_after_stop_failure(tmp_path: Path) -> None:
    now = datetime(2026, 8, 28, 9, 0, tzinfo=UTC)
    path = tmp_path / "health.db"
    AutopilotHealthStore(path).register_runtime(
        runtime_id="runtime",
        source_fingerprint="a" * 64,
        now=now - timedelta(minutes=6),
    )
    calls: list[list[str]] = []

    def runner(arguments):  # type: ignore[no-untyped-def]
        calls.append(list(arguments))
        if arguments[0] == "/Query":
            return _task_result("Running")
        return subprocess.CompletedProcess([], 1, "", "access denied")

    with pytest.raises(RuntimeError, match="failed to stop stale"):
        run_autopilot_watchdog(
            heartbeat_db=path,
            worker_task_name="Autopilot Worker",
            stale_seconds=300,
            now=now,
            task_runner=runner,
        )

    assert [call[0] for call in calls] == ["/Query", "/End"]


def test_watchdog_quarantines_corrupt_store_only_when_worker_is_stopped(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 28, 9, 0, tzinfo=UTC)
    path = tmp_path / "health.db"
    path.write_bytes(b"not sqlite")
    calls: list[list[str]] = []

    def runner(arguments):  # type: ignore[no-untyped-def]
        calls.append(list(arguments))
        if arguments[0] == "/Query":
            return _task_result("Ready")
        return subprocess.CompletedProcess([], 0, "SUCCESS", "")

    result = run_autopilot_watchdog(
        heartbeat_db=path,
        worker_task_name="Autopilot Worker",
        stale_seconds=300,
        now=now,
        task_runner=runner,
    )

    assert result["action"] == "restart"
    assert [call[0] for call in calls] == ["/Query", "/Run"]
    quarantined = result["quarantined"]
    assert isinstance(quarantined, list) and len(quarantined) == 1
    assert Path(quarantined[0]).read_bytes() == b"not sqlite"
    assert not path.exists()


def test_heartbeat_state_classifies_empty_and_malformed_stores_as_corrupt(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 28, 9, 0, tzinfo=UTC)
    empty = tmp_path / "empty.db"
    empty.touch()
    stale, reason, corrupt = heartbeat_state(empty, now=now, stale_seconds=300)
    assert stale is True
    assert reason == "heartbeat_store_corrupt_OperationalError"
    assert corrupt is True

    malformed = tmp_path / "malformed.db"
    with sqlite3.connect(malformed) as conn:
        conn.execute(
            "CREATE TABLE autopilot_health(singleton INTEGER, schema_version TEXT)"
        )
        conn.execute("INSERT INTO autopilot_health VALUES(1,NULL)")
    stale, reason, corrupt = heartbeat_state(malformed, now=now, stale_seconds=300)
    assert stale is True
    assert reason == "heartbeat_store_corrupt_DatabaseError"
    assert corrupt is True
    assert autopilot_health_status(malformed)["valid"] is False


def test_heartbeat_state_does_not_restart_for_transient_locked_store(
    monkeypatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "health.db"
    path.touch()

    def locked(_self) -> dict[str, object]:
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(AutopilotHealthStore, "read", locked)
    with pytest.raises(sqlite3.OperationalError, match="locked"):
        heartbeat_state(
            path,
            now=datetime(2026, 8, 28, 9, 0, tzinfo=UTC),
            stale_seconds=300,
        )


def test_status_and_stale_threshold_are_fail_closed(tmp_path: Path) -> None:
    path = tmp_path / "health.db"
    assert autopilot_health_status(path)["valid"] is False
    with pytest.raises(ValueError, match="at least 190"):
        validate_stale_threshold(quant_timeout_seconds=120, stale_seconds=189)
    validate_stale_threshold(quant_timeout_seconds=120, stale_seconds=300)
