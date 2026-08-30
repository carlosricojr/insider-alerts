from __future__ import annotations

import sqlite3
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from insider_alerts.config import Settings
from insider_alerts.execution import autopilot_watchdog
from insider_alerts.execution.autopilot_watchdog import (
    AutopilotHealthStore,
    RuntimeOwnershipError,
    autopilot_health_status,
    heartbeat_state,
    notification_runtime_configuration_fingerprint,
    quarantine_corrupt_health_store,
    run_autopilot_watchdog,
    validate_stale_threshold,
)


def _task_result(state: str) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess([], 0, f'"host","task","next","{state}"\n', "")


def test_notification_configuration_fingerprint_binds_exact_startup_paths(
    tmp_path: Path,
) -> None:
    first = Settings(
        _env_file=None,
        DATABASE_PATH="data/producer-a.db",
        NOTIFICATION_TRANSPORT_DB="data/journal.db",
        NOTIFICATION_TRANSPORT_POLICY_PATH="policy.json",
    )
    second = Settings(
        _env_file=None,
        DATABASE_PATH="data/producer-b.db",
        NOTIFICATION_TRANSPORT_DB="data/journal.db",
        NOTIFICATION_TRANSPORT_POLICY_PATH="policy.json",
    )

    first_fingerprint = notification_runtime_configuration_fingerprint(
        first, repo_root=tmp_path
    )

    assert len(first_fingerprint) == 64
    assert first_fingerprint != notification_runtime_configuration_fingerprint(
        second, repo_root=tmp_path
    )


def test_health_store_migrates_v1_as_unbound_before_new_runtime_registration(
    tmp_path: Path,
) -> None:
    path = tmp_path / "health.db"
    now = datetime(2026, 8, 30, 18, 0, tzinfo=UTC)
    stamp = now.isoformat(timespec="microseconds").replace("+00:00", "Z")
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE autopilot_health (
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
            )
            """
        )
        conn.execute(
            "INSERT INTO autopilot_health VALUES(1,1,?,?,?,?,?,NULL,NULL,NULL,NULL)",
            ("old-runtime", stamp, "a" * 64, stamp, "cycle_wait"),
        )

    store = AutopilotHealthStore(path)
    store.initialize()
    migrated = store.read()
    assert migrated["schema_version"] == 2
    assert migrated["runtime_configuration_fingerprint"] == "unbound"

    store.register_runtime(
        runtime_id="new-runtime",
        source_fingerprint="b" * 64,
        runtime_configuration_fingerprint="c" * 64,
        now=now + timedelta(seconds=1),
    )
    rebound = store.read()
    assert rebound["runtime_configuration_fingerprint"] == "c" * 64


def test_health_store_repairs_interrupted_legacy_migration(tmp_path: Path) -> None:
    path = tmp_path / "health.db"
    now = datetime(2026, 8, 30, 18, 0, tzinfo=UTC)
    store = AutopilotHealthStore(path)
    store.register_runtime(runtime_id="runtime", source_fingerprint="a" * 64, now=now)
    with sqlite3.connect(path) as conn:
        conn.execute(
            "UPDATE autopilot_health SET schema_version=1, "
            "runtime_configuration_fingerprint='ambiguous'"
        )

    store.initialize()

    repaired = store.read()
    assert repaired["schema_version"] == 2
    assert repaired["runtime_configuration_fingerprint"] == "unbound"


def test_health_store_rolls_back_legacy_column_when_version_update_fails(
    tmp_path: Path,
) -> None:
    path = tmp_path / "health.db"
    now = datetime(2026, 8, 30, 18, 0, tzinfo=UTC)
    stamp = now.isoformat(timespec="microseconds").replace("+00:00", "Z")
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE autopilot_health (
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
            CREATE TRIGGER reject_health_update
            BEFORE UPDATE ON autopilot_health BEGIN
              SELECT RAISE(ABORT, 'injected migration failure');
            END;
            """
        )
        conn.execute(
            "INSERT INTO autopilot_health VALUES(1,1,?,?,?,?,?,NULL,NULL,NULL,NULL)",
            ("old-runtime", stamp, "a" * 64, stamp, "cycle_wait"),
        )

    with pytest.raises(sqlite3.IntegrityError, match="injected migration failure"):
        AutopilotHealthStore(path).initialize()

    with sqlite3.connect(path) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(autopilot_health)")}
        version = conn.execute("SELECT schema_version FROM autopilot_health").fetchone()[0]
    assert "runtime_configuration_fingerprint" not in columns
    assert version == 1


def test_unbound_runtime_never_masquerades_as_configuration_fingerprint(
    tmp_path: Path,
) -> None:
    store = AutopilotHealthStore(tmp_path / "health.db")
    store.register_runtime(
        runtime_id="runtime",
        source_fingerprint="a" * 64,
        now=datetime(2026, 8, 30, 18, 0, tzinfo=UTC),
    )

    assert store.read()["runtime_configuration_fingerprint"] == "unbound"


def test_task_state_api_does_not_depend_on_localized_display_text() -> None:
    running = subprocess.CompletedProcess([], 0, "__TASK_STATE__=4", "")
    ready = subprocess.CompletedProcess([], 0, "__TASK_STATE__=3", "")

    assert autopilot_watchdog._task_is_running(running) is True
    assert autopilot_watchdog._task_is_running(ready) is False


def test_task_query_uses_hidden_scheduler_state_api(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def run(command, **kwargs):  # type: ignore[no-untyped-def]
        captured["command"] = command
        captured.update(kwargs)
        return subprocess.CompletedProcess(command, 0, "__TASK_STATE__=4", "")

    monkeypatch.setattr(autopilot_watchdog.subprocess, "run", run)

    result = autopilot_watchdog._run_schtasks(
        ["/Query", "/TN", "Arbeitnehmerüberwachung", "/FO", "CSV"]
    )

    assert result.stdout == "__TASK_STATE__=4"
    assert captured["command"][0] == "powershell.exe"  # type: ignore[index]
    assert captured["timeout"] == 5
    assert captured["creationflags"] == getattr(subprocess, "CREATE_NO_WINDOW", 0)
    assert captured["env"]["INSIDER_ALERTS_TASK_QUERY_NAME"] == "Arbeitnehmerüberwachung"  # type: ignore[index]


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


def test_health_store_preserves_last_error_until_a_cycle_succeeds(tmp_path: Path) -> None:
    now = datetime(2026, 8, 28, 9, 0, tzinfo=UTC)
    store = AutopilotHealthStore(tmp_path / "health.db")
    store.register_runtime(runtime_id="runtime-a", source_fingerprint="a" * 64, now=now)
    store.progress(
        runtime_id="runtime-a",
        stage="cycle_retryable_failure",
        now=now + timedelta(seconds=1),
        error=RuntimeError("upstream unavailable"),
    )
    store.progress(
        runtime_id="runtime-a",
        stage="cycle_wait",
        now=now + timedelta(seconds=2),
    )
    store.register_runtime(
        runtime_id="runtime-b",
        source_fingerprint="b" * 64,
        now=now + timedelta(seconds=3),
    )

    health = store.read()
    assert health["last_error_kind"] == "RuntimeError"
    assert health["last_error_message"] == "upstream unavailable"

    store.progress(
        runtime_id="runtime-b",
        stage="cycle_succeeded",
        now=now + timedelta(seconds=4),
        cycle_succeeded=True,
    )
    health = store.read()
    assert health["last_error_kind"] is None
    assert health["last_error_message"] is None


def test_health_store_records_error_without_advancing_progress(tmp_path: Path) -> None:
    now = datetime(2026, 8, 28, 9, 0, tzinfo=UTC)
    store = AutopilotHealthStore(tmp_path / "health.db")
    store.register_runtime(runtime_id="runtime-a", source_fingerprint="a" * 64, now=now)
    store.progress(
        runtime_id="runtime-a",
        stage="cycle_started",
        now=now + timedelta(seconds=1),
        cycle_started=True,
    )
    before = store.read()

    store.progress(
        runtime_id="runtime-a",
        stage="cycle_retryable_failure",
        now=now + timedelta(seconds=2),
        error=RuntimeError("upstream unavailable"),
        advance_progress=False,
    )

    after = store.read()
    assert after["last_progress_utc"] == before["last_progress_utc"]
    assert after["last_progress_stage"] == "cycle_started"
    assert after["last_error_kind"] == "RuntimeError"
    assert after["last_error_message"] == "upstream unavailable"


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
            _task_result("Running"),
            subprocess.CompletedProcess([], 0, "SUCCESS", ""),
            _task_result("Ready"),
            subprocess.CompletedProcess([], 0, "SUCCESS", ""),
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
    assert [call[0] for call in calls] == [
        "/Query",
        "/Change",
        "/Query",
        "/End",
        "/Query",
        "/Change",
        "/Run",
    ]
    assert calls[1][-1] == "/Disable"
    assert calls[-2][-1] == "/Enable"


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
        if arguments[0] == "/Change":
            return subprocess.CompletedProcess([], 0, "SUCCESS", "")
        return subprocess.CompletedProcess([], 1, "", "access denied")

    with pytest.raises(RuntimeError, match="failed to stop stale"):
        run_autopilot_watchdog(
            heartbeat_db=path,
            worker_task_name="Autopilot Worker",
            stale_seconds=300,
            now=now,
            task_runner=runner,
        )

    assert [call[0] for call in calls] == ["/Query", "/Change", "/Query", "/End"]


def test_watchdog_never_stops_or_quarantines_without_scheduler_fence(tmp_path: Path) -> None:
    path = tmp_path / "health.db"
    path.write_bytes(b"not sqlite")
    calls: list[list[str]] = []

    def runner(arguments):  # type: ignore[no-untyped-def]
        calls.append(list(arguments))
        if arguments[0] == "/Query":
            return _task_result("Running")
        return subprocess.CompletedProcess([], 1, "", "disable denied")

    with pytest.raises(RuntimeError, match="failed to fence"):
        run_autopilot_watchdog(
            heartbeat_db=path,
            worker_task_name="Autopilot Worker",
            stale_seconds=300,
            now=datetime(2026, 8, 28, 9, 0, tzinfo=UTC),
            task_runner=runner,
        )

    assert [call[0] for call in calls] == ["/Query", "/Change"]
    assert path.read_bytes() == b"not sqlite"


def test_watchdog_never_starts_when_reenable_fails(tmp_path: Path) -> None:
    calls: list[list[str]] = []
    results = iter(
        [
            _task_result("Ready"),
            subprocess.CompletedProcess([], 0, "SUCCESS", ""),
            _task_result("Ready"),
            subprocess.CompletedProcess([], 1, "", "enable denied"),
        ]
    )

    def runner(arguments):  # type: ignore[no-untyped-def]
        calls.append(list(arguments))
        return next(results)

    with pytest.raises(RuntimeError, match="failed to re-enable"):
        run_autopilot_watchdog(
            heartbeat_db=tmp_path / "missing.db",
            worker_task_name="Autopilot Worker",
            stale_seconds=300,
            now=datetime(2026, 8, 28, 9, 0, tzinfo=UTC),
            task_runner=runner,
        )

    assert [call[0] for call in calls] == ["/Query", "/Change", "/Query", "/Change"]
    assert calls[-1][-1] == "/Enable"


def test_watchdog_propagates_query_failure_without_starting(tmp_path: Path) -> None:
    calls: list[list[str]] = []

    def runner(arguments):  # type: ignore[no-untyped-def]
        calls.append(list(arguments))
        return subprocess.CompletedProcess([], 1, "", "query denied")

    with pytest.raises(RuntimeError, match="failed to query"):
        run_autopilot_watchdog(
            heartbeat_db=tmp_path / "missing.db",
            worker_task_name="Autopilot Worker",
            stale_seconds=300,
            now=datetime(2026, 8, 28, 9, 0, tzinfo=UTC),
            task_runner=runner,
        )

    assert [call[0] for call in calls] == ["/Query"]


def test_scheduled_task_control_timeout_is_bounded_and_actionable(monkeypatch) -> None:
    def timeout(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        raise subprocess.TimeoutExpired("schtasks.exe", 5)

    monkeypatch.setattr(autopilot_watchdog.subprocess, "run", timeout)

    with pytest.raises(RuntimeError, match=r"control /Query timed out after 5 seconds"):
        autopilot_watchdog._run_schtasks(["/Query", "/TN", "Autopilot Worker"])


def test_watchdog_propagates_start_failure(tmp_path: Path) -> None:
    calls: list[list[str]] = []

    def runner(arguments):  # type: ignore[no-untyped-def]
        calls.append(list(arguments))
        if arguments[0] == "/Query":
            return _task_result("Ready")
        if arguments[0] == "/Change":
            return subprocess.CompletedProcess([], 0, "SUCCESS", "")
        return subprocess.CompletedProcess([], 1, "", "start denied")

    with pytest.raises(RuntimeError, match="failed to start"):
        run_autopilot_watchdog(
            heartbeat_db=tmp_path / "missing.db",
            worker_task_name="Autopilot Worker",
            stale_seconds=300,
            now=datetime(2026, 8, 28, 9, 0, tzinfo=UTC),
            task_runner=runner,
        )

    assert [call[0] for call in calls] == [
        "/Query",
        "/Change",
        "/Query",
        "/Change",
        "/Run",
    ]


def test_watchdog_propagates_quarantine_failure_without_starting(
    monkeypatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "health.db"
    path.write_bytes(b"not sqlite")
    calls: list[list[str]] = []

    def runner(arguments):  # type: ignore[no-untyped-def]
        calls.append(list(arguments))
        if arguments[0] == "/Query":
            return _task_result("Ready")
        return subprocess.CompletedProcess([], 0, "SUCCESS", "")

    def fail_replace(_source, _destination) -> None:  # type: ignore[no-untyped-def]
        raise OSError("quarantine denied")

    monkeypatch.setattr("insider_alerts.execution.autopilot_watchdog.os.replace", fail_replace)
    with pytest.raises(OSError, match="quarantine denied"):
        run_autopilot_watchdog(
            heartbeat_db=path,
            worker_task_name="Autopilot Worker",
            stale_seconds=300,
            now=datetime(2026, 8, 28, 9, 0, tzinfo=UTC),
            task_runner=runner,
        )

    assert [call[0] for call in calls] == ["/Query", "/Change", "/Query"]
    assert calls[1][-1] == "/Disable"


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
    assert [call[0] for call in calls] == [
        "/Query",
        "/Change",
        "/Query",
        "/Change",
        "/Run",
    ]
    assert calls[1][-1] == "/Disable"
    assert calls[-2][-1] == "/Enable"
    quarantined = result["quarantined"]
    assert isinstance(quarantined, list) and len(quarantined) == 1
    assert Path(quarantined[0]).read_bytes() == b"not sqlite"
    assert not path.exists()


def test_quarantine_manifest_resumes_after_partial_bundle_move(
    monkeypatch,
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 28, 9, 0, tzinfo=UTC)
    path = tmp_path / "health.db"
    path.write_bytes(b"corrupt main")
    Path(f"{path}-wal").write_bytes(b"wal")
    Path(f"{path}-shm").write_bytes(b"shm")
    real_replace = autopilot_watchdog.os.replace
    moves: list[str] = []

    failed_once = False

    def fail_on_shm_once(source, destination) -> None:  # type: ignore[no-untyped-def]
        nonlocal failed_once
        source_path = Path(source)
        if source_path.name.startswith(".health.db.quarantine.json"):
            real_replace(source_path, destination)
            return
        moves.append(source_path.name)
        if source_path == Path(f"{path}-shm") and not failed_once:
            failed_once = True
            raise OSError("shm quarantine denied")
        real_replace(source_path, destination)

    monkeypatch.setattr(autopilot_watchdog.os, "replace", fail_on_shm_once)

    with pytest.raises(OSError, match="shm quarantine denied"):
        quarantine_corrupt_health_store(path, now=now)

    assert moves == ["health.db", "health.db-wal", "health.db-shm"]
    manifest = Path(f"{path.resolve()}.quarantine.json")
    assert manifest.is_file()
    assert heartbeat_state(path, now=now, stale_seconds=300) == (
        True,
        "heartbeat_quarantine_incomplete",
        True,
    )
    assert not path.exists()
    assert not Path(f"{path}-wal").exists()
    assert Path(f"{path}-shm").exists()

    quarantined = quarantine_corrupt_health_store(path, now=now + timedelta(seconds=1))

    assert len(quarantined) == 3
    assert not manifest.exists()
    assert not Path(f"{path}-shm").exists()


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

    incomplete_v1 = tmp_path / "incomplete-v1.db"
    with sqlite3.connect(incomplete_v1) as conn:
        conn.execute(
            "CREATE TABLE autopilot_health(singleton INTEGER, schema_version INTEGER)"
        )
        conn.execute("INSERT INTO autopilot_health VALUES(1,1)")
    stale, reason, corrupt = heartbeat_state(
        incomplete_v1,
        now=now,
        stale_seconds=300,
    )
    assert stale is True
    assert reason == "heartbeat_store_corrupt_DatabaseError"
    assert corrupt is True


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
    with pytest.raises(ValueError, match="at least 300"):
        validate_stale_threshold(quant_timeout_seconds=120, stale_seconds=299)
    validate_stale_threshold(quant_timeout_seconds=120, stale_seconds=300)


def test_worker_runtime_verification_requires_changed_runtime_id(tmp_path: Path) -> None:
    path = tmp_path / "health.db"
    now = datetime(2026, 8, 28, 9, 0, tzinfo=UTC)
    store = AutopilotHealthStore(path)
    store.register_runtime(runtime_id="old", source_fingerprint="a" * 64, now=now)
    sleeps = 0

    def replace_runtime(_seconds: float) -> None:
        nonlocal sleeps
        sleeps += 1
        if sleeps == 1:
            store.register_runtime(
                runtime_id="new",
                source_fingerprint="b" * 64,
                now=now + timedelta(seconds=1),
            )

    assert autopilot_watchdog._await_worker_runtime(
        path,
        previous_runtime_id="old",
        sleep_fn=replace_runtime,
        attempts=3,
    )
    assert sleeps == 1
