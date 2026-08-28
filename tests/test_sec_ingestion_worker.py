from __future__ import annotations

import sqlite3
from pathlib import Path

from typer.testing import CliRunner

import insider_alerts.cli as cli
from insider_alerts.sec.client import SecHttpError
from insider_alerts.sec.pipeline import EnrichResult, PollResult, QueueResult


def _zero_enrich(*_args, **_kwargs) -> EnrichResult:  # type: ignore[no-untyped-def]
    return EnrichResult(scanned=0, updated=0)


def _zero_enqueue(*_args, **_kwargs) -> QueueResult:  # type: ignore[no-untyped-def]
    return QueueResult(processed=0, enqueued=0)


def test_sec_ingestion_once_runs_only_acquisition_stages(monkeypatch, tmp_path: Path) -> None:
    calls: list[str] = []

    def poll(*_args, **_kwargs) -> PollResult:  # type: ignore[no-untyped-def]
        calls.append("poll")
        return PollResult(
            fetched=2,
            inserted=1,
            skipped_existing=1,
            source_items_seen=40,
            source_boundary_rejected=38,
        )

    def enrich(*_args, **_kwargs) -> EnrichResult:  # type: ignore[no-untyped-def]
        calls.append("enrich")
        return EnrichResult(scanned=2, updated=2)

    def enqueue(*_args, **_kwargs) -> QueueResult:  # type: ignore[no-untyped-def]
        calls.append("enqueue")
        return QueueResult(processed=2, enqueued=1, parse_failed=1)

    monkeypatch.setattr(cli, "run_sec_poll_once", poll)
    monkeypatch.setattr(cli, "enrich_filings_with_xml_url", enrich)
    monkeypatch.setattr(cli, "enqueue_review_packets", enqueue)
    output_log = tmp_path / "sec.out.log"
    error_log = tmp_path / "sec.err.log"

    result = CliRunner().invoke(
        cli.app,
        [
            "ops",
            "sec-ingestion",
            "--once",
            "--output-log",
            str(output_log),
            "--error-log",
            str(error_log),
        ],
    )

    assert result.exit_code == 0
    assert calls == ["poll", "enrich", "enqueue"]
    assert "source_boundary_rejected=38" in result.stdout
    assert "enqueue_parse_failed=1" in output_log.read_text(encoding="utf-8")
    assert "review enrichment degraded" in error_log.read_text(encoding="utf-8")


def test_sec_ingestion_once_publishes_durable_success(monkeypatch, tmp_path: Path) -> None:
    heartbeat_db = tmp_path / "health.db"
    monkeypatch.setattr(cli, "runtime_source_fingerprint", lambda: "a" * 64)
    monkeypatch.setattr(
        cli,
        "run_sec_poll_once",
        lambda *_args, **_kwargs: PollResult(0, 0, 0),
    )
    monkeypatch.setattr(cli, "enrich_filings_with_xml_url", _zero_enrich)
    monkeypatch.setattr(cli, "enqueue_review_packets", _zero_enqueue)

    result = CliRunner().invoke(
        cli.app,
        [
            "ops",
            "sec-ingestion",
            "--once",
            "--heartbeat-db",
            str(heartbeat_db),
            "--heartbeat-stale-seconds",
            "10000",
        ],
    )

    assert result.exit_code == 0
    health = cli.AutopilotHealthStore(heartbeat_db).read()
    assert health["source_fingerprint"] == "a" * 64
    assert health["last_progress_stage"] == "cycle_succeeded"
    assert health["last_cycle_started_utc"] is not None
    assert health["last_cycle_success_utc"] is not None
    assert health["last_error_kind"] is None


def test_sec_ingestion_loop_recovers_then_exits_on_source_change(
    monkeypatch, tmp_path: Path
) -> None:
    poll_calls = 0
    fingerprints = iter(("a" * 64, "a" * 64, "a" * 64, "b" * 64))
    sleeps: list[float] = []

    def poll(*_args, **_kwargs) -> PollResult:  # type: ignore[no-untyped-def]
        nonlocal poll_calls
        poll_calls += 1
        if poll_calls == 1:
            raise SecHttpError("temporary SEC outage")
        return PollResult(fetched=0, inserted=0, skipped_existing=0)

    monkeypatch.setattr(cli, "run_sec_poll_once", poll)
    monkeypatch.setattr(cli, "enrich_filings_with_xml_url", _zero_enrich)
    monkeypatch.setattr(cli, "enqueue_review_packets", _zero_enqueue)
    monkeypatch.setattr(cli, "runtime_source_fingerprint", lambda: next(fingerprints))
    monkeypatch.setattr(cli, "ensure_kill_on_close_process_tree", lambda: None)
    monkeypatch.setattr(cli.time, "sleep", lambda seconds: sleeps.append(seconds))
    error_log = tmp_path / "sec.err.log"
    heartbeat_db = tmp_path / "health.db"

    result = CliRunner().invoke(
        cli.app,
        [
            "ops",
            "sec-ingestion",
            "--loop",
            "--interval",
            "10",
            "--heartbeat-db",
            str(heartbeat_db),
            "--heartbeat-stale-seconds",
            "10000",
            "--error-log",
            str(error_log),
        ],
    )

    assert result.exit_code == 0
    assert poll_calls == 2
    assert sleeps == [10.0]
    assert "temporary SEC outage" in error_log.read_text(encoding="utf-8")
    assert "SEC ingestion source changed" in result.stderr
    health = cli.AutopilotHealthStore(heartbeat_db).read()
    assert health["last_progress_stage"] == "source_changed"
    assert health["last_cycle_success_utc"] is not None
    assert health["last_error_kind"] is None


def test_sec_ingestion_retryable_failure_stops_advancing_heartbeat(
    monkeypatch, tmp_path: Path
) -> None:
    heartbeat_db = tmp_path / "health.db"
    fingerprints = iter(("a" * 64, "b" * 64))

    monkeypatch.setattr(
        cli,
        "run_sec_poll_once",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(SecHttpError("SEC unavailable")),
    )
    monkeypatch.setattr(cli, "runtime_source_fingerprint", lambda: next(fingerprints))
    monkeypatch.setattr(cli, "ensure_kill_on_close_process_tree", lambda: None)

    result = CliRunner().invoke(
        cli.app,
        [
            "ops",
            "sec-ingestion",
            "--loop",
            "--heartbeat-db",
            str(heartbeat_db),
            "--heartbeat-stale-seconds",
            "10000",
        ],
    )

    assert result.exit_code == 0
    health = cli.AutopilotHealthStore(heartbeat_db).read()
    assert health["last_progress_stage"] == "cycle_started"
    assert health["last_error_kind"] == "SecHttpError"


def test_sec_ingestion_reasserts_ownership_before_retry_poll(
    monkeypatch, tmp_path: Path
) -> None:
    heartbeat_db = tmp_path / "health.db"
    poll_calls = 0

    def poll(*_args, **_kwargs) -> PollResult:  # type: ignore[no-untyped-def]
        nonlocal poll_calls
        poll_calls += 1
        raise SecHttpError("SEC unavailable")

    def supersede_during_wait(_seconds: float) -> None:
        cli.AutopilotHealthStore(heartbeat_db).register_runtime(
            runtime_id="replacement-runtime",
            source_fingerprint="b" * 64,
            now=cli.datetime.now(cli.UTC),
        )

    monkeypatch.setattr(cli, "run_sec_poll_once", poll)
    monkeypatch.setattr(cli, "runtime_source_fingerprint", lambda: "a" * 64)
    monkeypatch.setattr(cli, "ensure_kill_on_close_process_tree", lambda: None)
    monkeypatch.setattr(cli.time, "sleep", supersede_during_wait)
    error_log = tmp_path / "sec.err.log"

    result = CliRunner().invoke(
        cli.app,
        [
            "ops",
            "sec-ingestion",
            "--loop",
            "--interval",
            "10",
            "--heartbeat-db",
            str(heartbeat_db),
            "--heartbeat-stale-seconds",
            "10000",
            "--error-log",
            str(error_log),
        ],
    )

    assert result.exit_code == 1
    assert isinstance(result.exception, cli.RuntimeOwnershipError)
    assert poll_calls == 1
    assert cli.AutopilotHealthStore(heartbeat_db).read()["runtime_id"] == "replacement-runtime"


def test_sec_ingestion_loop_stops_when_heartbeat_ownership_is_superseded(
    monkeypatch, tmp_path: Path
) -> None:
    class SupersededStore:
        def register_runtime(self, **_kwargs) -> None:  # type: ignore[no-untyped-def]
            return None

        def progress(self, **_kwargs) -> None:  # type: ignore[no-untyped-def]
            raise cli.RuntimeOwnershipError("superseded")

    poll_called = False

    def forbidden_poll(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        nonlocal poll_called
        poll_called = True
        raise AssertionError("superseded worker must not poll")

    monkeypatch.setattr(cli, "AutopilotHealthStore", lambda _path: SupersededStore())
    monkeypatch.setattr(cli, "runtime_source_fingerprint", lambda: "a" * 64)
    monkeypatch.setattr(cli, "ensure_kill_on_close_process_tree", lambda: None)
    monkeypatch.setattr(cli, "run_sec_poll_once", forbidden_poll)
    error_log = tmp_path / "sec.err.log"

    result = CliRunner().invoke(
        cli.app,
        [
            "ops",
            "sec-ingestion",
            "--loop",
            "--heartbeat-db",
            str(tmp_path / "health.db"),
            "--heartbeat-stale-seconds",
            "10000",
            "--error-log",
            str(error_log),
        ],
    )

    assert result.exit_code == 1
    assert poll_called is False
    assert "SEC ingestion process stopped (RuntimeOwnershipError: superseded)" in (
        error_log.read_text(encoding="utf-8")
    )


def test_sec_ingestion_loop_retries_transient_sqlite_contention(monkeypatch) -> None:
    poll_calls = 0
    fingerprints = iter(("a" * 64, "b" * 64))

    def poll(*_args, **_kwargs) -> PollResult:  # type: ignore[no-untyped-def]
        nonlocal poll_calls
        poll_calls += 1
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(cli, "run_sec_poll_once", poll)
    monkeypatch.setattr(cli, "runtime_source_fingerprint", lambda: next(fingerprints))

    result = CliRunner().invoke(
        cli.app,
        ["ops", "sec-ingestion", "--loop", "--interval", "10"],
    )

    assert result.exit_code == 0
    assert poll_calls == 1
    assert "retryable, OperationalError: database is locked" in result.stderr


def test_sec_ingestion_exits_on_structural_sqlite_failure(monkeypatch, tmp_path: Path) -> None:
    def fail_poll(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        raise sqlite3.OperationalError("attempt to write a readonly database")

    monkeypatch.setattr(cli, "runtime_source_fingerprint", lambda: "a" * 64)
    monkeypatch.setattr(cli, "run_sec_poll_once", fail_poll)
    error_log = tmp_path / "sec.err.log"

    result = CliRunner().invoke(
        cli.app,
        [
            "ops",
            "sec-ingestion",
            "--loop",
            "--error-log",
            str(error_log),
        ],
    )

    assert result.exit_code == 1
    assert isinstance(result.exception, sqlite3.OperationalError)
    log = error_log.read_text(encoding="utf-8")
    assert "SEC ingestion process failed" in log
    assert "readonly database" in log
    assert "retryable" not in log


def test_sec_ingestion_exits_on_structural_heartbeat_failure(monkeypatch, tmp_path: Path) -> None:
    class ReadonlyHealthStore:
        def register_runtime(self, **_kwargs) -> None:  # type: ignore[no-untyped-def]
            return None

        def progress(self, **_kwargs) -> None:  # type: ignore[no-untyped-def]
            raise sqlite3.OperationalError("attempt to write a readonly database")

    poll_called = False

    def forbidden_poll(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        nonlocal poll_called
        poll_called = True
        raise AssertionError("structural heartbeat failure must stop before polling")

    monkeypatch.setattr(cli, "AutopilotHealthStore", lambda _path: ReadonlyHealthStore())
    monkeypatch.setattr(cli, "runtime_source_fingerprint", lambda: "a" * 64)
    monkeypatch.setattr(cli, "run_sec_poll_once", forbidden_poll)
    error_log = tmp_path / "sec.err.log"

    result = CliRunner().invoke(
        cli.app,
        [
            "ops",
            "sec-ingestion",
            "--loop",
            "--heartbeat-db",
            str(tmp_path / "health.db"),
            "--heartbeat-stale-seconds",
            "10000",
            "--error-log",
            str(error_log),
        ],
    )

    assert result.exit_code == 1
    assert isinstance(result.exception, sqlite3.OperationalError)
    assert poll_called is False
    log = error_log.read_text(encoding="utf-8")
    assert "SEC ingestion process failed" in log
    assert "readonly database" in log


def test_sec_ingestion_ownership_probe_fails_closed_on_heartbeat_lock(
    monkeypatch, tmp_path: Path
) -> None:
    class LockedHealthStore:
        def register_runtime(self, **_kwargs) -> None:  # type: ignore[no-untyped-def]
            return None

        def progress(self, **_kwargs) -> None:  # type: ignore[no-untyped-def]
            raise sqlite3.OperationalError("database is locked")

    poll_called = False

    def forbidden_poll(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        nonlocal poll_called
        poll_called = True
        raise AssertionError("an unproven owner must not poll")

    monkeypatch.setattr(cli, "AutopilotHealthStore", lambda _path: LockedHealthStore())
    monkeypatch.setattr(cli, "runtime_source_fingerprint", lambda: "a" * 64)
    monkeypatch.setattr(cli, "ensure_kill_on_close_process_tree", lambda: None)
    monkeypatch.setattr(cli, "run_sec_poll_once", forbidden_poll)

    result = CliRunner().invoke(
        cli.app,
        [
            "ops",
            "sec-ingestion",
            "--loop",
            "--heartbeat-db",
            str(tmp_path / "health.db"),
            "--heartbeat-stale-seconds",
            "10000",
        ],
    )

    assert result.exit_code == 1
    assert isinstance(result.exception, sqlite3.OperationalError)
    assert poll_called is False


def test_autopilot_external_ingestion_mode_never_calls_sec_pipeline(
    monkeypatch, tmp_path: Path
) -> None:
    def forbidden(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("decision worker must not run SEC ingestion")

    monkeypatch.setattr(cli, "run_sec_poll_once", forbidden)
    monkeypatch.setattr(cli, "enrich_filings_with_xml_url", forbidden)
    monkeypatch.setattr(cli, "enqueue_review_packets", forbidden)
    monkeypatch.setattr(cli, "list_pending_review_packets", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        cli,
        "get_settings",
        lambda: cli.Settings(_env_file=None, DATABASE_PATH=str(tmp_path / "autopilot.db")),
    )

    result = CliRunner().invoke(
        cli.app,
        [
            "ops",
            "autopilot",
            "--once",
            "--no-sec-ingestion",
            "--decision-engine",
            "rules",
            "--no-notify",
        ],
    )

    assert result.exit_code == 0
    assert "fetched=0" in result.stdout
    assert "pending_seen=0" in result.stdout


def test_sec_ingestion_watchdog_uses_dedicated_budget_and_health_store(
    monkeypatch, tmp_path: Path
) -> None:
    calls: list[dict[str, object]] = []

    def watchdog(**kwargs):  # type: ignore[no-untyped-def]
        calls.append(kwargs)
        return {"action": "already_running", "reason": "heartbeat_fresh_1s"}

    monkeypatch.setattr(cli, "run_autopilot_watchdog", watchdog)
    output_log = tmp_path / "watchdog.log"
    heartbeat_db = tmp_path / "sec-health.db"

    result = CliRunner().invoke(
        cli.app,
        [
            "ops",
            "sec-ingestion-watchdog",
            "--worker-task-name",
            "SEC Worker",
            "--heartbeat-db",
            str(heartbeat_db),
            "--stale-seconds",
            "10000",
            "--output-log",
            str(output_log),
        ],
    )

    assert result.exit_code == 0
    assert calls == [
        {
            "heartbeat_db": heartbeat_db,
            "worker_task_name": "SEC Worker",
            "stale_seconds": 10000,
        }
    ]
    assert '"action": "already_running"' in output_log.read_text(encoding="utf-8")
