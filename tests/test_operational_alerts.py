from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import monotonic

import pytest
from typer.testing import CliRunner

from insider_alerts import cli
from insider_alerts.config import Settings
from insider_alerts.execution import operational_alerts as alert_module
from insider_alerts.execution.canary import CanaryStore, CycleResult
from insider_alerts.execution.errors import IbkrExecutionError
from insider_alerts.execution.operational_alerts import (
    OperationalIncidentTracker,
    OperationalNotificationAction,
    classify_operational_failure,
    operational_incident_status,
    operational_notification_content,
    send_operational_notification,
)
from insider_alerts.notify.ntfy import NtfyDeliveryReceipt, NtfyNotificationError


def _receipt(now: datetime, body: str = "a" * 64) -> NtfyDeliveryReceipt:
    return NtfyDeliveryReceipt(
        attempt_number=1,
        responded_at_utc=now,
        request_body_sha256=body,
        route_sha256="b" * 64,
        http_status=200,
    )


def _dispatch_success(monkeypatch, responded_at: datetime, body: str = "a" * 64) -> None:
    async def send(settings, action):  # type: ignore[no-untyped-def]
        return _receipt(responded_at, body)

    monkeypatch.setattr(alert_module, "send_operational_notification", send)


def test_outage_threshold_durable_dedupe_and_recovery(monkeypatch, tmp_path) -> None:
    started = datetime(2026, 9, 1, 4, 0, tzinfo=UTC)
    ledger = tmp_path / "canary.db"
    CanaryStore(str(ledger))
    tracker = OperationalIncidentTracker(ledger)

    assert tracker.record_failure("ibkr_gateway_unavailable", now=started) is None
    assert (
        tracker.record_failure(
            "ibkr_gateway_unavailable", now=started + timedelta(seconds=299)
        )
        is None
    )
    outage = tracker.record_failure(
        "ibkr_gateway_unavailable", now=started + timedelta(seconds=300)
    )
    assert outage is not None and outage.phase == "outage"
    restarted_tracker = OperationalIncidentTracker(ledger)
    assert (
        restarted_tracker.record_failure(
            "ibkr_gateway_unavailable", now=started + timedelta(seconds=301)
        )
        is None
    )
    _dispatch_success(monkeypatch, started + timedelta(seconds=302))
    assert asyncio.run(restarted_tracker.dispatch(Settings(), outage)).status == "delivered"
    assert (
        restarted_tracker.record_failure(
            "ibkr_gateway_unavailable", now=started + timedelta(minutes=20)
        )
        is None
    )

    recovery = restarted_tracker.record_success(now=started + timedelta(minutes=21))
    assert recovery is not None and recovery.phase == "recovery"
    _dispatch_success(
        monkeypatch, started + timedelta(minutes=21, seconds=1), "c" * 64
    )
    assert asyncio.run(restarted_tracker.dispatch(Settings(), recovery)).status == "delivered"
    assert restarted_tracker.record_success(now=started + timedelta(minutes=22)) is None

    status = operational_incident_status(ledger)
    assert status["available"] is True
    assert status["active"] is None
    assert status["latest"] == {
        "incident_id": outage.incident_id,
        "started_at": started.isoformat(),
        "last_failure_at": (started + timedelta(minutes=20)).isoformat(),
        "failure_count": 5,
        "latest_failure_kind": "ibkr_gateway_unavailable",
        "outage_notified_at": (started + timedelta(seconds=302)).isoformat(),
        "recovered_at": (started + timedelta(minutes=21)).isoformat(),
        "recovery_notified_at": (
            started + timedelta(minutes=21, seconds=1)
        ).isoformat(),
        "recovery_abandoned_at": None,
    }


def test_tracker_closes_every_sqlite_connection(monkeypatch, tmp_path) -> None:
    started = datetime(2026, 9, 1, 4, 0, tzinfo=UTC)
    ledger = tmp_path / "canary.db"
    CanaryStore(str(ledger))
    tracker = OperationalIncidentTracker(ledger)
    connections: list[sqlite3.Connection] = []
    real_connect = tracker._connect

    def tracked_connect() -> sqlite3.Connection:
        connection = real_connect()
        connections.append(connection)
        return connection

    monkeypatch.setattr(tracker, "_connect", tracked_connect)
    tracker.record_failure("ibkr_gateway_unavailable", now=started)
    outage = tracker.record_failure(
        "ibkr_gateway_unavailable", now=started + timedelta(minutes=5)
    )
    assert outage is not None
    _dispatch_success(monkeypatch, started + timedelta(minutes=5, seconds=1))
    assert asyncio.run(tracker.dispatch(Settings(), outage)).status == "delivered"
    recovery = tracker.record_success(now=started + timedelta(minutes=6))
    assert recovery is not None
    _dispatch_success(monkeypatch, started + timedelta(minutes=6, seconds=1))
    assert asyncio.run(tracker.dispatch(Settings(), recovery)).status == "delivered"

    assert len(connections) == 7
    for connection in connections:
        with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
            connection.execute("SELECT 1")


def test_indeterminate_outage_attempt_is_preserved_through_recovery(
    monkeypatch, tmp_path
) -> None:
    started = datetime(2026, 9, 1, 4, 0, tzinfo=UTC)
    ledger = tmp_path / "canary.db"
    CanaryStore(str(ledger))
    tracker = OperationalIncidentTracker(ledger)
    tracker.record_failure("ibkr_gateway_unavailable", now=started)
    reserved = tracker.record_failure(
        "ibkr_gateway_unavailable", now=started + timedelta(minutes=5)
    )
    assert reserved is not None

    # Simulate either crash window: reservation committed, but receipt did not. The HTTP side
    # effect may or may not have escaped before the process died.
    restarted = OperationalIncidentTracker(ledger)
    recovery = restarted.record_success(now=started + timedelta(minutes=6))
    assert recovery is not None and recovery.phase == "recovery_indeterminate"
    _, message, _, _ = operational_notification_content(recovery)
    assert "also the durable outage notice" in message
    _dispatch_success(monkeypatch, started + timedelta(minutes=6, seconds=1))
    assert asyncio.run(restarted.dispatch(Settings(), recovery)).status == "delivered"
    assert restarted.record_success(now=started + timedelta(minutes=7)) is None


def test_failed_delivery_is_throttled_without_persisting_raw_error(
    monkeypatch, tmp_path
) -> None:
    started = datetime(2026, 9, 1, 4, 0, tzinfo=UTC)
    ledger = tmp_path / "canary.db"
    CanaryStore(str(ledger))
    tracker = OperationalIncidentTracker(ledger)
    tracker.record_failure("ibkr_gateway_unavailable", now=started)
    first = tracker.record_failure(
        "ibkr_gateway_unavailable", now=started + timedelta(minutes=5)
    )
    assert first is not None

    async def fail(settings, action):  # type: ignore[no-untyped-def]
        raise RuntimeError("secret detail must not persist")

    monkeypatch.setattr(alert_module, "send_operational_notification", fail)
    result = asyncio.run(tracker.dispatch(Settings(), first))
    assert result.status == "failed" and result.error_kind == "RuntimeError"
    assert (
        tracker.record_failure(
            "ibkr_gateway_unavailable",
            now=started + timedelta(minutes=9, seconds=59),
        )
        is None
    )
    retry = tracker.record_failure(
        "ibkr_gateway_unavailable", now=started + timedelta(minutes=10)
    )
    assert retry is not None and retry.incident_id == first.incident_id

    with sqlite3.connect(ledger) as conn:
        serialized = "\n".join(
            str(row[0]) for row in conn.execute("SELECT detail_json FROM events")
        )
    assert "secret detail" not in serialized
    assert "RuntimeError" in serialized


def test_stale_recovery_cannot_escape_after_renewed_failure(monkeypatch, tmp_path) -> None:
    started = datetime(2026, 9, 1, 4, 0, tzinfo=UTC)
    ledger = tmp_path / "canary.db"
    CanaryStore(str(ledger))
    tracker = OperationalIncidentTracker(ledger)
    tracker.record_failure("ibkr_gateway_unavailable", now=started)
    outage = tracker.record_failure(
        "ibkr_gateway_unavailable", now=started + timedelta(minutes=5)
    )
    assert outage is not None
    _dispatch_success(monkeypatch, started + timedelta(minutes=5, seconds=1))
    asyncio.run(tracker.dispatch(Settings(), outage))
    stale_recovery = tracker.record_success(now=started + timedelta(minutes=6))
    assert stale_recovery is not None
    tracker.record_failure(
        "ibkr_gateway_unavailable", now=started + timedelta(minutes=7)
    )

    sends = 0

    async def forbidden(settings, action):  # type: ignore[no-untyped-def]
        nonlocal sends
        sends += 1
        return _receipt(started + timedelta(minutes=7))

    monkeypatch.setattr(alert_module, "send_operational_notification", forbidden)
    assert asyncio.run(tracker.dispatch(Settings(), stale_recovery)).status == "stale"
    assert sends == 0


def test_dispatch_mutex_fences_concurrent_lifecycle_transition(
    monkeypatch, tmp_path
) -> None:
    started = datetime(2026, 9, 1, 4, 0, tzinfo=UTC)
    ledger = tmp_path / "canary.db"
    CanaryStore(str(ledger))
    tracker = OperationalIncidentTracker(ledger)
    tracker.record_failure("ibkr_gateway_unavailable", now=started)
    outage = tracker.record_failure(
        "ibkr_gateway_unavailable", now=started + timedelta(minutes=5)
    )
    assert outage is not None

    async def scenario() -> None:
        entered = asyncio.Event()
        release = asyncio.Event()

        async def blocked_send(settings, action):  # type: ignore[no-untyped-def]
            entered.set()
            await release.wait()
            return _receipt(started + timedelta(minutes=5, seconds=1))

        monkeypatch.setattr(alert_module, "send_operational_notification", blocked_send)
        dispatch = asyncio.create_task(tracker.dispatch(Settings(), outage))
        await entered.wait()
        with pytest.raises(TimeoutError, match="mutex acquisition timed out"):
            await asyncio.to_thread(
                tracker.record_success, now=started + timedelta(minutes=6)
            )
        release.set()
        assert (await dispatch).status == "delivered"

    asyncio.run(scenario())


def test_windows_mutex_uses_cross_session_namespace() -> None:
    source = Path(alert_module.__file__).read_text(encoding="utf-8")
    assert 'f"Global\\\\InsiderAlertsCanaryOps-{identity}"' in source
    assert 'f"Local\\\\InsiderAlertsCanaryOps-{identity}"' not in source


def test_operational_http_has_hard_total_deadline(monkeypatch) -> None:
    async def slow_post(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        await asyncio.sleep(1)
        raise AssertionError("deadline failed")

    monkeypatch.setattr(alert_module, "DELIVERY_DEADLINE_SECONDS", 0.02)
    monkeypatch.setattr(alert_module.httpx.AsyncClient, "post", slow_post)
    action = OperationalNotificationAction(
        incident_id="deadline",
        phase="outage",
        failure_kind="ibkr_gateway_unavailable",
        started_at_utc=datetime.now(UTC),
        recovered_at_utc=None,
        reserved_at_utc=datetime.now(UTC),
    )
    before = monotonic()
    with pytest.raises(NtfyNotificationError, match="TimeoutError"):
        asyncio.run(send_operational_notification(Settings(), action))
    assert monotonic() - before < 0.5


def test_optional_schema_is_separate_and_migrates_existing_ledger(tmp_path) -> None:
    ledger = tmp_path / "canary.db"
    core = CanaryStore(str(ledger))
    core.set_metadata({"sentinel": "preserved"}, now=datetime.now(UTC))
    assert operational_incident_status(ledger) == {
        "available": False,
        "active": None,
        "latest": None,
    }
    OperationalIncidentTracker(ledger)
    assert operational_incident_status(ledger)["available"] is True
    with core.connect() as conn:
        assert conn.execute(
            "SELECT value FROM metadata WHERE key='sentinel'"
        ).fetchone()[0] == "preserved"


def test_alert_database_contention_fails_within_short_budget(tmp_path) -> None:
    ledger = tmp_path / "canary.db"
    CanaryStore(str(ledger))
    tracker = OperationalIncidentTracker(ledger)
    blocker = sqlite3.connect(ledger, timeout=1)
    blocker.execute("BEGIN IMMEDIATE")
    before = monotonic()
    try:
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            tracker.record_failure("sqlite_failure", now=datetime.now(UTC))
    finally:
        blocker.rollback()
        blocker.close()
    assert monotonic() - before < 0.5


def test_classification_and_payload_never_include_raw_error() -> None:
    secret = "account=DU123456 token=super-secret"
    exc = IbkrExecutionError(f"IBKR_GATEWAY_STARTUP_SYNC_FAILED: {secret}")
    assert classify_operational_failure(exc) == "ibkr_gateway_unavailable"
    assert classify_operational_failure(sqlite3.OperationalError(secret)) == "sqlite_failure"
    assert classify_operational_failure(OSError(secret)) == "operating_system_failure"
    assert classify_operational_failure(ValueError(secret)) == "validation_failure"
    now = datetime(2026, 9, 1, 4, 0, tzinfo=UTC)
    action = OperationalNotificationAction(
        incident_id="incident-safe",
        phase="outage",
        failure_kind=classify_operational_failure(exc),
        started_at_utc=now,
        recovered_at_utc=None,
        reserved_at_utc=now,
    )
    _, message, _, _ = operational_notification_content(action)
    assert secret not in message
    assert "port 4001" in message


class _FakeBroker:
    def disconnect(self) -> None:
        return None


class _SuccessfulRunner:
    def __init__(self, config, broker) -> None:  # type: ignore[no-untyped-def]
        self.store = CanaryStore(str(config.ledger_db))
        self.broker = _FakeBroker()

    def source_revision_changed(self) -> bool:
        return False

    async def cycle(self, *, disconnect_after: bool) -> CycleResult:
        assert disconnect_after is True
        return CycleResult()


def test_cli_isolates_optional_schema_initialization_failure(monkeypatch, tmp_path) -> None:
    ledger = tmp_path / "canary.db"

    class BrokenTracker:
        def __init__(self, ledger_path) -> None:  # type: ignore[no-untyped-def]
            raise sqlite3.OperationalError("optional migration failed")

    monkeypatch.setattr(cli, "CanaryRunner", _SuccessfulRunner)
    monkeypatch.setattr(cli, "OperationalIncidentTracker", BrokenTracker)
    result = CliRunner().invoke(
        cli.app,
        ["ops", "live-canary", "--once", "--notify", "--ledger-path", str(ledger)],
    )
    assert result.exit_code == 0
    assert "alert initialization isolated: OperationalError" in result.output


def test_cli_retries_transient_tracker_initialization_after_backoff(
    monkeypatch, tmp_path
) -> None:
    ledger = tmp_path / "canary.db"
    state = {"cycles": 0, "attempts": 0}
    current = datetime(2026, 9, 1, 4, 0, tzinfo=UTC)

    class AdvancingDateTime(datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[no-untyped-def]
            nonlocal current
            current += timedelta(minutes=2)
            return current if tz is not None else current.replace(tzinfo=None)

    class TwoCycleRunner(_SuccessfulRunner):
        def source_revision_changed(self) -> bool:
            return state["cycles"] >= 2

        async def cycle(self, *, disconnect_after: bool) -> CycleResult:
            assert disconnect_after is False
            state["cycles"] += 1
            return CycleResult()

    real_tracker = OperationalIncidentTracker

    def flaky_tracker(path):  # type: ignore[no-untyped-def]
        state["attempts"] += 1
        if state["attempts"] == 1:
            raise sqlite3.OperationalError("temporary lock")
        return real_tracker(path)

    async def no_delay(seconds):  # type: ignore[no-untyped-def]
        await asyncio.tasks.sleep(0)

    monkeypatch.setattr(cli, "datetime", AdvancingDateTime)
    monkeypatch.setattr(cli, "CanaryRunner", TwoCycleRunner)
    monkeypatch.setattr(cli, "OperationalIncidentTracker", flaky_tracker)
    monkeypatch.setattr(cli.asyncio, "sleep", no_delay)
    result = CliRunner().invoke(
        cli.app,
        ["ops", "live-canary", "--loop", "--notify", "--ledger-path", str(ledger)],
    )
    assert result.exit_code == 0
    assert state == {"cycles": 2, "attempts": 2}
    assert operational_incident_status(ledger)["available"] is True


def test_cli_operational_diagnostic_log_failure_is_best_effort(
    monkeypatch, tmp_path
) -> None:
    ledger = tmp_path / "canary.db"
    error_log_directory = tmp_path / "not-a-file"
    error_log_directory.mkdir()

    def broken_success(self, *, now):  # type: ignore[no-untyped-def]
        raise sqlite3.OperationalError("tracking unavailable")

    monkeypatch.setattr(cli, "CanaryRunner", _SuccessfulRunner)
    monkeypatch.setattr(
        cli.OperationalIncidentTracker,
        "record_success",
        broken_success,
    )
    result = CliRunner().invoke(
        cli.app,
        [
            "ops",
            "live-canary",
            "--once",
            "--notify",
            "--ledger-path",
            str(ledger),
            "--error-log",
            str(error_log_directory),
        ],
    )
    assert result.exit_code == 0
    assert "operational recovery tracking isolated: OperationalError" in result.output


def test_loop_does_not_gate_next_broker_cycle_on_notification_dispatch(
    monkeypatch, tmp_path
) -> None:
    ledger = tmp_path / "canary.db"
    started = datetime.now(UTC) - timedelta(minutes=10)
    CanaryStore(str(ledger))
    OperationalIncidentTracker(ledger).record_failure(
        "ibkr_gateway_unavailable", now=started
    )
    state = {
        "cycles": 0,
        "failure_transitions": 0,
        "dispatch_started": False,
        "dispatch_cancelled": False,
    }

    class FailingRunner:
        def __init__(self, config, broker) -> None:  # type: ignore[no-untyped-def]
            self.store = CanaryStore(str(config.ledger_db))
            self.broker = _FakeBroker()

        def source_revision_changed(self) -> bool:
            return state["cycles"] >= 2

        async def cycle(self, *, disconnect_after: bool) -> CycleResult:
            assert disconnect_after is False
            state["cycles"] += 1
            raise IbkrExecutionError("IBKR_GATEWAY_STARTUP_SYNC_FAILED: unavailable")

    async def blocked_dispatch(self, settings, action):  # type: ignore[no-untyped-def]
        state["dispatch_started"] = True
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            state["dispatch_cancelled"] = True
            raise

    async def yield_once(seconds):  # type: ignore[no-untyped-def]
        await asyncio.tasks.sleep(0)

    real_record_failure = OperationalIncidentTracker.record_failure

    def counted_failure(self, failure_kind, *, now):  # type: ignore[no-untyped-def]
        state["failure_transitions"] += 1
        return real_record_failure(self, failure_kind, now=now)

    monkeypatch.setattr(cli, "CanaryRunner", FailingRunner)
    monkeypatch.setattr(cli.OperationalIncidentTracker, "dispatch", blocked_dispatch)
    monkeypatch.setattr(
        cli.OperationalIncidentTracker,
        "record_failure",
        counted_failure,
    )
    monkeypatch.setattr(cli.asyncio, "sleep", yield_once)
    result = CliRunner().invoke(
        cli.app,
        ["ops", "live-canary", "--loop", "--notify", "--ledger-path", str(ledger)],
    )
    assert result.exit_code == 0
    assert state == {
        "cycles": 2,
        "failure_transitions": 1,
        "dispatch_started": True,
        "dispatch_cancelled": True,
    }
