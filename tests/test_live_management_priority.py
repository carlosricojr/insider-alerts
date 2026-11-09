from __future__ import annotations

import asyncio
import json
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import insider_alerts.execution.canary as module
from insider_alerts.execution.canary import AccountSnapshot, BrokerOrder, broker_token
from insider_alerts.execution.errors import IbkrExecutionError
from tests.test_execution_canary_adversarial import _runner, _signal


def _filled_entry(broker) -> None:
    token = broker_token("crash-test-packet")
    broker.snapshot = AccountSnapshot("ACCOUNT", 293.5, 293.5, 293.5, {"TEST": 20}, 0)
    broker.order_values = [
        BrokerOrder(101, f"IA-E07-{token}-ENTRY", "TEST", "entry", "Filled", 20, 0, 10),
    ]


def _metadata(runner) -> dict[str, str]:
    with runner.store.connect() as connection:
        return dict(connection.execute("SELECT key,value FROM metadata"))


def test_filled_entry_is_protected_before_retryable_discovery_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)
    runner, broker = _runner(tmp_path, monkeypatch, now)
    asyncio.run(runner.cycle(now))
    prior_success = _metadata(runner)["last_cycle_success_utc"]
    _filled_entry(broker)
    bad = replace(_signal(now), packet_id="retry-later", symbol="BAD")
    monkeypatch.setattr(module, "load_delivered_signals", lambda *args, **kwargs: [bad])
    broker.daily_bar_errors["BAD"] = ConnectionError("temporary history outage")
    disconnected = []
    broker.disconnect = lambda: disconnected.append(True)

    with pytest.raises(ConnectionError, match="temporary history outage"):
        asyncio.run(runner.cycle(now + timedelta(minutes=12), disconnect_after=False))

    assert broker.protected[0][0:2] == ("TEST", 20)
    assert runner.store.rows()[0]["live_state"] == "open"
    assert not runner.store.has_candidate("retry-later")
    assert broker.entry_calls == 1
    assert disconnected == [True]
    metadata = _metadata(runner)
    assert metadata["last_cycle_success_utc"] == prior_success
    assert metadata["last_live_management_success_utc"] > prior_success
    assert metadata["cycle_phase"] == "auxiliary"
    assert "temporary history outage" in metadata["last_cycle_error"]


def test_due_exit_runs_before_shadow_history_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)
    runner, broker = _runner(tmp_path, monkeypatch, now)
    asyncio.run(runner.cycle(now))
    _filled_entry(broker)
    asyncio.run(runner.cycle(now + timedelta(minutes=12)))
    runner.store.update("crash-test-packet", live_exit_session="2026-01-05")
    token = broker_token("crash-test-packet")
    broker.order_values = [
        BrokerOrder(201, f"IA-E07-{token}-TARGET", "TEST", "target", "Submitted", 0, 20, 0),
        BrokerOrder(202, f"IA-E07-{token}-STOP", "TEST", "stop", "Submitted", 0, 20, 0),
    ]

    async def shadow_failure(*args, **kwargs):
        raise OSError("shadow history unavailable")

    monkeypatch.setattr(runner, "_settle_shadow", shadow_failure)
    with pytest.raises(OSError, match="shadow history unavailable"):
        asyncio.run(runner.cycle(datetime(2026, 1, 6, 15, 0, tzinfo=UTC)))

    assert broker.market_exits[-1][0:2] == ("TEST", 20)
    assert runner.store.rows()[0]["live_state"] == "closing"
    assert _metadata(runner)["last_live_management_time_exits"] == "1"
    assert broker.entry_calls == 1


def test_stalled_auxiliary_is_bounded_canceled_and_disconnected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)
    runner, broker = _runner(tmp_path, monkeypatch, now)
    events = []

    async def stalled_history(symbol, **kwargs):
        events.append("history")
        try:
            await asyncio.Event().wait()
        finally:
            events.append("canceled")

    original_snapshot = broker.account_snapshot

    async def managed_snapshot():
        events.append("management")
        return await original_snapshot()

    monkeypatch.setattr(broker, "account_snapshot", managed_snapshot)
    monkeypatch.setattr(broker, "daily_bars", stalled_history)
    monkeypatch.setattr(broker, "disconnect", lambda: events.append("disconnected"))
    monkeypatch.setattr(module, "LIVE_AUXILIARY_BUDGET_SECONDS", 0.02)

    with pytest.raises(IbkrExecutionError, match="AUXILIARY_BUDGET_EXCEEDED"):
        asyncio.run(runner.cycle(now, disconnect_after=False))

    assert events == ["management", "history", "canceled", "disconnected"]
    assert broker.entry_calls == 0
    assert runner.store.rows() == []
    metadata = _metadata(runner)
    assert metadata["cycle_phase"] == "auxiliary"
    assert metadata.get("last_cycle_success_utc") is None
    assert metadata["last_live_management_success_utc"]


def test_external_auxiliary_cancellation_discards_sdk_session(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)
    runner, broker = _runner(tmp_path, monkeypatch, now)
    started = asyncio.Event()
    disconnected = []

    async def stalled_history(*args, **kwargs):
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(broker, "daily_bars", stalled_history)
    monkeypatch.setattr(broker, "disconnect", lambda: disconnected.append(True))

    async def scenario():
        task = asyncio.create_task(runner.cycle(now, disconnect_after=False))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())
    assert disconnected == [True]
    assert broker.entry_calls == 0
    assert _metadata(runner).get("last_cycle_success_utc") is None


def test_synchronous_auxiliary_overrun_finishes_then_blocks_entries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)
    runner, broker = _runner(tmp_path, monkeypatch, now)
    disconnected = []

    async def slow_synchronous_storage(*args, **kwargs):
        # No asyncio yield: its scheduled timeout callback cannot run here.
        time.sleep(0.04)
        return (0, 0)

    monkeypatch.setattr(runner, "_settle_shadow", slow_synchronous_storage)
    monkeypatch.setattr(broker, "disconnect", lambda: disconnected.append(True))
    monkeypatch.setattr(module, "LIVE_AUXILIARY_BUDGET_SECONDS", 0.01)
    with pytest.raises(IbkrExecutionError, match="AUXILIARY_BUDGET_EXCEEDED"):
        asyncio.run(runner.cycle(now, disconnect_after=False))
    assert broker.entry_calls == 0
    assert disconnected == [True]
    assert runner.store.rows()[0]["live_state"] == "queued"
    assert _metadata(runner).get("last_cycle_success_utc") is None


@pytest.mark.parametrize("change", ["positions", "orders", "valuations"])
def test_entry_requires_fresh_stable_management_state_not_stable_valuation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)
    runner, broker = _runner(tmp_path, monkeypatch, now)
    calls = []

    async def fresh():
        calls.append("fresh")
        if change == "positions":
            broker.snapshot = replace(broker.snapshot, positions={"TEST": 20})
        elif change == "orders":
            broker.order_values = [
                BrokerOrder(999, "IA-E07-OTHER-ENTRY", "OTHER", "entry", "Submitted", 0, 1, 0),
            ]
        else:
            broker.snapshot = replace(broker.snapshot, net_liquidation=500, available_funds=490)
        return broker.snapshot

    monkeypatch.setattr(broker, "fresh_account_snapshot", fresh)
    result = asyncio.run(runner.cycle(now))
    assert calls == ["fresh"]
    assert broker.entry_calls == int(change == "valuations")
    if change != "valuations":
        assert result.live_gate == "management_state_changed_before_entry;time_exits=0"


def test_auxiliary_failure_preserves_already_committed_candidate_for_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 1, 5, 14, 18, 30, tzinfo=UTC)
    runner, broker = _runner(tmp_path, monkeypatch, now)

    async def fail_after_discovery(*args, **kwargs):
        raise OSError("shadow unavailable")

    monkeypatch.setattr(runner, "_settle_shadow", fail_after_discovery)
    with pytest.raises(OSError, match="shadow unavailable"):
        asyncio.run(runner.cycle(now))
    row = runner.store.rows()[0]
    assert row["live_state"] == "queued"
    assert row["eligible"] == 1
    assert broker.entry_calls == 0
    with runner.store.connect() as connection:
        records = [json.loads(r[0]) for r in connection.execute(
            "SELECT detail_json FROM events WHERE event_type='candidate_detected'",
        )]
    assert records  # Discovery evidence survives; no failure-driven rejection is fabricated.
