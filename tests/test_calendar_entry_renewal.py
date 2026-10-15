from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from insider_alerts.execution.calendar import NEW_YORK, bounds, calendar_dates, entry_approval
from insider_alerts.execution.canary import AccountSnapshot, CanaryConfig, CycleResult
from insider_alerts.execution.errors import IbkrExecutionError
from tests.test_execution_calendar import (
    FakeIb,
    broker,
    open_dates,
    projected_schedule,
    schedule_for_dates,
)
from tests.test_execution_canary_adversarial import _runner, _signal


def hours(day: date) -> str:
    parts = []
    for offset in range(7):
        current = day + timedelta(days=offset)
        if current.year != 2026:
            break
        session = bounds(current)
        stamp = current.strftime("%Y%m%d")
        parts.append(f"{stamp}:CLOSED" if session is None else (
            f"{stamp}:0930-{stamp}:{session[1]:%H%M}"
        ))
    return ";".join(parts)


def adapter_at(now: datetime, kind: str = "fallback"):
    today = now.astimezone(NEW_YORK).date()
    end = today + timedelta(days=45)
    response = (
        schedule_for_dates(open_dates(end - timedelta(days=119), end))
        if kind == "native" else projected_schedule(now) if kind == "projection" else []
    )
    adapter = broker(FakeIb(response, hours(today)))
    asyncio.run(adapter.sessions(around=now, count=120))
    return adapter


@pytest.mark.parametrize("stamp,approved", [
    ("2026-10-01T03:59:59+00:00", True),
    ("2026-10-01T04:00:00+00:00", False),
    ("2026-10-15T03:59:59+00:00", False),
    ("2026-10-15T04:00:00+00:00", True),
    ("2027-01-01T04:59:59+00:00", True),
    ("2027-01-01T05:00:00+00:00", False),
])
def test_explicit_approval_boundaries_are_new_york_dates(stamp: str, approved: bool) -> None:
    now = datetime.fromisoformat(stamp)
    assert (entry_approval(now.astimezone(NEW_YORK).date()) is not None) is approved


@pytest.mark.parametrize("kind", ["fallback", "projection", "native"])
def test_renewal_admits_only_fresh_complete_calendar(kind: str) -> None:
    now = datetime(2026, 10, 15, 13, 18, tzinfo=UTC)
    adapter = adapter_at(now, kind)
    assert adapter.calendar_gate(now, for_entry=True) is None
    assert adapter.schedule_evidence["entry_horizon_exit"] == "2026-10-28"
    assert adapter.schedule_evidence["entry_approval_id"] == "renewal-2026-10-15"
    assert adapter.calendar_gate(now + timedelta(seconds=61), for_entry=True) == (
        "calendar_validation_stale"
    )
    assert adapter.session_bounds(now + timedelta(minutes=5)) == bounds(date(2026, 10, 15))


@pytest.mark.parametrize("kind", ["fallback", "native"])
@pytest.mark.parametrize("day,allowed", [(17, True), (18, False), (24, False), (31, False)])
def test_full_horizon_cannot_extend_into_unverified_year(
    kind: str, day: int, allowed: bool,
) -> None:
    now = datetime(2026, 12, day, 14, 18, tzinfo=UTC)
    adapter = adapter_at(now, kind)
    assert (adapter.calendar_gate(now, for_entry=True) is None) is allowed
    if allowed:
        assert adapter.schedule_evidence["entry_horizon_exit"] == "2026-12-31"
    else:
        assert adapter.calendar_gate(now, for_entry=True) == "calendar_entry_horizon_incomplete"
    assert adapter.session_bounds(now) == bounds(now.date())


@pytest.mark.parametrize("kind", ["native", "fallback"])
@pytest.mark.parametrize("day", [date(2026, 11, 27), date(2026, 12, 24)])
def test_early_close_bounds_are_source_validated(day: date, kind: str) -> None:
    now = datetime.combine(day, time(12, 30), NEW_YORK)
    adapter = adapter_at(now, kind)
    assert adapter.session_bounds(now) == bounds(day)
    assert adapter.session_bounds(now + timedelta(days=1)) is None
    assert adapter.session_bounds(now - timedelta(seconds=1)) is None
    adapter.schedule_evidence.pop("today_close_et")
    assert adapter.session_bounds(now) is None


def due_position(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, now: datetime):
    runner, fake = _runner(tmp_path, monkeypatch, now)
    runner.store.insert_candidate(
        _signal(now), session=now.astimezone(NEW_YORK).date(), rank="test", is_eligible=True,
        reason="eligible", prior_close=10, median_dollar_volume=1_000_000, quantity=20, now=now,
    )
    runner.store.update("crash-test-packet", live_state="open", live_quantity=20,
                        live_entry_price=10, live_exit_session=now.date().isoformat(),
                        oca_group="test-oca", target_order_id=201, stop_order_id=202)
    return runner, fake


@pytest.mark.parametrize("day,clock,moc,market", [
    ("2026-10-15", "15:29:59", 0, 0),
    ("2026-10-15", "15:30:00", 1, 0),
    ("2026-10-15", "15:44:59", 1, 0),
    ("2026-10-15", "15:45:00", 0, 1),
    ("2026-10-15", "16:00:00", 0, 0),
    ("2026-11-27", "12:29:59", 0, 0),
    ("2026-11-27", "12:30:00", 1, 0),
    ("2026-11-27", "12:44:59", 1, 0),
    ("2026-11-27", "12:45:00", 0, 1),
    ("2026-11-27", "13:00:00", 0, 0),
    ("2026-12-24", "12:30:00", 1, 0),
    ("2026-12-24", "12:45:00", 0, 1),
    ("2026-12-24", "13:00:00", 0, 0),
])
def test_session_relative_dispatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    day: str, clock: str, moc: int, market: int,
) -> None:
    now = datetime.fromisoformat(f"{day}T{clock}").replace(tzinfo=NEW_YORK)
    runner, fake = due_position(tmp_path, monkeypatch, now)
    adapter = adapter_at(now)
    monkeypatch.setattr(fake, "session_bounds", adapter.session_bounds)
    windows = []
    original_moc = fake.submit_market_on_close
    original_market = fake.submit_market_exit

    async def capture_moc(*args, **kwargs):
        windows.append(kwargs["dispatch_window"])
        return await original_moc(*args, **kwargs)

    async def capture_market(*args, **kwargs):
        windows.append(kwargs["dispatch_window"])
        return await original_market(*args, **kwargs)

    monkeypatch.setattr(fake, "submit_market_on_close", capture_moc)
    monkeypatch.setattr(fake, "submit_market_exit", capture_market)
    assert asyncio.run(runner._submit_due_time_exits(now)) == moc + market
    assert fake.moc_calls == moc
    assert len(fake.market_exits) == market
    assert fake.cancelled_orders == []
    assert runner.store.rows()[0]["live_exit_session"] == day
    close = bounds(date.fromisoformat(day))[1]
    assert windows == (
        [(close - timedelta(minutes=30), close - timedelta(minutes=15))] if moc else
        [(close - timedelta(minutes=15), close)] if market else []
    )
    # A later cycle cannot place another exit after the ledger transition.
    if moc + market:
        assert asyncio.run(runner._submit_due_time_exits(now + timedelta(seconds=1))) == 0


@pytest.mark.parametrize("stamp", [
    "2026-10-15T09:29:59", "2026-10-15T09:30:00",
    "2026-10-15T16:00:00", "2026-10-17T12:00:00", "2026-11-26T12:00:00",
])
def test_overdue_recovery_retains_existing_day_order_behavior(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stamp: str,
) -> None:
    now = datetime.fromisoformat(stamp).replace(tzinfo=NEW_YORK)
    runner, fake = due_position(tmp_path, monkeypatch, now)
    runner.store.update("crash-test-packet", live_exit_session="2026-10-01")
    assert asyncio.run(runner._submit_due_time_exits(now)) == 1
    assert len(fake.market_exits) == 1
    with runner.store.connect() as connection:
        assert connection.execute(
            "select count(*) from events where event_type='timed_exit_calendar_unavailable'"
        ).fetchone()[0] == 0


def test_runner_uses_dispatch_clock_not_cycle_start(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import insider_alerts.execution.canary as module
    now = datetime(2026, 11, 27, 12, 44, tzinfo=NEW_YORK)
    runner, fake = due_position(tmp_path, monkeypatch, now)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return (now + timedelta(minutes=1)).astimezone(tz)

    monkeypatch.setattr(module, "datetime", Clock)
    assert asyncio.run(runner._submit_due_time_exits(now, enforce_wall_clock=True)) == 1
    assert fake.moc_calls == 0
    assert len(fake.market_exits) == 1


@pytest.mark.parametrize("kind", ["moc", "market"])
def test_adapter_rechecks_after_contract_lookup_before_place_order(
    monkeypatch: pytest.MonkeyPatch, kind: str,
) -> None:
    import insider_alerts.execution.ibkr as module
    now = datetime(2026, 11, 27, 12, 44, tzinfo=NEW_YORK)
    adapter = adapter_at(now)
    fake = adapter.ib
    fake.trades = lambda: []
    fake.managedAccounts = lambda: ["TEST"]
    placed = []
    fake.placeOrder = lambda *args: placed.append(args)
    end = now.replace(minute=45) if kind == "moc" else now.replace(hour=13, minute=0)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return end.astimezone(tz)

    async def slow_contract(symbol: str):
        monkeypatch.setattr(module, "datetime", Clock)
        return SimpleNamespace()

    monkeypatch.setattr(adapter, "_contract", slow_contract)
    method = adapter.submit_market_on_close if kind == "moc" else adapter.submit_market_exit
    with pytest.raises(IbkrExecutionError, match="EXIT_DISPATCH_WINDOW_MISSED"):
        asyncio.run(method("TEST", 20, oca_group="same-oca", order_ref="test-TIME",
                           dispatch_window=(now.replace(minute=30), end)))
    assert placed == []


def test_missing_horizon_protects_then_blocks_entries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 10, 15, 13, 18, tzinfo=UTC)
    runner, fake = due_position(tmp_path, monkeypatch, now)
    runner.store.update("crash-test-packet", live_exit_session=None)
    row = runner.store.rows()[0]
    asyncio.run(runner._protect_position(row, quantity=20, price=10, sessions=[],
                                         recovery="test", commission=0.35))
    assert fake.protected
    fake.snapshot = AccountSnapshot("ACCOUNT", 500, 300, 300, {"TEST": 20}, 0)
    result = asyncio.run(runner._run_live(CycleResult(), [], now, enforce_wall_clock=False))
    assert result.live_gate == "live_exit_horizon_missing;time_exits=0"
    assert fake.entry_calls == 0
    with runner.store.connect() as connection:
        events = [json.loads(row[0]) for row in connection.execute(
            "select detail_json from events where event_type='live_exit_horizon_missing'"
        )]
    assert events


def test_config_rejects_reversed_or_after_close_exit_references() -> None:
    for submission, cutoff in [
        (time(15, 45), time(15, 30)), (time(15, 30), time(16)), (time(12, 29), time(15, 45)),
    ]:
        with pytest.raises(ValueError, match="timed exit references"):
            CanaryConfig(source_db="unused", timed_exit_submission_time=submission,
                         moc_submission_deadline=cutoff)


def test_renewed_projection_cycle_enters_today_only_and_persists_exit_before_send(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 10, 15, 13, 18, tzinfo=UTC)
    runner, fake = _runner(tmp_path, monkeypatch, now)
    adapter = adapter_at(now, "projection")
    fake.session_values = calendar_dates(now, 120)
    monkeypatch.setattr(fake, "calendar_gate", adapter.calendar_gate, raising=False)
    monkeypatch.setattr(fake, "session_bounds", adapter.session_bounds)
    runner.store.insert_candidate(
        replace(_signal(now), packet_id="old-blocked-entry"), session=date(2026, 10, 14),
        rank="test", is_eligible=True, reason="eligible", prior_close=10,
        median_dollar_volume=1_000_000, quantity=20, now=now,
    )
    original = fake.submit_market_on_open

    async def submit(symbol: str, quantity: int, order_ref: str) -> int:
        row = next(row for row in runner.store.rows() if row["packet_id"] == "crash-test-packet")
        assert row["live_exit_session"] == "2026-10-28"
        return await original(symbol, quantity, order_ref)

    monkeypatch.setattr(fake, "submit_market_on_open", submit)
    result = asyncio.run(runner.cycle(now))
    assert result.live_submitted == 1
    assert fake.entry_calls == 1
    rows = {row["packet_id"]: row for row in runner.store.rows()}
    assert rows["old-blocked-entry"]["live_state"] == "entry_window_missed"
    assert rows["crash-test-packet"]["live_exit_session"] == "2026-10-28"
    assert asyncio.run(runner.cycle(now)).live_submitted == 0
    assert fake.entry_calls == 1


def test_submit_stage_rechecks_horizon_after_discovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 10, 15, 13, 18, tzinfo=UTC)
    runner, fake = _runner(tmp_path, monkeypatch, now)
    asyncio.run(runner._discover(now - timedelta(hours=2), fake.session_values, now))
    assert runner.store.rows()[0]["eligible"]
    assert asyncio.run(runner._submit_due_entries(
        fake.snapshot, now, sessions=[now.date()], enforce_wall_clock=False,
    )) == 0
    assert fake.entry_calls == 0
    with runner.store.connect() as connection:
        reasons = [json.loads(row[0]).get("reason") for row in connection.execute(
            "select detail_json from events where event_type='live_gate_blocked'"
        )]
    assert "calendar_entry_horizon_incomplete" in reasons
    assert asyncio.run(runner._submit_due_entries(
        fake.snapshot, now, sessions=fake.session_values, enforce_wall_clock=False,
    )) == 1


def test_protection_preserves_previously_planned_exit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 10, 15, 13, 30, tzinfo=UTC)
    runner, fake = due_position(tmp_path, monkeypatch, now)
    runner.store.update("crash-test-packet", live_exit_session="2026-10-28")
    asyncio.run(runner._protect_position(
        runner.store.rows()[0], quantity=20, price=10,
        sessions=[now.date() + timedelta(days=i) for i in range(20)],
        recovery="crash", commission=0.35,
    ))
    assert fake.protected
    assert runner.store.rows()[0]["live_exit_session"] == "2026-10-28"
    with runner.store.connect() as connection:
        assert connection.execute(
            "select count(*) from events where event_type='live_exit_horizon_disagreement'"
        ).fetchone()[0] == 1


def test_missing_same_session_bounds_records_reason_without_exit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 10, 15, 15, 30, tzinfo=NEW_YORK)
    runner, fake = due_position(tmp_path, monkeypatch, now)
    monkeypatch.setattr(fake, "session_bounds", lambda now: None)
    assert asyncio.run(runner._submit_due_time_exits(now)) == 0
    assert fake.moc_calls == 0
    assert fake.market_exits == []
    with runner.store.connect() as connection:
        assert connection.execute(
            "select level from events where event_type='timed_exit_calendar_unavailable'"
        ).fetchone()[0] == "warning"


def test_entry_send_cannot_cross_new_york_date(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import insider_alerts.execution.canary as module
    now = datetime(2026, 10, 15, 13, 18, tzinfo=UTC)
    runner, fake = _runner(tmp_path, monkeypatch, now)
    asyncio.run(runner._discover(now - timedelta(hours=2), fake.session_values, now))

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return (now + timedelta(days=1)).astimezone(tz)

    monkeypatch.setattr(module, "datetime", Clock)
    assert asyncio.run(runner._submit_due_entries(
        fake.snapshot, now, sessions=fake.session_values, enforce_wall_clock=True,
    )) == 0
    assert fake.entry_calls == 0
    assert runner.store.rows()[0]["live_state"] == "entry_window_missed"
    with runner.store.connect() as connection:
        assert connection.execute(
            "select count(*) from events where event_type='entry_send_boundary_missed'"
        ).fetchone()[0] == 1


@pytest.mark.parametrize("kind", ["fallback", "projection"])
@pytest.mark.parametrize("day", [14, 15])
def test_adapter_renewal_boundary_preserves_unapproved_gap(kind: str, day: int) -> None:
    now = datetime(2026, 10, day, 13, 18, tzinfo=UTC)
    adapter = adapter_at(now, kind)
    assert adapter.calendar_gate(now, for_entry=True) == (
        "calendar_fallback_entry_approval_expired" if day == 14 else None
    )
    assert adapter.session_bounds(now) == bounds(now.date())


def test_runner_horizon_block_is_visible_in_cycle_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 10, 15, 13, 18, tzinfo=UTC)
    runner, fake = _runner(tmp_path, monkeypatch, now)
    result = asyncio.run(runner._run_live(
        CycleResult(), [now.date()], now, enforce_wall_clock=False,
    ))
    assert result.live_gate == "calendar_entry_horizon_incomplete;time_exits=0"
    assert fake.entry_calls == 0


def test_missed_close_is_reported_once_even_after_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from insider_alerts.execution.canary import CanaryRunner
    now = datetime(2026, 11, 27, 13, 0, tzinfo=NEW_YORK)
    runner, fake = due_position(tmp_path, monkeypatch, now)
    assert asyncio.run(runner._submit_due_time_exits(now)) == 0
    restarted = CanaryRunner(runner.config, fake)
    assert asyncio.run(restarted._submit_due_time_exits(now + timedelta(seconds=15))) == 0
    with runner.store.connect() as connection:
        assert connection.execute(
            "select count(*) from events where event_type='timed_exit_window_missed'"
        ).fetchone()[0] == 1
    assert fake.moc_calls == 0
    assert fake.market_exits == []


@pytest.mark.parametrize("kind", ["moc", "market"])
def test_adapter_dispatch_inside_window_preserves_oca_and_order_type(
    monkeypatch: pytest.MonkeyPatch, kind: str,
) -> None:
    import insider_alerts.execution.ibkr as module
    now = datetime(2026, 11, 27, 12, 30, tzinfo=NEW_YORK)
    adapter = adapter_at(now)
    fake = adapter.ib
    placed = []
    fake.trades = lambda: []
    fake.managedAccounts = lambda: ["TEST"]

    def place(contract, order):
        placed.append(order)
        return SimpleNamespace(order=SimpleNamespace(orderId=77),
                               orderStatus=SimpleNamespace(status="Submitted"))

    fake.placeOrder = place

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now.astimezone(tz)

    async def contract(symbol):
        return SimpleNamespace()

    async def no_sleep(seconds):
        pass

    monkeypatch.setattr(module, "datetime", Clock)
    monkeypatch.setattr(adapter, "_contract", contract)
    monkeypatch.setattr(module.asyncio, "sleep", no_sleep)
    method = adapter.submit_market_on_close if kind == "moc" else adapter.submit_market_exit
    assert asyncio.run(method("TEST", 20, oca_group="same-oca", order_ref="test-TIME",
                              dispatch_window=(now, now + timedelta(minutes=15)))) == 77
    assert len(placed) == 1
    assert (placed[0].ocaGroup, placed[0].ocaType, placed[0].totalQuantity) == ("same-oca", 1, 20)
    assert placed[0].orderType == ("MOC" if kind == "moc" else "MKT")
    assert placed[0].tif == "DAY"
