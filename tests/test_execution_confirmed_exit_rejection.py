from __future__ import annotations

import asyncio
import json
import sqlite3
import sys
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from eventkit import Event
from ib_async import IB, LimitOrder, OrderState, OrderStatus, Stock, StopOrder, Trade

import insider_alerts.execution.ibkr as ibkr_module
from insider_alerts.execution.errors import ConfirmedNewOrderRejection, IbkrExecutionError
from insider_alerts.execution.ibkr import IbkrBroker


class ExitIb:
    """Complete request/end responses, distinct from the streamed fill/position caches."""

    def __init__(self, broker: IbkrBroker, *, target: bool = True) -> None:
        self.errorEvent = Event()
        self.errorEvent += broker._on_error
        self.positionEvent = Event()
        self.connected = True
        self.contract = Stock("TEST", "SMART", "USD", conId=123)
        self.held = SimpleNamespace(account="ACCT", contract=self.contract, position=10, avgCost=10)
        stop = StopOrder("SELL", 10, 9, orderId=20, permId=120, clientId=1,
                         account="ACCT", orderRef="IA-E07-TEST-STOP", ocaGroup="oca",
                         ocaType=1, tif="GTC", transmit=True)
        self.active = [Trade(self.contract, stop, OrderStatus(
            status="Submitted", filled=0, remaining=10,
        ))]
        if target:
            limit = LimitOrder("SELL", 10, 11, orderId=21, permId=121, clientId=1,
                               account="ACCT", orderRef="IA-E07-TEST-TARGET", ocaGroup="oca",
                               ocaType=1, tif="GTC", transmit=True)
            self.active.append(Trade(self.contract, limit, OrderStatus(
                status="Submitted", filled=0, remaining=10,
            )))
        self.attempts: list[Trade] = []
        self.cached: list[Trade] = []
        self.mode = "confirmed"
        self.execution_requests = 0
        self.position_requests = 0
        self.order_requests = 0
        self.native = IB()
        self.wrapper = self.native.wrapper
        self.wrapper.clientId = 1
        self.wire_defect = ""
        self.wire_failure = ""

    def managedAccounts(self) -> list[str]:
        return ["ACCT"]

    def isConnected(self) -> bool:
        return self.connected

    def disconnect(self) -> None:
        self.connected = False

    def accountValues(self, account: str) -> list[Any]:
        return []  # Cash and NAV do not gate a risk-reducing exit.

    def positions(self, account: str) -> list[Any]:
        return [self.held]

    def openTrades(self) -> list[Trade]:
        return self.active

    def trades(self) -> list[Trade]:
        return [*self.active, *self.cached, *self.attempts]

    async def reqPositionsAsync(self) -> list[Any]:
        self.position_requests += 1
        if self.attempts and self.mode == "position_timeout":
            raise TimeoutError()
        if self.attempts and self.mode == "position_changed":
            return [SimpleNamespace(account="ACCT", contract=self.contract, position=9, avgCost=10)]
        return [deepcopy(self.held)]

    def reqAllOpenOrdersAsync(self) -> Any:
        future = self.wrapper.startReq("openOrders")
        async def deliver() -> None:
            try:
                sources = await self.wire_orders()
                if self.wire_failure == "cancel":
                    raise asyncio.CancelledError()
                for cached in sources:
                    contract, order = deepcopy(cached.contract), deepcopy(cached.order)
                    state = OrderState(status=cached.orderStatus.status)
                    order.transmit = True  # Decoder leaves this at its default; it is not on wire.
                    key = self.wrapper.orderKey(order.clientId, order.orderId, order.permId)
                    self.wrapper.trades.setdefault(key, cached)
                    if self.attempts and order.orderType == "STP":
                        if self.wire_defect == "oca":
                            order.ocaGroup = "BROKER_CHANGED_GROUP"
                        elif self.wire_defect == "account":
                            order.account = "OTHER"
                        elif self.wire_defect == "action":
                            order.action = "BUY"
                        elif self.wire_defect == "tif":
                            order.tif = "DAY"
                        elif self.wire_defect == "contract":
                            contract.conId = 999
                        elif self.wire_defect == "status":
                            state.status = "Cancelled"
                    self.wrapper.openOrder(order.orderId, contract, order, state)
                    if self.wire_failure != "missing_status":
                        remaining = (9 if self.wire_failure == "partial_status"
                                     else cached.orderStatus.remaining)
                        self.wrapper.orderStatus(
                            order.orderId, cached.orderStatus.status, cached.orderStatus.filled,
                            remaining, 0, order.permId, order.parentId, 0, order.clientId, "",
                        )
                        if self.wire_failure == "partial_then_zero":
                            for filled, left in [(1, 9), (0, 10)]:
                                self.wrapper.orderStatus(
                                    order.orderId, "Submitted", filled, left, 0,
                                    order.permId, order.parentId, 0, order.clientId, "",
                                )
                if self.wire_failure == "missing_end":
                    self.wrapper._endReq("openOrders")
                    return
                if self.wire_failure == "timeout":
                    return
                if self.wire_failure == "extra_response":
                    self.wrapper._results["openOrders"].append(sources[0])
                if self.wire_failure == "callback_drift":
                    self.wrapper.openOrder = lambda *args: None
                self.wrapper.openOrderEnd()
                if self.wire_failure == "callback_after_end":
                    order.ocaGroup = "CHANGED_AFTER_END"
                    self.wrapper.openOrder(order.orderId, contract, order, state)
            except BaseException as exc:
                self.wrapper._endReq("openOrders", exc, success=False)
        asyncio.create_task(deliver())
        return future

    async def wire_orders(self) -> list[Trade]:
        self.order_requests += 1
        if self.attempts:
            if self.mode == "orders_timeout":
                raise TimeoutError()
            if self.mode == "stop_missing":
                return self.active[1:]
            if self.mode == "stop_price_changed":
                self.active[0].order.auxPrice = 8
            if self.mode == "stop_owner_changed":
                self.active[0].order.clientId = 9
            if self.mode == "stop_cancelled":
                self.active[0].orderStatus.status = "Cancelled"
            if self.mode == "streamed_position_zero":
                self.positionEvent.emit(SimpleNamespace(
                    account="ACCT", contract=self.contract, position=0, avgCost=10,
                ))
            if self.mode == "late_cache_fill":
                self.attempts[-1].fills.append(SimpleNamespace())
            if self.mode == "attempt_still_open":
                return [*self.active, self.attempts[-1]]
            if self.mode == "unknown_order":
                unknown = deepcopy(self.active[0])
                unknown.order.orderRef = "unknown"
                return [*self.active, unknown]
        return list(self.active)

    async def reqExecutionsAsync(self, execution_filter: Any) -> list[Any]:
        self.execution_requests += 1
        assert execution_filter.acctCode == "ACCT" and execution_filter.clientId == 1
        if self.mode == "execution_timeout":
            raise TimeoutError()
        if self.mode == "cancelled":
            raise asyncio.CancelledError()
        if self.mode in {"query_fill", "query_fill_wrong_account", "query_fill_ref_only"}:
            order = self.attempts[-1].order
            return [SimpleNamespace(execution=SimpleNamespace(
                orderId=order.orderId if self.mode != "query_fill_ref_only" else 999,
                clientId=1, permId=0, shares=1,
                acctNumber="OTHER" if self.mode == "query_fill_wrong_account" else "ACCT",
                orderRef=order.orderRef,
            ))]  # Deliberately never appended to trade.fills or emitted live.
        if self.mode == "transport_during_query":
            self.errorEvent.emit(-1, 1100, "connection lost", None)
        return []

    def placeOrder(self, contract: Any, order: Any) -> Trade:
        order.orderId = 100 + len(self.attempts)
        order.clientId = 1
        status = {"accepted": "Submitted", "confirmed_inactive": "Inactive",
                  "confirmed_apicancelled": "ApiCancelled"}.get(self.mode, "Cancelled")
        trade = Trade(contract, order, OrderStatus(status=status, filled=0, remaining=10))
        self.attempts.append(trade)
        if self.mode == "partial":
            trade.orderStatus.filled = 1
        if self.mode == "nan_filled":
            trade.orderStatus.filled = float("nan")
        if self.mode == "ambiguous_remaining":
            trade.orderStatus.remaining = 9
        if self.mode == "cache_fill":
            trade.fills.append(SimpleNamespace())
        if self.mode == "identity_changed":
            order.ocaGroup = "different"
        if self.mode == "wrong_client":
            order.clientId = 9
        if self.mode == "disconnected":
            self.connected = False
        if self.mode not in {"stale_201", "accepted"}:
            code = 202 if self.mode == "cancel_only" else 201
            self.errorEvent.emit(999 if self.mode == "wrong_201" else order.orderId,
                                 code, "refused ACCT U1234567", contract)
        if self.mode == "transport":
            self.errorEvent.emit(-1, 1100, "connection lost", None)
        if status == "Submitted":
            self.active.append(trade)
        return trade


def adapter(*, target: bool = True) -> tuple[IbkrBroker, ExitIb]:
    broker = IbkrBroker(host="127.0.0.1", port=4001, client_id=1, account="ACCT")
    fake = ExitIb(broker, target=target)
    broker.ib = fake
    broker._contracts["TEST"] = fake.contract
    broker.session_bounds = lambda now: (now - timedelta(hours=1), now + timedelta(hours=1))
    return broker, fake


def submit(broker: IbkrBroker, *, market: bool = False) -> Any:
    now = datetime.now(UTC)
    kwargs = dict(oca_group="oca", order_ref=f"IA-E07-TEST-{'OVERDUE' if market else 'TIME'}",
                  dispatch_window=(now - timedelta(seconds=1), now + timedelta(minutes=1)))
    return (broker.submit_market_exit("TEST", 10, **kwargs) if market
            else broker.submit_market_on_close("TEST", 10, **kwargs))


@pytest.mark.parametrize("mode", ["confirmed", "confirmed_inactive", "confirmed_apicancelled"])
@pytest.mark.parametrize("market", [False, True])
def test_only_proved_new_refusal_can_be_isolated(market: bool, mode: str) -> None:
    broker, fake = adapter(target=not market)
    fake.mode = mode
    with pytest.raises(ConfirmedNewOrderRejection) as captured:
        asyncio.run(submit(broker, market=market))
    refusal = captured.value
    assert refusal.order_id == 100 and refusal.quantity == 10 and refusal.symbol == "TEST"
    assert "code=201" in refusal.detail
    assert "ACCT" not in refusal.detail and "U1234567" not in refusal.detail
    assert datetime.fromisoformat(refusal.verified_at).tzinfo is not None
    assert fake.execution_requests == 1 and fake.position_requests >= 2
    assert fake.connected and broker.ib is fake
    assert len(fake.errorEvent) == 1 and len(fake.positionEvent) == 0
    assert broker._exit_batch_errors is None
    assert broker._exit_batch_receipts == []


@pytest.mark.parametrize("mode", [
    "stale_201", "wrong_201", "cancel_only", "transport", "partial", "nan_filled",
    "ambiguous_remaining",
    "cache_fill", "query_fill", "query_fill_wrong_account", "query_fill_ref_only",
    "position_changed", "streamed_position_zero", "stop_missing", "stop_price_changed",
    "stop_owner_changed", "stop_cancelled", "late_cache_fill", "unknown_order",
    "attempt_still_open", "execution_timeout", "position_timeout", "orders_timeout",
    "identity_changed", "wrong_client", "transport_during_query", "disconnected",
])
def test_uncertain_or_changed_state_stops_globally(mode: str) -> None:
    broker, fake = adapter()
    fake.mode = mode
    broker._errors.append((100, 201, "old rejection must not qualify"))
    with pytest.raises(IbkrExecutionError) as captured:
        asyncio.run(submit(broker))
    assert not isinstance(captured.value, ConfirmedNewOrderRejection)
    assert len(fake.attempts) == 1
    assert not fake.connected and broker.ib is None
    assert len(fake.errorEvent) == 1 and len(fake.positionEvent) == 0


@pytest.mark.parametrize("defect", [
    "missing_stop", "duplicate_stop", "wrong_conid", "wrong_account", "wrong_owner",
    "stop_zero", "stop_nan", "target_zero", "parent", "wrong_quantity", "unset_stop",
    "unset_target",
])
def test_malformed_protection_or_position_prevents_any_dispatch(defect: str) -> None:
    broker, fake = adapter()
    if defect == "missing_stop":
        fake.active = fake.active[1:]
    elif defect == "duplicate_stop":
        fake.active.append(deepcopy(fake.active[0]))
    elif defect == "wrong_conid":
        fake.held.contract = Stock("TEST", "SMART", "USD", conId=999)
    elif defect == "wrong_account":
        fake.held.account = "OTHER"
    elif defect == "wrong_owner":
        fake.active[0].order.clientId = 9
    elif defect in {"stop_zero", "stop_nan"}:
        fake.active[0].order.auxPrice = 0 if defect == "stop_zero" else float("nan")
    elif defect == "target_zero":
        fake.active[1].order.lmtPrice = 0
    elif defect == "parent":
        fake.active[0].order.parentId = 999
    elif defect == "unset_stop":
        fake.active[0].order.auxPrice = sys.float_info.max
    elif defect == "unset_target":
        fake.active[1].order.lmtPrice = sys.float_info.max
    else:
        fake.held.position = 9
    with pytest.raises(IbkrExecutionError):
        asyncio.run(submit(broker))
    assert not fake.attempts
    assert len(fake.errorEvent) == 1 and len(fake.positionEvent) == 0


def test_reused_new_order_id_does_not_qualify_as_rejection() -> None:
    broker, fake = adapter()
    prior = deepcopy(fake.active[0])
    prior.order.orderId = 100
    prior.orderStatus.status = "Filled"
    fake.cached.append(prior)
    with pytest.raises(IbkrExecutionError, match="NEW_IDENTITY_NOT_PROVEN"):
        asyncio.run(submit(broker))
    assert fake.execution_requests == 0


def test_cancelled_proof_detaches_callbacks_and_disconnects() -> None:
    broker, fake = adapter()
    fake.mode = "cancelled"
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(submit(broker))
    assert not fake.connected and broker.ib is None
    assert len(fake.errorEvent) == 1 and len(fake.positionEvent) == 0


def test_accepted_new_exit_preserves_original_oca_and_order_fields() -> None:
    broker, fake = adapter()
    fake.mode = "accepted"
    assert asyncio.run(submit(broker)) == 100
    order = fake.attempts[0].order
    assert (order.action, order.orderType, order.totalQuantity, order.ocaGroup, order.ocaType,
            order.tif, order.transmit) == ("SELL", "MOC", 10, "oca", 1, "DAY", True)
    assert fake.execution_requests == 0 and fake.connected


def test_deadline_rechecked_after_fresh_preflight(monkeypatch: pytest.MonkeyPatch) -> None:
    broker, fake = adapter()
    clock = datetime(2026, 11, 27, 17, 44, 59, tzinfo=UTC)  # Early-close MOC cutoff.
    class LateClock(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> datetime:
            return clock + timedelta(seconds=2)
    monkeypatch.setattr(ibkr_module, "datetime", LateClock)
    with pytest.raises(IbkrExecutionError, match="DISPATCH_WINDOW"):
        asyncio.run(broker.submit_market_on_close(
            "TEST", 10, oca_group="oca", order_ref="IA-E07-TEST-TIME",
            dispatch_window=(clock - timedelta(minutes=10), clock + timedelta(seconds=1)),
        ))
    assert fake.position_requests == 1 and not fake.attempts


def test_account_context_drift_cannot_prove_a_refusal(monkeypatch: pytest.MonkeyPatch) -> None:
    from dataclasses import replace

    broker, fake = adapter()
    original = broker._fresh_account_state
    async def changed_account(**kwargs: Any) -> Any:
        snapshot, positions = await original(**kwargs)
        return replace(snapshot, account="OTHER"), positions
    monkeypatch.setattr(broker, "_fresh_account_state", changed_account)
    with pytest.raises(IbkrExecutionError, match="POSITION_NOT_PROVEN"):
        asyncio.run(submit(broker))
    assert not fake.attempts


@pytest.mark.parametrize("defect", ["oca", "account", "action", "tif", "contract", "status"])
def test_native_wrapper_cached_projection_cannot_hide_changed_broker_order(defect: str) -> None:
    broker, fake = adapter()
    fake.wire_defect = defect
    with pytest.raises(IbkrExecutionError) as captured:
        asyncio.run(submit(broker))
    assert not isinstance(captured.value, ConfirmedNewOrderRejection)
    # The installed SDK intentionally retains these old cached fields. The raw
    # callback proof must detect their change without rewriting the SDK cache.
    cached = fake.active[0]
    assert cached.order.ocaGroup == "oca" and cached.order.action == "SELL"
    assert cached.order.account == "ACCT" and cached.order.tif == "GTC"
    assert cached.contract.conId == 123 and cached.orderStatus.status == "Submitted"
    assert "openOrder" not in vars(fake.wrapper) and "openOrderEnd" not in vars(fake.wrapper)


def test_raw_reader_rejects_overlapping_request_and_restores_original_overrides() -> None:
    broker, fake = adapter()
    async def check() -> None:
        overlapping = fake.wrapper.startReq("openOrders")
        with pytest.raises(IbkrExecutionError, match="OVERLAP"):
            await broker._raw_open_order_proof()
        fake.wrapper._endReq("openOrders")
        await overlapping
        original = fake.wrapper.openOrder
        def observer(*args: Any) -> None:
            original(*args)
        fake.wrapper.openOrder = observer
        await broker._raw_open_order_proof()
        assert fake.wrapper.openOrder is observer
        assert "openOrderEnd" not in vars(fake.wrapper)
    asyncio.run(check())


@pytest.mark.parametrize("failure", [
    "missing_end", "extra_response", "callback_drift", "callback_after_end", "cancel",
    "missing_status", "partial_status", "partial_then_zero",
])
def test_raw_reader_incomplete_or_changed_callback_contract_fails_closed(failure: str) -> None:
    broker, fake = adapter()
    fake.wire_failure = failure
    expected = asyncio.CancelledError if failure == "cancel" else IbkrExecutionError
    with pytest.raises(expected):
        asyncio.run(submit(broker))
    assert not fake.attempts and not fake.connected
    assert all(name not in vars(fake.wrapper)
               for name in ("openOrder", "openOrderEnd", "orderStatus"))
    assert len(fake.positionEvent) == 0 and len(fake.errorEvent) == 1


def test_capture_failure_still_forwards_native_callbacks_and_restores(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker, fake = adapter()
    original_copy = ibkr_module.deepcopy
    def broken_copy(value: Any) -> Any:
        if isinstance(value, Stock):
            raise ValueError("capture failed")
        return original_copy(value)
    monkeypatch.setattr(ibkr_module, "deepcopy", broken_copy)
    with pytest.raises(IbkrExecutionError, match="INCOMPLETE"):
        asyncio.run(submit(broker))
    assert len(fake.wrapper.permId2Trade) == 2  # Original wrapper received both callbacks.
    assert "openOrder" not in vars(fake.wrapper) and "openOrderEnd" not in vars(fake.wrapper)
    assert not fake.attempts


def test_raw_reader_timeout_restores_all_handlers(monkeypatch: pytest.MonkeyPatch) -> None:
    broker, fake = adapter()
    fake.wire_failure = "timeout"
    monkeypatch.setattr(ibkr_module, "_OPEN_ORDERS_TIMEOUT_SECONDS", .01)
    with pytest.raises(TimeoutError):
        asyncio.run(submit(broker))
    assert all(name not in vars(fake.wrapper)
               for name in ("openOrder", "openOrderEnd", "orderStatus"))
    assert len(fake.positionEvent) == 0 and not fake.connected


def test_quantity_failure_path_never_isolates_new_rejection() -> None:
    broker, fake = adapter()
    with pytest.raises(IbkrExecutionError) as captured:
        asyncio.run(broker.submit_market_exit(
            "TEST", 10, oca_group="oca", order_ref="IA-E07-TEST-QTYFAIL",
        ))
    assert not isinstance(captured.value, ConfirmedNewOrderRejection)
    assert fake.execution_requests == 0


class PairExitIb(ExitIb):
    def __init__(self, broker: IbkrBroker, reject_symbol: str, *, target: bool) -> None:
        from insider_alerts.execution.canary import broker_token

        super().__init__(broker, target=target)
        self.reject_symbol = reject_symbol
        self.second = SimpleNamespace(account="ACCT", contract=Stock(
            "SECOND", "SMART", "USD", conId=124,
        ), position=10, avgCost=10)
        for symbol, packet in [("TEST", "one"), ("SECOND", "two")]:
            trades = self.active if symbol == "TEST" else deepcopy(self.active)
            for trade in trades:
                suffix = trade.order.orderRef.rsplit("-", 1)[1]
                trade.order.orderRef = f"IA-E07-{broker_token(packet)}-{suffix}"
                trade.order.ocaGroup = "oca-" + packet
                if symbol == "SECOND":
                    trade.contract = self.second.contract
                    trade.order.orderId += 10
                    trade.order.permId += 10
            if symbol == "SECOND":
                self.active.extend(trades)

    def positions(self, account: str) -> list[Any]:
        return [self.held, self.second]

    async def reqPositionsAsync(self) -> list[Any]:
        return [*(await super().reqPositionsAsync()), deepcopy(self.second)]

    def placeOrder(self, contract: Any, order: Any) -> Trade:
        self.mode = "confirmed" if contract.symbol == self.reject_symbol else "accepted"
        return super().placeOrder(contract, order)


def pair_runner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, reject_symbol: str = "TEST",
    market: bool = False,
) -> tuple[Any, PairExitIb, datetime]:
    from insider_alerts.backtest.signal_study import DeliveredSignal
    from insider_alerts.execution.calendar import NEW_YORK, bounds
    from insider_alerts.execution.canary import ARM_PHRASE, CanaryConfig, CanaryRunner

    now = datetime(2026, 11, 9, 20, 35 if not market else 50, tzinfo=UTC)
    class Clock(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> datetime:
            return now
    monkeypatch.setattr(ibkr_module, "datetime", Clock)
    broker = IbkrBroker(host="127.0.0.1", port=4001, client_id=1, account="ACCT")
    fake = PairExitIb(broker, reject_symbol, target=not market)
    broker.ib = fake
    broker._contracts.update(TEST=fake.contract, SECOND=fake.second.contract)
    broker.session_bounds = lambda when: bounds(when.astimezone(NEW_YORK).date())
    async def connect(**kwargs: Any) -> None:
        return None
    async def sessions(**kwargs: Any) -> list[Any]:
        return [now.date()]
    monkeypatch.setattr(broker, "connect", connect)
    monkeypatch.setattr(broker, "sessions", sessions)
    runner = CanaryRunner(CanaryConfig(source_db="unused", ledger_db=str(tmp_path / "live.db"),
                                      live_requested=True, arm_phrase=ARM_PHRASE), broker)
    for rank, (symbol, packet) in enumerate([("TEST", "one"), ("SECOND", "two")]):
        signal = DeliveredSignal(packet_id=packet, accession_number=packet, cik="1", symbol=symbol,
                                 filed_at=now, signal_at=now, score=.9, rationale={})
        runner.store.insert_candidate(signal, session=now.date(), rank=str(rank),
                                      is_eligible=True, reason="ok", prior_close=10,
                                      median_dollar_volume=1_000_000, quantity=10, now=now)
        runner.store.update(packet, live_state="open", shadow_state="complete", live_quantity=10,
                            live_entry_price=10, stop_order_id=20 + rank * 10,
                            target_order_id=None if market else 21 + rank * 10,
                            live_exit_session=now.date().isoformat(), oca_group="oca-" + packet)
    async def reconciled(*args: Any) -> tuple[int, int]:
        return 0, 0  # This integration isolates the due-exit phase after normal reconciliation.
    async def forbidden(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("A rejected timed exit must prevent auxiliary work and every new entry")
    monkeypatch.setattr(runner, "_reconcile_orders", reconciled)
    monkeypatch.setattr(runner, "_run_auxiliary", forbidden)
    monkeypatch.setattr(runner, "_submit_live_entries", forbidden)
    return runner, fake, now


@pytest.mark.parametrize("reject_symbol", ["TEST", "SECOND"])
@pytest.mark.parametrize("market", [False, True])
def test_real_adapter_two_positions_preserves_accepted_exit_and_fails_cycle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, reject_symbol: str, market: bool,
) -> None:
    runner, fake, now = pair_runner(
        tmp_path, monkeypatch, reject_symbol=reject_symbol, market=market,
    )
    with pytest.raises(IbkrExecutionError, match="TIMED_EXIT_REJECTIONS: confirmed=1; accepted=1"):
        asyncio.run(runner.cycle(now, disconnect_after=False))
    assert [trade.contract.symbol for trade in fake.attempts] == ["TEST", "SECOND"]
    assert fake.execution_requests == 1 and fake.position_requests >= 3
    rows = {row["symbol"]: row for row in runner.store.rows()}
    accepted = rows["SECOND" if reject_symbol == "TEST" else "TEST"]
    assert rows[reject_symbol]["timed_exit_order_id"] is None
    assert accepted["timed_exit_order_id"] == (101 if reject_symbol == "TEST" else 100)
    with runner.store.connect() as connection:
        events = list(connection.execute("SELECT event_type,detail_json FROM events"))
        metadata = dict(connection.execute("SELECT key,value FROM metadata"))
    rejections = [json.loads(e["detail_json"]) for e in events
                  if e["event_type"] == "timed_exit_rejected_confirmed"]
    assert len(rejections) == 1 and rejections[0]["broker_code"] == 201
    assert "TIMED_EXIT_REJECTIONS" in metadata["last_cycle_error"]
    assert "last_cycle_success_utc" not in metadata
    assert "last_live_management_success_utc" not in metadata
    assert len(fake.errorEvent) == 1 and len(fake.positionEvent) == 0


@pytest.mark.parametrize("changed", ["quantity", "protection"])
def test_next_row_gets_fresh_preflight_after_first_rejection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, changed: str,
) -> None:
    runner, fake, now = pair_runner(tmp_path, monkeypatch)
    original = runner._record_confirmed_exit_rejection
    def record(*args: Any) -> None:
        original(*args)
        if changed == "quantity":
            fake.second.position = 9
        else:
            next(t for t in fake.active if t.contract.symbol == "SECOND"
                 and t.order.orderType == "STP").orderStatus.status = "Cancelled"
    monkeypatch.setattr(runner, "_record_confirmed_exit_rejection", record)
    with pytest.raises(IbkrExecutionError, match="NOT_PROVEN"):
        asyncio.run(runner.cycle(now, disconnect_after=False))
    assert len(fake.attempts) == 1
    assert all(row["timed_exit_order_id"] is None for row in runner.store.rows())


@pytest.mark.parametrize("event_type,attempts", [
    ("timed_exit_rejected_confirmed", 1), ("timed_exit_submitted", 2),
])
def test_persistence_failure_stops_loop_and_rolls_back_acceptance_pointer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, event_type: str, attempts: int,
) -> None:
    runner, fake, now = pair_runner(tmp_path, monkeypatch)
    with runner.store.connect() as connection:
        connection.executescript(
            "CREATE TRIGGER reject_evidence BEFORE INSERT ON events "
            f"WHEN NEW.event_type='{event_type}' BEGIN "
            "SELECT RAISE(ABORT,'evidence unavailable'); END;"
        )
    with pytest.raises(sqlite3.IntegrityError, match="evidence unavailable"):
        asyncio.run(runner.cycle(now, disconnect_after=False))
    assert len(fake.attempts) == attempts
    assert all(row["timed_exit_order_id"] is None for row in runner.store.rows())
    assert len(fake.errorEvent) == 1 and runner.broker._exit_batch_errors is None
    assert runner.broker._exit_batch_receipts == []


@pytest.mark.parametrize("change", ["fill", "position"])
def test_late_first_rejection_change_during_second_preflight_stops_dispatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str,
) -> None:
    runner, fake, now = pair_runner(tmp_path, monkeypatch)
    original = fake.reqPositionsAsync
    async def changed() -> list[Any]:
        rows = await original()
        if fake.position_requests == 3:  # A baseline/end proof finished; B is refreshing.
            if change == "fill":
                fake.attempts[0].fills.append(SimpleNamespace())
            else:
                fake.held.position = 9
                rows[0].position = 9
        return rows
    monkeypatch.setattr(fake, "reqPositionsAsync", changed)
    with pytest.raises(IbkrExecutionError, match="BATCH_STATE_CHANGED"):
        asyncio.run(runner.cycle(now, disconnect_after=False))
    assert len(fake.attempts) == 1
    assert len(fake.errorEvent) == 1 and runner.broker._exit_batch_errors is None
    assert runner.broker._exit_batch_receipts == []


@pytest.mark.parametrize("market", [False, True])
def test_systemic_error_between_attempt_listeners_stops_next_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, market: bool,
) -> None:
    runner, fake, now = pair_runner(tmp_path, monkeypatch, market=market)
    observed_connected: list[bool] = []
    if market:
        original = fake.wire_orders
        async def orders() -> list[Trade]:
            rows = await original()
            if fake.order_requests == 6:  # Initial management refresh + A's four reads.
                observed_connected.append(fake.isConnected())
                fake.errorEvent.emit(-1, 1100, "gateway lost", None)
            return rows
        monkeypatch.setattr(fake, "wire_orders", orders)
    else:
        original_contract = runner.broker._contract
        async def contract(symbol: str) -> Any:
            if symbol == "SECOND":
                observed_connected.append(fake.isConnected())
                fake.errorEvent.emit(-1, 1100, "gateway lost", None)
            return await original_contract(symbol)
        monkeypatch.setattr(runner.broker, "_contract", contract)
    with pytest.raises(IbkrExecutionError, match="BATCH_STATE_CHANGED"):
        asyncio.run(runner.cycle(now, disconnect_after=False))
    assert observed_connected == [True]
    assert len(fake.attempts) == 1 and len(fake.errorEvent) == 1
    assert runner.broker._exit_batch_errors is None and runner.broker._exit_batch_receipts == []


def test_batch_cancellation_and_nested_entry_restore_listener_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker, fake = adapter()
    async def run() -> None:
        with broker.timed_exit_batch():
            with (pytest.raises(IbkrExecutionError, match="BATCH_OVERLAP"),
                  broker.timed_exit_batch()):
                pytest.fail("Nested batch must fail")
            fake.mode = "cancelled"
            await submit(broker)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(run())
    assert len(fake.errorEvent) == 1 and len(fake.positionEvent) == 0
    assert broker._exit_batch_errors is None and broker._exit_batch_receipts == []


@pytest.mark.parametrize("late_fill", [False, True])
def test_continuation_to_existing_target_revalidates_prior_refusal_before_modification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, late_fill: bool,
) -> None:
    runner, fake, now = pair_runner(tmp_path, monkeypatch, market=True)
    stop = next(t for t in fake.active if t.contract.symbol == "SECOND")
    target_order = LimitOrder("SELL", 10, 11, orderId=31, permId=131, clientId=1,
                              account="ACCT", ocaGroup=stop.order.ocaGroup, ocaType=1,
                              orderRef=stop.order.orderRef.replace("-STOP", "-TARGET"), tif="GTC")
    target = Trade(stop.contract, target_order, OrderStatus(status="Submitted", filled=0,
                                                           remaining=10))
    fake.active.append(target)
    runner.store.update("two", target_order_id=31)
    quote = SimpleNamespace(bid=10.5, bidSize=100, marketDataType=1, updateEvent=Event(),
                            ticks=[SimpleNamespace(tickType=1, price=10.5,
                                                   time=ibkr_module.datetime.fromtimestamp(
                                                       now.timestamp(), UTC))])
    def quote_request(*args: Any) -> Any:
        if late_fill:
            fake.attempts[0].fills.append(SimpleNamespace())
        asyncio.get_running_loop().call_soon(quote.updateEvent.emit, quote)
        return quote
    monkeypatch.setattr(fake, "reqMktData", quote_request, raising=False)
    monkeypatch.setattr(fake, "cancelMktData", lambda *args: None, raising=False)
    original = fake.placeOrder
    modifications: list[Any] = []
    def place(contract: Any, order: Any) -> Trade:
        if order.orderId == 31:
            modifications.append(order)
            target.order = order
            return target
        return original(contract, order)
    monkeypatch.setattr(fake, "placeOrder", place)
    reason = "BATCH_STATE_CHANGED" if late_fill else "TIMED_EXIT_REJECTIONS"
    with pytest.raises(IbkrExecutionError, match=reason):
        asyncio.run(runner.cycle(now, disconnect_after=False))
    assert len(fake.attempts) == 1 and len(modifications) == int(not late_fill)
    assert len(quote.updateEvent) == 0 and len(fake.errorEvent) == 1
    assert runner.broker.exit_modification_intent is None
    assert runner.broker._exit_batch_errors is None and runner.broker._exit_batch_receipts == []
