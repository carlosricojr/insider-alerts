from __future__ import annotations

import asyncio
import sys

import ib_async
import pytest

import insider_alerts.execution.ibkr as ibkr_module
from insider_alerts.execution.errors import (
    ContractQualificationError,
    IbkrContractQualificationError,
)
from insider_alerts.execution.ibkr import IbkrBroker, IbkrExecutionError


class _State:
    def __init__(self, **kwargs: object) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)


def _mk_broker() -> IbkrBroker:
    broker = IbkrBroker(host="127.0.0.1", port=4001, client_id=1)
    broker.ib = type(
        "_FakeIb",
        (),
        {
            "managedAccounts": staticmethod(lambda: ["ACCT"]),
        },
    )()

    async def _contract(symbol: str) -> object:
        return _State(symbol=symbol, conId=123, secType="STK", currency="USD")

    broker._contract = _contract
    return broker


def test_unqualified_contract_uses_terminal_contract_error() -> None:
    broker = IbkrBroker(host="127.0.0.1", port=4001, client_id=1)

    class _FakeIb:
        RaiseRequestErrors = False

        async def qualifyContractsAsync(self, contract):  # type: ignore[no-untyped-def]
            assert self.RaiseRequestErrors is True
            return []

    fake = _FakeIb()
    broker.ib = fake

    with pytest.raises(IbkrContractQualificationError) as captured:
        asyncio.run(broker._contract("bad"))

    assert isinstance(captured.value, ContractQualificationError)
    assert isinstance(captured.value, IbkrExecutionError)
    assert "BAD" in str(captured.value)
    assert fake.RaiseRequestErrors is False


def test_contract_maps_only_error_200_to_terminal_qualification_error() -> None:
    from ib_async import RequestError

    broker = IbkrBroker(host="127.0.0.1", port=4001, client_id=1)

    class _FakeIb:
        def __init__(self, error: RequestError) -> None:
            self.error = error
            self.RaiseRequestErrors = False

        async def qualifyContractsAsync(self, contract):  # type: ignore[no-untyped-def]
            assert self.RaiseRequestErrors is True
            raise self.error

    terminal_broker = _FakeIb(RequestError(1, 200, "No security definition"))
    broker.ib = terminal_broker
    with pytest.raises(IbkrContractQualificationError):
        asyncio.run(broker._contract("bad"))
    assert terminal_broker.RaiseRequestErrors is False

    transient = RequestError(2, 162, "Historical data service error")
    transient_broker = _FakeIb(transient)
    broker.ib = transient_broker
    with pytest.raises(IbkrExecutionError) as captured:
        asyncio.run(broker._contract("retry"))
    assert captured.value.__cause__ is transient
    assert transient_broker.RaiseRequestErrors is False


def test_preview_entry_marks_unexpected_payload_invalid() -> None:
    broker = _mk_broker()

    async def _what_if(*args, **kwargs):  # noqa: ARG001
        return _State()

    broker.ib.whatIfOrderAsync = _what_if

    result = asyncio.run(broker.preview_entry("TEST", 1))

    assert result.commission_valid is False
    assert result.commission_error == "unexpected what-if payload"
    assert result.warning == "what-if payload was not an order state"


def test_preview_entry_marks_sentinel_commission_invalid() -> None:
    broker = _mk_broker()

    async def _what_if(*args, **kwargs):  # noqa: ARG001
        return _State(
            commission=sys.float_info.max,
            commissionCurrency="USD",
            warningText="",
        )

    broker.ib.whatIfOrderAsync = _what_if

    result = asyncio.run(broker.preview_entry("TEST", 1))

    assert result.commission_valid is False
    assert result.commission_error == "invalid IBKR commission preview for preflight"
    assert result.warning == ""


def test_preview_entry_uses_tiered_range_upper_bound() -> None:
    broker = _mk_broker()
    captured_order: dict[str, object] = {}

    async def _what_if(contract, order):  # noqa: ARG001
        captured_order["order"] = order
        return _State(
            commission=sys.float_info.max,
            minCommission=0.33577225,
            maxCommission=0.36627225,
            commissionCurrency="USD",
            warningText="",
        )

    broker.ib.whatIfOrderAsync = _what_if

    result = asyncio.run(broker.preview_entry("TEST", 5))

    assert result.commission_valid is True
    assert result.commission == pytest.approx(0.36627225)
    assert result.min_commission == pytest.approx(0.33577225)
    assert result.max_commission == pytest.approx(0.36627225)
    assert result.estimate_source == "range_upper_bound"
    assert result.commission_error == ""
    assert not bool(getattr(captured_order["order"], "allOrNone", False))


def test_preview_entry_defaults_missing_commission_currency_to_usd() -> None:
    broker = _mk_broker()

    async def _what_if(*args, **kwargs):  # noqa: ARG001
        return _State(commission=0.35, warningText="")

    broker.ib.whatIfOrderAsync = _what_if

    result = asyncio.run(broker.preview_entry("TEST", 1))

    assert result.commission_valid is False
    assert result.currency == "USD"
    assert result.commission_error == "IBKR commission preview missing currency"


@pytest.mark.parametrize(
    ("minimum", "maximum"),
    [
        (0.40, 0.35),
        (float("nan"), 0.35),
        (0.35, sys.float_info.max),
        (None, 0.35),
    ],
)
def test_preview_entry_rejects_malformed_tiered_range(
    minimum: float | None,
    maximum: float,
) -> None:
    broker = _mk_broker()

    async def _what_if(*args, **kwargs):  # noqa: ARG001
        return _State(
            commission=sys.float_info.max,
            minCommission=minimum,
            maxCommission=maximum,
            commissionCurrency="USD",
            warningText="",
        )

    broker.ib.whatIfOrderAsync = _what_if

    result = asyncio.run(broker.preview_entry("TEST", 5))

    assert result.commission_valid is False
    assert result.estimate_source == "unavailable"


def test_preview_entry_prefers_exact_commission_over_range() -> None:
    broker = _mk_broker()

    async def _what_if(*args, **kwargs):  # noqa: ARG001
        return _State(
            commission=0.34,
            minCommission=0.30,
            maxCommission=0.38,
            commissionCurrency="USD",
            warningText="",
        )

    broker.ib.whatIfOrderAsync = _what_if

    result = asyncio.run(broker.preview_entry("TEST", 5))

    assert result.commission_valid is True
    assert result.commission == pytest.approx(0.34)
    assert result.estimate_source == "exact"


def test_preview_entry_captures_broker_error_warning() -> None:
    broker = _mk_broker()

    async def _what_if(*args, **kwargs):  # noqa: ARG001
        broker._on_error(0, 110, "what-if rejected", None)
        return _State(
            commission=0.35,
            commissionCurrency="USD",
            warningText="",
        )

    broker.ib.whatIfOrderAsync = _what_if

    result = asyncio.run(broker.preview_entry("TEST", 1))

    assert result.commission_valid is True
    assert "what-if rejected" in result.warning


def test_preview_entry_timeout_maps_to_invalid(monkeypatch: pytest.MonkeyPatch) -> None:
    broker = _mk_broker()

    async def _what_if(*args, **kwargs):  # noqa: ARG001
        return _State()

    async def _instant_timeout(awaitable, timeout):  # noqa: ARG001
        await awaitable
        raise TimeoutError()

    monkeypatch.setattr(ibkr_module.asyncio, "wait_for", _instant_timeout)
    broker.ib.whatIfOrderAsync = _what_if

    result = asyncio.run(broker.preview_entry("TEST", 1))

    assert result.commission_valid is False
    assert result.commission_error == "what-if preview timed out"


def test_connect_disconnects_partially_connected_client(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Event:
        def __iadd__(self, callback):  # type: ignore[no-untyped-def]
            return self

        def __isub__(self, callback):  # type: ignore[no-untyped-def]
            return self

    class _FailingIB:
        def __init__(self) -> None:
            self.errorEvent = _Event()
            self.connected = False
            self.disconnected = False
            self.connect_kwargs: dict[str, object] = {}

        async def connectAsync(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            self.connect_kwargs = kwargs
            self.connected = True
            raise ConnectionError("handshake failed")

        def isConnected(self) -> bool:
            return self.connected and not self.disconnected

        def disconnect(self) -> None:
            self.disconnected = True

    fake = _FailingIB()
    monkeypatch.setattr(ib_async, "IB", lambda: fake)
    broker = IbkrBroker(host="127.0.0.1", port=4001, client_id=1)

    with pytest.raises(IbkrExecutionError, match="IBKR_GATEWAY_STARTUP_SYNC_FAILED"):
        asyncio.run(broker.connect(readonly=False))

    assert fake.disconnected is True
    assert fake.connect_kwargs["raiseSyncErrors"] is True
    assert broker.ib is None


def test_connect_classifies_empty_handshake_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Event:
        def __iadd__(self, callback):  # type: ignore[no-untyped-def]
            return self

        def __isub__(self, callback):  # type: ignore[no-untyped-def]
            return self

    class _TimedOutIB:
        def __init__(self) -> None:
            self.errorEvent = _Event()

        async def connectAsync(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            raise TimeoutError()

        def isConnected(self) -> bool:
            return False

        def disconnect(self) -> None:
            pass

    monkeypatch.setattr(ib_async, "IB", _TimedOutIB)
    broker = IbkrBroker(host="127.0.0.1", port=4001, client_id=1)

    with pytest.raises(IbkrExecutionError) as captured:
        asyncio.run(broker.connect(readonly=False))

    assert str(captured.value) == (
        "IBKR_GATEWAY_HANDSHAKE_TIMEOUT: IB Gateway unavailable at "
        "127.0.0.1:4001: TimeoutError"
    )


def test_readonly_connect_skips_unneeded_order_startup_fetches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Event:
        def __iadd__(self, callback):  # type: ignore[no-untyped-def]
            return self

        def emit(self) -> None:
            pass

    class _ReadonlyIB:
        def __init__(self) -> None:
            self.errorEvent = _Event()
            self.wrapper = _State(clientId=-1)
            self.connectedEvent = _Event()
            self.client = _ReadonlyClient()

        async def connectAsync(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            raise AssertionError("readonly connection must not start account synchronization")

        def reqMarketDataType(self, market_data_type: int) -> None:
            assert market_data_type == 1

        def isConnected(self) -> bool:
            return True

    class _ReadonlyClient:
        def __init__(self) -> None:
            self.connect_args: tuple[object, ...] = ()

        async def connectAsync(self, *args) -> None:  # type: ignore[no-untyped-def]
            self.connect_args = args

        def isReady(self) -> bool:
            return True

    fake = _ReadonlyIB()
    monkeypatch.setattr(ib_async, "IB", lambda: fake)
    broker = IbkrBroker(host="127.0.0.1", port=4001, client_id=1)

    asyncio.run(broker.connect(readonly=True))

    assert fake.wrapper.clientId == 1
    assert fake.client.connect_args == ("127.0.0.1", 4001, 1, 10.0)


def test_account_snapshot_times_out_unknown_order_state_and_disconnects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Event:
        def __isub__(self, callback):  # type: ignore[no-untyped-def]
            return self

    class _StalledIB:
        def __init__(self) -> None:
            self.errorEvent = _Event()
            self.disconnected = False

        def managedAccounts(self) -> list[str]:
            return ["ACCT"]

        async def reqAllOpenOrdersAsync(self) -> list[object]:
            return []

        def isConnected(self) -> bool:
            return not self.disconnected

        def disconnect(self) -> None:
            self.disconnected = True

    async def _instant_timeout(awaitable, timeout):  # noqa: ARG001
        await awaitable
        raise TimeoutError()

    fake = _StalledIB()
    broker = IbkrBroker(host="127.0.0.1", port=4001, client_id=1)
    broker.ib = fake
    monkeypatch.setattr(ibkr_module.asyncio, "wait_for", _instant_timeout)

    with pytest.raises(IbkrExecutionError, match="IBKR_ALL_OPEN_ORDERS_TIMEOUT"):
        asyncio.run(broker.account_snapshot())

    assert fake.disconnected is True
    assert broker.ib is None
    assert broker._all_open_trades == []
    assert broker._all_orders_checked_at == 0.0


def test_submit_market_on_open_allows_partial_fills() -> None:
    broker = _mk_broker()
    captured: dict[str, object] = {}

    def _place_order(contract, order):  # type: ignore[no-untyped-def]
        captured["order"] = order
        return _State(
            order=order,
            orderStatus=_State(status="PreSubmitted"),
            log=[],
        )

    broker.ib.placeOrder = _place_order
    broker._existing_trade = lambda order_ref: None  # type: ignore[method-assign]

    order_id = asyncio.run(broker.submit_market_on_open("TEST", 5, "ENTRY"))

    assert order_id == int(captured["order"].orderId)  # type: ignore[attr-defined]
    assert not bool(getattr(captured["order"], "allOrNone", False))


@pytest.mark.parametrize("conflict,position", [(False, 16), (True, 16), (False, 15), (False, 0)])
def test_missing_exit_uses_fresh_account_orders_and_residual(conflict: bool, position: int) -> None:
    broker = _mk_broker()
    async def orders():
        return ([_State(order=_State(account="ACCT", ocaGroup="other", action="SELL",
                                    orderRef="foreign", orderType="MKT"),
                        contract=_State(symbol="TEST"))] if conflict else [])
    async def positions():
        return [_State(account="ACCT", contract=_State(
            symbol="TEST", conId=123, secType="STK", currency="USD"), position=position)]
    broker.ib.reqAllOpenOrdersAsync = orders
    broker.ib.reqPositionsAsync = positions
    assert asyncio.run(broker.confirm_missing_exit(
        "TEST", 16, oca_group="oca", order_ref_prefix="IA-E07-TEST",
    )) == (not conflict and position == 16)


def test_missing_exit_timeout_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    broker = _mk_broker()
    async def orders():
        raise TimeoutError()
    broker.ib.reqAllOpenOrdersAsync = orders
    broker.disconnect = lambda: None  # type: ignore[method-assign]
    with pytest.raises(IbkrExecutionError, match="MISSING_EXIT_RECONCILIATION_FAILED"):
        asyncio.run(broker.confirm_missing_exit(
            "TEST", 16, oca_group="oca", order_ref_prefix="ref",
        ))


def test_market_exit_does_not_reuse_cached_filled_order() -> None:
    broker = _mk_broker()
    async def orders():
        return []
    async def positions():
        return [_State(account="ACCT", contract=_State(
            symbol="TEST", conId=123, secType="STK", currency="USD"), position=16)]
    broker.ib.reqAllOpenOrdersAsync = orders
    broker.ib.reqPositionsAsync = positions
    broker._existing_trade = lambda ref: _State(order=_State(orderId=123))  # type: ignore[method-assign]
    sent = []
    def place(contract, order):
        sent.append(order)
        order.orderId = 456
        return _State(order=order, orderStatus=_State(status="Submitted"), log=[])
    broker.ib.placeOrder = place
    assert asyncio.run(broker.submit_market_exit(
        "TEST", 16, oca_group="oca", order_ref="IA-E07-TEST-OVERDUE",
    )) == 456
    assert len(sent) == 1


@pytest.mark.parametrize("kind,allowed", [("QTYFAIL", True), ("OVERDUE", False)])
def test_market_exit_quantity_failure_keeps_emergency_residual_flatten(kind: str, allowed: bool):
    broker = _mk_broker()
    protective = _State(
        contract=_State(symbol="TEST", conId=123, secType="STK", currency="USD"),
        order=_State(account="ACCT", action="SELL", orderRef="IA-E07-TEST-TARGET",
                     orderType="LMT", ocaGroup="oca", ocaType=1, totalQuantity=20),
        orderStatus=_State(status="Submitted", filled=5),
    )
    async def orders():
        return [protective]
    async def positions():
        return [_State(account="ACCT", contract=protective.contract, position=15)]
    broker.ib.reqAllOpenOrdersAsync = orders
    broker.ib.reqPositionsAsync = positions
    placed = []
    def place(contract, order):
        placed.append(order)
        order.orderId = 456
        return _State(order=order, orderStatus=_State(status="Submitted"), log=[])
    broker.ib.placeOrder = place
    coro = broker.submit_market_exit(
        "TEST", 15, oca_group="oca", order_ref=f"IA-E07-TEST-{kind}",
    )
    if allowed:
        assert asyncio.run(coro) == 456
        assert placed[0].totalQuantity == 15
        assert placed[0].ocaGroup == "oca"
    else:
        with pytest.raises(IbkrExecutionError, match="POSITION_OR_ORDER_CONFLICT"):
            asyncio.run(coro)
        assert not placed


@pytest.mark.parametrize("status", ["Inactive", "Cancelled", "ApiCancelled"])
def test_exit_rejection_retains_order_specific_broker_error(status: str):
    broker = _mk_broker()
    broker._errors.extend([(77, 201, "equity insufficient U1234567"),
                           (88, 201, "unrelated private rejection")])
    detail = broker._order_rejection_detail(77, "ACCT", status)
    assert "code=201" in detail and "equity insufficient" in detail
    assert "U1234567" not in detail and "unrelated" not in detail
    assert f"status={status}" in detail


def test_exit_rejection_bounds_and_marks_missing_reason():
    broker = _mk_broker()
    assert "no broker error recorded" in broker._order_rejection_detail(77, "ACCT", "Cancelled")
    broker._errors.extend((77, 201, "x" * 1000) for _ in range(10))
    detail = broker._order_rejection_detail(77, "ACCT", "Cancelled")
    assert detail.count("code=201") == 3 and len(detail) < 900


@pytest.mark.parametrize("outcome", ["accepted", "rejected", "foreign", "partial"])
def test_due_exit_reprices_existing_target_without_another_sell(outcome: str):
    from copy import deepcopy
    from datetime import UTC, datetime, timedelta

    from ib_async import LimitOrder, OrderStatus, Stock, StopOrder, Trade

    broker = _mk_broker()
    contract = Stock("TEST", "SMART", "USD", conId=123)
    target_order = LimitOrder("SELL", 16, 13.76, account="ACCT", orderId=20,
                              permId=120, clientId=1, ocaGroup="oca", ocaType=1,
                              orderRef="IA-E07-TEST-TARGET", tif="GTC")
    stop_order = StopOrder("SELL", 16, 11.26, account="ACCT", orderId=21,
                          permId=121, clientId=1, ocaGroup="oca", ocaType=1,
                          orderRef="IA-E07-TEST-STOP", tif="GTC")
    target = Trade(contract, target_order, OrderStatus(status="Submitted", filled=0, remaining=16))
    stop = Trade(contract, stop_order, OrderStatus(status="PreSubmitted", filled=0, remaining=16))
    original = deepcopy(target.order)
    if outcome == "foreign":
        target.order.clientId = 99
    if outcome == "partial":
        target.orderStatus.filled = 1
    async def orders():
        return [target, stop]
    async def positions():
        return [_State(account="ACCT", contract=contract, position=16)]
    broker.ib.reqAllOpenOrdersAsync = orders
    broker.ib.reqPositionsAsync = positions
    broker._check_exit_window = lambda window: None  # type: ignore[method-assign]
    disconnected = []
    broker.disconnect = lambda: disconnected.append(True)  # type: ignore[method-assign]
    placed = []
    def place(_contract, order):
        placed.append(order)
        assert order is not target_order
        if outcome == "rejected":
            broker._errors.append((20, 201, "rejected modification"))
        else:
            target.order = order  # Emulate openOrder callback acknowledgement.
        return target
    broker.ib.placeOrder = place
    now = datetime.now(UTC)
    coro = broker.submit_market_exit("TEST", 16, oca_group="oca",
                                    order_ref="IA-E07-TEST-OVERDUE",
                                    dispatch_window=(now, now + timedelta(minutes=1)))
    if outcome == "accepted":
        assert asyncio.run(coro) == 20
        expected = deepcopy(original)
        expected.lmtPrice = 0.01
        assert len(placed) == 1
        assert vars(placed[0]) == vars(expected)
        assert stop.order == stop_order
        assert not disconnected
    else:
        with pytest.raises(IbkrExecutionError):
            asyncio.run(coro)
        assert len(placed) == int(outcome == "rejected")
        if outcome == "rejected":
            assert disconnected
            assert target.order.lmtPrice == 13.76
