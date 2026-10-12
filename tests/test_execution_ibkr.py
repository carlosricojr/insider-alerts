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

    async def _contract(symbol: str) -> str:
        return symbol

    broker._contract = _contract
    return broker


@pytest.mark.parametrize(
    "status", ["Inactive", "Cancelled", "ApiCancelled", "Submitted", "ValidationError", "Filled"]
)
def test_moc_diagnostics_preserve_submission_behavior(
    monkeypatch: pytest.MonkeyPatch, status: str
) -> None:
    broker = _mk_broker()
    orders: list[ib_async.Order] = []
    sleeps: list[float] = []
    broker.ib.trades = lambda: []

    def place_order(contract: str, order: ib_async.Order) -> _State:
        assert contract == "TEST"
        orders.append(order)
        order.orderId = 42
        broker._on_error(-1, 1100, "connection error must not appear", None)
        broker._on_error(43, 201, "other order must not appear", None)
        broker._on_error(42, 2104, "farm message must not appear", None)
        broker._on_error(42, 201, "Rejected for ACCT: reason from broker", None)
        return _State(order=order, orderStatus=_State(status=status))

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    broker.ib.placeOrder = place_order
    monkeypatch.setattr(ibkr_module.asyncio, "sleep", sleep)
    call = broker.submit_market_on_close("TEST", 7, oca_group="exit-group", order_ref="ref")
    if status in {"Inactive", "Cancelled", "ApiCancelled"}:
        with pytest.raises(IbkrExecutionError) as captured:
            asyncio.run(call)
        assert str(captured.value) == (
            f"IBKR rejected market-on-close for TEST: order_id=42 status={status}; "
            "code=201 message=Rejected for [account]: reason from broker"
        )
    else:
        assert asyncio.run(call) == 42
    assert sleeps == [0.5]
    assert len(orders) == 1
    order = orders[0]
    assert (order.action, order.totalQuantity, order.orderType, order.tif) == (
        "SELL",
        7,
        "MOC",
        "DAY",
    )
    assert (order.account, order.ocaGroup, order.ocaType, order.orderRef, order.transmit) == (
        "ACCT",
        "exit-group",
        1,
        "ref",
        True,
    )


def test_moc_existing_order_returns_without_resubmission() -> None:
    broker = _mk_broker()
    broker.ib.trades = lambda: [
        _State(order=_State(orderRef="ref", orderId=42), orderStatus=_State(status="Submitted"))
    ]
    assert (
        asyncio.run(
            broker.submit_market_on_close("TEST", 7, oca_group="exit-group", order_ref="ref")
        )
        == 42
    )


def test_moc_diagnostics_missing_and_malformed_evidence() -> None:
    broker = _mk_broker()
    assert broker._moc_rejection_detail(42, "ACCT", "Inactive") == (
        "order_id=42 status=Inactive; no broker error recorded"
    )
    # Unexpected payloads must not stringify arbitrary objects or mask the rejection.
    broker._errors.append((42, object(), object()))  # type: ignore[arg-type]
    assert broker._moc_rejection_detail(42, "ACCT", "Inactive") == (
        "order_id=42 status=Inactive; code=unknown message=broker message unavailable"
    )


def test_moc_diagnostics_redact_before_truncation_and_bound_output() -> None:
    broker = _mk_broker()
    broker.configured_account = "configured-account"
    broker._on_error(42, 200, "old error not included", None)
    message = (
        "ACCT configured-account U1234567 DU1234567 DF1234567 F1234567 "
        "U123\n4567\x00\r\t\u202e" + "x" * 175 + "U7654321 " + "x" * 2000
    )
    for code in (201, 202, 203):
        broker._on_error(42, code, message, None)
    detail = broker._moc_rejection_detail(42, "ACCT", "Inactive")
    assert "old error" not in detail
    assert detail.count("code=") == 3
    assert all(f"code={code}" in detail for code in (201, 202, 203))
    assert "[account]" in detail
    assert all(
        value not in detail
        for value in (
            "ACCT",
            "configured-account",
            "1234567",
            "7654321",
            "U765",
            "\n",
            "\x00",
            "\r",
            "\t",
            "\u202e",
        )
    )
    assert len(detail) < 850
    # Deliberately straddle the raw-text cutoff: truncation first would leak "U7".
    broker._errors.clear()
    broker._on_error(42, 201, "x" * 238 + "U7654321", None)
    detail = broker._moc_rejection_detail(42, "ACCT", "Inactive")
    assert "U7" not in detail
    assert "message=" + "x" * 238 + "[a..." in detail


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
