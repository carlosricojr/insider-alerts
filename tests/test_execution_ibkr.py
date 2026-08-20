from __future__ import annotations

import asyncio
import sys

import ib_async
import pytest

import insider_alerts.execution.ibkr as ibkr_module
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

        async def connectAsync(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            self.connected = True
            raise ConnectionError("handshake failed")

        def isConnected(self) -> bool:
            return self.connected and not self.disconnected

        def disconnect(self) -> None:
            self.disconnected = True

    fake = _FailingIB()
    monkeypatch.setattr(ib_async, "IB", lambda: fake)
    broker = IbkrBroker(host="127.0.0.1", port=4001, client_id=1)

    with pytest.raises(IbkrExecutionError, match="Gateway unavailable"):
        asyncio.run(broker.connect(readonly=False))

    assert fake.disconnected is True
    assert broker.ib is None


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
