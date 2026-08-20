from __future__ import annotations

import asyncio
import sys

import pytest

import insider_alerts.execution.ibkr as ibkr_module
from insider_alerts.execution.ibkr import IbkrBroker


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

    async def _what_if(*args, **kwargs):  # noqa: ARG001
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
