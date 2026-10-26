from __future__ import annotations

import asyncio
import copy
from datetime import UTC, date, datetime, timedelta
from importlib.metadata import version
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from ib_async import IB, RequestError, Stock
from ib_async.objects import BarData, HistoricalSchedule, HistoricalSession

from insider_alerts.research.commissioning_source import (
    CommissioningSource,
    broker_category,
    error_message,
)

NOW = datetime(2026, 10, 26, 22, tzinfo=UTC)
END = datetime(2026, 10, 26, 20, tzinfo=UTC)


async def exercise(
    monkeypatch: pytest.MonkeyPatch,
    *,
    response: Any = None,
    details_count: int = 1,
    conid: int | None = None,
    symbol: str = "SPY",
    schedule: bool = False,
    pending: bool = False,
    budget: Any = 1,
    connect_error: Exception | None = None,
    clock: Any = lambda: NOW,
    disconnect_error: bool = False,
    symbol_current_conid: int | None = None,
    symbol_error: Exception | None = None,
    notices: bool = False,
) -> tuple[dict[str, Any], dict[str, Any], IB]:
    ib = IB()
    state: dict[str, Any] = {"cancel": [], "disconnected": 0, "dispatch": []}
    contract = Stock("BRK B" if symbol == "BRK.B" else symbol, "SMART", "USD")
    contract.conId = 123
    contract.primaryExchange = "ARCA"
    contract.localSymbol = contract.symbol
    contract.tradingClass = contract.symbol
    monkeypatch.setattr(ib.client, "connectAsync", AsyncMock(side_effect=connect_error))
    monkeypatch.setattr(ib.client, "isReady", lambda: True)
    monkeypatch.setattr(ib.client, "getReqId", lambda: 7)
    monkeypatch.setattr(ib.client, "cancelHistoricalData", state["cancel"].append)
    current = copy.deepcopy(contract)
    if symbol_current_conid is not None:
        current.conId = symbol_current_conid
    monkeypatch.setattr(
        ib,
        "reqContractDetailsAsync",
        AsyncMock(
            side_effect=[
                [SimpleNamespace(contract=contract)] * details_count,
                symbol_error if symbol_error is not None else [SimpleNamespace(contract=current)],
            ]
        ),
    )

    def disconnect() -> None:
        state["disconnected"] += 1
        if disconnect_error:
            raise RuntimeError("injected cleanup error")

    def dispatch(*args: Any) -> None:
        state["dispatch"].append(args)
        state["dispatch_clock"] = clock()
        if notices:
            ib.errorEvent.emit(-1, 2105, "Historical data farm disconnected", None)
        if not pending:
            if isinstance(response, Exception):
                ib.wrapper._endReq(7, response, success=False)
            else:
                ib.wrapper._endReq(7, response if response is not None else [])

    monkeypatch.setattr(ib.client, "reqHistoricalData", dispatch)
    monkeypatch.setattr(ib, "disconnect", disconnect)
    source = CommissioningSource(
        clock=clock, budget=budget if callable(budget) else lambda: budget, factory=lambda: ib
    )
    result = await source.acquire(
        symbol, start=date(2026, 9, 15), end=END, expected_conid=conid, schedule=schedule
    )
    return result, state, ib


def test_history_identity_clocks_and_no_account_sync(monkeypatch: pytest.MonkeyPatch) -> None:
    bars = [BarData(date=date(2026, 10, 26), open=5, high=6, low=4, close=5, volume=100)]
    result, state, ib = asyncio.run(exercise(monkeypatch, response=bars))
    assert result["result"] == "received"
    assert result["identity"]["conid"] == 123
    assert result["identity"]["historical_issuer_identity_verified"] is False
    assert result["client_dispatch_at"] == "2026-10-26T22:00:00.000000Z"
    assert state["dispatch_clock"] == NOW
    assert result["response"][0]["date"] == "2026-10-26"
    args = state["dispatch"][0]
    assert args[4:] == ("1 day", "TRADES", True, 2, False, [])
    assert not state["cancel"] and state["disconnected"] == 1
    assert not ib.wrapper._futures and not ib.wrapper._results


def test_budget_expiring_immediately_before_dispatch_sends_no_request_or_cancel(monkeypatch):
    remaining = iter([1, 1, 1, 0])
    result, state, ib = asyncio.run(exercise(monkeypatch, budget=lambda: next(remaining)))
    assert result["result"] == "timeout" and result["client_dispatch_at"] is None
    assert not state["dispatch"] and not state["cancel"]
    assert not ib.wrapper._futures and not ib.wrapper._results


@pytest.mark.parametrize("code", [162, 200, 354, 366])
def test_broker_errors_retain_exact_code(monkeypatch: pytest.MonkeyPatch, code: int) -> None:
    result, state, _ = asyncio.run(exercise(monkeypatch, response=RequestError(7, code, "error")))
    assert result["result"] == "broker_error" and result["error_code"] == code
    assert not state["cancel"] and state["disconnected"] == 1


def test_timeout_is_not_empty_response(monkeypatch: pytest.MonkeyPatch) -> None:
    result, state, ib = asyncio.run(exercise(monkeypatch, pending=True, budget=0.001))
    assert result["result"] == "timeout"
    assert state["cancel"] == [7] and not ib.wrapper._futures
    assert state["disconnected"] == 1
    result, state, _ = asyncio.run(exercise(monkeypatch, response=[]))
    assert result["result"] == "empty_response" and state["cancel"] == []


@pytest.mark.parametrize("count,reason", [(0, "contract_missing"), (2, "contract_ambiguous")])
def test_qualification_cannot_fall_back(
    monkeypatch: pytest.MonkeyPatch, count: int, reason: str
) -> None:
    result, state, _ = asyncio.run(exercise(monkeypatch, details_count=count))
    assert result["result"] == reason
    assert not state["dispatch"] and state["disconnected"] == 1


def test_wrong_pinned_contract_prevents_history(monkeypatch: pytest.MonkeyPatch) -> None:
    result, state, _ = asyncio.run(exercise(monkeypatch, conid=999))
    assert result["result"] == "pinned_contract_not_returned" and not state["dispatch"]


def test_symbol_drift_does_not_replace_or_drop_pinned_history(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result, state, _ = asyncio.run(exercise(monkeypatch, conid=123, symbol_current_conid=999))
    assert result["symbol_resolution_changed"] is True
    assert result["symbol_resolution"]["conids"] == [999]
    assert state["dispatch"][0][1].conId == 123
    assert result["identity"]["conid"] == 123


def test_symbol_lookup_failure_still_requests_pinned_history(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result, state, _ = asyncio.run(
        exercise(
            monkeypatch, conid=123, symbol_error=RequestError(2, 200, "No security definition")
        )
    )
    assert result["symbol_resolution"]["code"] == 200
    assert state["dispatch"][0][1].conId == 123


def test_explicit_symbol_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    result, _, _ = asyncio.run(exercise(monkeypatch, symbol="BRK.B"))
    assert result["symbol"] == "BRK.B" and result["mapped_symbol"] == "BRK B"


def test_connect_failure_and_cleanup_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    result, state, _ = asyncio.run(exercise(monkeypatch, connect_error=ConnectionError()))
    assert result["result"] == "source_error" and result["stage"] == "connect"
    assert state["disconnected"] == 1 and not state["dispatch"]
    result, _, _ = asyncio.run(exercise(monkeypatch, disconnect_error=True))
    assert result["result"] == "empty_response" and result["cleanup_errors"] == ["RuntimeError"]


def test_clock_reversal_is_typed(monkeypatch: pytest.MonkeyPatch) -> None:
    times = iter([NOW, NOW - timedelta(seconds=1), NOW, NOW, NOW, NOW])
    result, _, _ = asyncio.run(exercise(monkeypatch, clock=lambda: next(times)))
    assert result["clock_integrity"] == "reversed" and result["result"] == "empty_response"


def test_schedule_projection_does_not_discard_future_or_contradictions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = HistoricalSchedule(
        startDateTime="20260915-00:00:00",
        endDateTime="20261026-16:00:00",
        timeZone="US/Eastern",
        sessions=[HistoricalSession("20261027-09:30:00", "20261027-16:00:00", "20261027")],
    )
    result, state, _ = asyncio.run(exercise(monkeypatch, response=response, schedule=True))
    assert result["response"]["sessions"][0]["date"] == "20261027"
    assert state["dispatch"][0][5] == "SCHEDULE"
    # Custody is raw; the commissioning validator must reject this contradiction.


def test_nonfinite_sdk_values_are_preserved_not_zeroed(monkeypatch: pytest.MonkeyPatch) -> None:
    result, _, _ = asyncio.run(exercise(monkeypatch, response=[BarData(open=float("nan"))]))
    assert result["response"][0]["open"] == "nan"


def test_off_hours_budget_exhaustion_has_no_dispatch(monkeypatch: pytest.MonkeyPatch) -> None:
    result, state, _ = asyncio.run(exercise(monkeypatch, budget=0))
    assert result["result"] == "timeout" and not state["dispatch"]


@pytest.mark.parametrize(
    "message,category",
    [
        ("Historical data request pacing violation", "hmds_162_pacing"),
        ("No market data permissions for ISLAND", "hmds_162_permissions"),
        ("HMDS query returned no data", "hmds_162_no_data"),
        ("Unknown historical service error", "hmds_162_unclassified"),
    ],
)
def test_error_162_does_not_imply_only_pacing(message: str, category: str) -> None:
    assert broker_category(162, message) == category


def test_error_messages_are_bounded_and_sanitized() -> None:
    value = error_message("Account U1234567\n https://example.test/?token=secret " + "x" * 3000)
    assert len(value) <= 2000
    assert "U1234567" not in value and "secret" not in value and "\n" not in value


def test_source_versions_and_farm_notices(monkeypatch: pytest.MonkeyPatch) -> None:
    result, _, ib = asyncio.run(exercise(monkeypatch, notices=True))
    assert version("ib_async") == result["sdk_version"] == "2.1.0"
    assert callable(ib.wrapper.startReq) and callable(ib.wrapper._endReq)
    assert isinstance(ib.wrapper._results, dict)
    assert result["server_version"] == 0  # The fake session has no real server.
    assert result["connection"]["client_id"] == 179
    assert result["notices"][0]["code"] == 2105
