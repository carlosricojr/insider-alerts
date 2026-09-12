from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace

import pytest
from ib_async import RequestError

from insider_alerts.execution.calendar import (
    CALENDAR_SHA256,
    SOURCE,
    bounds,
    calendar_dates,
    validate_contract_hours,
    validate_native_schedule,
)
from insider_alerts.execution.errors import IbkrExecutionError
from insider_alerts.execution.ibkr import IbkrBroker
from insider_alerts.research.ibkr_bar_source import IbkrHistoricalBarSource

NOW = datetime(2026, 9, 12, 19, 32, tzinfo=UTC)
CONTRACT = SimpleNamespace(conId=756733, symbol="SPY", secType="STK", currency="USD")
HOURS = "20260912:CLOSED;20260913:CLOSED;20260914:0930-20260914:1600;" "20260915:0930-20260915:1600"


def details(hours: str = HOURS) -> list[SimpleNamespace]:
    return [SimpleNamespace(contract=CONTRACT, timeZoneId="US/Eastern", liquidHours=hours)]


@pytest.mark.parametrize(
    "day",
    [
        "2026-01-01",
        "2026-01-19",
        "2026-02-16",
        "2026-04-03",
        "2026-05-25",
        "2026-06-19",
        "2026-07-03",
        "2026-09-07",
        "2026-11-26",
        "2026-12-25",
        "2026-09-12",
        "2026-09-13",
    ],
)
def test_published_closures(day: str) -> None:
    assert bounds(date.fromisoformat(day)) is None


@pytest.mark.parametrize(
    "day,open_hour,close_hour",
    [
        ("2026-09-14", 13, 20),
        ("2026-11-02", 14, 21),
        ("2026-11-27", 14, 18),
        ("2026-12-24", 14, 18),
    ],
)
def test_published_boundaries_and_dst(day: str, open_hour: int, close_hour: int) -> None:
    session = bounds(date.fromisoformat(day))
    assert session is not None
    assert session[0].astimezone(UTC).hour == open_hour
    assert session[0].minute == 30
    assert session[1].astimezone(UTC).hour == close_hour


def test_range_and_tenth_session_exclude_labor_day() -> None:
    dates = calendar_dates(NOW, 120)
    horizon = [day for day in dates if day >= date(2026, 8, 31)][:10]
    assert len(horizon) == 10
    assert date(2026, 9, 7) not in horizon
    assert horizon[-1] == date(2026, 9, 14)
    assert len(CALENDAR_SHA256) == 64
    with pytest.raises(IbkrExecutionError, match="UNSUPPORTED_RANGE"):
        calendar_dates(datetime(2027, 1, 1, 12, tzinfo=UTC), 120)
    bounded = calendar_dates(datetime(2026, 12, 1, tzinfo=UTC), 120)
    assert bounded[-1] == date(2026, 12, 31)
    with pytest.raises(IbkrExecutionError, match="INVALID_REQUEST"):
        calendar_dates(NOW.replace(tzinfo=None), 120)


@pytest.mark.parametrize(
    "hours",
    [
        "",
        HOURS + ";",
        HOURS + ";20260912:CLOSED",
        HOURS.replace("0930", "0931"),
        HOURS.replace("20260913:CLOSED;", ""),
        HOURS.replace("20260914:0930-20260914:1600", "20260914:CLOSED"),
        "20260912:CLOSED;20260913:CLOSED",
        "20260911:0930-20260911:1600",
        HOURS.replace("20260914:1600", "20260915:1600"),
        HOURS + ";20261225:CLOSED",
        HOURS.replace("0930-20260914:1600", "0930-20260914:1200,1300-20260914:1600"),
        "x" * 4097,
    ],
)
def test_hours_fail_closed(hours: str) -> None:
    with pytest.raises(IbkrExecutionError):
        validate_contract_hours(details(hours), CONTRACT, NOW)


@pytest.mark.parametrize(
    "field,value", [("conId", 0), ("symbol", "QQQ"), ("secType", "OPT"), ("currency", "EUR")]
)
def test_identity_fail_closed(field: str, value: object) -> None:
    rows = details()
    changed = vars(CONTRACT).copy()
    changed[field] = value
    rows[0].contract = SimpleNamespace(**changed)
    with pytest.raises(IbkrExecutionError, match="IDENTITY"):
        validate_contract_hours(rows, CONTRACT, NOW)


def test_coverage_identity_and_timezone() -> None:
    assert validate_contract_hours(details(), CONTRACT, NOW)["hours_first_date"] == "2026-09-12"
    for rows in ([], details() * 2):
        with pytest.raises(IbkrExecutionError):
            validate_contract_hours(rows, CONTRACT, NOW)
    rows = details()
    rows[0].timeZoneId = "UTC"
    with pytest.raises(IbkrExecutionError):
        validate_contract_hours(rows, CONTRACT, NOW)


def native() -> SimpleNamespace:
    rows = []
    for day in calendar_dates(NOW, 120):
        session = bounds(day)
        assert session is not None
        rows.append(
            SimpleNamespace(
                refDate=day.strftime("%Y%m%d"),
                startDateTime=session[0].strftime("%Y%m%d-%H:%M:%S"),
                endDateTime=session[1].strftime("%Y%m%d-%H:%M:%S"),
            )
        )
    return SimpleNamespace(timeZone="US/Eastern", sessions=rows)


def test_native_rejects_contradictions_incomplete_and_duplicate() -> None:
    expected = calendar_dates(NOW, 120)
    assert validate_native_schedule(native(), expected) == expected
    for mode in ("missing", "duplicate", "holiday", "malformed"):
        value = native()
        if mode == "missing":
            value.sessions.pop()
        elif mode == "duplicate":
            value.sessions.append(value.sessions[0])
        elif mode == "holiday":
            value.sessions.append(
                SimpleNamespace(
                    refDate="20260907",
                    startDateTime="20260907-09:30:00",
                    endDateTime="20260907-16:00:00",
                )
            )
        else:
            value.sessions = [object()]
        with pytest.raises(IbkrExecutionError):
            validate_native_schedule(value, expected)


class FakeIb:
    RaiseRequestErrors = False

    def __init__(self, response: object, hours: str = HOURS) -> None:
        self.response = response
        self.hours = hours
        self.details_requested = 0
        self.connected = True

    def isConnected(self) -> bool:
        return self.connected

    def disconnect(self) -> None:
        self.connected = False

    async def reqHistoricalScheduleAsync(self, *args: object) -> object:
        assert self.RaiseRequestErrors
        if isinstance(self.response, BaseException):
            raise self.response
        return self.response

    async def reqContractDetailsAsync(self, *args: object) -> object:
        self.details_requested += 1
        return details(self.hours)


def broker(fake: FakeIb, *, armed: bool = True) -> IbkrBroker:
    value = IbkrBroker(host="localhost", port=4001, client_id=1)
    value.ib = fake
    value._contracts["SPY"] = CONTRACT
    value._allow_calendar_fallback = armed
    return value


@pytest.mark.parametrize("response", [[], RequestError(1, 162, "No data of type EODChart")])
def test_authorized_fallback_restores_request_configuration(response: object) -> None:
    fake = FakeIb(response)
    value = broker(fake)
    assert asyncio.run(value.sessions(around=NOW, count=120)) == calendar_dates(NOW, 120)
    assert value.schedule_evidence["source"] == SOURCE
    assert fake.details_requested == 1
    assert fake.RaiseRequestErrors is False


@pytest.mark.parametrize(
    "response",
    [
        None,
        [object()],
        object(),
        RequestError(1, 162, "Historical data pacing violation"),
        RequestError(1, 200, "contract"),
    ],
)
def test_unrelated_or_malformed_response_never_falls_back(response: object) -> None:
    fake = FakeIb(response)
    value = broker(fake)
    with pytest.raises(IbkrExecutionError):
        asyncio.run(value.sessions(around=NOW, count=120))
    assert value.schedule_evidence == {}
    assert fake.details_requested == 0
    assert fake.RaiseRequestErrors is False


def test_unarmed_cannot_use_fallback_and_cancellation_propagates() -> None:
    fake = FakeIb([])
    with pytest.raises(IbkrExecutionError, match="NOT_AUTHORIZED"):
        asyncio.run(broker(fake, armed=False).sessions(around=NOW, count=120))
    fake = FakeIb(asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(broker(fake).sessions(around=NOW, count=120))
    assert fake.RaiseRequestErrors is False


def test_native_receipt_and_disagreement_do_not_fallback() -> None:
    value = broker(FakeIb(native()))
    asyncio.run(value.sessions(around=NOW, count=120))
    assert value.schedule_evidence["source"] == "IBKR-historical-schedule"
    fake = FakeIb([], HOURS.replace("0930", "0931"))
    value = broker(fake)
    with pytest.raises(IbkrExecutionError, match="DISAGREEMENT"):
        asyncio.run(value.sessions(around=NOW, count=120))
    assert value.schedule_evidence == {}


def test_freshness_expiry_and_management_continuity() -> None:
    value = broker(FakeIb([]))
    asyncio.run(value.sessions(around=NOW, count=120))
    value.schedule_evidence["today_open"] = "True"
    assert value.calendar_gate(NOW + timedelta(seconds=60), for_entry=True) is None
    assert (
        value.calendar_gate(NOW + timedelta(seconds=61), for_entry=True)
        == "calendar_validation_stale"
    )
    assert (
        value.calendar_gate(NOW - timedelta(seconds=1), for_entry=True)
        == "calendar_validation_stale"
    )
    assert value.calendar_gate(NOW + timedelta(seconds=61)) is None
    expiry = datetime(2026, 10, 1, 4, tzinfo=UTC)
    value.schedule_evidence["validated_at_utc"] = expiry.isoformat()
    assert value.calendar_gate(expiry) is None
    assert value.calendar_gate(expiry, for_entry=True) == "calendar_fallback_entry_approval_expired"
    before = expiry - timedelta(seconds=1)
    value.schedule_evidence["validated_at_utc"] = before.isoformat()
    assert value.calendar_gate(before, for_entry=True) is None
    assert value.calendar_gate(expiry, for_entry=True) == "calendar_validation_stale"


def test_research_adapter_has_no_fallback() -> None:
    fake = FakeIb([])
    source = IbkrHistoricalBarSource(host="localhost", port=4001, client_id=2)
    source._IbkrHistoricalBarSource__ib = fake  # type: ignore[attr-defined]
    source._IbkrHistoricalBarSource__contracts["SPY"] = CONTRACT  # type: ignore[attr-defined]
    with pytest.raises(ValueError, match="unavailable or malformed"):
        asyncio.run(source.exchange_sessions(end=NOW, calendar_days=120))
    assert fake.details_requested == 0
    assert fake.RaiseRequestErrors is False


def test_schedule_and_details_timeouts_reset_connection_without_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import insider_alerts.execution.ibkr as module

    monkeypatch.setattr(module, "_SCHEDULE_TIMEOUT_SECONDS", 0.001)

    class HungSchedule(FakeIb):
        async def reqHistoricalScheduleAsync(self, *args: object) -> object:
            await asyncio.sleep(60)
            return None

    fake = HungSchedule([])
    fake.RaiseRequestErrors = True
    value = broker(fake)
    with pytest.raises(IbkrExecutionError, match="TIMEOUT_CONNECTION_RESET"):
        asyncio.run(value.sessions(around=NOW, count=120))
    assert value.schedule_evidence == {}
    assert fake.connected is False
    assert value.ib is None
    assert fake.RaiseRequestErrors is True

    class HungDetails(FakeIb):
        async def reqContractDetailsAsync(self, *args: object) -> object:
            await asyncio.sleep(60)
            return None

    fake = HungDetails([])
    value = broker(fake)
    value.schedule_evidence = {"source": "stale previous success"}
    with pytest.raises(IbkrExecutionError, match="TIMEOUT_CONNECTION_RESET"):
        asyncio.run(value.sessions(around=NOW, count=120))
    assert value.schedule_evidence == {}
    assert fake.RaiseRequestErrors is False
    assert fake.connected is False


def test_closed_day_blocks_new_entry_without_blocking_management() -> None:
    value = broker(FakeIb([]))
    asyncio.run(value.sessions(around=NOW, count=120))
    assert value.calendar_gate(NOW) is None
    assert value.calendar_gate(NOW, for_entry=True) == "calendar_market_closed"


def test_buy_is_rechecked_after_contract_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(UTC)
    value = broker(FakeIb([]))
    value.schedule_evidence = {
        "validated_at_utc": now.isoformat(),
        "source": "IBKR-historical-schedule",
    }
    monkeypatch.setattr(value, "_existing_trade", lambda ref: None)
    monkeypatch.setattr(value, "_account", lambda: "TEST")
    # Do not depend on the wall-clock test day's market-open status.
    checks = []

    def gate(when: datetime, *, for_entry: bool = False) -> str | None:
        checks.append(when)
        return None if len(checks) == 1 else "calendar_validation_stale"

    monkeypatch.setattr(value, "calendar_gate", gate)

    async def contract(symbol: str) -> object:
        return CONTRACT

    monkeypatch.setattr(value, "_contract", contract)
    with pytest.raises(IbkrExecutionError, match="validation_stale"):
        asyncio.run(value.submit_market_on_open("TEST", 1, "TEST-ENTRY"))
    assert len(checks) == 2


def test_valid_native_schedule_is_not_limited_by_fallback_year() -> None:
    around = datetime(2027, 4, 15, 12, tzinfo=UTC)
    rows = []
    for offset in range(-60, 46):
        day = around.date() + timedelta(days=offset)
        if day.weekday() < 5:
            rows.append(
                SimpleNamespace(
                    refDate=day.strftime("%Y%m%d"),
                    startDateTime=day.strftime("%Y%m%d-09:30:00"),
                    endDateTime=day.strftime("%Y%m%d-16:00:00"),
                )
            )
    fake = FakeIb(SimpleNamespace(timeZone="US/Eastern", sessions=rows))
    value = broker(fake)
    result = asyncio.run(value.sessions(around=around, count=120))
    assert len(result) > 60
    assert value.schedule_evidence["source"] == "IBKR-historical-schedule"
    assert fake.details_requested == 0


def test_contract_details_request_error_is_typed() -> None:
    class RejectedDetails(FakeIb):
        async def reqContractDetailsAsync(self, *args: object) -> object:
            raise RequestError(1, 200, "unavailable")

    fake = RejectedDetails([])
    value = broker(fake)
    with pytest.raises(IbkrExecutionError, match="CONTRACT_DETAILS_REQUEST_FAILED"):
        asyncio.run(value.sessions(around=NOW, count=120))
    assert fake.RaiseRequestErrors is False
    assert value.schedule_evidence == {}
