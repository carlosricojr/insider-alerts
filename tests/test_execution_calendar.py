from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from ib_async import RequestError

from insider_alerts.execution.calendar import (
    CALENDAR_SHA256,
    NEW_YORK,
    SOURCE,
    bounds,
    calendar_dates,
    validate_contract_hours,
    validate_future_projection_recovery,
    validate_native_schedule,
)
from insider_alerts.execution.errors import IbkrExecutionError
from insider_alerts.execution.ibkr import IbkrBroker
from insider_alerts.research.ibkr_bar_source import IbkrHistoricalBarSource

NOW = datetime(2026, 9, 12, 19, 32, tzinfo=UTC)
CONTRACT = SimpleNamespace(conId=756733, symbol="SPY", secType="STK", currency="USD")
HOURS = "20260912:CLOSED;20260913:CLOSED;20260914:0930-20260914:1600;20260915:0930-20260915:1600"


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


def test_native_interior_gap_rejected_through_broker() -> None:
    schedule = native()
    schedule.sessions = [row for row in schedule.sessions if row.refDate != "20260915"]
    value = broker(FakeIb(schedule))
    with pytest.raises(IbkrExecutionError, match="COVERAGE_MISMATCH"):
        asyncio.run(value.sessions(around=NOW, count=120))
    assert value.schedule_evidence == {}


def test_year_end_hours_preserve_bounded_management_only() -> None:
    now = datetime(2026, 12, 31, 15, tzinfo=UTC)
    hours = "20261231:0930-20261231:1600;20270101:CLOSED;20270102:CLOSED"
    value = broker(FakeIb([], hours))
    dates = asyncio.run(value.sessions(around=now, count=120))
    assert dates[-1] == date(2026, 12, 31)
    assert value.calendar_gate(now) is None
    assert value.calendar_gate(now, for_entry=True) == "calendar_fallback_entry_approval_expired"
    assert value.schedule_evidence["unvalidated_future_dates"] == "2027-01-01,2027-01-02"
    assert value.schedule_evidence["hours_last_date"] == "2026-12-31"
    bad = broker(FakeIb([], hours.replace("20261231:1600", "20261231:1300")))
    with pytest.raises(IbkrExecutionError, match="DISAGREEMENT"):
        asyncio.run(bad.sessions(around=now, count=120))


def schedule_for_dates(days: list[date]) -> SimpleNamespace:
    rows = []
    for day in days:
        if day.year == 2026:
            session = bounds(day)
            assert session is not None
            opening, closing = session
        else:
            opening = datetime.combine(day, datetime.min.time()).replace(hour=9, minute=30)
            closing = opening.replace(hour=16, minute=0)
        rows.append(
            SimpleNamespace(
                refDate=day.strftime("%Y%m%d"),
                startDateTime=opening.strftime("%Y%m%d-%H:%M:%S"),
                endDateTime=closing.strftime("%Y%m%d-%H:%M:%S"),
            )
        )
    return SimpleNamespace(timeZone="US/Eastern", sessions=rows)


def open_dates(start: date, end: date) -> list[date]:
    return [
        day
        for offset in range((end - start).days + 1)
        if (day := start + timedelta(days=offset)).weekday() < 5
        and (day.year != 2026 or bounds(day) is not None)
    ]


@pytest.mark.parametrize(
    "around",
    [
        datetime(2026, 9, 13, 21, 49, tzinfo=UTC),
        datetime(2026, 9, 14, 0, 30, tzinfo=UTC),  # Still September 13 in New York.
        datetime(2026, 2, 15, 12, tzinfo=UTC),  # 2025/2026 window.
        datetime(2026, 12, 1, 12, tzinfo=UTC),  # 2026/2027 window.
    ],
)
def test_native_validates_then_bounds_response(around: datetime) -> None:
    from insider_alerts.execution.calendar import NEW_YORK

    end = around.astimezone(NEW_YORK).date() + timedelta(days=45)
    start = end - timedelta(days=119)
    expected = open_dates(start, end)
    fake = FakeIb(
        schedule_for_dates(open_dates(start - timedelta(days=60), end + timedelta(days=20)))
    )
    value = broker(fake)
    assert asyncio.run(value.sessions(around=around, count=120)) == expected
    assert value.schedule_evidence["first_session"] == str(expected[0])
    assert value.schedule_evidence["last_session"] == str(expected[-1])
    assert value.schedule_evidence["source"] == "IBKR-historical-schedule"
    assert fake.details_requested == 0
    assert fake.RaiseRequestErrors is False


@pytest.mark.parametrize("missing", ["2026-07-01", "2026-09-15", "2026-10-28"])
def test_native_extras_do_not_hide_missing_requested_date(missing: str) -> None:
    days = open_dates(date(2026, 5, 8), date(2026, 11, 10))
    days.remove(date.fromisoformat(missing))
    fake = FakeIb(schedule_for_dates(days))
    value = broker(fake)
    value.schedule_evidence = {"source": "stale"}
    with pytest.raises(IbkrExecutionError, match="COVERAGE_MISMATCH"):
        asyncio.run(value.sessions(around=NOW + timedelta(days=1), count=120))
    assert value.schedule_evidence == {}
    assert fake.details_requested == 0
    assert fake.RaiseRequestErrors is False


@pytest.mark.parametrize("mode", ["duplicate", "hours", "holiday", "weekend", "malformed", "zone"])
def test_native_invalid_out_of_window_rows_still_fail_closed(mode: str) -> None:
    response = schedule_for_dates(open_dates(date(2026, 5, 8), date(2026, 11, 10)))
    if mode == "duplicate":
        response.sessions.append(response.sessions[0])
    elif mode == "hours":
        response.sessions[0].endDateTime = "20260508-13:00:00"
    elif mode in {"holiday", "weekend"}:
        day = "20260525" if mode == "holiday" else "20260509"
        response.sessions.append(
            SimpleNamespace(
                refDate=day, startDateTime=f"{day}-09:30:00", endDateTime=f"{day}-16:00:00"
            )
        )
    elif mode == "malformed":
        response.sessions.append(object())
    else:
        response.timeZone = "UTC"
    fake = FakeIb(response)
    value = broker(fake)
    with pytest.raises(IbkrExecutionError):
        asyncio.run(value.sessions(around=NOW, count=120))
    assert value.schedule_evidence == {}
    assert fake.details_requested == 0
    assert fake.RaiseRequestErrors is False


@pytest.mark.parametrize(
    "past,future,passes", [(19, 10, False), (20, 9, False), (20, 10, True), (0, 0, False)]
)
def test_native_horizon_minimums_use_only_bounded_dates(
    past: int, future: int, passes: bool
) -> None:
    # Outside the published calendar year, isolate the unchanged minimum-count gates.
    around = datetime(2027, 4, 15, 12, tzinfo=UTC)
    end = around.date() + timedelta(days=45)
    start = end - timedelta(days=119)
    before = open_dates(start, around.date() - timedelta(days=1))[:past]
    after = open_dates(around.date() + timedelta(days=1), end)[:future]
    extras = open_dates(start - timedelta(days=60), start - timedelta(days=1))
    extras += open_dates(end + timedelta(days=1), end + timedelta(days=60))
    fake = FakeIb(schedule_for_dates(extras + before + after))
    value = broker(fake)
    if passes:
        assert asyncio.run(value.sessions(around=around, count=120)) == sorted(before + after)
    else:
        with pytest.raises(IbkrExecutionError, match="COVERAGE_MISMATCH"):
            asyncio.run(value.sessions(around=around, count=120))
        assert value.schedule_evidence == {}
    assert fake.details_requested == 0
    assert fake.RaiseRequestErrors is False


@pytest.mark.parametrize("count", [True, False, 0, 59, 366, 120.5, "120"])
def test_native_invalid_count_rejected_before_broker_access(count: object) -> None:
    value = IbkrBroker(host="localhost", port=4001, client_id=1)
    value.schedule_evidence = {"source": "stale"}
    with pytest.raises(IbkrExecutionError, match="INVALID_REQUEST"):
        asyncio.run(value.sessions(around=NOW, count=count))  # type: ignore[arg-type]
    assert value.schedule_evidence == {}


@pytest.mark.parametrize("around", [NOW.replace(tzinfo=None), None, NOW.date(), NOW.isoformat()])
def test_native_invalid_clock_rejected_before_broker_access(around: object) -> None:
    value = IbkrBroker(host="localhost", port=4001, client_id=1)
    value.schedule_evidence = {"source": "stale"}
    with pytest.raises(IbkrExecutionError, match="INVALID_REQUEST"):
        asyncio.run(value.sessions(around=around, count=120))  # type: ignore[arg-type]
    assert value.schedule_evidence == {}


class EndpointSensitiveIb(FakeIb):
    """Model the observed omission when a session opens after the endpoint."""

    def __init__(self, response: SimpleNamespace) -> None:
        super().__init__(response)
        self.endpoints: list[datetime] = []

    async def reqHistoricalScheduleAsync(self, *args: object) -> object:
        assert args[0] is CONTRACT
        assert args[1] == 120
        assert args[3] is True
        endpoint = args[2]
        assert isinstance(endpoint, datetime)
        self.endpoints.append(endpoint)
        response = await super().reqHistoricalScheduleAsync(*args)
        assert isinstance(response, SimpleNamespace)
        return SimpleNamespace(
            timeZone=response.timeZone,
            sessions=[
                row
                for row in response.sessions
                if datetime.strptime(row.startDateTime, "%Y%m%d-%H:%M:%S").replace(tzinfo=NEW_YORK)
                <= endpoint
            ],
        )


@pytest.mark.parametrize(
    "around_text,endpoint_text",
    [
        ("2026-09-14T00:00:00-04:00", "2026-10-30T03:59:59+00:00"),
        ("2026-09-14T08:00:00-04:00", "2026-10-30T03:59:59+00:00"),
        ("2026-09-14T09:29:59-04:00", "2026-10-30T03:59:59+00:00"),
        ("2026-09-14T09:30:00-04:00", "2026-10-30T03:59:59+00:00"),
        ("2026-09-14T10:00:00-04:00", "2026-10-30T03:59:59+00:00"),
        ("2026-09-15T00:30:00+00:00", "2026-10-30T03:59:59+00:00"),
        ("2026-09-14T23:59:59-04:00", "2026-10-30T03:59:59+00:00"),
        ("2026-09-15T00:00:00-04:00", "2026-10-31T03:59:59+00:00"),
        ("2026-09-18T00:00:00-04:00", "2026-11-03T04:59:59+00:00"),
        ("2026-01-25T00:00:00-05:00", "2026-03-12T03:59:59+00:00"),
        ("2026-10-13T00:00:00-04:00", "2026-11-28T04:59:59+00:00"),
        ("2026-11-09T00:00:00-05:00", "2026-12-25T04:59:59+00:00"),
        ("2026-09-17T00:00:00-04:00", "2026-11-02T04:59:59+00:00"),
        ("2026-10-12T00:00:00-04:00", "2026-11-27T04:59:59+00:00"),
        ("2026-11-17T00:00:00-05:00", "2027-01-02T04:59:59+00:00"),
        ("2028-01-15T00:00:00-05:00", "2028-03-01T04:59:59+00:00"),  # Ends Feb 29.
        ("2028-02-29T00:00:00-05:00", "2028-04-15T03:59:59+00:00"),  # Starts Feb 29.
        ("2027-01-15T00:00:00-05:00", "2027-03-02T04:59:59+00:00"),  # Non-leap.
        ("2100-01-15T00:00:00-05:00", "2100-03-02T04:59:59+00:00"),  # Century non-leap.
    ],
)
def test_native_endpoint_covers_entire_target_ny_date(around_text: str, endpoint_text: str) -> None:
    around = datetime.fromisoformat(around_text)
    end = around.astimezone(NEW_YORK).date() + timedelta(days=45)
    start = end - timedelta(days=119)
    expected = open_dates(start, end)
    fake = EndpointSensitiveIb(
        schedule_for_dates(open_dates(start - timedelta(days=60), end + timedelta(days=10)))
    )
    value = broker(fake)
    assert asyncio.run(value.sessions(around=around, count=120)) == expected
    assert fake.endpoints == [datetime.fromisoformat(endpoint_text)]
    assert fake.endpoints[0].tzinfo is UTC
    assert value.schedule_evidence["last_session"] == str(expected[-1])
    assert value.schedule_evidence["validated_at_utc"] == around.astimezone(UTC).isoformat()
    assert fake.RaiseRequestErrors is False
    assert fake.details_requested == 0


def test_end_of_day_request_does_not_excuse_missing_final_session() -> None:
    around = datetime(2026, 9, 14, 0, tzinfo=NEW_YORK)
    days = open_dates(date(2026, 5, 8), date(2026, 11, 10))
    days.remove(date(2026, 10, 29))
    fake = EndpointSensitiveIb(schedule_for_dates(days))
    value = broker(fake)
    value.schedule_evidence = {"source": "stale"}
    with pytest.raises(IbkrExecutionError, match="COVERAGE_MISMATCH"):
        asyncio.run(value.sessions(around=around, count=120))
    assert value.schedule_evidence == {}
    assert fake.details_requested == 0
    assert fake.RaiseRequestErrors is False


PROJECTION_NOW = datetime(2026, 10, 14, 18, 45, tzinfo=UTC)
PROJECTION_HOURS = (
    "20261014:0930-20261014:1600;20261015:0930-20261015:1600;"
    "20261016:0930-20261016:1600;20261017:CLOSED;20261018:CLOSED;"
    "20261019:0930-20261019:1600"
)


def projected_schedule(
    around: datetime = PROJECTION_NOW,
    projected_days: tuple[date, ...] = (date(2026, 11, 26), date(2026, 11, 27)),
) -> SimpleNamespace:
    # Model the observed Oct 14 response, not a claim of a historical snapshot.
    end = around.astimezone(NEW_YORK).date() + timedelta(days=45)
    response = schedule_for_dates(open_dates(end - timedelta(days=119), end))
    for day in projected_days:
        day_text = day.strftime("%Y%m%d")
        row = next((row for row in response.sessions if row.refDate == day_text), None)
        if row is None:
            response.sessions.append(SimpleNamespace(
                refDate=day_text, startDateTime=f"{day_text}-09:30:00",
                endDateTime=f"{day_text}-16:00:00",
            ))
        else:
            row.endDateTime = f"{day_text}-16:00:00"
    return response


def test_future_projection_uses_verified_management_fallback_only() -> None:
    response = projected_schedule()
    with pytest.raises(IbkrExecutionError, match="DISAGREEMENT"):
        validate_native_schedule(response)
    fake = FakeIb(response, PROJECTION_HOURS)
    value = broker(fake)
    expected = calendar_dates(PROJECTION_NOW, 120)
    assert asyncio.run(value.sessions(around=PROJECTION_NOW, count=120)) == expected
    assert date(2026, 11, 26) not in expected
    assert fake.details_requested == 1
    receipt = value.schedule_evidence
    assert receipt["source"] == SOURCE
    assert receipt["fallback_reason"] == "IBKR_NATIVE_FUTURE_PROJECTION_REJECTED"
    assert receipt["native_rejected_future_dates"] == "2026-11-26,2026-11-27"
    assert "HOLIDAY_PROJECTED_OPEN" in receipt["native_contradiction_kinds"]
    assert "EARLY_CLOSE_PROJECTED_REGULAR" in receipt["native_contradiction_kinds"]
    assert len(receipt["native_schedule_sha256"]) == 64
    assert int(receipt["native_past_sessions_validated"]) >= 20
    assert value.calendar_gate(PROJECTION_NOW) is None
    assert value.calendar_gate(PROJECTION_NOW, for_entry=True) == (
        "calendar_fallback_entry_approval_expired"
    )
    assert fake.RaiseRequestErrors is False


@pytest.mark.parametrize("kind", ["holiday", "early_close"])
def test_each_allowlisted_future_projection_recovers_independently(kind: str) -> None:
    response = projected_schedule()
    if kind == "holiday":
        next(row for row in response.sessions if row.refDate == "20261127").endDateTime = (
            "20261127-13:00:00"
        )
    else:
        response.sessions = [row for row in response.sessions if row.refDate != "20261126"]
    value = broker(FakeIb(response, PROJECTION_HOURS))
    assert asyncio.run(value.sessions(around=PROJECTION_NOW, count=120)) == calendar_dates(
        PROJECTION_NOW, 120
    )
    assert value.schedule_evidence["native_rejected_future_dates"] == (
        "2026-11-26" if kind == "holiday" else "2026-11-27"
    )


@pytest.mark.parametrize("armed,around", [
    (False, PROJECTION_NOW),
    (True, datetime(2026, 9, 30, 18, tzinfo=UTC)),
    (True, datetime(2027, 1, 1, 18, tzinfo=UTC)),
    (True, datetime(2026, 11, 26, 18, tzinfo=UTC)),
    (True, datetime(2026, 11, 27, 18, tzinfo=UTC)),
])
def test_projection_recovery_authorization_and_date_boundaries(
    armed: bool, around: datetime,
) -> None:
    response = projected_schedule(around)
    if armed:
        error = "NOT_AUTHORIZED" if around.year != 2026 or around.month < 10 else "NOT_ELIGIBLE"
        with pytest.raises(IbkrExecutionError, match=error):
            validate_future_projection_recovery(response, around, 120)
    fake = FakeIb(response, PROJECTION_HOURS)
    value = broker(fake, armed=armed)
    with pytest.raises(IbkrExecutionError):
        asyncio.run(value.sessions(around=around, count=120))
    assert fake.details_requested == 0
    assert value.schedule_evidence == {}
    assert fake.RaiseRequestErrors is False


def test_december_projection_caps_known_year_without_inventing_2027_holidays() -> None:
    around = datetime(2026, 12, 1, 18, tzinfo=UTC)
    response = projected_schedule(around, (date(2026, 12, 24), date(2026, 12, 25)))
    assert any(row.refDate.startswith("2027") for row in response.sessions)
    hours = "20261201:0930-20261201:1600;20261202:0930-20261202:1600"
    value = broker(FakeIb(response, hours))
    days = asyncio.run(value.sessions(around=around, count=120))
    assert days == calendar_dates(around, 120)
    assert days[-1] == date(2026, 12, 31)
    assert date(2026, 12, 25) not in days
    assert value.schedule_evidence["native_rejected_future_dates"] == "2026-12-24,2026-12-25"
    assert value.calendar_gate(around, for_entry=True) == "calendar_fallback_entry_approval_expired"


@pytest.mark.parametrize("mode", [
    "past_holiday", "today", "regular_early", "holiday_half_day", "missing_past",
    "missing_today", "missing_future", "duplicate", "zone", "malformed", "weekend",
    "outside_window_bad_hours",
])
def test_projection_recovery_cannot_mask_other_schedule_failures(mode: str) -> None:
    response = projected_schedule()
    if mode in {"past_holiday", "weekend", "outside_window_bad_hours"}:
        day = {"past_holiday": "20260907", "weekend": "20261017",
               "outside_window_bad_hours": "20260730"}[mode]
        closing = "13:00:00" if mode == "outside_window_bad_hours" else "16:00:00"
        response.sessions.append(SimpleNamespace(
            refDate=day, startDateTime=f"{day}-09:30:00", endDateTime=f"{day}-{closing}"
        ))
    elif mode in {"today", "regular_early", "holiday_half_day"}:
        day = {"today": "20261014", "regular_early": "20261125",
               "holiday_half_day": "20261126"}[mode]
        next(row for row in response.sessions if row.refDate == day).endDateTime = (
            f"{day}-13:00:00"
        )
    elif mode.startswith("missing_"):
        day = {"missing_past": "20261013", "missing_today": "20261014",
               "missing_future": "20261125"}[mode]
        response.sessions = [row for row in response.sessions if row.refDate != day]
    elif mode == "duplicate":
        response.sessions.append(response.sessions[0])
    elif mode == "zone":
        response.timeZone = "UTC"
    else:
        response.sessions.append(object())
    fake = FakeIb(response, PROJECTION_HOURS)
    value = broker(fake)
    with pytest.raises(IbkrExecutionError):
        asyncio.run(value.sessions(around=PROJECTION_NOW, count=120))
    assert value.schedule_evidence == {}
    assert fake.details_requested == 0
    assert fake.RaiseRequestErrors is False


@pytest.mark.parametrize("hours", [
    PROJECTION_HOURS.replace("0930", "0931"),
    "20261014:0930-20261014:1600",  # next session absent
    "20261013:0930-20261013:1600",  # stale coverage
])
def test_projection_recovery_still_requires_fresh_exact_contract_hours(hours: str) -> None:
    fake = FakeIb(projected_schedule(), hours)
    value = broker(fake)
    with pytest.raises(IbkrExecutionError):
        asyncio.run(value.sessions(around=PROJECTION_NOW, count=120))
    assert fake.details_requested == 1
    assert value.schedule_evidence == {}
    assert fake.RaiseRequestErrors is False


def test_projection_recovery_timeout_clears_evidence_and_resets_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import insider_alerts.execution.ibkr as module

    class HungDetails(FakeIb):
        async def reqContractDetailsAsync(self, *args: object) -> object:
            await asyncio.sleep(60)
            return None

    monkeypatch.setattr(module, "_SCHEDULE_TIMEOUT_SECONDS", 0.001)
    fake = HungDetails(projected_schedule())
    value = broker(fake)
    value.schedule_evidence = {"source": "previous success"}
    with pytest.raises(IbkrExecutionError, match="TIMEOUT_CONNECTION_RESET"):
        asyncio.run(value.sessions(around=PROJECTION_NOW, count=120))
    assert value.schedule_evidence == {}
    assert value.ib is None
    assert fake.connected is False
    assert fake.RaiseRequestErrors is False


def test_projection_recovery_requires_twenty_past_sessions_and_actual_disagreement() -> None:
    with pytest.raises(IbkrExecutionError, match="COVERAGE_MISMATCH"):
        validate_future_projection_recovery(projected_schedule(), PROJECTION_NOW, 60)
    valid = schedule_for_dates(calendar_dates(PROJECTION_NOW, 120))
    with pytest.raises(IbkrExecutionError, match="NOT_ELIGIBLE"):
        validate_future_projection_recovery(valid, PROJECTION_NOW, 120)


def test_projection_recovery_full_cycle_keeps_protection_and_submits_overdue_exit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from insider_alerts.execution.canary import AccountSnapshot, BrokerOrder, broker_token
    from tests.test_execution_canary_adversarial import _runner, _signal

    runner, live_broker = _runner(tmp_path, monkeypatch, PROJECTION_NOW)
    token = broker_token("crash-test-packet")
    runner.store.insert_candidate(
        _signal(PROJECTION_NOW), session=date(2026, 10, 1), rank="test",
        is_eligible=True, reason="eligible", prior_close=10, median_dollar_volume=1_000_000,
        quantity=20, now=PROJECTION_NOW,
    )
    runner.store.update(
        "crash-test-packet", live_state="open", live_quantity=20, live_entry_price=10,
        live_entry_commission=0.35, live_exit_session="2026-10-13",
        oca_group=f"IA-E07-{token}-EXIT", target_order_id=201, stop_order_id=202,
    )
    live_broker.snapshot = AccountSnapshot("ACCOUNT", 500, 300, 300, {"TEST": 20}, 2)
    live_broker.order_values = [
        BrokerOrder(201, f"IA-E07-{token}-TARGET", "TEST", "target", "Submitted", 0, 20, 0),
        BrokerOrder(202, f"IA-E07-{token}-STOP", "TEST", "stop", "Submitted", 0, 20, 0),
    ]
    adapter = broker(FakeIb(projected_schedule(), PROJECTION_HOURS))

    async def sessions(*, around: datetime, count: int = 90) -> list[date]:
        result = await adapter.sessions(around=around, count=count)
        live_broker.schedule_evidence = adapter.schedule_evidence  # type: ignore[attr-defined]
        return result

    async def forbidden_entries(*args: object, **kwargs: object) -> int:
        pytest.fail("projection recovery must not attempt new buys")

    monkeypatch.setattr(live_broker, "sessions", sessions)
    monkeypatch.setattr(live_broker, "calendar_gate", adapter.calendar_gate, raising=False)
    monkeypatch.setattr(runner, "_submit_due_entries", forbidden_entries)
    result = asyncio.run(runner.cycle(PROJECTION_NOW))
    assert result.live_gate == "calendar_fallback_entry_approval_expired;time_exits=1"
    assert live_broker.market_exits == [("TEST", 20, f"IA-E07-{token}-EXIT")]
    assert live_broker.cancelled_orders == []
    assert live_broker.entry_calls == 0
    row = runner.store.rows()[0]
    assert row["live_state"] == "closing"
    assert row["timed_exit_order_id"] == 204
    assert row["live_exit_session"] == "2026-10-13"
