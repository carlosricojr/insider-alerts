"""Bounded operational calendar; never an input to the frozen research trial.

NYSE published 2026 cash-equity holidays and early closes, retrieved 2026-09-12:
https://www.nyse.com/trade/hours-calendars
No inferred years, network downloads, or dependency on broker schedule availability.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from insider_alerts.execution.errors import IbkrExecutionError

NEW_YORK = ZoneInfo("America/New_York")
SOURCE = "NYSE-published-2026-v1"
SOURCE_PDF_SHA256 = "70f5577eb43e60a9dbbecaae3cec23d0f02028c05c7f175013bb3e97816d394f"
MANAGEMENT_RECOVERY_START = date(2026, 10, 1)
ENTRY_RENEWAL_START = date(2026, 10, 15)
FALLBACK_ENTRY_EXPIRY = date(2027, 1, 1)
ENTRY_HORIZON_LIMIT = date(2026, 12, 31)


def entry_approval(day: date) -> str | None:
    """Explicit half-open approvals; never retroactively fill the October gap."""
    if day < MANAGEMENT_RECOVERY_START:
        return "original-2026-09"
    if ENTRY_RENEWAL_START <= day < FALLBACK_ENTRY_EXPIRY:
        return "renewal-2026-10-15"
    return None


HOLIDAYS = frozenset(
    date.fromisoformat(value)
    for value in (
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
    )
)
EARLY_CLOSES = frozenset((date(2026, 11, 27), date(2026, 12, 24)))
CALENDAR_SHA256 = hashlib.sha256(
    json.dumps(
        {
            "source": SOURCE,
            "source_pdf_sha256": SOURCE_PDF_SHA256,
            "holidays": sorted(map(str, HOLIDAYS)),
            "early_closes": sorted(map(str, EARLY_CLOSES)),
            "open": "09:30",
            "close": "16:00",
            "early_close": "13:00",
            "zone": NEW_YORK.key,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
).hexdigest()


def bounds(day: date) -> tuple[datetime, datetime] | None:
    if day.year != 2026:
        raise IbkrExecutionError("CALENDAR_UNSUPPORTED_YEAR")
    if day.weekday() >= 5 or day in HOLIDAYS:
        return None
    return (
        datetime.combine(day, time(9, 30), NEW_YORK),
        datetime.combine(day, time(13) if day in EARLY_CLOSES else time(16), NEW_YORK),
    )


def calendar_dates(around: datetime, count: int) -> list[date]:
    if around.tzinfo is None or not 60 <= count <= 365:
        raise IbkrExecutionError("CALENDAR_INVALID_REQUEST")
    end = around.astimezone(NEW_YORK).date() + timedelta(days=45)
    start = end - timedelta(days=count - 1)
    today = around.astimezone(NEW_YORK).date()
    if today.year != 2026 or start.year != 2026:
        raise IbkrExecutionError("CALENDAR_UNSUPPORTED_RANGE")
    # Bound the published horizon independently of entry authorization. Entry
    # gates require the entire holding period; management may use a shorter tail.
    end = min(end, date(2026, 12, 31))
    count = (end - start).days + 1
    return [day for offset in range(count) if bounds(day := start + timedelta(days=offset))]


def validate_contract_hours(details: Any, contract: Any, around: datetime) -> dict[str, str]:
    """Accept a fresh request's exact SPY RTH agreement, including closed dates.

    Missing dates are unknown, not closed. Broker identity, timezone, duplicate dates,
    multi-interval sessions, and today's/next-session coverage are all fail-closed.
    """
    if not isinstance(details, list) or len(details) != 1:
        raise IbkrExecutionError("CALENDAR_CONTRACT_DETAILS_UNAVAILABLE")
    item = details[0]
    actual = getattr(item, "contract", None)
    if (
        not isinstance(getattr(contract, "conId", None), int)
        or isinstance(contract.conId, bool)
        or contract.conId <= 0
        or getattr(actual, "conId", None) != contract.conId
        or getattr(actual, "symbol", None) != "SPY"
        or getattr(actual, "secType", None) != "STK"
        or getattr(actual, "currency", None) != "USD"
        or getattr(item, "timeZoneId", None) not in {"US/Eastern", "America/New_York"}
    ):
        raise IbkrExecutionError("CALENDAR_CONTRACT_IDENTITY_OR_ZONE_MISMATCH")
    text = getattr(item, "liquidHours", None)
    if not isinstance(text, str) or not text or len(text) > 4096:
        raise IbkrExecutionError("CALENDAR_LIQUID_HOURS_UNAVAILABLE")
    observed: dict[date, tuple[datetime, datetime] | None] = {}
    unvalidated: list[str] = []
    all_days: set[date] = set()
    today = around.astimezone(NEW_YORK).date()
    coverage_end = date(2026, 12, 31)
    if today.year != 2026:
        raise IbkrExecutionError("CALENDAR_UNSUPPORTED_YEAR")
    try:
        for entry in text.split(";"):
            if re.fullmatch(r"\d{8}:(?:CLOSED|\d{4}-\d{8}:\d{4})", entry) is None:
                raise ValueError("invalid hours syntax")
            day_text, hours = entry.split(":", 1)
            day = datetime.strptime(day_text, "%Y%m%d").date()
            if abs((day - around.astimezone(NEW_YORK).date()).days) > 14:
                raise ValueError("hours outside bounded observation window")
            if day in all_days:
                raise ValueError("duplicate date")
            all_days.add(day)
            if hours == "CLOSED":
                actual_bounds = None
            else:
                start_text, end_text = entry.split("-", 1)
                opens = datetime.strptime(start_text, "%Y%m%d:%H%M").replace(tzinfo=NEW_YORK)
                closes = datetime.strptime(end_text, "%Y%m%d:%H%M").replace(tzinfo=NEW_YORK)
                actual_bounds = (opens, closes)
            if day > coverage_end and MANAGEMENT_RECOVERY_START <= today <= coverage_end:
                # Preserve current-year management, but do not claim to validate
                # next-year dates. The independent holding-horizon gate blocks buys.
                unvalidated.append(str(day))
                continue
            if actual_bounds != bounds(day):
                raise IbkrExecutionError(f"CALENDAR_BROKER_HOURS_DISAGREEMENT:{day}")
            observed[day] = actual_bounds
    except (ValueError, TypeError) as exc:
        raise IbkrExecutionError("CALENDAR_MALFORMED_LIQUID_HOURS") from exc
    next_open = today
    while next_open <= coverage_end and bounds(next_open) is None:
        next_open += timedelta(days=1)
    # On an open day require the following open date too, not just today's stale hours.
    if next_open == today:
        next_open += timedelta(days=1)
        while next_open <= coverage_end and bounds(next_open) is None:
            next_open += timedelta(days=1)
    next_open = min(next_open, coverage_end)
    required = {today + timedelta(days=i) for i in range((next_open - today).days + 1)}
    if not required.issubset(observed):
        raise IbkrExecutionError("CALENDAR_BROKER_HOURS_STALE_OR_INCOMPLETE")
    return {
        "hours_sha256": hashlib.sha256(text.encode()).hexdigest(),
        "hours_first_date": str(min(observed)),
        "hours_last_date": str(max(observed)),
        "validated_at_utc": around.astimezone(UTC).isoformat(),
        "unvalidated_future_dates": ",".join(unvalidated),
    }


def parse_native_schedule(schedule: Any) -> list[tuple[date, datetime, datetime]]:
    """Validate every row's structure, including extras outside the request window."""
    if getattr(schedule, "timeZone", None) not in {"US/Eastern", "America/New_York"}:
        raise IbkrExecutionError("CALENDAR_SCHEDULE_ZONE_INVALID")
    rows = getattr(schedule, "sessions", None)
    if not isinstance(rows, list) or not rows:
        raise IbkrExecutionError("CALENDAR_SCHEDULE_MALFORMED")
    found: set[date] = set()
    parsed: list[tuple[date, datetime, datetime]] = []
    try:
        for row in rows:
            day = datetime.strptime(str(row.refDate), "%Y%m%d").date()
            opens = datetime.strptime(str(row.startDateTime), "%Y%m%d-%H:%M:%S").replace(
                tzinfo=NEW_YORK
            )
            closes = datetime.strptime(str(row.endDateTime), "%Y%m%d-%H:%M:%S").replace(
                tzinfo=NEW_YORK
            )
            if (
                day.weekday() >= 5
                or opens.date() != day
                or closes.date() != day
                or opens.time() != time(9, 30)
                or closes.time() not in {time(13), time(16)}
                or day in found
            ):
                raise IbkrExecutionError(f"CALENDAR_SCHEDULE_DISAGREEMENT:{day}")
            found.add(day)
            parsed.append((day, opens, closes))
    except (AttributeError, ValueError, TypeError) as exc:
        raise IbkrExecutionError("CALENDAR_SCHEDULE_MALFORMED") from exc
    return parsed


def native_contradictions(
    rows: list[tuple[date, datetime, datetime]],
) -> dict[date, str]:
    """Classify known-calendar disagreements; do not treat them as valid sessions."""
    disagreements: dict[date, str] = {}
    for day, opens, closes in rows:
        if day.year != 2026 or bounds(day) == (opens, closes):
            continue
        kind = "OTHER"
        if day in HOLIDAYS and closes.time() == time(16):
            kind = "HOLIDAY_PROJECTED_OPEN"
        elif day in EARLY_CLOSES and closes.time() == time(16):
            kind = "EARLY_CLOSE_PROJECTED_REGULAR"
        disagreements[day] = kind
    return disagreements


def validate_native_schedule(schedule: Any, expected: list[date] | None = None) -> list[date]:
    rows = parse_native_schedule(schedule)
    disagreements = native_contradictions(rows)
    if disagreements:
        raise IbkrExecutionError(f"CALENDAR_SCHEDULE_DISAGREEMENT:{next(iter(disagreements))}")
    found = sorted(day for day, _, _ in rows)
    if expected is not None and found != expected:
        raise IbkrExecutionError("CALENDAR_SCHEDULE_COVERAGE_MISMATCH")
    return found


def validate_future_projection_recovery(
    schedule: Any, around: datetime, count: int,
) -> dict[str, str]:
    """Reject a narrowly classified future projection, not the published calendar.

    The caller must separately require live authorization and fresh contract hours.
    All published requested sessions must still exist, and all non-projected rows
    retain native validation. Returned evidence describes rejected data, not a
    corrected historical observation. Only the pinned 2026 calendar is used.
    """
    today = around.astimezone(NEW_YORK).date()
    # Management authority survives entry expiry. Entry approval is separately
    # evaluated by the broker gate, including the explicit October renewal.
    if not MANAGEMENT_RECOVERY_START <= today <= date(2026, 12, 31):
        raise IbkrExecutionError("CALENDAR_PROJECTION_RECOVERY_NOT_AUTHORIZED")
    expected = calendar_dates(around, count)
    rows = parse_native_schedule(schedule)
    disagreements = native_contradictions(rows)
    allowed = {"HOLIDAY_PROJECTED_OPEN", "EARLY_CLOSE_PROJECTED_REGULAR"}
    if not disagreements or any(
        day <= today or kind not in allowed for day, kind in disagreements.items()
    ):
        raise IbkrExecutionError("CALENDAR_PROJECTION_RECOVERY_NOT_ELIGIBLE")
    found = {day for day, _, _ in rows}
    past_count = sum(day < today for day in expected)
    if not set(expected).issubset(found) or past_count < 20:
        raise IbkrExecutionError("CALENDAR_SCHEDULE_COVERAGE_MISMATCH")
    payload = {
        "zone": schedule.timeZone,
        "sessions": [
            (str(day), opens.isoformat(), closes.isoformat()) for day, opens, closes in rows
        ],
    }
    return {
        "native_rejected_future_dates": ",".join(str(day) for day in sorted(disagreements)),
        "native_contradiction_kinds": json.dumps(
            {str(day): kind for day, kind in sorted(disagreements.items())}, sort_keys=True
        ),
        "native_schedule_sha256": hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "native_past_sessions_validated": str(past_count),
    }
