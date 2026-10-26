"""Validate acquired inputs and immutable references without computing outcomes."""

from __future__ import annotations

import math
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

import rfc8785

from insider_alerts.research import calendar_v2 as calendar
from insider_alerts.research.commissioning_journal import digest
from insider_alerts.research.commissioning_source import EXCHANGES, SYMBOL_MAP
from insider_alerts.research.commissioning_window import parse


def bar_date(value: str) -> date:
    return date.fromisoformat(value)  # Supports IB YYYYMMDD and SDK YYYY-MM-DD.


def qualified_identity(attempt: dict[str, Any], record: dict[str, Any]) -> int | None:
    """Pin the first valid qualification even when its following history call fails."""
    source, intent = record["payload"], attempt["payload"]
    try:
        identity = source["identity"]
        conid = identity["conid"]
        clocks = [
            parse(value)
            for value in (
                attempt["recorded_at"],
                source["started_at"],
                identity["observed_at"],
                source["finished_at"],
                record["recorded_at"],
            )
        ]
        if (
            record["entity"] != attempt["sha256"]
            or source["symbol"] != intent["symbol"]
            or source["clock_integrity"] != "valid"
            or clocks != sorted(clocks)
            or type(conid) is not int
            or conid <= 0
            or identity["security_type"] != "STK"
            or identity["currency"] != "USD"
            or identity["primary_exchange"] not in EXCHANGES
            or (intent["expected_conid"] is not None and conid != intent["expected_conid"])
            or (
                intent["expected_conid"] is None
                and identity["symbol"] != SYMBOL_MAP.get(intent["symbol"], intent["symbol"])
            )
        ):
            return None
        return conid
    except (KeyError, TypeError, ValueError):
        return None


def inspect_receipt(
    attempt: dict[str, Any], record: dict[str, Any], version: calendar.Version
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "usable": False,
        "issues": [],
        "bars": {},
        "date_issues": {},
        "unassigned_row_issues": [],
        "completed_schedule": {},
        "projected_schedule": [],
        "conid": None,
        "response_at": None,
        "receipt_sha256": record["sha256"],
    }
    issues: list[str] = result["issues"]
    source = record["payload"]
    intent = attempt["payload"]
    try:
        if record["entity"] != attempt["sha256"]:
            raise ValueError("wrong_attempt")
        if source["result"] not in {"received", "empty_response"}:
            issues.append(f"source:{source['result']}")
        if source["clock_integrity"] != "valid" or source["cleanup_errors"]:
            issues.append("source_clock_or_cleanup")
        identity = source["identity"]
        result["conid"] = qualified_identity(attempt, record)
        if result["conid"] is None:
            issues.append("contract_scope_or_pin")
        request = source["request"]
        if (
            source["symbol"] != intent["symbol"]
            or request["start_date"] != intent["start_date"]
            or request["end_at"] != intent["end_at"]
            or request["what_to_show"] != ("SCHEDULE" if intent["schedule"] else "TRADES")
            or request["bar_size"] != "1 day"
            or request["use_rth"] is not True
            or request["format_date"] != (1 if intent["schedule"] else 2)
        ):
            issues.append("request_differs_from_committed_intent")
        clocks = [
            parse(value)
            for value in (
                attempt["recorded_at"],
                source["started_at"],
                identity["observed_at"],
                source["client_dispatch_at"],
                source["response_at"],
                source["finished_at"],
                record["recorded_at"],
            )
        ]
        if clocks != sorted(clocks) or parse(request["end_at"]) > clocks[3]:
            issues.append("receipt_clock_order")
        local_dispatch = clocks[3].astimezone(calendar.NY)
        if 8 <= local_dispatch.hour < 18:
            issues.append("dispatch_outside_network_window")
        result["response_at"] = source["response_at"]
        if intent["schedule"]:
            _schedule(source["response"], result, version, clocks[4])
        else:
            _bars(source["response"], result, version, clocks[4], parse(request["end_at"]))
    except (KeyError, TypeError, ValueError, AttributeError, OverflowError) as exc:
        issues.append(f"malformed_receipt:{type(exc).__name__}")
    result["usable"] = not issues
    return result


def _bars(
    rows: Any,
    result: dict[str, Any],
    version: calendar.Version,
    received: datetime,
    requested_end: datetime,
) -> None:
    if not isinstance(rows, list):
        raise ValueError("missing daily bars")
    seen: set[date] = set()
    for index, row in enumerate(rows):
        try:
            day = bar_date(row["date"])
        except (KeyError, TypeError, ValueError):
            result["unassigned_row_issues"].append({"index": index, "reason": "invalid_bar_date"})
            continue
        problem = None
        if day in seen:
            problem = "duplicate"
        seen.add(day)
        try:
            values = [float(row[key]) for key in ("open", "high", "low", "close", "volume")]
            opening, high, low, close, volume = values
            if (
                not all(math.isfinite(value) for value in values)
                or min(values[:4]) <= 0
                or volume < 0
                or low > min(opening, close)
                or high < max(opening, close)
            ):
                problem = problem or "invalid_ohlcv"
            session = version.day(day)
            if session.state != "open" or session.closes_at is None:
                problem = problem or "bar_on_non_session"
            elif session.closes_at > min(received, requested_end):
                problem = problem or "session_not_completed"
            if problem is None:
                result["bars"][str(day)] = {
                    "receipt_sha256": result["receipt_sha256"],
                    "index": index,
                    "content_sha256": digest(rfc8785.dumps(row)),
                }
        except (KeyError, TypeError, ValueError, OverflowError):
            problem = problem or "invalid_ohlcv"
        if problem:
            result["bars"].pop(str(day), None)
            result["date_issues"].setdefault(str(day), []).append(problem)


def _schedule(
    response: Any, result: dict[str, Any], version: calendar.Version, received: datetime
) -> None:
    if response["timezone"] not in {"US/Eastern", "America/New_York"}:
        raise ValueError("unsupported schedule timezone")
    zone = ZoneInfo(response["timezone"])
    seen: set[date] = set()
    for index, row in enumerate(response["sessions"]):
        day = None
        try:
            day = bar_date(row["date"])
            opening = datetime.strptime(row["open"], "%Y%m%d-%H:%M:%S").replace(tzinfo=zone)
            close = datetime.strptime(row["close"], "%Y%m%d-%H:%M:%S").replace(tzinfo=zone)
            if close <= opening or opening.date() != day or close.date() != day or day in seen:
                raise ValueError("invalid or duplicate schedule")
            seen.add(day)
            if close > received:
                result["projected_schedule"].append(str(day))
                continue
            expected = version.day(day)
            if expected.state != "open" or (opening, close) != (
                expected.opens_at,
                expected.closes_at,
            ):
                raise ValueError("completed schedule contradicts frozen calendar")
            result["completed_schedule"][str(day)] = {
                "receipt_sha256": result["receipt_sha256"],
                "index": index,
                "content_sha256": digest(rfc8785.dumps(row)),
            }
        except (KeyError, TypeError, ValueError, OverflowError):
            if day is None:
                result["unassigned_row_issues"].append(
                    {"index": index, "reason": "invalid_schedule_date"}
                )
            else:
                result["completed_schedule"].pop(str(day), None)
                result["date_issues"].setdefault(str(day), []).append(
                    "invalid_or_contradicted_schedule"
                )


def covers(receipt: dict[str, Any], dates: list[str], *, schedule: bool = False) -> bool:
    key = "completed_schedule" if schedule else "bars"
    return bool(
        receipt["usable"]
        and dates
        and all(day in receipt[key] and day not in receipt["date_issues"] for day in dates)
    )


def missing_dates(
    receipt: dict[str, Any], dates: list[str], *, schedule: bool = False
) -> dict[str, list[str]]:
    key = "completed_schedule" if schedule else "bars"
    return {
        day: (
            ["receipt_not_usable", *receipt["issues"]]
            if not receipt["usable"]
            else receipt["date_issues"].get(day, ["provider_omitted_session"])
        )
        for day in dates
        if not receipt["usable"] or day not in receipt[key] or day in receipt["date_issues"]
    }


def stable(left: dict[str, Any], right: dict[str, Any], dates: list[str]) -> bool:
    """Empirical SDK projection stability only; never a vendor finality assertion."""
    return bool(
        covers(left, dates)
        and covers(right, dates)
        and left["conid"] == right["conid"]
        and all(
            left["bars"][day]["content_sha256"] == right["bars"][day]["content_sha256"]
            for day in dates
        )
    )
