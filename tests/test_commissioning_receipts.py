from __future__ import annotations

import copy

import pytest

from insider_alerts.research.commissioning_receipts import (
    covers,
    inspect_receipt,
    missing_dates,
    qualified_identity,
    stable,
)
from tests.test_commissioning_window import VERSION


def receipts(*, schedule=False):
    at = "2026-10-27T22:00:00.000000Z"
    attempt = {
        "sha256": "a" * 64,
        "recorded_at": at,
        "payload": {
            "symbol": "SPY",
            "expected_conid": None,
            "start_date": "2026-09-29",
            "end_at": "2026-10-27T20:00:00.000000Z",
            "schedule": schedule,
        },
    }
    bar = {
        "date": "2026-10-26",
        "open": "10.0",
        "high": "11.0",
        "low": "9.0",
        "close": "10.5",
        "volume": "100",
    }
    response = {
        "sha256": "b" * 64,
        "entity": attempt["sha256"],
        "recorded_at": at,
        "payload": {
            "symbol": "SPY",
            "result": "received",
            "clock_integrity": "valid",
            "cleanup_errors": [],
            "started_at": at,
            "client_dispatch_at": at,
            "response_at": at,
            "finished_at": at,
            "identity": {
                "conid": 123,
                "symbol": "SPY",
                "security_type": "STK",
                "currency": "USD",
                "primary_exchange": "ARCA",
                "observed_at": at,
            },
            "request": {
                "start_date": "2026-09-29",
                "end_at": attempt["payload"]["end_at"],
                "what_to_show": "SCHEDULE" if schedule else "TRADES",
                "bar_size": "1 day",
                "use_rth": True,
                "format_date": 1 if schedule else 2,
            },
            "response": {
                "timezone": "US/Eastern",
                "sessions": [
                    {"date": "20261026", "open": "20261026-09:30:00", "close": "20261026-16:00:00"}
                ],
            }
            if schedule
            else [bar],
        },
    }
    return attempt, response


def test_receipt_exact_references_partial_coverage_and_empirical_stability():
    attempt, response = receipts()
    result = inspect_receipt(attempt, response, VERSION)
    assert result["usable"]
    assert covers(result, ["2026-10-26"])
    assert not covers(result, ["2026-10-26", "2026-10-27"])
    ref = result["bars"]["2026-10-26"]
    assert ref["receipt_sha256"] == response["sha256"] and ref["index"] == 0
    assert len(ref["content_sha256"]) == 64
    assert stable(result, result, ["2026-10-26"])
    changed = copy.deepcopy(response)
    changed["payload"]["response"][0]["close"] = "10.6"
    assert not stable(result, inspect_receipt(attempt, changed, VERSION), ["2026-10-26"])


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("open", "NaN"),
        ("high", "inf"),
        ("close", "0"),
        ("low", "11"),
        ("high", "10"),
        ("volume", "-1"),
        ("date", "2026-10-25"),
        ("date", "2026-10-28"),
        ("date", "bad"),
        ("date", None),
    ],
)
def test_invalid_or_uncompleted_bar_has_explicit_date_missingness(key, value):
    attempt, response = receipts()
    response["payload"]["response"][0][key] = value
    result = inspect_receipt(attempt, response, VERSION)
    assert result["usable"] and (result["date_issues"] or result["unassigned_row_issues"])
    assert not covers(result, ["2026-10-26"])


def test_duplicate_bar_never_counts_as_coverage():
    attempt, response = receipts()
    response["payload"]["response"] *= 3
    result = inspect_receipt(attempt, response, VERSION)
    assert not covers(result, ["2026-10-26"])
    assert "2026-10-26" not in result["bars"]


@pytest.mark.parametrize(
    ("area", "key", "value"),
    [
        ("request", "what_to_show", "MIDPOINT"),
        ("request", "use_rth", 1),
        ("request", "start_date", "2026-10-01"),
        ("request", "end_at", "2026-10-27T23:00:00Z"),
        ("identity", "conid", True),
        ("identity", "currency", "CAD"),
        ("identity", "symbol", "OTHER"),
        ("identity", "primary_exchange", "PINK"),
    ],
)
def test_wrong_instrument_or_request_cannot_be_healthy(area, key, value):
    attempt, response = receipts()
    response["payload"][area][key] = value
    assert not inspect_receipt(attempt, response, VERSION)["usable"]


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("result", "timeout"),
        ("clock_integrity", "reversed"),
        ("cleanup_errors", ["disconnect"]),
        ("response_at", "2026-10-27T21:00:00Z"),
        ("client_dispatch_at", "2026-10-27T17:00:00Z"),
    ],
)
def test_failure_axes_independently_preclude_healthy_receipt(key, value):
    attempt, response = receipts()
    response["payload"][key] = value
    assert not inspect_receipt(attempt, response, VERSION)["usable"]


def test_pins_identity_even_when_history_failed_and_accepts_pinned_symbol_change():
    attempt, response = receipts()
    response["payload"].update(result="timeout", response_at=None, response=None)
    assert qualified_identity(attempt, response) == 123
    attempt["payload"]["expected_conid"] = 123
    response["payload"]["identity"]["symbol"] = "CHANGED"
    assert qualified_identity(attempt, response) == 123
    response["payload"]["identity"]["conid"] = 456
    assert qualified_identity(attempt, response) is None


def test_completed_schedule_compared_but_future_projection_is_only_retained():
    attempt, response = receipts(schedule=True)
    response["payload"]["response"]["sessions"].append(
        {"date": "20261028", "open": "20261028-09:00:00", "close": "20261028-14:00:00"}
    )
    result = inspect_receipt(attempt, response, VERSION)
    assert covers(result, ["2026-10-26"], schedule=True)
    assert result["projected_schedule"] == ["2026-10-28"]
    assert not covers(result, ["2026-10-26", "2026-10-27"], schedule=True)
    response["payload"]["response"]["sessions"][0]["close"] = "20261026-13:00:00"
    assert not covers(inspect_receipt(attempt, response, VERSION), ["2026-10-26"], schedule=True)


def test_unknown_schedule_timezone_and_duplicate_sessions_rejected():
    attempt, response = receipts(schedule=True)
    response["payload"]["response"]["timezone"] = "UTC"
    assert not inspect_receipt(attempt, response, VERSION)["usable"]
    response["payload"]["response"]["timezone"] = "US/Eastern"
    response["payload"]["response"]["sessions"] *= 2
    assert not covers(inspect_receipt(attempt, response, VERSION), ["2026-10-26"], schedule=True)


def test_bad_extra_rows_do_not_discard_valid_required_dates():
    attempt, response = receipts()
    bad = copy.deepcopy(response["payload"]["response"][0])
    bad.update(date="2026-10-27", high="0")
    response["payload"]["response"].append(bad)
    result = inspect_receipt(attempt, response, VERSION)
    assert covers(result, ["2026-10-26"])
    assert missing_dates(result, ["2026-10-26", "2026-10-27", "2026-10-23"]) == {
        "2026-10-27": ["invalid_ohlcv"],
        "2026-10-23": ["provider_omitted_session"],
    }


def test_successful_empty_response_has_explicit_missing_dates():
    attempt, response = receipts()
    response["payload"].update(result="empty_response", response=[])
    result = inspect_receipt(attempt, response, VERSION)
    assert result["usable"] and not covers(result, ["2026-10-26"])
    assert missing_dates(result, ["2026-10-26"]) == {"2026-10-26": ["provider_omitted_session"]}
