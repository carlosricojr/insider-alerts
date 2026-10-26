from __future__ import annotations

from dataclasses import fields, replace
from datetime import UTC, date, datetime, time, timedelta

import pytest

from insider_alerts.execution.canary import CanaryConfig
from insider_alerts.research import calendar_v2 as cal
from insider_alerts.research.commissioning_window import (
    FrozenE07,
    admit,
    history_dates,
    manifest,
    parse,
    session_for,
    validate_manifest,
)

CREATED = datetime(2026, 10, 26, 21, tzinfo=UTC)
BOUNDARY = datetime(2026, 10, 27, 10, tzinfo=UTC)
VERSION = cal.Version("a" * 64, None, CREATED, cal._baseline_days(), ())


def empty_inventory():
    return {
        "source_path": "synthetic",
        "source_device": "1",
        "source_inode": "1",
        "read_started_at": CREATED.isoformat(),
        "observed_at": CREATED.isoformat(),
        "packet_id_hashes": [],
    }


def frozen(*, inventory=None):
    return manifest(
        VERSION,
        boundary=BOUNDARY,
        created_at=CREATED,
        revision="b" * 40,
        review_reference="review/commissioning",
        implementation_sha256="c" * 64,
        inventory=empty_inventory() if inventory is None else inventory,
    )


def test_control_constants_match_and_only_entry_function_is_called():
    policy = FrozenE07()
    control = CanaryConfig(source_db="unused")
    for field in fields(policy):
        assert getattr(policy, field.name) == getattr(control, field.name)


def test_manifest_fixed_window_dst_recovery_and_strict_replay():
    value = frozen()
    assert value["entry_sessions"] == [
        "2026-10-27",
        "2026-10-28",
        "2026-10-29",
        "2026-10-30",
        "2026-11-02",
        "2026-11-03",
        "2026-11-04",
        "2026-11-05",
        "2026-11-06",
        "2026-11-09",
    ]
    assert value["sessions"][3]["open"].endswith("13:30:00.000000Z")
    assert value["sessions"][4]["open"].endswith("14:30:00.000000Z")
    assert value["sessions"][18]["date"] == "2026-11-20"
    assert value["sessions"][19]["date"] == "2026-11-23"
    assert value["recovery_deadline"] == "2026-11-26T13:00:00.000000Z"
    validate_manifest(value, VERSION)
    for change in ({"strategy_trial_ready": 0}, {"network_minimum_seconds": 299}, {"extra": 1}):
        with pytest.raises(ValueError):
            validate_manifest(value | change, VERSION)


@pytest.mark.parametrize(
    "change",
    [
        {"boundary": CREATED + timedelta(hours=1, minutes=59)},
        {"created_at": CREATED - timedelta(minutes=1)},
        {"revision": "main"},
        {"review_reference": " "},
        {"boundary": datetime(2026, 10, 27)},
    ],
)
def test_manifest_rejects_unobserved_or_nonprospective_contract(change):
    args = dict(
        boundary=BOUNDARY,
        created_at=CREATED,
        revision="b" * 40,
        review_reference="review",
        implementation_sha256="c" * 64,
        inventory=empty_inventory(),
    )
    with pytest.raises(ValueError):
        manifest(VERSION, **(args | change))


@pytest.mark.parametrize(
    ("signal", "expected"),
    [
        ("2026-10-27T09:19:59-04:00", "2026-10-27"),
        ("2026-10-27T09:20:00-04:00", "2026-10-28"),
        ("2026-11-26T08:00:00-05:00", "2026-11-27"),
        ("2026-11-27T09:20:00-05:00", "2026-11-30"),
        ("2028-02-28T16:00:00-05:00", "2028-02-29"),
        ("2027-12-30T16:00:00-05:00", "2027-12-31"),
    ],
)
def test_entry_unchanged_on_holiday_leap_and_cutoff(signal, expected):
    assert str(session_for(VERSION, parse(signal))) == expected


def test_pre_signal_early_close_uses_frozen_16_hour_completion():
    before = history_dates(VERSION, parse("2026-11-27T15:59:59-05:00"))
    after = history_dates(VERSION, parse("2026-11-27T16:00:00-05:00"))
    assert len(before) == len(after) == 20
    assert before[-1] == date(2026, 11, 25)
    assert after[-1] == date(2026, 11, 27)
    assert before[1:] == after[:-1]


def test_missing_calendar_is_not_silently_skipped():
    days = tuple(
        cal.Day(d.day, "unknown") if d.day == date(2026, 10, 27) else d for d in VERSION.days
    )
    version = replace(VERSION, days=days)
    with pytest.raises(ValueError, match="unknown entry"):
        session_for(version, BOUNDARY)
    with pytest.raises(ValueError, match="unknown pre-signal"):
        history_dates(version, parse("2026-10-28T08:00:00-04:00"))


def test_invalid_symbol_and_late_observation_stay_in_original_denominator():
    row = {"signal_at": "2026-10-27T08:00:00-04:00", "symbol": "?", "entry_session": "2026-10-28"}
    result = admit(row, frozen(), VERSION, first_observed_at=parse("2026-11-10T12:00:00Z"))
    assert result["membership"] == "admitted"
    assert result["reason"] == "invalid_symbol"
    assert result["entry_session"] == "2026-10-27"
    assert not result["recorded_entry_agrees"]
    assert not result["contemporaneous_custody"]
    assert len(result["history_dates"]) == 20 and len(result["horizon_dates"]) == 10
    assert result["stability_session"] == "2026-11-10"


def test_bad_history_does_not_remove_admitted_case():
    days = tuple(
        cal.Day(d.day, "unknown") if d.day == date(2026, 10, 26) else d for d in VERSION.days
    )
    result = admit(
        {"signal_at": "2026-10-27T12:00:00Z", "symbol": "TEST"},
        frozen(),
        replace(VERSION, days=days),
        first_observed_at=parse("2026-10-27T13:00:00Z"),
    )
    assert result["membership"] == "admitted"
    assert result["reason"].startswith("invalid_source_or_calendar")


@pytest.mark.parametrize(
    ("signal", "membership"),
    [
        ("2026-10-27T09:59:59Z", "outside"),
        ("2026-11-09T09:19:59-05:00", "admitted"),
        ("2026-11-09T09:20:00-05:00", "outside"),
        ("2026-11-10T09:00:00-05:00", "outside"),
        ("bad", "unassignable"),
        (None, "unassignable"),
        ("2026-11-15T09:00:00-05:00", "unassignable"),
    ],
)
def test_exact_enrollment_and_malformed_clocks(signal, membership):
    result = admit(
        {"signal_at": signal, "symbol": "TEST"},
        frozen(),
        VERSION,
        first_observed_at=parse("2026-11-10T16:00:00Z"),
    )
    assert result["membership"] == membership


def test_cutoff_is_0920_not_auction_open():
    row = {"signal_at": "2026-10-27T12:00:00Z", "symbol": "TEST"}
    for minute, timely in ((19, True), (20, False), (29, False)):
        at = datetime.combine(date(2026, 10, 27), time(9, minute), cal.NY)
        assert (
            admit(row, frozen(), VERSION, first_observed_at=at)["contemporaneous_custody"] == timely
        )
