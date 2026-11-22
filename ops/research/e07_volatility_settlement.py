"""Reviewed regular-way U.S. equity cash-release schedule for E07 research."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from types import MappingProxyType
from typing import Literal

import rfc8785

from e07_volatility_exits.auction import Auction
from e07_volatility_exits.replay import (
    ActionCoverage,
    BarKey,
    CollisionPolicy,
    ExitReplay,
    ReplayCandidate,
    ReplayCosts,
    compare_exit_widths,
)
from insider_alerts.backtest.models import DailyBar
from insider_alerts.research.calendar_v2 import Version

# The source bytes, observation times and scope are documented in calendar-sources.
DTC_SCHEDULE_SHA256 = "943e9bbb70de5ea0e074fa9942a6f08a5661a8a3f105c46326a5e8a29c39cddf"
NSCC_VETERANS_DAY_SHA256 = "bbb4dfec7877309a2d7267e8c3fe7d07fb3136f1f509df9d26a6d9185fee5d2f"
COVERED_FROM = date(2026, 10, 1)
COVERED_THROUGH = date(2026, 12, 31)
NON_SETTLEMENT_WEEKDAYS = (
    date(2026, 10, 12),
    date(2026, 11, 11),
    date(2026, 11, 26),
    date(2026, 12, 25),
)
ONE_DAY = timedelta(days=1)
SOURCE_STATUS = "anticipated_annual_schedule_operational_notices_unverified"
CashRelease = Literal["settlement_date", "next_exchange_open_after_settlement"]


@dataclass(frozen=True, slots=True)
class SettlementBinding:
    """Supply cash_release_dates to the replay and retain the binding digest."""

    digest: str
    exchange_version: str
    previous_session: date
    source_status: str
    release: CashRelease
    settlement_dates: Mapping[date, date] = field(hash=False)
    cash_release_dates: Mapping[date, date] = field(hash=False)


@dataclass(frozen=True, slots=True)
class SettlementReplay:
    settlement: SettlementBinding
    replay: ExitReplay


def _next_settlement(trade_day: date) -> date:
    day = trade_day + ONE_DAY
    while day <= COVERED_THROUGH:
        if day.weekday() < 5 and day not in NON_SETTLEMENT_WEEKDAYS:
            return day
        day += ONE_DAY
    raise ValueError(f"settlement after {trade_day} exceeds reviewed coverage")


def _next_exchange_open(version: Version, settlement: date) -> date:
    day = settlement + ONE_DAY
    while day <= COVERED_THROUGH:
        state = version.day(day).state
        if state == "open":
            return day
        if state == "unknown":
            raise ValueError(f"unknown exchange session at {day}")
        day += ONE_DAY
    raise ValueError(f"cash release after {settlement} exceeds reviewed coverage")


def bind_settlement(
    version: Version,
    sessions: Sequence[date],
    *,
    previous_session: date,
    release: CashRelease,
) -> SettlementBinding:
    """Bind the full replay range using a validated CalendarJournal version.

    Published clearing dates do not establish when a broker displays settled
    cash. Include idle sessions, the full replay drain and the preceding
    exchange session.
    """
    sessions = tuple(sessions)
    if not sessions or not COVERED_FROM <= sessions[0] <= sessions[-1] <= COVERED_THROUGH:
        raise ValueError("replay sessions outside reviewed settlement coverage")
    if not previous_session < sessions[0]:
        raise ValueError("invalid preceding exchange session")
    if release not in ("settlement_date", "next_exchange_open_after_settlement"):
        raise ValueError("invalid cash-release rule")

    expected_sessions = []
    day = previous_session
    while day <= sessions[-1]:
        state = version.day(day).state
        if state == "unknown":
            raise ValueError(f"unknown exchange session at {day}")
        if state == "open":
            expected_sessions.append(day)
        day += ONE_DAY
    if tuple(expected_sessions) != (previous_session, *sessions):
        raise ValueError("replay requires consecutive exchange sessions and the previous session")

    settlements = {day: _next_settlement(day) for day in sessions}
    # In this reviewed window, every regular settlement day is also a scheduled
    # exchange session. An exception needs its own clearing evidence.
    for due in sorted(set(settlements.values())):
        if version.day(due).state != "open":
            raise ValueError(f"settlement on {due} requires separate clearing evidence")
    cash_releases = (
        {trade: _next_exchange_open(version, due) for trade, due in settlements.items()}
        if release == "next_exchange_open_after_settlement"
        else settlements
    )
    material = {
        "contract": "e07-published-regular-way-us-equity-t1-v1",
        "source_sha256": [DTC_SCHEDULE_SHA256, NSCC_VETERANS_DAY_SHA256],
        "exchange_version": version.digest,
        "source_status": SOURCE_STATUS,
        "previous_session": str(previous_session),
        "covered_from": str(COVERED_FROM),
        "covered_through": str(COVERED_THROUGH),
        "non_settlement_weekdays": [str(day) for day in NON_SETTLEMENT_WEEKDAYS],
        "release": release,
        "dates": [[str(day), str(settlements[day]), str(cash_releases[day])] for day in sessions],
    }
    return SettlementBinding(
        hashlib.sha256(rfc8785.dumps(material)).hexdigest(),
        version.digest,
        previous_session,
        SOURCE_STATUS,
        release,
        MappingProxyType(settlements),
        MappingProxyType(cash_releases),
    )


def compare_with_settlement(
    version: Version,
    sessions: Sequence[date],
    candidates: Sequence[ReplayCandidate],
    bars: Mapping[BarKey, DailyBar],
    *,
    action_coverage: ActionCoverage,
    opening_evidence: Mapping[BarKey, Auction],
    protection_delay: timedelta,
    initial_cash: Decimal,
    costs: ReplayCosts,
    previous_session: date,
    release: CashRelease,
    collision: CollisionPolicy = "stop_first",
) -> SettlementReplay:
    """Use one verified session sequence for both the binding and the replay."""
    sessions = tuple(sessions)
    binding = bind_settlement(version, sessions, previous_session=previous_session, release=release)
    hours: dict[date, tuple[datetime, datetime]] = {}
    for day in sessions:
        session = version.day(day)
        if session.opens_at is None or session.closes_at is None:
            raise ValueError(f"missing exchange hours on {day}")
        hours[day] = (session.opens_at, session.closes_at)
    replay = compare_exit_widths(
        sessions,
        binding.cash_release_dates,
        candidates,
        bars,
        action_coverage=action_coverage,
        opening_evidence=opening_evidence,
        session_hours=hours,
        protection_delay=protection_delay,
        initial_cash=initial_cash,
        costs=costs,
        previous_session=previous_session,
        collision=collision,
    )
    return SettlementReplay(binding, replay)
