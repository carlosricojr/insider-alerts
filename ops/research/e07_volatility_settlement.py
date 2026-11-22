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

from e07_volatility_exits.auction import Auction, EntryConvention, SessionHours
from e07_volatility_exits.replay import (
    ActionCoverage,
    BarKey,
    CollisionPolicy,
    ExitReplay,
    PrimaryExitReplay,
    ReplayAgreementError,
    ReplayCandidate,
    ReplayCosts,
    compare_exit_widths,
    compare_primary_exit_widths,
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
DTC_2027_SCHEDULE_SHA256 = "d857405c9a32650bf75e2321eb3e50ffca10bb718b674b3c8f342cf22815336b"
NSCC_2027_SCHEDULE_SHA256 = "13506e89e673a33f34f4f9e7412ce44d46d04845cbc96aee12c0d88726dfbba0"
COVERED_THROUGH_2027 = date(2027, 12, 31)
NON_SETTLEMENT_WEEKDAYS_2027 = (
    date(2027, 1, 1),
    date(2027, 1, 18),
    date(2027, 2, 15),
    date(2027, 5, 31),
    date(2027, 7, 5),
    date(2027, 9, 6),
    date(2027, 10, 11),
    date(2027, 11, 11),
    date(2027, 11, 25),
)
UNRESOLVED_SETTLEMENT_WEEKDAYS_2027 = (
    date(2027, 3, 26),
    date(2027, 6, 18),
    date(2027, 12, 24),
)
ONE_DAY = timedelta(days=1)
SOURCE_STATUS = "anticipated_annual_schedule_operational_notices_unverified"
SOURCE_STATUS_2027 = "anticipated_annual_schedule_limited_equity_settlement_unresolved"
CashRelease = Literal["settlement_date", "next_exchange_open_after_settlement"]
SettlementSchedule = Literal["q4_2026", "through_2027"]


@dataclass(frozen=True, slots=True)
class _Schedule:
    contract: str
    source_status: str
    source_sha256: tuple[str, ...]
    covered_through: date
    non_settlement_weekdays: tuple[date, ...]
    unresolved_weekdays: tuple[date, ...] = ()


_SCHEDULES: Mapping[SettlementSchedule, _Schedule] = MappingProxyType(
    {
        "q4_2026": _Schedule(
            "e07-published-regular-way-us-equity-t1-v1",
            SOURCE_STATUS,
            (DTC_SCHEDULE_SHA256, NSCC_VETERANS_DAY_SHA256),
            COVERED_THROUGH,
            NON_SETTLEMENT_WEEKDAYS,
        ),
        "through_2027": _Schedule(
            "e07-published-regular-way-us-equity-t1-v2",
            SOURCE_STATUS_2027,
            (
                DTC_SCHEDULE_SHA256,
                NSCC_VETERANS_DAY_SHA256,
                DTC_2027_SCHEDULE_SHA256,
                NSCC_2027_SCHEDULE_SHA256,
            ),
            COVERED_THROUGH_2027,
            NON_SETTLEMENT_WEEKDAYS + NON_SETTLEMENT_WEEKDAYS_2027,
            UNRESOLVED_SETTLEMENT_WEEKDAYS_2027,
        ),
    }
)


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
class PrimarySettlementReplay:
    settlement: SettlementBinding
    replay: PrimaryExitReplay


@dataclass(frozen=True, slots=True)
class SettlementReplay:
    settlement: SettlementBinding
    replay: ExitReplay

    def require_primary_agreement(self, primary: PrimarySettlementReplay) -> None:
        if self.settlement != primary.settlement:
            raise ReplayAgreementError("diagnostic replay has a different settlement binding")
        self.replay.require_primary_agreement(primary.replay)


def _next_settlement(trade_day: date, schedule: _Schedule) -> date:
    day = trade_day + ONE_DAY
    while day <= schedule.covered_through:
        if day in schedule.unresolved_weekdays:
            raise ValueError(f"equity settlement on {day} requires a service-specific notice")
        if day.weekday() < 5 and day not in schedule.non_settlement_weekdays:
            return day
        day += ONE_DAY
    raise ValueError(f"settlement after {trade_day} exceeds reviewed coverage")


def _next_exchange_open(version: Version, settlement: date, *, through: date) -> date:
    day = settlement + ONE_DAY
    while day <= through:
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
    schedule: SettlementSchedule = "q4_2026",
) -> SettlementBinding:
    """Bind the full replay range using a validated CalendarJournal version.

    Published clearing dates do not establish when a broker displays settled
    cash. Include idle sessions, the full replay drain and the preceding
    exchange session. Select through_2027 for the bounded extension and
    retain the resulting binding digest.
    """
    selected = _SCHEDULES.get(schedule)
    if selected is None:
        raise ValueError("unknown settlement schedule")
    sessions = tuple(sessions)
    if not sessions or not COVERED_FROM <= sessions[0] <= sessions[-1] <= selected.covered_through:
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

    settlements = {day: _next_settlement(day, selected) for day in sessions}
    # In this reviewed window, every regular settlement day is also a scheduled
    # exchange session. An exception needs its own clearing evidence.
    for due in sorted(set(settlements.values())):
        if version.day(due).state != "open":
            raise ValueError(f"settlement on {due} requires separate clearing evidence")
    cash_releases = (
        {
            trade: _next_exchange_open(version, due, through=selected.covered_through)
            for trade, due in settlements.items()
        }
        if release == "next_exchange_open_after_settlement"
        else settlements
    )
    material: dict[str, str | list[str] | list[list[str]] | dict[str, str]] = {
        "contract": selected.contract,
        "source_sha256": list(selected.source_sha256),
        "exchange_version": version.digest,
        "source_status": selected.source_status,
        "previous_session": str(previous_session),
        "covered_from": str(COVERED_FROM),
        "covered_through": str(selected.covered_through),
        "non_settlement_weekdays": [str(day) for day in selected.non_settlement_weekdays],
        "release": release,
        "dates": [[str(day), str(settlements[day]), str(cash_releases[day])] for day in sessions],
    }
    if schedule == "through_2027":
        material["unresolved_settlement_weekdays"] = [
            str(day) for day in selected.unresolved_weekdays
        ]
        material["source_status_by_period"] = {"2026_q4": SOURCE_STATUS, "2027": SOURCE_STATUS_2027}
    return SettlementBinding(
        hashlib.sha256(rfc8785.dumps(material)).hexdigest(),
        version.digest,
        previous_session,
        selected.source_status,
        release,
        MappingProxyType(settlements),
        MappingProxyType(cash_releases),
    )


def _replay_calendar(
    version: Version,
    sessions: tuple[date, ...],
    *,
    previous_session: date,
    release: CashRelease,
    schedule: SettlementSchedule,
) -> tuple[SettlementBinding, SessionHours]:
    binding = bind_settlement(
        version, sessions, previous_session=previous_session, release=release, schedule=schedule
    )
    hours: dict[date, tuple[datetime, datetime]] = {}
    for day in sessions:
        session = version.day(day)
        if session.opens_at is None or session.closes_at is None:
            raise ValueError(f"missing exchange hours on {day}")
        hours[day] = (session.opens_at, session.closes_at)
    return binding, hours


def compare_with_settlement(
    version: Version,
    sessions: Sequence[date],
    candidates: Sequence[ReplayCandidate],
    bars: Mapping[BarKey, DailyBar],
    *,
    action_coverage: ActionCoverage,
    opening_evidence: Mapping[BarKey, Auction],
    entry_convention: EntryConvention,
    initial_cash: Decimal,
    costs: ReplayCosts,
    previous_session: date,
    release: CashRelease,
    collision: CollisionPolicy = "stop_first",
    schedule: SettlementSchedule = "q4_2026",
) -> SettlementReplay:
    """Use one verified session sequence for both the binding and the replay."""
    sessions = tuple(sessions)
    binding, hours = _replay_calendar(
        version, sessions, previous_session=previous_session, release=release, schedule=schedule
    )
    replay = compare_exit_widths(
        sessions,
        binding.cash_release_dates,
        candidates,
        bars,
        action_coverage=action_coverage,
        opening_evidence=opening_evidence,
        session_hours=hours,
        entry_convention=entry_convention,
        initial_cash=initial_cash,
        costs=costs,
        previous_session=previous_session,
        collision=collision,
    )
    return SettlementReplay(binding, replay)


def compare_primary_with_settlement(
    version: Version,
    sessions: Sequence[date],
    candidates: Sequence[ReplayCandidate],
    bars: Mapping[BarKey, DailyBar],
    *,
    action_coverage: ActionCoverage,
    opening_evidence: Mapping[BarKey, Auction],
    entry_convention: EntryConvention,
    initial_cash: Decimal,
    costs: ReplayCosts,
    previous_session: date,
    release: CashRelease,
    collision: CollisionPolicy = "stop_first",
    schedule: SettlementSchedule = "q4_2026",
) -> PrimarySettlementReplay:
    """Replay the primary comparison using the verified settlement calendar."""
    sessions = tuple(sessions)
    binding, hours = _replay_calendar(
        version, sessions, previous_session=previous_session, release=release, schedule=schedule
    )
    replay = compare_primary_exit_widths(
        sessions,
        binding.cash_release_dates,
        candidates,
        bars,
        action_coverage=action_coverage,
        opening_evidence=opening_evidence,
        session_hours=hours,
        entry_convention=entry_convention,
        initial_cash=initial_cash,
        costs=costs,
        previous_session=previous_session,
        collision=collision,
    )
    return PrimarySettlementReplay(binding, replay)
