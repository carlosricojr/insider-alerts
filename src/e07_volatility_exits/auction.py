"""Opening execution evidence for the offline E07 replay."""

from __future__ import annotations

import hashlib
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

import rfc8785

from insider_alerts.backtest.models import DailyBar
from insider_alerts.strategy.e07 import NEW_YORK

BarKey = tuple[str, date]
SessionHours = Mapping[date, tuple[datetime, datetime]]


class UnavailableExecution(ValueError):
    """A candidate or held name lacks a usable outcome observation."""


@dataclass(frozen=True, slots=True)
class EntryConvention:
    """One execution convention for every entry and arm in a comparison."""

    protection_delay: timedelta
    closing_report_grace: timedelta
    close_can_trigger: bool

    def __post_init__(self) -> None:
        if not (
            isinstance(self.protection_delay, timedelta)
            and timedelta() < self.protection_delay < timedelta(days=1)
            and isinstance(self.closing_report_grace, timedelta)
            and self.closing_report_grace >= timedelta()
            and type(self.close_can_trigger) is bool
        ):
            raise ValueError("invalid entry execution convention")


@dataclass(frozen=True, slots=True)
class EntryPath:
    """Continuous protected range and independently qualified closing price."""

    instrument: str
    symbol: str
    session: date
    first_low: float
    first_high: float
    low: float
    high: float
    close: float


@dataclass(frozen=True, slots=True)
class AuctionFill:
    """Full fill and executable prices under the declared protection convention."""

    source_sha256: str
    price: float
    filled_at: datetime
    protected_from: datetime
    first_protected_trade_at: datetime
    protected_through: datetime
    closing_print_at: datetime
    path: EntryPath


@dataclass(frozen=True, slots=True)
class AuctionMiss:
    """An independently established miss with a source record."""

    source_sha256: str


Auction = AuctionFill | AuctionMiss


def valid_price(value: float) -> bool:
    return type(value) in (int, float) and math.isfinite(value) and value > 0


def valid_bar(bar: DailyBar, symbol: str, session: date) -> bool:
    return (
        isinstance(bar, DailyBar)
        and bar.symbol == symbol
        and bar.trade_date == session
        and all(
            valid_price(value) for value in (bar.open, bar.high, bar.low, bar.close, bar.volume)
        )
        and bar.low <= min(bar.open, bar.close) <= max(bar.open, bar.close) <= bar.high
    )


def _utc(moment: datetime) -> datetime:
    if not isinstance(moment, datetime) or moment.utcoffset() is None:
        raise UnavailableExecution("opening evidence requires an aware clock")
    return moment.astimezone(UTC)


def validate_opening_evidence(
    names: Mapping[BarKey, str],
    auctions: Mapping[BarKey, Auction],
    hours: SessionHours,
    convention: EntryConvention,
    holding_bars: Mapping[BarKey, DailyBar],
) -> str:
    """Validate projected prices; upstream custody establishes source truth."""
    rows: list[list[str | float | int]] = []
    for key, symbol in sorted(names.items()):
        instrument, session = key
        bounds = hours.get(session)
        if bounds is None:
            raise UnavailableExecution(f"missing opening session hours on {session}")
        start, end = map(_utc, bounds)
        if start >= end or any(
            stamp.astimezone(NEW_YORK).date() != session for stamp in (start, end)
        ):
            raise UnavailableExecution(f"invalid opening session hours on {session}")
        auction = auctions[key]
        if not isinstance(auction.source_sha256, str) or not re.fullmatch(
            r"[0-9a-f]{64}", auction.source_sha256
        ):
            raise UnavailableExecution(f"missing opening source digest for {instrument}")
        row: list[str | float | int] = [
            instrument,
            str(session),
            symbol,
            start.isoformat(),
            end.isoformat(),
            auction.source_sha256,
        ]
        if isinstance(auction, AuctionMiss):
            rows.append([*row, "miss"])
            continue
        filled, armed, first, through, closing = map(
            _utc,
            (
                auction.filled_at,
                auction.protected_from,
                auction.first_protected_trade_at,
                auction.protected_through,
                auction.closing_print_at,
            ),
        )
        if not (
            start <= filled < armed <= first < through == end <= closing
            and closing.astimezone(NEW_YORK).date() == session
            and closing - end <= convention.closing_report_grace
            and armed - filled == convention.protection_delay
        ):
            raise UnavailableExecution(
                f"invalid auction or protected-trade timing for {instrument}"
            )
        path = auction.path
        if not (
            valid_price(auction.price)
            and isinstance(path, EntryPath)
            and path.instrument == instrument
            and path.symbol == symbol
            and path.session == session
            and all(
                valid_price(p)
                for p in (path.first_low, path.first_high, path.low, path.high, path.close)
            )
            and path.low <= path.first_low <= path.first_high <= path.high
        ):
            raise UnavailableExecution(f"invalid auction or protected path for {instrument}")
        holding = holding_bars.get(key)
        if holding is not None and holding.close != path.close:
            raise UnavailableExecution(
                f"entry and holding marks disagree for {instrument} on {session}"
            )
        rows.append(
            [
                *row,
                "fill",
                filled.isoformat(),
                auction.price,
                armed.isoformat(),
                first.isoformat(),
                through.isoformat(),
                closing.isoformat(),
                path.first_low,
                path.first_high,
                path.low,
                path.high,
                path.close,
            ]
        )
    held: list[list[str | int | float]] = [
        [instrument, str(session), bar.symbol, bar.open, bar.high, bar.low, bar.close, bar.volume]
        for (instrument, session), bar in sorted(holding_bars.items())
    ]
    unit = timedelta(microseconds=1)
    return hashlib.sha256(
        rfc8785.dumps(
            [
                "e07-execution-evidence-v2",
                convention.protection_delay // unit,
                convention.closing_report_grace // unit,
                convention.close_can_trigger,
                rows,
                held,
            ]
        )
    ).hexdigest()
