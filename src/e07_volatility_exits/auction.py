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
class AuctionFill:
    """Full fill and executable prices under the declared protection convention."""

    source_sha256: str
    price: float
    filled_at: datetime
    protected_from: datetime
    first_protected_trade_at: datetime
    protected_through: datetime
    protected_bar: DailyBar


@dataclass(frozen=True, slots=True)
class AuctionMiss:
    """An independently established miss with a source record."""

    source_sha256: str


Auction = AuctionFill | AuctionMiss


def valid_bar(bar: DailyBar, symbol: str, session: date) -> bool:
    return (
        isinstance(bar, DailyBar)
        and bar.symbol == symbol
        and bar.trade_date == session
        and all(
            type(value) in (int, float) and math.isfinite(value) and value > 0
            for value in (bar.open, bar.high, bar.low, bar.close, bar.volume)
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
    protection_delay: timedelta,
    holding_bars: Mapping[BarKey, DailyBar],
) -> str:
    """Validate projected prices; upstream custody establishes source truth."""
    if not isinstance(
        protection_delay, timedelta
    ) or not timedelta() < protection_delay < timedelta(days=1):
        raise ValueError("opening protection delay must be positive and shorter than a day")
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
        filled, armed, first, through = map(
            _utc,
            (
                auction.filled_at,
                auction.protected_from,
                auction.first_protected_trade_at,
                auction.protected_through,
            ),
        )
        if not (
            start <= filled < armed <= first <= through == end
            and armed - filled == protection_delay
        ):
            raise UnavailableExecution(
                f"invalid auction or protected-trade timing for {instrument}"
            )
        if (
            type(auction.price) not in (float, int)
            or not math.isfinite(auction.price)
            or auction.price <= 0
            or not valid_bar(auction.protected_bar, symbol, session)
        ):
            raise UnavailableExecution(f"invalid auction or protected bar for {instrument}")
        bar = auction.protected_bar
        holding = holding_bars.get(key)
        if holding is not None and holding.close != bar.close:
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
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.volume,
            ]
        )
    held: list[list[str | int | float]] = [
        [instrument, str(session), bar.symbol, bar.open, bar.high, bar.low, bar.close, bar.volume]
        for (instrument, session), bar in sorted(holding_bars.items())
    ]
    delay_us = protection_delay // timedelta(microseconds=1)
    return hashlib.sha256(
        rfc8785.dumps(["e07-execution-evidence-v1", delay_us, rows, held])
    ).hexdigest()
