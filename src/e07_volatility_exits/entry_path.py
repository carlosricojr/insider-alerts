"""Project source-qualified whole-second equity observations into an opening fill."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import TypeVar

import rfc8785

from e07_volatility_exits.auction import (
    AuctionFill,
    EntryConvention,
    EntryPath,
    UnavailableExecution,
    valid_price,
)
from insider_alerts.backtest.signal_study import MinuteBar
from insider_alerts.strategy.e07 import NEW_YORK

SECOND = timedelta(seconds=1)
MINUTE = timedelta(minutes=1)
Observation = TypeVar("Observation")


@dataclass(frozen=True, slots=True)
class TradePrint:
    """A qualified auction print at the start of its source second."""

    at: datetime
    price: float


@dataclass(frozen=True, slots=True)
class SecondPrices:
    """Eligible extrema and the feasible first-price range in a source second."""

    first_low: float
    first_high: float
    low: float
    high: float


@dataclass(frozen=True, slots=True)
class EntryTape:
    """Normalized observations bound to a verified source manifest.

    The manifest attests the listing, source clocks, eligible conditions,
    full auction fill and source coverage. Each None attests a no-trade
    interval. Normalize source exclusions and corrections before projection.
    """

    manifest_sha256: str
    instrument: str
    symbol: str
    session: date
    opening: TradePrint
    prefix_start: datetime
    prefix_end: datetime
    seconds: Mapping[datetime, SecondPrices | None]
    minutes: Mapping[datetime, MinuteBar | None]
    closing: TradePrint


def _whole_second(clock: datetime) -> datetime:
    if not isinstance(clock, datetime) or clock.utcoffset() is None or clock.microsecond:
        raise UnavailableExecution("entry tape requires aware whole-second source timestamps")
    return clock.astimezone(UTC)


def _print(trade: TradePrint) -> tuple[datetime, float]:
    if not valid_price(trade.price):
        raise UnavailableExecution("entry tape has an invalid print price")
    return _whole_second(trade.at), trade.price


def _grid(
    observations: Mapping[datetime, Observation],
    start: datetime,
    end: datetime,
    width: timedelta,
    label: str,
) -> list[tuple[datetime, Observation]]:
    rows = sorted(
        ((_whole_second(clock), row) for clock, row in observations.items()),
        key=lambda item: item[0],
    )
    count = (end - start) // width
    if len(rows) != count or any(at != start + index * width for index, (at, _) in enumerate(rows)):
        raise UnavailableExecution(f"entry tape does not cover the complete {label} grid")
    return rows


def _minute_grid(
    tape: EntryTape, start: datetime, end: datetime
) -> list[tuple[datetime, MinuteBar | None]]:
    minutes = _grid(tape.minutes, start, end, MINUTE, "minute")
    for at, bar in minutes:
        if bar is not None and not (
            bar.symbol == tape.symbol
            and _whole_second(bar.timestamp) == at
            and all(valid_price(p) for p in (bar.open, bar.high, bar.low, bar.close, bar.volume))
            and bar.low <= min(bar.open, bar.close) <= max(bar.open, bar.close) <= bar.high
        ):
            raise UnavailableExecution("entry tape has an invalid minute observation")
    return minutes


def assemble_opening_fill(
    tape: EntryTape,
    hours: tuple[datetime, datetime],
    *,
    convention: EntryConvention,
) -> AuctionFill:
    """Combine qualified prices under caller-supplied execution conventions."""
    if not (
        isinstance(tape.instrument, str)
        and tape.instrument
        and isinstance(tape.symbol, str)
        and tape.symbol
        and isinstance(tape.manifest_sha256, str)
        and re.fullmatch(r"[0-9a-f]{64}", tape.manifest_sha256)
    ):
        raise UnavailableExecution("entry tape lacks a qualified listing or source manifest")

    opened, closed = map(_whole_second, hours)
    filled_at, open_price = _print(tape.opening)
    closing_at, closing_price = _print(tape.closing)
    prefix_start, prefix_end = map(_whole_second, (tape.prefix_start, tape.prefix_end))
    armed = filled_at + convention.protection_delay
    if not (
        opened <= filled_at < closed
        and opened <= prefix_start <= armed < prefix_end <= closed
        and not (opened.second or prefix_end.second or closed.second)
        and closed <= closing_at
        and closing_at - closed <= convention.closing_report_grace
        and all(
            at.astimezone(NEW_YORK).date() == tape.session for at in (opened, closed, closing_at)
        )
    ):
        raise UnavailableExecution("entry tape has an invalid session or auction clock")

    seconds = _grid(tape.seconds, prefix_start, prefix_end, SECOND, "second")
    populated = [(at, row) for at, row in seconds if row is not None]
    if any(
        not (
            all(valid_price(p) for p in (row.low, row.first_low, row.first_high, row.high))
            and row.low <= row.first_low <= row.first_high <= row.high
        )
        for _, row in populated
    ):
        raise UnavailableExecution("entry tape has an invalid second observation")
    if any(at < armed < at + SECOND for at, _ in populated):
        raise UnavailableExecution("entry tape has a trade bucket straddling protection activation")
    protected = [(at, row) for at, row in populated if at >= armed]
    if not protected:
        raise UnavailableExecution("entry tape is missing the first protected trade")

    minutes = _minute_grid(tape, prefix_end, closed)
    first_at, first = protected[0]
    minute_bars = [bar for _, bar in minutes if bar is not None]
    path = EntryPath(
        tape.instrument,
        tape.symbol,
        tape.session,
        first.first_low,
        first.first_high,
        min([*(row.low for _, row in protected), *(bar.low for bar in minute_bars)]),
        max([*(row.high for _, row in protected), *(bar.high for bar in minute_bars)]),
        closing_price,
    )
    second_rows: list[list[str | float | None]] = [
        [at.isoformat(), None]
        if row is None
        else [at.isoformat(), row.first_low, row.first_high, row.low, row.high]
        for at, row in seconds
    ]
    minute_rows: list[list[str | float | None]] = [
        [at.isoformat(), None]
        if bar is None
        else [at.isoformat(), bar.open, bar.high, bar.low, bar.close, bar.volume]
        for at, bar in minutes
    ]
    material = rfc8785.dumps(
        [
            "e07-qualified-entry-tape-v2",
            tape.manifest_sha256,
            tape.instrument,
            tape.symbol,
            tape.session.isoformat(),
            opened.isoformat(),
            closed.isoformat(),
            filled_at.isoformat(),
            open_price,
            prefix_start.isoformat(),
            prefix_end.isoformat(),
            second_rows,
            minute_rows,
            closing_at.isoformat(),
            closing_price,
            armed.isoformat(),
        ]
    )
    return AuctionFill(
        hashlib.sha256(material).hexdigest(),
        open_price,
        filled_at,
        armed,
        first_at,
        closed,
        closing_at,
        path,
    )
