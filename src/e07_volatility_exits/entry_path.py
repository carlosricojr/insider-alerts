"""Project source-qualified whole-second equity observations into an opening fill."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

import rfc8785

from e07_volatility_exits.auction import AuctionFill, EntryPath, UnavailableExecution, valid_price
from insider_alerts.backtest.signal_study import MinuteBar
from insider_alerts.strategy.e07 import NEW_YORK

SECOND = timedelta(seconds=1)
MINUTE = timedelta(minutes=1)


@dataclass(frozen=True, slots=True)
class TradePrint:
    """An eligible price timestamped at the start of its source second."""

    at: datetime
    price: float


@dataclass(frozen=True, slots=True)
class EntryTape:
    """Normalized observations bound to a verified source manifest.

    The manifest attests the listing, source clocks, eligible conditions,
    full auction fill and prefix pagination. Each None attests a no-trade
    minute. Normalize source exclusions and corrections before projection.
    """

    manifest_sha256: str
    instrument: str
    symbol: str
    session: date
    opening: TradePrint
    prefix_start: datetime
    prefix_end: datetime
    prefix: tuple[TradePrint, ...]
    minutes: Mapping[datetime, MinuteBar | None]
    closing: TradePrint


def _whole_second(clock: datetime) -> datetime:
    if clock.utcoffset() is None or clock.microsecond:
        raise UnavailableExecution("entry tape requires aware whole-second source timestamps")
    return clock.astimezone(UTC)


def _print(trade: TradePrint) -> tuple[datetime, float]:
    if not valid_price(trade.price):
        raise UnavailableExecution("entry tape has an invalid print price")
    return _whole_second(trade.at), trade.price


def _minute_grid(
    tape: EntryTape, start: datetime, end: datetime
) -> list[tuple[datetime, MinuteBar | None]]:
    minutes = sorted(
        ((_whole_second(clock), bar) for clock, bar in tape.minutes.items()),
        key=lambda item: item[0],
    )
    count = (end - start) // MINUTE
    if len(minutes) != count or any(
        at != start + index * MINUTE for index, (at, _) in enumerate(minutes)
    ):
        raise UnavailableExecution("entry tape does not cover the complete minute grid")
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
    protection_delay: timedelta,
    closing_report_grace: timedelta,
    close_can_trigger: bool,
) -> AuctionFill:
    """Combine qualified prices under caller-supplied execution conventions."""
    if not timedelta() < protection_delay < timedelta(days=1) or closing_report_grace < timedelta():
        raise ValueError("entry tape has an invalid delay or closing report window")
    if type(close_can_trigger) is not bool:
        raise ValueError("entry tape requires an explicit closing eligibility rule")
    if (
        not tape.instrument
        or not tape.symbol
        or not re.fullmatch(r"[0-9a-f]{64}", tape.manifest_sha256)
    ):
        raise UnavailableExecution("entry tape lacks a qualified listing or source manifest")

    opened, closed = map(_whole_second, hours)
    filled_at, open_price = _print(tape.opening)
    closing_at, closing_price = _print(tape.closing)
    prefix_start, prefix_end = map(_whole_second, (tape.prefix_start, tape.prefix_end))
    armed = filled_at + protection_delay
    if not (
        opened <= filled_at < closed
        and opened <= prefix_start <= armed < prefix_end <= closed
        and not (opened.second or prefix_end.second or closed.second)
        and closed <= closing_at
        and closing_at - closed <= closing_report_grace
        and all(
            at.astimezone(NEW_YORK).date() == tape.session for at in (opened, closed, closing_at)
        )
    ):
        raise UnavailableExecution("entry tape has an invalid session or auction clock")

    trades = sorted(map(_print, tape.prefix))
    if any(not prefix_start <= at < prefix_end for at, _ in trades):
        raise UnavailableExecution("entry tape has a print outside its prefix window")
    if any(at < armed < at + SECOND for at, _ in trades):
        raise UnavailableExecution("entry tape has a trade bucket straddling protection activation")
    protected = [(at, price) for at, price in trades if at >= armed]
    if not protected:
        raise UnavailableExecution("entry tape is missing the first protected trade")

    minutes = _minute_grid(tape, prefix_end, closed)
    first_at = protected[0][0]
    first_prices = [price for at, price in protected if at == first_at]
    tick_prices = [price for _, price in protected]
    path = EntryPath(
        tape.symbol,
        tape.session,
        min(first_prices),
        max(first_prices),
        min([*tick_prices, *(bar.low for _, bar in minutes if bar is not None)]),
        max([*tick_prices, *(bar.high for _, bar in minutes if bar is not None)]),
        closing_price,
        close_can_trigger,
    )
    minute_rows: list[list[str | float | None]] = [
        [at.isoformat(), None]
        if bar is None
        else [at.isoformat(), bar.open, bar.high, bar.low, bar.close, bar.volume]
        for at, bar in minutes
    ]
    material = rfc8785.dumps(
        [
            "e07-qualified-entry-tape-v1",
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
            [[at.isoformat(), price] for at, price in trades],
            minute_rows,
            closing_at.isoformat(),
            closing_price,
            protection_delay // timedelta(microseconds=1),
            closing_report_grace // timedelta(microseconds=1),
            close_can_trigger,
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
