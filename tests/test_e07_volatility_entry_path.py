from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from e07_volatility_exits.auction import AuctionFill, EntryPath, UnavailableExecution
from e07_volatility_exits.entry_path import EntryTape, TradePrint, assemble_opening_fill
from insider_alerts.backtest.signal_study import MinuteBar
from insider_alerts.strategy.e07 import NEW_YORK

OPEN = datetime(2026, 11, 27, 9, 30, tzinfo=NEW_YORK)
CLOSE = OPEN.replace(hour=13, minute=0)
PREFIX_END = CLOSE - timedelta(minutes=2)
FILL = PREFIX_END - timedelta(seconds=10)
DELAY = timedelta(seconds=2)
ARMED = FILL + DELAY


def _tape() -> EntryTape:
    final_minute = CLOSE - timedelta(minutes=1)
    return EntryTape(
        "a" * 64,
        "ibkr:42",
        "ABC",
        OPEN.date(),
        TradePrint(FILL, 10),
        FILL.replace(second=0),
        PREFIX_END,
        (
            TradePrint(FILL, 8),
            TradePrint(ARMED, 10.1),
            TradePrint(ARMED, 9.9),
            TradePrint(ARMED + timedelta(seconds=1), 10.4),
        ),
        {
            PREFIX_END: None,
            final_minute: MinuteBar("ABC", final_minute, 10.1, 10.5, 9.6, 10.2, 5_000),
        },
        TradePrint(CLOSE + timedelta(seconds=3), 8.9),
    )


def _assemble(tape: EntryTape, delay: timedelta = DELAY) -> AuctionFill:
    return assemble_opening_fill(
        tape,
        (OPEN, CLOSE),
        protection_delay=delay,
        closing_report_grace=timedelta(seconds=3),
        close_can_trigger=False,
    )


def test_delayed_open_sparse_minutes_and_reported_early_close() -> None:
    fill = _assemble(_tape())
    assert fill.price == 10
    assert fill.filled_at == FILL and fill.protected_from == ARMED
    assert fill.first_protected_trade_at == ARMED
    assert fill.protected_through == CLOSE and fill.closing_print_at == CLOSE + timedelta(seconds=3)
    assert fill.path == EntryPath("ABC", OPEN.date(), 9.9, 10.1, 9.6, 10.5, 8.9, False)


def test_straddling_trade_bucket_fails_and_a_qualified_empty_bucket_passes() -> None:
    delay = DELAY + timedelta(milliseconds=500)
    tape = _tape()
    with pytest.raises(UnavailableExecution, match="straddling protection"):
        _assemble(tape, delay)

    after = replace(tape, prefix=tuple(tick for tick in tape.prefix if tick.at != ARMED))
    fill = _assemble(after, delay)
    assert fill.protected_from == ARMED + timedelta(milliseconds=500)
    assert fill.first_protected_trade_at == ARMED + timedelta(seconds=1)


def test_missing_minute_is_rejected_even_if_an_earlier_trade_is_present() -> None:
    tape = _tape()
    with pytest.raises(UnavailableExecution, match="complete minute grid"):
        _assemble(replace(tape, minutes={CLOSE - timedelta(minutes=1): None}))


def test_late_closing_report_needs_to_fit_declared_window() -> None:
    tape = _tape()
    with pytest.raises(UnavailableExecution, match="auction clock"):
        _assemble(replace(tape, closing=replace(tape.closing, at=CLOSE + timedelta(seconds=4))))


def test_overlapping_or_wrong_symbol_minute_is_rejected() -> None:
    tape = _tape()
    minute_at = CLOSE - timedelta(minutes=1)
    minute = tape.minutes[minute_at]
    assert minute is not None
    for invalid in (replace(minute, timestamp=PREFIX_END), replace(minute, symbol="OTHER")):
        with pytest.raises(UnavailableExecution, match="invalid minute observation"):
            _assemble(replace(tape, minutes={**tape.minutes, minute_at: invalid}))


def test_no_protected_trade_does_not_infer_an_open_from_a_minute_bar() -> None:
    tape = _tape()
    with pytest.raises(UnavailableExecution, match="missing the first protected trade"):
        _assemble(replace(tape, prefix=(TradePrint(FILL, 8),)))


def test_all_normalized_components_and_source_manifest_enter_the_digest() -> None:
    tape = _tape()
    original = _assemble(tape)
    reordered = replace(
        tape,
        prefix=tuple(reversed(tape.prefix)),
        minutes={at.astimezone(UTC): bar for at, bar in reversed(tuple(tape.minutes.items()))},
    )
    assert _assemble(reordered).source_sha256 == original.source_sha256
    assert (
        _assemble(replace(tape, manifest_sha256="b" * 64)).source_sha256 != original.source_sha256
    )
    with_extra = replace(tape, prefix=(*tape.prefix, TradePrint(FILL, 8.5)))
    assert _assemble(with_extra).source_sha256 != original.source_sha256
    assert _assemble(with_extra).path == original.path
