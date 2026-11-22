from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from e07_volatility_exits.auction import (
    AuctionFill,
    EntryConvention,
    EntryPath,
    UnavailableExecution,
)
from e07_volatility_exits.entry_path import (
    EntryTape,
    SecondPrices,
    TradePrint,
    assemble_opening_fill,
)
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
    prefix_start = FILL.replace(second=0)
    seconds: dict[datetime, SecondPrices | None] = {
        prefix_start + timedelta(seconds=index): None for index in range(60)
    }
    seconds[FILL] = SecondPrices(8, 8, 8, 8)
    seconds[ARMED] = SecondPrices(9.9, 10.1, 9.9, 10.1)
    seconds[ARMED + timedelta(seconds=1)] = SecondPrices(10.4, 10.4, 10.4, 10.4)
    return EntryTape(
        "a" * 64,
        "ibkr:42",
        "ABC",
        OPEN.date(),
        TradePrint(FILL, 10),
        prefix_start,
        PREFIX_END,
        seconds,
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
        convention=EntryConvention(delay, timedelta(seconds=3), False),
    )


def test_delayed_open_sparse_minutes_and_reported_early_close() -> None:
    fill = _assemble(_tape())
    assert fill.price == 10
    assert fill.filled_at == FILL and fill.protected_from == ARMED
    assert fill.first_protected_trade_at == ARMED
    assert fill.protected_through == CLOSE and fill.closing_print_at == CLOSE + timedelta(seconds=3)
    assert fill.path == EntryPath("ibkr:42", "ABC", OPEN.date(), 9.9, 10.1, 9.6, 10.5, 8.9)


def test_straddling_trade_bucket_fails_and_a_qualified_empty_bucket_passes() -> None:
    delay = DELAY + timedelta(milliseconds=500)
    tape = _tape()
    with pytest.raises(UnavailableExecution, match="straddling protection"):
        _assemble(tape, delay)

    after = replace(tape, seconds={**tape.seconds, ARMED: None})
    fill = _assemble(after, delay)
    assert fill.protected_from == ARMED + timedelta(milliseconds=500)
    assert fill.first_protected_trade_at == ARMED + timedelta(seconds=1)


def test_missing_minute_is_rejected_even_if_an_earlier_trade_is_present() -> None:
    tape = _tape()
    with pytest.raises(UnavailableExecution, match="complete minute grid"):
        _assemble(replace(tape, minutes={CLOSE - timedelta(minutes=1): None}))


def test_missing_second_is_distinct_from_verified_empty_bucket() -> None:
    tape = _tape()
    seconds = {at: row for at, row in tape.seconds.items() if at != ARMED}
    with pytest.raises(UnavailableExecution, match="complete second grid"):
        _assemble(replace(tape, seconds=seconds))


def test_first_price_can_be_known_inside_a_wider_second_range() -> None:
    tape = _tape()
    seconds = {**tape.seconds, ARMED: SecondPrices(10.1, 10.1, 9.8, 10.4)}
    fill = _assemble(replace(tape, seconds=seconds))
    assert fill.path.first_low == fill.path.first_high == 10.1


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


def test_malformed_source_values_fail_before_projection() -> None:
    tape = _tape()
    for invalid, message in (
        (replace(tape, instrument=""), "qualified listing"),
        (replace(tape, manifest_sha256="unbound"), "source manifest"),
        (replace(tape, opening=replace(tape.opening, price=0)), "invalid print price"),
        (replace(tape, opening=replace(tape.opening, at=FILL.replace(tzinfo=None))), "timestamps"),
        (replace(tape, prefix_start=tape.prefix_start + timedelta(microseconds=1)), "timestamps"),
        (replace(tape, prefix_end=PREFIX_END + timedelta(seconds=1)), "auction clock"),
        (
            replace(tape, seconds={**tape.seconds, ARMED: SecondPrices(9, 11, 9, 10)}),
            "second observation",
        ),
    ):
        with pytest.raises(UnavailableExecution, match=message):
            _assemble(invalid)


def test_no_protected_trade_does_not_infer_an_open_from_a_minute_bar() -> None:
    tape = _tape()
    with pytest.raises(UnavailableExecution, match="missing the first protected trade"):
        _assemble(
            replace(
                tape, seconds={at: row if at < ARMED else None for at, row in tape.seconds.items()}
            )
        )


def test_all_normalized_components_and_source_manifest_enter_the_digest() -> None:
    tape = _tape()
    original = _assemble(tape)
    reordered = replace(
        tape,
        seconds={at.astimezone(UTC): row for at, row in reversed(tuple(tape.seconds.items()))},
        minutes={at.astimezone(UTC): bar for at, bar in reversed(tuple(tape.minutes.items()))},
    )
    assert _assemble(reordered).source_sha256 == original.source_sha256
    assert (
        _assemble(replace(tape, manifest_sha256="b" * 64)).source_sha256 != original.source_sha256
    )
    with_extra = replace(tape, seconds={**tape.seconds, FILL: SecondPrices(8, 8.5, 8, 8.5)})
    assert _assemble(with_extra).source_sha256 != original.source_sha256
    assert _assemble(with_extra).path == original.path
