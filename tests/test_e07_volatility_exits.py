from __future__ import annotations

import math
import statistics
from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Literal, cast

import pytest

from e07_volatility_exits.auction import (
    Auction,
    AuctionFill,
    AuctionMiss,
    EntryConvention,
    EntryPath,
)
from e07_volatility_exits.entry_path import (
    EntryTape,
    SecondPrices,
    TradePrint,
    assemble_opening_fill,
)
from e07_volatility_exits.features import (
    BASE_WIDTH,
    FEATURE_CUTOFF,
    MAX_WIDTH,
    MIN_WIDTH,
    CloseSnapshot,
    ExitWidths,
    VolatilityEstimate,
    calibrate_exits,
    decide_widths,
    estimate_volatility,
)
from e07_volatility_exits.placebo import match_widths_within_session
from e07_volatility_exits.replay import (
    COMMISSION_CAP,
    COMMISSION_RATE_CAP,
    CUSHION,
    POLICY,
    RESERVE,
    SLOT_COUNT,
    Action,
    BarKey,
    ExitReplay,
    OrdinaryCashDistribution,
    PrimaryExitReplay,
    ReplayAgreementError,
    ReplayCandidate,
    ReplayCosts,
    UnavailableExecution,
    UnsupportedAction,
    compare_exit_widths,
    compare_primary_exit_widths,
)
from insider_alerts.backtest.models import DailyBar
from insider_alerts.execution.canary import CanaryConfig
from insider_alerts.strategy.e07 import NEW_YORK, deterministic_rank, evaluate_shadow

D = Decimal
CASH = D("475")
COSTS = ReplayCosts(D("0.35"), D("10"))


def _dates(start: date, count: int) -> tuple[date, ...]:
    # The fixture supplies the authoritative sequence, including a closed Wednesday.
    dates = (start + timedelta(days=n) for n in range(count * 3))
    return tuple(day for day in dates if day.weekday() not in {2, 5, 6})[:count]


SESSIONS = _dates(date(2026, 2, 26), 16)
SETTLEMENTS = dict(zip(SESSIONS, _dates(SESSIONS[0], 17)[1:], strict=True))
HISTORY = tuple(day for day in _dates(date(2025, 12, 1), 60) if day < SESSIONS[0])[-21:]
PREVIOUS_CLOSE = datetime.combine(HISTORY[-1], time(13), NEW_YORK)
DECISION = datetime.combine(SESSIONS[0], time(9, 18), NEW_YORK)
HOURS = {
    day: (
        datetime.combine(day, time(9, 30), NEW_YORK),
        datetime.combine(day, time(16), NEW_YORK),
    )
    for day in SESSIONS
}
DELAY = timedelta(seconds=1)
CONVENTION = EntryConvention(DELAY, timedelta(seconds=3), False)


def _snapshot() -> CloseSnapshot:
    return CloseSnapshot(
        "101",
        DECISION - timedelta(seconds=1),
        tuple((day, 100.0 + n % 4) for n, day in enumerate(HISTORY)),
    )


def _candidate(
    packet: str = "a",
    *,
    day: date = SESSIONS[0],
    instrument: str = "101",
    width: float = 0.05,
) -> ReplayCandidate:
    return ReplayCandidate(
        packet,
        "0000000001-26-000001",
        "ABC" + instrument,
        datetime.combine(day, time(9), NEW_YORK),
        instrument,
        day,
        10.0,
        ExitWidths(
            instrument, datetime.combine(day, time(9, 18), NEW_YORK), width, BASE_WIDTH, None
        ),
    )


def _bars(*candidates: ReplayCandidate) -> dict[tuple[str, date], DailyBar]:
    return {
        (candidate.instrument, day): DailyBar(candidate.symbol, day, 10, 10, 10, 10, 100_000)
        for candidate in candidates
        for day in SESSIONS
        if day >= candidate.entry_day
    }


def _action_coverage(candidates: Sequence[ReplayCandidate]) -> dict[BarKey, tuple[Action, ...]]:
    return {
        (candidate.instrument, day): ()
        for candidate in candidates
        for day in SESSIONS
        if day >= candidate.entry_day
    }


def _auctions(
    candidates: Sequence[ReplayCandidate],
    bars: Mapping[BarKey, DailyBar],
    misses: frozenset[BarKey] = frozenset(),
) -> dict[BarKey, Auction]:
    evidence: dict[BarKey, Auction] = {}
    for candidate in candidates:
        day = candidate.entry_day
        key = (candidate.instrument, day)
        if key in misses:
            evidence[key] = AuctionMiss("4" * 64)
            continue
        bar = bars.get(key, DailyBar(candidate.symbol, day, 10, 10, 10, 10, 100_000))
        opened, closed = HOURS.get(
            day,
            (
                datetime.combine(day, time(9, 30), NEW_YORK),
                datetime.combine(day, time(16), NEW_YORK),
            ),
        )
        evidence[key] = AuctionFill(
            "5" * 64,
            bar.open,
            opened,
            opened + DELAY,
            opened + DELAY,
            closed,
            closed,
            _path(candidate.instrument, bar),
        )
    return evidence


def _path(instrument: str, bar: DailyBar) -> EntryPath:
    return EntryPath(
        instrument, bar.symbol, bar.trade_date, bar.open, bar.open, bar.low, bar.high, bar.close
    )


def test_auction_basis_and_protection_exclude_earlier_prints() -> None:
    candidate = _candidate()
    day = candidate.entry_day
    key = (candidate.instrument, day)
    bars = _bars(candidate)
    bars[key] = DailyBar(candidate.symbol, day, 11, 12, 8, 10, 100_000)
    start, end = HOURS[day]
    fill = start + timedelta(minutes=4)
    protected_bar = DailyBar(candidate.symbol, day, 10.1, 10.4, 9.8, 10, 50_000)
    replay = compare_exit_widths(
        SESSIONS,
        SETTLEMENTS,
        [candidate],
        bars,
        action_coverage=_action_coverage([candidate]),
        opening_evidence={
            key: AuctionFill(
                "5" * 64,
                10,
                fill,
                fill + DELAY,
                fill + DELAY,
                end,
                end,
                _path(candidate.instrument, protected_bar),
            )
        },
        session_hours=HOURS,
        entry_convention=CONVENTION,
        initial_cash=CASH,
        costs=COSTS,
        previous_session=HISTORY[-1],
    )
    assert replay.scaled.sessions[0].settled_cash == D("274.45")
    assert replay.scaled.sessions[0].exits == ()
    assert replay.scaled.sessions[9].exits[0].reason == "time"
    assert replay.scaled.net_pnl == replay.fixed.net_pnl == replay.matched.net_pnl == D("-1.10")


@pytest.mark.parametrize(("first", "reason", "pnl"), [(8.8, "stop", "-24"), (11.2, "target", "24")])
@pytest.mark.parametrize("collision_rule", ["stop_first", "target_first"])
def test_first_protected_print_sets_the_gap_exit(
    first: float, reason: str, pnl: str, collision_rule: Literal["stop_first", "target_first"]
) -> None:
    candidate = _candidate()
    day = candidate.entry_day
    key = (candidate.instrument, day)
    start, end = HOURS[day]
    protected = DailyBar(candidate.symbol, day, first, 12, 8, 10, 50_000)
    replay = compare_exit_widths(
        SESSIONS,
        SETTLEMENTS,
        [candidate],
        _bars(candidate),
        action_coverage=_action_coverage([candidate]),
        opening_evidence={
            key: AuctionFill(
                "5" * 64,
                10,
                start,
                start + DELAY,
                start + DELAY,
                end,
                end,
                _path(candidate.instrument, protected),
            )
        },
        session_hours=HOURS,
        entry_convention=CONVENTION,
        initial_cash=CASH,
        costs=ReplayCosts(D("0"), D("0")),
        previous_session=HISTORY[-1],
        collision=collision_rule,
    )
    assert replay.fixed.sessions[0].exits[0].reason == reason
    assert replay.fixed.sessions[0].exits[0].same_bar_collision
    assert not replay.fixed.sessions[0].exits[0].ambiguous_collision
    assert replay.fixed.net_pnl == D(pnl)


def test_early_close_and_protection_are_checked_against_the_session_clock() -> None:
    candidate = _candidate()
    day = candidate.entry_day
    key = (candidate.instrument, day)
    bars = _bars(candidate)
    start, end = HOURS[day]
    evidence = AuctionFill(
        "5" * 64,
        10,
        start,
        start + DELAY,
        start + DELAY,
        end,
        end,
        _path(candidate.instrument, bars[key]),
    )
    early_close = end - timedelta(hours=3)
    early = replace(evidence, protected_through=early_close, closing_print_at=early_close)
    variants = [
        (evidence, "timing"),
        (
            replace(early, protected_through=early_close - timedelta(seconds=1)),
            "timing",
        ),
        (
            replace(
                early,
                protected_from=start + timedelta(seconds=2),
                first_protected_trade_at=start + timedelta(seconds=3),
            ),
            "timing",
        ),
        (
            replace(early, first_protected_trade_at=start),
            "timing",
        ),
        (
            replace(early, filled_at=start.replace(tzinfo=None)),
            "aware",
        ),
        (replace(early, closing_print_at=early_close + timedelta(seconds=4)), "timing"),
        (replace(early, closing_print_at=early_close - timedelta(seconds=1)), "timing"),
        (replace(early, path=replace(early.path, instrument="other-listing")), "protected path"),
        (replace(early, path=replace(early.path, low=100)), "protected path"),
        (AuctionMiss("unbound"), "source digest"),
    ]
    for record, expected in variants:
        with pytest.raises(UnavailableExecution, match=expected):
            compare_exit_widths(
                SESSIONS,
                SETTLEMENTS,
                [candidate],
                bars,
                action_coverage=_action_coverage([candidate]),
                opening_evidence={key: record},
                session_hours=HOURS | {day: (start.astimezone(UTC), early_close.astimezone(UTC))},
                entry_convention=CONVENTION,
                initial_cash=CASH,
                costs=COSTS,
                previous_session=HISTORY[-1],
            )


@pytest.mark.parametrize("compare", [compare_exit_widths, compare_primary_exit_widths])
def test_unknown_auction_fails_even_without_cash_to_enter(
    compare: Callable[..., ExitReplay | PrimaryExitReplay],
) -> None:
    candidate = _candidate()
    with pytest.raises(UnavailableExecution, match="missing or invalid opening evidence"):
        compare(
            SESSIONS,
            SETTLEMENTS,
            [candidate],
            _bars(candidate),
            action_coverage=_action_coverage([candidate]),
            opening_evidence={},
            session_hours=HOURS,
            entry_convention=CONVENTION,
            initial_cash=D("60"),
            costs=COSTS,
            previous_session=HISTORY[-1],
        )


@pytest.mark.parametrize("compare", [compare_exit_widths, compare_primary_exit_widths])
def test_holding_coverage_is_required_before_an_early_exit(
    compare: Callable[..., ExitReplay | PrimaryExitReplay],
) -> None:
    candidate = _candidate()
    bars = _bars(candidate)
    key = candidate.instrument, candidate.entry_day
    bars[key] = replace(bars[key], high=12)
    del bars[candidate.instrument, SESSIONS[9]]
    with pytest.raises(UnavailableExecution, match="missing or invalid execution bar"):
        compare(
            SESSIONS,
            SETTLEMENTS,
            [candidate],
            bars,
            action_coverage=_action_coverage([candidate]),
            opening_evidence=_auctions([candidate], bars),
            session_hours=HOURS,
            entry_convention=CONVENTION,
            initial_cash=CASH,
            costs=COSTS,
            previous_session=HISTORY[-1],
        )


def test_old_and_new_positions_use_one_session_close_mark() -> None:
    previous = _candidate()
    arrival = _candidate("b", day=SESSIONS[2], instrument=previous.instrument)
    cases = [previous, arrival]
    bars = _bars(*cases)
    opening = _auctions(cases, bars)
    key = (arrival.instrument, arrival.entry_day)
    fill = opening[key]
    assert isinstance(fill, AuctionFill)
    opening[key] = replace(fill, path=replace(fill.path, close=10.1))
    with pytest.raises(UnavailableExecution, match="entry and holding marks disagree"):
        compare_exit_widths(
            SESSIONS,
            SETTLEMENTS,
            cases,
            bars,
            action_coverage=_action_coverage(cases),
            opening_evidence=opening,
            session_hours=HOURS,
            entry_convention=CONVENTION,
            initial_cash=CASH,
            costs=COSTS,
            previous_session=HISTORY[-1],
        )


@pytest.mark.parametrize(
    ("can_trigger", "trade_price", "closing_price", "expected_exit", "expected_equity"),
    [
        (False, 10, 8, None, "435"),
        (True, 10, 8, "closing_stop", "435"),
        (True, 10, 12, "closing_target", "515"),
        (True, 11.2, 8, "target", "495"),
    ],
)
def test_projected_auction_mark_is_applied_after_continuous_prices(
    can_trigger: bool,
    trade_price: float,
    closing_price: float,
    expected_exit: str | None,
    expected_equity: str,
) -> None:
    candidate = _candidate(width=BASE_WIDTH)
    day = candidate.entry_day
    start, close = HOURS[day]
    fill = start + timedelta(seconds=4)
    prefix_end = start + timedelta(minutes=1)
    minutes = {prefix_end + timedelta(minutes=index): None for index in range(389)}
    seconds: dict[datetime, SecondPrices | None] = {
        start + timedelta(seconds=index): None for index in range(60)
    }
    seconds[fill] = SecondPrices(8, 8, 8, 8)
    seconds[fill + DELAY] = SecondPrices(9.95, 10, 9.95, 10)
    seconds[fill + timedelta(seconds=2)] = SecondPrices(
        trade_price, trade_price, trade_price, trade_price
    )
    convention = replace(CONVENTION, close_can_trigger=can_trigger)
    tape = EntryTape(
        "9" * 64,
        candidate.instrument,
        candidate.symbol,
        day,
        TradePrint(fill, 10),
        start,
        prefix_end,
        seconds,
        minutes,
        TradePrint(close + timedelta(seconds=2), closing_price),
    )
    evidence = assemble_opening_fill(
        tape,
        HOURS[day],
        convention=convention,
    )
    result = compare_exit_widths(
        SESSIONS,
        SETTLEMENTS,
        [candidate],
        _bars(candidate),
        action_coverage=_action_coverage([candidate]),
        opening_evidence={(candidate.instrument, day): evidence},
        session_hours=HOURS,
        entry_convention=convention,
        initial_cash=CASH,
        costs=ReplayCosts(D("0"), D("0")),
        previous_session=HISTORY[-1],
    ).fixed.sessions[0]
    assert result.equity == D(expected_equity)
    assert [exit.reason for exit in result.exits] == (
        [] if expected_exit is None else [expected_exit]
    )
    assert not any(exit.same_bar_collision for exit in result.exits)


@pytest.mark.parametrize("compare", [compare_exit_widths, compare_primary_exit_widths])
def test_ambiguous_first_second_cannot_touch_a_barrier_even_when_entry_is_suppressed(
    compare: Callable[..., ExitReplay | PrimaryExitReplay],
) -> None:
    candidate = _candidate()
    bars = _bars(candidate)
    opening = _auctions([candidate], bars)
    key = candidate.instrument, candidate.entry_day
    fill = opening[key]
    assert isinstance(fill, AuctionFill)
    opening[key] = replace(fill, path=replace(fill.path, low=9.5, first_low=9.5))
    with pytest.raises(UnavailableExecution, match="first protected second can touch a barrier"):
        compare(
            SESSIONS,
            SETTLEMENTS,
            [candidate],
            bars,
            action_coverage=_action_coverage([candidate]),
            opening_evidence=opening,
            session_hours=HOURS,
            entry_convention=CONVENTION,
            initial_cash=D("60"),
            costs=COSTS,
            previous_session=HISTORY[-1],
        )


@pytest.mark.parametrize("hit_session", [0, 1, 8, 9, 10])
def test_coincident_fill_and_protection_price_preserve_fixed_stop_first(hit_session: int) -> None:
    candidate = _candidate()
    bars = _bars(candidate)
    key = candidate.instrument, SESSIONS[hit_session]
    bars[key] = replace(bars[key], low=8.8, high=11.1)
    ordered = [bars[candidate.instrument, day] for day in SESSIONS]
    expected = evaluate_shadow(POLICY, ordered, candidate.entry_day)
    assert expected.exit_bar is not None and expected.exit_price is not None

    result = compare_exit_widths(
        SESSIONS,
        SETTLEMENTS,
        [candidate],
        bars,
        action_coverage=_action_coverage([candidate]),
        opening_evidence=_auctions([candidate], bars),
        session_hours=HOURS,
        entry_convention=CONVENTION,
        initial_cash=CASH,
        costs=ReplayCosts(D("0"), D("0")),
        previous_session=HISTORY[-1],
    ).fixed
    assert (
        result.sessions[SESSIONS.index(expected.exit_bar.trade_date)].exits[0].reason
        == expected.exit_reason
    )
    assert result.net_pnl == (D(str(expected.exit_price)) - D("10")) * 20


def test_digest_binds_holding_prices_and_opening_source() -> None:
    candidate = _candidate()
    bars = _bars(candidate)
    opening = _auctions([candidate], bars)
    key = candidate.instrument, candidate.entry_day
    evidence = opening[key]
    assert isinstance(evidence, AuctionFill)

    def run(convention: EntryConvention = CONVENTION) -> ExitReplay:
        return compare_exit_widths(
            SESSIONS,
            SETTLEMENTS,
            [candidate],
            bars,
            action_coverage=_action_coverage([candidate]),
            opening_evidence=opening,
            session_hours=HOURS,
            entry_convention=convention,
            initial_cash=CASH,
            costs=COSTS,
            previous_session=HISTORY[-1],
        )

    prior = run()
    opening[key] = replace(evidence, source_sha256="6" * 64)
    new_source = run()
    assert prior.fixed == new_source.fixed
    assert prior.action_coverage_sha256 == new_source.action_coverage_sha256
    assert prior.execution_evidence_sha256 != new_source.execution_evidence_sha256
    for alternate in (
        replace(CONVENTION, close_can_trigger=True),
        replace(CONVENTION, closing_report_grace=timedelta(seconds=4)),
    ):
        changed = run(alternate)
        assert changed.fixed == new_source.fixed
        assert changed.execution_evidence_sha256 != new_source.execution_evidence_sha256
    holding_key = candidate.instrument, SESSIONS[1]
    bars[holding_key] = replace(bars[holding_key], volume=10)
    assert new_source.execution_evidence_sha256 != run().execution_evidence_sha256


def test_distribution_follows_overnight_ownership_even_with_an_ex_date_exit() -> None:
    candidate = _candidate()
    bars = _bars(candidate)
    open_day, ex_day = SESSIONS[:2]
    bars[candidate.instrument, open_day] = DailyBar(
        candidate.symbol, open_day, 10, 10.6, 9.9, 10, 100_000
    )
    bars[candidate.instrument, ex_day] = DailyBar(candidate.symbol, ex_day, 9, 9.1, 8.9, 9, 100_000)
    coverage = _action_coverage([candidate])
    payment = SESSIONS[-1] + timedelta(days=5)
    coverage[candidate.instrument, open_day] = (
        OrdinaryCashDistribution("entry-ex", D("0.10"), payment, payment),
    )
    coverage[candidate.instrument, ex_day] = (
        OrdinaryCashDistribution("held-ex", D("0.12675"), payment, payment),
    )

    replay = compare_exit_widths(
        SESSIONS,
        SETTLEMENTS,
        [candidate],
        bars,
        action_coverage=coverage,
        opening_evidence=_auctions([candidate], bars),
        session_hours=HOURS,
        entry_convention=CONVENTION,
        initial_cash=CASH,
        costs=COSTS,
        previous_session=HISTORY[-1],
    )

    assert replay.scaled.sessions[0].exits[0].cash_distributions == D("0")
    assert replay.fixed.sessions[0].distribution_receivable == D("0")
    exit_on_ex = replay.fixed.sessions[1].exits[0]
    assert exit_on_ex.reason == "stop"
    assert exit_on_ex.cash_distributions == D("2.54")
    assert replay.fixed.sessions[-1].distribution_receivable == D("2.54")
    assert replay.fixed.sessions[-1].unsettled_cash == D("2.54")
    assert replay.scaled.sessions[-1].distribution_receivable == D("0")
    assert replay.fixed.net_pnl == exit_on_ex.net_pnl == D("-18.54")
    assert replay.fixed == replay.matched


def test_distribution_cannot_fund_an_entry_before_its_preopen_release() -> None:
    first = _candidate(width=BASE_WIDTH)
    before_release = _candidate("b", day=SESSIONS[2], instrument="102")
    after_release = _candidate("c", day=SESSIONS[3], instrument="103")
    candidates = [first, before_release, after_release]
    bars = _bars(*candidates)
    ex_day = SESSIONS[1]
    bars[first.instrument, ex_day] = DailyBar(first.symbol, ex_day, 9, 9, 9, 9, 100_000)
    coverage = _action_coverage(candidates)
    coverage[first.instrument, ex_day] = (
        OrdinaryCashDistribution("quarterly", D("0.10"), SESSIONS[2], SESSIONS[3]),
    )

    fixed = compare_exit_widths(
        SESSIONS,
        SETTLEMENTS,
        candidates,
        bars,
        action_coverage=coverage,
        opening_evidence=_auctions(candidates, bars),
        session_hours=HOURS,
        entry_convention=CONVENTION,
        initial_cash=D("281.75"),
        costs=COSTS,
        previous_session=HISTORY[-1],
    ).fixed

    assert fixed.sessions[2].entries[0].state == "cash_suppressed"
    assert fixed.sessions[2].settled_cash == D("260.67")
    assert fixed.sessions[2].distribution_receivable == D("2")
    assert fixed.sessions[3].entries[0].state == "opened"
    assert fixed.sessions[3].distribution_receivable == D("0")
    assert fixed.net_pnl == sum(exit.net_pnl for day in fixed.sessions for exit in day.exits)


@pytest.mark.parametrize("compare", [compare_exit_widths, compare_primary_exit_widths])
def test_action_preflight_requires_complete_windows_independent_of_execution(
    compare: Callable[..., ExitReplay | PrimaryExitReplay],
) -> None:
    candidate = _candidate()
    coverage = _action_coverage([candidate])
    del coverage[candidate.instrument, SESSIONS[9]]
    with pytest.raises(UnavailableExecution, match="missing corporate-action coverage"):
        compare(
            SESSIONS,
            SETTLEMENTS,
            [candidate],
            {},
            action_coverage=coverage,
            opening_evidence=_auctions(
                [candidate], {}, frozenset({(candidate.instrument, candidate.entry_day)})
            ),
            session_hours=HOURS,
            entry_convention=CONVENTION,
            initial_cash=CASH,
            costs=COSTS,
            previous_session=HISTORY[-1],
        )


@pytest.mark.parametrize("offset", [0, 1, 5])
def test_unsupported_listing_event_blocks_a_possible_position_before_fills(offset: int) -> None:
    candidate = _candidate()
    coverage = _action_coverage([candidate])
    coverage[candidate.instrument, SESSIONS[offset]] = (
        UnsupportedAction("split", "share basis changed"),
    )
    with pytest.raises(UnavailableExecution, match="unsupported corporate action.*split"):
        compare_exit_widths(
            SESSIONS,
            SETTLEMENTS,
            [candidate],
            {},
            action_coverage=coverage,
            opening_evidence=_auctions([candidate], {}),
            session_hours=HOURS,
            entry_convention=CONVENTION,
            initial_cash=CASH,
            costs=COSTS,
            previous_session=HISTORY[-1],
        )

    replay = compare_exit_widths(
        SESSIONS,
        SETTLEMENTS,
        [candidate],
        {},
        action_coverage=coverage,
        opening_evidence=_auctions(
            [candidate], {}, frozenset({(candidate.instrument, candidate.entry_day)})
        ),
        session_hours=HOURS,
        entry_convention=CONVENTION,
        initial_cash=CASH,
        costs=COSTS,
        previous_session=HISTORY[-1],
    )
    assert replay.fixed.net_pnl == D("0")


def test_coverage_digest_is_order_independent_and_binds_dividend_terms() -> None:
    candidate = _candidate()
    coverage = _action_coverage([candidate])
    key = candidate.instrument, SESSIONS[1]
    first = OrdinaryCashDistribution("first", D("0.10"), SESSIONS[4], SESSIONS[5])
    second = replace(first, event_id="second", cash_per_share=D("0.01"))
    coverage[key] = (first, second)

    def result() -> ExitReplay:
        return compare_exit_widths(
            SESSIONS,
            SETTLEMENTS,
            [candidate],
            _bars(candidate),
            action_coverage=coverage,
            opening_evidence=_auctions([candidate], _bars(candidate)),
            session_hours=HOURS,
            entry_convention=CONVENTION,
            initial_cash=CASH,
            costs=COSTS,
            previous_session=HISTORY[-1],
        )

    original = result()
    coverage[key] = (second, first)
    assert original == result()
    assert original.fixed.sessions[1].distribution_receivable == D("2.20")
    coverage[key] = (second, replace(first, cash_per_share=D("1E-1")))
    assert original == result()
    coverage[key] = (second, replace(first, cash_available_on=SESSIONS[6]))
    assert original.action_coverage_sha256 != result().action_coverage_sha256


def test_duplicate_cash_event_across_ex_dates_is_rejected_before_replay() -> None:
    candidate = _candidate()
    coverage = _action_coverage([candidate])
    event = OrdinaryCashDistribution("id", D("0.10"), SESSIONS[3], SESSIONS[4])
    coverage[candidate.instrument, SESSIONS[1]] = (event,)
    coverage[candidate.instrument, SESSIONS[2]] = (event,)
    with pytest.raises(ValueError, match="duplicate distribution event"):
        compare_exit_widths(
            SESSIONS,
            SETTLEMENTS,
            [candidate],
            {},
            action_coverage=coverage,
            opening_evidence=_auctions([candidate], {}),
            session_hours=HOURS,
            entry_convention=CONVENTION,
            initial_cash=CASH,
            costs=COSTS,
            previous_session=HISTORY[-1],
        )


def test_action_preflight_rejects_unknown_event_types() -> None:
    candidate = _candidate()
    coverage = _action_coverage([candidate])
    coverage[candidate.instrument, candidate.entry_day] = (cast(Action, object()),)
    with pytest.raises(ValueError, match="unknown corporate-action type"):
        compare_exit_widths(
            SESSIONS,
            SETTLEMENTS,
            [candidate],
            {},
            action_coverage=coverage,
            opening_evidence=_auctions([candidate], {}),
            session_hours=HOURS,
            entry_convention=CONVENTION,
            initial_cash=CASH,
            costs=COSTS,
            previous_session=HISTORY[-1],
        )


def test_cash_availability_requires_pay_date_and_an_open_session() -> None:
    with pytest.raises(ValueError, match="invalid ordinary cash distribution"):
        OrdinaryCashDistribution("id", D("0.10"), SESSIONS[2], SESSIONS[1])
    candidate = _candidate()
    coverage = _action_coverage([candidate])
    closed_day = SESSIONS[1] + timedelta(days=1)
    assert closed_day not in SESSIONS
    for event, reason in (
        (
            OrdinaryCashDistribution("id", D("0.10"), SESSIONS[0], SESSIONS[0]),
            "invalid distribution timing",
        ),
        (
            OrdinaryCashDistribution("id", D("0.10"), SESSIONS[1], closed_day),
            "requires an exchange session",
        ),
    ):
        coverage[candidate.instrument, SESSIONS[0]] = (event,)
        with pytest.raises(ValueError, match=reason):
            compare_exit_widths(
                SESSIONS,
                SETTLEMENTS,
                [candidate],
                {},
                action_coverage=coverage,
                opening_evidence=_auctions([candidate], {}),
                session_hours=HOURS,
                entry_convention=CONVENTION,
                initial_cash=CASH,
                costs=COSTS,
                previous_session=HISTORY[-1],
            )


def test_volatility_uses_twenty_adjacent_log_returns_from_one_vintage() -> None:
    snapshot = _snapshot()
    prices = tuple(close for _, close in snapshot.closes)
    expected = statistics.stdev(math.log(b / a) for a, b in zip(prices, prices[1:], strict=False))
    estimate = estimate_volatility(
        "101", DECISION, HISTORY, snapshot, previous_session_close=PREVIOUS_CLOSE
    )
    assert estimate.daily == pytest.approx(expected)
    assert estimate.missing is None
    multiplied = replace(snapshot, closes=tuple((day, px * 100) for day, px in snapshot.closes))
    assert estimate_volatility(
        "101", DECISION, HISTORY, multiplied, previous_session_close=PREVIOUS_CLOSE
    ).daily == pytest.approx(expected)


def test_missing_volatility_preserves_fixed_width_in_both_challengers() -> None:
    snapshot = _snapshot()
    invalid_close = replace(snapshot, closes=((HISTORY[0], math.nan), *snapshot.closes[1:]))
    flat = replace(snapshot, closes=tuple((day, 15.0) for day in HISTORY))
    calibration = calibrate_exits([0.01, 0.02, 0.04])
    for bad, reason in (
        (None, "no_snapshot"),
        (replace(snapshot, instrument="102"), "instrument_mismatch"),
        (replace(snapshot, received_at=DECISION.astimezone(UTC)), "late_snapshot"),
        (replace(snapshot, received_at=PREVIOUS_CLOSE), "invalid_snapshot_clock"),
        (
            replace(snapshot, received_at=DECISION.replace(tzinfo=None)),
            "invalid_snapshot_clock",
        ),
        (replace(snapshot, closes=snapshot.closes[:-1]), "missing_or_unexpected_sessions"),
        (invalid_close, "invalid_close"),
        (flat, "zero_volatility"),
    ):
        estimate = estimate_volatility(
            "101", DECISION, HISTORY, bad, previous_session_close=PREVIOUS_CLOSE
        )
        assert estimate.daily is None
        assert estimate.missing == reason
        assert decide_widths(calibration, estimate, "packet") == ExitWidths(
            "101", DECISION, BASE_WIDTH, BASE_WIDTH, reason
        )


def test_bad_calendar_and_unfittable_reference_are_errors() -> None:
    with pytest.raises(ValueError, match="ordered calendar"):
        estimate_volatility(
            "101",
            DECISION,
            (*HISTORY[:-1], SESSIONS[0]),
            _snapshot(),
            previous_session_close=PREVIOUS_CLOSE,
        )
    with pytest.raises(ValueError, match="positive development"):
        calibrate_exits([0.0])


def test_width_calibration_is_deterministic_and_bounded() -> None:
    fitted = calibrate_exits([1, 0.01, 0.02])
    assert fitted.daily_reference == 0.02
    assert fitted.matched_widths == (MIN_WIDTH, BASE_WIDTH, MAX_WIDTH)
    quiet = decide_widths(fitted, VolatilityEstimate("101", DECISION, 0.001, None), "packet")
    volatile = decide_widths(fitted, VolatilityEstimate("101", DECISION, 0.5, None), "packet")
    assert quiet.scaled == MIN_WIDTH
    assert volatile.scaled == MAX_WIDTH
    assert quiet.matched == volatile.matched
    assert quiet.matched in fitted.matched_widths
    assert fitted == calibrate_exits([0.02, 1, 0.01])


def test_replay_parameters_match_frozen_live_defaults() -> None:
    live = CanaryConfig(source_db="unused.db")
    for key in (
        "lottery_salt",
        "entry_submission_deadline",
        "min_price",
        "max_price",
        "min_median_dollar_volume_20d",
        "slot_budget",
        "stop_loss_pct",
        "take_profit_pct",
        "max_sessions",
    ):
        assert getattr(POLICY, key) == getattr(live, key)
    assert live.max_live_slots == SLOT_COUNT
    assert live.entry_submission_start == FEATURE_CUTOFF
    assert float(RESERVE) == live.cash_reserve
    assert float(CUSHION) - 1 == pytest.approx(live.market_order_cushion)
    assert float(COMMISSION_CAP) == live.max_one_way_commission
    assert float(COMMISSION_RATE_CAP) * 10000 == live.max_one_way_commission_bps


def test_replay_measures_after_cost_incremental_pnl_and_settlement() -> None:
    candidate = _candidate()
    bars = _bars(candidate)
    day = SESSIONS[0]
    bars[(candidate.instrument, day)] = DailyBar(candidate.symbol, day, 10, 10.6, 9.6, 10, 100_000)
    result = compare_exit_widths(
        SESSIONS,
        SETTLEMENTS,
        [candidate],
        bars,
        action_coverage=_action_coverage([candidate]),
        opening_evidence=_auctions([candidate], bars),
        session_hours=HOURS,
        entry_convention=CONVENTION,
        initial_cash=CASH,
        costs=COSTS,
        previous_session=HISTORY[-1],
    )
    assert result.scaled.sessions[0].exits[0].reason == "target"
    assert result.scaled.sessions[0].settled_cash == D("274.45")
    assert result.scaled.sessions[0].unsettled_cash == D("209.44")
    assert result.scaled.sessions[1].settled_cash == D("483.89")
    assert result.fixed.sessions[9].exits[0].reason == "time"
    assert result.fixed.net_pnl == D("-1.10")
    assert result.scaled.net_pnl == D("8.89")
    assert result.matched == result.fixed
    assert sum(result.paired_daily_pnl) == sum(result.scaled_vs_matched_daily_pnl) == D("9.99")


def test_collision_stress_keeps_an_open_through_the_stop() -> None:
    candidate = _candidate()
    bars = _bars(candidate)
    day = SESSIONS[1]
    bars[(candidate.instrument, day)] = DailyBar(candidate.symbol, day, 9, 11, 8, 10, 100_000)
    primary = compare_exit_widths(
        SESSIONS,
        SETTLEMENTS,
        [candidate],
        bars,
        action_coverage=_action_coverage([candidate]),
        opening_evidence=_auctions([candidate], bars),
        session_hours=HOURS,
        entry_convention=CONVENTION,
        initial_cash=CASH,
        costs=COSTS,
        previous_session=HISTORY[-1],
    )
    stress = compare_exit_widths(
        SESSIONS,
        SETTLEMENTS,
        [candidate],
        bars,
        action_coverage=_action_coverage([candidate]),
        opening_evidence=_auctions([candidate], bars),
        session_hours=HOURS,
        entry_convention=CONVENTION,
        initial_cash=CASH,
        costs=COSTS,
        previous_session=HISTORY[-1],
        collision="target_first",
    )
    assert primary.fixed.net_pnl == primary.scaled.net_pnl == D("-21.08")
    assert primary.scaled.sessions[1].exits[0].same_bar_collision
    assert stress.fixed.net_pnl == stress.scaled.net_pnl == D("-21.08")
    assert stress.fixed.sessions[1].exits[0].same_bar_collision
    assert not stress.fixed.sessions[1].exits[0].ambiguous_collision


def test_no_close_exit_releases_a_slot_for_that_mornings_auction() -> None:
    first = _candidate()
    second = _candidate("b", instrument="102")
    auction_too_early = _candidate("c", day=SESSIONS[1], instrument="103")
    auction_after_settlement = _candidate("d", day=SESSIONS[2], instrument="104")
    candidates = [first, second, auction_too_early, auction_after_settlement]
    bars = _bars(*candidates)
    day = SESSIONS[1]
    bars[(first.instrument, day)] = DailyBar(first.symbol, day, 10, 11, 10, 10, 100_000)
    fixed = compare_exit_widths(
        SESSIONS,
        SETTLEMENTS,
        candidates,
        bars,
        action_coverage=_action_coverage(candidates),
        opening_evidence=_auctions(candidates, bars),
        session_hours=HOURS,
        entry_convention=CONVENTION,
        initial_cash=CASH,
        costs=COSTS,
        previous_session=HISTORY[-1],
    ).fixed
    assert fixed.sessions[1].entries[0].state == "capacity_suppressed"
    assert fixed.sessions[1].exits[0].reason == "target"
    assert fixed.sessions[2].entries[0].state == "opened"


def test_no_auction_fill_does_not_reassign_the_slot_with_hindsight() -> None:
    candidates = [_candidate(str(n), instrument=str(n)) for n in range(1, 4)]
    first, second, third = sorted(candidates, key=_rank)
    bars = _bars(second, third)
    misses = frozenset({(first.instrument, first.entry_day)})
    fixed = compare_exit_widths(
        SESSIONS,
        SETTLEMENTS,
        candidates,
        bars,
        action_coverage=_action_coverage(candidates),
        opening_evidence=_auctions(candidates, bars, misses),
        session_hours=HOURS,
        entry_convention=CONVENTION,
        initial_cash=CASH,
        costs=COSTS,
        previous_session=HISTORY[-1],
    ).fixed
    assert [(entry.packet_id, entry.state) for entry in fixed.sessions[0].entries] == [
        (first.packet_id, "no_auction_fill"),
        (second.packet_id, "opened"),
        (third.packet_id, "capacity_suppressed"),
    ]


def _rank(row: ReplayCandidate) -> str:
    return deterministic_rank(POLICY, row, row.entry_day)


def test_missing_held_bar_is_not_a_zero_return_or_a_delayed_exit() -> None:
    candidate = _candidate()
    bars = _bars(candidate)
    del bars[(candidate.instrument, SESSIONS[1])]
    with pytest.raises(UnavailableExecution, match=SESSIONS[1].isoformat()):
        compare_exit_widths(
            SESSIONS,
            SETTLEMENTS,
            [candidate],
            bars,
            action_coverage=_action_coverage([candidate]),
            opening_evidence=_auctions([candidate], bars),
            session_hours=HOURS,
            entry_convention=CONVENTION,
            initial_cash=CASH,
            costs=COSTS,
            previous_session=HISTORY[-1],
        )


def test_replay_rejects_after_cutoff_candidate_and_missing_settlement() -> None:
    candidate = _candidate()
    late = replace(candidate, signal_at=datetime.combine(SESSIONS[0], time(9, 20), NEW_YORK))
    with pytest.raises(ValueError, match="invalid replay candidate"):
        compare_exit_widths(
            SESSIONS,
            SETTLEMENTS,
            [late],
            _bars(candidate),
            action_coverage=_action_coverage([late]),
            opening_evidence=_auctions([late], _bars(candidate)),
            session_hours=HOURS,
            entry_convention=CONVENTION,
            initial_cash=CASH,
            costs=COSTS,
            previous_session=HISTORY[-1],
        )
    later = replace(candidate, entry_day=SESSIONS[1])
    with pytest.raises(ValueError, match="invalid replay candidate"):
        compare_exit_widths(
            SESSIONS,
            SETTLEMENTS,
            [later],
            _bars(candidate),
            action_coverage=_action_coverage([later]),
            opening_evidence=_auctions([later], _bars(candidate)),
            session_hours=HOURS,
            entry_convention=CONVENTION,
            initial_cash=CASH,
            costs=COSTS,
            previous_session=HISTORY[-1],
        )
    with pytest.raises(ValueError, match="invalid replay calendar"):
        compare_exit_widths(
            SESSIONS,
            {},
            [],
            {},
            action_coverage={},
            opening_evidence={},
            session_hours=HOURS,
            entry_convention=CONVENTION,
            initial_cash=CASH,
            costs=COSTS,
            previous_session=HISTORY[-1],
        )


def test_sale_proceeds_wait_for_the_supplied_settlement_date() -> None:
    initial = _candidate()
    before_settlement = _candidate("b", day=SESSIONS[1], instrument="102")
    after_settlement = _candidate("c", day=SESSIONS[2], instrument="103")
    candidates = [initial, before_settlement, after_settlement]
    bars = _bars(*candidates)
    first = SESSIONS[0]
    bars[(initial.instrument, first)] = DailyBar(initial.symbol, first, 10, 11, 10, 11, 100_000)
    settlements = SETTLEMENTS | {first: SESSIONS[2]}
    fixed = compare_exit_widths(
        SESSIONS,
        settlements,
        candidates,
        bars,
        action_coverage=_action_coverage(candidates),
        opening_evidence=_auctions(candidates, bars),
        session_hours=HOURS,
        entry_convention=CONVENTION,
        initial_cash=D("265"),
        costs=COSTS,
        previous_session=HISTORY[-1],
    ).fixed
    assert fixed.sessions[0].open_positions == 0
    assert fixed.sessions[1].unsettled_cash == fixed.sessions[0].unsettled_cash
    assert fixed.sessions[1].entries[0].state == "cash_suppressed"
    assert fixed.sessions[2].entries[0].state == "opened"


def test_collision_stress_exercises_a_genuinely_ambiguous_bar() -> None:
    candidate = _candidate()
    bars = _bars(candidate)
    day = SESSIONS[1]
    bars[(candidate.instrument, day)] = DailyBar(candidate.symbol, day, 10, 11, 8, 10, 100_000)
    stress = compare_exit_widths(
        SESSIONS,
        SETTLEMENTS,
        [candidate],
        bars,
        action_coverage=_action_coverage([candidate]),
        opening_evidence=_auctions([candidate], bars),
        session_hours=HOURS,
        entry_convention=CONVENTION,
        initial_cash=CASH,
        costs=COSTS,
        previous_session=HISTORY[-1],
        collision="target_first",
    )
    assert stress.fixed.net_pnl == D("18.88")
    assert stress.scaled.net_pnl == D("8.89")
    assert stress.fixed.sessions[1].exits[0].ambiguous_collision


def test_unsupported_gap_raises_instead_of_guessing_a_broker_rejection() -> None:
    candidate = _candidate()
    bars = _bars(candidate)
    day = SESSIONS[0]
    bars[(candidate.instrument, day)] = DailyBar(candidate.symbol, day, 20, 20, 20, 20, 100_000)
    with pytest.raises(UnavailableExecution, match="fills exceed settled cash"):
        compare_exit_widths(
            SESSIONS,
            SETTLEMENTS,
            [candidate],
            bars,
            action_coverage=_action_coverage([candidate]),
            opening_evidence=_auctions([candidate], bars),
            session_hours=HOURS,
            entry_convention=CONVENTION,
            initial_cash=D("265"),
            costs=COSTS,
            previous_session=HISTORY[-1],
        )


def test_opening_execution_can_use_the_cushion_or_exceed_the_reserve() -> None:
    candidate = _candidate()
    bars = _bars(candidate)
    day = SESSIONS[0]
    bars[(candidate.instrument, day)] = DailyBar(candidate.symbol, day, 11, 11, 11, 11, 100_000)
    result = compare_exit_widths(
        SESSIONS,
        SETTLEMENTS,
        [candidate],
        bars,
        action_coverage=_action_coverage([candidate]),
        opening_evidence=_auctions([candidate], bars),
        session_hours=HOURS,
        entry_convention=CONVENTION,
        initial_cash=D("265"),
        costs=COSTS,
        previous_session=HISTORY[-1],
    )
    assert result.fixed.sessions[0].entries[0].state == "opened"
    assert result.fixed.sessions[0].settled_cash == D("44.43")


def test_replay_rejects_a_stale_bootstrap_signal_or_unbound_width() -> None:
    candidate = _candidate()
    stale = replace(candidate, signal_at=PREVIOUS_CLOSE - timedelta(hours=8))
    wrong_name = replace(candidate, widths=replace(candidate.widths, instrument="wrong"))
    late_width = replace(
        candidate, widths=replace(candidate.widths, cutoff=DECISION + timedelta(minutes=1))
    )
    for invalid in (stale, wrong_name, late_width):
        with pytest.raises(ValueError, match="invalid replay candidate"):
            compare_exit_widths(
                SESSIONS,
                SETTLEMENTS,
                [invalid],
                _bars(candidate),
                action_coverage=_action_coverage([invalid]),
                opening_evidence=_auctions([invalid], _bars(candidate)),
                session_hours=HOURS,
                entry_convention=CONVENTION,
                initial_cash=CASH,
                costs=COSTS,
                previous_session=HISTORY[-1],
            )


def test_feature_requires_the_previous_close_and_registered_cutoff() -> None:
    snapshot = _snapshot()
    with pytest.raises(ValueError, match="feature cutoff"):
        estimate_volatility(
            "101",
            DECISION + timedelta(minutes=1),
            HISTORY,
            snapshot,
            previous_session_close=PREVIOUS_CLOSE,
        )
    with pytest.raises(ValueError, match="ordered calendar"):
        estimate_volatility(
            "101",
            DECISION,
            HISTORY,
            snapshot,
            previous_session_close=PREVIOUS_CLOSE + timedelta(days=1),
        )
    finalized = replace(snapshot, received_at=PREVIOUS_CLOSE + timedelta(minutes=1))
    assert (
        estimate_volatility(
            "101",
            DECISION,
            HISTORY,
            finalized,
            previous_session_close=PREVIOUS_CLOSE,
        ).daily
        is not None
    )


def test_primary_obeys_frozen_stop_first_when_a_target_gap_reverses() -> None:
    candidate = _candidate()
    bars = _bars(candidate)
    day = SESSIONS[1]
    bars[(candidate.instrument, day)] = DailyBar(
        candidate.symbol, day, 11.2, 11.5, 8.5, 10, 100_000
    )
    primary = compare_exit_widths(
        SESSIONS,
        SETTLEMENTS,
        [candidate],
        bars,
        action_coverage=_action_coverage([candidate]),
        opening_evidence=_auctions([candidate], bars),
        session_hours=HOURS,
        entry_convention=CONVENTION,
        initial_cash=CASH,
        costs=COSTS,
        previous_session=HISTORY[-1],
    )
    stress = compare_exit_widths(
        SESSIONS,
        SETTLEMENTS,
        [candidate],
        bars,
        action_coverage=_action_coverage([candidate]),
        opening_evidence=_auctions([candidate], bars),
        session_hours=HOURS,
        entry_convention=CONVENTION,
        initial_cash=CASH,
        costs=COSTS,
        previous_session=HISTORY[-1],
        collision="target_first",
    )
    assert primary.fixed.net_pnl == D("-21.08")
    assert stress.fixed.net_pnl == D("22.88")
    assert primary.fixed.sessions[1].exits[0].same_bar_collision
    assert not primary.fixed.sessions[1].exits[0].ambiguous_collision


def test_matched_arm_uses_its_own_width_and_opens_no_extra_positions() -> None:
    candidate = _candidate(width=0.05)
    candidate = replace(candidate, widths=replace(candidate.widths, matched=0.075))
    bars = _bars(candidate)
    day = SESSIONS[0]
    bars[(candidate.instrument, day)] = DailyBar(candidate.symbol, day, 10, 10.8, 9.8, 10, 100_000)
    result = compare_exit_widths(
        SESSIONS,
        SETTLEMENTS,
        [candidate],
        bars,
        action_coverage=_action_coverage([candidate]),
        opening_evidence=_auctions([candidate], bars),
        session_hours=HOURS,
        entry_convention=CONVENTION,
        initial_cash=CASH,
        costs=COSTS,
        previous_session=HISTORY[-1],
    )
    assert result.matched.sessions[0].exits[0].reason == "target"
    assert result.matched.net_pnl == D("13.88")
    assert result.scaled.net_pnl == D("8.89")
    assert result.fixed.net_pnl == D("-1.10")
    assert sum(result.scaled_vs_matched_daily_pnl) == D("-4.99")


def test_earlier_scaled_exit_changes_later_capacity_and_net_pnl() -> None:
    first = _candidate()
    second = _candidate("b", instrument="102")
    third = _candidate("c", day=SESSIONS[1], instrument="103")
    bars = _bars(first, second, third)
    first_day = SESSIONS[0]
    next_day = SESSIONS[1]
    bars[first.instrument, first_day] = DailyBar(first.symbol, first_day, 10, 10.6, 10, 10, 100_000)
    bars[third.instrument, next_day] = DailyBar(third.symbol, next_day, 10, 10.6, 10, 10, 100_000)
    result = compare_exit_widths(
        SESSIONS,
        SETTLEMENTS,
        [first, second, third],
        bars,
        action_coverage=_action_coverage([first, second, third]),
        opening_evidence=_auctions([first, second, third], bars),
        session_hours=HOURS,
        entry_convention=CONVENTION,
        initial_cash=CASH,
        costs=COSTS,
        previous_session=HISTORY[-1],
    )
    assert result.fixed.sessions[1].entries[0].state == "capacity_suppressed"
    assert result.scaled.sessions[1].entries[0].state == "opened"
    assert result.paired_daily_pnl[1] == D("8.89")


def test_replay_mirrors_overlap_and_commission_suppression() -> None:
    first = replace(_candidate(), planning_close=110)
    duplicate = replace(first, packet_id="b")
    one_way_cap = ReplayCosts(COMMISSION_CAP, D(0))
    portfolio = compare_exit_widths(
        SESSIONS,
        SETTLEMENTS,
        [first, duplicate],
        _bars(first, duplicate),
        action_coverage=_action_coverage([first, duplicate]),
        opening_evidence=_auctions([first, duplicate], {}),
        session_hours=HOURS,
        entry_convention=CONVENTION,
        initial_cash=CASH,
        costs=one_way_cap,
        previous_session=HISTORY[-1],
    ).fixed
    assert [entry.state for entry in portfolio.sessions[0].entries] == [
        "commission_suppressed",
        "overlap_suppressed",
    ]


def test_incomplete_horizon_and_duplicate_packet_are_rejected() -> None:
    for candidates in ([_candidate(day=SESSIONS[-9])], [_candidate(), _candidate()]):
        with pytest.raises(ValueError, match="invalid replay candidate"):
            compare_exit_widths(
                SESSIONS,
                SETTLEMENTS,
                candidates,
                {},
                action_coverage=_action_coverage(candidates),
                opening_evidence=_auctions(candidates, {}),
                session_hours=HOURS,
                entry_convention=CONVENTION,
                initial_cash=CASH,
                costs=COSTS,
                previous_session=HISTORY[-1],
            )


def test_invalid_price_basis_clock_and_cost_proxies_are_rejected() -> None:
    with pytest.raises(ValueError, match="outside supported regular-hours"):
        estimate_volatility(
            "101",
            DECISION,
            HISTORY,
            _snapshot(),
            previous_session_close=PREVIOUS_CLOSE.replace(hour=0),
        )
    for flat, bps in (("NaN", "10"), ("0.76", "10"), ("0.35", "-1"), ("0.35", "10000")):
        with pytest.raises(ValueError, match="invalid replay costs"):
            ReplayCosts(D(flat), D(bps))


def test_same_session_placebo_only_changes_the_matched_replay_book() -> None:
    original = [_candidate("a", width=0.05), _candidate("b", instrument="102", width=0.15)]
    day = SESSIONS[0]
    bars = _bars(*original)
    for case in original:
        key = (case.instrument, day)
        bars[key] = replace(bars[key], high=10.70)

    ready = {case.packet_id: case.widths for case in original}
    same_session = match_widths_within_session(DECISION, ready, qualified_packets=ready.keys())
    assert (same_session["a"].matched, same_session["b"].matched) == (0.15, 0.05)

    def run(cases: Sequence[ReplayCandidate]) -> ExitReplay:
        return compare_exit_widths(
            SESSIONS,
            SETTLEMENTS,
            cases,
            bars,
            action_coverage=_action_coverage(cases),
            opening_evidence=_auctions(cases, bars),
            session_hours=HOURS,
            entry_convention=CONVENTION,
            initial_cash=CASH,
            costs=COSTS,
            previous_session=HISTORY[-1],
        )

    baseline = run(original)
    alternative = run([replace(case, widths=same_session[case.packet_id]) for case in original])
    assert alternative.fixed == baseline.fixed
    assert alternative.scaled == baseline.scaled
    assert alternative.execution_evidence_sha256 == baseline.execution_evidence_sha256
    assert alternative.action_coverage_sha256 == baseline.action_coverage_sha256
    assert baseline.matched.sessions[0].exits == ()
    assert tuple(exit.packet_id for exit in baseline.scaled.sessions[0].exits) == ("a",)
    assert tuple(exit.packet_id for exit in alternative.matched.sessions[0].exits) == ("b",)


@pytest.mark.parametrize("matched", [0.05, 0.20, 0.04, math.nan])
def test_primary_replay_is_independent_of_matched_width(matched: float) -> None:
    candidate = _candidate(width=0.15)
    candidate = replace(candidate, widths=replace(candidate.widths, matched=matched))
    bars = _bars(candidate)
    key = candidate.instrument, candidate.entry_day
    later = candidate.instrument, SESSIONS[1]
    bars[later] = replace(bars[later], low=9.0)
    opening = _auctions([candidate], bars)
    fill = opening[key]
    assert isinstance(fill, AuctionFill)
    opening[key] = replace(fill, path=replace(fill.path, low=9.45, first_low=9.45, first_high=9.65))
    inputs = dict(
        action_coverage=_action_coverage([candidate]),
        opening_evidence=opening,
        session_hours=HOURS,
        entry_convention=CONVENTION,
        initial_cash=CASH,
        costs=COSTS,
        previous_session=HISTORY[-1],
    )
    primary = compare_primary_exit_widths(SESSIONS, SETTLEMENTS, [candidate], bars, **inputs)
    assert primary.fixed.sessions[1].exits[0].reason == "stop"
    assert not primary.scaled.sessions[1].exits
    assert primary.paired_daily_pnl[1] == D("20.53")
    assert (
        sum(primary.paired_daily_pnl)
        == primary.scaled.net_pnl - primary.fixed.net_pnl
        == D("19.98")
    )

    if matched == 0.05:
        with pytest.raises(
            UnavailableExecution, match="first protected second can touch a barrier"
        ):
            compare_exit_widths(SESSIONS, SETTLEMENTS, [candidate], bars, **inputs)
    elif matched == 0.20:
        diagnostic = compare_exit_widths(SESSIONS, SETTLEMENTS, [candidate], bars, **inputs)
        diagnostic.require_primary_agreement(primary)
        assert diagnostic.paired_daily_pnl == primary.paired_daily_pnl
        with pytest.raises(ReplayAgreementError, match="differs from the primary"):
            diagnostic.require_primary_agreement(
                replace(primary, execution_evidence_sha256="0" * 64)
            )
        with pytest.raises(ReplayAgreementError, match="differs from the primary"):
            diagnostic.require_primary_agreement(replace(primary, fixed=primary.scaled))
    else:
        with pytest.raises(ValueError, match="invalid replay candidate"):
            compare_exit_widths(SESSIONS, SETTLEMENTS, [candidate], bars, **inputs)


@pytest.mark.parametrize(
    ("scaled", "missing", "accepted"),
    [(float("nan"), None, False), (0.05, "no_snapshot", False), (BASE_WIDTH, "no_snapshot", True)],
)
def test_primary_requires_the_scaled_width_and_fixed_fallback(
    scaled: float, missing: Literal["no_snapshot"] | None, accepted: bool
) -> None:
    candidate = _candidate(width=scaled)
    candidate = replace(candidate, widths=replace(candidate.widths, matched=0.05, missing=missing))
    bars = _bars(candidate)
    inputs = dict(
        action_coverage=_action_coverage([candidate]),
        opening_evidence=_auctions([candidate], bars),
        session_hours=HOURS,
        entry_convention=CONVENTION,
        initial_cash=CASH,
        costs=COSTS,
        previous_session=HISTORY[-1],
    )
    if accepted:
        primary = compare_primary_exit_widths(SESSIONS, SETTLEMENTS, [candidate], bars, **inputs)
        assert primary.fixed == primary.scaled
    else:
        with pytest.raises(ValueError, match="invalid replay candidate"):
            compare_primary_exit_widths(SESSIONS, SETTLEMENTS, [candidate], bars, **inputs)
    with pytest.raises(ValueError, match="invalid replay candidate"):
        compare_exit_widths(SESSIONS, SETTLEMENTS, [candidate], bars, **inputs)


def test_primary_completes_when_only_the_matched_book_cannot_afford_a_fill() -> None:
    first = _candidate(width=0.15)
    first = replace(first, widths=replace(first.widths, matched=0.05))
    second = _candidate("b", instrument="102", width=0.15)
    gapped = _candidate("c", day=SESSIONS[1], instrument="103", width=0.15)
    candidates = [first, second, gapped]
    bars = _bars(*candidates)
    first_key = first.instrument, first.entry_day
    gapped_key = gapped.instrument, gapped.entry_day
    bars[first_key] = replace(bars[first_key], high=10.70)
    bars[gapped_key] = DailyBar(gapped.symbol, gapped.entry_day, 30, 30, 30, 30, 100_000)
    inputs = dict(
        action_coverage=_action_coverage(candidates),
        opening_evidence=_auctions(candidates, bars),
        session_hours=HOURS,
        entry_convention=CONVENTION,
        initial_cash=CASH,
        costs=COSTS,
        previous_session=HISTORY[-1],
    )
    primary = compare_primary_exit_widths(SESSIONS, SETTLEMENTS, candidates, bars, **inputs)
    assert primary.fixed.sessions[1].entries[0].state == "capacity_suppressed"
    assert primary.scaled.sessions[1].entries[0].state == "capacity_suppressed"
    with pytest.raises(
        UnavailableExecution, match=f"fills exceed settled cash on {gapped.entry_day}"
    ):
        compare_exit_widths(SESSIONS, SETTLEMENTS, candidates, bars, **inputs)
