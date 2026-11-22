from __future__ import annotations

import math
import statistics
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal

import pytest

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
from e07_volatility_exits.replay import (
    COMMISSION_CAP,
    COMMISSION_RATE_CAP,
    CUSHION,
    POLICY,
    RESERVE,
    SLOT_COUNT,
    ReplayCandidate,
    ReplayCosts,
    UnavailableExecution,
    compare_exit_widths,
)
from insider_alerts.backtest.models import DailyBar
from insider_alerts.execution.canary import CanaryConfig
from insider_alerts.strategy.e07 import NEW_YORK, deterministic_rank

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
        initial_cash=CASH,
        costs=COSTS,
        previous_session=HISTORY[-1],
    )
    stress = compare_exit_widths(
        SESSIONS,
        SETTLEMENTS,
        [candidate],
        bars,
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
    bars = _bars(second)
    misses = frozenset({(first.instrument, first.entry_day)})
    fixed = compare_exit_widths(
        SESSIONS,
        SETTLEMENTS,
        candidates,
        bars,
        initial_cash=CASH,
        costs=COSTS,
        previous_session=HISTORY[-1],
        confirmed_auction_misses=misses,
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
            initial_cash=CASH,
            costs=COSTS,
            previous_session=HISTORY[-1],
        )
    with pytest.raises(ValueError, match="invalid replay calendar"):
        compare_exit_widths(
            SESSIONS, {}, [], {}, initial_cash=CASH, costs=COSTS, previous_session=HISTORY[-1]
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
        initial_cash=CASH,
        costs=COSTS,
        previous_session=HISTORY[-1],
    )
    stress = compare_exit_widths(
        SESSIONS,
        SETTLEMENTS,
        [candidate],
        bars,
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
        {},
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
