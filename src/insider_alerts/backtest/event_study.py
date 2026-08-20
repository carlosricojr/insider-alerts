from __future__ import annotations

import math
import random
import statistics
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date, timedelta

from insider_alerts.backtest.event_data import CanonicalEvent
from insider_alerts.backtest.models import DailyBar


@dataclass(slots=True, frozen=True)
class TradabilityConfig:
    min_price: float = 2.0
    min_median_dollar_volume_20d: float = 500_000.0
    trailing_liquidity_days: int = 20


@dataclass(slots=True)
class EventReturnObservation:
    packet_id: str
    symbol: str
    filed_date: date
    horizon_days: int
    score: float
    entry_date: date | None
    exit_date: date | None
    entry_price: float | None
    exit_price: float | None
    trade_executed: bool
    benchmark_available: bool
    skip_reason: str | None
    trailing_median_dollar_volume: float | None
    net_return: float | None
    benchmark_return: float | None
    alpha_return: float | None


@dataclass(slots=True)
class ScoreBucketMetrics:
    horizon_days: int
    bucket_index: int
    bucket_count: int
    bucket_score_min: float | None
    bucket_score_max: float | None
    total_events: int
    executed_events: int
    benchmark_available_events: int
    execution_coverage_rate: float
    benchmark_coverage_rate: float
    mean_alpha: float | None
    median_alpha: float | None
    win_rate: float | None
    mean_alpha_ci_low: float | None
    mean_alpha_ci_high: float | None
    alpha_p_value: float | None
    alpha_q_value: float | None = None


@dataclass(slots=True)
class OosFoldResult:
    fold_index: int
    train_start: date
    train_end: date
    test_start: date
    test_end: date
    train_event_count: int
    test_event_count: int
    score_bucket_edges: list[float]
    bucket_metrics: list[ScoreBucketMetrics]
    skip_diagnostics: dict[str, int]
    skip_diagnostics_by_horizon: dict[int, dict[str, int]]


@dataclass(slots=True)
class OosFoldSkip:
    fold_index: int
    train_start: date
    train_end: date
    test_start: date
    test_end: date
    train_event_count: int
    test_event_count: int
    reason: str


@dataclass(slots=True)
class MonotonicityResult:
    horizon_days: int
    spearman_rho: float | None
    p_value_proxy: float | None
    non_negative: bool
    bucket_points_used: int


@dataclass(slots=True)
class NegativeControlSummary:
    horizon_days: int
    actual_top_bucket_mean_alpha: float | None
    null_mean_alpha: float | None
    null_ci_low: float | None
    null_ci_high: float | None
    p_value_proxy: float | None
    iterations: int


@dataclass(slots=True)
class OosEventStudyResult:
    folds: list[OosFoldResult]
    skipped_folds: list[OosFoldSkip]
    aggregate_bucket_metrics: list[ScoreBucketMetrics]
    aggregate_skip_diagnostics: dict[str, int]
    aggregate_skip_diagnostics_by_horizon: dict[int, dict[str, int]]
    monotonicity: list[MonotonicityResult]
    negative_control: list[NegativeControlSummary]


def _find_entry_index(bars: list[DailyBar], *, signal_date: date) -> int | None:
    for idx, bar in enumerate(bars):
        if bar.trade_date > signal_date:
            return idx
    return None


def _find_bar_on_or_after(bars: list[DailyBar], *, target_date: date) -> DailyBar | None:
    for bar in bars:
        if bar.trade_date >= target_date:
            return bar
    return None


def _round_trip_cost_fraction(*, transaction_cost_bps: float, slippage_bps: float) -> float:
    return 2.0 * (max(transaction_cost_bps, 0.0) + max(slippage_bps, 0.0)) / 10000.0


def _trailing_median_dollar_volume(
    bars: list[DailyBar],
    *,
    end_index_exclusive: int,
    window_days: int,
) -> float | None:
    if window_days <= 0:
        return None
    start_idx = end_index_exclusive - window_days
    if start_idx < 0:
        return None
    window = bars[start_idx:end_index_exclusive]
    if len(window) < window_days:
        return None
    values = [bar.close * bar.volume for bar in window]
    if not values:
        return None
    return float(statistics.median(values))


def _iter_horizons(horizons: Sequence[int]) -> list[int]:
    deduped: list[int] = []
    seen: set[int] = set()
    for horizon in horizons:
        value = int(horizon)
        if value <= 0:
            raise ValueError(f"invalid horizon: {value}")
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    if not deduped:
        raise ValueError("horizons cannot be empty")
    deduped.sort()
    return deduped


def _skip_observation(
    *,
    event: CanonicalEvent,
    horizon_days: int,
    reason: str,
    entry_date: date | None = None,
    exit_date: date | None = None,
    entry_price: float | None = None,
    exit_price: float | None = None,
    trailing_median_dollar_volume: float | None = None,
) -> EventReturnObservation:
    return EventReturnObservation(
        packet_id=event.packet_id,
        symbol=event.symbol,
        filed_date=event.filed_at.date(),
        horizon_days=horizon_days,
        score=event.score,
        entry_date=entry_date,
        exit_date=exit_date,
        entry_price=entry_price,
        exit_price=exit_price,
        trade_executed=False,
        benchmark_available=False,
        skip_reason=reason,
        trailing_median_dollar_volume=trailing_median_dollar_volume,
        net_return=None,
        benchmark_return=None,
        alpha_return=None,
    )


def compute_event_forward_returns(
    events: Sequence[CanonicalEvent],
    *,
    bars_by_symbol: dict[str, list[DailyBar]],
    horizons: Sequence[int],
    benchmark_symbol: str = "SPY",
    transaction_cost_bps: float = 5.0,
    slippage_bps: float = 5.0,
    tradability: TradabilityConfig | None = None,
) -> list[EventReturnObservation]:
    selected_horizons = _iter_horizons(horizons)
    tradability_cfg = tradability or TradabilityConfig()
    benchmark_bars = bars_by_symbol.get(benchmark_symbol.upper())
    round_trip_cost = _round_trip_cost_fraction(
        transaction_cost_bps=transaction_cost_bps,
        slippage_bps=slippage_bps,
    )

    observations: list[EventReturnObservation] = []
    for event in events:
        symbol_bars = bars_by_symbol.get(event.symbol.upper())
        if not symbol_bars:
            observations.extend(
                _skip_observation(
                    event=event,
                    horizon_days=horizon,
                    reason="missing_symbol_bars",
                )
                for horizon in selected_horizons
            )
            continue

        entry_idx = _find_entry_index(symbol_bars, signal_date=event.filed_at.date())
        if entry_idx is None:
            observations.extend(
                _skip_observation(
                    event=event,
                    horizon_days=horizon,
                    reason="missing_entry",
                )
                for horizon in selected_horizons
            )
            continue

        entry_bar = symbol_bars[entry_idx]
        entry_price = entry_bar.open

        trailing_median_dollar_volume: float | None = None
        if tradability_cfg.min_price > 0 and entry_price < tradability_cfg.min_price:
            observations.extend(
                _skip_observation(
                    event=event,
                    horizon_days=horizon,
                    reason="fails_tradability_price",
                    entry_date=entry_bar.trade_date,
                    entry_price=entry_price,
                )
                for horizon in selected_horizons
            )
            continue

        if tradability_cfg.min_median_dollar_volume_20d > 0:
            trailing_median_dollar_volume = _trailing_median_dollar_volume(
                symbol_bars,
                end_index_exclusive=entry_idx,
                window_days=tradability_cfg.trailing_liquidity_days,
            )
            if (
                trailing_median_dollar_volume is None
                or trailing_median_dollar_volume < tradability_cfg.min_median_dollar_volume_20d
            ):
                observations.extend(
                    _skip_observation(
                        event=event,
                        horizon_days=horizon,
                        reason="fails_tradability_liquidity",
                        entry_date=entry_bar.trade_date,
                        entry_price=entry_price,
                        trailing_median_dollar_volume=trailing_median_dollar_volume,
                    )
                    for horizon in selected_horizons
                )
                continue

        for horizon in selected_horizons:
            exit_idx = entry_idx + horizon - 1
            if exit_idx >= len(symbol_bars):
                observations.append(
                    _skip_observation(
                        event=event,
                        horizon_days=horizon,
                        reason="missing_exit",
                        entry_date=entry_bar.trade_date,
                        entry_price=entry_price,
                        trailing_median_dollar_volume=trailing_median_dollar_volume,
                    )
                )
                continue

            exit_bar = symbol_bars[exit_idx]
            if benchmark_bars is None:
                observations.append(
                    _skip_observation(
                        event=event,
                        horizon_days=horizon,
                        reason="missing_benchmark",
                        entry_date=entry_bar.trade_date,
                        exit_date=exit_bar.trade_date,
                        entry_price=entry_price,
                        exit_price=exit_bar.close,
                        trailing_median_dollar_volume=trailing_median_dollar_volume,
                    )
                )
                continue

            benchmark_entry = _find_bar_on_or_after(
                benchmark_bars,
                target_date=entry_bar.trade_date,
            )
            benchmark_exit = _find_bar_on_or_after(benchmark_bars, target_date=exit_bar.trade_date)
            if benchmark_entry is None or benchmark_exit is None or benchmark_entry.open <= 0:
                observations.append(
                    _skip_observation(
                        event=event,
                        horizon_days=horizon,
                        reason="missing_benchmark",
                        entry_date=entry_bar.trade_date,
                        exit_date=exit_bar.trade_date,
                        entry_price=entry_price,
                        exit_price=exit_bar.close,
                        trailing_median_dollar_volume=trailing_median_dollar_volume,
                    )
                )
                continue

            gross_return = (exit_bar.close / entry_price) - 1.0
            net_return = gross_return - round_trip_cost
            benchmark_return = (benchmark_exit.close / benchmark_entry.open) - 1.0
            alpha_return = net_return - benchmark_return
            observations.append(
                EventReturnObservation(
                    packet_id=event.packet_id,
                    symbol=event.symbol,
                    filed_date=event.filed_at.date(),
                    horizon_days=horizon,
                    score=event.score,
                    entry_date=entry_bar.trade_date,
                    exit_date=exit_bar.trade_date,
                    entry_price=entry_price,
                    exit_price=exit_bar.close,
                    trade_executed=True,
                    benchmark_available=True,
                    skip_reason=None,
                    trailing_median_dollar_volume=trailing_median_dollar_volume,
                    net_return=net_return,
                    benchmark_return=benchmark_return,
                    alpha_return=alpha_return,
                )
            )

    return observations


def load_bars_for_event_study(
    events: Iterable[CanonicalEvent],
    *,
    bars_by_symbol: dict[str, list[DailyBar]],
    benchmark_symbol: str = "SPY",
) -> dict[str, list[DailyBar]]:
    symbols = {event.symbol.upper() for event in events}
    symbols.add(benchmark_symbol.upper())
    return {symbol: bars_by_symbol.get(symbol, []) for symbol in symbols}


def _quantile(sorted_values: list[float], q: float) -> float:
    if not sorted_values:
        raise ValueError("cannot compute quantile of empty values")
    if q <= 0:
        return float(sorted_values[0])
    if q >= 1:
        return float(sorted_values[-1])
    n = len(sorted_values)
    position = q * (n - 1)
    low_idx = math.floor(position)
    high_idx = math.ceil(position)
    if low_idx == high_idx:
        return float(sorted_values[low_idx])
    low_value = float(sorted_values[low_idx])
    high_value = float(sorted_values[high_idx])
    weight = position - low_idx
    return low_value + (high_value - low_value) * weight


def _score_bucket_edges(scores: list[float], *, bucket_count: int) -> list[float]:
    if bucket_count < 2:
        raise ValueError("bucket_count must be >= 2")
    if not scores:
        raise ValueError("cannot build bucket edges from empty scores")
    sorted_scores = sorted(scores)
    edges: list[float] = []
    for bucket_idx in range(1, bucket_count):
        edges.append(_quantile(sorted_scores, bucket_idx / bucket_count))
    return edges


def _assign_bucket(score: float, *, edges: Sequence[float]) -> int:
    bucket = 1
    for edge in edges:
        if score > edge:
            bucket += 1
            continue
        break
    return bucket


def _bucket_bounds(
    *,
    bucket_index: int,
    edges: Sequence[float],
) -> tuple[float | None, float | None]:
    if not edges:
        return None, None
    lower = None if bucket_index <= 1 else float(edges[bucket_index - 2])
    upper = None if bucket_index > len(edges) else float(edges[bucket_index - 1])
    return lower, upper


def _safe_mean(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return float(statistics.fmean(values))


def _safe_median(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return float(statistics.median(values))


def _win_rate(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return float(sum(1 for value in values if value > 0) / len(values))


def _bootstrap_mean_ci(
    values: Sequence[float],
    *,
    random_seed: int,
    iterations: int = 1000,
    confidence: float = 0.95,
) -> tuple[float | None, float | None]:
    if not values:
        return None, None
    if len(values) == 1:
        value = float(values[0])
        return value, value
    if iterations <= 0:
        raise ValueError("iterations must be > 0")
    rng = random.Random(random_seed)
    n = len(values)
    means: list[float] = []
    for _ in range(iterations):
        sample = [values[rng.randrange(n)] for _ in range(n)]
        means.append(float(statistics.fmean(sample)))
    means.sort()
    alpha = max(0.0, min(1.0, 1.0 - confidence))
    low_q = alpha / 2.0
    high_q = 1.0 - (alpha / 2.0)
    low_idx = min(len(means) - 1, max(0, int(math.floor(low_q * (len(means) - 1)))))
    high_idx = min(len(means) - 1, max(0, int(math.ceil(high_q * (len(means) - 1)))))
    return means[low_idx], means[high_idx]


def bootstrap_mean_alpha_ci(
    alpha_values: Sequence[float],
    *,
    random_seed: int,
    iterations: int = 1000,
    confidence: float = 0.95,
) -> tuple[float | None, float | None]:
    return _bootstrap_mean_ci(
        alpha_values,
        random_seed=random_seed,
        iterations=iterations,
        confidence=confidence,
    )


def _positive_alpha_p_value(
    values: Sequence[float],
    *,
    random_seed: int,
    iterations: int = 1000,
) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return 0.0 if values[0] > 0 else 1.0
    rng = random.Random(random_seed)
    n = len(values)
    count_non_positive = 0
    for _ in range(iterations):
        sample = [values[rng.randrange(n)] for _ in range(n)]
        mean_value = float(statistics.fmean(sample))
        if mean_value <= 0:
            count_non_positive += 1
    return float(count_non_positive / iterations)


def _rank_values(values: Sequence[float]) -> list[float]:
    indexed = sorted(enumerate(values), key=lambda pair: pair[1])
    ranks = [0.0] * len(values)
    pos = 0
    while pos < len(indexed):
        start = pos
        current_value = indexed[pos][1]
        while pos + 1 < len(indexed) and indexed[pos + 1][1] == current_value:
            pos += 1
        end = pos
        avg_rank = (start + end) / 2.0 + 1.0
        for idx in range(start, end + 1):
            original_pos = indexed[idx][0]
            ranks[original_pos] = avg_rank
        pos += 1
    return ranks


def _spearman_rho(x_values: Sequence[float], y_values: Sequence[float]) -> float | None:
    if len(x_values) != len(y_values) or len(x_values) < 2:
        return None
    x_ranks = _rank_values(x_values)
    y_ranks = _rank_values(y_values)
    x_mean = statistics.fmean(x_ranks)
    y_mean = statistics.fmean(y_ranks)
    numerator = 0.0
    x_var = 0.0
    y_var = 0.0
    for x_rank, y_rank in zip(x_ranks, y_ranks, strict=False):
        x_delta = x_rank - x_mean
        y_delta = y_rank - y_mean
        numerator += x_delta * y_delta
        x_var += x_delta * x_delta
        y_var += y_delta * y_delta
    if x_var <= 0 or y_var <= 0:
        return None
    return float(numerator / math.sqrt(x_var * y_var))


def _spearman_positive_p_value_proxy(
    x_values: Sequence[float],
    y_values: Sequence[float],
    *,
    random_seed: int,
    iterations: int = 1000,
) -> float | None:
    observed = _spearman_rho(x_values, y_values)
    if observed is None:
        return None
    if len(x_values) < 3:
        return 1.0 if observed <= 0 else 0.0
    rng = random.Random(random_seed)
    y_list = list(y_values)
    greater_or_equal = 0
    for _ in range(iterations):
        shuffled = y_list[:]
        rng.shuffle(shuffled)
        permuted = _spearman_rho(x_values, shuffled)
        if permuted is None:
            continue
        if permuted >= observed:
            greater_or_equal += 1
    return float(greater_or_equal / iterations)


def compute_score_bucket_monotonicity(
    mean_alpha_by_bucket: Sequence[float],
    *,
    horizon_days: int,
    random_seed: int,
    iterations: int = 1000,
) -> MonotonicityResult:
    if not mean_alpha_by_bucket:
        return MonotonicityResult(
            horizon_days=horizon_days,
            spearman_rho=None,
            p_value_proxy=None,
            non_negative=False,
            bucket_points_used=0,
        )
    x_values = [float(index) for index in range(1, len(mean_alpha_by_bucket) + 1)]
    y_values = [float(value) for value in mean_alpha_by_bucket]
    rho = _spearman_rho(x_values, y_values)
    p_value = _spearman_positive_p_value_proxy(
        x_values,
        y_values,
        random_seed=random_seed,
        iterations=iterations,
    )
    return MonotonicityResult(
        horizon_days=horizon_days,
        spearman_rho=rho,
        p_value_proxy=p_value,
        non_negative=(rho is not None and rho >= 0.0),
        bucket_points_used=len(y_values),
    )


def benjamini_hochberg_adjust(
    values: Sequence[tuple[str, float | None]],
) -> dict[str, float | None]:
    valid = [(key, p_value) for key, p_value in values if p_value is not None]
    if not valid:
        return {key: None for key, _ in values}
    sorted_valid = sorted(valid, key=lambda pair: pair[1])
    m = len(sorted_valid)
    adjusted: dict[str, float] = {}
    running_min = 1.0
    for idx in range(m, 0, -1):
        key, p_value = sorted_valid[idx - 1]
        raw_q = (p_value * m) / idx
        running_min = min(running_min, raw_q)
        adjusted[key] = min(1.0, running_min)
    out: dict[str, float | None] = {}
    for key, _ in values:
        out[key] = adjusted.get(key)
    return out


def _build_bucket_metrics(
    *,
    horizon_days: int,
    bucket_index: int,
    bucket_count: int,
    edges: Sequence[float],
    total_events: int,
    executed_events: int,
    benchmark_available_events: int,
    alpha_values: Sequence[float],
    random_seed: int,
    bootstrap_iterations: int,
) -> ScoreBucketMetrics:
    execution_coverage = (executed_events / total_events) if total_events > 0 else 0.0
    benchmark_coverage = (
        benchmark_available_events / total_events if total_events > 0 else 0.0
    )
    ci_low, ci_high = _bootstrap_mean_ci(
        alpha_values,
        random_seed=random_seed,
        iterations=bootstrap_iterations,
    )
    p_value = _positive_alpha_p_value(
        alpha_values,
        random_seed=random_seed + 17,
        iterations=bootstrap_iterations,
    )
    bucket_score_min, bucket_score_max = _bucket_bounds(
        bucket_index=bucket_index,
        edges=edges,
    )
    return ScoreBucketMetrics(
        horizon_days=horizon_days,
        bucket_index=bucket_index,
        bucket_count=bucket_count,
        bucket_score_min=bucket_score_min,
        bucket_score_max=bucket_score_max,
        total_events=total_events,
        executed_events=executed_events,
        benchmark_available_events=benchmark_available_events,
        execution_coverage_rate=execution_coverage,
        benchmark_coverage_rate=benchmark_coverage,
        mean_alpha=_safe_mean(alpha_values),
        median_alpha=_safe_median(alpha_values),
        win_rate=_win_rate(alpha_values),
        mean_alpha_ci_low=ci_low,
        mean_alpha_ci_high=ci_high,
        alpha_p_value=p_value,
        alpha_q_value=None,
    )


def _metric_key(*, horizon_days: int, bucket_index: int) -> str:
    return f"h{horizon_days}|b{bucket_index}"


def _build_negative_control(
    *,
    folds: Sequence[OosFoldResult],
    fold_executed_alpha: dict[tuple[int, int], list[tuple[int, float]]],
    bucket_count: int,
    horizons: Sequence[int],
    random_seed: int,
    iterations: int = 500,
) -> list[NegativeControlSummary]:
    summaries: list[NegativeControlSummary] = []
    for horizon in horizons:
        top_bucket = bucket_count
        actual_values: list[float] = []
        for fold in folds:
            for metric in fold.bucket_metrics:
                if metric.horizon_days == horizon and metric.bucket_index == top_bucket:
                    fold_values = fold_executed_alpha.get((fold.fold_index, horizon), [])
                    actual_values.extend(
                        value for bucket_idx, value in fold_values if bucket_idx == top_bucket
                    )
        actual_mean = _safe_mean(actual_values)
        if iterations <= 0:
            summaries.append(
                NegativeControlSummary(
                    horizon_days=horizon,
                    actual_top_bucket_mean_alpha=actual_mean,
                    null_mean_alpha=None,
                    null_ci_low=None,
                    null_ci_high=None,
                    p_value_proxy=None,
                    iterations=0,
                )
            )
            continue

        rng = random.Random(random_seed + (horizon * 31))
        null_distribution: list[float] = []
        for _ in range(iterations):
            top_values: list[float] = []
            for fold in folds:
                pairs = fold_executed_alpha.get((fold.fold_index, horizon), [])
                if not pairs:
                    continue
                buckets = [bucket_idx for bucket_idx, _ in pairs]
                values = [value for _, value in pairs]
                shuffled = values[:]
                rng.shuffle(shuffled)
                top_values.extend(
                    shuffled[idx]
                    for idx, bucket_idx in enumerate(buckets)
                    if bucket_idx == top_bucket
                )
            if top_values:
                null_distribution.append(float(statistics.fmean(top_values)))

        if not null_distribution:
            summaries.append(
                NegativeControlSummary(
                    horizon_days=horizon,
                    actual_top_bucket_mean_alpha=actual_mean,
                    null_mean_alpha=None,
                    null_ci_low=None,
                    null_ci_high=None,
                    p_value_proxy=None,
                    iterations=iterations,
                )
            )
            continue

        null_distribution.sort()
        null_mean = float(statistics.fmean(null_distribution))
        ci_low = _quantile(null_distribution, 0.025)
        ci_high = _quantile(null_distribution, 0.975)
        if actual_mean is None:
            p_value_proxy = None
        else:
            p_value_proxy = float(
                sum(1 for value in null_distribution if value >= actual_mean)
                / len(null_distribution)
            )
        summaries.append(
            NegativeControlSummary(
                horizon_days=horizon,
                actual_top_bucket_mean_alpha=actual_mean,
                null_mean_alpha=null_mean,
                null_ci_low=ci_low,
                null_ci_high=ci_high,
                p_value_proxy=p_value_proxy,
                iterations=iterations,
            )
        )
    return summaries


def run_oos_event_study(
    events: Sequence[CanonicalEvent],
    *,
    bars_by_symbol: dict[str, list[DailyBar]],
    horizons: Sequence[int],
    bucket_count: int = 5,
    train_window_days: int = 365,
    test_window_days: int = 90,
    min_train_events: int = 100,
    min_test_events: int = 25,
    benchmark_symbol: str = "SPY",
    transaction_cost_bps: float = 5.0,
    slippage_bps: float = 5.0,
    tradability: TradabilityConfig | None = None,
    random_seed: int = 7,
    bootstrap_iterations: int = 1000,
    monotonicity_iterations: int = 1000,
    negative_control_iterations: int = 500,
) -> OosEventStudyResult:
    if bucket_count < 2:
        raise ValueError("bucket_count must be >= 2")
    if train_window_days <= 0 or test_window_days <= 0:
        raise ValueError("train_window_days and test_window_days must be > 0")
    selected_horizons = _iter_horizons(horizons)
    if not events:
        return OosEventStudyResult(
            folds=[],
            skipped_folds=[],
            aggregate_bucket_metrics=[],
            aggregate_skip_diagnostics={},
            aggregate_skip_diagnostics_by_horizon={},
            monotonicity=[],
            negative_control=[],
        )

    sorted_events = sorted(events, key=lambda event: (event.filed_at, event.packet_id))
    min_date = sorted_events[0].filed_at.date()
    max_date = sorted_events[-1].filed_at.date()
    fold_start = min_date + timedelta(days=train_window_days)
    fold_index = 0

    folds: list[OosFoldResult] = []
    skipped_folds: list[OosFoldSkip] = []
    aggregate_skip_diagnostics: dict[str, int] = defaultdict(int)
    aggregate_skip_diagnostics_by_horizon: dict[int, dict[str, int]] = defaultdict(
        lambda: defaultdict(int)
    )
    aggregate_alpha_values: dict[tuple[int, int], list[float]] = defaultdict(list)
    aggregate_total_events: dict[tuple[int, int], int] = defaultdict(int)
    aggregate_executed_events: dict[tuple[int, int], int] = defaultdict(int)
    aggregate_benchmark_events: dict[tuple[int, int], int] = defaultdict(int)
    fold_executed_alpha: dict[tuple[int, int], list[tuple[int, float]]] = {}

    while fold_start <= max_date:
        fold_index += 1
        train_start = fold_start - timedelta(days=train_window_days)
        train_end = fold_start - timedelta(days=1)
        test_start = fold_start
        test_end = fold_start + timedelta(days=test_window_days - 1)

        train_events = [
            event
            for event in sorted_events
            if train_start <= event.filed_at.date() <= train_end
        ]
        test_events = [
            event for event in sorted_events if test_start <= event.filed_at.date() <= test_end
        ]

        skip_reason: str | None = None
        if len(train_events) < min_train_events:
            skip_reason = "insufficient_train_events"
        elif len(test_events) < min_test_events:
            skip_reason = "insufficient_test_events"

        if skip_reason is not None:
            skipped_folds.append(
                OosFoldSkip(
                    fold_index=fold_index,
                    train_start=train_start,
                    train_end=train_end,
                    test_start=test_start,
                    test_end=test_end,
                    train_event_count=len(train_events),
                    test_event_count=len(test_events),
                    reason=skip_reason,
                )
            )
            fold_start = test_end + timedelta(days=1)
            continue

        edges = _score_bucket_edges(
            [event.score for event in train_events],
            bucket_count=bucket_count,
        )
        bucket_by_packet = {
            event.packet_id: _assign_bucket(event.score, edges=edges) for event in test_events
        }
        observations = compute_event_forward_returns(
            test_events,
            bars_by_symbol=bars_by_symbol,
            horizons=selected_horizons,
            benchmark_symbol=benchmark_symbol,
            transaction_cost_bps=transaction_cost_bps,
            slippage_bps=slippage_bps,
            tradability=tradability,
        )

        fold_skip: dict[str, int] = defaultdict(int)
        fold_skip_by_horizon: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        fold_totals: dict[tuple[int, int], int] = defaultdict(int)
        fold_exec: dict[tuple[int, int], int] = defaultdict(int)
        fold_benchmark: dict[tuple[int, int], int] = defaultdict(int)
        fold_alpha_values: dict[tuple[int, int], list[float]] = defaultdict(list)
        per_horizon_executed_for_negative: dict[int, list[tuple[int, float]]] = defaultdict(list)

        for horizon in selected_horizons:
            for bucket in range(1, bucket_count + 1):
                fold_totals[(horizon, bucket)] = 0

        for event in test_events:
            bucket = bucket_by_packet[event.packet_id]
            for horizon in selected_horizons:
                fold_totals[(horizon, bucket)] += 1

        for obs in observations:
            observation_bucket = bucket_by_packet.get(obs.packet_id)
            if observation_bucket is None:
                continue
            key = (obs.horizon_days, observation_bucket)
            if obs.benchmark_available:
                fold_benchmark[key] += 1
            if obs.trade_executed and obs.alpha_return is not None:
                fold_exec[key] += 1
                alpha_value = float(obs.alpha_return)
                fold_alpha_values[key].append(alpha_value)
                per_horizon_executed_for_negative[obs.horizon_days].append(
                    (observation_bucket, alpha_value)
                )
            elif obs.skip_reason is not None:
                fold_skip[obs.skip_reason] += 1
                fold_skip_by_horizon[obs.horizon_days][obs.skip_reason] += 1

        bucket_metrics: list[ScoreBucketMetrics] = []
        for horizon in selected_horizons:
            for bucket in range(1, bucket_count + 1):
                key = (horizon, bucket)
                metric_seed = random_seed + (fold_index * 1_000_003) + (horizon * 101) + bucket
                metric = _build_bucket_metrics(
                    horizon_days=horizon,
                    bucket_index=bucket,
                    bucket_count=bucket_count,
                    edges=edges,
                    total_events=fold_totals.get(key, 0),
                    executed_events=fold_exec.get(key, 0),
                    benchmark_available_events=fold_benchmark.get(key, 0),
                    alpha_values=fold_alpha_values.get(key, []),
                    random_seed=metric_seed,
                    bootstrap_iterations=bootstrap_iterations,
                )
                bucket_metrics.append(metric)

                aggregate_total_events[key] += metric.total_events
                aggregate_executed_events[key] += metric.executed_events
                aggregate_benchmark_events[key] += metric.benchmark_available_events
                aggregate_alpha_values[key].extend(fold_alpha_values.get(key, []))

        for reason, count in fold_skip.items():
            aggregate_skip_diagnostics[reason] += count
        for horizon, by_reason in fold_skip_by_horizon.items():
            for reason, count in by_reason.items():
                aggregate_skip_diagnostics_by_horizon[horizon][reason] += count

        fold_executed_alpha.update({
            (fold_index, horizon): values
            for horizon, values in per_horizon_executed_for_negative.items()
        })
        folds.append(
            OosFoldResult(
                fold_index=fold_index,
                train_start=train_start,
                train_end=train_end,
                test_start=test_start,
                test_end=test_end,
                train_event_count=len(train_events),
                test_event_count=len(test_events),
                score_bucket_edges=edges,
                bucket_metrics=bucket_metrics,
                skip_diagnostics=dict(fold_skip),
                skip_diagnostics_by_horizon={
                    horizon: dict(counts) for horizon, counts in fold_skip_by_horizon.items()
                },
            )
        )
        fold_start = test_end + timedelta(days=1)

    aggregate_metrics: list[ScoreBucketMetrics] = []
    for horizon in selected_horizons:
        for bucket in range(1, bucket_count + 1):
            key = (horizon, bucket)
            seed = random_seed + (horizon * 997) + (bucket * 31)
            metric = _build_bucket_metrics(
                horizon_days=horizon,
                bucket_index=bucket,
                bucket_count=bucket_count,
                edges=[],
                total_events=aggregate_total_events.get(key, 0),
                executed_events=aggregate_executed_events.get(key, 0),
                benchmark_available_events=aggregate_benchmark_events.get(key, 0),
                alpha_values=aggregate_alpha_values.get(key, []),
                random_seed=seed,
                bootstrap_iterations=bootstrap_iterations,
            )
            aggregate_metrics.append(metric)

    p_values = [
        (
            _metric_key(
                horizon_days=metric.horizon_days,
                bucket_index=metric.bucket_index,
            ),
            metric.alpha_p_value,
        )
        for metric in aggregate_metrics
    ]
    q_values = benjamini_hochberg_adjust(p_values)
    for metric in aggregate_metrics:
        metric.alpha_q_value = q_values.get(
            _metric_key(horizon_days=metric.horizon_days, bucket_index=metric.bucket_index)
        )

    monotonicity_results: list[MonotonicityResult] = []
    for horizon in selected_horizons:
        means = [
            metric.mean_alpha
            for metric in sorted(
                [metric for metric in aggregate_metrics if metric.horizon_days == horizon],
                key=lambda metric: metric.bucket_index,
            )
            if metric.mean_alpha is not None
        ]
        monotonicity_results.append(
            compute_score_bucket_monotonicity(
                [float(mean) for mean in means],
                horizon_days=horizon,
                random_seed=random_seed + (horizon * 17),
                iterations=monotonicity_iterations,
            )
        )

    negative_control = _build_negative_control(
        folds=folds,
        fold_executed_alpha=fold_executed_alpha,
        bucket_count=bucket_count,
        horizons=selected_horizons,
        random_seed=random_seed,
        iterations=negative_control_iterations,
    )

    return OosEventStudyResult(
        folds=folds,
        skipped_folds=skipped_folds,
        aggregate_bucket_metrics=aggregate_metrics,
        aggregate_skip_diagnostics=dict(aggregate_skip_diagnostics),
        aggregate_skip_diagnostics_by_horizon={
            horizon: dict(counts)
            for horizon, counts in aggregate_skip_diagnostics_by_horizon.items()
        },
        monotonicity=monotonicity_results,
        negative_control=negative_control,
    )
