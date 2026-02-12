from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from datetime import date, timedelta

from insider_alerts.backtest.models import DailyBar, SignalEvent, TradeResult


@dataclass(slots=True, frozen=True)
class BacktestParams:
    min_score: float
    hold_days: int
    stop_loss_pct: float
    take_profit_rr: float


@dataclass(slots=True)
class BacktestMetrics:
    trade_count: int
    skipped_count: int
    mean_return: float
    median_return: float
    win_rate: float
    profit_factor: float | None
    max_drawdown: float
    sharpe_like: float | None
    mean_alpha: float | None
    median_alpha: float | None
    objective_score: float


@dataclass(slots=True)
class GridSearchResult:
    params: BacktestParams
    metrics: BacktestMetrics


@dataclass(slots=True)
class WalkForwardFoldResult:
    train_start: date
    train_end: date
    test_start: date
    test_end: date
    selected_params: BacktestParams
    train_metrics: BacktestMetrics
    test_metrics: BacktestMetrics


@dataclass(slots=True)
class WalkForwardResult:
    folds: list[WalkForwardFoldResult]
    aggregate_test_metrics: BacktestMetrics
    recommended_params: BacktestParams | None


def _find_entry_index(bars: list[DailyBar], *, signal_date: date) -> int | None:
    for idx, bar in enumerate(bars):
        if bar.trade_date > signal_date:
            return idx
    return None


def _benchmark_return(
    benchmark_bars: list[DailyBar] | None,
    *,
    entry_date: date,
    exit_date: date,
) -> float | None:
    if benchmark_bars is None:
        return None
    entry_bar = next((bar for bar in benchmark_bars if bar.trade_date >= entry_date), None)
    exit_bar = next((bar for bar in benchmark_bars if bar.trade_date >= exit_date), None)
    if entry_bar is None or exit_bar is None:
        return None
    if entry_bar.open <= 0:
        return None
    return (exit_bar.close / entry_bar.open) - 1.0


def _simulate_trade(
    signal: SignalEvent,
    *,
    bars: list[DailyBar],
    params: BacktestParams,
    round_trip_cost_fraction: float,
    benchmark_bars: list[DailyBar] | None,
) -> TradeResult | None:
    entry_idx = _find_entry_index(bars, signal_date=signal.filed_at.date())
    if entry_idx is None:
        return None

    entry_bar = bars[entry_idx]
    entry_price = entry_bar.open
    if entry_price <= 0:
        return None

    stop_price = (
        entry_price * (1.0 - params.stop_loss_pct) if params.stop_loss_pct > 0 else None
    )
    take_profit_price = (
        entry_price * (1.0 + (params.stop_loss_pct * params.take_profit_rr))
        if params.stop_loss_pct > 0 and params.take_profit_rr > 0
        else None
    )

    last_idx = min(entry_idx + params.hold_days - 1, len(bars) - 1)
    exit_idx = last_idx
    exit_price = bars[last_idx].close
    exit_reason = "time"

    for idx in range(entry_idx, last_idx + 1):
        bar = bars[idx]
        hit_stop = stop_price is not None and bar.low <= stop_price
        hit_take = take_profit_price is not None and bar.high >= take_profit_price
        if hit_stop and hit_take:
            # Conservative assumption when daily bars cannot resolve intraday order.
            exit_idx = idx
            exit_price = float(stop_price)
            exit_reason = "stop_and_take_same_day_stop_assumed"
            break
        if hit_stop:
            exit_idx = idx
            exit_price = float(stop_price)
            exit_reason = "stop"
            break
        if hit_take:
            exit_idx = idx
            exit_price = float(take_profit_price)
            exit_reason = "take_profit"
            break

    exit_bar = bars[exit_idx]
    gross_return = (exit_price / entry_price) - 1.0
    net_return = gross_return - round_trip_cost_fraction
    benchmark_return = _benchmark_return(
        benchmark_bars,
        entry_date=entry_bar.trade_date,
        exit_date=exit_bar.trade_date,
    )
    alpha_return = net_return - benchmark_return if benchmark_return is not None else None

    return TradeResult(
        packet_id=signal.packet_id,
        symbol=signal.symbol,
        signal_date=signal.filed_at.date(),
        entry_date=entry_bar.trade_date,
        exit_date=exit_bar.trade_date,
        hold_days=(exit_idx - entry_idx + 1),
        entry_price=entry_price,
        exit_price=exit_price,
        stop_price=stop_price,
        take_profit_price=take_profit_price,
        exit_reason=exit_reason,
        gross_return=gross_return,
        net_return=net_return,
        benchmark_return=benchmark_return,
        alpha_return=alpha_return,
    )


def _empty_metrics(*, skipped_count: int) -> BacktestMetrics:
    return BacktestMetrics(
        trade_count=0,
        skipped_count=skipped_count,
        mean_return=0.0,
        median_return=0.0,
        win_rate=0.0,
        profit_factor=None,
        max_drawdown=0.0,
        sharpe_like=None,
        mean_alpha=None,
        median_alpha=None,
        objective_score=float("-inf"),
    )


def _build_metrics(*, trades: list[TradeResult], skipped_count: int) -> BacktestMetrics:
    if not trades:
        return _empty_metrics(skipped_count=skipped_count)

    returns = [trade.net_return for trade in trades]
    wins = [value for value in returns if value > 0]
    losses = [value for value in returns if value < 0]
    mean_return = statistics.fmean(returns)
    median_return = statistics.median(returns)
    win_rate = len(wins) / len(returns)

    profit_factor: float | None = None
    if losses:
        profit_factor = sum(wins) / abs(sum(losses)) if wins else 0.0
    elif wins:
        profit_factor = float("inf")

    if len(returns) >= 2:
        stdev = statistics.pstdev(returns)
        sharpe_like = (mean_return / stdev) * math.sqrt(len(returns)) if stdev > 0 else None
    else:
        sharpe_like = None

    equity = 1.0
    peak = 1.0
    max_drawdown = 0.0
    for value in returns:
        equity *= 1.0 + value
        if equity > peak:
            peak = equity
        drawdown = (peak - equity) / peak if peak > 0 else 0.0
        if drawdown > max_drawdown:
            max_drawdown = drawdown

    alpha_values = [trade.alpha_return for trade in trades if trade.alpha_return is not None]
    mean_alpha = statistics.fmean(alpha_values) if alpha_values else None
    median_alpha = statistics.median(alpha_values) if alpha_values else None

    base_metric = mean_alpha if mean_alpha is not None else mean_return
    objective_score = base_metric * math.sqrt(len(returns))

    return BacktestMetrics(
        trade_count=len(trades),
        skipped_count=skipped_count,
        mean_return=mean_return,
        median_return=median_return,
        win_rate=win_rate,
        profit_factor=profit_factor,
        max_drawdown=max_drawdown,
        sharpe_like=sharpe_like,
        mean_alpha=mean_alpha,
        median_alpha=median_alpha,
        objective_score=objective_score,
    )


def run_backtest(
    signals: list[SignalEvent],
    *,
    bars_by_symbol: dict[str, list[DailyBar]],
    params: BacktestParams,
    benchmark_symbol: str = "SPY",
    transaction_cost_bps: float = 5.0,
    slippage_bps: float = 5.0,
) -> tuple[BacktestMetrics, list[TradeResult]]:
    round_trip_cost_fraction = (
        2.0 * (max(transaction_cost_bps, 0.0) + max(slippage_bps, 0.0)) / 10000.0
    )
    benchmark_bars = bars_by_symbol.get(benchmark_symbol.upper())

    trades: list[TradeResult] = []
    skipped_count = 0
    for signal in signals:
        if signal.score < params.min_score:
            skipped_count += 1
            continue
        if signal.open_market_buy_shares <= 0 or signal.open_market_net_shares <= 0:
            skipped_count += 1
            continue
        if signal.has_10b5_1_plan:
            skipped_count += 1
            continue
        if signal.has_equity_comp_event and signal.has_tax_withholding_language:
            skipped_count += 1
            continue

        bars = bars_by_symbol.get(signal.symbol)
        if not bars:
            skipped_count += 1
            continue

        trade = _simulate_trade(
            signal,
            bars=bars,
            params=params,
            round_trip_cost_fraction=round_trip_cost_fraction,
            benchmark_bars=benchmark_bars,
        )
        if trade is None:
            skipped_count += 1
            continue
        trades.append(trade)

    return _build_metrics(trades=trades, skipped_count=skipped_count), trades


def evaluate_parameter_grid(
    signals: list[SignalEvent],
    *,
    bars_by_symbol: dict[str, list[DailyBar]],
    parameter_grid: list[BacktestParams],
    benchmark_symbol: str = "SPY",
    transaction_cost_bps: float = 5.0,
    slippage_bps: float = 5.0,
) -> list[GridSearchResult]:
    results: list[GridSearchResult] = []
    for params in parameter_grid:
        metrics, _ = run_backtest(
            signals,
            bars_by_symbol=bars_by_symbol,
            params=params,
            benchmark_symbol=benchmark_symbol,
            transaction_cost_bps=transaction_cost_bps,
            slippage_bps=slippage_bps,
        )
        results.append(GridSearchResult(params=params, metrics=metrics))
    results.sort(key=lambda item: item.metrics.objective_score, reverse=True)
    return results


def run_walk_forward(
    signals: list[SignalEvent],
    *,
    bars_by_symbol: dict[str, list[DailyBar]],
    parameter_grid: list[BacktestParams],
    train_window_days: int = 365,
    test_window_days: int = 90,
    min_train_trades: int = 15,
    benchmark_symbol: str = "SPY",
    transaction_cost_bps: float = 5.0,
    slippage_bps: float = 5.0,
) -> WalkForwardResult:
    if not signals:
        return WalkForwardResult(
            folds=[],
            aggregate_test_metrics=_empty_metrics(skipped_count=0),
            recommended_params=None,
        )

    sorted_signals = sorted(signals, key=lambda signal: signal.filed_at)
    min_date = sorted_signals[0].filed_at.date()
    max_date = sorted_signals[-1].filed_at.date()
    fold_start = min_date + timedelta(days=train_window_days)

    folds: list[WalkForwardFoldResult] = []
    all_test_trades: list[TradeResult] = []
    selected_params: list[BacktestParams] = []

    while fold_start <= max_date:
        train_start = fold_start - timedelta(days=train_window_days)
        test_end_exclusive = fold_start + timedelta(days=test_window_days)

        train_signals = [
            signal
            for signal in sorted_signals
            if train_start <= signal.filed_at.date() < fold_start
        ]
        test_signals = [
            signal
            for signal in sorted_signals
            if fold_start <= signal.filed_at.date() < test_end_exclusive
        ]

        if not train_signals or not test_signals:
            fold_start = test_end_exclusive
            continue

        train_grid = evaluate_parameter_grid(
            train_signals,
            bars_by_symbol=bars_by_symbol,
            parameter_grid=parameter_grid,
            benchmark_symbol=benchmark_symbol,
            transaction_cost_bps=transaction_cost_bps,
            slippage_bps=slippage_bps,
        )
        eligible = [
            result
            for result in train_grid
            if result.metrics.trade_count >= min_train_trades
        ]
        if not eligible:
            fold_start = test_end_exclusive
            continue
        best_train = eligible[0]

        test_metrics, test_trades = run_backtest(
            test_signals,
            bars_by_symbol=bars_by_symbol,
            params=best_train.params,
            benchmark_symbol=benchmark_symbol,
            transaction_cost_bps=transaction_cost_bps,
            slippage_bps=slippage_bps,
        )
        all_test_trades.extend(test_trades)
        selected_params.append(best_train.params)
        folds.append(
            WalkForwardFoldResult(
                train_start=train_start,
                train_end=fold_start - timedelta(days=1),
                test_start=fold_start,
                test_end=test_end_exclusive - timedelta(days=1),
                selected_params=best_train.params,
                train_metrics=best_train.metrics,
                test_metrics=test_metrics,
            )
        )
        fold_start = test_end_exclusive

    aggregate_test_metrics = _build_metrics(trades=all_test_trades, skipped_count=0)
    if not selected_params:
        recommended_params = None
    else:
        counts: dict[BacktestParams, int] = {}
        for params in selected_params:
            counts[params] = counts.get(params, 0) + 1
        recommended_params = max(
            counts.keys(),
            key=lambda params: (counts[params], params.min_score, -params.hold_days),
        )

    return WalkForwardResult(
        folds=folds,
        aggregate_test_metrics=aggregate_test_metrics,
        recommended_params=recommended_params,
    )
