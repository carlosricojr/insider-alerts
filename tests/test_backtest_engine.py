from datetime import UTC, date, datetime

import pytest

from insider_alerts.backtest.engine import (
    BacktestParams,
    evaluate_parameter_grid,
    run_backtest,
    run_walk_forward,
)
from insider_alerts.backtest.models import DailyBar, SignalEvent


def _bars(symbol: str) -> list[DailyBar]:
    return [
        DailyBar(
            symbol=symbol,
            trade_date=date(2026, 1, 1),
            open=10.0,
            high=10.2,
            low=9.8,
            close=10.0,
            volume=1_000_000.0,
        ),
        DailyBar(
            symbol=symbol,
            trade_date=date(2026, 1, 2),
            open=10.0,
            high=11.5,
            low=9.5,
            close=10.8,
            volume=1_000_000.0,
        ),
        DailyBar(
            symbol=symbol,
            trade_date=date(2026, 1, 5),
            open=10.9,
            high=11.2,
            low=10.7,
            close=11.0,
            volume=1_000_000.0,
        ),
        DailyBar(
            symbol=symbol,
            trade_date=date(2026, 1, 6),
            open=11.0,
            high=11.4,
            low=10.9,
            close=11.3,
            volume=1_000_000.0,
        ),
        DailyBar(
            symbol=symbol,
            trade_date=date(2026, 1, 7),
            open=11.2,
            high=11.6,
            low=11.0,
            close=11.5,
            volume=1_000_000.0,
        ),
    ]


def _signal(packet_id: str, signal_day: int) -> SignalEvent:
    return SignalEvent(
        packet_id=packet_id,
        symbol="ABC",
        filed_at=datetime(2026, 1, signal_day, 20, 0, tzinfo=UTC),
        score=95.0,
        open_market_buy_shares=10_000.0,
        open_market_net_shares=10_000.0,
        has_10b5_1_plan=False,
        has_equity_comp_event=False,
        has_tax_withholding_language=False,
        role_tier="chief_exec",
    )


def test_run_backtest_uses_conservative_stop_when_stop_and_take_hit_same_day() -> None:
    params = BacktestParams(min_score=90.0, hold_days=3, stop_loss_pct=0.05, take_profit_rr=2.0)
    metrics, trades = run_backtest(
        [_signal("p1", 1)],
        bars_by_symbol={"ABC": _bars("ABC"), "SPY": _bars("SPY")},
        params=params,
        benchmark_symbol="SPY",
        transaction_cost_bps=0.0,
        slippage_bps=0.0,
    )
    assert metrics.trade_count == 1
    assert trades[0].entry_date == date(2026, 1, 2)
    assert trades[0].exit_reason == "stop_and_take_same_day_stop_assumed"
    assert trades[0].net_return == pytest.approx(-0.05, rel=1e-9)


def test_run_backtest_exits_on_stop_only_day() -> None:
    bars = [
        DailyBar(
            symbol="ABC",
            trade_date=date(2026, 1, 2),
            open=10.0,
            high=10.4,
            low=9.4,
            close=9.6,
            volume=1_000_000.0,
        ),
        DailyBar(
            symbol="ABC",
            trade_date=date(2026, 1, 5),
            open=9.7,
            high=9.9,
            low=9.3,
            close=9.4,
            volume=1_000_000.0,
        ),
    ]
    params = BacktestParams(min_score=90.0, hold_days=2, stop_loss_pct=0.05, take_profit_rr=2.0)
    metrics, trades = run_backtest(
        [_signal("p1", 1)],
        bars_by_symbol={"ABC": bars},
        params=params,
        benchmark_symbol="",
        transaction_cost_bps=0.0,
        slippage_bps=0.0,
    )
    assert metrics.trade_count == 1
    assert trades[0].exit_reason == "stop"
    assert trades[0].exit_price == pytest.approx(9.5, rel=1e-9)


def test_run_backtest_exits_on_take_profit_only_day() -> None:
    bars = [
        DailyBar(
            symbol="ABC",
            trade_date=date(2026, 1, 2),
            open=10.0,
            high=11.1,
            low=9.8,
            close=10.9,
            volume=1_000_000.0,
        ),
        DailyBar(
            symbol="ABC",
            trade_date=date(2026, 1, 5),
            open=10.8,
            high=11.2,
            low=10.7,
            close=11.0,
            volume=1_000_000.0,
        ),
    ]
    params = BacktestParams(min_score=90.0, hold_days=2, stop_loss_pct=0.05, take_profit_rr=2.0)
    metrics, trades = run_backtest(
        [_signal("p1", 1)],
        bars_by_symbol={"ABC": bars},
        params=params,
        benchmark_symbol="",
        transaction_cost_bps=0.0,
        slippage_bps=0.0,
    )
    assert metrics.trade_count == 1
    assert trades[0].exit_reason == "take_profit"
    assert trades[0].exit_price == pytest.approx(11.0, rel=1e-9)


def test_evaluate_parameter_grid_sorts_by_objective() -> None:
    signals = [_signal("p1", 1), _signal("p2", 2)]
    grid = [
        BacktestParams(min_score=80.0, hold_days=1, stop_loss_pct=0.05, take_profit_rr=1.0),
        BacktestParams(min_score=95.0, hold_days=1, stop_loss_pct=0.05, take_profit_rr=1.0),
    ]
    results = evaluate_parameter_grid(
        signals,
        bars_by_symbol={"ABC": _bars("ABC"), "SPY": _bars("SPY")},
        parameter_grid=grid,
        benchmark_symbol="SPY",
        transaction_cost_bps=0.0,
        slippage_bps=0.0,
    )
    assert len(results) == 2
    assert results[0].metrics.objective_score >= results[1].metrics.objective_score


def test_run_walk_forward_returns_folds_and_recommended_params() -> None:
    signals = [_signal(f"p{i}", 1 + i) for i in range(12)]
    grid = [
        BacktestParams(min_score=70.0, hold_days=2, stop_loss_pct=0.05, take_profit_rr=1.0),
        BacktestParams(min_score=90.0, hold_days=3, stop_loss_pct=0.05, take_profit_rr=2.0),
    ]
    result = run_walk_forward(
        signals,
        bars_by_symbol={"ABC": _bars("ABC"), "SPY": _bars("SPY")},
        parameter_grid=grid,
        train_window_days=5,
        test_window_days=3,
        min_train_trades=1,
        benchmark_symbol="SPY",
        transaction_cost_bps=0.0,
        slippage_bps=0.0,
    )
    assert result.folds
    assert result.recommended_params is not None
