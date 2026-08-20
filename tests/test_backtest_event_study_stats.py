from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from insider_alerts.backtest.event_data import CanonicalEvent
from insider_alerts.backtest.event_study import (
    TradabilityConfig,
    benjamini_hochberg_adjust,
    bootstrap_mean_alpha_ci,
    compute_score_bucket_monotonicity,
    run_oos_event_study,
)
from insider_alerts.backtest.models import DailyBar


def _event(*, index: int, filed_date: date, score: float) -> CanonicalEvent:
    accession = f"{index:010d}-25-000001"
    packet_id = f"{accession}|0000000001|4"
    return CanonicalEvent(
        packet_id=packet_id,
        accession_number=accession,
        cik="0000000001",
        form_type="4",
        symbol="MAT",
        filed_at=datetime(
            filed_date.year,
            filed_date.month,
            filed_date.day,
            20,
            0,
            tzinfo=UTC,
        ),
        score=score,
        rationale={},
        cluster_packet_count=1,
        cluster_max_score=score,
    )


def _bar(symbol: str, trade_date: date, *, price: float, volume: float) -> DailyBar:
    return DailyBar(
        symbol=symbol,
        trade_date=trade_date,
        open=price,
        high=price,
        low=price,
        close=price,
        volume=volume,
    )


def test_bootstrap_mean_alpha_ci_is_deterministic_with_fixed_seed() -> None:
    values = [0.01, -0.02, 0.03, 0.01, -0.01]
    ci1 = bootstrap_mean_alpha_ci(values, random_seed=42, iterations=300)
    ci2 = bootstrap_mean_alpha_ci(values, random_seed=42, iterations=300)
    assert ci1 == ci2


def test_compute_score_bucket_monotonicity_detects_direction() -> None:
    positive = compute_score_bucket_monotonicity(
        [0.001, 0.004, 0.009, 0.015],
        horizon_days=5,
        random_seed=7,
        iterations=600,
    )
    negative = compute_score_bucket_monotonicity(
        [0.015, 0.009, 0.004, 0.001],
        horizon_days=5,
        random_seed=7,
        iterations=600,
    )

    assert positive.non_negative is True
    assert (positive.spearman_rho or 0.0) > 0.9
    assert (positive.p_value_proxy or 1.0) < 0.30
    assert negative.non_negative is False
    assert (negative.spearman_rho or 0.0) < 0.0


def test_benjamini_hochberg_adjust_matches_fixed_fixture() -> None:
    adjusted = benjamini_hochberg_adjust(
        [
            ("h5|b1", 0.01),
            ("h5|b2", 0.04),
            ("h10|b1", 0.03),
            ("h10|b2", None),
        ]
    )
    assert adjusted["h5|b1"] == 0.03
    assert adjusted["h10|b1"] == 0.04
    assert adjusted["h5|b2"] == 0.04
    assert adjusted["h10|b2"] is None


def test_negative_control_stays_near_zero_on_synthetic_null() -> None:
    start = date(2025, 1, 1)
    events = [
        _event(
            index=index + 1,
            filed_date=start + timedelta(days=index),
            score=float((index % 10) * 10 + 5),
        )
        for index in range(70)
    ]
    bars_by_symbol = {
        "MAT": [
            _bar(
                "MAT",
                start + timedelta(days=index + 1),
                price=10.0,
                volume=5_000_000.0,
            )
            for index in range(100)
        ],
        "SPY": [
            _bar(
                "SPY",
                start + timedelta(days=index + 1),
                price=100.0,
                volume=20_000_000.0,
            )
            for index in range(100)
        ],
    }

    result = run_oos_event_study(
        events,
        bars_by_symbol=bars_by_symbol,
        horizons=[5],
        bucket_count=3,
        train_window_days=30,
        test_window_days=15,
        min_train_events=20,
        min_test_events=10,
        benchmark_symbol="SPY",
        transaction_cost_bps=0.0,
        slippage_bps=0.0,
        tradability=TradabilityConfig(min_price=0.0, min_median_dollar_volume_20d=0.0),
        random_seed=11,
        bootstrap_iterations=200,
        negative_control_iterations=200,
    )

    assert len(result.folds) >= 1
    assert len(result.negative_control) == 1
    summary = result.negative_control[0]
    assert summary.actual_top_bucket_mean_alpha is not None
    assert summary.null_mean_alpha is not None
    assert abs(float(summary.actual_top_bucket_mean_alpha)) < 1e-12
    assert abs(float(summary.null_mean_alpha)) < 1e-12
