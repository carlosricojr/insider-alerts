from __future__ import annotations

from datetime import UTC, datetime

from insider_alerts.backtest.event_data import CanonicalEvent
from insider_alerts.backtest.event_study import (
    TradabilityConfig,
    compute_event_forward_returns,
    run_oos_event_study,
)
from insider_alerts.backtest.models import DailyBar


def _bar(day: str, *, open_: float, close: float, volume: float = 1_000_000.0) -> DailyBar:
    year, month, date_text = day.split("-")
    return DailyBar(
        symbol="MAT",
        trade_date=datetime(int(year), int(month), int(date_text), tzinfo=UTC).date(),
        open=open_,
        high=max(open_, close),
        low=min(open_, close),
        close=close,
        volume=volume,
    )


def _spy_bar(day: str, *, open_: float, close: float) -> DailyBar:
    year, month, date_text = day.split("-")
    return DailyBar(
        symbol="SPY",
        trade_date=datetime(int(year), int(month), int(date_text), tzinfo=UTC).date(),
        open=open_,
        high=max(open_, close),
        low=min(open_, close),
        close=close,
        volume=10_000_000.0,
    )


def _event(packet_id: str, filed_at: datetime, *, score: float = 90.0) -> CanonicalEvent:
    return CanonicalEvent(
        packet_id=packet_id,
        accession_number=packet_id.split("|", 1)[0],
        cik="0000000001",
        form_type="4",
        symbol="MAT",
        filed_at=filed_at,
        score=score,
        rationale={},
        cluster_packet_count=1,
        cluster_max_score=score,
    )


def test_compute_event_forward_returns_uses_next_day_entry_and_horizon_close() -> None:
    events = [_event("0000000001-25-000001|0000000001|4", datetime(2025, 1, 1, 23, 0, tzinfo=UTC))]
    bars_by_symbol = {
        "MAT": [
            _bar("2025-01-02", open_=10.0, close=11.0),
            _bar("2025-01-03", open_=11.0, close=12.0),
            _bar("2025-01-06", open_=12.0, close=13.0),
        ],
        "SPY": [
            _spy_bar("2025-01-02", open_=100.0, close=100.5),
            _spy_bar("2025-01-03", open_=100.5, close=101.0),
            _spy_bar("2025-01-06", open_=101.0, close=101.5),
        ],
    }
    observations = compute_event_forward_returns(
        events,
        bars_by_symbol=bars_by_symbol,
        horizons=[1, 3],
        benchmark_symbol="SPY",
        transaction_cost_bps=0.0,
        slippage_bps=0.0,
        tradability=TradabilityConfig(min_price=0.0, min_median_dollar_volume_20d=0.0),
    )

    by_horizon = {obs.horizon_days: obs for obs in observations}
    h1 = by_horizon[1]
    h3 = by_horizon[3]
    assert h1.trade_executed is True
    assert h1.entry_date.isoformat() == "2025-01-02"
    assert h1.exit_date.isoformat() == "2025-01-02"
    assert round(float(h1.net_return), 6) == 0.1
    assert h3.exit_date.isoformat() == "2025-01-06"
    assert round(float(h3.net_return), 6) == 0.3


def test_compute_event_forward_returns_is_timestamp_invariant_for_same_filing_date() -> None:
    events = [
        _event("0000000001-25-000001|0000000001|4", datetime(2025, 1, 1, 0, 1, tzinfo=UTC)),
        _event("0000000001-25-000002|0000000001|4", datetime(2025, 1, 1, 23, 59, tzinfo=UTC)),
    ]
    bars_by_symbol = {
        "MAT": [
            _bar("2025-01-02", open_=10.0, close=10.5),
            _bar("2025-01-03", open_=10.5, close=11.0),
        ],
        "SPY": [
            _spy_bar("2025-01-02", open_=100.0, close=100.1),
            _spy_bar("2025-01-03", open_=100.1, close=100.2),
        ],
    }
    observations = compute_event_forward_returns(
        events,
        bars_by_symbol=bars_by_symbol,
        horizons=[1],
        benchmark_symbol="SPY",
        transaction_cost_bps=0.0,
        slippage_bps=0.0,
        tradability=TradabilityConfig(min_price=0.0, min_median_dollar_volume_20d=0.0),
    )
    entry_dates = {obs.packet_id: obs.entry_date for obs in observations}
    assert entry_dates["0000000001-25-000001|0000000001|4"].isoformat() == "2025-01-02"
    assert entry_dates["0000000001-25-000002|0000000001|4"].isoformat() == "2025-01-02"


def test_compute_event_forward_returns_reports_tradability_and_exit_skips() -> None:
    event = _event("0000000001-25-000001|0000000001|4", datetime(2025, 1, 1, 12, 0, tzinfo=UTC))
    bars_by_symbol = {
        "MAT": [
            _bar("2025-01-02", open_=1.2, close=1.3, volume=2_000.0),
            _bar("2025-01-03", open_=1.3, close=1.4, volume=2_000.0),
        ],
        "SPY": [
            _spy_bar("2025-01-02", open_=100.0, close=100.1),
            _spy_bar("2025-01-03", open_=100.1, close=100.2),
        ],
    }
    observations_price_fail = compute_event_forward_returns(
        [event],
        bars_by_symbol=bars_by_symbol,
        horizons=[1],
        benchmark_symbol="SPY",
        transaction_cost_bps=0.0,
        slippage_bps=0.0,
        tradability=TradabilityConfig(min_price=2.0, min_median_dollar_volume_20d=500_000.0),
    )
    h1 = observations_price_fail[0]
    assert h1.trade_executed is False
    assert h1.skip_reason == "fails_tradability_price"

    observations_missing_exit = compute_event_forward_returns(
        [event],
        bars_by_symbol=bars_by_symbol,
        horizons=[5],
        benchmark_symbol="SPY",
        transaction_cost_bps=0.0,
        slippage_bps=0.0,
        tradability=TradabilityConfig(min_price=0.0, min_median_dollar_volume_20d=0.0),
    )
    h5 = observations_missing_exit[0]
    assert h5.trade_executed is False
    assert h5.skip_reason == "missing_exit"


def test_compute_event_forward_returns_reports_missing_benchmark() -> None:
    event = _event("0000000001-25-000001|0000000001|4", datetime(2025, 1, 1, 12, 0, tzinfo=UTC))
    bars_by_symbol = {
        "MAT": [
            _bar("2025-01-02", open_=10.0, close=10.5),
            _bar("2025-01-03", open_=10.5, close=11.0),
        ],
    }
    observations = compute_event_forward_returns(
        [event],
        bars_by_symbol=bars_by_symbol,
        horizons=[1],
        benchmark_symbol="SPY",
        transaction_cost_bps=0.0,
        slippage_bps=0.0,
        tradability=TradabilityConfig(min_price=0.0, min_median_dollar_volume_20d=0.0),
    )
    obs = observations[0]
    assert obs.trade_executed is False
    assert obs.skip_reason == "missing_benchmark"


def _build_daily_bars_for_month(
    symbol: str,
    *,
    start_day: int = 1,
    end_day: int = 28,
) -> list[DailyBar]:
    bars: list[DailyBar] = []
    price = 10.0
    for day in range(start_day, end_day + 1):
        trade_date = datetime(2025, 1, day, tzinfo=UTC).date()
        bars.append(
            DailyBar(
                symbol=symbol,
                trade_date=trade_date,
                open=price,
                high=price + 0.2,
                low=price - 0.2,
                close=price + 0.1,
                volume=5_000_000.0,
            )
        )
        price += 0.1
    return bars


def test_run_oos_event_study_builds_non_overlapping_folds() -> None:
    events = []
    for day in range(1, 13):
        events.append(
            _event(
                f"00000000{day:02d}-25-000001|0000000001|4",
                datetime(2025, 1, day, 20, 0, tzinfo=UTC),
                score=60.0 + day,
            )
        )
    bars_by_symbol = {
        "MAT": _build_daily_bars_for_month("MAT"),
        "SPY": _build_daily_bars_for_month("SPY"),
    }
    result = run_oos_event_study(
        events,
        bars_by_symbol=bars_by_symbol,
        horizons=[1],
        bucket_count=3,
        train_window_days=4,
        test_window_days=3,
        min_train_events=2,
        min_test_events=2,
        benchmark_symbol="SPY",
        transaction_cost_bps=0.0,
        slippage_bps=0.0,
        tradability=TradabilityConfig(min_price=0.0, min_median_dollar_volume_20d=0.0),
    )
    assert len(result.folds) >= 2
    for prev, current in zip(result.folds, result.folds[1:], strict=False):
        assert prev.test_end < current.test_start
        assert current.train_end < current.test_start


def test_run_oos_event_study_uses_train_only_scores_for_bucket_edges() -> None:
    # Train scores are in a low range; test scores are very high and should not affect edges.
    events = [
        _event(
            "0000000001-25-000001|0000000001|4",
            datetime(2025, 1, 1, 12, 0, tzinfo=UTC),
            score=10.0,
        ),
        _event(
            "0000000002-25-000001|0000000001|4",
            datetime(2025, 1, 2, 12, 0, tzinfo=UTC),
            score=20.0,
        ),
        _event(
            "0000000003-25-000001|0000000001|4",
            datetime(2025, 1, 3, 12, 0, tzinfo=UTC),
            score=30.0,
        ),
        _event(
            "0000000004-25-000001|0000000001|4",
            datetime(2025, 1, 4, 12, 0, tzinfo=UTC),
            score=40.0,
        ),
        _event(
            "0000000005-25-000001|0000000001|4",
            datetime(2025, 1, 5, 12, 0, tzinfo=UTC),
            score=90.0,
        ),
        _event(
            "0000000006-25-000001|0000000001|4",
            datetime(2025, 1, 6, 12, 0, tzinfo=UTC),
            score=95.0,
        ),
    ]
    bars_by_symbol = {
        "MAT": _build_daily_bars_for_month("MAT"),
        "SPY": _build_daily_bars_for_month("SPY"),
    }
    result = run_oos_event_study(
        events,
        bars_by_symbol=bars_by_symbol,
        horizons=[1],
        bucket_count=2,
        train_window_days=4,
        test_window_days=2,
        min_train_events=4,
        min_test_events=2,
        benchmark_symbol="SPY",
        transaction_cost_bps=0.0,
        slippage_bps=0.0,
        tradability=TradabilityConfig(min_price=0.0, min_median_dollar_volume_20d=0.0),
    )
    assert len(result.folds) == 1
    edges = result.folds[0].score_bucket_edges
    assert len(edges) == 1
    assert edges[0] < 50.0


def test_run_oos_event_study_builds_conviction_buckets_from_training_fold() -> None:
    events = [
        _event(
            f"00000000{day:02d}-25-000001|0000000001|4",
            datetime(2025, 1, day, 12, 0, tzinfo=UTC),
            score=50.0,
        )
        for day in range(1, 7)
    ]
    for index, event in enumerate(events, start=1):
        scale = float(index if index <= 4 else index * 100)
        event.rationale = {
            "holding_change_ratio": scale,
            "open_market_gross_value": scale * 1_000.0,
            "trade_pct_daily_turnover": scale / 10.0,
        }
    bars_by_symbol = {
        "MAT": _build_daily_bars_for_month("MAT"),
        "SPY": _build_daily_bars_for_month("SPY"),
    }

    result = run_oos_event_study(
        events,
        bars_by_symbol=bars_by_symbol,
        horizons=[1],
        bucket_count=2,
        train_window_days=4,
        test_window_days=2,
        min_train_events=4,
        min_test_events=2,
        benchmark_symbol="SPY",
        transaction_cost_bps=0.0,
        slippage_bps=0.0,
        tradability=TradabilityConfig(min_price=0.0, min_median_dollar_volume_20d=0.0),
        bucket_dimension="conviction",
    )

    assert len(result.folds) == 1
    assert result.folds[0].score_bucket_edges == [62.5]
    top = next(metric for metric in result.folds[0].bucket_metrics if metric.bucket_index == 2)
    assert top.total_events == 2


def test_run_oos_event_study_skips_fold_when_samples_are_too_small() -> None:
    events = [
        _event(
            "0000000001-25-000001|0000000001|4",
            datetime(2025, 1, 1, 12, 0, tzinfo=UTC),
            score=10.0,
        ),
        _event(
            "0000000002-25-000001|0000000001|4",
            datetime(2025, 1, 2, 12, 0, tzinfo=UTC),
            score=20.0,
        ),
        _event(
            "0000000003-25-000001|0000000001|4",
            datetime(2025, 1, 3, 12, 0, tzinfo=UTC),
            score=30.0,
        ),
    ]
    bars_by_symbol = {
        "MAT": _build_daily_bars_for_month("MAT"),
        "SPY": _build_daily_bars_for_month("SPY"),
    }
    result = run_oos_event_study(
        events,
        bars_by_symbol=bars_by_symbol,
        horizons=[1],
        bucket_count=2,
        train_window_days=2,
        test_window_days=1,
        min_train_events=5,
        min_test_events=2,
        benchmark_symbol="SPY",
        transaction_cost_bps=0.0,
        slippage_bps=0.0,
        tradability=TradabilityConfig(min_price=0.0, min_median_dollar_volume_20d=0.0),
    )
    assert result.folds == []
