# Backtesting Runbook

This runbook explains how to evaluate whether pre-LLM insider signals are economically useful, while minimizing overfitting risk.

## Goal
- Measure if score-based signals produce positive expected returns after realistic frictions.
- Identify robust hold-time and risk/reward settings.
- Keep the process walk-forward and out-of-sample first.

## Data Flow
1. Read scored signals from SQLite (`review_packets` + `filings` join).
2. Pull/cache daily OHLCV bars per symbol in SQLite (`price_bars_daily`).
3. Simulate long-only event trades:
   - Entry: next trading day open after filing date.
   - Exit: first of stop-loss, take-profit, or max hold-day close.
4. Subtract round-trip costs and slippage.
5. Compute benchmark-relative alpha (default benchmark `SPY`).
6. Evaluate parameter grid in-sample and with walk-forward folds.

## Why this avoids common backtest mistakes
- No lookahead on entry timing (next-day open).
- Conservative intraday ambiguity handling:
  - If stop and take-profit are both touched on the same daily bar, stop is assumed first.
- Friction-aware returns:
  - net return = gross return - round-trip(cost + slippage).
- Walk-forward selection:
  - Parameter selection uses train window only, then tested out-of-sample.
- Trade-count floor per fold:
  - Skips fragile train fits with too few trades.

## Command
```powershell
uv run python -m insider_alerts.cli ops backtest `
  --start-date 2024-01-01 `
  --end-date 2026-12-31 `
  --min-score-grid "70,80,90" `
  --hold-days-grid "3,5,10,20" `
  --stop-loss-grid "0.03,0.05" `
  --take-profit-rr-grid "1.5,2.0,3.0" `
  --transaction-cost-bps 5 `
  --slippage-bps 5 `
  --train-window-days 365 `
  --test-window-days 90 `
  --min-train-trades 15 `
  --benchmark-symbol SPY `
  --output-json reports/backtest_latest.json
```

## Interpreting Output
- `best_in_sample_*`: useful for diagnostics only, not deployment by itself.
- `walk_forward_aggregate_metrics`: primary decision metric set.
- `walk_forward_recommended_params`: stable parameter choice from fold winners.
- `top_grid_results`: sanity-check shape of parameter landscape.
- `price_errors`: data quality/network visibility.

## Deployment Gate
Promote a parameter set only if:
1. Walk-forward mean alpha is positive and stable across folds.
2. Max drawdown and win/loss profile are acceptable for your risk budget.
3. Result remains positive under stricter friction assumptions.
4. Performance is not concentrated in a single short time bucket.

## Suggested Robustness Checks
- Friction stress:
  - rerun with higher `transaction_cost_bps` and `slippage_bps`.
- Regime stress:
  - split results by high-volatility windows.
- Sensitivity:
  - confirm neighboring parameters are similar; avoid sharp isolated optima.
- Benchmark variants:
  - rerun against `SPY` and sector ETF proxies.

## References
- SEC Form 4 filing timing rules: https://www.sec.gov/about/forms/form4data.pdf
- Probability of Backtest Overfitting (Bailey et al., 2014): https://www.davidhbailey.com/dhbpapers/probability_of_backtest_overfitting.pdf
- Deflated Sharpe Ratio (Bailey & de Prado): https://www.davidhbailey.com/dhbpapers/deflated_sharpe_ratio.pdf
