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

Date window behavior:
- Default (no date flags): runs on the last 365 days ending today.
- Explicit override: pass both `--start-date` and `--end-date` together.
- Backtest checks local filing coverage for the requested window.
- If coverage is missing (or there are no local signals), it runs SEC historical Form 4 backfill from quarterly `master.idx` files for the window, then runs bounded enrich/enqueue bootstrap batches before retrying signal load.
- Price data refresh: missing/stale symbol histories are requested from the provider and cached.
- Manual historical ingest command:
  - `uv run python -m insider_alerts.cli sec backfill --start-date 2025-01-01 --end-date 2025-12-31`

Provider safety defaults:
- `MARKET_DATA_RATE_LIMIT_PER_SECOND` defaults to `1.0` request/sec.
- `MARKET_DATA_RETRY_ATTEMPTS` defaults to `3` with bounded exponential backoff.
- SEC ingestion uses `SEC_RATE_LIMIT_PER_SECOND` (default `5.0`, capped at `10.0`) with retry/backoff via `SecHttpClient`.
- Cached data is reused to avoid repeated full-history requests on every run.

## Interpreting Output
- `best_in_sample_*`: useful for diagnostics only, not deployment by itself.
- `walk_forward_aggregate_metrics`: primary decision metric set.
- `walk_forward_recommended_params`: stable parameter choice from fold winners.
- `top_grid_results`: sanity-check shape of parameter landscape.
- `price_errors`: data quality/network visibility.

## Event Study Workflow (OOS-First)
Before tuning or trusting strategy-level backtest parameters, run event-study validation:

```powershell
uv run python -m insider_alerts.cli ops event-study `
  --horizons "1,3,5,10,20" `
  --bucket-count 5 `
  --train-window-days 365 `
  --test-window-days 90 `
  --min-train-events 100 `
  --min-test-events 25 `
  --min-total-canonical-events 500 `
  --min-monthly-canonical-events 20 `
  --benchmark-symbol SPY `
  --transaction-cost-bps 5 `
  --slippage-bps 5 `
  --min-price 2 `
  --min-median-dollar-volume-20d 500000 `
  --conviction-feature-coverage-min 0.8 `
  --random-seed 7 `
  --output-json reports/event_study_latest.json `
  --output-csv reports/event_study_latest.csv
```

What this command does:
- Builds canonical events (deduped by accession/symbol/filed-date).
- Runs readiness checks (coverage continuity, sample floors, feature coverage, price coverage).
- Computes OOS bucketed alpha by horizon using train-only quantile cutoffs.
- Adds uncertainty outputs (bootstrap CI), monotonicity checks, FDR-adjusted q-values, and
  negative-control baselines.
- Emits a `go_no_go` block:
  - `promising_edge`
  - `no_go`
  - `non_decision_grade` (hard gates failed).

This command is explicitly exploratory and reports
`analysis_class=exploratory_oos_diagnostic`. Its adjustable parameters and FDR values cannot
override the locked 168-hypothesis Bonferroni/Holm result from `ops signal-study`.

Hard gate behavior:
- `ops event-study` exits with code `3` when the run is not decision-grade.
- This is intentional; do not proceed to strategy tuning when hard gates fail.

## Interpretation Matrix
- `promising_edge`: dataset is decision-grade and OOS edge gates passed. Candidate for controlled live pilot design.
- `no_go`: dataset is decision-grade but edge gates failed. Improve feature set/filters before pilot.
- `non_decision_grade`: data sufficiency/coverage quality gates failed. Backfill/repair data first.

## Failure Modes To Treat As Blocking
- Missing internal months in requested window.
- Too few canonical events overall or in full months.
- Too few folds/events per fold.
- High missing-price execution failure rates.
- Top-bucket alpha not robust after CI/FDR/negative-control checks.

## Process Gate
- Do not optimize stop-loss/take-profit/hold-day grids before OOS event-study is decision-grade.
- Parameter search is downstream of edge validation, not a substitute for it.

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
- Stooq terms and service limits/no-availability guarantee: https://stooq.com/pp/
- SEC fair access guidance (automation/rate limits): https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data
- SEC Form 4 filing timing rules: https://www.sec.gov/about/forms/form4data.pdf
- Probability of Backtest Overfitting (Bailey et al., 2014): https://www.davidhbailey.com/dhbpapers/probability_of_backtest_overfitting.pdf
- Deflated Sharpe Ratio (Bailey & de Prado): https://www.davidhbailey.com/dhbpapers/deflated_sharpe_ratio.pdf
