# Sprint 9 Review - Out-of-Sample Event Study for Alpha Validation

Status: **IMPLEMENTED AS AN EXPLORATORY DIAGNOSTIC**

Protocol boundary: this sprint does not alter or replace the locked confirmatory study in
`docs/research/SIGNAL-STUDY-2026-08-17-PREREG.md`. The `ops event-study` score/conviction buckets,
free CLI parameters, horizons, and Benjamini-Hochberg values are exploratory only. Confirmatory
claims come exclusively from `ops signal-study` on the frozen database, all 12 execution rules,
all 14 filters, and Bonferroni/Holm correction across the fixed family of 168 hypotheses.

## Objective
Establish a data-readiness and out-of-sample (OOS) diagnostic workflow for Form 4 signals that:
- avoids parameter overfitting,
- quantifies whether signal strength maps to forward excess returns,
- and produces clear go/no-go evidence for live strategy development.

## Problem Statement (Empirical Baseline)
Current actionable backtest output is not sufficient to conclude robust edge quality:
- One-year run generated many trades after price backfill, but benchmark-relative edge was negative.
- Walk-forward folds are currently `0` (no OOS fold evidence).
- Filing-date coverage in the current backtest dataset is clustered and non-contiguous (observed months: `2025-05`, `2025-06`, `2026-02`), which breaks meaningful walk-forward validation.
- Review payload shape is heterogeneous across historical packets (older packets missing many modern rationale fields), so feature availability must be audited before advanced bucketing.

## Scope
1. Build a canonical event-study pipeline focused on OOS alpha validation.
2. Add data-readiness diagnostics (coverage, continuity, feature completeness, tradability).
3. Evaluate forward returns and alpha by score/conviction buckets across multiple horizons.
4. Enforce strict TDD, reproducibility, and quality gates before any strategy tuning.
5. Update runbook with production-safe execution flow.

Scope clarifier:
- Score buckets are mandatory.
- Conviction buckets are conditional: only enabled when required rationale fields meet coverage threshold (default `>=80%` non-null in canonical events); otherwise report falls back to score-only buckets and marks conviction analysis as unavailable.
- All outputs are "as-of" reproducible: reports must include run timestamp, requested window, and DB path/hash metadata.

## Out of Scope
- Changing live decision policy thresholds in this sprint.
- Position sizing / portfolio construction optimization.
- Confirmatory testing or changes to the 12-rule execution family.
- Intraday execution model upgrades beyond the preregistered `ops signal-study` implementation.
- LLM policy/prompt changes.

## Verified Constraints From Current Codebase
- Signal source is `review_packets` joined to `filings` (`src/insider_alerts/backtest/data.py`).
- Existing backtest uses daily bars in `price_bars_daily` (`src/insider_alerts/backtest/prices.py`).
- Existing trade simulation logic is daily-bar based and conservative on stop/take ambiguity (`src/insider_alerts/backtest/engine.py`).
- Existing `ops backtest` can bootstrap filings and refresh prices, but OOS folds still require contiguous filing coverage (`src/insider_alerts/cli.py`).

## Deliverables
1. New event-study engine module (OOS-first).
2. New CLI command for event-study reports.
3. Readiness report (data coverage + quality gates).
4. JSON + CSV artifacts for reproducible analysis.
5. Test suite (unit + integration) with deterministic fixtures.
6. Runbook updates for execution and interpretation.

## Atomic Backlog (TDD-First)

### S09-001 Data Readiness Audit
Goal:
- Add a preflight audit that answers: "Do we have enough contiguous data to trust OOS results?"

Implementation:
- Add readiness utility in `src/insider_alerts/backtest/readiness.py`.
- Compute:
  - filing date min/max and monthly continuity,
  - event counts by month,
  - symbol tradability coverage,
  - price coverage window match,
  - rationale-feature availability percentages.
- Define explicit monthly continuity rule:
  - For each full calendar month intersecting requested window, require at least one Form 4 filing in `filings` and at least one canonical event candidate after dedupe.
  - Any internal month with zero count is a hard readiness failure.
- Continuity is evaluated on canonical pre-tradability events (not post-tradability) to avoid rejecting due solely to liquidity filters.
- Add minimum sample-size checks:
  - `min_total_canonical_events` (default 500),
  - `min_monthly_canonical_events` (default 20 for full months).
- Emit machine-readable readiness report.

Tests (write first):
- `tests/test_backtest_readiness.py`
  - detects missing months,
  - detects insufficient event counts,
  - detects missing price coverage,
  - handles empty DB safely.

Acceptance:
- Readiness report is deterministic and blocks OOS study when hard prerequisites fail.

### S09-002 Canonical Event Set and Dedupe Policy
Goal:
- Prevent overweighting duplicated filing packets for the same economic event.

Implementation:
- Add canonical event extractor `src/insider_alerts/backtest/event_data.py`.
- Define deterministic exploratory event key:
  - baseline key: `(accession_number, normalized_symbol, filed_date)`.
- For duplicate packets under that exploratory key:
  - keep deterministic representative (highest score, then stable tie-break by packet_id).
  - This differs from the frozen confirmatory cohort's earliest-decision representative and
    therefore cannot be substituted into the 168-hypothesis family.
  - record dedupe diagnostics.
- Preserve cluster signal:
  - add `cluster_packet_count` and `cluster_max_score` fields to canonical event outputs so dedupe does not discard potential multi-insider information.

Tests (write first):
- `tests/test_backtest_event_data.py`
  - duplicate packets collapse to one event,
  - tie-break behavior deterministic,
  - unsupported symbols excluded.

Acceptance:
- Canonical event counts are stable across repeated runs.

### S09-003 Forward Return/Alpha Computation Engine
Goal:
- Compute forward returns and benchmark-relative alpha per event and horizon with no lookahead.

Implementation:
- Add `src/insider_alerts/backtest/event_study.py`.
- Add explicit tradability eligibility filter (configurable):
  - default filters: actual next-session `entry_open >= 2.00`,
    `median_dollar_volume_20d >= 500000`.
  - include filter pass/fail reason in event-level output.
- Ensure no hidden survivorship/leakage behavior:
  - return calculations use bars available as-of event date only,
  - 20-day median dollar volume uses only completed sessions strictly before the entry session.
- Reuse existing entry convention:
  - entry = next trading day open after filing date.
  - filing-time granularity policy is fixed and conservative: regardless of intraday timestamp presence/absence, never allow same-day entry.
- For horizons `[1,3,5,10,20]`:
  - exit = close at horizon trading-day offset from entry.
- Compute:
  - gross return,
  - friction-adjusted return (configurable),
  - benchmark return (SPY default),
  - alpha = net - benchmark.
  - skip diagnostics by reason (`missing_entry`, `missing_exit`, `missing_benchmark`, `fails_tradability`).

Tests (write first):
- `tests/test_backtest_event_study.py`
  - correct entry/exit day logic,
  - no-lookahead enforcement,
  - benchmark alignment edge cases,
  - missing-bars skip accounting,
  - tradability filter behavior and diagnostics,
  - timezone/intraday timestamp invariance (same event date always maps to same next-day entry policy).

Acceptance:
- Event-level outputs reconcile with hand-calculated fixtures.

### S09-004 OOS Fold Evaluation (Event Study, Not Param Optimization)
Goal:
- Produce fold-level OOS evidence without fitting trade parameters.

Implementation:
- Add fold runner in `event_study.py`:
  - rolling windows (`train_window_days`, `test_window_days`),
  - train used only to set bucket cutoffs (quantiles),
  - test used for performance estimation.
- Report fold-level and aggregate metrics by bucket and horizon.
- Enforce leakage guards:
  - bucket boundaries computed from train fold only,
  - test fold metrics computed strictly on test events,
  - no fold-level normalization using full-sample statistics.

Tests (write first):
- fold generation correctness,
- no train/test overlap,
- quantile cutoffs derived from train only,
- fold skip rules for insufficient sample size.

Acceptance:
- At least three folds are required for a data-ready exploratory output; otherwise report is
  explicitly non-decision-grade. This label never confers confirmatory significance.

### S09-005 Statistical Inference and Robustness Outputs
Goal:
- Move from raw averages to uncertainty-aware edge evaluation.

Implementation:
- Add stats helpers:
  - mean/median alpha,
  - win rate,
  - bootstrap confidence intervals for mean alpha,
  - monotonicity check across score buckets (Spearman sign and significance proxy).
- Output per horizon and per bucket.
- Add skip-bias diagnostics:
  - per-bucket execution coverage rate,
  - per-bucket benchmark-availability rate.
- Add false-positive controls:
  - exploratory multiple-testing adjustment across bucket/horizon combinations
    (Benjamini-Hochberg FDR),
  - negative-control baseline (label permutation or shuffled event dates within fold) to estimate chance-level alpha.

Tests (write first):
- bootstrap determinism with fixed seed,
- monotonicity calculator correctness on synthetic monotone/non-monotone fixtures.
- FDR adjustment correctness on fixed p-value fixture.
- Negative-control metrics converge near zero edge on synthetic null dataset.

Acceptance:
- Report includes uncertainty bounds and monotonicity verdicts.

### S09-006 CLI Surface and Artifact Contracts
Goal:
- Provide a stable command for repeatable research and CI checks.

Implementation:
- Add `ops event-study` command in `src/insider_alerts/cli.py`.
- Inputs:
  - `--start-date`, `--end-date`,
  - `--horizons`,
  - `--bucket-count`,
  - `--train-window-days`, `--test-window-days`,
  - `--min-train-events`, `--min-test-events`,
  - `--min-total-canonical-events`, `--min-monthly-canonical-events`,
  - `--benchmark-symbol`,
  - `--transaction-cost-bps`, `--slippage-bps`,
  - `--min-price`, `--min-median-dollar-volume-20d`,
  - `--conviction-feature-coverage-min`,
  - `--random-seed`,
  - `--output-json`, `--output-csv`.
- Output includes:
  - readiness block,
  - dedupe diagnostics,
  - skip diagnostics,
  - fold summaries,
  - aggregate bucket/horizon metrics,
  - negative-control metrics and adjusted significance outputs,
  - explicit go/no-go recommendation from objective criteria.

Tests (write first):
- `tests/test_cli_extended.py` additions for command behavior and schema.

Acceptance:
- Command returns stable JSON schema and clear non-zero exit codes for failing hard gates.

### S09-007 Runbook and Analyst Interpretation Guide
Goal:
- Ensure outputs are actionable and not misread.

Implementation:
- Update `docs/runbook/BACKTESTING.md` with:
  - event-study workflow,
  - interpretation matrix,
  - failure modes,
  - "do not optimize parameters before OOS edge validation" gate.

Tests:
- Doc lint / link check (if available).

Acceptance:
- Runbook enables repeatable execution by another engineer without tribal knowledge.

## Quality Gates (Hard)

### Gate A: Build and Static Checks
- `ruff check` passes on touched files.
- Type and import hygiene clean.

### Gate B: Unit/Integration Test Gates
- New tests for readiness, dedupe, event study, stats, CLI pass.
- Existing backtest tests remain green.

### Gate C: Determinism and Reproducibility
- Identical inputs yield identical report artifacts (except timestamps).
- Bootstrap/inference routines use explicit seeds.
- Report includes metadata required for replay (`database_path`, run window, seed, command options).

### Gate D: Data Sufficiency Gates (must pass for decision-grade verdict)
- Contiguous monthly coverage in requested window (no missing full calendar month in canonical pre-tradability events).
- Minimum fold count >= 3.
- Minimum events per test fold and per top bucket/horizon.
- Execution coverage guard: skipped-for-missing-price events <= 25% in every evaluated horizon.
- Canonical sample-size guards (`min_total_canonical_events`, `min_monthly_canonical_events`) pass.
- At least three non-overlapping OOS folds pass their sample floors.

### Gate E: Exploratory Edge Flags (for "promising edge" label)
- Positive OOS mean alpha in top bucket for at least 2 core horizons (`5d`, `10d`).
- 95% bootstrap CI lower bound for top-bucket alpha is > -25 bps for those core horizons.
- Bucket monotonicity check is non-negative in aggregate OOS evidence for majority of horizons.
- FDR-adjusted significance for top-bucket alpha passes configured threshold (default `q <= 0.10`) in at least one core horizon.
- Top-bucket alpha materially exceeds negative-control baseline in core horizons.

Gate E is diagnostic only. Promotion still requires the locked `ops signal-study` gates: 30
entry-date clusters, 40 trades, positive 50-bps results, best-trade and best-month removal,
symbol concentration below 25%, both 5,000-iteration controls, a stable execution-rule
neighborhood, chronological 20-slot replay, and Bonferroni/Holm significance across all 168
hypotheses. Any missing item is non-confirmatory.
- All gate thresholds are emitted in report metadata (no hidden criteria).

## TDD Protocol (Strict)
For each atomic ticket:
1. Write failing tests first.
2. Implement minimal code to pass.
3. Refactor while keeping tests green.
4. Run full relevant suite.
5. Update sprint checklist and artifact samples.

No feature is marked complete without:
- tests proving behavior,
- artifact example,
- and explicit acceptance check passed.

## Implementation Order (Dependency-Safe)
1. `S09-001` readiness audit.
2. `S09-002` canonical event extraction/dedupe.
3. `S09-003` return engine.
4. `S09-004` OOS fold runner.
5. `S09-005` inference.
6. `S09-006` CLI and report contracts.
7. `S09-007` runbook updates.

## Risk Register and Mitigations
- Risk: sparse/clustered filing history yields false confidence.
  - Mitigation: hard readiness gates + explicit non-decision-grade output.
- Risk: duplicate packet inflation.
  - Mitigation: deterministic event-key dedupe + diagnostics in report.
- Risk: stale/incomplete rationale features in older packets.
  - Mitigation: feature availability audit and fallback bucketing on guaranteed fields (`score`).
- Risk: apparent edge from non-tradable microcap tails.
  - Mitigation: explicit tradability filter + configurable thresholds + diagnostics.
- Risk: biased results from high skip rates due missing price bars.
  - Mitigation: horizon-level skip caps and mandatory skip diagnostics in report.
- Risk: false discovery from many bucket/horizon comparisons.
  - Mitigation: FDR adjustment plus negative-control reporting.
- Risk: overfocus on in-sample results.
  - Mitigation: OOS-first report and gate checks for fold count and sample size.

## Go/No-Go Checklist
- [ ] All S09 tickets implemented with red-green TDD evidence.
- [ ] All quality gates A-E pass.
- [ ] Event-study output is reproducible on clean DB clone.
- [ ] Report indicates decision-grade dataset (otherwise block strategy conclusions).
- [ ] Runbook updated and validated by second-run reproduction.

## Definition of Done
Sprint is done when `ops event-study` can run end-to-end on a prepared DB and output:
- readiness verdict,
- OOS fold metrics by score bucket and horizon,
- confidence-aware edge interpretation,
- and an objective go/no-go label backed by explicit gates.

## Outcome
Implemented as an exploratory OOS diagnostic; it is not a confirmatory promotion gate.
