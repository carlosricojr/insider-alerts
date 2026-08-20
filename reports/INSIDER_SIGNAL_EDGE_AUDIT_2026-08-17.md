# Insider Alerts Edge Audit — 2026-08-17

## Decision

**Do not trade the alerts immediately, and do not promote a production strategy yet.**

There is one credible strategy candidate worth a frozen prospective paper trial:

- long only;
- enter at the next regular-session open after an approved alert (same-day open is allowed only
  when the alert arrived premarket);
- entry price >= $2;
- trailing 20-session median dollar volume >= $500,000;
- 10% stop loss;
- 10% take profit;
- otherwise exit at the 10th session close;
- ignore later alerts while the same symbol is already in a position;
- assume at least 20 bps round-trip friction and require survival at 50 bps.

This is `E07|F00` in the locked study. It is a **paper-tradable candidate, not a statistically
confirmed live edge**. The evidence is unusually coherent for a retrospective signal study, but
the live family-wise statistical gate has not passed.

## What was tested

The study was locked before inspecting live-cohort returns in
`docs/research/SIGNAL-STUDY-2026-08-17-PREREG.md`.

- Primary family: 12 execution rules x 14 point-in-time filters = 168 hypotheses.
- Family-wise threshold: raw one-sided p <= `0.05 / 168 = 0.000297619`.
- Reported corrections: Bonferroni and Holm across all 168 hypotheses, including unavailable
  hypotheses.
- Primary endpoint: post-20-bps SPY-relative mean return.
- Live cohort: approved `sec_rss` alerts, using `review_packets.updated_at` as the durable signal
  timestamp and retaining the first approved accession/symbol packet.
- Historical falsification cohort: approved `sec_master_index` packets, with the synthetic signal
  clock forced to the filing-date close so entry cannot occur until the next session.
- Missing/unfinished horizons were censored, never scored as zero returns.
- Same-symbol overlapping positions were suppressed.
- Daily barriers use the adverse assumption when stop and target are touched on the same bar.
- Intraday rules use cached IBKR one-minute RTH trades and matched SPY minute bars.

## Running-service audit

- Live RSS review population: 29,797 rejects, 1,683 escalations, 185 approvals, and 40
  deadletters in the database snapshot.
- Canonical usable live alerts: 180 after accession/symbol deduplication and symbol validation.
- Average filing-to-decision latency for live approvals: about 410 seconds; observed range 46 to
  3,658 seconds.
- Notification delivery timestamps are not persisted. `updated_at` is therefore an optimistic
  proxy for alert receipt; the exact phone/network delay cannot be reconstructed.
- The approval stream is nonstationary: 77 of 185 packets arrived in the first 17 days of August.
  Most of those do not yet have a mature 10-session outcome.
- The logs show recurring invalid/non-common identifiers and intermittent IB/Yahoo context
  failures. Four live symbols could not be resolved by either daily provider.
- The final IB minute cache covered 167 of 178 requested symbol/session pairs (93.8%). The eleven
  misses remain explicit rather than being imputed. Depending on the delay rule, 112-113 of 125
  otherwise eligible signals have usable minute executions, so the intraday tier clears the
  preregistered 80% coverage gate.

## Critical data bug found and fixed

Yahoo silently returned monthly bars for long-lived symbols when queried with `range=max`, even
though `interval=1d` was requested. The old cache therefore mixed daily and monthly bars, making
"five-session" exits span weeks or months. Prior reports that used this cache are not decision
grade.

The provider now:

- requests an explicit `period1`/`period2` daily interval;
- rejects non-daily `dataGranularity` responses; and
- converts OHLC to the adjusted-close basis while preserving raw dollar turnover through the
  corresponding volume adjustment.

All study symbols were refetched into the isolated research snapshot after this fix.

## Live results

### Immediate and next-session behavior is mostly noise

| Rule | Trades | Mean net return | Mean SPY alpha | Profit factor | Raw p | Bonferroni p |
|---|---:|---:|---:|---:|---:|---:|
| Next open, exit first-session close (`E01\|F00`) | 104 | -0.835% | -0.670% | 0.50 | 0.977 | 1.000 |
| 0-minute entry, exit same-session close (`E09\|F00`) | 96 | -0.556% | -0.438% | 0.63 | 0.898 | 1.000 |
| 5-minute entry, exit same-session close (`E10\|F00`) | 95 | -0.081% | +0.028% | 0.92 | 0.461 | 1.000 |
| 15-minute entry, exit same-session close (`E11\|F00`) | 94 | +0.098% | +0.163% | 1.11 | 0.287 | 1.000 |
| 30-minute entry, exit same-session close (`E12\|F00`) | 95 | -0.201% | -0.138% | 0.80 | 0.732 | 1.000 |

There is no statistically or economically usable same-day edge. The superficially best 15-minute
rule becomes negative at 50 bps friction and loses its best month.

### The credible swing candidate

`E07|F00` — next open, 10% stop, 10% target, 10-session maximum:

- 55 non-overlapping mature live trades across 33 entry dates;
- +0.891% mean net return after 20 bps;
- +1.381% mean SPY alpha;
- +1.317% median net return and +1.739% median alpha;
- 50.9% win rate;
- 1.28 profit factor;
- +1.081% mean alpha at 50 bps friction;
- raw clustered-bootstrap p = 0.0431;
- Bonferroni and Holm adjusted p = 1.0.

It beats same-symbol random entry dates by +2.311 percentage points of alpha in the live cohort
(`p = 0.0092`, 5,000 matched simulations). That timing result is useful falsification evidence,
but it does not replace the preregistered 168-test family correction.

The live rule also fails one temporal robustness gate: post-2026-07-01 mean alpha is -1.41%.
That estimate contains only four mature trades because July was sparse and August alerts have not
completed the 10-session horizon. It is inconclusive rather than a stable negative regime result.

### The negative-trend filter

Adding trailing 20-session return <= 0 (`E07|F06`) improves the live point estimates:

- 42 trades / 30 entry dates;
- +1.191% mean net return;
- +1.695% mean alpha;
- profit factor 1.40;
- raw p = 0.0431; Bonferroni p = 1.0.

Its same-symbol negative-trend random-date uplift is +1.685 percentage points, but the matched
control is only borderline (`p = 0.0624`). The unfiltered rule is therefore the cleaner candidate.

### Market-cap and volatility/trend filters

- Point-in-time market cap was reconstructed from SEC companyfacts filed no later than the signal
  and the prior adjusted close. Coverage is 66.1%.
- The >= $2 billion filter has only 27 one-session trades and 17 longer-horizon trades. Its
  one-session result is -1.445% net / -1.123% alpha. It is both unhelpful and underpowered.
- The trend and realized-volatility filters produce some positive small pockets, but none passes
  the live family-wise threshold.
- Historical single-name option-chain and volatility-surface snapshots do not exist for these
  alerts. Alpha-core can map a current surface, but using today's chain for a past signal would be
  lookahead. No options-derived filter is claimed.

## Historical replay results and why they are not enough by themselves

The retrospective approved replay contains 643 canonical signals; 410 pass daily tradability and
price-coverage gates. It strongly supports the same E07 rule:

- 331 trades across 142 entry dates;
- +1.717% mean net return;
- +1.322% mean SPY alpha;
- 64.7% win rate;
- 1.82 profit factor;
- raw clustered-bootstrap p = 0.000080;
- Bonferroni p = 0.01344;
- Holm p = 0.01224;
- +1.022% mean alpha at 50 bps friction;
- +1.133% mean alpha after removing the best month;
- +1.285% mean alpha after removing the best trade;
- positive mean alpha on both sides of the 2025-08-12 temporal split;
- largest symbol contributes only 2.9% of positive P&L.

The same-symbol random-date control is also strong:

- actual mean alpha: +1.322%;
- random-date mean alpha: -0.470%;
- signal-timing uplift: +1.792 percentage points;
- one-sided matched-control p = 0.00020.

A conservative 20-slot replay retains 281 of 331 trades, skips 50 when full, and produces +18.7%
realized return. Its 5.35% realized-only drawdown omits intratrade mark-to-market drawdown and must
not be treated as a complete risk estimate.

The replay is not pristine out-of-sample evidence. The historical packets were approved in
February-April 2026, after their filing dates. The Quant agent was prohibited from web access and
used packet fields only, which limits direct outcome leakage, but the approval policy, conviction
percentiles, and scoring implementation were not replayed sequentially as-of each historical
event. The replay can falsify an unstable strategy; it cannot independently authorize live
capital.

## Operational recommendation

Freeze exactly one prospective paper sleeve after 2026-08-17:

1. Use `E07|F00` exactly as specified above. Do not add the negative-trend, market-cap, option, or
   volatility filters during the trial.
2. Use 20 equal-notional slots. A 5% notional slot with a 10% stop caps planned per-trade loss at
   roughly 0.5% of sleeve capital before gaps and slippage.
3. Skip a new alert if the symbol is already held or all 20 slots are occupied.
4. Persist the actual notification-send and client-receipt timestamps, policy version, model/prompt
   hash, and realized fill/spread. The current `updated_at` proxy is insufficient for execution
   attribution.
5. Persist an alpha-core option-chain/surface snapshot at alert time if options features are to be
   tested later. Do not retroactively enrich old alerts with current chains.
6. Promote from paper only after at least 100 new post-2026-08-17 trades and 60 independent entry
   dates, with positive 50-bps results, positive pre/post split alpha, same-symbol random-date
   outperformance, and a one-sided date-clustered p < 0.05 for this single frozen hypothesis.
7. Treat any parameter or filter change as a new registered hypothesis and reset its prospective
   sample clock.

Until that gate passes, the correct production action is **no automated trading and no increase in
alert volume**. The current alerts are not useful as immediate buys; only the delayed swing sleeve
has enough coherent evidence to justify a controlled forward paper test.

## Reproduction

```powershell
# Live cohort, including the cached IB one-minute tier and matched-date controls
uv run --extra dev python -m insider_alerts.cli ops signal-study `
  --cohort live `
  --database-path data/insider_alerts_research_2026-08-17.db `
  --start-date 2026-02-11 --end-date 2026-08-17 `
  --bootstrap-iterations 50000 `
  --matched-control-iterations 5000 `
  --output-json reports/live_signal_study_2026-08-17.json

# Retrospective approval replay (daily tier only)
uv run --extra dev python -m insider_alerts.cli ops signal-study `
  --cohort historical-replay `
  --database-path data/insider_alerts_research_2026-08-17.db `
  --start-date 2025-02-11 --end-date 2026-02-11 `
  --bootstrap-iterations 50000 `
  --matched-control-iterations 5000 `
  --output-json reports/historical_approval_replay_2025-02-11_2026-02-11.json
```

Validation at PR revision `cd70ca4`: Ruff and strict mypy pass; 188 tests pass on Windows and the
cross-platform coverage gate is 71%. Later review-remediation commits must repeat these gates.
