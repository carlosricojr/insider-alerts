# Live Insider Signal Strategy Study: Retrospective Preregistration

Locked: 2026-08-17, before querying forward returns for the live-approved cohort.

## Decision question

Does an alert actually delivered by the running service support a long-only strategy with
positive post-cost SPY-relative return, after controlling the complete, bounded hypothesis
family for multiple testing?

This is a retrospective preregistration: the signals already exist, but their live-cohort
forward returns were not inspected before this document was written. Time-split results are
therefore temporal robustness checks, not a claim of a pristine prospective holdout. Any
surviving rule still requires prospective paper validation after 2026-08-17.

## Frozen cohort and timestamp policy

- Database snapshot: `data/insider_alerts_research_2026-08-17.db`.
- Include only `review_packets.status = 'approve'` joined to `filings.source = 'sec_rss'` on
  accession number, CIK, and form type. Historical `sec_master_index` backfills are excluded.
- Signal time is `review_packets.updated_at`, the closest durable timestamp preceding
  notification. This is slightly optimistic because notification transport latency is not
  stored; latency stress adds 60 seconds to intraday entry times.
- For repeated approved packets with the same `(accession_number, normalized_symbol)`, keep the
  earliest decision only. On a given symbol and entry session, trade only the first eligible
  alert; later same-session alerts are diagnostic cluster evidence and do not create additional
  positions.
- Normalize symbols with the repository's canonical normalizer. Unsupported identifiers are
  ineligible and counted, never silently mapped.
- Do not use a bar until it is complete. Signals without the full required exit horizon are
  right-censored for that execution rule rather than scored as wins or losses.

## Mandatory tradability rules (not optimized filters)

- Entry price at least $2.00.
- Trailing 20-session median dollar volume at least $500,000, using only sessions completed
  before the signal/entry.
- Positive, finite OHLCV and benchmark bars.
- Round-trip friction: 20 bps primary and 50 bps stress. Intraday quotes, if available, use the
  next bar open; no midpoint fills.
- Equal-notional event returns. A rule cannot be promoted without a separate concurrent-position
  portfolio simulation.

## Execution-rule family (12)

Daily rules enter at the next regular-session open after the alert. Daily OHLC barrier ambiguity
is resolved against the strategy: if stop and target are touched on the same bar, the stop occurs
first.

| ID | Entry | Exit |
|---|---|---|
| E01 | next open | 1st-session close |
| E02 | next open | 5th-session close |
| E03 | next open | 10th-session close |
| E04 | next open | 20th-session close |
| E05 | next open | 5% stop / 5% target / 10-session maximum |
| E06 | next open | 5% stop / 10% target / 10-session maximum |
| E07 | next open | 10% stop / 10% target / 10-session maximum |
| E08 | next open | 10% stop / 20% target / 10-session maximum |
| E09 | next tradable 1-minute bar | same-session close |
| E10 | 5 minutes after first tradable time | same-session close |
| E11 | 15 minutes after first tradable time | same-session close |
| E12 | 30 minutes after first tradable time | same-session close |

For E09-E12, an alert outside regular hours, or too near the close to leave the requested delay,
maps to the next regular-session open plus the delay. If one-minute coverage cannot reach 80% of
otherwise tradable events, the intraday tier is non-decision-grade. Five-minute data may be used
only as a declared coarser sensitivity analysis, not substituted into the primary family.

## Filter family (14)

Every filter must use only information available by signal time. Missing feature values fail the
filter; they are not imputed from future or cross-sectional data.

| ID | Eligibility rule |
|---|---|
| F00 | no additional filter |
| F01 | open-market gross purchase value >= $100,000 |
| F02 | open-market gross purchase value >= $500,000 |
| F03 | `role_tier == chief_exec` |
| F04 | insider purchase value >= 1% of trailing daily dollar turnover |
| F05 | trailing 20-session stock return > 0 |
| F06 | trailing 20-session stock return <= 0 |
| F07 | prior stock close > trailing 50-session simple moving average |
| F08 | prior stock close <= trailing 50-session simple moving average |
| F09 | trailing 20-session annualized realized volatility <= 40% |
| F10 | trailing 20-session annualized realized volatility > 40% |
| F11 | prior SPY close > trailing 50-session SPY simple moving average |
| F12 | prior SPY close <= trailing 50-session SPY simple moving average |
| F13 | point-in-time market capitalization >= $2 billion |

F13 is reported as unavailable, rather than guessed, if a point-in-time share-count observation
filed no later than the alert cannot be joined. Current market capitalization is prohibited as a
historical proxy. Historical single-name option-chain/vol-surface filters are likewise prohibited
unless timestamped snapshots already existed; a chain fetched today cannot describe a past alert.

## Primary endpoint and family-wise error control

- Primary endpoint: mean post-20-bps trade alpha versus SPY over matched entry/exit timestamps.
- Economic co-gates: mean post-cost absolute return > 0, profit factor > 1, and no dependence on a
  single calendar month for more than half of total P&L.
- Statistical test: one-sided, date-clustered moving-block bootstrap of mean alpha under the null,
  with block length at least the rule's holding horizon and a fixed seed.
- Confirmatory family size: `12 execution rules * 14 filters = 168`.
- Bonferroni threshold: `0.05 / 168 = 0.000297619...` raw p-value. Holm-adjusted p-values are also
  reported over all 168 hypotheses. A missing/non-decision-grade hypothesis remains in the family
  denominator.
- Minimum evidence: 30 independent entry-date clusters and 40 trades. Smaller pockets are shown
  only as exploratory and cannot be promoted.

## Robustness and falsification gates

A statistically surviving rule must also:

1. remain positive at 50 bps round-trip friction;
2. have positive mean alpha both before and after 2026-07-01;
3. remain positive after removing its best trade and its best calendar month;
4. avoid a single-symbol contribution above 25% of total P&L;
5. beat shuffled signal-date and random matched-entry controls;
6. exhibit a stable neighborhood rather than an isolated stop/target optimum; and
7. pass a chronological portfolio replay with no overlapping same-symbol positions.

If no hypothesis passes the family-wise threshold and these gates, the conclusion is **no
demonstrated tradable edge**, not proof that the true effect is exactly zero. The forward action is
then to improve data capture and collect a prospective sample, not to widen the search family.
