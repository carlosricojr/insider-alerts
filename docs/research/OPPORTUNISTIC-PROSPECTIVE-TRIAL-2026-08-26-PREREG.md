# Opportunistic-insider E07 prospective trial

Registry ID: `OPP-E07-V1`<br>
Status: draft; activates only through the procedure below<br>
Drafted: 2026-08-26, before implementation or inspection of challenger outcomes<br>
Supersedes: nothing; the existing E07/F00 canary and its preregistration remain unchanged

## Decision and scope

Does the already-frozen E07/F00 strategy have positive post-cost SPY-relative mean return when a
live-approved open-market purchase is made by a prospectively classifiable opportunistic insider?

This trial tests one literature-selected filter on a new, non-overlapping prospective sample. It
does not optimize entry delay, stop, target, holding period, market cap, trend, volatility, option
surface, role, or purchase size. Those values may be captured for future research but are not
eligible covariates, subgroups, interactions, or decision criteria here.

## Activation and immutable cohort boundary

The draft becomes active only after all of the following occur:

1. point-in-time evidence capture and the owner-history classifier are implemented, tested,
   independently reviewed, merged to `origin/main`, and deployed invisibly;
2. the complete inference executable and its environment are implemented, tested, independently
   reviewed, and merged before activation;
3. an append-only activation record binds the registry definition, preregistration, both JSON
   Schemas, inference executable, and dependency lock SHA-256 digests; schema version; merged Git
   commit; policy hash; classifier version; UTC activation timestamp; and enrollment sequence;
4. the evidence store is empty for this registry ID and passes an integrity check; and
5. no challenger outcomes from on/after the proposed activation boundary have been inspected.

Only signals first observed at or after the sealed activation timestamp are eligible. Earlier
signals, including all current canary observations, never count. A signal missed during a capture
pause is recorded as missed and cannot be enrolled later. An amendment that changes cohort,
strategy, classifier, endpoint, test, alpha, sample size, or decision gates retires this registry
entry and requires a new ID and fresh activation boundary.

The registry definition SHA-256 is computed over RFC 8785 canonical JSON after removing the
top-level `status` and `activation` members. This avoids a self-referential digest while keeping the
scientific definition stable across activation. The first enrolled position accession is not
knowable at activation; it is sealed by confirmatory sequence 1 in its immutable enrollment
snapshot.
An opportunistic signal is first recorded as `pending_entry_selection`. After the complete entry
date is deterministically ranked, a new immutable snapshot supersedes it with `enrolled`,
`overlap_suppressed`, or `capacity_suppressed`. Only `enrolled` positions receive a positive,
gap-free confirmatory sequence, assigned transactionally in ascending rank; every other state has a
null sequence. The original point-in-time snapshot is never rewritten.

## Base signal and execution policy

The control and challenger use the exact live-canary E07/F00 policy in
[`LIVE_CANARY.md`](../runbook/LIVE_CANARY.md):

- future `sec_rss` review approvals only;
- next regular-session opening auction, with signals seen at/after 09:20 ET deferred;
- prior close from $2 through $200 and trailing 20-session median dollar volume of at least
  $500,000, using only completed pre-signal sessions;
- no overlapping same-symbol exposure;
- 10% stop, 10% target, otherwise session-10 close; shadow same-bar collision charged to stop;
- a separately capacity-limited 20-slot challenger book, using the existing SHA-256 rank over the
  policy salt, entry session, packet, accession, and symbol; $200 planning notional and whole-share
  eligibility are retained, while primary return aggregation remains equal-notional; and
- 20 bps round trip primary cost, 50 bps stress cost.

The control ledger records every otherwise eligible E07/F00 signal and its opened, overlap-, or
capacity-suppressed outcome; at most 20 control positions are open. The challenger independently
applies the same 20-slot, same-symbol, and ascending deterministic-rank policy within the
opportunistic eligible set. Neither book changes live slot selection or orders.

## Point-in-time owner classification

Classification follows the trader-level rule in Cohen, Malloy, and Pomorski, with explicit states
for records their study would not partition. It uses reporting-owner CIK, never a normalized name.

At the start of calendar year `Y`, use only non-derivative open-market purchase or sale transaction
codes `P` and `S` whose SEC filing was public before `Y-01-01T00:00:00 America/New_York` and whose
transaction date falls in `Y-3`, `Y-2`, or `Y-1`. Use the EDGAR acceptance timestamp when present;
when the quarterly bulk data exposes only a filing date, include dates no later than December 31 of
`Y-1`. For each owner:

1. `unpartitionable`: the owner has no qualifying trade in any one of the three preceding calendar
   years, or the required SEC archive/history coverage is incomplete;
2. `routine`: the owner has qualifying trades in all three years and the intersection of the three
   sets of transaction calendar months is non-empty;
3. `opportunistic`: the owner has qualifying trades in all three years but that month intersection
   is empty.

Once an owner becomes `routine`, the owner remains routine for later years, matching the published
trader-level rule. Otherwise classification is recalculated only at the next calendar-year cutoff;
current-year trades cannot change current-year status. Amendments public after a cutoff cannot
rewrite that cutoff; a later classification record may reference the correction.

Because the SEC quarterly bulk archive begins in 2006, it cannot by itself prove that an owner did
not become routine earlier under the absorbing rule. Before activation, the history builder must
acquire earlier authoritative Section 16 history where available and define its verified coverage
boundary. If an owner's pre-boundary state cannot be established, that owner is left-censored and
must remain `unpartitionable`; an absence of bulk rows is never evidence of opportunism.

A confirmatory event must contain exactly one reporting-owner CIK associated with the qualifying
purchase. Multiple-owner filings are `ambiguous_multi_owner`; missing CIKs and any failure to map
transaction ownership are `unpartitionable`. These states remain in the evidence ledger but are
excluded from the challenger. No state may be converted after signal time to enroll an event.

## Confirmatory hypothesis family

There is one primary hypothesis:

- `H-OPP-ALPHA`: among challenger trades, mean 20-bps post-cost SPY-relative return over matched
  E07 entry and exit timestamps is greater than zero.

The new family contains one test. In recognition of the program's earlier filter search, this
trial spends only `alpha = 0.025` rather than 0.05. Its Bonferroni threshold is therefore
`0.025 / 1 = 0.025`. It does not borrow alpha, observations, or outcomes from the earlier
168-hypothesis family. Routine, unpartitionable, and full-control results are diagnostics and
falsification context, not additional confirmatory tests.

## Estimation and single terminal look

- Unit: one eligible position after same-symbol overlap suppression.
- Primary endpoint: equal-notional arithmetic mean of trade return net of 20 bps minus matched SPY
  return over the same timestamps.
- Statistical test: one-sided circular moving-block bootstrap over chronologically ordered
  entry-date clusters, block length 10 dates, 10,000 resamples, deterministic seed `260826`.
  Within a resample, concatenate every trade from each sampled date and calculate the trade-weighted
  mean, preserving within-date dependence and unequal cluster sizes. For the null distribution,
  subtract the observed all-trade mean from every trade before sampling; truncate the final block
  to exactly the original number of date clusters. The raw p-value is
  `(1 + count(null_bootstrap_mean >= observed_mean)) / 10001`.
- Confidence interval: two-sided 95% percentile interval from the otherwise identical uncentered
  circular moving-block resamples, reported for effect-size context and not substituted for the
  registered p-value.
- Cohort freeze: after each entry date is complete, freeze the cohort on the first date for which
  cumulative enrollment contains at least 100 challenger positions and at least 60 distinct entry
  dates. Include every enrolled position on that boundary date, even when this exceeds 100. The
  trigger uses only entry/enrollment state, never exit timing or returns.
- Terminal information time: after cohort freeze, wait until every frozen position has a final E07
  outcome and all integrity checks pass, then seal the terminal dataset digest before any aggregate
  outcome calculation.
- Enrollment deadline: convert `activated_at_utc` to `America/New_York`, add exactly 18 calendar
  months while preserving the local wall-clock time, and convert the result back to UTC. If the
  activation day does not exist in the target month, use that target month's final calendar day.
  If the target wall-clock time is in a daylight-saving gap, advance by the gap to the first valid
  local instant; if it is ambiguous, use the second occurrence (`fold=1`). Signals first observed
  at or after that UTC instant are excluded. If the cohort-freeze thresholds have not been reached
  before the deadline, stop admitting new signals, allow every pre-deadline
  `pending_entry_selection` record to reach its deterministic entry-selection result, and only then
  return `KILL` with reason `insufficient_enrollment` without calculating a confirmatory p-value or
  outcome aggregates.
- There are no confirmatory interim p-values, optional stopping, extensions, or second looks.
  Health and exposure counts may be monitored; return aggregates and inferential statistics remain
  unavailable until the sealed terminal look.

If data integrity is invalid at the information time, the result is `INVALID`, not a pass or fail,
and no outcome is inspected until the integrity issue is adjudicated. A correction made without
outcome access may produce a new sealed digest under the same look; otherwise the trial is retired.

## Economic and robustness co-gates

`PROMOTE_RECOMMENDED` requires the primary p-value at or below 0.025 and every gate below:

1. mean absolute return net of 20 bps is positive;
2. mean SPY-relative return remains positive at 50 bps round-trip friction;
3. profit factor at 20 bps exceeds 1.0;
4. mean 50-bps alpha is positive in both chronological halves, split before any outcomes are read
   at the median entry-date boundary;
5. mean 50-bps alpha remains positive after removing the best trade and, separately, the best
   calendar month;
6. no symbol supplies more than 25% of total positive P&L and no calendar month supplies more than
   50% of total net P&L;
7. the chronological 20-slot equal-notional replay, with no overlapping same-symbol positions, has
   positive net return at 50 bps; and
8. timestamp ordering, snapshot hashes, SEC archive coverage, classification provenance, outcome
   completeness, and shadow-book reconciliation all pass.

The full E07/F00 control and routine subgroup must be reported with counts, effect sizes, and
confidence intervals as descriptive falsification context. They cannot rescue a failed primary
test or create a different promoted rule.

## Decision rule

- `COLLECTING`: cohort freeze or final outcome maturity has not been reached, the 18-month deadline
  has not failed enrollment, and integrity is valid.
- `PROMOTE_RECOMMENDED`: primary statistical and every economic/robustness gate pass.
- `KILL`: the valid single terminal look fails any statistical or co-gate, or the enrollment
  deadline is reached without the frozen sample floor. This means the filter is not supported for
  promotion under this policy and sample; it does not prove a universal zero effect.
- `INVALID`: preregistration boundary, point-in-time provenance, data integrity, or outcome-blinding
  was violated.

Promotion is never automatic. A recommendation permits a separate reviewed proposal; it does not
alter the live canary, send orders, change capital, or authorize a new statistical family.

## Disallowed analyses before the decision

- challenger return, alpha, hit rate, profit factor, drawdown, p-value, confidence interval, or
  outcome-ranked examples;
- slicing challenger outcomes by market cap, role, trend, volatility, options, purchase size,
  month, symbol, sector, latency, or any other feature;
- changing missingness, classification, costs, barriers, holding horizon, overlap handling, or
  capacity after observing any challenger outcome; and
- treating exploratory telemetry or the existing canary's pre-activation results as evidence for
  `H-OPP-ALPHA`.

Operational staff may inspect individual fills and exits only as necessary to reconcile broker and
shadow state. Such access is logged and does not expose aggregate challenger performance.

## References

- Lauren Cohen, Christopher Malloy, and Lukasz Pomorski,
  [*Decoding Inside Information*](https://www.nber.org/papers/w16454), Journal of Finance 67
  (2012), 1009–1043.
- Campbell R. Harvey, Yan Liu, and Heqing Zhu,
  [*... and the Cross-Section of Expected Returns*](https://www.nber.org/papers/w20592), Review of
  Financial Studies 29 (2016), 5–68.
- U.S. Securities and Exchange Commission,
  [Insider Transactions Data Sets](https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets).
