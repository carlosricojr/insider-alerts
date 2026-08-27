# Opportunistic-insider E07 prospective trial

Registry ID: `OPP-E07-V1`<br>
Status: draft; activates only through the procedure below<br>
Drafted: 2026-08-26, before implementation or inspection of challenger outcomes<br>
Draft correction: 2026-08-26, before activation and with zero challenger snapshots, to match the
published finite-observation state machine rather than require unknowable lifetime history<br>
Draft correction: 2026-08-27, before activation and with zero challenger snapshots, to freeze the
inference byte stream, ordering, percentile, tie, and economic-gate arithmetic<br>
Draft correction: 2026-08-27, before activation and with zero challenger snapshots, to freeze
pre-open entry-date completion, executable evidence readiness, input watermarks, and lapse
missingness semantics<br>
Draft correction: 2026-08-27, before activation and with zero challenger snapshots, to bind
schedule revisions, healthy-poll receipts, pre-signal eligibility history, the final in-transaction
decision clock,
and outcome-independent prior-book occupancy to each entry-date seal<br>
Draft correction: 2026-08-27, before activation and with zero challenger snapshots, to bind the
terminal outcome materialization time, exact stock/SPY bar and schedule provenance, and terminal
missing-session behavior<br>
Draft correction: 2026-08-27, before activation and with zero evidence snapshots or challenger
candidates, to freeze prospective full-control and routine diagnostic membership, provenance,
outcome authority, failure isolation, and reconciliation semantics<br>
Draft correction: 2026-08-27, before activation and with zero evidence snapshots or challenger
candidates, to bind the terminal-dataset producer, permanent cohort-freeze behavior, explicit
  diagnostic completeness accounting, stable multi-store sealing snapshot, and non-aggregating
  pre-seal validation<br>
Draft correction: 2026-08-27, before activation and with zero evidence snapshots or challenger
candidates, to make explicit that diagnostic receipt or membership catch-up is non-blocking at
challenger terminal readiness and is sealed as typed group-level `unavailable`, preventing an
optional diagnostic wait from delaying or conditioning the single primary look<br>
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
2. the complete inference executable, terminal-dataset producer, and their environment are
   implemented, tested, independently reviewed, and merged before activation;
3. an append-only activation record binds the registry definition, preregistration, both JSON
   Schemas, inference executable, terminal-dataset producer, and dependency lock SHA-256 digests;
   schema version; merged Git implementation Git commit; policy hash; classifier version; UTC
   activation timestamp; and enrollment sequence. The implementation commit must contain the exact
   bound artifacts and
   registry definition and remain an ancestor of the deployed commit; every content-bound artifact
   must still match after platform-stable CRLF-to-LF canonicalization;
4. the evidence store is empty for this registry ID and passes an integrity check; and
5. no challenger outcomes from on/after the proposed activation boundary have been inspected.

Only signals first observed at or after the sealed activation timestamp are eligible. Earlier
signals, including all current canary observations, never count. A signal missed during a capture
pause is recorded as missed and cannot be enrolled later. An amendment that changes cohort,
strategy, classifier, endpoint, test, alpha, sample size, or decision gates **after activation**
retires this registry entry and requires a new ID and fresh activation boundary. Before activation,
a draft correction is permitted only while the evidence store contains no challenger snapshots; it
changes the definition digest that will later be sealed.

The registry definition SHA-256 is computed over RFC 8785 canonical JSON after removing the
top-level `status` and `activation` members. This avoids a self-referential digest while keeping the
scientific definition stable across activation. The first enrolled position accession is not
knowable at activation; it is sealed by confirmatory sequence 1 in its immutable enrollment
snapshot.
Text artifact SHA-256 values use raw content after replacing CRLF pairs with LF so the reviewed Git
blob and its Windows checkout have one platform-stable identity. No other byte transformation is
permitted.
An opportunistic signal is first recorded as `pending_entry_selection` with its deterministic
planned entry date and capacity-rank digest already fixed. After the complete entry date is ranked,
a new immutable snapshot supersedes it with `enrolled`,
`overlap_suppressed`, or `capacity_suppressed`. Only `enrolled` positions receive a positive,
gap-free confirmatory sequence, assigned transactionally in ascending rank; every other state has a
null sequence. The original point-in-time snapshot is never rewritten.

The source first-observed timestamp fixes the intended entry session under the 09:20 ET cutoff.
Enrollment on that intended date additionally requires both the immutable evidence-recorded time
and trial import time to be strictly before that date's official submission cutoff. A snapshot or
import arriving at or after the cutoff does not re-plan the stale signal: it resolves `missed` and
cannot enter a later cohort. Non-decisional option and feature capture never delays or reopens this
decision.

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

For an official session date `D`, its candidate set is completed in one transaction at or after the
registered 09:20 ET cutoff and strictly before the official open, using the point-in-time schedule
known at that transaction. Eligibility uses the exact shared E07 history: watermark-bounded
first-observed records whose session close is strictly before each signal's
source-first-observed timestamp. A qualifying session may be collected after the signal because its
market information was already complete before the signal; no session closing at or after the
signal is eligible merely because its date precedes `D`. The transaction first snapshots both the
bar-observation, bar-poll-receipt, and schedule-feed observation watermarks, then binds all
point-in-time schedule records needed
for the entry and frozen horizon, exact watermark-bounded bar-record digests, qualifying
poll-receipt digests, each receipt's transactionally captured bar-observation watermark,
candidate-set digest, and canonical prior-book material plus digest. Capacity and overlap at
`D`'s open are recomputed from those bound inputs and the pure E07 kernel; outcome-materialization
timing is never an enrollment input. Same-date candidates are processed in ascending rank, and
lower-ranked enrollments immediately consume symbol and slot capacity.

The fixed resolution precedence is `missed`, `ineligible`, `overlap_suppressed`,
`capacity_suppressed`, then `enrolled`. A healthy completed source poll has an immutable success
receipt after the last session close needed by that candidate, covers the requested history through
that session, and reports zero source or validation rejections. Under that proof, fewer than 20
eligible pre-signal bars is the registered E07 `ineligible` result. Invalid feed integrity remains
`INVALID`. No qualifying receipt or missing data needed to decide a current candidate's E07
eligibility is systemic missingness: finalization waits only until the official open. Missing bars
for a prior enrolled position instead use a conservative book rule: the position occupies its
symbol and one slot
through its frozen final session, then expires unconditionally after that session; no outcome field
is consulted. The final in-transaction decision clock is sampled after resolution rows are staged
and immediately before the immutable completion seal is staged. If that clock is not strictly
before the official open, the transaction rolls back and an
append-only lapse resolves every candidate for `D` as `missed`, including later arrivals whose
intended date was `D`. The schedule watermark and all schedule-record digests used for the official
open and ten-session horizon are sealed; valid revisions between candidate imports do not prevent a
completion or lapse and therefore do not halt later dates. A lapse never backdates a completion.
Digest, ordering, or feed-integrity violations remain `INVALID` and halt completion visibly. A
completion transaction may return from its durable SQLite commit after the open due to bounded
system latency, but it cannot incorporate any input or decision sampled after the sealed pre-open
decision clock.

The existing live-canary ledger is the sole selection authority for the full E07/F00 diagnostic
control; the research runtime does not create or replay a second control selection engine. Its
prospective membership begins only when both the packet's immutable source-first-observed time and
the canary candidate's approval-time `signal_at` are at or after activation, preserves every canary
state, and ends after every candidate whose frozen canary entry session is on or before the
challenger cohort-freeze boundary is final. The source-first-observed time must be no later than the
canary approval time; equality is not required because packet creation and approval are distinct
events. Each binding content-addresses the canary candidate row and its activation metadata. A
later canary row change or ledger replacement creates a typed diagnostic reconciliation record; it
does not silently rewrite membership.

At most 20 control positions are open. The challenger independently applies the same 20-slot,
same-symbol, and ascending deterministic-rank policy within the opportunistic eligible set.
Neither book changes live slot selection or orders. The terminal `shadow_book_reconciliation`
integrity check and economic gate apply only to the confirmatory challenger book. Control-ledger
reconciliation is descriptive diagnostic evidence and cannot veto or rescue the primary decision.

The routine subgroup is the subset of prospectively bound control positions whose first valid
evidence snapshot was recorded strictly before the position's registered 09:20 ET entry cutoff and
froze `routine` classification, exact transaction-owner mapping, one reporting-owner CIK, and
complete required history. It is not a second selected book. Evidence unavailable or late for a
control record creates typed diagnostic missingness; it does not remove the record from control or
invalidate the challenger.

Because the frozen execution policy deliberately evaluates stop/target barriers from completed
daily bars, it does not infer an unobserved intraday barrier-hit minute. Gross trade return uses the
frozen stop-first barrier price. Matched SPY return is the SPY entry-session RTH open through the
exit-session RTH close, exactly matching the earlier daily E07 study convention. Persisted trade
entry/exit timestamps are the official exchange RTH open/close boundaries from the point-in-time
IBKR SPY schedule; early closes therefore use their actual close, not 16:00 ET.

An individual outcome is materialized only after the candidate's already-frozen tenth session has
closed and successful zero-rejection stock and SPY polls both cover that session. Their observation
watermarks must contain the contiguous stock path through the registered exit and SPY bars at the
entry and exit endpoints. This delay applies even when a stop or target occurred earlier, so
materialization timing cannot reveal early exits to operations.
The immutable record binds the entry-completion schedule watermark and all ten frozen session
digests, the outcome-time bar and receipt watermarks, both receipt digests, and every stock and SPY
bar digest used to reproduce the barrier and benchmark results. If terminal healthy polls exist but
a stock bar needed to establish the exit or either SPY endpoint remains absent, the trial is
`INVALID`; the position is never silently discarded. Bars strictly after an established stock exit
are irrelevant. Routine status exposes only counts and health, never individual returns or an
aggregate.

Control and routine outcomes are descriptive and are recomputed after the frozen tenth session
from the same first-observed research bars, point-in-time session records, healthy-poll proof, and
pure E07 outcome kernel used for the challenger. Canary shadow rows are agreement evidence, not the
diagnostic outcome authority. Their exact row digests, research-input watermarks, and outcome
provenance remain in a separate append-only diagnostic SQLite store. A missing join, timestamp
mismatch, unavailable bar, ledger replacement, canary/research disagreement, or corruption is
reported per trade or per group as typed `unavailable`; it never mutates the confirmatory store,
changes a challenger integrity check, or delays the single terminal primary decision. The terminal
dataset uses deterministic diagnostic trade IDs and strips confirmatory sequence, evidence-record,
and capacity-rank fields as required by the frozen inference schema; the diagnostic-store receipt
retains the complete provenance.

## Point-in-time owner classification

Classification follows the trader-level rule in Cohen, Malloy, and Pomorski, with explicit states
for records their study would not partition. It uses reporting-owner CIK, never a normalized name.

The fixed observation boundary is `2006-01-01`, the first day of the first full calendar year in the
SEC quarterly bulk archive. Every classification is bound to the SHA-256 of an immutable, gap-free
archive snapshot covering that boundary through December 31 before the classification year. Public
history before the boundary is left-censored and is disclosed as a measurement limitation; it is
not treated as lifetime-complete history. Changing the boundary after activation requires a new
registry ID and sample.

Replay the owner state at each January 1 from 2009 through classification year `Y`. At cutoff `y`,
use only non-derivative open-market purchase or sale transaction codes `P` and `S` whose SEC filing
was public before `y-01-01T00:00:00 America/New_York`. Use the EDGAR acceptance timestamp when
present; when the quarterly bulk data exposes only a filing date, include filing dates no later
than December 31 of the prior year. Current-year trades cannot affect current-year status.

1. The initial state is `unpartitionable`. It remains so until the owner has at least one qualifying
   trade in each of a cutoff's three preceding calendar years.
2. At the first partitionable cutoff, a non-empty intersection of the three transaction-month sets
   makes the owner `routine`; an empty intersection makes the owner `opportunistic`.
3. An `opportunistic` owner retains that state. At each later cutoff with qualifying trades in all
   three preceding years, a common transaction month changes the owner to `routine`; a disjoint or
   incomplete newest window leaves the prior opportunistic state unchanged.
4. Once `routine`, the owner remains routine for every later year.

This is the published trader-level state machine applied to a finite observable dataset, as the
paper applies it to its January 1986 data boundary. It does not claim that pre-2006 history is
absent. Archive gaps, stale coverage, unresolved amendments, invalid transactions, or ambiguous
owner identity produce `unpartitionable` at the affected current cutoff. Replay uses only amendments
public at each historical cutoff, so a later amendment cannot rewrite a previously recorded event
classification; later classification records bind the then-visible correction.

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
  return from the entry session's RTH open through the exit session's RTH close. The stock barrier
  price remains the frozen daily-bar stop-first outcome; no unobserved intraday hit time is imputed.
- Statistical test: one-sided circular moving-block bootstrap over chronologically ordered
  entry-date clusters, block length 10 dates, 10,000 resamples, deterministic seed `260826`.
  Within a resample, concatenate every trade from each sampled date and calculate the trade-weighted
  mean, preserving within-date dependence and unequal cluster sizes. For the null distribution,
  subtract the observed all-trade mean from every trade before sampling; truncate the final block
  to exactly the original number of date clusters. The raw p-value is
  `(1 + count(null_bootstrap_mean >= observed_mean)) / 10001`.
- Deterministic byte stream: bounded draws use the first unsigned big-endian 64 bits of
  `SHA256(domain || NUL || seed_u64_be || counter_u128_be)`, with domain
  `OPP-E07-V1|circular-moving-block-bootstrap|v1`, seed `260826`, counter starting at zero, and
  rejection of values at or above `2^64 - (2^64 mod cluster_count)` before modulo. Dates are sorted
  ascending; trades within dates are sorted by entry UTC, confirmatory sequence, then trade ID.
  Each resample draws circular block starts until exactly the original number of date clusters is
  present. The same sampled clusters produce the centered null mean and uncentered interval mean;
  equality with the observed mean counts as an exceedance.
- Confidence interval: two-sided 95% percentile interval from the otherwise identical uncentered
  circular moving-block resamples using type-7 linear percentiles at 0.025 and 0.975, reported for
  effect-size context and not substituted for the registered p-value.
- Cohort freeze: after each entry date is complete, freeze the cohort on the first date for which
  cumulative enrollment contains at least 100 challenger positions and at least 60 distinct entry
  dates. Include every enrolled position on that boundary date, even when this exceeds 100. The
  trigger uses only entry/enrollment state, never exit timing or returns. Entry-date completeness
  is an ordered append-only record containing entry date, completion UTC, the official schedule
  proof, bound input watermarks/digests, and resolution-set digest. Completion UTC must map to the
  entry date in New York, must be at or after the registered 09:20 ET cutoff and strictly before
  its official open, and cannot skip a candidate imported before the cutoff for
  that date. A separately ordered lapse record is the only permitted way to advance an intended
  entry date after its open; every candidate for a lapsed date is permanently `missed`.
  Once that boundary exists, later evidence receives the immutable disposition
  `excluded: cohort_already_frozen`, and the entry-date finalizer cannot resolve another date.
- Terminal information time: after cohort freeze, wait until every frozen position has a final E07
  outcome and all integrity checks pass. A separate no-aggregation command then writes a terminal
  dataset receipt to the append-only seal store before the decision command can calculate any
  aggregate outcome. The producer validates unchanged trial and diagnostic fingerprints, then
  holds write-preventing snapshots of the trial, diagnostic, canary, and source stores until the
  receipt commits. It independently reconciles diagnostic membership to the authoritative canary
  and source stores. Each diagnostic group carries exact available/not-traded/unavailable
  accounting; group failure produces an empty, explicitly unavailable diagnostic and cannot alter
  the challenger. Missing diagnostic receipts or unresolved diagnostic membership at challenger
  terminal readiness are terminal group-level unavailability, not a reason to wait; operators may
  not time the primary seal around diagnostic catch-up. The receipt binds the dataset, complete
  candidate projection, and immutable
  candidate-universe digests. Before publication, its complete canonical bytes and candidate
  bindings are committed to an append-only pending-seal record. Publication or receipt failure is
  retried from those exact pending bytes, never from later diagnostic state, so a crash cannot
  create an alternate scientific dataset.
- Enrollment deadline: convert `activated_at_utc` to `America/New_York`, add exactly 18 calendar
  months while preserving the local wall-clock time, and convert the result back to UTC. If the
  activation day does not exist in the target month, use that target month's final calendar day.
  If the target wall-clock time is in a daylight-saving gap, advance by the gap to the first valid
  local instant; if it is ambiguous, use the second occurrence (`fold=1`). Signals first observed
  at or after that UTC instant are excluded. If the cohort-freeze thresholds have not been reached
  before the deadline, stop admitting new signals, allow every pre-deadline
  `pending_entry_selection` record to reach its deterministic entry-selection result, and only then
  write an append-only deadline-miss receipt binding the immutable pre-deadline candidate universe,
  then return `KILL` with reason `insufficient_enrollment` without calculating a confirmatory
  p-value or outcome aggregates. Later candidate resolution cannot rescind that receipt, add or
  remove a candidate, or rescue the trial.
- There are no confirmatory interim p-values, optional stopping, extensions, or second looks.
  Health and exposure counts may be monitored; return aggregates and inferential statistics remain
  unavailable until the sealed terminal look.

The seal store is SQLite with full synchronization, append-only update/delete triggers, one receipt
per receipt kind, and one terminal decision report. Alternate terminal receipts and a second
inferential report are prohibited. If data integrity is invalid before terminal sealing, the result
is `INVALID`, not a pass or fail, and a correction may be made only without outcome access. After a
terminal receipt exists, its digest cannot be replaced; a material invalidity retires the trial.

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
8. timestamp ordering, snapshot hashes, SEC archive coverage, classification provenance,
   challenger outcome completeness, and challenger shadow-book reconciliation all pass.

For exact gate arithmetic, gross trade return and matched SPY return are decimal fractions. Net
absolute return subtracts 0.002 (primary) or 0.005 (stress); alpha additionally subtracts matched
SPY return. Profit factor is the sum of positive 20-bps net absolute returns divided by the absolute
sum of negative returns; positive gains with no losses are positive infinity and pass. The unique
entry dates are split before date `floor(n_dates / 2)` (the latter date starts the second half).
The single best stress-alpha trade is removed with earliest frozen trade order breaking a tie. The
best entry calendar month is the month with greatest summed stress alpha, earliest month breaking a
tie. Symbol concentration is the largest positive symbol-level sum of primary alpha divided by all
positive symbol-level sums. Month concentration is the largest entry-month primary-alpha sum
divided by total primary alpha. Undefined or empty gate arithmetic fails closed. The 20-slot replay
gate is the sum of stress-cost net absolute returns over the already capacity-, overlap-, and
same-symbol-reconciled frozen challenger book.

The full E07/F00 control and routine subgroup must be reported with counts, effect sizes, and
confidence intervals as descriptive falsification context. They cannot rescue a failed primary
test or create a different promoted rule. A corrupt diagnostic group is reported as typed
`unavailable`; it cannot change the primary decision.

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
