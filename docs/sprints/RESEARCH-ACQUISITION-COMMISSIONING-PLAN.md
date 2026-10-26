# Outcome-blind acquisition commissioning

Objective: make a fresh, after-cost E07-versus-opportunistic comparison possible
by demonstrating input acquisition quality. This release does not test returns,
change the live strategy, activate a confirmatory trial, or complete the broader
measurable-profitability goal.

Base: reviewed/deployed PR #67, `09807eba`. Calendar foundation is available;
the INVALID OPP-E07-V1 evidence remains untouched. October 26 inspection found
419 observer candidate records, 4,658 bar versions, 211 distinct symbols, and
explicitly missing contract IDs/request-dispatch clocks. The old bar feed has
185 requested symbols; successful-response counts do not prove complete coverage.
These are metadata observations, not a return evaluation or a power estimate.

## Proposed release and boundaries

One isolated acquisition-commissioning pipeline with its own future activation,
append-only journal, source metadata, validated calendar, status command, and
hidden bounded scheduled worker. Read only candidate identity/signal-time fields
from the live ledger. Never read existing outcomes or write existing stores.
No frozen source/registry/dependency changes. No orders, accounts, positions,
executions, previews, realtime subscriptions, paid data or new services.

Archive the official NYSE regular-hours source and its actual retrieval time.
Keep the annual calendar source/version and hours source hashes in the commissioning
manifest. Publication date does not prove earlier possession. The baseline does
not establish absence of extraordinary closures; contradictory schedules or
missing expected sessions fail input readiness explicitly.

At exclusive activation, seal a future boundary (at least two hours ahead), the
reviewed source revision, calendar version, and ten following regular sessions.
Commissioning cases are excluded from any later confirmatory sample. Capture SPY
and all valid-symbol canary candidates whose signal is on/after that boundary,
without return-based selection. The window is fixed; missed observations are
missing, never silently moved to a more convenient window.

The narrow new IBKR adapter records qualified contract identity, actual dispatch
and response clocks, request parameters and SDK response projections. Describe
these as SDK projections, not raw wire bytes or verified auction fills. Preserve
first observations and revisions. Bar presence after the close is not proof of
vendor finality. Invalid/empty responses, rejections, clock reversals, and broker
outages receive typed evidence; no invented values or fills.

Capture ledger fields on a hidden one-minute cadence. Broker I/O operates only
18:00–08:00 New York, with a durable five-minute minimum between attempts,
hard I/O deadlines and separate OS-released custody/network locks. Keep
durable attempt receipts so a restart cannot defeat pacing. Fair rotation must
not starve the benchmark. Source reads close before broker I/O or output writes.
Qualification is by current instrument, with symbol/contract ambiguity rejected.

Status distinguishes integrity, source identity, date coverage, freshness and
commissioning completeness. Never emit profitability, trial readiness, or a
promotion verdict. Commissioning must quantify whether the bounded acquisition
budget actually services the required universe; a sample of liquid sentinels
alone cannot certify small-cap candidate coverage.

## Design issues to resolve before implementation

- Exact universe admission and required per-symbol ranges within the fixed window;
  early history needed by E07 versus horizon data, and late-arriving candidates.
- Explicit comparison of completed broker sessions with the independent calendar;
  never use projected future IBKR sessions as truth.
- Contract identity drift, corporate actions, stale/partial daily bars, and what
  can truthfully be concluded about finality from this source.
- Daily benchmark priority without starving candidate symbols; feasibility of
  one request per five minutes alongside existing acquisition traffic.
- Strict schema/provenance replay, source-code/manifest pinning, transaction
  boundaries, crash recovery and task installation/rollback.

## Verification and handoff

Before implementation: read-only Claude design challenge. Test synthetic outages,
partial/duplicate/wrong-symbol bars, contract changes, timing boundaries, holidays,
early closes, DST, empty/stale feeds, clock reversal, database corruption, concurrent
workers, restarts and missed windows. Keep all research outcome arithmetic absent.
Run full Ruff/MyPy/Pytest and independent adversarial review, then canonical PR
review of record and green exact-head CI. Deploy only reviewed merged main, verify
worker/broker reconciliation and invisibility, and retain all failed evidence.

## Design challenge and decisions

Read-only Claude session `865f0399-7fef-4d56-8b2e-eeccad175f25` challenged
the design before implementation. It confirmed that the current observer also
discards the pre-signal history required for E07's 20-session eligibility check.
The new adapter must distinguish broker errors, empty responses and timeouts;
current symbol resolution is not proof of historical issuer identity.

Fresh metadata diagnosis at 20:23 UTC October 26: 210 active 45-day symbols,
53 distinct symbols and 82 candidates in the last 14 calendar days. Healthy recent
days had 86–92 successful polls. Of 181 short attempt intervals on October 22–23,
150 were ten minutes and 31 were five minutes. Scheduler/clock jitter against
the exact five-minute durable gate loses alternate slots. The final design uses
a one-minute custody cadence with the five-minute network safety minimum; jitter
can then cost one minute rather than an entire five-minute trigger interval.
Measure actual service capacity instead of assuming theoretical supply.

Use event-driven collection: initial pre-signal-history request, a request after
the frozen tenth horizon session, and an independent later-session repeat to
measure stability. Merge overlapping symbol ranges, retain candidate identity
and deadlines, and avoid nightly polling of every 45-day window. Reserve bounded
benchmark service and use deterministic earliest-deadline ordering. No early
return, stop/target, or price-direction calculation chooses a request.

Admission follows the unchanged E07 09:20 signal cutoff into ten enrollment
sessions. Each admitted candidate needs 20 policy-completed pre-signal sessions
and its ten-session horizon. Thus the collection window extends through the
nineteenth session plus a fixed recovery allowance; ten enrollment sessions
does not mean ten days to a complete commissioning result. Invalid symbols and
unqualifiable contracts remain visible in the admission denominator. Late
observations never become contemporaneous or receive a shifted entry date.

Repeated identical post-session SDK bars mean empirical stability, not vendor
finality or official auction prints. Do not add a purported four-hour finality
guarantee: that suggestion had no source evidence. Preserve all revisions, exact
receipt times and explicit missingness. Historical eligibility reproduction is
a diagnostic and cannot substitute for inputs actually observed before entry.

Owner-history readiness, current-versus-historical issuer resolution, auction
execution realism and the conservative cost model remain mandatory gates of the
full comparison. This acquisition release must display those as unverified;
it cannot call the strategy or trial ready. The SEC classifier uses filings public
before its annual cutoff and a source snapshot known at decision time; it does
not require the archive itself to have been retrieved before that annual cutoff.

Do not automatically retire or change the existing observer in this PR. After
measured commissioning, separately evaluate migration of bar polling while
preserving candidate capture and all historical evidence. Weekend daytime
requests remain disallowed under the same off-hours boundary.

The second challenge in the same Claude session completed with concrete source
and custody corrections. Later requests now follow the originally pinned conId;
symbol re-resolution is recorded independently and never silently substitutes a
new instrument or drops a changed-symbol horizon. Unknown/delisted data remains
missing, not excluded. Symbol drift is not itself proof of a corporate event.
The source preserves sanitized broker messages (162 is ambiguous), SDK/server
versions and farm notices. Receipt result, cleanup and clock integrity are
separate axes, so received data is retained even when unusable.

Custody keeps actual first observation and source cycle-start clocks separate.
Use the 09:20 decision cutoff for availability diagnostics; merely being observed
before the 09:30 auction is insufficient. Copy only whitelisted non-outcome fields
and runtime metadata in one read-only source transaction. Current runtime metadata
is context, not automatic proof of an older row's producer: rows predating that
runtime require an unknown-producer flag. Preserve every eligibility revision.
The future trial must bind its own pre-cutoff read/decision snapshot; later R1
history can never be substituted for that snapshot.

Journal response identity is the preceding attempt digest. Sequence/hash bindings
and source observation clocks let future readers freeze a receipt watermark;
duplicate completion is idempotent and conflicting completion is rejected. The
receipt's contained bar index/content digest provides immutable per-bar identity.
No old V1 schema or decision semantics are silently inherited.

Freeze the recovery deadline at activation as the later of 08:00 New York on
W19+5 calendar days and W20+3 calendar days. This supplies two off-hours recovery
windows after the calendar session needed for the stability repeat. No extension
after observing data. Operational coverage proportions are descriptive; do not
adopt the challenge's unsupported Clopper–Pearson numerical claim as confidence
or power evidence. Scientific yield/power remain separate, required work.

Current state: adapter, append-only journal, calendar-bound window, source custody,
per-date receipt validation and deterministic planner are implemented locally.
Focused tests cover source failures, original-contract history, immutable custody,
concurrency/corruption, cutoff/DST/holiday/leap dates, source revisions, request
pacing, retry recovery, range merging and fixed first-receipt stability comparisons.
No worker, activation, PR or deployment exists yet. Full gates and independent
adversarial review are still required for the complete worker/installer diff.

Primary source semantics:
https://www.interactivebrokers.com/docs/tws-api/doc/md/historical-bars
and https://www.interactivebrokers.com/docs/tws-api/doc/error-handling/error-codes.
Record TRADES as the provider's daily RTH series; it is not proof of auction fills
or a total-return/dividend series. Do not infer corporate-action cause from price
direction or optimize request selection from realized prices.
The window, read-only custody, receipt validation and pure planner are now local.
The manifest freezes retries until the recovery deadline, 300-second minimum
pacing, capped exponential backoff and a one-in-five benchmark reservation while
candidates are due. Merge compatible
due ranges, but never merge distinct pinned contracts. Each new candidate first
resolves its own current identity; ticker reuse cannot inherit an older candidate's
pin. Qualification pins identity even if the following history request fails.
The first complete R2 and first complete later-session R3 are fixed; a changed R3
is not retried until a convenient matching pair appears. All raw receipts remain.

SPY price requests follow the same three stages through W19 and a later close;
daily completed SCHEDULE checks run through W20. This preserves benchmark horizon
coverage and calendar cross-checks without unnecessary daily SPY price polling.
Late source-field corrections retain the original admission and separately record
their actual revision observation and revised admission; they cannot backdate a
corrected feature or silently move the denominator. Report acquisition, custody,
canary agreement and admission as separate descriptive counts, not one overloaded
completeness flag. Unassignable rows and invalid symbols remain explicit.

Worker design still to implement/review: explicit activation creates the isolated
calendar and commissioning journals; default execution cannot initialize them.
Bind the reviewed implementation files (including pure shared dependencies), source
ledger path/file identity and archived hours bytes at activation. Compare the
implementation digest on every run, allowing unrelated reviewed main commits but
stopping both custody and acquisition if its actual code changes. A separate one-minute custody lock
and network lock are released by the OS on process death. Both are nonblocking;
custody commits before network work, with no live ledger connection held. A durable
attempt precedes broker I/O. Network time is capped at 50 seconds and the earlier
of the 08:00 cutoff and recovery deadline, using wall and monotonic clocks. Parallel
hidden scheduled invocations permit custody during a slow or stuck network call;
the task has a two-minute hard execution limit. Record bounded failure heartbeats,
retain unanswered attempts, and never infer success from process exit alone.

The third read-only challenge completed in Claude session
`865f0399-7fef-4d56-8b2e-eeccad175f25`, response UUID
`6c605b4d-323d-4110-96f5-25d28d045264`. It confirmed outcome-blind request selection
and required the following corrections:

- B1 implemented: remove permanent six-attempt exhaustion. Infrastructure failures
  (including unanswered/crashed attempts and client-179 conflicts) use a global
  exponential backoff capped at 30 minutes; content failures use per-job backoff
  capped at six hours. The fixed recovery deadline bounds both. Neither logout nor
  malformed content silently removes a candidate. Backoff classifies unknown
  errors conservatively as infrastructure, without relabeling error 162 as pacing.
- B2 implemented: identity/clock/request failures invalidate a whole receipt;
  individual malformed/duplicate/non-session bars invalidate only their own dates.
  Required missing dates remain missing, with reasons. Bad extra dates cannot
  discard valid required dates. Complete receipts and missing dates are separate.
- B4 implemented: seal a pre-boundary packet-ID inventory in activation. Capture
  new pre-boundary observations as preexisting. Existing malformed timestamps do
  not contaminate the fresh window's unassignable count. Source path/device/inode
  are bound and replacement is rejected. Custody polls include runtime context.
  One mixed completeness flag was removed in favor of separate descriptive axes.
- B3 remains worker work: load the originally bound calendar by digest, never by
  latest position. Compare the latest known descendant separately over the full
  history/enrollment/horizon span, flag affected cases, never rebind or stop custody
  merely because a later notice exists.
- B5 remains worker work: verify implementation digest over all commissioning
  modules, imported shared helpers, calendar, frozen control definitions and lock
  file/runtime dependency versions. An actual implementation change ends this run;
  recovery needs a new future activation, never a silent continuation. Activation
  requires clean local main equal to freshly fetched origin/main.
- B6 remains worker work: the first network-locked post-deadline invocation writes
  an idempotent seal with actual seal time, report digest and prior watermark.
  Interim reports never masquerade as final. Late responses remain outside the
  fixed deadline even when the PC was off at sealing time.

Finish status dimensions: five-minute-resolution custody continuity, SDK/server
and farm notices, attempts by overnight window, reasons for missing dates, pin
source receipt/symbol drift, and calendar contradictions. Add the proposed frozen
SPY W10 checkpoint (one extra request) and actual per-date observation lags; do not
assume differing bar observation times have harmless corporate-action semantics.
Runtime tests must cover simultaneous custody/network callers, crash/restart,
overnight outage recovery, client 326, source/code replacement, latest-calendar
notices, typed clock-regression status, and late/idempotent sealing. No new design
permission is needed: the remaining worker design has been challenged read-only.

Before release, clarify the timestamp labels: the current admission flag named
`contemporaneous_custody` compares source observation with 09:20, not SQLite commit.
Rename it to `observed_before_decision`, expose journal construction separately,
and retain the requirement that a future trial bind its own pre-cutoff read. A
source observation before cutoff does not prove durability before cutoff.

Intermediate core verification: `uv run ruff check .`, `uv run mypy src` (77 source
files) and the full `uv run pytest` completed successfully on this worktree. This
is a development checkpoint, not a production-readiness or review-of-record claim;
rerun the full gate after the worker/installer and remaining report corrections.

Next: implement worker/installer and the listed integration/fault tests, then full
gates, independent adversarial review and canonical PR. This is one complete
commissioning release; do not ship another unused prerequisite-only release.
Nothing in this worktree is deployed or activated; the goal remains unachieved.
