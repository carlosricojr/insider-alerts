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

Operate off-hours 18:00–08:00 New York, at most one symbol per six-minute
scheduled invocation, with a durable five-minute minimum, hard I/O deadlines
and OS-released process ownership. Keep
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
the exact five-minute durable gate loses alternate slots. Use a six-minute task
cadence with the five-minute safety minimum, and measure actual service capacity.

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

Current state: diagnosis/design challenge complete. The new order-incapable
`commissioning_source` adapter records contract IDs and exact local SDK-dispatch
times, preserves raw SDK projections and distinguishes broker error codes, empty
responses and timeouts. Fifteen focused tests and strict typing pass. No worker,
commissioning journal, activation, PR or deployment exists yet.

Before implementing the remaining pipeline, a follow-up design challenge is
resolving one-minute read-only eligibility-field custody alongside independently
paced off-hours broker requests. Later historical bar retrieval cannot become
pre-entry evidence. This may supersede the initial six-minute whole-worker cadence;
the durable five-minute minimum between network attempts remains a constraint.
The previous calendar foundation is deployed and live/SEC/autopilot workers are
healthy. Next action is the design challenge and a concrete final acquisition
contract, followed by implementation and prospective commissioning.
