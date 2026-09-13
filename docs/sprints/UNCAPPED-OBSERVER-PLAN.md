# Uncapped operational capture

Outcome: preserve future canary candidate coverage and observed daily price paths without a
slot cap. This is capture-only infrastructure, not a portfolio, missed-profit report, new
hypothesis, or repair/reset of INVALID OPP-E07-V1. Base: fa376e29456f654aac9c62e6c7bf1376bdf71000.

Authority: read fixed non-outcome columns of all canary candidates, including rejections; do
not recompute eligibility or claim all SEC signals. Seal a future activation and require source
signal times after it. Source created_at is a cycle clock, not insertion time. Preserve delays. Never
read trial outcomes or write live/research stores. Live 2x$200 and ghost 20 slots stay frozen.

Storage: independent data/observer SQLite journal, append-only RFC8785/SHA256 chain with
supersession references. Candidate revisions and source failures remain evidence. Materialize
and close read-only source snapshots before output writes or network calls. Fixed paths reject
links and foreign schemas. A separate SQLite write lock serializes workers; durable attempts
precede I/O so crashes cannot erase pacing history.

Market data: reuse only the narrow existing historical-bar adapter, client 178, no account,
preview, order or subscription APIs. One symbol per invocation, at most once per five minutes,
18:00-08:00 New York only; least-recently-attempted fairness. Capture 45 calendar days from the
signal as a storage/request window, NOT a trading horizon or endpoint. No new data subscription.
Prior New York dates only; record request/receive times, source identity, malformed values,
empty responses and errors. Historical bars are observed revisions, not immutable exchange
truth or contemporaneous entry information. No calendar feed dependency or session-count claim;
the broken research calendar remains untouched. Data gaps cannot silently become zero returns.

Verification: design challenge (Claude attempted; Fable limit), independent prior governance
challenge; focused synthetic tests for boundaries, isolation, revisions, malformed input,
restart pacing, fairness, lock contention, partial failures and no outcome access. Full ruff,
mypy and pytest, independent adversarial review and exact-head CodeRabbit loop before merge.
Deploy only reviewed origin/main; verify source task checkout, current fresh live heartbeat,
broker reconciliation, hidden observer task/process and clean synced main. Activation is sealed
after deployment at least two hours ahead. Blinded heartbeat/coverage are the only outputs.

Rollback: disable only the observer task; retain all evidence. No live policy rollback needed.
Handoff: implementation and 37 focused tests complete; independent review found and
prompted fixes for source cycle-clock semantics and cutoff cancellation during qualification.
Full final gates, exact-head review, merge and verified deployment remain required.
Profit analysis requires a separately authorized fresh-sample
preregistration and execution/cash assumptions; this release cannot promise recoverable profit.
