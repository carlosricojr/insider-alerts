# Complete native schedule endpoint

Outcome: eliminate the reproduced midnight-to-open coverage failure without
relaxing a single calendar safety check. Base fbf9a8c8e5ff90fc6866f78b734d52c003b4adc9.

Evidence: Sept 14 outage began 00:00:12 ET, recovered 09:30:10 ET (2,350 failed
cycles). Read-only comparative requests on Sept 14: Oct 29 08:00 ET endpoint
omitted Oct 29; 10:00 ET included it. The prior bounding fix correctly rejects
that missing required date, but the request copied the current intraday clock.
No historical response was retained from the outage; this is a current controlled
reproduction, not invented contemporaneous evidence.

Design: derive today and +45-day end from New York calendar dates; request through
23:59:59 New York on that end date, converted to UTC. Reuse exactly those date
bounds in native coverage validation. Keep full-row validation, exact 2026
coverage, bounded 20-past/10-future checks, fallback authorization and expiry,
timeouts, request-error restoration and closed-market gates unchanged.
Do not modify research adapters, registry or point-in-time records, orders,
cash-account mode, capital or frozen two nominal $200 E07/F00 slots.
API endDateTime is a timestamp, not just a date:
https://www.interactivebrokers.com/docs/tws-api/protobuf/historical-data-request

Verification: Claude design challenge attempted (usage limit); independent Codex
design challenge before implementation. Cutoff-sensitive fake must reproduce the
old failure and pass at midnight, pre/post open, both DST directions, weekend,
holiday/early-close and year boundary. Missing final session must still reject.
Explicit leap-day start/end, ordinary non-leap and 2100 century non-leap cases
verify date arithmetic only, not future-year exchange holiday authority.
Full lint/typing/tests; independent adversarial review; exact-head CodeRabbit
loop and CI before merged-main deployment. Read-only broker probe with pre-open
around value, then current deployed runtime fingerprint, fresh successes, broker
reconciliation/protective orders, hidden processes, observer and clean synced main.

Handoff: design challenge complete; implementation and endpoint-sensitive tests
added. Full gates, independent/exact-head review, merge and deployment pending.
Actual overnight continuity remains a follow-up check
after deployment; successful simulated-clock requests do not prove a future
overnight run. No backfill/reinterpretation of candidate or trial history.
Rollback via reviewed revert; retain ledgers/protective orders and fail closed.
