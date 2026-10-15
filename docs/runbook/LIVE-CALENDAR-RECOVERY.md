# Live calendar recovery — September/October 2026

Scope: the E07/F00 operational canary, including its existing operational shadow book.
Not a research calendar source, trial correction, or new hypothesis. The October 15
renewal includes explicitly authorized session-relative live exit dispatch; shadow rules stay fixed.
OPP-E07-V1 remains INVALID. Its immutable schedule, evidence, fault, and outcome stores are not
rewritten or replayed by this implementation. Dependencies and frozen registry/policy stay pinned.

## Authority

Prefer a valid IBKR historical SPY schedule. The live fallback is permitted on error 162
containing `No data of type EODChart`, an empty-list schedule response, or the narrowly validated
future-projection recovery described below. Other errors,
malformed successes, and other schedule/calendar disagreements fail closed. Timeout/cancellation resets
the live connection; it never uses fallback on a connection with an unanswered request.

Fallback table: official [NYSE cash-equity calendar](https://www.nyse.com/trade/hours-calendars),
2026 dates only, regular 09:30–16:00 New York sessions and published 13:00 early closes.
Source [2026 calendar PDF](https://www.nyse.com/publicdocs/nyse/ICE_NYSE_2026_Yearly_Trading_Calendar.pdf),
retrieved September 12, 2026, SHA-256:
`70f5577eb43e60a9dbbecaae3cec23d0f02028c05c7f175013bb3e97816d394f`.
Downloaded source retained with review evidence under the operator's temporary recovery directory.
The normalized calendar digest also binds the source digest, timezone, and regular/early hours.

Each fallback use requests fresh SPY contract details, requires matching positive conId, stock/USD
identity and New York timezone, and compares every advertised liquid-hours date with the table.
Today, intervening closed dates, and the next open date must be explicitly covered. Duplicate,
split, overnight, stale-date, oversized, unknown-zone, missing, and contradictory responses reject.
This proves fresh retrieval and date coverage, not a broker publication timestamp or a guarantee
against an unannounced closure after the request.

Qualification, historical schedule, and contract-details requests have five-second live deadlines.
The last successful schedule receipt is stored in live metadata `schedule_evidence`, containing
source, fallback reason, normalized calendar hash, hours hash/coverage, validation time, and returned
session horizon. It is cleared before each attempt so a failed cycle cannot present stale success.
Native responses must contain every independently known session in the requested 2026 overlap,
as well as enough past and future coverage; aggregate counts cannot substitute for completeness.
New buys recheck same-New-York-date freshness (at most 60 seconds, no future timestamps) immediately
before submission. Closed dates cannot admit buys. Existing broker safety checks remain required.

## Expiry and existing holdings

The October 14, 2026 recovery request authorizes restoring existing-position management
when IBKR projects full regular sessions on future published holidays or early-close dates.
This exception requires live-armed mode and a current date from October 1 through December 31,
2026. Every returned row must pass structural validation; all contradictions must be those
two shapes on strictly future 2026 dates. Every requested published session must still be
present (past AND future), with at least 20 validated past sessions. Past/current disagreement,
missing sessions, malformed or duplicate rows, wrong zones and other hour errors still reject.
Fresh strict contract-hours agreement is required exactly as for the original fallback.

The receipt identifies `IBKR_NATIVE_FUTURE_PROJECTION_REJECTED`, rejected dates/kinds,
normalized native-response SHA-256, and validated past count. Returned sessions come from
the published calendar, not a silently repaired broker observation. The original October 14
recovery was management-only. The separately authorized October 15 renewal below allows
new buys using this validated published calendar too. Discovery/shadow/research paths
are unchanged. Recovery permits the existing worker to submit overdue exits; it does not
place a separate manual trade. The implementation plan is
[Future calendar management](../sprints/FUTURE-CALENDAR-MANAGEMENT-PLAN.md).
The existing operational shadow book may settle using this same fallback; this does not
repair or promote the separate frozen research trial.

## October 15 entry renewal

The operator explicitly approved resolving the entry blocker and session-close-relative
exit dispatch on October 15, 2026. Original fallback approval ended October 1 at 00:00
New York; October 1-14 remains unapproved, with no retrospective entries or evidence replay.
Renewal `renewal-2026-10-15` runs October 15 inclusive through January 1, 2027 exclusive.
It covers validated unavailable-feed fallback AND rejected-future-projection fallback.
Recovery authority is independently fixed at October 1-December 31; renewing or expiring
entry permission cannot disable management recovery.

Both native and fallback entries require the entire ten-session holding period inside
cross-validated 2026 coverage. December 17 is the last possible 2026 entry (session ten:
December 31); December 18 and later fail `calendar_entry_horizon_incomplete`, even if
native IBKR supplies plausible but independently unverified 2027 rows. A separate calendar
review is required before extending coverage. Review by December 10; no new task is created.
No automatic renewal, capital increase, policy promotion, or broader disagreement exception.

Receipts record approval ID, approval expiry, entry basis, today's validated open/close and
the tenth session. The runner independently checks its configured holding horizon and persists
the planned exit date before sending an entry. Crash adoption preserves that date; protection
never silently replaces it. Missing exit horizons trigger a critical event and block new entries
after protective orders are placed. The existing expired-entry queue is marked missed, not bought.

After expiry, reconciliation, protective repair, and existing timed exits continue when calendar
acquisition succeeds; freshness/expiry entry gates do not stop those management steps. A calendar
disagreement outside the narrow recovery above or acquisition failure still fails the cycle closed
and must be investigated promptly;
recorded server-held GTC protective orders are not cancelled. The fallback's returned horizon is
explicitly capped at December 31, 2026, so crossing the +45-day projection into 2027 does not
prematurely stop management. Valid native IBKR schedules are not restricted to fallback-table years.
At that boundary, beyond-2026 hours are labeled unvalidated and excluded from returned coverage;
current-year agreement is still mandatory, and the holding-horizon gate blocks new entries. Fallback is
unavailable once the current date itself enters 2027; no unverified next-year calendar is inferred.

## Session-relative exits

Timed exit submission is 30 minutes before the validated close; the conservative MOC cutoff
is 15 minutes before it. Regular sessions retain 15:30/15:45 ET; November 27 and December 24
use 12:30/12:45 ET. This is our dispatch safety margin, not a claim about every venue's cutoff.
The existing config times remain regular-session references. Close comes from validated native
rows or the published fallback checked against fresh contract hours, never an assumed 16:00.

Same-session timed exits require same-New-York-day bounds, not entry approval or the entry-only
60-second freshness limit. Missing/future-dated/prior-day bounds prevent their dispatch. MOC is sent only
inside its dispatch window; after the cutoff, the existing market-exit path applies until close.
The existing overdue recovery is unchanged: once the stored exit date is past, it can enqueue
the existing DAY market exit outside regular hours (`outsideRth=False`). This separate emergency
path is not subject to same-session dispatch windows. Same-session orders are not newly sent
before open or at/after close. Each row uses the actual dispatch clock and the adapter checks
windowed orders again after contract lookup. A boundary crossed during lookup fails the cycle
closed with `CALENDAR_EXIT_DISPATCH_WINDOW_MISSED`: remaining rows/entries are deferred,
`last_cycle_error` is recorded, and the next cycle retries. No manual trade is added.
OCA groups, stop-first protection, order references, quantities, emergency quantity-repair exits,
and already stored exit dates remain unchanged. Network latency or halts can still prevent a fill.

If a projected disagreement persists when its date becomes current/past, the original strict
gate still stops the cycle, including management. Monitor November 26/27 and December 24/25;
investigate operational outage alerts promptly. Server-held protection remains but cannot
guarantee a fill, loss limit, or timely tenth-session exit. No claim of future broker availability.
Implementation and verification plan: [Calendar renewal](../sprints/CALENDAR-ENTRY-RENEWAL-PLAN.md).

## Release gates

Require focused malformed-response, timeout-reset, date/holiday/DST, expiry/freshness, no-buy,
management-continuity and research-isolation tests; full Ruff, strict mypy, pytest; independent
adversarial review; exact-head CodeRabbit review-of-record and green applicable CI.
Before deployment verify scheduled-task checkout and clean `main == origin/main`. After deployment,
verify installed revision, fresh successful cycles, source receipt, broker positions/protection,
hidden worker process, and unchanged research INVALID/fault state. Read-only broker probes must not
invoke `CanaryRunner` or any order method against the deployment ledger.
