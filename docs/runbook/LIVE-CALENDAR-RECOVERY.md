# Live calendar recovery — September 2026

Scope: the E07/F00 operational canary, including its existing operational shadow book.
Not a research calendar source, trial correction, new hypothesis, or order-policy change.
OPP-E07-V1 remains INVALID. Its immutable schedule, evidence, fault, and outcome stores are not
rewritten or replayed by this implementation. Dependencies and frozen registry/policy stay pinned.

## Authority

Prefer a valid IBKR historical SPY schedule. The live fallback is permitted only on error 162
containing `No data of type EODChart`, or an empty-list schedule response. Other errors,
malformed successes, and schedule/calendar disagreements fail closed. Timeout/cancellation resets
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
New buys recheck same-New-York-date freshness (at most 60 seconds, no future timestamps) immediately
before submission. Closed dates cannot admit buys. Existing broker safety checks remain required.

## Expiry and existing holdings

Fallback **new entry** approval expires October 1, 2026, 00:00 New York. It is not automatically
renewed. The operator must review on September 22, before expiry, including open positions and the
continued need for fallback. No new scheduled task was created by this change.

After expiry, reconciliation, protective repair, and existing timed exits continue when calendar
acquisition succeeds; freshness/expiry entry gates do not stop those management steps. A calendar
disagreement or acquisition failure still fails the cycle closed and must be investigated promptly;
recorded server-held GTC protective orders are not cancelled. The fallback's returned horizon is
explicitly capped at December 31, 2026, so crossing the +45-day projection into 2027 does not
prematurely stop management. Valid native IBKR schedules are not restricted to fallback-table years.

The last authorized September entries reach session ten in October, ahead of early-close season.
Existing 15:30 timed-exit submission and 15:45 MOC cutoff settings are unchanged. Extending fallback
entry approval requires a separate review of early-close handling; the calendar table alone does
not fix those intraday order timings. Do not change already stored exit dates in this recovery.

## Release gates

Require focused malformed-response, timeout-reset, date/holiday/DST, expiry/freshness, no-buy,
management-continuity and research-isolation tests; full Ruff, strict mypy, pytest; independent
adversarial review; exact-head CodeRabbit review-of-record and green applicable CI.
Before deployment verify scheduled-task checkout and clean `main == origin/main`. After deployment,
verify installed revision, fresh successful cycles, source receipt, broker positions/protection,
hidden worker process, and unchanged research INVALID/fault state. Read-only broker probes must not
invoke `CanaryRunner` or any order method against the deployment ledger.
