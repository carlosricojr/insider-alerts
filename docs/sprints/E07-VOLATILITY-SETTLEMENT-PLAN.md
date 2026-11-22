# E07 volatility-exit cash settlement

Bind the volatility-exit replay books to a reviewed U.S. equity settlement
schedule. The historical candidate archive spans Veterans Day, when stocks
traded and settlement paused. The rehearsal window spans Thanksgiving.

Use the validated exchange calendar for session continuity and the separate
DTC regular-way schedule for settlement. Fail closed beyond the published
October through December 2026 coverage, including the last possible sale and
the extra session required by the sensitivity. Bind both input schedules,
the exact dates and the cash-release convention in the output digest.

The primary proxy makes proceeds available before the open on settlement
day. A fixed sensitivity moves availability to the first exchange session
after that day. Broker cash-release timing requires separate evidence.
Before using either mapping, check for superseding clearing notices.
Retain corrections as new reviewed evidence that references the old version.

Verify archived source hashes, holiday pairs, continuity, failure at both
coverage ends and cash-gated entries through the existing pure replay. Run
full Python gates and the exact-head review. After merge, verify a read-only
binding from synced main and a fresh live heartbeat. This release contributes
calendar inputs only. Historical security identity, price basis and cost
coverage still gate the historical comparison.
