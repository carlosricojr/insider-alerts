# Native calendar response window

Outcome: restore the live canary's native schedule path when IBKR returns valid
sessions outside the requested calendar-date window. Base: bf3eb1279b3ce44a7e45c83c9e5e7fe67cd761d5.

Evidence: read-only SPY SCHEDULE probe on 2026-09-13 returned 120 sessions,
May 8 through October 28, for the requested July 1 through October 28 window.
All expected dates were present; 36 earlier valid dates caused exact equality
to fail. Historical TRADES bars now work after the user's deposit. This is an
observed response shape, not a claim that IBKR universally counts trading days.
Reference: https://interactivebrokers.github.io/tws-api/historical_bars.html
and pinned ib_async 2.1.0 reqHistoricalScheduleAsync (sends duration `count D`).

Design: validate every native row with the existing validator before narrowing
to the inclusive requested date window. Apply unchanged exact 2026-calendar
coverage and minimum past/future session checks to that bounded result; return
only that result and use its dates in the receipt. Invalid or duplicate extras,
missing in-window dates and malformed responses remain fail-closed, without
fallback. Keep existing non-2026 validation semantics; no new calendar authority.
The design challenge also identified a native-path input-validation gap: reject
non-datetime/naive clocks and non-integer/bool/out-of-range counts before broker access,
using the existing 60..365 range without the fallback's year restriction.

Constraints: isolated worktree only; no research registry/evidence changes,
no historical backfill, no account/subscription changes, no order API changes,
no relaxation of fallback authorization, expiry, timeouts or broker gates.
Frozen E07/F00 cash account and two nominal $200 slots remain unchanged.

Verification: Claude design challenge attempted (usage-limited); independent
Codex design challenge before implementation. Regression tests for overbroad
valid responses, missing boundary/interior dates, invalid extras, bounded
minimum horizons, time zones and cross-year behavior. Full ruff/mypy/pytest,
independent adversarial review, exact-head CodeRabbit review loop and CI.
Before deployment, inspect the live task's arguments and checkout; require clean
main == origin/main and merged reviewed commit. Verify fresh successful cycles,
current source fingerprint, broker positions/protective orders, hidden processes,
observer heartbeat and clean synced main after deployment.

Rollback: revert the scoped commit through the same reviewed workflow if needed;
do not bypass the calendar gate, reset ledgers or remove protective orders.
Handoff: independent design challenge complete; implementation and focused
calendar/broker tests pass (99 cases). CodeRabbit identified a missing datetime
shape guard; the guard and regression cases were added. Full gates, final exact-head review,
merge and verified deployment remain required. Deployment evidence belongs in
the PR handoff so this plan does not assert unperformed production checks.
