# Calendar entry renewal and session-relative exits

## Outcome and authorization

Restore new-entry eligibility without changing E07/F00, two nominal USD200 slots,
cash account, reserve, sizing, stop/target, rank, or research. On October 15, 2026
the operator explicitly approved close-minus-30-minute timed-exit submission and
close-minus-15-minute cutoff, including 12:30/12:45 on 13:00 early closes.
Base: 68587af. PR #63 remains a separate objective, not bundled here.

## Constraints and design

- Separate the original October 1 management-recovery boundary from entry approval.
  Preserve the October 1-14 approval gap; renew prospectively from October 15.
- Retain the pinned 2026 calendar and its digest. No inferred 2027 dates; renewal
  ends January 1, 2027 and buys require a complete ten-session validated horizon.
  This can block entries earlier than expiry. No retrospective entries or replay.
- Keep all malformed, missing, stale, current/past-contradictory schedule checks.
  Continued broker projection errors becoming current/past remain fail-closed.
- Renewal explicitly covers the validated projection-rejected fallback that is
  causing the live blocker, as well as unavailable-feed fallback; no widening of
  allowed contradictions. Both native and fallback entry horizons end in 2026.
- Carry validated current session bounds and holding horizon in the receipt.
  Recheck actual dispatch time; do not send a late MOC using a stale cycle clock.
  Keep protective exits independent of entry approval; never cancel them to renew.
  Preserve existing overdue DAY market-exit recovery, including pre-open queueing;
  only same-session timed dispatch receives the approved close-relative windows.
- Preserve research adapter, registry, evidence, and operational shadow semantics.
  Official calendar and auction sources checked October 15:
  https://www.nyse.com/trade/hours-calendars and https://www.nyse.com/trade/auctions.

## Verification and handoff

Read-only Claude design challenge before implementation; focused tests for approval
gap/renewal/expiry, midnight/DST/early close, current/past contradiction, missing
horizon, stale receipts, dispatch delay, OCA/restart idempotence, no new buys on
unknown data, and management continuity. Full Ruff, mypy, pytest; independent
adversarial review and canonical exact-head PR review/CI before merge.

Deploy only clean synced main from reviewed merged origin/main after resolving the
hidden scheduled task. Verify source fingerprint, fresh cycles, no cycle error,
entry gate, broker reconciliation/protection and task/process invisibility. Never
invoke live order methods in preflight. Rollback via reviewed revert, no ledger
rewrite. Status: implementation complete; merge/deployment require exact-head gates
and review evidence, with final handoff recorded in the PR. Full local gates passed
before the final review refinements and must pass again on the release head.
Read-only IBKR preflight at
2026-10-15T19:21:28Z cleared the candidate entry gate, validated 52 past sessions,
and retained FUL's quantity-matched stop/target. No order methods were called.
Review refinements: retain overdue behavior rather than expand authorization;
validate custom references for shortest sessions; assert exact dispatch windows,
NY-date rollover, crash/adoption and missing-horizon reasons. Window misses remain
explicit cycle errors, not silent successes. Final deployment evidence belongs in PR.
Final review refinements add one durable missed-close warning, downgrade expected
midnight missing-bounds warnings, expose runner horizon rejection in cycle status,
and distinguish published future coverage from near-term broker corroboration.
