# Live management before auxiliary work

Outcome: historical-data discovery or shadow enrichment must not prevent existing
live positions from being reconciled, protected or exited. New entries remain
fail closed when auxiliary work fails or broker state changes after management.
This reduces avoidable unmanaged exposure; no guaranteed P&L improvement or alpha
promotion is claimed.

Root-approved design: validated connect/calendar, live management, a five-second
whole asynchronous auxiliary budget, fresh entry-window positions/order proof,
then unchanged entry gates. The budget is an operational service limit below the
15-second poll and existing 30-second history-request timeout, not an investment
parameter. It cannot preempt synchronous SQLite/OS waits. Shadow-only execution
retains its current behavior. Auxiliary errors/timeouts preserve retryable source
records, disconnect the broker to clean canceled SDK requests, retain cycle error
visibility, and submit no new buys. Record management success separately from
whole-cycle success.

Keep E07/F00 eligibility, ranking, timing, capital, cash, two $200 slots, broker
safety gates and frozen research evidence unchanged. The commissioning collector
stays disabled. Per-position order-rejection isolation and partial-fill accounting
require separate designs and are outside this change.

Verification: ordered management-before-bars traces, filled-entry protection and
due exits despite discovery/shadow failure, bounded stalled history and disconnect
cleanup, no buy on failure/state drift, real fresh-entry broker refresh, existing
calendar/ranking/cash/cutoff regressions; ruff, mypy, full pytest with Windows job
tests isolated, independent adversarial review and canonical exact-head PR gates.
Claude design review was attempted by root but unavailable due spend limit; an
independent read-only audit and root design challenge approved this staged scope.

Handoff: implementation authorized in isolated worktree. Root must inspect settled
diff/readiness before merge/deployment. Deploy only reviewed merged origin/main,
verify task arguments, clean synced main, hidden worker, fresh live-management and
cycle heartbeats, broker reconciliation, and disabled commissioning collector.
