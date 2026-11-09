# Missing timed-exit recovery

Outcome: recover BPRE's intended exit safely and prevent an expired, unobservable
DAY exit from leaving the worker successfully cycling in `closing` forever.
Preserve E07/F00 sizing, horizon, cash mode and OCA protection.

Evidence November 9: BPRE 16 shares remain at IBKR, exit due November 6;
ledger timed order 771507 is absent from all-open orders and completed orders.
Ten earlier cancellations/reopens and weekend submissions are recorded. The
original broker cancellation reason is unavailable; do not invent it.

Design: only regular-session overdue submission; recover prior-date missing exits
after bounded fresh all-client open-order and position reconciliation. Block on
ambiguous identity, mismatched quantities, unknown competing orders or request
failure. Preserve protective OCA and record recovery. Preserve broker rejection
details. Exercise restart, retries, active/partial orders, off-session clocks,
timeouts and terminal/idempotency cases. Read-only Claude challenge unavailable
(spend limit); independent read-only Codex challenge requested before changes.

Verification: focused regressions, ruff, mypy, complete pytest gate and independent
adversarial review; canonical CodeRabbit review of exact PR head and green CI.
Deploy only reviewed origin/main, disable all triggers and stop/preserve the frozen commissioning collector
before package change (never restart or silently reactivate), restart hidden live
task, verify broker positions/orders, new heartbeats and clean synced main.
Rollback through reviewed main; preserve evidence and broker protection.

Handoff: design investigation underway; no production changes yet.
