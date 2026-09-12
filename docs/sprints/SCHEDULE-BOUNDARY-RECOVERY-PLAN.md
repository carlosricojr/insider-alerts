# Schedule boundary recovery

## Outcome and constraints

Restore live canary calendar acquisition with an independently sourced, fail-closed fallback.
User authorized live-only fallback after IBKR historical schedules failed with error 162.
Keep OPP-E07-V1 INVALID and all frozen evidence, registry, dependencies, orders, and capital unchanged.
No historical corrections, trial restart, or research calendar fallback.

Base: `0d8ba33247aebaa8aa89953bd6b56f9d5ae21560`.
Isolated branch: `fix/schedule-boundary-recovery`.

## Design

- Pin official NYSE 2026 calendar dates; no new dependency or network calendar at order time.
- Require fresh, bounded IBKR SPY contract-hours agreement including today and next session.
- Reject malformed, stale, contradictory or unsupported fallback input; native IBKR schedules
  remain usable outside fallback-table years. Fallback requires fresh contract-hours agreement.
- Fallback NEW ENTRY approval expires October 1, 2026 (exclusive); existing positions retain
  calendar-validated reconciliation and exits after expiry. Review September 22 before expiry.
  Last permitted entries finish their ten-session horizon before early-close season. Do not
  silently extend approval or alter existing 15:30 submission / 15:45 MOC cutoff timing.
- Bound historical schedule calls and preserve IBKR request-error configuration.
- Record source, calendar digest, validation time and fallback reason in live operational metadata.
- Research adapter reports missing schedules explicitly; never imports the live fallback.

## Verification and handoff

Read-only broker check confirmed INBX and RWAY quantities and both stop/target orders.
Claude design challenge attempted: unavailable due subscription usage limit. Codex independent
design challenge completed. Its findings drove explicit armed-canary-only authorization, expiry
blocking entries rather than protection, 60-second/midnight new-entry freshness gates, exact hours
identity and coverage validation, and strict non-recoverable disagreement handling. The canary's
existing operational shadow book shares its calendar; the separate frozen trial never imports it.
Require focused tests, full ruff/mypy/pytest, adversarial review, CodeRabbit loop,
exact-head CI, reviewed merge, and post-deploy broker/worker/hidden-process checks.
Independent adversarial review identified four issues, addressed before final review: stale entry
validation no longer blocks management; fallback-year bounds do not block valid native schedules;
timeout/cancellation resets the live connection rather than leaving unanswered requests; broker
request failures use typed retryable operational errors. Focused regressions cover each case.

Live read-only preflight passed September 12: explicit 162 fallback, fresh SPY liquidHours agreement,
correct September 14–25 next-ten-session horizon. No ledger or order methods invoked.
Status: implementation and final verification, no production changes yet.
Rollback uses reviewed source revert, never evidence rollback.
