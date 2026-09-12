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
- Reject malformed, stale, contradictory, empty, or unsupported-year input.
- Operational fallback approval expires October 1, 2026 (exclusive), ahead of early-close season;
  review September 22. Do not silently extend approval or alter existing intraday order timing.
- Bound historical schedule calls and preserve IBKR request-error configuration.
- Record source, calendar digest, validation time and fallback reason in live operational metadata.
- Research adapter reports missing schedules explicitly; never imports the live fallback.

## Verification and handoff

Read-only broker check confirmed INBX and RWAY quantities and both stop/target orders.
Claude design challenge attempted: unavailable due subscription usage limit. Independent alternative
review pending. Require focused tests, full ruff/mypy/pytest, adversarial review, CodeRabbit loop,
exact-head CI, reviewed merge, and post-deploy broker/worker/hidden-process checks.
Status: design, no production changes. Rollback uses reviewed source revert, never evidence rollback.
