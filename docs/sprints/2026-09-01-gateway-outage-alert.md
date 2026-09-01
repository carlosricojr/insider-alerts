# Gateway outage notification

## Outcome

Notify the operator when the live canary has continuously failed for five minutes, and once when
it recovers. The notification path must remain isolated from trading and research custody.

## Constraints

- Preserve E07/F00, cash-account mode, the two nominal $200 slots, and every broker gate.
- Keep the existing hidden `pythonw.exe` worker and watchdog task topology.
- Persist incident and delivery state in the live-canary ledger so watchdog or source-revision
  restarts do not normally duplicate an alert.
- Store only bounded failure categories, timestamps, counters, and secret-free ntfy receipt hashes.
- Prefer delivery over perfect deduplication: a crash after ntfy accepts a message but before its
  receipt commits may cause one duplicate after the retry interval; it must never suppress the
  outage indefinitely.
- Do not add operational notices to the packet-scoped research notification journal.

## Design

1. Add an optional single-open-incident table, initialized separately from `CanaryStore`, with a
   partial unique index and a 100 ms database budget.
2. On each caught cycle failure, create or update the open incident. Reserve at most one delivery
   attempt per five-minute retry interval once the outage is five minutes old.
3. On the first successful cycle, resolve the incident. If an outage send was attempted but its
   receipt was not committed, send a combined recovery/outage notice rather than silently losing
   the indeterminate side effect. Resolve a pre-threshold transient silently.
4. Abandon an undelivered recovery notice if another failure begins, avoiding a stale recovery
   message during a renewed outage.
5. Fence transition and dispatch with both a coroutine lock and a cross-session, ledger-keyed OS
   mutex. Revalidate the exact reservation under those fences, use an asynchronous five-second
   total HTTP deadline, and dispatch in the background so the next broker cycle is not gated.
   Diagnostics are best-effort and transient tracker initialization failures retry once per minute.

## Verification

- Focused state-machine tests: threshold, durable dedupe, failed-delivery throttling, recovery,
  renewed outage, restart state, sanitized payloads, and send/commit crash window.
- CLI tests proving alert failures are isolated from both failed and successful canary cycles.
- Full `ruff`, strict `mypy`, and `pytest` gates.
- Independent adversarial review and the exact-head CodeRabbit PR loop.
- Post-deploy: current revision, hidden task actions, fresh heartbeat, one outage notice if Gateway
  remains unauthenticated, then port 4001/broker reconciliation and one recovery notice after login.

## Handoff state

- Root cause proven: Windows reboot at 2026-08-31 23:13:38 ET ended the authenticated Gateway
  session; the relaunched GUI never authenticated or opened port 4001.
- `claude -p` design challenge attempted on 2026-09-01 but refused by the service because the
  account had reached its Fable 5 usage limit. No Claude review result was produced.
- Implemented on `fix/gateway-outage-alert` with 15 focused state-machine and CLI tests.
- Full local gates passed on 2026-09-01: `ruff`, strict `mypy` on Windows and Linux, and the
  complete `pytest` suite (five expected skips).
- Initial Linux CI exposed interpreter-dependent literal narrowing in `_failure_kind`; the final
  typed lookup passes Mypy under the CI-equivalent Python 3.12/Linux target as well as Windows.
- Three independent hostile `codex exec` review passes found and drove fixes for crash-window
  suppression, stale cross-process sends, blocking budgets, optional-schema isolation,
  same-event-loop Windows mutex re-entry, and retryable tracker initialization. The final pass
  reported no actionable findings. Remaining at-least-once duplicate and one-cycle timestamp lag
  risks are documented design tradeoffs to verify after deployment.
- CodeRabbit's substantive first-head review found two issues: CI had already replaced the
  platform-dependent cast, and every tracker connection is now explicitly closed with a focused
  no-leak regression test. The current-head App is adaptively limited and the CLI is absent, so
  the required exact-head rung-2 review remains pending.
- PR review, merge, deployment, and live outage/recovery verification remain pending.
