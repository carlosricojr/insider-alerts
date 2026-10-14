# Future calendar projection recovery

## Outcome / evidence

Restore validated existing-position management without admitting new risk from
contradictory future schedule data. User requested triage, design, implementation,
reviewed shipping and deployment on 2026-10-14 after being told the calendar gate
blocked management and a scheduled exit was overdue. Base acd6695.

Read-only SPY probes on October 14 returned 09:30-16:00 on November 26 and 27.
NYSE publishes November 26 closed and November 27 closing at 13:00:
https://www.nyse.com/trade/hours-calendars
The +45-day request horizon reached November 26 on October 12. The strict native
validator correctly rejects the response, but the prior fallback only admitted
empty/162 responses, so the cycle stopped before reconciliation/time exits.
Fresh SPY liquidHours observed 2026-10-14T18:48:49Z agrees with the published
calendar through October 19. No claim about unrecorded historical responses.

## Design / constraints

- Shared structural parser; native validation still rejects contradictions.
- Recovery requires live-armed mode, October 1-December 31 2026, only FUTURE
  published holidays projected as full days or early closes projected as full
  days, all requested published sessions present, at least 20 validated past
  sessions, and fresh strict SPY liquidHours agreement today/next session.
- Reuse the existing bounded published-calendar fallback. Its expired entry
  approval blocks all new buys. Never extend approval, infer another year's
  holidays, change E07/F00, cash mode, capital, sizing, OCA, or order timings.
- Preserve discovery/shadow ordering and research adapter/registry/evidence.
  Missing sessions, malformed/duplicate/zone errors, past/current discrepancies,
  other future discrepancies, stale/mismatched contract hours remain fail-closed.
- Receipt records source, rejected future dates/kinds, normalized response digest,
  validated past count and observation time. This is operational provenance, not
  replacement point-in-time research evidence. No ledger/history rewrite.

## Verification / handoff

Read-only Claude design challenge completed before edits. It narrowed eligible
disagreements to the two observed shapes, required future coverage, and excluded
cycle-order changes. Add adapter and canary tests, then full ruff/mypy/pytest,
independent adversarial review and canonical exact-head PR review/CI. Read-only
live preflight must pass without order calls before merge/deployment. The normal
worker (not a manual trade) will submit existing overdue exits after recovery.

Status: implementation and verification in progress. Deployment evidence belongs
in the PR handoff: clean synced main, merged reviewed revision, fresh successes,
receipt, broker reconciliation/protections, hidden processes. Rollback via reviewed
revert; never remove protection or rewrite evidence. Future holiday rollover and
2027 remain explicit validation boundaries; no claim of unobserved continuity.
Monitor November 26/27 and December 24/25: if projected errors persist once those
dates become current/past, recovery intentionally stops. Existing 15:30/15:45
timings are not early-close aware; no new fallback buys or stored-exit-date
changes are authorized here. Before any new-entry approval, review that separately.
Read-only adapter preflight at 2026-10-14T18:52:44Z succeeded with 51 past sessions,
fresh contract hours through October 19, rejected November 26/27 projection and
the expired-entry gate. Independent review found date-guard tests could pass for
other reasons; fixtures now contain complete per-clock coverage and assert exact
recovery rejection codes. Added December/year-crossing coverage and retained
both native and recovery-rejection reasons in operational errors.
