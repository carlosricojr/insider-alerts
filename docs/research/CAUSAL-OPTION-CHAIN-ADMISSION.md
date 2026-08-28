# Causal option-chain admission

## Outcome

Record a prospective IBKR option-chain definition before each would-be approval is committed, so a
later closed-venue evidence request can use only a chain observed by its information cutoff. This is
capture infrastructure for `OPP-E07-V1`; it changes no hypothesis, classifier, canary policy,
capital, account mode, or order behavior and makes no predictive claim.

## Why this boundary

Recent review packets are committed as decisions in 1.4 seconds at the minimum and 3.6 seconds at
the median. A minute-cadence poller therefore cannot establish pre-decision causality. The capture
must occur after the immutable in-memory rule is known but before `apply_decision` supplies the
authoritative decision timestamp.

The external IBKR request cannot be exactly-once across process death. A durable admission written
before launch closes the retry ambiguity: any existing packet admission, including `admitted`, is
never launched again, and any actual launch suppresses another provider attempt for that symbol for
900 seconds. Skipped rows do not extend that provider cadence.

## Runtime contract

- The alpha script and interpreter resolve to one runtime root: `scripts/` and
  `.venv/Scripts/python.exe`, respectively. The chain database resolves beneath this checkout's
  `data/research` directory. Invalid placement or missing files fail the research capture only.
- Admission uses `BEGIN IMMEDIATE` in the source database. Identity fields are immutable, terminal
  rows cannot be updated, and rows cannot be deleted.
- The child receives an argv list with no shell and runs through the existing Windows
  `CREATE_NO_WINDOW` process-tree helper. The whole process is bounded at 15 seconds.
- Exit 0 means a durable fresh reuse or snapshot. Nonzero and timeout are recorded with exit/error
  metadata and stdout/stderr hashes, never raw child output. There is no automatic retry.
- Capture status is logged and counted, but every classifier decision proceeds to the unchanged
  `apply_decision` path regardless of research success, failure, timeout, or internal exception.
- Only would-be approvals invoke this boundary. A later decision race may leave generic chain
  evidence without an approved signal; it cannot fabricate a signal or enrollment.

## Verification and rollback

Tests cover stable identities, concurrent same-symbol admission, crash-left ambiguity, cadence,
terminal immutability, path confinement, hidden argv launch, timeout/no-retry, and fail-isolated
decision persistence. Deployment retains `pythonw.exe`, hidden task settings, and overlap
suppression. Rollback removes the three capture arguments from the autopilot task; durable admission
and chain records remain append-only evidence.
