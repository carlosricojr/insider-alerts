# Terminal coordinator startup retry

## Outcome

Keep a transient Windows-logon failure to launch or complete Git artifact verification from being
misreported as scientific `INVALID`, while preserving the blinded coordinator's at-most-one
terminal transition invariant.

## Constraints

- Do not change the activation-bound inference, terminal-builder, registry, policy, or lockfile
  artifacts.
- Retry only exact Git-unverifiable results from a separate startup custody preflight. Execute the
  transition-capable coordinator body at most once after that preflight succeeds.
- Keep retry timing bounded inside the existing hidden `pythonw.exe` process. Do not enable Task
  Scheduler restarts, because a post-transition logging failure must wait for the next invocation.
- Preserve blinded logs, order incapability, E07/F00, cash-account mode, two nominal $200 slots,
  and every broker gate.

## Verification

- Force the real `git show` and `git merge-base` exception paths through the startup preflight,
  prove bounded retries, and prove repeated unavailability finishes as operational degradation.
- Prove scientific invalidity and every result that could follow a transition are never retried.
- Run focused tests, Ruff, strict Mypy, the complete Pytest suite, and an exact-head adversarial
  review followed by the CodeRabbit PR review race.
- After deployment, invoke the hidden scheduled task and verify a successful blinded collecting
  result, no visible process, and clean `main == origin/main`.

## Handoff state

- At 2026-09-01 03:14 ET, the logon-triggered task returned
  `prospective_registry_invalid:activation_git_artifact_unverifiable`; that exact inner code is
  produced only by `OSError` or `subprocess.TimeoutExpired` before terminal state inspection.
- A later invocation through the same registered hidden task succeeded with
  `COLLECTING / transition_deferred_outside_after_hours_window`; activation custody is intact.
- `claude -p` was attempted before implementation and refused because the account had reached its
  Fable 5 usage limit; no Claude result was produced.
- Implemented a separate startup preflight with three bounded retries; the transition-capable
  coordinator body is invoked at most once and repeated preflight unavailability is `degraded`.
- The first independent adversarial review found phase-blind retry authorization and fabricated
  retry fixtures. Both findings are addressed: production retries only the isolated preflight, and
  28 focused tests now force real `git show`/`merge-base` timeouts before startup, during the body,
  after pending-terminal staging, after a deadline receipt, and during decision revalidation.
- The complete post-fix local gate passed: Ruff, strict Mypy across all 66 source files, and the
  full Pytest suite (five expected skips). All 28 focused coordinator tests also pass.
- The pre-commit exact-diff adversarial re-review reported no actionable findings; it independently
  verified phase-isolated retries, real Git timeout paths, at-most-once body execution, unchanged
  hidden-task behavior, and no activation-bound or live-policy changes.
- The exact-head rung-2 review found one low-severity observability gap: recovered retries were
  indistinguishable from clean startup. The wrapper now logs only blinded retry count and first
  failure reason; no outcome or transition data was added. The complete Ruff, strict Mypy, focused
  coordinator, and full Pytest gates pass after this response.
- PR review, merge, deployment, and post-deployment verification remain pending.
