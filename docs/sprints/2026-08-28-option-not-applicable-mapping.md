# Option-surface not-applicable mapping

## Outcome

Recognize alpha-core's exact versioned `OPTION_CHAIN_NOT_LISTED` process result and persist the
capture-only option observation as `not_applicable`, without treating expected absence as an
operational error. Preserve every existing retry, closed-session fallback, evidence, enrollment,
capital, and order rule.

## Constraints

- Trust only exit code 4 plus a strict JSON stdout envelope whose schema, status, reason, source,
  request ID, canonical symbol, client ID, timestamp, and exact field set all match.
- Never inspect IBKR stderr prose to infer absence. Generic exit 4, malformed JSON, identity drift,
  or an unexpected artifact remains a terminal typed capture failure.
- A valid not-applicable result publishes no artifact, creates no global evidence error, and does
  not affect confirmatory enrollment because option surface is capture-only in OPP-E07-V1.
- Existing immutable RCG and LCNB evidence is not rewritten or backfilled.
- Deploy only after the compatible alpha-core producer is merged and installed in the detached
  runtime. Keep scheduled workers hidden under `pythonw.exe`.

## Verification

- Failing-first tests cover exact acceptance, all identity fields, malformed/extra content, wrong
  exit code, and forbidden artifact publication.
- Snapshot tests prove `not_applicable` has no error and the job/attempt complete without an error.
- Existing process failure, venue fallback, artifact validation, enrollment, and store integrity
  tests remain green, followed by Ruff, mypy, full pytest, adversarial review, and the mandatory
  CodeRabbit PR loop.
- After deployment, validate clean `main == origin/main`, runtime revision binding, fresh capture
  and trial heartbeats, zero faults, Gateway connectivity, and invisible scheduled-task actions.

## Rollback

Revert this mapping after reverting or retaining compatibility with the alpha producer. No schema
migration or artifact cleanup is required; the prior behavior records a generic missing/error
observation. Append-only evidence records remain immutable in either direction.

## Handoff state

- Compatible alpha-core producer PR 387 is merged and deployed at `bcdb0ce75d353d83b8e32100a21f5169725f5f87`.
- Deployed read-only Gateway smokes proved RCG returns the exact no-chain result without an artifact
  and LCNB recovers its listed monthly chain instead of being falsely classified as absent.
- The downstream focused suite passes 49 tests; the full suite passes 586 tests. Repository Ruff and
  strict mypy are clean.
- Adversarial review findings about request-time timestamp custody and typed durable provenance were
  fixed; the settled-diff review reported no further correctness finding.
