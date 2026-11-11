# Certifi trust-store maintenance — 2026-11-11

## Outcome and constraints

Refresh only the locked Certifi bundle from 2026.1.4 to 2026.10.23. This retains
the emSign G3 removal and the subsequent C1/C3 removals. Primary evidence:

- [Certifi advisory GHSA-6q48-6c7w-9gw2](https://github.com/certifi/python-certifi/security/advisories/GHSA-6q48-6c7w-9gw2)
- [Mozilla compromise notice](https://blog.mozilla.org/security/2026/10/20/protecting-firefox-users-following-the-compromise-of-emudhra-intermediate-certificates/)
- [Certifi 2026.10.19 to 2026.10.23](https://github.com/certifi/python-certifi/compare/2026.10.19...2026.10.23)

Reuse PR #66 and preserve its published commits with a normal merge into current
main. No other locked package, live E07/F00 policy, capital, account mode, broker
gate, research registry, activation record or historical snapshot changes.

## Verification

- Resolve with CI-pinned uv 0.9.15 and Python 3.12; verify frozen installation,
  all other package records, and PyPI artifact hashes/sizes.
- Demonstrate that the prior bundle fails the three DER-fingerprint distrust
  regressions and the new bundle passes them without a network connection.
- Run Ruff, strict Mypy, the complete test suite and the 71% coverage gate.
  Require current-head Linux quality and Windows research-boundary CI.
- Obtain independent settled-head review and canonical CodeRabbit review of
  record, pin evidence, and verify readiness before an exact-head merge.

The required preimplementation Claude challenge was attempted on 2026-11-11.
The authenticated provider refused it with a usage-limit response before any
review. AGENTS requires this challenge when available; no limit, billing or
provider setting was changed. This attempt is not substantive review evidence.

## Deployment and handoff

This is CLI/worker maintenance with no web interface; browser verification is
inapplicable. Offline CLI and broker-boundary tests are the applicable checks.

The live Windows target is not verified from this worktree. Before deployment,
read the actual worker scheduled-task arguments, resolve its checkout, and
confirm a clean `main == origin/main` at the reviewed merged revision. Inspect
installed Certifi version and its trust-store fingerprints with the target
interpreter. Preserve the live ledger, environment and research evidence.

The canary source fingerprint hashes Python source, so a lock-only update does
not itself prove that a running process loaded the new bundle. A deployment
requires explicit installed-revision and dependency evidence plus fresh cycle
heartbeats, no cycle error, broker reconciliation and hidden tasks/processes.
Use documented order-free diagnostics; do not start or restart a live-armed
worker as a maintenance check. If a safe deployment cannot be verified, record
the missing evidence and continue independent fleet work.

Research activation/commissioning provenance also binds the dependency lock.
The active `OPP-E07-V1` registry binds main's exact lock digest, and the proposed
lock produces `activation_artifact_digest_mismatch:dependency_lock_sha256` in
the real registry validator. An in-memory diagnostic substituting only main's
lock digest passes. This is a merge blocker, not a test to relax. The frozen
preregistration requires every bound artifact to remain identical after
activation. Retain immutable historical records; do not overwrite or re-sign
them merely to make validation pass. Resolving this conflict requires an
explicit scientific/environment maintenance decision and separately reviewed
implementation; this PR does not authorize a new cohort or amended inference.

If a regression is demonstrated, use a reviewed revert on main and the same
verified deployment procedure. Do not disable TLS verification or restore
distrusted roots as a workaround. Production remains unverified until the
target evidence above is captured.
