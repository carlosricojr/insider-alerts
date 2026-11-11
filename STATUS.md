# Status — 2026-11-11

## Phase

Environment-isolation planning only. Carlos selected separate research/runtime
environments while preserving the current OPP-E07-V1 cohort and sealed records.
No implementation, trust override, task cutover or activation is approved here.

## Ball is with

Coordinator: current-head independent review, canonical review and applicable
verification. Carlos: the plan's explicit architecture/interim-posture choices,
then evidence-dependent security and activation decisions. No live worker is
paused or restarted by this document.

## Next action

Review [the isolation plan](docs/deployment/2026-11-11-environment-isolation-plan.md).
Complete blinded Trial-integrity and per-consumer/process/store/TLS evidence.
Reject any split requiring sealed-artifact edits or a validator bypass. Preserve
position management, the cash account, two nominal $200 slots and broker gates.

## Evidence

- PR #66 remains blocked: its Certifi lock differs from the active registry.
  A virtualenv split alone cannot satisfy checkout/activation artifact checks.
- Read-only Windows metadata shows the shared clean main checkout at c791c4f,
  its LF-normalized sealed lock and installed Certifi 2026.1.4. Per-process trust
  is not verified. Trial result 2 has a blinded invalid_evidence_dispositions
  error; its underlying integrity cause remains a blocking diagnostic.
- Required available-Claude and independent Sol design reviews requested
  revisions. The plan addresses ownership, custody, trust and safe operational
  handoff without changing runtime or scientific records.
- Frozen uv 0.9.15 / Python 3.12 Linux full gate: Ruff/Mypy/coverage pass;
  pytest records 1,319 passed, 13 skipped in an isolated network-disconnected
  container. Runtime, test and lock bytes match the reviewed source. Earlier
  Darwin failures and a read-only fixture-mount failure remain in local evidence.
- TLS admission includes compromised-intermediate rejection through alternate
  cross-signed paths. Root removal or successful HTTPS alone is insufficient.
- This PR changes documentation only. Runtime code, locks, registry, policy,
  historical artifacts, installed checkout and running workers are unchanged.
