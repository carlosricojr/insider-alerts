# Measurable strategy improvement

Goal: implement a demonstrable improvement in deployable net strategy performance.
Research plumbing alone is not completion. Preserve live E07/F00, cash-account
mode, two nominal $200 slots, and all broker gates until a specific live change
has supporting evidence and explicit approval.

## Evidence and sequencing

Previous audit was progress: local commit `2d01a9f` proves false holiday sessions
in the INVALID OPP-E07-V1 trial. Three August 31 horizons end September 11 rather
than September 14; the store cannot represent withdrawn sessions. No challenger
return evaluation was performed. Existing trial artifacts stay immutable.

1. **Current: independently versioned research-calendar foundation.** Pin the
   NYSE/ICE published 2026-2028 facts and source bytes. Model every covered date
   explicitly as open/closed/unknown, preserve observed and publication times,
   append reviewed closures/unknown notices with supersession, and freeze horizon
   bindings. Missing dates or unsupported years cannot silently become weekdays.
2. Add isolated commissioning acquisition and date-coverage/readiness checks;
   prove operational freshness and handle extraordinary closures before enrollment.
3. Freeze a new shadow-only protocol testing incremental after-cost performance
   against E07 under equal resources, with conservative execution/cash assumptions,
   synthetic power checks, one terminal look, and a fresh non-overlapping sample.
4. Run the reviewed prospective comparison without interim outcome selection.
5. Implement only an evidence-supported improvement through canonical review and
   explicit live-change approval; verify its intended metric and operational safety.

The full goal remains active until steps 3-5 provide actual strategy evidence and
implementation, not merely successful calendar tests. A null result is evidence
against promotion, not permission to tune the same sample.

## Current release boundaries and design

One PR, calendar foundation only, branched from `83181f4`. No edits to frozen
research modules/registry, live worker or tasks. New module is initially unconnected
to existing consumers and cannot activate a trial or submit orders. No new paid data.
The published annual schedule is a **baseline**, not proof that an extraordinary
closure cannot occur; acquisition freshness and commissioning remain separate gates.

Use a new explicitly-created append-only SQLite journal. Refuse to initialize an
existing file; verify its application/schema identity before writes. Store canonical
content-addressed versions and source bytes, with observed-time monotonicity and
parent links. Readers use `mode=ro`; historical queries bind a specific version.
Support explicit reviewed closure/unknown notices, never infer retractions from
omission in a partial response. Reopening/changing published facts requires another
reviewed definition, not arbitrary broker projections.

## Verification and handoff

Read-only Claude design challenge before implementation; focused tests for source
hashes, calendar facts, early closes, DST, leap day, year rollover, unknown coverage,
withdrawal, immutable versions, historical replay, concurrency, corrupt/foreign DBs,
and no implicit creation. Full Ruff/MyPy/Pytest and independent adversarial review.
Then canonical App/CLI review race, exact-head green gates, merged main only, and
post-deployment heartbeat/reconciliation/invisibility/clean-checkout verification.
No rollout before the review gates. Implementation is isolated in `calendar_v2`;
94 focused cases pass. The initial full suite, Ruff and strict MyPy passed.
Final full-suite and adversarial verification follow the last hardening changes;
exact-head review/CI/merge/deployment evidence belongs in the PR review record.

The read-only design challenge and initial adversarial review verified the PDF
facts. Findings addressed: future observation poisoning, consistently rehashed
corrupt records, original-version binding verification and descendant checks,
unknown dates previously skipped as closed, canonical timestamp validation,
malformed-record errors, SQLite integrity checks, and explicit connection closure.
Observation times remain inside version hashes; semantic retries preserve the
original clock. Head-only retry and local-clock trust limits are documented.
The regular-hours webpage is not yet archived; the README explicitly records
that provenance limitation. It must be resolved before commissioning.

Next release: commission a separate order-incapable acquisition path with frozen
input-quality gates, complete finalized bar-date coverage, closure/freshness
checks, and an outcome-blind observation window. Do not reuse V1 or inspected
observer data as a fresh confirmatory sample. No current profit-improvement claim.
