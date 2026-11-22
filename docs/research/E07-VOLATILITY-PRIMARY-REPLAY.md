# Standalone primary comparison for E07 exit-width research

## Outcome

Provide a two-book replay of scaled versus fixed 10% exits through the
same cash settlement and evidence rules as the existing three-book replay.
An outcome-blind synthetic case with a $10 auction and a $9.45–$9.65 first
protected range reproduced the coupling. Only the 5% placebo stop could
be crossed. The existing comparison aborted before either primary book
ran. The 10% fixed and 15% scaled stops were both resolved. An invalid
placebo width can also abort candidate validation. Different exit dates can
free a slot in only the placebo book, where an unaffordable opening gap can
abort the shared run. The two-book path isolates all three failure points.

## Contract

Keep the existing three-book contract for diagnostics. The primary result
contains exactly the fixed and scaled books, their paired dollar increments,
and the shared action and opening evidence digests. It checks only the
active width choices. A missing scale keeps the fixed fallback. A price
range that can cross either active barrier fails the primary replay.

Reuse one replay kernel for both entry points and one settlement calendar
binding. Keep the frozen roster, all required sessions and full drain,
whole shares, costs, corporate actions, marks and cash-release rules.
Run diagnostics against the same sealed data with a separate result.
Unavailability of a diagnostic supplies no reason to discard a valid
primary outcome. Freeze candidate, width and diagnostic provenance before
the outcome stage. A diagnostic that completes must agree with the primary
books on the same source evidence and policy. Check agreement with
`require_primary_agreement` before using that diagnostic. A disagreement is
an integrity error. The caller records why a diagnostic was unavailable.

This work belongs to the offline research replay. The existing worker
fingerprints and live E07/F00 trading policy are outside the diff.

## Verification and handoff

Prove the matched-only failure, primary-side failure, and ignored unused
matched width using synthetic inputs. Verify a cash failure isolated to the
placebo and preserve the primary's complete shared-evidence checks. Compare
primary and diagnostic results when both are available, including all shared
digests and the settlement binding. Retest the legacy path and complete strict Python checks and
an exact-head review. Install only the reviewed merge on clean main.
Actual strategy evaluation still requires qualified sources, sufficient
power, verified pre-open custody and a frozen study registry.
