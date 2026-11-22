# Same-session width placebo for E07 volatility research

## Outcome

Prepare a separate shadow-only placebo for the proposed volatility-scaled
exit study. The placebo uses the same available widths as the scaled arm
on each entry session and changes which listing gets each width. This
lets the study diagnose the value of the assignment to particular names.

An outcome-blind archive split fitted the existing formula using four
entry sessions and applied it to the following five. Across 47 candidates,
the scaled width changed from 10% by at least 2.5 percentage points for
28 cases and hit the 20% cap for 14. Assigned widths averaged 13.97%
in the scaled arm and 11.82% in the training-distribution placebo.
The content-addressed engineering receipt
`6bf820d86dcd56a17cfbe9688c71b5694d94cb5dc82fb8c493b0766b1614d422`
binds the two periods and the frozen pre-open input archive. The sample
contains nine entry sessions and at most 18 openings per arm. The five later
sessions allow at most ten openings. The archive check uses pre-open inputs.

## Contract

Freeze calibration before enrollment. At 09:18 New York time, seal the
exact eligible packet IDs with durable proof of their qualified forecast.
Use these ready packets within the complete entry-session replay roster.
The lottery, overlap, capacity, cash, and auction result play no role in
placebo membership. A signal admitted to the entry session after cutoff
cannot enter the sealed ready set.

Take one scaled width per ready listing, keyed by the replay's instrument
identity. Hash the canonical JSON list of the versioned
`E07-VOL-EXITS-SESSION-PLACEBO-V1` domain, New York session date and
instrument. Order by digest and use instrument as the tiebreaker. Assign
the ascending width list to the ordered listings. Use the same assignment
for repeated ready cases on a listing. A case outside the sealed set has
its own scaled width as its placebo width, which is 10% for a missing
forecast. Singleton and equal-width groups keep their scaled assignment.

Use the existing equal-cash replay entry point. Run the same-session control
as a separate named comparison from any training-distribution control.
Keep the original candidate roster, fixed widths and scaled widths across
the two runs and verify the fixed and scaled books agree. Conflicting ready
widths on a listing raise `ConflictingListingWidths`. The future runner
records that placebo-only reason and retains the original comparison.

The future runner records the method, cutoff receipt, reassigned-listing count,
assignments
among names actually opened and the mean width of those positions in each arm.
Matching the widths among eligible listings allows realized fills to differ.
This is a descriptive placebo and a single deterministic draw supplies no
confirmatory p-value. The primary comparison remains scaled versus fixed
10% exits. [Conditional randomization and exchangeability](https://academic.oup.com/jrsssb/article/80/3/551/7048447)

The outcome-blind implementation check preserved the exact listing width
multiset on all five later sessions and changed 31 of 38 ready listing-day
assignments. Nine of the ten hypothetical top-ranked positions would receive
a different width if both slots opened on every date. That count ignores
carried positions, cash and execution. The private intake manifest at
`0b41839674df203e9c8226bf26c2bbb18163200875d404650a7e3874cd038f66` binds the implementation and assignment digest.

## Verification and handoff

Exercise exact width preservation, duplicate listing consistency, cutoff
and missingness checks, order independence, singleton behavior and the
existing offline replay entry point. Run an outcome-blind measurement on
archived pre-open inputs only. Review and deploy the isolated research
implementation. Activation belongs to a separate frozen study with
qualified outcomes, sufficient power and pre-open custody.
