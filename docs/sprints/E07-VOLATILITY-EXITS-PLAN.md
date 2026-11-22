# E07 volatility-scaled exit study

Status: design and input qualification. Owner objective: implement and operate a
separately registered historical and prospective shadow comparison of volatility-scaled
exits against frozen E07/F00.

## Scope and completion

The strategy idea changes the stop and target width only. Use identical candidate
selection, ranking, two $200 whole-share slots, comparable cash and costs, and a
ten-session maximum hold. Existing ledgers, frozen research definitions, live tasks
and broker/order code are outside the release. The study writes to its own custody
store and has no order or preview methods.

Complete when reviewed merged releases provide a reproducible historical replay
with an honest evidence label, a frozen separate registry, and a witnessed hidden
shadow cycle with decision-time provenance. The shadow inference has a fixed
terminal look. A result or promotion is a separate decision.

## Evidence boundary

Legacy candidate metadata and completed-bar receipts can support a labeled
development replay. Symbol-level date coverage does not prove that the exact
closes had a common contract, price basis and durable pre-open read. Preserve the
invalid trial and commissioning evidence in their original stores. Previously
studied and commissioning dates are ineligible for the successor's confirmation.

Run the new code from the editable deployment checkout as a sibling package to
`insider_alerts`. Keeping it outside the live package preserves the running
canary's source fingerprint. The study binds each response's contents to the
qualified instrument and provider clock before freezing widths. The first
eligible response, prospective calendar, settlement dates, and action
qualification are independent of results.

## Release sequence and gates

1. Build and test the frozen, order-incapable calculation and equal-cash replay
   on explicit data bundles. Use 21 consecutive prior-session closes from one
   point-in-time snapshot for 20 daily log returns. Calibrate one reference
   on a closed development prefix using features only. Bind the fitted reference,
   bounds, fixed-width missing-data fallback and deterministic matched-width control.
2. Qualify historical input adapters and run a labeled development replay. Freeze
   the sample and eligibility inputs before outcomes are opened. Require known
   settlement dates, reasonable cost/friction scenarios, actual fill assumptions
   and explicit missing outcome data. Report after-cost paired dollar P&L, both
   arms' occupancy and cash, and coverage even when the result is unusable.
3. Preregister the successor with a fresh sample, a quantified minimum effect and
   power gate, pre-open completeness receipts, locked inputs and terminal inference.
   Install only the reviewed order-incapable shadow task. Block confirmation on
   missing or inconsistent evidence rather than reusing a contaminated cohort.
4. Observe a fresh successful shadow cycle and verify enrollment or stated
   missingness, locked artifacts, scheduled task invisibility, and unchanged
   live worker reconciliation from clean synced main.

Use separate one-objective PRs wherever independently deployable stages warrant
them. Before coding, challenge the design with read-only Claude. Before merge,
run targeted and full tests, strict typing, lint, adversarial review, the PR review
of record and required CI on the settled head. Retain deployment SHA and health
evidence in the PR. Rollback stops the new shadow task and retains evidence.

## Current replay assumptions

The primary comparison is scaled exits minus fixed exits in net portfolio
dollars. The matched-width book is a diagnostic with the same deterministic
candidate rank and cash constraints. The fitted width distribution comes from
the development prefix, so its distribution can differ from scaled widths in a
later volatility regime.

The primary fill proxy follows E07's daily-bar stop-first rule even when a
bar first opens at the target and subsequently touches the stop. The collision
sensitivity honors an opening print at or through the stop and otherwise uses
the target if both barriers are reached. Count bars touching both barriers and
the subset with an open strictly between them. Portfolio selection reserves cash
before observing the auction. Fees and adverse slippage are charged separately
from the modeled execution price. An auction that would consume more than the
available settled cash needs independent broker or venue evidence and otherwise
makes the replay unavailable. Bind the
[reviewed securities settlement schedule](E07-VOLATILITY-SETTLEMENT-PLAN.md)
and the cash-release sensitivity before replay.

The replay consumes already-qualified candidate and price records. Adapter
verification must bind the as-of candidate roster and rank, the actual close used
for whole-share sizing, independently evidenced auction misses, and one
instrument and price basis through the holding period. A positive daily-bar
result is conditional on this execution model and its stress checks.

## Verification and handoff

The independent design and implementation challenges found that late or mixed
observations could create a false forecast, and that an open through the stop
must remain a stop in the collision sensitivity. Widths bind the instrument and the fixed 09:18 New York feature cutoff.
The isolated kernel validates
the official prior close, the bar observation clock, and the first executable
session within the supplied calendar. Capital and exit fixtures test independent
book occupancy, no hindsight replacement, realized cash checks, T+1 availability,
missing outcome evidence and exact frozen default parity.

Current handoff is the synthetic-tested offline kernel. Historical data custody,
the separate frozen protocol, and prospective shadow commissioning are the
remaining deliverables. Keep their missingness visible rather than populating
eligible rows from later data.
