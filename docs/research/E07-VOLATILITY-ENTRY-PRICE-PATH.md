# Assemble qualified entry prices for the E07 replay

## Outcome

Build the entry-day auction and protection input for the offline volatility
replay from a qualified opening print, one-second observations around activation,
regular-session minute observations, and a primary closing print. The
assembler takes the latency and closing-print convention as study inputs.

The three-venue controls covered full and early-close sessions. Their
primary auction close differed from the final minute close on all six
instrument-sessions. The live ledger's 20 matched opening fills show a
local submission-return interval of 1.63 to 16.65 seconds from the tape
timestamp. It does not identify broker acceptance or election time.
Eleven of 24 out-of-window engineering cutoffs returned different eligible
prices at the same first second. Preserve their range. An ambiguous first
price inside all possible barriers can still support the planned daily
collision rules.
[IBKR order-status definitions](https://www.interactivebrokers.com/docs/tws-api/doc/order-management/order-status)
[IBKR historical time and sales](https://www.interactivebrokers.com/docs/tws-api/doc/md/historical-ticks)

## Contract

- Source custody establishes dated listing identity, primary venue, eligible
  conditions, action treatment, complete source windows, and explicit
  no-trade intervals. The assembler binds the source digest and the normalized
  observations to the replay input.
- Require one qualified primary opening and closing print. Resolve the
  modeled activation against the published opening timestamp. Require a
  complete second-by-second prefix containing the first eligible price range.
  Qualified native second bars can establish the first price and extrema from
  a consistent source. A normalized trade sequence can also supply that
  range. The handoff to minute data must be on a minute boundary, with one
  observation for every remaining regular-session minute. An empty interval
  requires a source-backed no-trade observation.
- Fail closed on a timestamp bucket straddling activation, gaps, inconsistent
  identity and invalid clocks or prices. Before simulating any book, check
  whether a first-second price range with distinct endpoints can touch a
  barrier in any arm. Reject that comparison until finer evidence or a
  separately preregistered ambiguity treatment is available.
  Preserve early closes and permit a primary closing print inside the
  declared reporting window. Value or liquidate positions at the qualified
  closing mark. Evaluate a qualified closing price after the continuous
  range when the study convention allows a closing trigger. Label exits
  triggered by the closing print separately. The reporting window and
  closing eligibility apply to the whole comparison.
- Compare the resulting evidence through the normal replay entry point.
  Keep compatibility between a new fill and an older possible holding by
  requiring one closing mark for the same listing and session.

## Verification and handoff

Exercise late opening, partial activation minute, sparse sessions, repeated
timestamps, early-close auction reporting, malformed or truncated paths,
deterministic source binding and independent closing-mark and barrier
behavior. Run strict typing, lint, the full test suite and the review of
record before deployment. Historical outcomes require qualified source
records. A prospective trial requires registered execution conventions and
proof of the pre-open evidence capture.
