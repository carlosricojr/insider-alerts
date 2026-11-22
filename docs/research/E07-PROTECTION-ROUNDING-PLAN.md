# E07 replay protection price rounding

## Outcome

The canary rounds the prices it requests for both protective legs to
cents. The research replay currently uses the unrounded arithmetic
result. A synthetic 10.05 USD fill and 9.05 USD protected low reproduced
a fixed stop missed by the replay. Make the existing canary rounding
available as an explicit research convention for both books and every
diagnostic. The default unrounded path preserves prior results and digests for valid
protective levels.

## Contract

Bind the selected protective rounding convention in execution evidence.
Unrounded runs retain their original digest. A new digest domain covers
the cent-rounded path. Use the same derived stop and target in opening
ambiguity checks and portfolio evaluation. Reject nonfinite or invalid protective levels for any possible fill
before running a book. Keep the
first-second ambiguity rule, closing-print rule, cash rules and complete
roster. Check rounding on requested protective levels. Preserve raw
auction, trade and mark prices.

The exact canary function is a pure expression inside the live module.
Keep the research replica inside the offline package and test it against
the canary function. The live and rehearsal source scopes stay outside
the diff. The projected levels describe the initial requested protection. Orders
repaired later can use different prices. Broker activation and actual
execution still require separate source qualification.

## Verification

Prove the stop case, a target case, a newly ambiguous first protected
second and a nonpositive or collapsed rounded bracket. Exercise scaled
and matched arms and both replay entry points. Compare against exact live
rounding including half-cent boundaries. Verify the original digest and
path parity with the current default. Run the full tests, exact-head
review and installed checks. Freeze the final convention in a new
research registration before judging outcomes.
