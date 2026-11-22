# Opening execution in the E07 volatility replay

## Outcome

Replay all three research books from qualified auction evidence. Measure the
entry debit and exit levels from the modeled or observed fill. Evaluate the
entry session's barriers using prices executable after protection becomes
active. This research kernel runs offline.

## Acceptance

- Bind the instrument and session to the primary venue, auction evidence,
  and one declared protection timing convention. A missing auction print
  needs investigation. A confirmed miss requires independent evidence.
- Every candidate entry needs an immutable evidence reference before replay,
  including candidates that later compete for the two cash slots.
- Qualify the fills and eligible price interval against the frozen exchange
  calendar. One explicit positive delay, frozen before outcomes, defines
  the start of protection for all entries and arms. Take the first
  executable price in that interval and attest coverage through the close.
  If the source cannot separate prices around activation, or the stop and
  target have materially different activation times, reject that path until
  its execution convention is decided and registered.
- A known first executable print through a barrier on the entry day
  determines the gap exit in both sensitivity paths. Preserve the
  primary stop-first rule and target-first sensitivity for ambiguous entry
  intervals and the existing rules for the full holding-session bars.
  Keep the tenth-session time exit and reserve slots at submission.
- Record the digest of consumed opening records, effective clocks, delay
  and holding prices alongside corporate-action and settlement evidence.
  Require the complete potential holding window before simulating any
  book. The source adapter supplies eligible trade conditions, immutable
  record references and complete price coverage. Delayed reopen gaps,
  exchange close basis, tick rounding and the measured broker protection
  convention require separate calibration before a prospective trial.

SMART market-on-open routing is designed around the primary listing venue.
The NYSE opening process can extend beyond 09:30 ET. Venue-specific auction
evidence and the regular-hours clock establish the evaluation boundary.
[IBKR order types](https://portal.interactivebrokers.com/en/trading/ordertypes.php)
[NYSE opening and closing auction schedule](https://www.nyse.com/publicdocs/nyse/markets/nyse/NYSE_Opening_and_Closing_Auctions_Fact_Sheet.pdf)

## Verification and handoff

Challenge the design before implementation. Test a delayed opening, an
activation gap across collision rules, explicit misses, clock and early-close
violations, fixed-arm parity for coincident prices, and independent audit
digests. Run the full local tests and review the settled revision before merge
and deployment.

A successful software gate establishes replay behavior on the supplied
evidence. Historical results require the qualified prices and corporate
actions. Prospective activation still requires actual pre-open custody
evidence, a reviewed registry and a frozen analysis plan.
