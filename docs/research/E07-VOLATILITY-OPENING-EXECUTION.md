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
  calendar. Take the first executable price from the protected interval.
  If the source cannot separate prices around activation, or the stop and
  target have materially different activation times, reject that path until
  its execution convention is decided and registered.
- Preserve time exits, the primary stop-first convention and the
  target-first sensitivity. A continuing position uses its qualified
  holding-session bar. Entries cannot reuse capacity freed after submission.
- Record the entry evidence digest with the corporate-action and settlement
  evidence. The data adapter will preserve raw prices, latency provenance,
  source identity, and qualified coverage for every potential holding session.

SMART market-on-open routing is designed around the primary listing venue.
The NYSE opening process can extend beyond 09:30 ET. Venue-specific auction
evidence and the regular-hours clock establish the evaluation boundary.
[IBKR order types](https://portal.interactivebrokers.com/en/trading/ordertypes.php)
[NYSE opening and closing auction schedule](https://www.nyse.com/publicdocs/nyse/markets/nyse/NYSE_Opening_and_Closing_Auctions_Fact_Sheet.pdf)

## Verification and handoff

Challenge the design before implementation. Test a delayed opening, an
activation gap, explicit misses, clock and early-close violations, all-arm
parity for coincident prices, and independent audit digests. Run the full
local tests and review the settled revision before merge and deployment.

A successful software gate establishes replay behavior on the supplied
evidence. Historical results require the qualified prices and corporate
actions. Prospective activation still requires actual pre-open custody
evidence, a reviewed registry and a frozen analysis plan.
