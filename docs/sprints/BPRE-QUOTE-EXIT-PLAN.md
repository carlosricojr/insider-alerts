# Broker price-band compatible due exit

PR71 same-ID repricing was rejected by IBKR price bands: .01 was more aggressive
than the accepted threshold11.15774 vs market12.37. Broker STOP survived; worker
was disabled to prevent repeated rejected repricing while correcting selection.
Design challenge confirmed that quote freshness must track real bid ticks rather
than ticker.time, and that exact price/identity intent must precede dispatch.

Replace fixed floor with a bounded live streaming bid (real bid tick1, live type1,
finite positive size, aware bid time, <=3seconds old after revalidation). Cancel
owned subscription in finally. Repeat broker order/position/identity checks after
quoting, require unchanged accepted SDK state/remaining quantity, and change only
the existing target limit price. No overridePercentageConstraints, account, size,
order type, TIF, reference or OCA changes. Durable callback stores exact requested
price, quote timestamp, conId, permId and orderId before any mutation; persistence
failure blocks dispatch. Already-closing target-bound exits can reprice on later
cycles without new sell orders. Partial/foreign/unknown state fails closed.

Tests: fresh real bid vs delayed/stale/missing quotes, unsubscribe, no added sell,
price-band fields preserved, rejection/disconnection, exact intent attribution and
unchanged bid adoption. Full Python gates, independent review, canonical PR gates.
Deploy reviewed main only, retain commissioning journals/disabled collector, then
verify BPRE flat, OCA canceled, NPB protection and healthy hidden worker. Quote probe
at17:46UTC verified live BPRE bid12.34 size700, ask12.38, actualbid timestamp.

Handoff: correction in isolated worktree; live worker intentionally paused with
broker stops present until reviewed correction passes gates.
