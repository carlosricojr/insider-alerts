# Recover live management from future schedule corruption

Live management has failed before reconciliation because IBKR's SPY SCHEDULE
projects early closes on regular days, opens Christmas with a short session,
and omits valid future sessions. A fresh qualified SPY LiquidHours request
agreed with the published 2026 exchange calendar across its near-term coverage.

Recover only in the live worker when the structured response has this class of
future short-session contradiction with future omissions. Require complete,
consistent requested sessions through today and the existing corroboration of
today and the next open date using fresh LiquidHours. Keep the validated NYSE
session clock and bounded calendar for existing positions. Mark this mode in
the receipt and exclude it from entry authorization. Continue bounded
operational discovery and shadow settlement on the existing published-calendar
path so signals retain their normal discovery cadence during recovery. Preserve
existing native and previously approved fallback paths. Reject structural errors,
missing historical or same-day rows, and missing or conflicting near-term hours.

Verify the captured response shape and negative boundary cases, including the
November 27 early close, then run local and hosted gates, independent review,
and the canonical review of record. Deploy reviewed main through the current
hidden task. Verify fresh management, the calendar receipt and entry block, and
reconcile holdings and their protective orders via a separate read-only client.
If rollback is necessary, revert through review and retain broker protection.

References: NYSE published hours and holidays, and IBKR TWS API Contract
Details (LiquidHours). All account evidence remains local.
