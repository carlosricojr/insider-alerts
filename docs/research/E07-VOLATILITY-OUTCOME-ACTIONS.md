# Corporate actions in the E07 volatility replay

## Outcome

Run the fixed, volatility-scaled and matched-width research books on a common
as-traded price path. A complete replay includes cash earned from ordinary
distributions. Unknown coverage and actions that can change the security,
the position or protection make the comparison unavailable.

## Contract

Use stable listing identity and the regular exchange session to key coverage.
An explicit empty entry means a qualified source attests to the absence of
relevant actions. The provider adapter must bind the issuer and share class,
complete its event pagination, resolve revisions, and preserve source, event,
observation and effective timestamps. Feed failures, gaps, identity ambiguity
and unresolved conflicts cannot supply a covered empty entry.
Require coverage for every potential holding window before the first fill,
including candidates that may be suppressed in one or more books. Preserve the
replay's coverage digest alongside the provenance of the upstream records.

Include an ordinary cash distribution only when the qualified record verifies
its cash entitlement per share in USD under the study's frozen fee and
withholding treatment, the exchange ex-date, payment date and the first
session the cash is available to a pre-open decision. Qualify availability
from broker evidence or a preregistered release convention. Availability
must follow the ex-date and fall on or after the payment date.
Forbid duplicate cash event identities for a listing across the replay.

Positions carried into the confirmed ex-date earn the distribution before
the opening trades. An opening purchase that day has no entitlement.
Mark earned receivables in equity and release them into spendable cash on or
after the specified date. Keep cash earnings attributed to the position after
sale, including distributions payable beyond the final session. Round each
position's event cash to cents with the same half-up convention as the replay's
trade cash. Keep the as-traded trigger prices and order levels.

Reorganizations, splits, delistings, discontinuities, due bills, or
distributions that can cancel the broker protection need separately qualified
execution mechanics. Identify them on the sessions where they can invalidate
sizing or execution. Qualify the separate volatility lookback's close basis
across share-changing events. IBKR leaves ordinary GTC levels unchanged and generally
cancels GTC orders when a distribution exceeds 3% of the prior close or when
shares are split, exchanged or distributed. The data adapter must establish
that protection stays in force before classifying an event as ordinary.
[IBKR GTC treatment](https://www.interactivebrokers.com/support/faq/content/cancel-gtc-orders-ex-dividend-68621616)
[DTC distributions guide](https://www.dtcc.com/-/media/Files/Downloads/legal/service-guides/Distributions-Service-Guide.pdf)

## Verification and handoff

Verify same-session buys and exits, entitlements after disposal, different
holding paths across books, delayed cash reuse, and fail-closed behavior for
missing coverage, invalid release dates and duplicate or unsupported events.
Run full lint, strict typing, tests, and the review of record before deployment.
The caller still needs independently qualified prices, listing identity and
action records from the frozen source contract before reading replay outcomes.
