# Preserve IBKR historical price-basis evidence for E07

## Outcome

Build the offline historical source reader for the observed IBKR price basis.
A dated listing-session receipt must compare independently requested
historical SMART TRADES daily bars and the primary closing Time & Sales print.
One source response can be adjusted while the other retains the price traded.
Keep disagreement visible and withhold any joined historical bar.

Sandisk and KLA showed eightfold and tenfold differences, respectively, on
the session before confirmed splits. Their daily, minute and second bars were
back-adjusted. The closing prints stayed in the original units. The native
daily bars were rounded to cents, so multiplying them by the split factor
would still change historical prices. The NYSE reverse-split control had
native bars and no returned pre-split closing window on the current contract.
[Sandisk SEC filing](https://www.sec.gov/Archives/edgar/data/2023554/000162828026075127/sndk-20261119.htm)
[KLA split announcement](https://ir.kla.com/sec-filings/all-sec-filings/content/0000319201-26-212093/d116682dex991.htm)
[Enhanced Group SEC filing](https://www.sec.gov/Archives/edgar/data/1956439/000162828026074160/a424sticker333-295777x1116.htm)

## Contract

Use one canonical custody payload for a requested listing-session. It
describes a pinned broker contract, independently provided listing identity
and calendar binding, source observation clocks, the full SDK daily response,
and the bounded Time & Sales replies. Observe before processing and record
the SDK object fields relevant to qualification, including the auction price,
size, exchange, sale conditions and attributes. Store canonical bytes under
their digest with create-only semantics. Record partial or failed capture
without converting missing data into a zero-action or no-trade claim.

Check daily and closing response request identity, completion, clock and
uniqueness during reading. Only one valid reported primary-market closing
trade may occur within the declared window. Historical tick pages include the
last second in full. The terminal page must close the window; a gap, invalid
trade, partial page, absent auction or a price mismatch remains unresolved.
Never infer a split factor from mismatched prices. The observed prices can
be usefully corroborated independently on dates without adjustment.

Use evidence-grade names for results. Equality is a necessary broker basis
cross-check. Historical as-traded admission still needs a dated listing and
structural-action record extending through the price observation timestamp,
as well as the existing action coverage over the replay. Confirming that a
closing price matches does not establish executions or complete coverage
for the second and minute paths. The current research gate and study
denominator govern admission. Count missing sessions by reason and keep them
in the frozen roster. A conflict can reflect a later event, so excluding
such sessions after inspection could bias the sample. This check concerns
execution prices. The volatility lookback has its own consistent-basis
contract. The recorder has no ledger or order access.

## Record and inspect a control

Create a canonical JSON plan with `rfc8785.dumps(BasisRequest(...).record())`.
Use the listing identity and calendar record digests, the session's scheduled
close, and the end of the largest declared reporting window. Run one
request at a time during the existing research off-hours window. The CLI
uses a separate Gateway client and writes a content-addressed receipt.
An optional `--supersedes` identifies an earlier receipt for the same plan.

```powershell
.venv/Scripts/python.exe -m ops.research.e07_volatility_ibkr_basis --control-plan <path> --plan-sha256 <digest> --archive <directory>
```

Use `audit_basis` to verify the receipt digest and reapply a tighter
whole-second grace inside the captured window. The output reports source
agreement, conflicting closes, or an explicit unresolved reason. Keep the
receipt for every attempted control. Later study activation must separately
govern capture pacing, provider availability and treatment of missingness.

The live Gateway controls returned the expected states on all five sessions
outside the E07 roster. Sandisk and KLA differed before and agreed after
their splits. Enhanced Group's pre-split tape stayed unresolved.

## Verification and handoff

Exercise controls before and after a known adjustment, matching close,
missing close, multiple tagged prints, invalid rows, pagination boundaries,
wrong venue, wrong subject, revised receipts and preserved missingness.
Exercise short sessions and a reported late auction. Run the full local and
CI gates and review the exact commit before installing on clean main. Keep
study data behind the existing preregistration and scope actual provider
tests to out-of-window controls.
