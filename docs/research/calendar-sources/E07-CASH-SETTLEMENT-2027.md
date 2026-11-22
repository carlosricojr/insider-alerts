# E07 research settlement calendar through 2027

Select `schedule="through_2027"` in either settlement-aware replay entry
point. The extension supports dates from October 1, 2026 through
December 31, 2027, including the last modeled cash-release date. The
default Q4 2026 schedule retains its original contract and digests.

Two official annual notices were published November 9, 2026. The retained
sources and their observation clocks are bound in the [provenance
record](e07-cash-settlement-2027.provenance.json). The extended binding
uses these source digests and the two inherited Q4 2026 source digests,
with the qualification status of each period.
[DTC Settlement 25243-26](https://files.dtcc.com/download/assets/25243-26/1bc95d78dc7f11f1bddf0ae2e4272629)
[NSCC Equity Clearing a9854](https://files.dtcc.com/download/assets/a9854/78ba7964dc7e11f1aaaf3a4dd749b5ad)

The notices identify seven full closures and two dates when DTC provides
no settlement services. NYSE equity sessions on October 11 and November
11 remain trading sessions. The replay skips those dates for T+1 while
validating the full exchange session sequence. The next-exchange-open
cash-release stress starts after the computed settlement date.

| Limited service date | Earliest potentially affected trade date |
| --- | --- |
| March 26, 2027 | March 25, 2027 |
| June 18, 2027 | June 17, 2027 |
| December 24, 2027 | December 23, 2027 |

The annual notices leave regular-way equity clearing unresolved on the
three limited dates. If the earliest possible settlement falls on one
of them, the entire requested replay binding is unavailable, including
when the affected trade date is an idle or drain session. Incorporate
date-specific DTC and NSCC operational notices in a separately reviewed
schedule version before binding such a window. Preserve this version and
its source evidence.

The projection assumes ordinary clearing service on unlisted weekdays.
It checks that each projected regular settlement day is an open session
in the selected exchange calendar. Unexpected exchange closures require
additional clearing evidence. The projection has no authority to infer
actual IBKR cash-credit time or approve a prospective trial.
