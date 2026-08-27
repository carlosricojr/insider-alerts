# Blinded prospective outcome materializer

The OPP-E07-V1 trial worker materializes individual challenger outcomes only after the entry
finalizer has durably enrolled a candidate and the candidate's frozen tenth official session has
closed. It does not connect to IBKR, place orders, calculate aggregate returns, freeze a terminal
cohort, or run inference.

For both the stock and SPY, a successful zero-rejection poll receipt must cover the frozen final
session. The stock receipt's transactionally captured observation watermark must contain the
contiguous first-observed path through the registered exit, and SPY must contain the entry and exit
benchmark bars. Bars strictly after an established stock exit are not required. The materializer
uses the exact schedule watermark and horizon records bound by the pre-open entry completion. A
later schedule revision or bar backfill cannot rewrite an existing outcome.

The pure shared E07 kernel applies the registered stop-first same-day collision rule, gap-aware stop
and target prices, and session-ten close. Gross stock return is exit price divided by entry-auction
open minus one. Matched SPY return is the SPY entry-session open through the stock exit-session
close. Entry and exit timestamps are the bound official RTH boundaries, including early closes.

Each append-only outcome binds candidate and enrollment provenance, prices, reason, individual
returns, the schedule/bar/receipt watermarks, and all source record digests. Worker output and
`ops research-trial-status` expose only health and counts. Individual values remain uninspected and
no aggregate is produced until a later frozen terminal dataset is sealed under the inference
protocol.

If healthy terminal polls prove that a required pre-exit stock bar or SPY endpoint is absent, the
trial fails closed as `INVALID`; it never drops that trade. If required bars exist but the receipts
predate those observations, the materializer waits for a later poll receipt. A waiting symbol does
not block independent later-enrolled outcomes; its unresolved record remains required for the
terminal cohort. All three worker phases are sequential in one hidden `pythonw.exe` task,
preventing importer, entry-finalizer, and outcome-finalizer overlap.
