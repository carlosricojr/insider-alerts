# Blinded prospective outcome materializer

The OPP-E07-V1 trial worker materializes individual challenger outcomes only after the entry
finalizer has durably enrolled a candidate and the candidate's frozen tenth official session has
closed. It does not connect to IBKR, place orders, calculate aggregate returns, freeze a terminal
cohort, or run inference.

The challenger and diagnostic-control paths call the same order-incapable
`research.outcome_proof` kernel. Each caller supplies its own immutable schedule binding and storage
authority; the kernel reconstructs the bound ten-session schedule, selects healthy stock/SPY poll
receipts, evaluates the pure stop-first E07 rule, and returns the economic result with exact feed
watermarks and record digests.

For both the stock and SPY, a successful zero-rejection poll receipt must cover the frozen final
session. The stock receipt's transactionally captured observation watermark must contain the
contiguous first-observed path through the registered exit, and SPY must contain the entry and exit
benchmark bars. Bars strictly after an established stock exit are not required. The materializer
uses the exact schedule watermark and horizon records bound by the pre-open entry completion. A
later schedule revision or bar backfill cannot rewrite an existing outcome.
Diagnostic bindings must equal the ten horizon digests exactly. Challenger entry completions also
bind earlier schedule records used by the pre-open eligibility decision, so that broader set must
contain the horizon and may contain only records known at the same frozen watermark and decision
clock.

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
terminal cohort. All diagnostic and confirmatory worker phases are sequential in one hidden
`pythonw.exe` task,
preventing importer, entry-finalizer, and outcome-finalizer overlap.

The separate diagnostic store uses the canary ledger only to decide whether a control position was
actually selected. Closed controls are recomputed from the research feeds after the same
tenth-session proof; canary prices and returns are agreement evidence only. Suppressed or rejected
candidates receive explicit `not_traded` receipts. Proven terminal missingness receives a typed
`unavailable` receipt, and processing continues with later controls. Diagnostic outcomes and their
disposition receipts are committed atomically and never read by the challenger finalizer.
