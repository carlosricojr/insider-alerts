# Uncapped operational observer

This separate, order-incapable observer preserves all **canary-ledger candidates after its
sealed activation**, including recorded rejections, without a slot or symbol-overlap admission
cap. It is not the complete SEC universe: candidates missing upstream or during canary outages
remain outside its authority. It neither changes the two $200 live slots nor the 20-slot ghost
book, and never reads trial outcomes or writes the existing operational/research databases.

Only a fixed non-outcome candidate projection is read using SQLite `mode=ro`. Signal timestamps
must be on/after activation; malformed projections get typed receipts. The source `created_at`
is the canary's cycle-start clock, not an insertion timestamp; it may precede signal arrival.
First observation time is distinct from both source clocks. Revisions reference prior records;
request planning retains the first candidate identity rather than silently adopting revisions.

`data/observer/evidence.db` contains RFC8785/SHA256 chained, append-only records. Source reads
close before output writes/network I/O. `worker-lock.db` is only an OS-released SQLite writer
lock, held across the invocation; durable attempts in the evidence journal preserve pacing after
crashes. A failed activation is not reset automatically. Preserve evidence through any rollback.

The hidden one-shot worker runs every five minutes while the Windows user is logged in. Candidate
capture runs at each invocation; market requests run only 18:00-08:00 America/New_York, serially,
one symbol per invocation, at least five minutes apart, with least-recent-attempt fairness. A
successful symbol poll is not repeated on the same local date. This bounds additional load to
normally 168 attempts per complete off-hours window (180 across the fall DST clock change);
it is not a guarantee that all symbols will
be serviced daily. Existing live/research clients have their own traffic. IBKR still applies
[soft throttling to historical requests](https://interactivebrokers.github.io/tws-api/historical_limitations.html).

The existing narrow IBKR historical adapter uses localhost:4001, client 178; only contract
qualification and RTH daily TRADES history are requested. Observer-side timeouts cancel pending
connection/qualification/history at 08:00. Attempt-start and actual response-observation clocks
are retained; the adapter does not expose the precise historical-request dispatch clock, which
is explicitly missing rather than inferred from connection start. No account sync, preview, order,
real-time subscription or new paid service is added. Its historical request may return extra
dates; only requested capture-window dates strictly before the request's New York date are
retained. A 45-calendar-day window from the signal bounds collection; it is **not** a strategy
holding period, trading-session calendar, or new test endpoint. Delayed retrieval is not evidence
of what was known at entry. First-seen prices and later revisions remain distinct. Successful
polls retain content hashes of accepted OHLCV; unchanged bars are not duplicated.

There is deliberately no exchange-session proof, verified contract identifier, auction-fill
proof, cash history or rejected-trade commission preview. Missing metadata is explicit. A
successful nonempty response means data was received, not that the required economic path is
complete. The broken research calendar feed and INVALID trial remain untouched. No PnL, capacity
recommendation or promotion is produced; any such study needs separately approved preregistration
and a fresh non-overlapping sample under the frozen research rules.

## Activation and operation

Deploy only a reviewed merged commit to the live task's clean, synced `main` checkout. Inspect
that task's executable, arguments and working directory and confirm client 178 is unused.
Choose an explicit timestamp at least two hours in the future, then run:

```powershell
uv run python -m insider_alerts.execution.observer_worker --activate-at <aware-UTC-timestamp>
powershell -NoProfile -ExecutionPolicy Bypass -File ops/windows/install-observer-task.ps1 -Start
uv run python -m insider_alerts.execution.observer_worker --status
```

Activation creates an exclusive new file; existing stores are never reset. The installer refuses
to overwrite an existing task. It registers direct `pythonw.exe`, hidden, limited interactive
principal, logon plus five-minute triggers, IgnoreNew, and a three-minute execution limit. Worker
git probes use Windows no-window flags and process-tree ownership. It does not create a daemon.

Status exposes only activation, last-cycle revision/time/result, freshness (15 minutes), candidate
and bar-version counts, typed missingness/failure counts, and explicit non-validation flags. A
fresh heartbeat alone is not healthy acquisition: inspect `last_cycle.result` and failure counts.
Errors appear in `logs/observer.err.log`; no console is required. `waiting_activation`, `paced`,
`off_hours_only` and `idle` are expected states. `source_unavailable`, `market_data_unavailable`
and `partial` require checking source/broker connectivity or missing evidence, not inventing data.
After activation, check fresh candidate custody and successful price collection on the next
eligible off-hours cycle. If the PC is off, delayed observations remain labelled by actual time.

Rollback: disable only `Insider Alerts Operational Observer`; do not delete its database or
modify live policy. Retain failed/partial records. This release cannot recover historical missed
profit or make the existing ghost selection history a valid portfolio.
