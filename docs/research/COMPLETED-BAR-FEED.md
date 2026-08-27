# Completed daily-bar feed

The prospective challenger never imports an IBKR client. A separate bounded worker uses client ID
176 and its source contains only API handshake, contract qualification, and historical RTH
`TRADES` daily-bar requests. It never requests accounts, positions, executions, or orders and has
no order method in its source module.

The worker reads immutable symbol/date requests and appends content-addressed observations to
`data/research/bar_feed.db`. It rejects the current New York date by default. Once the append-only
exchange schedule observed by that instant proves the official RTH close has passed, the integrated
worker may admit that date after a one-minute finalization buffer; before then, a provisional daily
bar remains ineligible. This
supports same-date trial finalization after actual closes, including early-close sessions, without
accepting incomplete bars.
Exact repeated values are idempotent. A later vendor revision is appended with a new digest; the
trial always consumes the earliest observed value for each symbol/date.
The completion proof is the registered SPY US-equity RTH calendar and is valid only for the trial's
US-listed equity and SPY requests; a different trading calendar requires a new feed contract.
Requests remain collectible until their exact final session is observed; they are never silently
abandoned. Unresolved requests older than 30 days are explicitly reported as overdue.
Each symbol is fetched at most once per New York calendar day after a successful fetch (unless a
new request needs an earlier history boundary). Requests are spaced by 11 seconds, a cycle is
bounded to 50 fairly rotated symbols, and each source call has a 40-second application deadline.
Empty IBKR responses are failures and remain retryable. Failures are isolated and appended, which
prevents one bad symbol from blocking later symbols or creating pointless pacing pressure.

Every successful symbol response also appends an immutable poll receipt in the same transaction
that advances mutable poll state. The receipt binds the poll instant, requested range,
officially-completed-through date, returned/in-range bar counts, and source/validation rejection
counts. This makes a healthy short history distinguishable from a failed or unattempted fetch. The
store also exposes a maximum observation-sequence watermark and first-observed bar records bounded
by that watermark, so a later backfill or revision cannot change an already sealed trial input.

The feed is intentionally separate from the live canary process. A broker outage or locked feed
database can delay research resolution, but cannot delay reconciliation or protective-order
management in the live account. No requests exist before a prospective candidate asks for them, so
the installed worker remains an offline, heartbeat-only process until activation.

The source module is order-incapable and intentionally avoids account, position, execution, and
order APIs. This is a code-level least-privilege boundary, not an IBKR permission boundary: the
Gateway does not provide per-client-ID trading permissions, and this task runs under the same
Windows identity as the live canary. A separate read-only IBKR login and OS identity would be
required for a broker-enforced boundary.

Install or refresh the invisible one-cycle scheduled task:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass `
  -File .\ops\windows\install-research-bar-feed-task.ps1 -Start
```

Read health without opening a broker connection:

```powershell
.\.venv\Scripts\python.exe -m insider_alerts.cli ops research-bar-feed-status
```
