# Completed daily-bar feed

The prospective challenger never imports an IBKR client. A separate bounded worker uses client ID
176 and its source contains only API handshake, contract qualification, and historical RTH
`TRADES` daily-bar requests. It never requests accounts, positions, executions, or orders and has
no order method in its source module.

The worker reads immutable symbol/date requests and appends content-addressed observations to
`data/research/bar_feed.db`. It discards the current New York date even after an early close; a bar
therefore becomes eligible on the following calendar day, when it cannot be an incomplete session.
Exact repeated values are idempotent. A later vendor revision is appended with a new digest; the
trial always consumes the earliest observed value for each symbol/date.
Requests remain collectible until their exact final session is observed; they are never silently
abandoned. Unresolved requests older than 30 days are explicitly reported as overdue.
Each symbol is fetched at most once per New York calendar day after a successful fetch (unless a
new request needs an earlier history boundary). Requests are spaced by 11 seconds, a cycle is
bounded to 50 fairly rotated symbols, and each source call has a 40-second application deadline.
Empty IBKR responses are failures and remain retryable. Failures are isolated and appended, which
prevents one bad symbol from blocking later symbols or creating pointless pacing pressure.

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
