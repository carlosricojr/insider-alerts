# Point-in-time exchange-session feed

The challenger needs the exchange's actual RTH session dates, opens, and closes for deterministic
entry selection, entry-date completion, early closes, and matched SPY timing. A separate bounded
worker requests the IBKR historical schedule for `SPY` through client ID 177 once per hour. Its
source contains no account, position, execution, or order request.

Every distinct schedule value is content-addressed and appended to
`data/research/session_feed.db`; exact repeats are idempotent and schedule corrections are retained
as revisions. The trial can reconstruct the latest schedule known at any UTC decision boundary,
while terminal outcome records bind the selected official session boundaries. This prevents a
later holiday or early-close correction from silently rewriting planned entry provenance.

Install the invisible direct-`pythonw.exe` task:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass `
  -File .\ops\windows\install-research-session-feed-task.ps1 -Start
```

Read health without connecting to IBKR:

```powershell
.\.venv\Scripts\python.exe -m insider_alerts.cli ops research-session-feed-status
```
