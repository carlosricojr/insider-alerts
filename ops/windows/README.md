# Windows Autopilot Task

Install or refresh the background autopilot task:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\ops\windows\install-autopilot-task.ps1 -Start
```

The default task is a non-elevated per-user watchdog named `Insider Alerts
Autopilot Watchdog`. It starts at user logon and has a one-minute recovery
trigger. Multiple instances are ignored, so recovery triggers do not start a
second worker while the long-running loop is already alive.

Pass `-RunElevated` only from an elevated PowerShell session if the task needs
highest-privilege execution.

The task launches the virtualenv's `pythonw.exe` directly, so it never creates a
console window and Task Scheduler retains ownership of the complete worker process
chain. The worker reads `.env`, writes to `logs\autopilot.out.log` and
`logs\autopilot.err.log`, and sends NTFY notifications for approved decisions by
default. Passing `-Start` performs a controlled restart so deployed source changes
are loaded immediately. The optional manual hidden launcher also invokes
`pythonw.exe` directly and never routes through `cmd.exe`.

Install the separate IBKR canary watchdog with:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\ops\windows\install-live-canary-task.ps1 -Start
```

It registers separate hidden `pythonw.exe` worker and watchdog tasks. The watchdog runs every
minute, ensures a stopped worker is started, and force-restarts a worker whose durable heartbeat
is stale for more than two minutes. Passing `-Start` stops any existing instances before starting
both registered definitions, ensuring deployed source changes are loaded. The worker also
fingerprints its Python source and exits for an invisible watchdog restart if the source later
changes. The live policy and broker gates are documented in `docs/runbook/LIVE_CANARY.md`.

Install the separate prospective evidence worker after deploying its pinned alpha-core runtime:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File .\ops\windows\install-research-capture-task.ps1 `
  -AlphaRoot C:\path\to\clean-alpha-core-runtime `
  -HistorySnapshotSha256 <pinned-lowercase-sha256> `
  -Start
```

The task runs a hidden bounded capture cycle through `pythonw.exe` once per interval, has no order
code, cannot block the live canary, ignores overlaps, hides all windows, and kills a timed-out
alpha-core capture process tree. Idle cycles do not hash the multi-gigabyte history store; every
exact-owner classification fully authenticates the sealed snapshot immediately before consuming
it. A 15-minute task limit recovers a hung cycle. Full pin validation also happens before task
replacement. The default history database is
`data/research/sec_history.db`; override it with `-HistoryDatabase` when needed. Health is
available from `ops research-capture-status`.

## Prospective completed-bar feed

Install or refresh the task with:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
  .\ops\windows\install-research-bar-feed-task.ps1 -Start
```

`install-research-bar-feed-task.ps1` registers a separate direct-`pythonw.exe` task that collects
only requested, completed IBKR daily bars through a narrow client ID. It does not run in the live
canary process and does not expose account or order operations. Inspect it with
`ops research-bar-feed-status`. See
[the completed-bar feed contract](../../docs/research/COMPLETED-BAR-FEED.md) for request, pacing,
and integrity semantics.

`install-research-session-feed-task.ps1` similarly runs a bounded hourly direct-`pythonw.exe`
worker on client ID 177. It appends point-in-time SPY RTH schedule observations, including early
closes, and exposes health through `ops research-session-feed-status`.
