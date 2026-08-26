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
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\ops\windows\install-research-capture-task.ps1 -Start
```

The task runs one bounded capture per minute through `pythonw.exe`. It has no order code, cannot
block the live canary, ignores overlapping launches, hides all windows, and kills a timed-out
alpha-core capture process tree. Health is available from `ops research-capture-status`.
