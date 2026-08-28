# Windows Autopilot Task

Install or refresh the background autopilot task:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\ops\windows\install-autopilot-task.ps1 -Start
```

The installer creates separate non-elevated per-user tasks named `Insider Alerts Autopilot
Worker` and `Insider Alerts Autopilot Watchdog`. The worker has no independent trigger, preventing
startup races. The bounded watchdog starts at logon and every minute, starts a stopped worker, and
restarts a worker only when its durable progress heartbeat exceeds the configured stale threshold.
That threshold is at least five minutes and may be longer when configured network or quant windows
require it. When no threshold is passed, the installer derives the minimum safe value from the
effective settings. The installer, worker, and watchdog validate it against every configured
network phase, retry stage, database window, and cleanup margin. It is also the hard wall for a
slow-drip or otherwise hung external call.

Pass `-RunElevated` only from an elevated PowerShell session if the task needs
highest-privilege execution.

Both tasks launch the virtualenv's `pythonw.exe` directly, so they never create a console window.
The worker owns its complete descendant tree in a kill-on-close Windows Job Object, so ending a
hung worker also ends quant and option-capture children before replacement. The worker reads `.env`,
writes to `logs\autopilot.out.log` and
`logs\autopilot.err.log`, and sends NTFY notifications for approved decisions by
default. The watchdog writes only bounded operational metadata to
`logs\autopilot-watchdog.log`; `ops autopilot-health-status` exposes the separate operational
heartbeat store without reading signal or outcome payloads. Passing `-Start` stops the legacy
same-named worker before registering both new definitions, then starts the watchdog. If cutover
fails, or a new worker does not produce a fresh stable runtime heartbeat within 90 seconds, the
installer stops both replacements, restores both prior definitions, and only then restarts their
prior running state. Approved notifications carry an atomic delivery intent and are retried by the
next managed cycle after an interrupted send (at-least-once delivery). Delivery acknowledgement is
compare-and-set against the exact decision version; co-filing suppression is recorded separately
and occurs only after the event has a confirmed representative delivery. The looping
worker also fingerprints the loaded Python source between
completed cycles. If source changes later, it exits cleanly within one 15-second wait slice so no
cycle is interrupted. The watchdog's next one-minute run starts the fresh worker. A stale restart
is conservative after system suspend/resume and fully stops the old scheduled task before starting
another; it never starts a replacement after a failed stop. The optional manual hidden launcher
also invokes `pythonw.exe` directly and never routes through `cmd.exe`.

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

## Prospective trial runtime

Install the order-incapable prospective trial worker only after both point-in-time feeds are
deployed:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
  .\ops\windows\install-research-trial-task.ps1 -Start
```

The task runs once per minute as direct hidden `pythonw.exe`, imports newly captured immutable
candidates, seals or lapses eligible entry dates, and then materializes mature individual outcomes
in the same sequential cycle. While the registry remains `draft`, it only records an idle heartbeat
and cannot enroll candidates. Inspect blinded counts and integrity without connecting to IBKR via
`ops research-trial-status`; that command never exposes return values.
