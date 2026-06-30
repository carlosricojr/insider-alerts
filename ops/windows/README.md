# Windows Autopilot Task

Install or refresh the background autopilot task:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\ops\windows\install-autopilot-task.ps1 -Start
```

The default task is a non-elevated per-user watchdog named `Insider Alerts
Autopilot Watchdog`. It starts at user logon and has a five-minute recovery
trigger. Multiple instances are ignored, so recovery triggers do not start a
second worker while the long-running loop is already alive.

Pass `-RunElevated` only from an elevated PowerShell session if the task needs
highest-privilege execution.

The worker reads `.env`, writes to `logs\autopilot.out.log` and
`logs\autopilot.err.log`, and sends NTFY notifications for approved decisions by
default.
