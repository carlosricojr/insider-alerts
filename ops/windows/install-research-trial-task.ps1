param(
  [string]$TaskName = "Insider Alerts Research Trial",
  [int]$IntervalMinutes = 1,
  [switch]$Start
)

$ErrorActionPreference = "Stop"

if ($IntervalMinutes -lt 1) { throw "IntervalMinutes must be at least 1." }

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptDir "..\..")).Path
$pythonExe = Join-Path $repoRoot ".venv\Scripts\pythonw.exe"
if (-not (Test-Path -LiteralPath $pythonExe -PathType Leaf)) {
  throw "Missing virtualenv pythonw executable at $pythonExe"
}

$user = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$arguments = @(
  "-m insider_alerts.research.trial_worker",
  "--trial-db `"$repoRoot\data\research\trial.db`"",
  "--evidence-db `"$repoRoot\data\research\evidence.db`"",
  "--bar-feed-db `"$repoRoot\data\research\bar_feed.db`"",
  "--session-feed-db `"$repoRoot\data\research\session_feed.db`"",
  "--registry-path `"$repoRoot\docs\research\registry\OPP-E07-V1.json`"",
  "--error-log `"$repoRoot\logs\research-trial-worker.err.log`""
) -join " "
$action = New-ScheduledTaskAction -Execute $pythonExe -Argument $arguments -WorkingDirectory $repoRoot
$logonTrigger = New-ScheduledTaskTrigger -AtLogOn -User $user
$intervalTrigger = New-ScheduledTaskTrigger `
  -Once `
  -At (Get-Date).AddMinutes(1) `
  -RepetitionInterval (New-TimeSpan -Minutes $IntervalMinutes) `
  -RepetitionDuration (New-TimeSpan -Days 3650)
$principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet `
  -AllowStartIfOnBatteries `
  -DontStopIfGoingOnBatteries `
  -ExecutionTimeLimit (New-TimeSpan -Minutes 5) `
  -Hidden `
  -MultipleInstances IgnoreNew `
  -RestartCount 2 `
  -RestartInterval (New-TimeSpan -Minutes 1) `
  -StartWhenAvailable

Register-ScheduledTask `
  -TaskName $TaskName `
  -Action $action `
  -Trigger @($logonTrigger, $intervalTrigger) `
  -Principal $principal `
  -Settings $settings `
  -Force | Out-Null

if ($Start) {
  $task = Get-ScheduledTask -TaskName $TaskName
  if ($task.State -eq "Running") { Stop-ScheduledTask -TaskName $TaskName }
  Start-ScheduledTask -TaskName $TaskName
}

Get-ScheduledTask -TaskName $TaskName
