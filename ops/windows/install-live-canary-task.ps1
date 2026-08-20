param(
  [string]$TaskName = "Insider Alerts Live Canary Watchdog",
  [string]$WorkerTaskName = "Insider Alerts Live Canary Worker",
  [int]$RecoveryIntervalMinutes = 1,
  [int]$StaleHeartbeatSeconds = 120,
  [switch]$Start
)

$ErrorActionPreference = "Stop"

if ($RecoveryIntervalMinutes -lt 1) {
  throw "RecoveryIntervalMinutes must be greater than or equal to 1."
}
if ($StaleHeartbeatSeconds -lt 30) {
  throw "StaleHeartbeatSeconds must be greater than or equal to 30."
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir "..\..")
$pythonExe = Join-Path $repoRoot ".venv\Scripts\pythonw.exe"

if (-not (Test-Path $pythonExe)) {
  throw "Missing windowless virtualenv Python at $pythonExe"
}

$user = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$workerAction = New-ScheduledTaskAction `
  -Execute $pythonExe `
  -Argument "-m insider_alerts.cli ops live-canary --loop --interval 15 --live --notify --invalid-commission-handling reject --arm-phrase I_ACCEPT_LIVE_CANARY_RISK --output-log logs/live-canary.out.log --error-log logs/live-canary.err.log" `
  -WorkingDirectory $repoRoot
$watchdogAction = New-ScheduledTaskAction `
  -Execute $pythonExe `
  -Argument "-m insider_alerts.cli ops live-canary-watchdog --worker-task-name `"$WorkerTaskName`" --stale-seconds $StaleHeartbeatSeconds --output-log logs/live-canary-watchdog.log" `
  -WorkingDirectory $repoRoot

$workerLogonTrigger = New-ScheduledTaskTrigger -AtLogOn -User $user
$watchdogLogonTrigger = New-ScheduledTaskTrigger -AtLogOn -User $user
$watchdogTrigger = New-ScheduledTaskTrigger `
  -Once `
  -At (Get-Date).AddMinutes(1) `
  -RepetitionInterval (New-TimeSpan -Minutes $RecoveryIntervalMinutes) `
  -RepetitionDuration (New-TimeSpan -Days 3650)

$principal = New-ScheduledTaskPrincipal `
  -UserId $user `
  -LogonType Interactive `
  -RunLevel Limited

$workerSettings = New-ScheduledTaskSettingsSet `
  -AllowStartIfOnBatteries `
  -DontStopIfGoingOnBatteries `
  -ExecutionTimeLimit (New-TimeSpan -Seconds 0) `
  -Hidden `
  -MultipleInstances IgnoreNew `
  -RestartCount 999 `
  -RestartInterval (New-TimeSpan -Minutes 1) `
  -StartWhenAvailable
$watchdogSettings = New-ScheduledTaskSettingsSet `
  -AllowStartIfOnBatteries `
  -DontStopIfGoingOnBatteries `
  -ExecutionTimeLimit (New-TimeSpan -Minutes 1) `
  -Hidden `
  -MultipleInstances IgnoreNew `
  -RestartCount 3 `
  -RestartInterval (New-TimeSpan -Minutes 1) `
  -StartWhenAvailable

Register-ScheduledTask `
  -TaskName $WorkerTaskName `
  -Action $workerAction `
  -Trigger $workerLogonTrigger `
  -Principal $principal `
  -Settings $workerSettings `
  -Force | Out-Null

Register-ScheduledTask `
  -TaskName $TaskName `
  -Action $watchdogAction `
  -Trigger @($watchdogLogonTrigger, $watchdogTrigger) `
  -Principal $principal `
  -Settings $watchdogSettings `
  -Force | Out-Null

if ($Start) {
  foreach ($name in @($TaskName, $WorkerTaskName)) {
    $task = Get-ScheduledTask -TaskName $name
    if ($task.State -eq "Running") {
      Stop-ScheduledTask -TaskName $name
      $deadline = (Get-Date).AddSeconds(15)
      do {
        Start-Sleep -Milliseconds 250
        $task = Get-ScheduledTask -TaskName $name
      } while ($task.State -eq "Running" -and (Get-Date) -lt $deadline)
      if ($task.State -eq "Running") {
        throw "Timed out stopping the existing $name task."
      }
    }
  }
  Start-ScheduledTask -TaskName $TaskName
}

Get-ScheduledTask -TaskName @($TaskName, $WorkerTaskName)
