param(
  [string]$TaskName = "Insider Alerts Autopilot Watchdog",
  [int]$RecoveryIntervalMinutes = 5,
  [switch]$RunElevated,
  [switch]$Start
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir "..\..")
$launcher = Join-Path $scriptDir "run-insider-autopilot-hidden.ps1"

if (-not (Test-Path (Join-Path $repoRoot ".venv\Scripts\python.exe"))) {
  throw "Missing virtualenv Python at $repoRoot\.venv\Scripts\python.exe"
}

if (-not (Test-Path (Join-Path $repoRoot ".env"))) {
  throw "Missing .env at $repoRoot\.env"
}

$user = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$action = New-ScheduledTaskAction `
  -Execute "powershell.exe" `
  -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$launcher`"" `
  -WorkingDirectory $repoRoot

$logonTrigger = New-ScheduledTaskTrigger -AtLogOn -User $user
$watchdogTrigger = New-ScheduledTaskTrigger `
  -Once `
  -At (Get-Date).AddMinutes(1) `
  -RepetitionInterval (New-TimeSpan -Minutes $RecoveryIntervalMinutes) `
  -RepetitionDuration (New-TimeSpan -Days 3650)

$runLevel = if ($RunElevated) { "Highest" } else { "Limited" }
$principal = New-ScheduledTaskPrincipal `
  -UserId $user `
  -LogonType Interactive `
  -RunLevel $runLevel

$settings = New-ScheduledTaskSettingsSet `
  -AllowStartIfOnBatteries `
  -DontStopIfGoingOnBatteries `
  -ExecutionTimeLimit (New-TimeSpan -Seconds 0) `
  -MultipleInstances IgnoreNew `
  -RestartCount 999 `
  -RestartInterval (New-TimeSpan -Minutes 1) `
  -StartWhenAvailable

Register-ScheduledTask `
  -TaskName $TaskName `
  -Action $action `
  -Trigger @($logonTrigger, $watchdogTrigger) `
  -Principal $principal `
  -Settings $settings `
  -Force | Out-Null

if ($Start) {
  Start-ScheduledTask -TaskName $TaskName
}

Get-ScheduledTask -TaskName $TaskName
