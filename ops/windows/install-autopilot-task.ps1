param(
  [string]$TaskName = "Insider Alerts Autopilot Watchdog",
  [int]$RecoveryIntervalMinutes = 1,
  [switch]$RunElevated,
  [switch]$Start
)

$ErrorActionPreference = "Stop"

if ($RecoveryIntervalMinutes -lt 1) {
  throw "RecoveryIntervalMinutes must be greater than or equal to 1."
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir "..\..")
$pythonExe = Join-Path $repoRoot ".venv\Scripts\pythonw.exe"

if (-not (Test-Path (Join-Path $repoRoot ".venv\Scripts\python.exe"))) {
  throw "Missing virtualenv Python at $repoRoot\.venv\Scripts\python.exe"
}

if (-not (Test-Path (Join-Path $repoRoot ".env"))) {
  throw "Missing .env at $repoRoot\.env"
}

$user = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$action = New-ScheduledTaskAction `
  -Execute $pythonExe `
  -Argument "-m insider_alerts.cli ops autopilot --loop --interval 300 --decision-engine quant --quant-agent-id quant-insider --quant-batch-size 8 --quant-thinking low --decision-limit 100 --notify --notify-approve-only --output-log logs/autopilot.out.log --error-log logs/autopilot.err.log" `
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
  -Hidden `
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
  $task = Get-ScheduledTask -TaskName $TaskName
  if ($task.State -eq "Running") {
    Stop-ScheduledTask -TaskName $TaskName
    $deadline = (Get-Date).AddSeconds(15)
    do {
      Start-Sleep -Milliseconds 250
      $task = Get-ScheduledTask -TaskName $TaskName
    } while ($task.State -eq "Running" -and (Get-Date) -lt $deadline)
    if ($task.State -eq "Running") {
      throw "Timed out stopping the existing $TaskName worker."
    }
  }
  Start-ScheduledTask -TaskName $TaskName
}

Get-ScheduledTask -TaskName $TaskName
