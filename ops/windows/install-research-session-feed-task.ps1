param(
  [string]$TaskName = "Insider Alerts Research Session Feed",
  [int]$Port = 4001,
  [int]$ClientId = 177,
  [int]$IntervalMinutes = 60,
  [switch]$Start
)

$ErrorActionPreference = "Stop"

if ($Port -lt 1 -or $Port -gt 65535) { throw "Port must be between 1 and 65535." }
if ($ClientId -lt 1) { throw "ClientId must be positive." }
if ($IntervalMinutes -lt 5) { throw "IntervalMinutes must be at least 5." }

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptDir "..\..")).Path
$pythonExe = Join-Path $repoRoot ".venv\Scripts\pythonw.exe"
if (-not (Test-Path -LiteralPath $pythonExe -PathType Leaf)) {
  throw "Missing virtualenv pythonw executable at $pythonExe"
}

$user = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$arguments = @(
  "-m insider_alerts.research.session_worker",
  "--feed-db `"$repoRoot\data\research\session_feed.db`"",
  "--port $Port",
  "--client-id $ClientId",
  "--error-log `"$repoRoot\logs\research-session-feed.err.log`""
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
