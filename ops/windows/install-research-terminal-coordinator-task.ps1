param(
  [string]$TaskName = "Insider Alerts Research Terminal Coordinator",
  [switch]$Start
)

$ErrorActionPreference = "Stop"

$localTimeZone = [System.TimeZoneInfo]::Local
if ($localTimeZone.Id -ne "Eastern Standard Time") {
  throw "Terminal coordinator requires Windows time zone 'Eastern Standard Time'; found '$($localTimeZone.Id)'."
}
$dailyStart = (Get-Date).Date.AddHours(20).AddMinutes(30)

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptDir "..\..")).Path
$pythonExe = Join-Path $repoRoot ".venv\Scripts\pythonw.exe"
if (-not (Test-Path -LiteralPath $pythonExe -PathType Leaf)) {
  throw "Missing virtualenv pythonw executable at $pythonExe"
}

$user = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$arguments = @(
  "-m insider_alerts.research.terminal_coordinator",
  "--trial-db `"$repoRoot\data\research\trial.db`"",
  "--diagnostics-db `"$repoRoot\data\research\diagnostics.db`"",
  "--canary-ledger-db `"$repoRoot\data\live_canary.db`"",
  "--source-db `"$repoRoot\data\insider_alerts.db`"",
  "--registry-path `"$repoRoot\docs\research\registry\OPP-E07-V1.json`"",
  "--seal-db `"$repoRoot\data\research\trial_seals.db`"",
  "--artifact-root `"$repoRoot\data\research\artifacts`"",
  "--activation-db `"$repoRoot\data\research\activation.db`"",
  "--output-log `"$repoRoot\logs\research-terminal-coordinator.log`"",
  "--error-log `"$repoRoot\logs\research-terminal-coordinator.err.log`""
) -join " "
$action = New-ScheduledTaskAction -Execute $pythonExe -Argument $arguments -WorkingDirectory $repoRoot
$dailyTrigger = New-ScheduledTaskTrigger -Daily -At $dailyStart
$s4uPrincipal = New-ScheduledTaskPrincipal -UserId $user -LogonType S4U -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet `
  -AllowStartIfOnBatteries `
  -DontStopIfGoingOnBatteries `
  -ExecutionTimeLimit (New-TimeSpan -Minutes 60) `
  -Hidden `
  -MultipleInstances IgnoreNew `
  -StartWhenAvailable

$registrationMode = "S4U"
try {
  Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $dailyTrigger `
    -Principal $s4uPrincipal `
    -Settings $settings `
    -ErrorAction Stop `
    -Force | Out-Null
} catch {
  if ($_.FullyQualifiedErrorId -ne "HRESULT 0x80070005,Register-ScheduledTask") {
    throw
  }

  # Some non-elevated Windows installations deny S4U task registration. Preserve the daily
  # trigger and add a logon catch-up trigger when falling back to an interactive principal.
  $registrationMode = "InteractiveFallback"
  $interactivePrincipal = New-ScheduledTaskPrincipal `
    -UserId $user `
    -LogonType Interactive `
    -RunLevel Limited
  $logonTrigger = New-ScheduledTaskTrigger -AtLogOn -User $user
  Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger @($dailyTrigger, $logonTrigger) `
    -Principal $interactivePrincipal `
    -Settings $settings `
    -ErrorAction Stop `
    -Force | Out-Null
  Write-Warning (
    "S4U registration was denied (HRESULT 0x80070005); registered '$TaskName' " +
    "for the current interactive session with daily and logon triggers."
  )
}

if ($Start) {
  $task = Get-ScheduledTask -TaskName $TaskName
  if ($task.State -eq "Running") { Stop-ScheduledTask -TaskName $TaskName }
  Start-ScheduledTask -TaskName $TaskName
}

$registeredTask = Get-ScheduledTask -TaskName $TaskName
$registeredTask | Add-Member -NotePropertyName RegistrationMode -NotePropertyValue $registrationMode
$registeredTask
