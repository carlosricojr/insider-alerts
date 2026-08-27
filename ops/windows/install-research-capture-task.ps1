param(
  [string]$TaskName = "Insider Alerts Research Evidence Capture",
  [Parameter(Mandatory = $true)]
  [string]$AlphaRoot,
  [string]$HistoryDatabase = "data\research\sec_history.db",
  [Parameter(Mandatory = $true)]
  [ValidatePattern('^[0-9a-f]{64}$')]
  [string]$HistorySnapshotSha256,
  [int]$IntervalMinutes = 1,
  [switch]$Start
)

$ErrorActionPreference = "Stop"

if ($IntervalMinutes -lt 1) {
  throw "IntervalMinutes must be greater than or equal to 1."
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptDir "..\..")).Path
$alphaRootResolved = (Resolve-Path $AlphaRoot).Path
$historyDatabasePath = if ([System.IO.Path]::IsPathRooted($HistoryDatabase)) {
  $HistoryDatabase
} else {
  Join-Path $repoRoot $HistoryDatabase
}
$historyDatabaseResolved = (Resolve-Path $historyDatabasePath).Path
$pythonExe = Join-Path $repoRoot ".venv\Scripts\pythonw.exe"
$validationPython = Join-Path $repoRoot ".venv\Scripts\python.exe"
$alphaPython = Join-Path $alphaRootResolved ".venv\Scripts\python.exe"
$alphaScript = Join-Path $alphaRootResolved "scripts\capture_insider_option_surface.py"

foreach ($path in @(
  $pythonExe,
  $validationPython,
  $alphaPython,
  $alphaScript,
  $historyDatabaseResolved
)) {
  if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
    throw "Missing required capture executable or script at $path"
  }
}

$validationOutput = & $validationPython `
  -m insider_alerts.research.history_worker `
  --database $historyDatabaseResolved `
  --validate-snapshot $HistorySnapshotSha256 2>&1
if ($LASTEXITCODE -ne 0) {
  throw "History snapshot preflight failed: $validationOutput"
}

$user = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$arguments = @(
  "-m insider_alerts.research.worker",
  "--alpha-python `"$alphaPython`"",
  "--alpha-script `"$alphaScript`"",
  "--history-db `"$historyDatabaseResolved`"",
  "--history-snapshot-sha256 $HistorySnapshotSha256",
  "--error-log `"$repoRoot\logs\research-capture.err.log`""
) -join " "
$action = New-ScheduledTaskAction `
  -Execute $pythonExe `
  -Argument $arguments `
  -WorkingDirectory $repoRoot
$logonTrigger = New-ScheduledTaskTrigger -AtLogOn -User $user
$intervalTrigger = New-ScheduledTaskTrigger `
  -Once `
  -At (Get-Date).AddMinutes(1) `
  -RepetitionInterval (New-TimeSpan -Minutes $IntervalMinutes) `
  -RepetitionDuration (New-TimeSpan -Days 3650)
$principal = New-ScheduledTaskPrincipal `
  -UserId $user `
  -LogonType Interactive `
  -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet `
  -AllowStartIfOnBatteries `
  -DontStopIfGoingOnBatteries `
  -ExecutionTimeLimit (New-TimeSpan -Minutes 15) `
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
  if ($task.State -eq "Running") {
    Stop-ScheduledTask -TaskName $TaskName
  }
  Start-ScheduledTask -TaskName $TaskName
}

Get-ScheduledTask -TaskName $TaskName
