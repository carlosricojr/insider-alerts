param(
  [string]$TaskName = "Insider Alerts Research Evidence Capture",
  [Parameter(Mandatory = $true)]
  [string]$AlphaRoot,
  [string]$HistoryDatabase = "data\research\sec_history.db",
  [Parameter(Mandatory = $true)]
  [ValidatePattern('^[0-9a-f]{64}$')]
  [string]$HistorySnapshotSha256,
  [string]$OptionChainStoreDatabase = "data\research\option_chain_feed.db",
  [string]$HistoricalPacingDatabase = "data\research\historical_option_pacing.db",
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
$researchRoot = [System.IO.Path]::GetFullPath((Join-Path $repoRoot "data\research"))
New-Item -ItemType Directory -Path $researchRoot -Force | Out-Null
$researchPrefix = $researchRoot.TrimEnd('\') + '\'
$chainStorePath = if ([System.IO.Path]::IsPathRooted($OptionChainStoreDatabase)) {
  [System.IO.Path]::GetFullPath($OptionChainStoreDatabase)
} else {
  [System.IO.Path]::GetFullPath((Join-Path $repoRoot $OptionChainStoreDatabase))
}
$pacingDatabasePath = if ([System.IO.Path]::IsPathRooted($HistoricalPacingDatabase)) {
  [System.IO.Path]::GetFullPath($HistoricalPacingDatabase)
} else {
  [System.IO.Path]::GetFullPath((Join-Path $repoRoot $HistoricalPacingDatabase))
}
foreach ($databasePath in @($chainStorePath, $pacingDatabasePath)) {
  if (-not $databasePath.StartsWith(
    $researchPrefix,
    [System.StringComparison]::OrdinalIgnoreCase
  )) {
    throw "Research option databases must remain beneath $researchRoot"
  }
  New-Item -ItemType Directory -Path (Split-Path -Parent $databasePath) -Force | Out-Null
  $cursor = Split-Path -Parent $databasePath
  while ($true) {
    $cursorItem = Get-Item -LiteralPath $cursor
    if ($cursorItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
      throw "Research option database parent cannot be a reparse point: $cursor"
    }
    if ($cursor.Equals($researchRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
      break
    }
    if (-not $cursor.StartsWith(
      $researchPrefix,
      [System.StringComparison]::OrdinalIgnoreCase
    )) {
      throw "Research option database parent escaped $researchRoot"
    }
    $parent = Split-Path -Parent $cursor
    if ($parent -eq $cursor) {
      throw "Unable to prove research option database confinement for $databasePath"
    }
    $cursor = $parent
  }
  if ((Test-Path -LiteralPath $databasePath) -and
      ((Get-Item -LiteralPath $databasePath).Attributes -band
       [System.IO.FileAttributes]::ReparsePoint)) {
    throw "Research option database cannot be a reparse point: $databasePath"
  }
}
$pythonExe = Join-Path $repoRoot ".venv\Scripts\pythonw.exe"
$validationPython = Join-Path $repoRoot ".venv\Scripts\python.exe"
$alphaPython = Join-Path $alphaRootResolved ".venv\Scripts\python.exe"
$alphaScript = Join-Path $alphaRootResolved "scripts\capture_insider_option_surface.py"
$alphaHistoricalScript = Join-Path $alphaRootResolved `
  "scripts\capture_insider_historical_option_evidence.py"

foreach ($path in @(
  $pythonExe,
  $validationPython,
  $alphaPython,
  $alphaScript,
  $alphaHistoricalScript,
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
  "--alpha-historical-script `"$alphaHistoricalScript`"",
  "--option-chain-store-db `"$chainStorePath`"",
  "--historical-pacing-db `"$pacingDatabasePath`"",
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
