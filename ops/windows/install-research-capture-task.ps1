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
. (Join-Path $scriptDir "research-capture-task-action.ps1")
. (Join-Path $scriptDir "research-path-validation.ps1")
$repoRoot = (Resolve-Path (Join-Path $scriptDir "..\..")).Path
$alphaRootResolved = (Resolve-Path $AlphaRoot).Path
$historyDatabasePath = if ([System.IO.Path]::IsPathRooted($HistoryDatabase)) {
  $HistoryDatabase
} else {
  Join-Path $repoRoot $HistoryDatabase
}
$historyDatabaseResolved = (Resolve-Path $historyDatabasePath).Path
$dataRoot = [System.IO.Path]::GetFullPath((Join-Path $repoRoot "data"))
$researchRoot = [System.IO.Path]::GetFullPath((Join-Path $repoRoot "data\research"))
foreach ($ancestor in @($repoRoot, $dataRoot)) {
  if (-not (Test-Path -LiteralPath $ancestor -PathType Container)) {
    throw "Research artifact ancestor is unavailable: $ancestor"
  }
  $ancestorItem = Get-Item -LiteralPath $ancestor
  if ($ancestorItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
    throw "Research artifact ancestor cannot be a reparse point: $ancestor"
  }
}
New-Item -ItemType Directory -Path $researchRoot -Force | Out-Null
$researchRootItem = Get-Item -LiteralPath $researchRoot
if (-not $researchRootItem.PSIsContainer -or
    ($researchRootItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint)) {
  throw "Research root must be a regular directory: $researchRoot"
}
$researchPrefix = $researchRoot.TrimEnd('\') + '\'
$artifactRoot = [System.IO.Path]::GetFullPath((Join-Path $researchRoot "artifacts"))
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
  Initialize-ResearchDatabaseParent `
    -DatabasePath $databasePath `
    -ResearchRoot $researchRoot
}
New-Item -ItemType Directory -Path $artifactRoot -Force | Out-Null
$artifactRootItem = Get-Item -LiteralPath $artifactRoot
if (-not $artifactRootItem.PSIsContainer -or
    ($artifactRootItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -or
    -not $artifactRootItem.FullName.StartsWith(
      $researchPrefix,
      [System.StringComparison]::OrdinalIgnoreCase
    )) {
  throw "Research artifact root must be a regular directory beneath $researchRoot"
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
$actionSpec = New-ResearchCaptureTaskActionSpec `
  -PythonExe $pythonExe `
  -RepoRoot $repoRoot `
  -ArtifactRoot $artifactRoot `
  -AlphaPython $alphaPython `
  -AlphaScript $alphaScript `
  -AlphaHistoricalScript $alphaHistoricalScript `
  -ChainStorePath $chainStorePath `
  -PacingDatabasePath $pacingDatabasePath `
  -HistoryDatabase $historyDatabaseResolved `
  -HistorySnapshotSha256 $HistorySnapshotSha256
$action = New-ScheduledTaskAction `
  -Execute $actionSpec.Execute `
  -Argument $actionSpec.Argument `
  -WorkingDirectory $actionSpec.WorkingDirectory
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
