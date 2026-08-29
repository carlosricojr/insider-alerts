param(
  [string]$TaskName = "Insider Alerts Notification Coverage",
  [int]$IntervalMinutes = 1,
  [string]$SourceDatabase = "",
  [string]$JournalDatabase = "",
  [string]$CoverageDatabase = "",
  [switch]$Start
)

$ErrorActionPreference = "Stop"

if ($IntervalMinutes -lt 1) {
  throw "IntervalMinutes must be greater than or equal to 1."
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$scriptRepoRoot = (Resolve-Path (Join-Path $scriptDir "..\..")).Path
$deploymentTaskName = "Insider Alerts Live Canary Worker"
$deploymentTask = Get-ScheduledTask -TaskName $deploymentTaskName -ErrorAction Stop
if ($deploymentTask.Actions.Count -ne 1) {
  throw "The live-canary deployment task must have exactly one action."
}
$deploymentAction = $deploymentTask.Actions[0]
$repoRoot = [System.IO.Path]::GetFullPath($deploymentAction.WorkingDirectory).TrimEnd("\")
if ($repoRoot -ne $scriptRepoRoot) {
  throw "Run this installer only from the live-canary deployment checkout at $repoRoot"
}
$pythonExe = Join-Path $repoRoot ".venv\Scripts\pythonw.exe"
$pythonConsole = Join-Path $repoRoot ".venv\Scripts\python.exe"
$expectedCanaryPrefix = "-m insider_alerts.cli ops live-canary"
if (
  $deploymentAction.Execute -ne $pythonExe -or
  -not $deploymentAction.Arguments.StartsWith("$expectedCanaryPrefix ")
) {
  throw "The live-canary task does not identify the expected deployment runtime."
}

$branch = (& git -C $repoRoot branch --show-current).Trim()
$head = (& git -C $repoRoot rev-parse HEAD).Trim()
$originMain = (& git -C $repoRoot rev-parse origin/main).Trim()
$dirty = @(& git -C $repoRoot status --porcelain=v1 --untracked-files=all)
if ($LASTEXITCODE -ne 0 -or $branch -ne "main" -or $head -ne $originMain -or $dirty.Count -ne 0) {
  throw "Deployment checkout must be clean main with HEAD exactly equal to origin/main."
}

function Resolve-ConfiguredPath([string]$Value, [string]$DefaultRelativePath) {
  $candidate = if ([string]::IsNullOrWhiteSpace($Value)) {
    Join-Path $repoRoot $DefaultRelativePath
  } else {
    $Value
  }
  return (Resolve-Path -LiteralPath $candidate -ErrorAction Stop).Path
}

$sourceDb = Resolve-ConfiguredPath $SourceDatabase "data\insider_alerts.db"
$journalDb = Resolve-ConfiguredPath $JournalDatabase "data\research\notification_transport.db"
$coverageDb = Resolve-ConfiguredPath $CoverageDatabase "data\research\notification_coverage.db"
$journalPolicy = Join-Path $repoRoot "docs\research\contracts\notification-transport-v1.json"
$coveragePolicy = Join-Path $repoRoot "docs\research\contracts\notification-coverage-v1.json"

foreach ($path in @($pythonExe, $pythonConsole, $sourceDb, $journalDb, $coverageDb, $journalPolicy, $coveragePolicy)) {
  if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
    throw "Missing required notification-coverage file at $path"
  }
}

$arguments = @(
  "-m insider_alerts.research.notification_coverage_worker",
  "--source-db `"$sourceDb`"",
  "--journal-db `"$journalDb`"",
  "--coverage-db `"$coverageDb`"",
  "--journal-policy `"$journalPolicy`"",
  "--coverage-policy `"$coveragePolicy`"",
  "--output-log `"$repoRoot\logs\notification-coverage.log`"",
  "--error-log `"$repoRoot\logs\notification-coverage.err.log`""
) -join " "

$statusArguments = @(
  "-m", "insider_alerts.research.notification_coverage_worker",
  "--source-db", $sourceDb,
  "--journal-db", $journalDb,
  "--coverage-db", $coverageDb,
  "--journal-policy", $journalPolicy,
  "--coverage-policy", $coveragePolicy,
  "--status"
)
& $pythonConsole @statusArguments | Out-Host
if ($LASTEXITCODE -ne 0) {
  throw "Refusing task registration because the sealed coverage paths or health are invalid."
}

$existingTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($null -ne $existingTask) {
  $existingAction = $existingTask.Actions[0]
  if (
    $existingAction.Execute -ne $pythonExe -or
    $existingAction.WorkingDirectory -ne $repoRoot -or
    $existingAction.Arguments -ne $arguments
  ) {
    throw "Refusing to move or reinterpret the installed notification-coverage task."
  }
}

$action = New-ScheduledTaskAction `
  -Execute $pythonExe `
  -Argument $arguments `
  -WorkingDirectory $repoRoot
$user = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$logonTrigger = New-ScheduledTaskTrigger -AtLogOn -User $user
$intervalTrigger = New-ScheduledTaskTrigger `
  -Once `
  -At (Get-Date).AddMinutes(1) `
  -RepetitionInterval (New-TimeSpan -Minutes $IntervalMinutes) `
  -RepetitionDuration (New-TimeSpan -Days 3650)
$s4uPrincipal = New-ScheduledTaskPrincipal -UserId $user -LogonType S4U -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet `
  -AllowStartIfOnBatteries `
  -DontStopIfGoingOnBatteries `
  -ExecutionTimeLimit (New-TimeSpan -Minutes 5) `
  -Hidden `
  -MultipleInstances IgnoreNew `
  -RestartCount 2 `
  -RestartInterval (New-TimeSpan -Minutes 1) `
  -StartWhenAvailable

$registrationMode = "S4U"
try {
  Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger @($logonTrigger, $intervalTrigger) `
    -Principal $s4uPrincipal `
    -Settings $settings `
    -ErrorAction Stop `
    -Force | Out-Null
} catch {
  if ($_.FullyQualifiedErrorId -ne "HRESULT 0x80070005,Register-ScheduledTask") {
    throw
  }
  $registrationMode = "InteractiveFallback"
  $interactivePrincipal = New-ScheduledTaskPrincipal `
    -UserId $user `
    -LogonType Interactive `
    -RunLevel Limited
  Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger @($logonTrigger, $intervalTrigger) `
    -Principal $interactivePrincipal `
    -Settings $settings `
    -ErrorAction Stop `
    -Force | Out-Null
  Write-Warning (
    "S4U registration was denied (HRESULT 0x80070005); registered '$TaskName' " +
    "for the current interactive session with interval and logon triggers."
  )
}

if ($Start) {
  Start-ScheduledTask -TaskName $TaskName
}

$registeredTask = Get-ScheduledTask -TaskName $TaskName
$registeredTask | Add-Member -NotePropertyName RegistrationMode -NotePropertyValue $registrationMode
$registeredTask
