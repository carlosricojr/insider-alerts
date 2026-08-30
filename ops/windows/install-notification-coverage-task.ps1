param(
  [string]$TaskName = "Insider Alerts Notification Coverage",
  [int]$IntervalMinutes = 1,
  [string]$SourceDatabase = "",
  [string]$JournalDatabase = "",
  [string]$CoverageDatabase = "",
  [switch]$Start
)

$ErrorActionPreference = "Stop"

if ($IntervalMinutes -ne 1) {
  throw "IntervalMinutes must be exactly 1 to satisfy the sealed 180-second freshness bound."
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
$expectedCanaryArguments = (
  "-m insider_alerts.cli ops live-canary --loop --interval 15 --live --notify " +
  "--invalid-commission-handling reject --arm-phrase I_ACCEPT_LIVE_CANARY_RISK " +
  "--output-log logs/live-canary.out.log --error-log logs/live-canary.err.log"
)
if (
  $deploymentAction.Execute -ne $pythonExe -or
  $deploymentAction.Arguments -ne $expectedCanaryArguments
) {
  throw "The live-canary task does not exactly match the reviewed frozen command."
}

$producerTaskName = "Insider Alerts Autopilot Worker"
$producerTask = Get-ScheduledTask -TaskName $producerTaskName -ErrorAction Stop
if ($producerTask.Actions.Count -ne 1) {
  throw "The notification-producing autopilot task must have exactly one action."
}
$producerAction = $producerTask.Actions[0]
$producerRepoRoot = [System.IO.Path]::GetFullPath($producerAction.WorkingDirectory).TrimEnd("\")
$expectedProducerArgumentsSha256 = "f83884612eb83cac6d2d1ee959689295d05f8364899f5b0340d9230ecc96fb67"
$sha256 = [System.Security.Cryptography.SHA256]::Create()
try {
  $producerArgumentBytes = [System.Text.Encoding]::UTF8.GetBytes($producerAction.Arguments)
  $producerArgumentsSha256 = (
    [System.BitConverter]::ToString($sha256.ComputeHash($producerArgumentBytes))
  ).Replace("-", "").ToLowerInvariant()
} finally {
  $sha256.Dispose()
}
if (
  $producerAction.Execute -ne $pythonExe -or
  $producerRepoRoot -ne $repoRoot -or
  $producerArgumentsSha256 -ne $expectedProducerArgumentsSha256
) {
  throw "The notification-producing autopilot task does not match the reviewed deployment."
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
$producerHeartbeatDb = Resolve-ConfiguredPath "" "data\autopilot_health.db"
$journalPolicy = Join-Path $repoRoot "docs\research\contracts\notification-transport-v1.json"
$coveragePolicy = Join-Path $repoRoot "docs\research\contracts\notification-coverage-v1.json"

foreach ($path in @(
  $pythonExe,
  $pythonConsole,
  $sourceDb,
  $journalDb,
  $coverageDb,
  $producerHeartbeatDb,
  $journalPolicy,
  $coveragePolicy
)) {
  if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
    throw "Missing required notification-coverage file at $path"
  }
}

$producerStateCommand = (
  "import json,sys; " +
  "from insider_alerts.execution.autopilot_watchdog import AutopilotHealthStore; " +
  "from insider_alerts.execution.canary import runtime_source_fingerprint; " +
  "h=AutopilotHealthStore(sys.argv[1]).read(); " +
  "print(json.dumps({'loaded':h['source_fingerprint']," +
  "'current':runtime_source_fingerprint()," +
  "'last_progress_utc':h['last_progress_utc']},separators=(',',':')))"
)
$producerStateOutput = @(& $pythonConsole -c $producerStateCommand $producerHeartbeatDb)
if ($LASTEXITCODE -ne 0 -or $producerStateOutput.Count -ne 1) {
  throw "Could not verify the notification producer's loaded source fingerprint."
}
try {
  $producerState = ([string]$producerStateOutput[0]).Trim() | ConvertFrom-Json
  $producerProgressAt = [System.DateTimeOffset]::Parse([string]$producerState.last_progress_utc)
} catch {
  throw "The notification producer's source-fingerprint evidence is invalid."
}
$producerProgressAge = [System.DateTimeOffset]::UtcNow - $producerProgressAt.ToUniversalTime()
if (
  [string]$producerState.loaded -ne [string]$producerState.current -or
  $producerProgressAge.TotalSeconds -lt -5 -or
  $producerProgressAge.TotalSeconds -gt 600
) {
  throw "The notification producer has not loaded the current deployment source recently."
}

$schedulerEnvironment = @{}
foreach ($environmentName in @(
  "DATABASE_PATH",
  "NOTIFICATION_TRANSPORT_DB",
  "NOTIFICATION_TRANSPORT_POLICY_PATH"
)) {
  $userValue = [System.Environment]::GetEnvironmentVariable($environmentName, "User")
  $machineValue = [System.Environment]::GetEnvironmentVariable($environmentName, "Machine")
  $schedulerEnvironment[$environmentName] = if ($null -ne $userValue) {
    $userValue
  } else {
    $machineValue
  }
}
$schedulerEnvironmentJson = $schedulerEnvironment | ConvertTo-Json -Compress
$schedulerEnvironmentBase64 = [System.Convert]::ToBase64String(
  [System.Text.Encoding]::UTF8.GetBytes($schedulerEnvironmentJson)
)
$settingsCommand = (
  "import base64,json,os,sys; from pathlib import Path; " +
  "values=json.loads(base64.b64decode(sys.argv[2]).decode('utf-8')); " +
  "[(os.environ.pop(k,None) if v is None else os.environ.__setitem__(k,v)) " +
  "for k,v in values.items()]; os.chdir(sys.argv[1]); " +
  "from insider_alerts.config import get_settings; s=get_settings(); " +
  "print(json.dumps({'source':str(Path(s.database_path).resolve())," +
  "'journal':str(Path(s.notification_transport_db).resolve())," +
  "'journal_policy':str(Path(s.notification_transport_policy_path).resolve())}," +
  "separators=(',',':')))"
)
$effectiveSettingsOutput = @(
  & $pythonConsole -c $settingsCommand $repoRoot $schedulerEnvironmentBase64
)
if ($LASTEXITCODE -ne 0 -or $effectiveSettingsOutput.Count -ne 1) {
  throw "Could not resolve the notification producer's scheduler-effective settings."
}
try {
  $effectiveSettings = ([string]$effectiveSettingsOutput[0]).Trim() | ConvertFrom-Json
  $effectiveSourceDb = (
    Resolve-Path -LiteralPath ([string]$effectiveSettings.source) -ErrorAction Stop
  ).Path
  $effectiveJournalDb = (
    Resolve-Path -LiteralPath ([string]$effectiveSettings.journal) -ErrorAction Stop
  ).Path
  $effectiveJournalPolicy = (
    Resolve-Path -LiteralPath ([string]$effectiveSettings.journal_policy) -ErrorAction Stop
  ).Path
} catch {
  throw "The notification producer's scheduler-effective settings are invalid."
}
if ($sourceDb -ne $effectiveSourceDb) {
  throw (
    "Notification coverage source '$sourceDb' does not match the autopilot source " +
    "'$effectiveSourceDb'."
  )
}
if ($journalDb -ne $effectiveJournalDb -or $journalPolicy -ne $effectiveJournalPolicy) {
  throw "Notification coverage journal paths do not match the autopilot's effective settings."
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

$preflightArguments = @(
  "-m", "insider_alerts.research.notification_coverage_worker",
  "--source-db", $sourceDb,
  "--journal-db", $journalDb,
  "--coverage-db", $coverageDb,
  "--journal-policy", $journalPolicy,
  "--coverage-policy", $coveragePolicy
)
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

# A one-shot reconciliation validates sealed structure and repairs stale health after deliberate
# containment. It remains order-incapable and must pass before task registration or restart.
& $pythonConsole @preflightArguments | Out-Host
if ($LASTEXITCODE -ne 0) {
  throw "Refusing task registration because coverage reconciliation is invalid."
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
$registeredAction = $registeredTask.Actions[0]
if (
  $registeredTask.Actions.Count -ne 1 -or
  $registeredAction.Execute -ne $pythonExe -or
  $registeredAction.WorkingDirectory -ne $repoRoot -or
  $registeredAction.Arguments -ne $arguments -or
  -not $registeredTask.Settings.Hidden -or
  [string]$registeredTask.Settings.MultipleInstances -ne "IgnoreNew"
) {
  throw "The registered notification-coverage task failed exact post-registration validation."
}
$registeredTask | Add-Member -NotePropertyName RegistrationMode -NotePropertyValue $registrationMode
$registeredTask
