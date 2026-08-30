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
$deploymentTask = Get-ScheduledTask -TaskPath "\" -TaskName $deploymentTaskName -ErrorAction Stop
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
$producerTask = Get-ScheduledTask -TaskPath "\" -TaskName $producerTaskName -ErrorAction Stop
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
  "'loaded_configuration':h['runtime_configuration_fingerprint']," +
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
  "from insider_alerts.execution.autopilot_watchdog import " +
  "notification_runtime_configuration_fingerprint as config_fingerprint; " +
  "print(json.dumps({'source':str(Path(s.database_path).resolve())," +
  "'journal':str(Path(s.notification_transport_db).resolve())," +
  "'journal_policy':str(Path(s.notification_transport_policy_path).resolve())," +
  "'configuration_fingerprint':config_fingerprint(s,repo_root=Path(sys.argv[1]))}," +
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
if (
  [string]$producerState.loaded_configuration -notmatch "^[0-9a-f]{64}$" -or
  [string]$producerState.loaded_configuration -ne
    [string]$effectiveSettings.configuration_fingerprint
) {
  throw "Notification coverage settings do not match the running producer's loaded settings."
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
$existingTask = Get-ScheduledTask -TaskPath "\" -TaskName $TaskName -ErrorAction SilentlyContinue
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
$userIdentity = [System.Security.Principal.WindowsIdentity]::GetCurrent()
$user = $userIdentity.Name
$userSid = [string]$userIdentity.User.Value
if ([string]::IsNullOrWhiteSpace($userSid)) {
  throw "Could not bind the current Windows account SID."
}
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
$settings.Enabled = $false

function Get-CoverageTaskSnapshot {
  $task = Get-ScheduledTask -TaskPath "\" -TaskName $TaskName -ErrorAction SilentlyContinue
  if ($null -eq $task) {
    return [pscustomobject]@{
      Exists = $false
      Xml = $null
      WasEnabled = $false
      WasRunning = $false
    }
  }
  return [pscustomobject]@{
    Exists = $true
    Xml = Export-ScheduledTask -TaskPath "\" -TaskName $TaskName
    WasEnabled = [bool]$task.Settings.Enabled
    # Rebound only after the existing definition is disabled, closing the trigger race.
    WasRunning = $false
  }
}

function Get-DisabledTaskXml([string]$Xml) {
  [xml]$document = $Xml
  $namespace = $document.DocumentElement.NamespaceURI
  $manager = New-Object System.Xml.XmlNamespaceManager($document.NameTable)
  $manager.AddNamespace("task", $namespace)
  $settingsNode = $document.SelectSingleNode("/task:Task/task:Settings", $manager)
  if ($null -eq $settingsNode) {
    throw "Scheduled task XML has no Settings element."
  }
  $enabled = $settingsNode.SelectSingleNode("task:Enabled", $manager)
  if ($null -eq $enabled) {
    $enabled = $document.CreateElement("Enabled", $namespace)
    [void]$settingsNode.AppendChild($enabled)
  }
  $enabled.InnerText = "false"
  return $document.OuterXml
}

function Stop-CoverageTaskAndWait([bool]$CaptureRunningState) {
  $task = Get-ScheduledTask -TaskPath "\" -TaskName $TaskName -ErrorAction SilentlyContinue
  if ($null -eq $task) {
    return
  }
  Disable-ScheduledTask -TaskPath "\" -TaskName $TaskName | Out-Null
  $task = Get-ScheduledTask -TaskPath "\" -TaskName $TaskName -ErrorAction Stop
  if ($CaptureRunningState) {
    $script:taskSnapshot.WasRunning = ($task.State -eq "Running")
  }
  if ($task.State -eq "Running") {
    Stop-ScheduledTask -TaskPath "\" -TaskName $TaskName
    $deadline = (Get-Date).AddSeconds(15)
    do {
      Start-Sleep -Milliseconds 250
      $task = Get-ScheduledTask -TaskPath "\" -TaskName $TaskName -ErrorAction Stop
    } while ($task.State -eq "Running" -and (Get-Date) -lt $deadline)
    if ($task.State -eq "Running") {
      throw "Timed out stopping the existing notification-coverage task."
    }
  }
}

function Assert-RegisteredCoverageTask(
  [string]$ExpectedLogonType,
  [bool]$ExpectedEnabled
) {
  $task = Get-ScheduledTask -TaskPath "\" -TaskName $TaskName -ErrorAction Stop
  if ($task.Actions.Count -ne 1) {
    throw "The registered task must have exactly one action."
  }
  $registeredAction = $task.Actions[0]
  $logonTriggers = @(
    $task.Triggers | Where-Object { $_.CimClass.CimClassName -eq "MSFT_TaskLogonTrigger" }
  )
  $timeTriggers = @(
    $task.Triggers | Where-Object { $_.CimClass.CimClassName -eq "MSFT_TaskTimeTrigger" }
  )
  if ($logonTriggers.Count -ne 1 -or $timeTriggers.Count -ne 1) {
    throw "The registered task must have exactly one logon and one time trigger."
  }
  try {
    $principalSid = [string](
      (New-Object System.Security.Principal.NTAccount($task.Principal.UserId)).Translate(
        [System.Security.Principal.SecurityIdentifier]
      ).Value
    )
    $logonSid = [string](
      (New-Object System.Security.Principal.NTAccount($logonTriggers[0].UserId)).Translate(
        [System.Security.Principal.SecurityIdentifier]
      ).Value
    )
  } catch {
    throw "The registered task account identity could not be resolved to a Windows SID."
  }
  if (
    $task.TaskPath -ne "\" -or
    $registeredAction.Execute -ne $pythonExe -or
    $registeredAction.WorkingDirectory -ne $repoRoot -or
    $registeredAction.Arguments -ne $arguments -or
    $task.Triggers.Count -ne 2 -or
    $principalSid -ne $userSid -or
    $logonSid -ne $userSid -or
    -not $logonTriggers[0].Enabled -or
    -not $timeTriggers[0].Enabled -or
    [string]$timeTriggers[0].Repetition.Interval -ne "PT1M" -or
    [string]$timeTriggers[0].Repetition.Duration -ne "P3650D" -or
    [string]$task.Principal.LogonType -ne $ExpectedLogonType -or
    [string]$task.Principal.RunLevel -ne "Limited" -or
    [bool]$task.Settings.Enabled -ne $ExpectedEnabled -or
    -not $task.Settings.Hidden -or
    [string]$task.Settings.MultipleInstances -ne "IgnoreNew" -or
    [string]$task.Settings.ExecutionTimeLimit -ne "PT5M" -or
    [int]$task.Settings.RestartCount -ne 2 -or
    [string]$task.Settings.RestartInterval -ne "PT1M" -or
    -not $task.Settings.StartWhenAvailable -or
    $task.Settings.DisallowStartIfOnBatteries -or
    $task.Settings.StopIfGoingOnBatteries
  ) {
    throw "The registered notification-coverage task failed exact post-registration validation."
  }
  return $task
}

function Restore-CoverageTask {
  Stop-CoverageTaskAndWait -CaptureRunningState $false
  if (-not $taskSnapshot.Exists) {
    $created = Get-ScheduledTask -TaskPath "\" -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($null -ne $created) {
      Unregister-ScheduledTask -TaskPath "\" -TaskName $TaskName -Confirm:$false
    }
    return
  }
  $disabledXml = Get-DisabledTaskXml -Xml $taskSnapshot.Xml
  Register-ScheduledTask `
    -TaskPath "\" `
    -TaskName $TaskName `
    -Xml $disabledXml `
    -Force | Out-Null
  if ($taskSnapshot.WasRunning) {
    Enable-ScheduledTask -TaskPath "\" -TaskName $TaskName | Out-Null
    Start-ScheduledTask -TaskPath "\" -TaskName $TaskName
    if (-not $taskSnapshot.WasEnabled) {
      Disable-ScheduledTask -TaskPath "\" -TaskName $TaskName | Out-Null
    }
  } elseif ($taskSnapshot.WasEnabled) {
    Enable-ScheduledTask -TaskPath "\" -TaskName $TaskName | Out-Null
  }
}

$installMutex = New-Object System.Threading.Mutex(
  $false,
  "Global\InsiderAlertsNotificationCoverageInstaller-v1"
)
$mutexAcquired = $false
try {
  try {
    $mutexAcquired = $installMutex.WaitOne(0)
  } catch [System.Threading.AbandonedMutexException] {
    $mutexAcquired = $true
  }
  if (-not $mutexAcquired) {
    throw "Another notification-coverage installation is already in progress."
  }

  $taskSnapshot = Get-CoverageTaskSnapshot
  $mutationStarted = $false
  try {
    if ($taskSnapshot.Exists) {
      $mutationStarted = $true
      Stop-CoverageTaskAndWait -CaptureRunningState $true
    }
    $registrationMode = "S4U"
    $expectedLogonType = "S4U"
    try {
      $mutationStarted = $true
      Register-ScheduledTask `
        -TaskPath "\" `
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
      $expectedLogonType = "Interactive"
      $interactivePrincipal = New-ScheduledTaskPrincipal `
        -UserId $user `
        -LogonType Interactive `
        -RunLevel Limited
      Register-ScheduledTask `
        -TaskPath "\" `
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

    $registeredTask = Assert-RegisteredCoverageTask `
      -ExpectedLogonType $expectedLogonType `
      -ExpectedEnabled $false
    Enable-ScheduledTask -TaskPath "\" -TaskName $TaskName | Out-Null
    $registeredTask = Assert-RegisteredCoverageTask `
      -ExpectedLogonType $expectedLogonType `
      -ExpectedEnabled $true
    if ($Start) {
      Start-ScheduledTask -TaskPath "\" -TaskName $TaskName
    }
  } catch {
    $installError = $_
    if ($mutationStarted) {
      try {
        Restore-CoverageTask
      } catch {
        throw (
          "Notification-coverage installation failed ('$($installError.Exception.Message)') " +
          "and prior task restoration failed ('$($_.Exception.Message)')."
        )
      }
    }
    throw $installError
  }
  $registeredTask | Add-Member -NotePropertyName RegistrationMode -NotePropertyValue $registrationMode
  $resultTask = $registeredTask
} finally {
  if ($mutexAcquired) {
    $installMutex.ReleaseMutex()
  }
  $installMutex.Dispose()
}
$resultTask
