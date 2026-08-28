param(
  [string]$TaskName = "Insider Alerts Autopilot Watchdog",
  [string]$WorkerTaskName = "Insider Alerts Autopilot Worker",
  [string]$SecTaskName = "Insider Alerts SEC Ingestion Watchdog",
  [string]$SecWorkerTaskName = "Insider Alerts SEC Ingestion Worker",
  [int]$RecoveryIntervalMinutes = 1,
  [int]$QuantTimeoutSeconds = 120,
  [int]$StaleHeartbeatSeconds = 0,
  [int]$SecStaleHeartbeatSeconds = 0,
  [string]$AlphaRoot = "",
  [switch]$RunElevated,
  [switch]$Start
)

$ErrorActionPreference = "Stop"
$staleHeartbeatExplicit = $PSBoundParameters.ContainsKey("StaleHeartbeatSeconds")
$secStaleHeartbeatExplicit = $PSBoundParameters.ContainsKey("SecStaleHeartbeatSeconds")

if (-not $Start) {
  throw "This transactional installer requires -Start so both new workers can be verified."
}

if ([string]::IsNullOrWhiteSpace($TaskName) -or [string]::IsNullOrWhiteSpace($WorkerTaskName) -or `
    [string]::IsNullOrWhiteSpace($SecTaskName) -or [string]::IsNullOrWhiteSpace($SecWorkerTaskName)) {
  throw "All task names must be nonempty."
}
$taskNames = @($TaskName, $WorkerTaskName, $SecTaskName, $SecWorkerTaskName)
if (($taskNames | Sort-Object -Unique).Count -ne $taskNames.Count) {
  throw "All task names must be distinct."
}
if ($RecoveryIntervalMinutes -lt 1) {
  throw "RecoveryIntervalMinutes must be greater than or equal to 1."
}
if ($QuantTimeoutSeconds -lt 10 -or $QuantTimeoutSeconds -gt 900) {
  throw "QuantTimeoutSeconds must be between 10 and 900."
}
if ($StaleHeartbeatSeconds -lt 0) {
  throw "StaleHeartbeatSeconds cannot be negative."
}
if ($SecStaleHeartbeatSeconds -lt 0) {
  throw "SecStaleHeartbeatSeconds cannot be negative."
}

$installMutexName = "Global\InsiderAlertsAutopilotTaskInstaller-v1"
$installMutex = [System.Threading.Mutex]::new($false, $installMutexName)
$installMutexHeld = $false
try {
  try {
    $installMutexHeld = $installMutex.WaitOne(0)
  } catch [System.Threading.AbandonedMutexException] {
    # WaitOne grants ownership before reporting abandonment. Continue while preserving the fence.
    $installMutexHeld = $true
  }
  if (-not $installMutexHeld) {
    throw "Another Insider Alerts task installation is already running."
  }

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir "..\..")
$pythonExe = Join-Path $repoRoot ".venv\Scripts\pythonw.exe"
$pythonConsoleExe = Join-Path $repoRoot ".venv\Scripts\python.exe"
$researchRoot = Join-Path $repoRoot "data\research"
New-Item -ItemType Directory -Path $researchRoot -Force | Out-Null
$chainStoreDb = Join-Path $researchRoot "option_chain_feed.db"
$heartbeatDb = Join-Path $repoRoot "data\autopilot_health.db"
$secHeartbeatDb = Join-Path $repoRoot "data\sec_ingestion_health.db"

if ([string]::IsNullOrWhiteSpace($AlphaRoot)) {
  $repositoriesRoot = Split-Path -Parent $repoRoot
  $AlphaRoot = Join-Path $repositoriesRoot "alpha-core-worktrees\insider-evidence-surface-runtime"
}
$alphaRootResolved = Resolve-Path -LiteralPath $AlphaRoot
$alphaPython = Join-Path $alphaRootResolved ".venv\Scripts\python.exe"
$alphaChainScript = Join-Path $alphaRootResolved "scripts\capture_insider_option_chain.py"

if (-not (Test-Path $pythonExe)) {
  throw "Missing windowless virtualenv Python at $pythonExe"
}
if (-not (Test-Path $pythonConsoleExe)) {
  throw "Missing virtualenv Python at $pythonConsoleExe"
}

if (-not (Test-Path -LiteralPath $alphaPython -PathType Leaf)) {
  throw "Missing alpha runtime interpreter at $alphaPython"
}

if (-not (Test-Path -LiteralPath $alphaChainScript -PathType Leaf)) {
  throw "Missing alpha option-chain entrypoint at $alphaChainScript"
}

if (-not (Test-Path (Join-Path $repoRoot ".env"))) {
  throw "Missing .env at $repoRoot\.env"
}

Push-Location $repoRoot
try {
  $budgetOutput = @(& $pythonConsoleExe -m insider_alerts.cli ops autopilot-config-validate `
    --quant-timeout-seconds $QuantTimeoutSeconds `
    --heartbeat-stale-seconds 2147483647)
  if ($LASTEXITCODE -ne 0) {
    throw "Autopilot watchdog preflight failed."
  }
  try {
    $runtimeBudget = ($budgetOutput -join "`n") | ConvertFrom-Json
    $requiredStaleHeartbeatSeconds = [int]$runtimeBudget.required_stale_seconds
  } catch {
    throw "Autopilot watchdog preflight returned an invalid runtime budget."
  }
  if ($requiredStaleHeartbeatSeconds -lt 300) {
    throw "Autopilot watchdog preflight returned an unsafe stale threshold."
  }
  if (-not $staleHeartbeatExplicit) {
    $StaleHeartbeatSeconds = $requiredStaleHeartbeatSeconds
  } elseif ($StaleHeartbeatSeconds -lt $requiredStaleHeartbeatSeconds) {
    throw "StaleHeartbeatSeconds must be at least $requiredStaleHeartbeatSeconds."
  }

  $secBudgetOutput = @(& $pythonConsoleExe -m insider_alerts.cli ops sec-ingestion-config-validate `
    --heartbeat-stale-seconds 2147483647)
  if ($LASTEXITCODE -ne 0) {
    throw "SEC ingestion watchdog preflight failed."
  }
  try {
    $secRuntimeBudget = ($secBudgetOutput -join "`n") | ConvertFrom-Json
    $requiredSecStaleHeartbeatSeconds = [int]$secRuntimeBudget.required_stale_seconds
  } catch {
    throw "SEC ingestion watchdog preflight returned an invalid runtime budget."
  }
  if ($requiredSecStaleHeartbeatSeconds -lt 300) {
    throw "SEC ingestion watchdog preflight returned an unsafe stale threshold."
  }
  if (-not $secStaleHeartbeatExplicit) {
    $SecStaleHeartbeatSeconds = $requiredSecStaleHeartbeatSeconds
  } elseif ($SecStaleHeartbeatSeconds -lt $requiredSecStaleHeartbeatSeconds) {
    throw "SecStaleHeartbeatSeconds must be at least $requiredSecStaleHeartbeatSeconds."
  }
} finally {
  Pop-Location
}

$user = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$workerAction = New-ScheduledTaskAction `
  -Execute $pythonExe `
  -Argument "-m insider_alerts.cli ops autopilot --loop --interval 300 --no-sec-ingestion --decision-engine quant --quant-agent-id quant-insider --quant-batch-size 8 --quant-thinking low --quant-timeout-seconds $QuantTimeoutSeconds --decision-limit 100 --notify --notify-approve-only --alpha-chain-python `"$alphaPython`" --alpha-chain-script `"$alphaChainScript`" --option-chain-store-db `"$chainStoreDb`" --heartbeat-db `"$heartbeatDb`" --heartbeat-stale-seconds $StaleHeartbeatSeconds --output-log logs/autopilot.out.log --error-log logs/autopilot.err.log" `
  -WorkingDirectory $repoRoot
$watchdogAction = New-ScheduledTaskAction `
  -Execute $pythonExe `
  -Argument "-m insider_alerts.cli ops autopilot-watchdog --worker-task-name `"$WorkerTaskName`" --heartbeat-db `"$heartbeatDb`" --stale-seconds $StaleHeartbeatSeconds --quant-timeout-seconds $QuantTimeoutSeconds --output-log logs/autopilot-watchdog.log" `
  -WorkingDirectory $repoRoot
$secWorkerAction = New-ScheduledTaskAction `
  -Execute $pythonExe `
  -Argument "-m insider_alerts.cli ops sec-ingestion --loop --interval 60 --poll-max-items 40 --enrich-limit 100 --enqueue-limit 100 --heartbeat-db `"$secHeartbeatDb`" --heartbeat-stale-seconds $SecStaleHeartbeatSeconds --output-log logs/sec-ingestion.out.log --error-log logs/sec-ingestion.err.log" `
  -WorkingDirectory $repoRoot
$secWatchdogAction = New-ScheduledTaskAction `
  -Execute $pythonExe `
  -Argument "-m insider_alerts.cli ops sec-ingestion-watchdog --worker-task-name `"$SecWorkerTaskName`" --heartbeat-db `"$secHeartbeatDb`" --stale-seconds $SecStaleHeartbeatSeconds --output-log logs/sec-ingestion-watchdog.log" `
  -WorkingDirectory $repoRoot

$watchdogLogonTrigger = New-ScheduledTaskTrigger -AtLogOn -User $user
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

$workerSettings = New-ScheduledTaskSettingsSet `
  -AllowStartIfOnBatteries `
  -DontStopIfGoingOnBatteries `
  -ExecutionTimeLimit (New-TimeSpan -Seconds 0) `
  -Hidden `
  -MultipleInstances IgnoreNew `
  -RestartCount 999 `
  -RestartInterval (New-TimeSpan -Minutes 1) `
  -StartWhenAvailable
$watchdogSettings = New-ScheduledTaskSettingsSet `
  -AllowStartIfOnBatteries `
  -DontStopIfGoingOnBatteries `
  -ExecutionTimeLimit (New-TimeSpan -Minutes 1) `
  -Hidden `
  -MultipleInstances IgnoreNew `
  -RestartCount 3 `
  -RestartInterval (New-TimeSpan -Minutes 1) `
  -StartWhenAvailable
# Watchdog time/logon triggers must remain fenced until their corresponding worker has passed
# startup verification. Starting the workers directly keeps the cutover order deterministic.
$watchdogSettings.Enabled = $false

function Stop-TaskAndWait(
  [string]$Name,
  [object]$Snapshot = $null,
  [switch]$RefreshEnabledState
) {
  $task = Get-ScheduledTask -TaskName $Name -ErrorAction SilentlyContinue
  if ($null -eq $task) {
    return
  }
  if ($null -ne $Snapshot -and $RefreshEnabledState -and -not $Snapshot.EnabledStateBound) {
    # Both watchdogs are already stopped before this is used for a worker, so bind rollback's
    # enabled state immediately before the worker itself is fenced.
    $Snapshot.WasEnabled = [bool]$task.Settings.Enabled
    $Snapshot.EnabledStateBound = $true
  }
  # Fence triggers and RestartOnFailure before observing or changing the running process.
  Disable-ScheduledTask -TaskName $Name | Out-Null
  # The task may have started between the first query and the fence. Only this post-fence state is
  # safe to use when deciding whether a running process must be stopped.
  $task = Get-ScheduledTask -TaskName $Name
  if ($null -ne $Snapshot) {
    $Snapshot.WasRunning = ($task.State -eq "Running")
    $Snapshot.StateBound = $true
  }
  if ($task.State -eq "Running") {
    Stop-ScheduledTask -TaskName $Name
    $deadline = (Get-Date).AddSeconds(15)
    do {
      Start-Sleep -Milliseconds 250
      $task = Get-ScheduledTask -TaskName $Name
    } while ($task.State -eq "Running" -and (Get-Date) -lt $deadline)
    if ($task.State -eq "Running") {
      throw "Timed out stopping the existing $Name task."
    }
  }
}

function Get-TaskSnapshot([string]$Name) {
  $task = Get-ScheduledTask -TaskName $Name -ErrorAction SilentlyContinue
  if ($null -eq $task) {
    return [pscustomobject]@{
      Name = $Name
      Exists = $false
      Xml = $null
      WasRunning = $false
      WasEnabled = $false
      StateBound = $true
      EnabledStateBound = $true
    }
  }
  return [pscustomobject]@{
    Name = $Name
    Exists = $true
    Xml = Export-ScheduledTask -TaskName $Name
    # Recorded only by Stop-TaskAndWait after the task has been fenced. A watchdog may start a
    # worker between this definition snapshot and that fence.
    WasRunning = $false
    WasEnabled = [bool]$task.Settings.Enabled
    StateBound = $false
    EnabledStateBound = $true
  }
}

function Get-DisabledTaskXml([string]$Xml) {
  [xml]$document = $Xml
  $namespace = $document.DocumentElement.NamespaceURI
  $manager = New-Object System.Xml.XmlNamespaceManager($document.NameTable)
  $manager.AddNamespace("task", $namespace)
  $settings = $document.SelectSingleNode("/task:Task/task:Settings", $manager)
  if ($null -eq $settings) {
    throw "Scheduled task XML has no Settings element."
  }
  $enabled = $settings.SelectSingleNode("task:Enabled", $manager)
  if ($null -eq $enabled) {
    $enabled = $document.CreateElement("Enabled", $namespace)
    [void]$settings.AppendChild($enabled)
  }
  $enabled.InnerText = "false"
  return $document.OuterXml
}

function Get-WorkerHealth([string]$HealthCommand, [string]$HealthDb) {
  Push-Location $repoRoot
  try {
    $raw = @(& $pythonConsoleExe -m insider_alerts.cli ops $HealthCommand `
      --heartbeat-db $HealthDb 2>$null)
    if ($LASTEXITCODE -ne 0) {
      return $null
    }
    return (($raw -join "`n") | ConvertFrom-Json)
  } catch {
    return $null
  } finally {
    Pop-Location
  }
}

function Wait-ForFreshWorker(
  [string]$PreviousRuntimeId,
  [string]$HealthCommand,
  [string]$HealthDb,
  [string]$WorkerName,
  [string]$WorkerLabel,
  [int]$DeadlineSeconds = 90,
  [bool]$RequireCycleSuccess = $false
) {
  $deadline = (Get-Date).AddSeconds($DeadlineSeconds)
  $stableRuntimeId = $null
  $stableSamples = 0
  $observedProgressStages = @{}
  do {
    $healthReport = Get-WorkerHealth -HealthCommand $HealthCommand -HealthDb $HealthDb
    $workerTask = Get-ScheduledTask -TaskName $WorkerName -ErrorAction SilentlyContinue
    if ($null -ne $healthReport -and $healthReport.valid -and $null -ne $healthReport.health) {
      $runtimeId = [string]$healthReport.health.runtime_id
      $progressText = [string]$healthReport.health.last_progress_utc
      $progressStage = [string]$healthReport.health.last_progress_stage
      $isNewRuntime = -not [string]::IsNullOrWhiteSpace($runtimeId) -and `
        ([string]::IsNullOrWhiteSpace($PreviousRuntimeId) -or $runtimeId -ne $PreviousRuntimeId)
      $progressIsFresh = $false
      try {
        $progressAge = [DateTimeOffset]::UtcNow - [DateTimeOffset]::Parse($progressText)
        $progressIsFresh = $progressAge.TotalSeconds -ge -5 -and $progressAge.TotalSeconds -le 30
      } catch {
        $progressIsFresh = $false
      }
      # A bounded initial cycle can legitimately have hundreds of individually heartbeating
      # items. Give each new stage one full stage budget, but never let repeated failed cycles
      # extend installation forever by replaying the same stage names.
      if ($isNewRuntime -and $progressIsFresh -and $null -ne $workerTask -and `
          $workerTask.State -eq "Running" -and -not [string]::IsNullOrWhiteSpace($progressStage) -and `
          -not $observedProgressStages.ContainsKey($progressStage)) {
        $observedProgressStages[$progressStage] = $true
        $deadline = (Get-Date).AddSeconds($DeadlineSeconds)
      }
      $cycleSucceeded = -not $RequireCycleSuccess
      if ($RequireCycleSuccess) {
        try {
          $runtimeStarted = [DateTimeOffset]::Parse([string]$healthReport.health.runtime_started_utc)
          $cycleSuccess = [DateTimeOffset]::Parse([string]$healthReport.health.last_cycle_success_utc)
          $cycleSucceeded = $cycleSuccess -ge $runtimeStarted -and `
            $cycleSuccess -le [DateTimeOffset]::UtcNow.AddSeconds(5)
        } catch {
          $cycleSucceeded = $false
        }
      }
      if ($isNewRuntime -and $progressIsFresh -and $null -ne $workerTask -and `
          $workerTask.State -eq "Running" -and $cycleSucceeded) {
        if ($stableRuntimeId -eq $runtimeId) {
          $stableSamples += 1
        } else {
          $stableRuntimeId = $runtimeId
          $stableSamples = 1
        }
        if ($stableSamples -ge 3) {
          return
        }
      } else {
        $stableRuntimeId = $null
        $stableSamples = 0
      }
    }
    Start-Sleep -Milliseconds 500
  } while ((Get-Date) -lt $deadline)
  throw "Timed out waiting for a new, fresh, stably running $WorkerLabel worker."
}

function Get-TaskLastRunTime([string]$Name) {
  $taskInfo = Get-ScheduledTaskInfo -TaskName $Name
  if ($null -eq $taskInfo.LastRunTime) {
    return [DateTime]::MinValue
  }
  return [DateTime]$taskInfo.LastRunTime
}

function Wait-ForSuccessfulTaskRun(
  [string]$Name,
  [DateTime]$PreviousLastRunTime,
  [int]$DeadlineSeconds = 60
) {
  $deadline = (Get-Date).AddSeconds($DeadlineSeconds)
  do {
    $task = Get-ScheduledTask -TaskName $Name -ErrorAction SilentlyContinue
    $taskInfo = Get-ScheduledTaskInfo -TaskName $Name -ErrorAction SilentlyContinue
    if ($null -ne $task -and $null -ne $taskInfo -and `
        [DateTime]$taskInfo.LastRunTime -gt $PreviousLastRunTime -and `
        $task.State -ne "Running") {
      if ([int64]$taskInfo.LastTaskResult -ne 0) {
        throw "$Name completed with task result $($taskInfo.LastTaskResult)."
      }
      return
    }
    Start-Sleep -Milliseconds 250
  } while ((Get-Date) -lt $deadline)
  throw "Timed out waiting for a new successful $Name run."
}

$priorHealth = Get-WorkerHealth `
  -HealthCommand "autopilot-health-status" `
  -HealthDb $heartbeatDb
$previousRuntimeId = ""
if ($null -ne $priorHealth -and $priorHealth.valid -and $null -ne $priorHealth.health) {
  $previousRuntimeId = [string]$priorHealth.health.runtime_id
}
$priorSecHealth = Get-WorkerHealth `
  -HealthCommand "sec-ingestion-health-status" `
  -HealthDb $secHeartbeatDb
$previousSecRuntimeId = ""
if ($null -ne $priorSecHealth -and $priorSecHealth.valid -and $null -ne $priorSecHealth.health) {
  $previousSecRuntimeId = [string]$priorSecHealth.health.runtime_id
}

$snapshots = @(
  (Get-TaskSnapshot -Name $WorkerTaskName),
  (Get-TaskSnapshot -Name $SecWorkerTaskName),
  (Get-TaskSnapshot -Name $TaskName),
  (Get-TaskSnapshot -Name $SecTaskName)
)
# Worker enabled state is rebound after both watchdogs stop. This marker prevents a partial first
# fence from overwriting the captured pre-fence state during rollback.
$snapshots[0].EnabledStateBound = -not $snapshots[0].Exists
$snapshots[1].EnabledStateBound = -not $snapshots[1].Exists
$rollbackTargets = @(
  [pscustomobject]@{
    Name = $TaskName
    Snapshot = $snapshots[2]
    RefreshEnabledState = $false
  },
  [pscustomobject]@{
    Name = $SecTaskName
    Snapshot = $snapshots[3]
    RefreshEnabledState = $false
  },
  [pscustomobject]@{
    Name = $WorkerTaskName
    Snapshot = $snapshots[0]
    RefreshEnabledState = $true
  },
  [pscustomobject]@{
    Name = $SecWorkerTaskName
    Snapshot = $snapshots[1]
    RefreshEnabledState = $true
  }
)

try {
  # Fence both watchdogs before stopping either worker, so no replacement can overlap them.
  Stop-TaskAndWait -Name $TaskName -Snapshot $snapshots[2]
  Stop-TaskAndWait -Name $SecTaskName -Snapshot $snapshots[3]
  Stop-TaskAndWait `
    -Name $WorkerTaskName `
    -Snapshot $snapshots[0] `
    -RefreshEnabledState
  Stop-TaskAndWait `
    -Name $SecWorkerTaskName `
    -Snapshot $snapshots[1] `
    -RefreshEnabledState

  Register-ScheduledTask `
    -TaskName $WorkerTaskName `
    -Action $workerAction `
    -Principal $principal `
    -Settings $workerSettings `
    -Force | Out-Null

  Register-ScheduledTask `
    -TaskName $SecWorkerTaskName `
    -Action $secWorkerAction `
    -Principal $principal `
    -Settings $workerSettings `
    -Force | Out-Null

  Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $watchdogAction `
    -Trigger @($watchdogLogonTrigger, $watchdogTrigger) `
    -Principal $principal `
    -Settings $watchdogSettings `
    -Force | Out-Null

  Register-ScheduledTask `
    -TaskName $SecTaskName `
    -Action $secWatchdogAction `
    -Trigger @($watchdogLogonTrigger, $watchdogTrigger) `
    -Principal $principal `
    -Settings $watchdogSettings `
    -Force | Out-Null

  # Start acquisition first. If decision startup then fails, rollback stops acquisition and
  # restores the prior all-in-one worker, which resumes SEC ingestion.
  Start-ScheduledTask -TaskName $SecWorkerTaskName
  Wait-ForFreshWorker `
    -PreviousRuntimeId $previousSecRuntimeId `
    -HealthCommand "sec-ingestion-health-status" `
    -HealthDb $secHeartbeatDb `
    -WorkerName $SecWorkerTaskName `
    -WorkerLabel "SEC ingestion" `
    -DeadlineSeconds ($SecStaleHeartbeatSeconds + 120) `
    -RequireCycleSuccess $true
  $previousSecWatchdogRunTime = Get-TaskLastRunTime -Name $SecTaskName
  Enable-ScheduledTask -TaskName $SecTaskName | Out-Null
  Start-ScheduledTask -TaskName $SecTaskName
  Wait-ForSuccessfulTaskRun `
    -Name $SecTaskName `
    -PreviousLastRunTime $previousSecWatchdogRunTime
  Start-ScheduledTask -TaskName $WorkerTaskName
  Wait-ForFreshWorker `
    -PreviousRuntimeId $previousRuntimeId `
    -HealthCommand "autopilot-health-status" `
    -HealthDb $heartbeatDb `
    -WorkerName $WorkerTaskName `
    -WorkerLabel "autopilot"
  $previousWatchdogRunTime = Get-TaskLastRunTime -Name $TaskName
  Enable-ScheduledTask -TaskName $TaskName | Out-Null
  Start-ScheduledTask -TaskName $TaskName
  Wait-ForSuccessfulTaskRun `
    -Name $TaskName `
    -PreviousLastRunTime $previousWatchdogRunTime
} catch {
  $installError = $_
  $rollbackErrors = @()
  # Phase 1: disable and stop every replacement before restoring either prior definition.
  foreach ($target in $rollbackTargets) {
    try {
      $stopParameters = @{ Name = $target.Name }
      if (-not $target.Snapshot.StateBound) {
        $stopParameters.Snapshot = $target.Snapshot
        if ($target.RefreshEnabledState) {
          $stopParameters.RefreshEnabledState = $true
        }
      }
      Stop-TaskAndWait @stopParameters
      if (-not $target.Snapshot.StateBound -or -not $target.Snapshot.EnabledStateBound) {
        throw "prior scheduled-task state could not be bound after fencing"
      }
    } catch {
      $rollbackErrors += "$($target.Name) stop: $($_.Exception.Message)"
    }
  }
  # Phase 2: restore every prior definition disabled, workers first. No trigger can run until all
  # worker/watchdog pairs are back on mutually compatible definitions.
  if ($rollbackErrors.Count -eq 0) {
    foreach ($snapshot in $snapshots) {
      try {
        if ($snapshot.Exists) {
          $disabledXml = Get-DisabledTaskXml -Xml $snapshot.Xml
          Register-ScheduledTask -TaskName $snapshot.Name -Xml $disabledXml -Force | Out-Null
        } else {
          $created = Get-ScheduledTask -TaskName $snapshot.Name -ErrorAction SilentlyContinue
          if ($null -ne $created) {
            Unregister-ScheduledTask -TaskName $snapshot.Name -Confirm:$false
          }
        }
      } catch {
        $rollbackErrors += "$($snapshot.Name) restore: $($_.Exception.Message)"
      }
    }
  }
  # Phase 3: restore worker state while both watchdogs remain fenced. Windows permits a task to
  # be disabled while its current instance keeps running, so restore that state with the exact
  # enable -> start -> disable sequence.
  if ($rollbackErrors.Count -eq 0) {
    foreach ($snapshot in @($snapshots[0], $snapshots[1])) {
      if ($snapshot.Exists) {
        try {
          if ($snapshot.WasRunning) {
            Enable-ScheduledTask -TaskName $snapshot.Name | Out-Null
            Start-ScheduledTask -TaskName $snapshot.Name
            if (-not $snapshot.WasEnabled) {
              Disable-ScheduledTask -TaskName $snapshot.Name | Out-Null
            }
          } elseif ($snapshot.WasEnabled) {
            Enable-ScheduledTask -TaskName $snapshot.Name | Out-Null
          }
        } catch {
          $rollbackErrors += "$($snapshot.Name) state restore: $($_.Exception.Message)"
        }
      }
    }
  }
  # Phase 4: restore watchdog state last, after both compatible workers are in their prior state.
  if ($rollbackErrors.Count -eq 0) {
    foreach ($snapshot in @($snapshots[2], $snapshots[3])) {
      if ($snapshot.Exists) {
        try {
          if ($snapshot.WasRunning) {
            Enable-ScheduledTask -TaskName $snapshot.Name | Out-Null
            Start-ScheduledTask -TaskName $snapshot.Name
            if (-not $snapshot.WasEnabled) {
              Disable-ScheduledTask -TaskName $snapshot.Name | Out-Null
            }
          } elseif ($snapshot.WasEnabled) {
            Enable-ScheduledTask -TaskName $snapshot.Name | Out-Null
          }
        } catch {
          $rollbackErrors += "$($snapshot.Name) state restore: $($_.Exception.Message)"
        }
      }
    }
  }
  if ($rollbackErrors.Count -gt 0) {
    throw "Autopilot install failed ($($installError.Exception.Message)); rollback also failed: $($rollbackErrors -join '; ')"
  }
  throw "Autopilot install failed and prior tasks were restored: $($installError.Exception.Message)"
}

Get-ScheduledTask -TaskName @($TaskName, $WorkerTaskName, $SecTaskName, $SecWorkerTaskName)
} finally {
  if ($installMutexHeld) {
    $installMutex.ReleaseMutex()
  }
  $installMutex.Dispose()
}
