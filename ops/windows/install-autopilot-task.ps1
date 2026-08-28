param(
  [string]$TaskName = "Insider Alerts Autopilot Watchdog",
  [string]$WorkerTaskName = "Insider Alerts Autopilot Worker",
  [int]$RecoveryIntervalMinutes = 1,
  [int]$QuantTimeoutSeconds = 120,
  [int]$StaleHeartbeatSeconds = 0,
  [string]$AlphaRoot = "",
  [switch]$RunElevated,
  [switch]$Start
)

$ErrorActionPreference = "Stop"
$staleHeartbeatExplicit = $PSBoundParameters.ContainsKey("StaleHeartbeatSeconds")

if ([string]::IsNullOrWhiteSpace($TaskName) -or [string]::IsNullOrWhiteSpace($WorkerTaskName)) {
  throw "TaskName and WorkerTaskName must be nonempty."
}
if ([string]::Equals($TaskName, $WorkerTaskName, [System.StringComparison]::OrdinalIgnoreCase)) {
  throw "TaskName and WorkerTaskName must be distinct."
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

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir "..\..")
$pythonExe = Join-Path $repoRoot ".venv\Scripts\pythonw.exe"
$pythonConsoleExe = Join-Path $repoRoot ".venv\Scripts\python.exe"
$researchRoot = Join-Path $repoRoot "data\research"
New-Item -ItemType Directory -Path $researchRoot -Force | Out-Null
$chainStoreDb = Join-Path $researchRoot "option_chain_feed.db"
$heartbeatDb = Join-Path $repoRoot "data\autopilot_health.db"

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
} finally {
  Pop-Location
}

$user = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$workerAction = New-ScheduledTaskAction `
  -Execute $pythonExe `
  -Argument "-m insider_alerts.cli ops autopilot --loop --interval 300 --decision-engine quant --quant-agent-id quant-insider --quant-batch-size 8 --quant-thinking low --quant-timeout-seconds $QuantTimeoutSeconds --decision-limit 100 --notify --notify-approve-only --alpha-chain-python `"$alphaPython`" --alpha-chain-script `"$alphaChainScript`" --option-chain-store-db `"$chainStoreDb`" --heartbeat-db `"$heartbeatDb`" --heartbeat-stale-seconds $StaleHeartbeatSeconds --output-log logs/autopilot.out.log --error-log logs/autopilot.err.log" `
  -WorkingDirectory $repoRoot
$watchdogAction = New-ScheduledTaskAction `
  -Execute $pythonExe `
  -Argument "-m insider_alerts.cli ops autopilot-watchdog --worker-task-name `"$WorkerTaskName`" --heartbeat-db `"$heartbeatDb`" --stale-seconds $StaleHeartbeatSeconds --quant-timeout-seconds $QuantTimeoutSeconds --output-log logs/autopilot-watchdog.log" `
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

function Stop-TaskAndWait([string]$Name) {
  $task = Get-ScheduledTask -TaskName $Name -ErrorAction SilentlyContinue
  if ($null -eq $task) {
    return
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
    return [pscustomobject]@{ Name = $Name; Exists = $false; Xml = $null; WasRunning = $false }
  }
  return [pscustomobject]@{
    Name = $Name
    Exists = $true
    Xml = Export-ScheduledTask -TaskName $Name
    WasRunning = ($task.State -eq "Running")
  }
}

function Get-AutopilotHealth {
  Push-Location $repoRoot
  try {
    $raw = @(& $pythonConsoleExe -m insider_alerts.cli ops autopilot-health-status `
      --heartbeat-db $heartbeatDb 2>$null)
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

function Wait-ForFreshWorker([string]$PreviousRuntimeId) {
  $deadline = (Get-Date).AddSeconds(90)
  $stableRuntimeId = $null
  $stableSamples = 0
  do {
    $healthReport = Get-AutopilotHealth
    $workerTask = Get-ScheduledTask -TaskName $WorkerTaskName -ErrorAction SilentlyContinue
    if ($null -ne $healthReport -and $healthReport.valid -and $null -ne $healthReport.health) {
      $runtimeId = [string]$healthReport.health.runtime_id
      $progressText = [string]$healthReport.health.last_progress_utc
      $isNewRuntime = -not [string]::IsNullOrWhiteSpace($runtimeId) -and `
        ([string]::IsNullOrWhiteSpace($PreviousRuntimeId) -or $runtimeId -ne $PreviousRuntimeId)
      $progressIsFresh = $false
      try {
        $progressAge = [DateTimeOffset]::UtcNow - [DateTimeOffset]::Parse($progressText)
        $progressIsFresh = $progressAge.TotalSeconds -ge -5 -and $progressAge.TotalSeconds -le 30
      } catch {
        $progressIsFresh = $false
      }
      if ($isNewRuntime -and $progressIsFresh -and $null -ne $workerTask -and `
          $workerTask.State -eq "Running") {
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
  throw "Timed out waiting for a new, fresh, stably running autopilot worker."
}

$priorHealth = Get-AutopilotHealth
$previousRuntimeId = ""
if ($null -ne $priorHealth -and $priorHealth.valid -and $null -ne $priorHealth.health) {
  $previousRuntimeId = [string]$priorHealth.health.runtime_id
}

$snapshots = @(
  (Get-TaskSnapshot -Name $TaskName),
  (Get-TaskSnapshot -Name $WorkerTaskName)
)

try {
  # Stop the old same-named long-running definition before either replacement can run.
  Stop-TaskAndWait -Name $TaskName
  Stop-TaskAndWait -Name $WorkerTaskName

  Register-ScheduledTask `
    -TaskName $WorkerTaskName `
    -Action $workerAction `
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

  if ($Start) {
    Start-ScheduledTask -TaskName $TaskName
    Wait-ForFreshWorker -PreviousRuntimeId $previousRuntimeId
  }
} catch {
  $installError = $_
  $rollbackErrors = @()
  foreach ($snapshot in $snapshots) {
    try {
      Stop-TaskAndWait -Name $snapshot.Name
      if ($snapshot.Exists) {
        Register-ScheduledTask -TaskName $snapshot.Name -Xml $snapshot.Xml -Force | Out-Null
        if ($snapshot.WasRunning) {
          Start-ScheduledTask -TaskName $snapshot.Name
        }
      } else {
        $created = Get-ScheduledTask -TaskName $snapshot.Name -ErrorAction SilentlyContinue
        if ($null -ne $created) {
          Unregister-ScheduledTask -TaskName $snapshot.Name -Confirm:$false
        }
      }
    } catch {
      $rollbackErrors += "$($snapshot.Name): $($_.Exception.Message)"
    }
  }
  if ($rollbackErrors.Count -gt 0) {
    throw "Autopilot install failed ($($installError.Exception.Message)); rollback also failed: $($rollbackErrors -join '; ')"
  }
  throw "Autopilot install failed and prior tasks were restored: $($installError.Exception.Message)"
}

Get-ScheduledTask -TaskName @($TaskName, $WorkerTaskName)
