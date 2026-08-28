param(
  [string]$TaskName = "Insider Alerts Autopilot Watchdog",
  [string]$WorkerTaskName = "Insider Alerts Autopilot Worker",
  [int]$RecoveryIntervalMinutes = 1,
  [int]$QuantTimeoutSeconds = 120,
  [int]$StaleHeartbeatSeconds = 300,
  [string]$AlphaRoot = "",
  [switch]$RunElevated,
  [switch]$Start
)

$ErrorActionPreference = "Stop"

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
$minimumStaleHeartbeatSeconds = [Math]::Max(300, $QuantTimeoutSeconds + 90)
if ($StaleHeartbeatSeconds -lt $minimumStaleHeartbeatSeconds) {
  throw "StaleHeartbeatSeconds must be at least $minimumStaleHeartbeatSeconds."
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
  & $pythonConsoleExe -m insider_alerts.cli ops autopilot-config-validate `
    --quant-timeout-seconds $QuantTimeoutSeconds `
    --heartbeat-stale-seconds $StaleHeartbeatSeconds
  if ($LASTEXITCODE -ne 0) {
    throw "Autopilot watchdog preflight failed."
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
  -Argument "-m insider_alerts.cli ops autopilot-watchdog --worker-task-name `"$WorkerTaskName`" --heartbeat-db `"$heartbeatDb`" --stale-seconds $StaleHeartbeatSeconds --output-log logs/autopilot-watchdog.log" `
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
