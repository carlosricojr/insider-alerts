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

if ($RecoveryIntervalMinutes -lt 1) {
  throw "RecoveryIntervalMinutes must be greater than or equal to 1."
}
if ($QuantTimeoutSeconds -lt 10 -or $QuantTimeoutSeconds -gt 900) {
  throw "QuantTimeoutSeconds must be between 10 and 900."
}
$minimumStaleHeartbeatSeconds = [Math]::Max(300, $QuantTimeoutSeconds + 70)
if ($StaleHeartbeatSeconds -lt $minimumStaleHeartbeatSeconds) {
  throw "StaleHeartbeatSeconds must be at least $minimumStaleHeartbeatSeconds."
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir "..\..")
$pythonExe = Join-Path $repoRoot ".venv\Scripts\pythonw.exe"
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

if (-not (Test-Path -LiteralPath $alphaPython -PathType Leaf)) {
  throw "Missing alpha runtime interpreter at $alphaPython"
}

if (-not (Test-Path -LiteralPath $alphaChainScript -PathType Leaf)) {
  throw "Missing alpha option-chain entrypoint at $alphaChainScript"
}

if (-not (Test-Path (Join-Path $repoRoot ".env"))) {
  throw "Missing .env at $repoRoot\.env"
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

Get-ScheduledTask -TaskName @($TaskName, $WorkerTaskName)
