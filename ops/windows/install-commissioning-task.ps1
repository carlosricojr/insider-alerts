param([switch]$Start)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$pythonExe = Join-Path $repoRoot ".venv\Scripts\pythonw.exe"
$taskName = "Insider Alerts Research Commissioning V2"
if (-not (Test-Path -LiteralPath $pythonExe -PathType Leaf)) { throw "Missing pythonw.exe" }
if (-not (Test-Path -LiteralPath (Join-Path $repoRoot "data\research-commissioning-v2\evidence.db") -PathType Leaf)) {
  throw "Explicitly seal a future commissioning activation before installing."
}
$canary = Get-ScheduledTask -TaskName "Insider Alerts Live Canary Worker"
if (@($canary.Actions).Count -ne 1) { throw "Unexpected live task action count." }
$canaryAction = $canary.Actions[0]
if ((Resolve-Path -LiteralPath $canaryAction.WorkingDirectory).Path -ne $repoRoot -or
    (Resolve-Path -LiteralPath $canaryAction.Execute).Path -ne $pythonExe -or
    $canaryAction.Arguments -notmatch '^\s*-m\s+insider_alerts\.cli\s+ops\s+live-canary\b') {
  throw "Commissioning must use the verified live task deployment checkout."
}
Push-Location $repoRoot
try {
  git fetch --quiet origin main
  if ($LASTEXITCODE -ne 0) { throw "Could not refresh origin/main." }
  $branch = git branch --show-current
  if ($LASTEXITCODE -ne 0) { throw "Could not read checkout branch." }
  $head = git rev-parse HEAD
  if ($LASTEXITCODE -ne 0) { throw "Could not read checkout revision." }
  $remote = git rev-parse origin/main
  if ($LASTEXITCODE -ne 0) { throw "Could not read refreshed origin/main." }
  $dirty = git status --porcelain
  if ($LASTEXITCODE -ne 0) { throw "Could not verify clean checkout." }
  if ($branch -ne "main" -or $head -ne $remote -or $dirty) {
    throw "Deployment checkout must be clean main == origin/main."
  }
} finally { Pop-Location }
if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
  throw "Commissioning task already exists; registration will not overwrite it."
}
$verification = Start-Process -FilePath $pythonExe -WindowStyle Hidden -Wait -PassThru `
  -WorkingDirectory $repoRoot -ArgumentList "-m insider_alerts.research.commissioning_worker --verify-installation"
if ($verification.ExitCode -ne 0) { throw "Commissioning implementation/source/window verification failed." }
$user = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$action = New-ScheduledTaskAction -Execute $pythonExe `
  -Argument "-m insider_alerts.research.commissioning_worker" -WorkingDirectory $repoRoot
$logon = New-ScheduledTaskTrigger -AtLogOn -User $user
$interval = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) `
  -RepetitionInterval (New-TimeSpan -Minutes 1) -RepetitionDuration (New-TimeSpan -Days 3650)
$principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -Hidden -MultipleInstances Parallel `
  -ExecutionTimeLimit (New-TimeSpan -Minutes 2) -StartWhenAvailable `
  -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
Register-ScheduledTask -TaskName $taskName -Action $action -Trigger @($logon, $interval) `
  -Principal $principal -Settings $settings | Out-Null
if ($Start) { Start-ScheduledTask -TaskName $taskName }
Get-ScheduledTask -TaskName $taskName
