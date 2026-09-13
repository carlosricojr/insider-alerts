param([switch]$Start)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$pythonExe = Join-Path $repoRoot ".venv\Scripts\pythonw.exe"
$taskName = "Insider Alerts Operational Observer"
if (-not (Test-Path -LiteralPath $pythonExe -PathType Leaf)) { throw "Missing pythonw.exe" }
if (-not (Test-Path -LiteralPath (Join-Path $repoRoot "data\observer\evidence.db") -PathType Leaf)) {
  throw "Seal observer activation before installing the task."
}
$canary = Get-ScheduledTask -TaskName "Insider Alerts Live Canary Worker"
if ((Resolve-Path $canary.Actions.WorkingDirectory).Path -ne $repoRoot) {
  throw "Observer must use the live task's deployment checkout."
}
Push-Location $repoRoot
try {
  git fetch --quiet origin main
  if ($LASTEXITCODE -ne 0) { throw "Could not refresh origin/main; installation refused." }
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
$existing = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if ($existing) { throw "Observer task already exists; inspect it before changing registration." }
$user = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$action = New-ScheduledTaskAction -Execute $pythonExe `
  -Argument "-m insider_alerts.execution.observer_worker" -WorkingDirectory $repoRoot
$logon = New-ScheduledTaskTrigger -AtLogOn -User $user
$interval = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) `
  -RepetitionInterval (New-TimeSpan -Minutes 5) -RepetitionDuration (New-TimeSpan -Days 3650)
$principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -Hidden -MultipleInstances IgnoreNew `
  -ExecutionTimeLimit (New-TimeSpan -Minutes 3) -StartWhenAvailable `
  -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
Register-ScheduledTask -TaskName $taskName -Action $action -Trigger @($logon, $interval) `
  -Principal $principal -Settings $settings | Out-Null
if ($Start) { Start-ScheduledTask -TaskName $taskName }
Get-ScheduledTask -TaskName $taskName
