param([switch]$Start)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$pythonExe = Join-Path $repoRoot ".venv\Scripts\pythonw.exe"
$taskName = "Insider Alerts E07 Volatility Rehearsal"
$module = "-m ops.research.e07_volatility_rehearsal"
if (-not (Test-Path -LiteralPath $pythonExe -PathType Leaf)) { throw "Missing pythonw.exe." }
$canary = Get-ScheduledTask -TaskName "Insider Alerts Live Canary Worker"
if (@($canary.Actions).Count -ne 1) { throw "Unexpected live task action count." }
$action = $canary.Actions[0]
if ((Resolve-Path -LiteralPath $action.WorkingDirectory).Path -ne $repoRoot -or
    (Resolve-Path -LiteralPath $action.Execute).Path -ne $pythonExe -or
    $action.Arguments -notmatch '^\s*-m\s+insider_alerts\.cli\s+ops\s+live-canary\b' -or
    $action.Arguments -match '(?i)--ledger-path(?:\s|=)' -or
    $action.Arguments -match '(?i)--database-path(?:\s|=)' -or
    $action.Arguments -match '(?i)--client-id(?:\s+|=)\s*179(?:\s|$)') {
    throw "Research must use the verified live task checkout and default ledger and approval database."
}
if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
    throw "Rehearsal task already exists."
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
    if ($LASTEXITCODE -ne 0 -or $branch -ne "main" -or $head -ne $remote -or $dirty) {
        throw "Deployment checkout must be clean main == origin/main."
    }
} finally { Pop-Location }
$check = Start-Process -FilePath $pythonExe -WindowStyle Hidden -Wait -PassThru `
    -WorkingDirectory $repoRoot -ArgumentList "$module --verify-installation"
if ($check.ExitCode -ne 0) { throw "Rehearsal activation or broker exclusivity verification failed." }

$user = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$action = New-ScheduledTaskAction -Execute $pythonExe -Argument "$module --cycle" -WorkingDirectory $repoRoot
$logon = New-ScheduledTaskTrigger -AtLogOn -User $user
$interval = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) `
    -RepetitionInterval (New-TimeSpan -Minutes 1) -RepetitionDuration (New-TimeSpan -Days 3650)
$principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -Hidden -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 2) -StartWhenAvailable `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
Register-ScheduledTask -TaskName $taskName -Action $action -Trigger @($logon, $interval) `
    -Principal $principal -Settings $settings | Out-Null
if ($Start) { Start-ScheduledTask -TaskName $taskName }
Get-ScheduledTask -TaskName $taskName
