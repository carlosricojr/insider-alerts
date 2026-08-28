param(
  [string]$TaskName = "Insider Alerts Point-in-Time Features",
  [Parameter(Mandatory = $true)]
  [ValidatePattern('^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z$')]
  [string]$ActivationAtUtc,
  [int]$IntervalMinutes = 1,
  [switch]$Start
)

$ErrorActionPreference = "Stop"

if ($IntervalMinutes -lt 1) {
  throw "IntervalMinutes must be greater than or equal to 1."
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptDir "..\..")).Path
$researchRoot = [System.IO.Path]::GetFullPath((Join-Path $repoRoot "data\research"))
New-Item -ItemType Directory -Path $researchRoot -Force | Out-Null
$featureDatabase = [System.IO.Path]::GetFullPath(
  (Join-Path $researchRoot "feature_evidence.db")
)
$artifactRoot = [System.IO.Path]::GetFullPath(
  (Join-Path $researchRoot "artifacts\companyfacts")
)
$policyPath = [System.IO.Path]::GetFullPath(
  (Join-Path $repoRoot "docs\research\contracts\companyfacts-capture-v1.json")
)
$pythonExe = Join-Path $repoRoot ".venv\Scripts\pythonw.exe"
$validationPython = Join-Path $repoRoot ".venv\Scripts\python.exe"

foreach ($path in @($pythonExe, $validationPython, $policyPath)) {
  if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
    throw "Missing required feature-capture file at $path"
  }
}
foreach ($path in @($researchRoot, $artifactRoot, (Split-Path -Parent $featureDatabase))) {
  New-Item -ItemType Directory -Path $path -Force | Out-Null
  $item = Get-Item -LiteralPath $path
  if ($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
    throw "Feature capture path cannot be a reparse point: $path"
  }
}
$researchPrefix = $researchRoot.TrimEnd('\') + '\'
foreach ($target in @($featureDatabase, $artifactRoot)) {
  $cursor = if (Test-Path -LiteralPath $target -PathType Container) {
    $target
  } else {
    Split-Path -Parent $target
  }
  while ($true) {
    $cursorItem = Get-Item -LiteralPath $cursor
    if ($cursorItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
      throw "Feature capture parent cannot be a reparse point: $cursor"
    }
    if ($cursor.Equals($researchRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
      break
    }
    if (-not $cursor.StartsWith(
      $researchPrefix,
      [System.StringComparison]::OrdinalIgnoreCase
    )) {
      throw "Feature capture path escaped $researchRoot"
    }
    $parent = Split-Path -Parent $cursor
    if ($parent -eq $cursor) {
      throw "Unable to prove feature capture confinement for $target"
    }
    $cursor = $parent
  }
}

$activation = [DateTimeOffset]::Parse(
  $ActivationAtUtc,
  [System.Globalization.CultureInfo]::InvariantCulture,
  [System.Globalization.DateTimeStyles]::AssumeUniversal -bor
    [System.Globalization.DateTimeStyles]::AdjustToUniversal
).ToUniversalTime().ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'")

$existingTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($null -ne $existingTask) {
  $existingArguments = $existingTask.Actions[0].Arguments
  if ($existingArguments -match '--activation-at\s+([^\s]+)') {
    $installedActivation = [DateTimeOffset]::Parse($Matches[1]).ToUniversalTime()
    if ($installedActivation -ne [DateTimeOffset]::Parse($activation)) {
      throw "Refusing to change the installed feature-capture activation boundary."
    }
  } else {
    throw "Existing feature-capture task has no provable activation boundary."
  }
}

$arguments = @(
  "-m insider_alerts.research.feature_worker",
  "--feature-db `"$featureDatabase`"",
  "--artifact-root `"$artifactRoot`"",
  "--policy-path `"$policyPath`"",
  "--activation-at $activation",
  "--error-log `"$repoRoot\logs\feature-capture.err.log`""
) -join " "

$preflight = & $validationPython `
  -m insider_alerts.research.feature_worker `
  --feature-db $featureDatabase `
  --artifact-root $artifactRoot `
  --policy-path $policyPath `
  --activation-at $activation `
  --initialize-only 2>&1
if ($LASTEXITCODE -ne 0) {
  throw "Feature capture preflight failed: $preflight"
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
$principal = New-ScheduledTaskPrincipal `
  -UserId $user `
  -LogonType Interactive `
  -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet `
  -AllowStartIfOnBatteries `
  -DontStopIfGoingOnBatteries `
  -ExecutionTimeLimit (New-TimeSpan -Minutes 5) `
  -Hidden `
  -MultipleInstances IgnoreNew `
  -RestartCount 2 `
  -RestartInterval (New-TimeSpan -Minutes 1) `
  -StartWhenAvailable

Register-ScheduledTask `
  -TaskName $TaskName `
  -Action $action `
  -Trigger @($logonTrigger, $intervalTrigger) `
  -Principal $principal `
  -Settings $settings `
  -Force | Out-Null

if ($Start) {
  Start-ScheduledTask -TaskName $TaskName
}

Get-ScheduledTask -TaskName $TaskName
