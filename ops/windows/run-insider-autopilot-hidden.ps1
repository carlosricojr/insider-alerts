$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir "..\..")
$pythonExe = Join-Path $repoRoot ".venv\Scripts\pythonw.exe"

if (-not (Test-Path $pythonExe)) {
  throw "Missing virtualenv Python at $pythonExe"
}

$arguments = @(
  "-m", "insider_alerts.cli", "ops", "autopilot",
  "--loop", "--interval", "300",
  "--decision-engine", "quant",
  "--quant-agent-id", "quant-insider",
  "--quant-batch-size", "8",
  "--quant-thinking", "low",
  "--decision-limit", "100",
  "--notify", "--notify-approve-only",
  "--output-log", "logs/autopilot.out.log",
  "--error-log", "logs/autopilot.err.log"
)

$process = Start-Process -FilePath $pythonExe `
  -ArgumentList $arguments `
  -WorkingDirectory $repoRoot `
  -WindowStyle Hidden `
  -PassThru `
  -Wait

exit $process.ExitCode
