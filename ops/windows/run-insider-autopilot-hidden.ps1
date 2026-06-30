$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir "..\..")
$launcher = Join-Path $scriptDir "run-insider-autopilot.cmd"

$process = Start-Process -FilePath "cmd.exe" `
  -ArgumentList "/c `"$launcher`"" `
  -WorkingDirectory $repoRoot `
  -WindowStyle Hidden `
  -PassThru `
  -Wait

exit $process.ExitCode
