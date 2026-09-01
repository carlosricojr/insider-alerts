function New-ResearchCaptureTaskActionSpec {
  param(
    [Parameter(Mandatory = $true)][string]$PythonExe,
    [Parameter(Mandatory = $true)][string]$RepoRoot,
    [Parameter(Mandatory = $true)][string]$ArtifactRoot,
    [Parameter(Mandatory = $true)][string]$AlphaPython,
    [Parameter(Mandatory = $true)][string]$AlphaScript,
    [Parameter(Mandatory = $true)][string]$AlphaHistoricalScript,
    [Parameter(Mandatory = $true)][string]$ChainStorePath,
    [Parameter(Mandatory = $true)][string]$PacingDatabasePath,
    [Parameter(Mandatory = $true)][string]$HistoryDatabase,
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{64}$')]
    [string]$HistorySnapshotSha256
  )

  $arguments = @(
    "-m insider_alerts.research.worker",
    "--artifact-root `"$ArtifactRoot`"",
    "--alpha-python `"$AlphaPython`"",
    "--alpha-script `"$AlphaScript`"",
    "--alpha-historical-script `"$AlphaHistoricalScript`"",
    "--option-chain-store-db `"$ChainStorePath`"",
    "--historical-pacing-db `"$PacingDatabasePath`"",
    "--history-db `"$HistoryDatabase`"",
    "--history-snapshot-sha256 $HistorySnapshotSha256",
    "--error-log `"$RepoRoot\logs\research-capture.err.log`""
  ) -join " "

  [pscustomobject]@{
    Execute = $PythonExe
    Argument = $arguments
    WorkingDirectory = $RepoRoot
  }
}
