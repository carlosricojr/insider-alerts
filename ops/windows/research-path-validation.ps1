function Initialize-ResearchDatabaseParent {
  param(
    [Parameter(Mandatory = $true)][string]$DatabasePath,
    [Parameter(Mandatory = $true)][string]$ResearchRoot
  )

  $researchRootFull = [System.IO.Path]::GetFullPath($ResearchRoot).TrimEnd('\')
  $researchPrefix = $researchRootFull + '\'
  $databaseFull = [System.IO.Path]::GetFullPath($DatabasePath)
  if (-not $databaseFull.StartsWith(
    $researchPrefix,
    [System.StringComparison]::OrdinalIgnoreCase
  )) {
    throw "Research option databases must remain beneath $researchRootFull"
  }

  $databaseParent = Split-Path -Parent $databaseFull
  $cursor = $databaseParent
  while (-not (Test-Path -LiteralPath $cursor)) {
    if (-not $cursor.StartsWith(
      $researchPrefix,
      [System.StringComparison]::OrdinalIgnoreCase
    )) {
      throw "Research option database parent escaped $researchRootFull"
    }
    $parent = Split-Path -Parent $cursor
    if ($parent -eq $cursor) {
      throw "Unable to prove research option database confinement for $databaseFull"
    }
    $cursor = $parent
  }

  while ($true) {
    $cursorItem = Get-Item -LiteralPath $cursor
    if ($cursorItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
      throw "Research option database parent cannot be a reparse point: $cursor"
    }
    if ($cursor.Equals($researchRootFull, [System.StringComparison]::OrdinalIgnoreCase)) {
      break
    }
    if (-not $cursor.StartsWith(
      $researchPrefix,
      [System.StringComparison]::OrdinalIgnoreCase
    )) {
      throw "Research option database parent escaped $researchRootFull"
    }
    $parent = Split-Path -Parent $cursor
    if ($parent -eq $cursor) {
      throw "Unable to prove research option database confinement for $databaseFull"
    }
    $cursor = $parent
  }

  New-Item -ItemType Directory -Path $databaseParent -Force | Out-Null
  $cursor = $databaseParent
  while ($true) {
    $cursorItem = Get-Item -LiteralPath $cursor
    if ($cursorItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
      throw "Research option database parent cannot be a reparse point: $cursor"
    }
    if ($cursor.Equals($researchRootFull, [System.StringComparison]::OrdinalIgnoreCase)) {
      break
    }
    if (-not $cursor.StartsWith(
      $researchPrefix,
      [System.StringComparison]::OrdinalIgnoreCase
    )) {
      throw "Research option database parent escaped $researchRootFull"
    }
    $cursor = Split-Path -Parent $cursor
  }

  if ((Test-Path -LiteralPath $databaseFull) -and
      ((Get-Item -LiteralPath $databaseFull).Attributes -band
       [System.IO.FileAttributes]::ReparsePoint)) {
    throw "Research option database cannot be a reparse point: $databaseFull"
  }
}
