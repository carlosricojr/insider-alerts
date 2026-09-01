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

function Assert-ResearchRuntimePath {
  param(
    [Parameter(Mandatory = $true)][string]$Path,
    [Parameter(Mandatory = $true)][string]$CheckoutRoot
  )

  $rootFull = [System.IO.Path]::GetFullPath($CheckoutRoot).TrimEnd('\')
  $pathFull = [System.IO.Path]::GetFullPath($Path)
  $rootPrefix = $rootFull + '\'
  if (-not $pathFull.StartsWith(
    $rootPrefix,
    [System.StringComparison]::OrdinalIgnoreCase
  )) {
    throw "Research runtime path escaped its configured checkout: $pathFull"
  }

  $cursor = $pathFull
  $isLeaf = $true
  while ($true) {
    if (-not (Test-Path -LiteralPath $cursor)) {
      throw "Research runtime path is unavailable: $cursor"
    }
    $item = Get-Item -LiteralPath $cursor -Force
    if ($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
      throw "Research runtime path cannot contain a reparse point: $cursor"
    }
    if ($isLeaf -and $item.PSIsContainer) {
      throw "Research runtime executable or script must be a regular file: $pathFull"
    }
    if (-not $isLeaf -and -not $item.PSIsContainer) {
      throw "Research runtime ancestor must be a regular directory: $cursor"
    }
    if ($cursor.Equals($rootFull, [System.StringComparison]::OrdinalIgnoreCase)) {
      break
    }
    $parent = Split-Path -Parent $cursor
    if ($parent -eq $cursor -or -not $parent.StartsWith(
      $rootFull,
      [System.StringComparison]::OrdinalIgnoreCase
    )) {
      throw "Unable to prove research runtime confinement for $pathFull"
    }
    $cursor = $parent
    $isLeaf = $false
  }
}
