from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.skipif(os.name != "nt", reason="Windows Task Scheduler installation boundary")
@pytest.mark.parametrize("failure", ["none", "branch", "existing", "verification", "canary_args"])
def test_installer_checks_deployment_and_registers_only_hidden_bounded_task(tmp_path, failure):
    installer = tmp_path / "ops/windows/install-commissioning-task.ps1"
    installer.parent.mkdir(parents=True)
    shutil.copyfile(ROOT / "ops/windows/install-commissioning-task.ps1", installer)
    pythonw = tmp_path / ".venv/Scripts/pythonw.exe"
    pythonw.parent.mkdir(parents=True)
    pythonw.touch()  # Start-Process is mocked below; never execute this placeholder.
    evidence = tmp_path / "data/research-commissioning-v2/evidence.db"
    evidence.parent.mkdir(parents=True)
    evidence.touch()
    # Exercise the real installer with only OS/process/git effects replaced.
    # No real scheduled task or subprocess can be registered/launched here.
    script = r"""
$ErrorActionPreference = 'Stop'
$global:commissioningTestRoot = '__ROOT__'
$global:commissioningTestFailure = '__FAILURE__'
$global:commissioningTestRegistered = $null
$global:commissioningTestVerified = $false
$global:commissioningTestStarted = $false
function git {
  $global:LASTEXITCODE = 0
  if ($args[0] -eq 'branch') {
    if ($global:commissioningTestFailure -eq 'branch') { 'research' } else { 'main' }
  } elseif ($args[0] -eq 'rev-parse') { 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' }
}
function Get-ScheduledTask {
  param($TaskName, $ErrorAction)
  if ($TaskName -eq 'Insider Alerts Live Canary Worker') {
    $arguments = '-m insider_alerts.cli ops live-canary --loop'
    if ($global:commissioningTestFailure -eq 'canary_args') { $arguments = '-m other.worker' }
    [pscustomobject]@{Actions=@([pscustomobject]@{
      Execute=(Join-Path $global:commissioningTestRoot '.venv\Scripts\pythonw.exe')
      Arguments=$arguments; WorkingDirectory=$global:commissioningTestRoot
    })}
  } elseif ($global:commissioningTestFailure -eq 'existing') { [pscustomobject]@{Foreign=$true} }
  elseif ($global:commissioningTestRegistered) { $global:commissioningTestRegistered }
}
function Start-Process {
  param($FilePath, $WindowStyle, [switch]$Wait, [switch]$PassThru, $WorkingDirectory, $ArgumentList)
  if ($WindowStyle -ne 'Hidden' -or -not $Wait -or -not $PassThru -or
      $FilePath -notlike '*pythonw.exe' -or $WorkingDirectory -ne $global:commissioningTestRoot -or
      $ArgumentList -ne '-m insider_alerts.research.commissioning_worker --verify-installation') {
    throw 'Unsafe installation verification process'
  }
  $global:commissioningTestVerified = $true
  $code = if ($global:commissioningTestFailure -eq 'verification') {2} else {0}
  [pscustomobject]@{ExitCode=$code}
}
function New-ScheduledTaskAction {
  param($Execute, $Argument, $WorkingDirectory)
  [pscustomobject]@{Execute=$Execute; Arguments=$Argument; WorkingDirectory=$WorkingDirectory}
}
function New-ScheduledTaskTrigger {
  param([switch]$AtLogOn, $User, [switch]$Once, $At, $RepetitionInterval, $RepetitionDuration)
  [pscustomobject]@{AtLogOn=$AtLogOn.IsPresent; Seconds=$RepetitionInterval.TotalSeconds}
}
function New-ScheduledTaskPrincipal {
  param($UserId, $LogonType, $RunLevel)
  [pscustomobject]@{LogonType=$LogonType; RunLevel=$RunLevel}
}
function New-ScheduledTaskSettingsSet {
  param([switch]$Hidden, $MultipleInstances, $ExecutionTimeLimit, [switch]$StartWhenAvailable,
        [switch]$AllowStartIfOnBatteries, [switch]$DontStopIfGoingOnBatteries)
  [pscustomobject]@{
    Hidden=$Hidden.IsPresent; MultipleInstances=$MultipleInstances
    Seconds=$ExecutionTimeLimit.TotalSeconds
  }
}
function Register-ScheduledTask {
  param($TaskName, $Action, $Trigger, $Principal, $Settings)
  if (-not $global:commissioningTestVerified) { throw 'Registration without verification' }
  $global:commissioningTestRegistered = [pscustomobject]@{
    TaskName=$TaskName; Action=$Action; Trigger=$Trigger; Principal=$Principal; Settings=$Settings
  }
}
function Start-ScheduledTask {
  param($TaskName)
  $global:commissioningTestStarted = $true
}
$problem = $null
try {
  & (Join-Path $global:commissioningTestRoot 'ops\windows\install-commissioning-task.ps1') `
    -Start | Out-Null
}
catch { $problem = $_.Exception.Message }
[pscustomobject]@{
  Error=$problem; Registered=$global:commissioningTestRegistered
  Verified=$global:commissioningTestVerified; Started=$global:commissioningTestStarted
} | ConvertTo-Json -Depth 8 -Compress
""".replace("__ROOT__", str(tmp_path).replace("'", "''")).replace("__FAILURE__", failure)
    result = subprocess.run(
        ["powershell", "-NoProfile", "-NonInteractive", "-Command", script],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
        creationflags=subprocess.CREATE_NO_WINDOW,
    )
    observed = json.loads(result.stdout)
    if failure != "none":
        assert observed["Error"] and observed["Registered"] is None and not observed["Started"]
        assert observed["Verified"] == (failure == "verification")
        return
    assert not observed["Error"] and observed["Verified"] and observed["Started"]
    registered = observed["Registered"]
    assert registered["TaskName"] == "Insider Alerts Research Commissioning V2"
    assert registered["Action"]["Execute"] == str(pythonw)
    assert registered["Action"]["Arguments"] == "-m insider_alerts.research.commissioning_worker"
    assert registered["Settings"] == {
        "Hidden": True,
        "MultipleInstances": "Parallel",
        "Seconds": 120,
    }
    assert registered["Principal"] == {"LogonType": "Interactive", "RunLevel": "Limited"}
    assert registered["Trigger"][1]["Seconds"] == 60
