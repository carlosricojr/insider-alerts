"""Synthetic metadata guards only; never run a Trial, terminal or inference."""

import os
import subprocess
from pathlib import Path

import pytest

from insider_alerts.research import commissioning_worker as commissioning
from insider_alerts.research import inference

ROOT = Path(__file__).resolve().parents[1]


def selection_root(path: Path) -> Path:
    path.mkdir(parents=True)
    (path / ".git").write_text("gitdir: absent-synthetic-authority\n", encoding="utf-8")
    (path / "uv.lock").write_bytes(b"synthetic-lock\n")
    schema = path / "docs/research/contracts/hypothesis-registry.schema.json"
    schema.parent.mkdir(parents=True)
    schema.write_text("{}", encoding="utf-8")
    return path


def test_cwd_selection_does_not_establish_git_or_module_authority(tmp_path, monkeypatch):
    cwd = selection_root(tmp_path / "cwd")
    module = selection_root(tmp_path / "module")
    monkeypatch.chdir(cwd)
    monkeypatch.setattr(inference, "__file__", str(module / "src/research/inference.py"))
    assert inference._reviewed_repo_root() == cwd
    # Existence of .git selects a root but cannot admit an activation artifact.
    for key in tuple(os.environ):
        if key.startswith("GIT_"):
            monkeypatch.delenv(key)
    with pytest.raises(inference.TrialInvalid, match="activation_git_artifact_missing"):
        inference._git_blob(cwd, "0" * 40, Path("uv.lock"))


def test_incomplete_cwd_falls_back_to_module_selection(tmp_path, monkeypatch):
    cwd = tmp_path / "incomplete"
    cwd.mkdir()
    module = selection_root(tmp_path / "module")
    monkeypatch.chdir(cwd)
    monkeypatch.setattr(inference, "__file__", str(module / "src/research/inference.py"))
    assert inference._reviewed_repo_root() == module


def test_lock_line_endings_bind_registry_and_commissioning_differently(tmp_path):
    # Only this disposable synthetic root is modified; no sealed registry/lock.
    package = tmp_path / "src/insider_alerts"
    package.mkdir(parents=True)
    (package / "synthetic.py").write_bytes(b"value = 1\n")
    lock = tmp_path / "uv.lock"
    lock.write_bytes(b"synthetic = true\n")
    (tmp_path / "pyproject.toml").write_bytes(b"[project]\nname = 'synthetic'\n")
    relative = Path("docs/research/calendar-sources/nyse-hours-2026-10-26.html")
    archive = tmp_path / relative
    archive.parent.mkdir(parents=True)
    archive.write_bytes((ROOT / relative).read_bytes())
    registry_digest = inference._file_sha256(lock)
    commissioning_digest = commissioning.implementation_digest(tmp_path)

    lock.write_bytes(b"synthetic = true\r\n")
    assert inference._file_sha256(lock) == registry_digest
    assert commissioning.implementation_digest(tmp_path) != commissioning_digest

    lock.write_bytes(b"synthetic = false\n")
    assert inference._file_sha256(lock) != registry_digest
    assert commissioning.implementation_digest(tmp_path) != commissioning_digest


def test_missing_bound_file_is_not_admitted(tmp_path):
    with pytest.raises(inference.TrialInvalid, match="activation_artifact_missing"):
        inference._file_sha256(tmp_path / "absent-lock")


@pytest.mark.skipif(os.name != "nt", reason="actual Windows path helper")
@pytest.mark.parametrize("case", ("inside", "sibling", "missing", "directory"))
def test_existing_windows_helper_preserves_runtime_confinement(tmp_path, case):
    checkout = tmp_path / "checkout"
    inside = checkout / "runtime/python.exe"
    inside.parent.mkdir(parents=True)
    inside.write_bytes(b"synthetic, never executed")
    sibling = tmp_path / "checkout-other/python.exe"
    sibling.parent.mkdir()
    sibling.write_bytes(b"synthetic, never executed")
    paths = {
        "inside": inside,
        "sibling": sibling,
        "missing": inside.with_name("missing.exe"),
        "directory": inside.parent,
    }
    environment = os.environ.copy()
    environment.update(
        PATH_HELPER=str(ROOT / "ops/windows/research-path-validation.ps1"),
        CHECKOUT_ROOT=str(checkout),
        SYNTHETIC_RUNTIME=str(paths[case]),
    )
    script = (
        "$ErrorActionPreference='Stop'; . $env:PATH_HELPER; "
        "try { Assert-ResearchRuntimePath -Path $env:SYNTHETIC_RUNTIME "
        "-CheckoutRoot $env:CHECKOUT_ROOT; [Console]::WriteLine('accepted'); exit 0 } "
        "catch { [Console]::WriteLine('rejected'); exit 3 }"
    )
    completed = subprocess.run(
        ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", script],
        env=environment,
        capture_output=True,
        text=True,
        timeout=10,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    assert completed.returncode == (0 if case == "inside" else 3)
    assert completed.stdout.strip() == ("accepted" if case == "inside" else "rejected")
