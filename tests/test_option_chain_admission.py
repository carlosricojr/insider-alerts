from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from insider_alerts.research.capture import ProcessResult
from insider_alerts.research.option_chain_admission import (
    OptionChainAdmissionConfig,
    OptionChainAdmissionError,
    capture_predecision_option_chain,
    option_chain_admission_rows,
)

NOW = datetime(2026, 8, 28, 5, 30, tzinfo=UTC)


def _config(tmp_path: Path) -> OptionChainAdmissionConfig:
    repo = tmp_path / "insider"
    research = repo / "data" / "research"
    research.mkdir(parents=True)
    source_db = repo / "data" / "insider_alerts.db"
    source_db.touch()
    runtime = tmp_path / "alpha-runtime"
    script = runtime / "scripts" / "capture_insider_option_chain.py"
    python = runtime / ".venv" / "Scripts" / "python.exe"
    script.parent.mkdir(parents=True)
    python.parent.mkdir(parents=True)
    script.write_text("# capture boundary\n", encoding="utf-8")
    python.write_bytes(b"python-launcher")
    return OptionChainAdmissionConfig(
        source_db=source_db,
        repo_root=repo,
        chain_store_db=research / "option_chain_feed.db",
        alpha_python=python,
        alpha_script=script,
        timeout_seconds=15,
    )


def test_success_is_admitted_before_one_hidden_argv_launch(tmp_path: Path) -> None:
    config = _config(tmp_path)
    commands: list[tuple[list[str], Path, int]] = []

    def runner(command: list[str], *, cwd: Path, timeout_seconds: int) -> ProcessResult:
        rows = option_chain_admission_rows(config.source_db)
        assert len(rows) == 1 and rows[0]["status"] == "admitted"
        commands.append((command, cwd, timeout_seconds))
        return ProcessResult(0, '{"status":"captured"}', "", False)

    result = capture_predecision_option_chain(
        config,
        packet_id="packet-1",
        symbol="abc",
        clock=lambda: NOW,
        process_runner=runner,
    )

    assert result.status == "succeeded"
    assert result.batch_id.startswith("insider-")
    assert result.exit_code == 0
    assert commands == [
        (
            [
                str(config.alpha_python.resolve()),
                str(config.alpha_script.resolve()),
                "--store-db",
                str(config.chain_store_db.resolve()),
                "--symbol",
                "ABC",
                "--batch-id",
                result.batch_id,
            ],
            config.alpha_script.resolve().parents[1],
            15,
        )
    ]
    row = option_chain_admission_rows(config.source_db)[0]
    assert row["stdout_sha256"] is not None
    assert row["stderr_sha256"] is not None
    assert row["alpha_script_sha256"] is not None


def test_same_packet_never_relaunches_after_process_death(tmp_path: Path) -> None:
    config = _config(tmp_path)
    launches = 0

    def crash(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        nonlocal launches
        launches += 1
        raise RuntimeError("simulated process death")

    with pytest.raises(RuntimeError, match="simulated process death"):
        capture_predecision_option_chain(
            config,
            packet_id="packet-1",
            symbol="ABC",
            clock=lambda: NOW,
            process_runner=crash,
        )
    replay = capture_predecision_option_chain(
        config,
        packet_id="packet-1",
        symbol="ABC",
        clock=lambda: NOW + timedelta(seconds=1),
        process_runner=crash,
    )

    assert launches == 1
    assert replay.status == "admitted"
    assert replay.launch_required is False


def test_concurrent_same_symbol_launches_once_and_skips_without_extending_cadence(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    launches: list[str] = []

    def runner(command: list[str], **_kwargs) -> ProcessResult:  # type: ignore[no-untyped-def]
        launches.append(command[-1])
        return ProcessResult(0, "", "", False)

    def capture(packet_id: str, at: datetime):
        return capture_predecision_option_chain(
            config,
            packet_id=packet_id,
            symbol="ABC",
            clock=lambda: at,
            process_runner=runner,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        first, second = tuple(
            pool.map(lambda packet: capture(packet, NOW), ("packet-1", "packet-2"))
        )
    third = capture("packet-3", NOW + timedelta(seconds=899))
    fourth = capture("packet-4", NOW + timedelta(seconds=901))

    assert sorted((first.status, second.status)) == ["skipped_cadence", "succeeded"]
    assert third.status == "skipped_cadence"
    assert fourth.status == "succeeded"
    assert len(launches) == 2


def test_timeout_is_terminal_and_never_retried(tmp_path: Path) -> None:
    config = _config(tmp_path)
    launches = 0

    def timeout(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        nonlocal launches
        launches += 1
        return ProcessResult(-1, "partial", "timed out", True)

    first = capture_predecision_option_chain(
        config,
        packet_id="packet-1",
        symbol="ABC",
        clock=lambda: NOW,
        process_runner=timeout,
    )
    replay = capture_predecision_option_chain(
        config,
        packet_id="packet-1",
        symbol="ABC",
        clock=lambda: NOW + timedelta(minutes=20),
        process_runner=timeout,
    )

    assert launches == 1
    assert first.status == replay.status == "timed_out"
    assert first.error_kind == "CHILD_TIMEOUT"


def test_script_disappearing_after_child_return_is_terminal_and_never_retried(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    launches = 0

    def remove_script(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        nonlocal launches
        launches += 1
        config.alpha_script.unlink()
        return ProcessResult(0, "captured", "", False)

    first = capture_predecision_option_chain(
        config,
        packet_id="packet-1",
        symbol="ABC",
        clock=lambda: NOW,
        process_runner=remove_script,
    )

    assert first.status == "failed"
    assert first.error_kind == "SCRIPT_UNAVAILABLE_OR_CHANGED_DURING_CAPTURE"
    assert option_chain_admission_rows(config.source_db)[0]["status"] == "failed"
    config.alpha_script.write_text("# capture boundary\n", encoding="utf-8")
    replay = capture_predecision_option_chain(
        config,
        packet_id="packet-1",
        symbol="ABC",
        clock=lambda: NOW + timedelta(seconds=1),
        process_runner=remove_script,
    )
    assert replay.status == "failed"
    assert replay.launch_required is False
    assert launches == 1


def test_terminal_rows_and_identity_are_immutable(tmp_path: Path) -> None:
    config = _config(tmp_path)
    capture_predecision_option_chain(
        config,
        packet_id="packet-1",
        symbol="ABC",
        clock=lambda: NOW,
        process_runner=lambda *_args, **_kwargs: ProcessResult(2, "", "bad", False),
    )

    with sqlite3.connect(config.source_db) as conn:
        with pytest.raises(sqlite3.IntegrityError, match="terminal"):
            conn.execute(
                "UPDATE research_option_chain_admissions SET status='succeeded' "
                "WHERE packet_id='packet-1'"
            )
        with pytest.raises(sqlite3.IntegrityError, match="cannot be deleted"):
            conn.execute(
                "DELETE FROM research_option_chain_admissions WHERE packet_id='packet-1'"
            )


@pytest.mark.parametrize("field", ["alpha_script", "alpha_python", "chain_store_db"])
def test_runtime_paths_are_confined_before_admission(tmp_path: Path, field: str) -> None:
    config = _config(tmp_path)
    outside = tmp_path / "outside" / ("python.exe" if field == "alpha_python" else "file")
    outside.parent.mkdir(parents=True, exist_ok=True)
    outside.write_bytes(b"outside")
    invalid = replace(config, **{field: outside})

    with pytest.raises(OptionChainAdmissionError):
        capture_predecision_option_chain(
            invalid,
            packet_id="packet-1",
            symbol="ABC",
            clock=lambda: NOW,
            process_runner=lambda *_args, **_kwargs: pytest.fail("must not launch"),
        )
    with sqlite3.connect(config.source_db) as conn:
        table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='research_option_chain_admissions'"
        ).fetchone()
    assert table is None


def test_missing_expected_runtime_interpreter_is_a_domain_error(tmp_path: Path) -> None:
    config = _config(tmp_path)
    config.alpha_python.unlink()
    outside_python = tmp_path / "outside" / "python.exe"
    outside_python.parent.mkdir()
    outside_python.write_bytes(b"outside")

    with pytest.raises(OptionChainAdmissionError, match="interpreter is unavailable"):
        capture_predecision_option_chain(
            replace(config, alpha_python=outside_python),
            packet_id="packet-1",
            symbol="ABC",
            clock=lambda: NOW,
            process_runner=lambda *_args, **_kwargs: pytest.fail("must not launch"),
        )


def test_clock_regression_fails_without_second_launch(tmp_path: Path) -> None:
    config = _config(tmp_path)
    capture_predecision_option_chain(
        config,
        packet_id="packet-1",
        symbol="ABC",
        clock=lambda: NOW,
        process_runner=lambda *_args, **_kwargs: ProcessResult(0, "", "", False),
    )
    with pytest.raises(OptionChainAdmissionError, match="regressed"):
        capture_predecision_option_chain(
            config,
            packet_id="packet-2",
            symbol="XYZ",
            clock=lambda: NOW - timedelta(seconds=1),
            process_runner=lambda *_args, **_kwargs: pytest.fail("must not launch"),
        )


def test_finalization_clock_cannot_precede_admission(tmp_path: Path) -> None:
    config = _config(tmp_path)
    times = iter((NOW, NOW - timedelta(seconds=1)))

    with pytest.raises(OptionChainAdmissionError, match="finalization clock regressed"):
        capture_predecision_option_chain(
            config,
            packet_id="packet-1",
            symbol="ABC",
            clock=lambda: next(times),
            process_runner=lambda *_args, **_kwargs: ProcessResult(0, "", "", False),
        )
    assert option_chain_admission_rows(config.source_db)[0]["status"] == "admitted"


def test_autopilot_task_remains_windowless_and_enables_reviewed_chain_boundary() -> None:
    root = Path(__file__).resolve().parents[1]
    installer = (root / "ops" / "windows" / "install-autopilot-task.ps1").read_text(
        encoding="utf-8"
    )

    assert ".venv\\Scripts\\pythonw.exe" in installer
    assert "-Hidden" in installer
    assert "-MultipleInstances IgnoreNew" in installer
    assert "--alpha-chain-python" in installer
    assert "--alpha-chain-script" in installer
    assert "capture_insider_option_chain.py" in installer
    assert "--option-chain-store-db" in installer
    assert '"Insider Alerts Autopilot Worker"' in installer
    assert "--heartbeat-db" in installer
    assert "--heartbeat-stale-seconds $StaleHeartbeatSeconds" in installer
    assert "ops autopilot-watchdog" in installer
    assert "--worker-task-name `\"$WorkerTaskName`\"" in installer
    assert "-ExecutionTimeLimit (New-TimeSpan -Minutes 1)" in installer
    assert installer.count("New-ScheduledTaskTrigger -AtLogOn") == 1
    assert "$workerLogonTrigger" not in installer
    assert installer.index("Stop-TaskAndWait -Name $TaskName") < installer.index(
        "Register-ScheduledTask `"
    )
    assert installer.count("Register-ScheduledTask `") == 2
    assert 'New-ScheduledTaskAction `\n  -Execute $pythonExe' in installer
