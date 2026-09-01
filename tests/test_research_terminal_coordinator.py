from __future__ import annotations

import json
import sqlite3
import subprocess
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import rfc8785

import insider_alerts.research.inference as inference
import insider_alerts.research.terminal_coordinator as coordinator
from insider_alerts.research.terminal_builder import TerminalBuildConfig
from insider_alerts.research.trial_runtime import TrialStore
from tests.test_research_terminal_builder import (
    ACTIVATED_AT,
    ROOT,
    _active_registry_path,
    _empty_external_stores,
    _install_trial,
)


def _config(tmp_path: Path, *, populated: bool) -> TerminalBuildConfig:
    trial = tmp_path / "trial.db"
    if populated:
        _install_trial(trial)
    else:
        TrialStore(trial).validate_integrity()
    diagnostics, canary, source = _empty_external_stores(tmp_path)
    return TerminalBuildConfig(
        trial_db=trial,
        diagnostics_db=diagnostics,
        canary_ledger_db=canary,
        source_db=source,
        registry_path=_active_registry_path(tmp_path),
        seal_db=tmp_path / "trial_seals.db",
        artifact_root=tmp_path / "artifacts",
        activation_db=tmp_path / "activation.db",
    )


def _main_args(
    config: TerminalBuildConfig, *, output: Path, error: Path
) -> list[str]:
    return [
        "--trial-db",
        str(config.trial_db),
        "--diagnostics-db",
        str(config.diagnostics_db),
        "--canary-ledger-db",
        str(config.canary_ledger_db),
        "--source-db",
        str(config.source_db),
        "--registry-path",
        str(config.registry_path),
        "--seal-db",
        str(config.seal_db),
        "--artifact-root",
        str(config.artifact_root),
        "--activation-db",
        str(config.activation_db),
        "--output-log",
        str(output),
        "--error-log",
        str(error),
    ]


def test_deadline_path_records_outcome_free_insufficient_enrollment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path, populated=False)
    monkeypatch.setattr(coordinator, "_transition_allowed", lambda _now: True)
    now = inference.enrollment_deadline(ACTIVATED_AT) + timedelta(minutes=1)

    result = coordinator.run_terminal_coordinator_once(config, now=now)
    replay = coordinator.run_terminal_coordinator_once(config, now=now + timedelta(days=1))
    store = inference.TrialSealStore(config.seal_db)
    report = store.existing_report()

    assert result.status == "decided"
    assert result.action == "deadline_decide"
    assert result.reason == "KILL"
    assert result.deadline_miss_receipt_sha256 is not None
    assert result.decision_report_sha256 is not None
    assert report is not None
    assert report["state"] == "KILL"
    assert report["reason_codes"] == ["insufficient_enrollment"]
    assert report["inference"] is None
    assert report["economic_metrics"] is None
    assert report["economic_gates"] is None
    assert replay.status == "decided"
    assert replay.action == "none"
    assert replay.decision_report_sha256 == result.decision_report_sha256


def test_ready_cohort_seals_then_decides_on_a_later_invocation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(inference, "TARGET_ENROLLED_TRADES", 2)
    monkeypatch.setattr(inference, "MINIMUM_DISTINCT_ENTRY_DATES", 2)
    monkeypatch.setattr(coordinator, "_transition_allowed", lambda _now: True)
    config = _config(tmp_path, populated=True)
    now = datetime(2026, 2, 10, 15, 0, tzinfo=UTC)

    sealed = coordinator.run_terminal_coordinator_once(config, now=now)
    store = inference.TrialSealStore(config.seal_db)
    assert sealed.status == "sealed"
    assert sealed.action == "seal"
    assert store.receipt("terminal_seal") is not None
    assert store.existing_report() is None

    decided = coordinator.run_terminal_coordinator_once(config, now=now + timedelta(days=1))
    report = store.existing_report()
    assert decided.status == "decided"
    assert decided.action == "decide"
    assert decided.reason in {"KILL", "PROMOTE_RECOMMENDED"}
    assert report is not None
    assert decided.decision_report_sha256 == report["report_sha256"]


def test_transition_is_deferred_outside_the_after_hours_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(inference, "TARGET_ENROLLED_TRADES", 2)
    monkeypatch.setattr(inference, "MINIMUM_DISTINCT_ENTRY_DATES", 2)
    config = _config(tmp_path, populated=True)

    result = coordinator.run_terminal_coordinator_once(
        config, now=datetime(2026, 2, 10, 15, 0, tzinfo=UTC)
    )

    assert result.status == "collecting"
    assert result.action == "none"
    assert result.reason == "transition_deferred_outside_after_hours_window"
    store = inference.TrialSealStore(config.seal_db)
    assert store.pending_terminal() is None
    assert store.receipt("terminal_seal") is None


def test_sqlite_contention_is_degraded_not_scientific_invalid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path, populated=False)
    monkeypatch.setattr(
        coordinator,
        "terminal_status",
        lambda _config: (_ for _ in ()).throw(sqlite3.OperationalError("database is locked")),
    )

    result = coordinator.run_terminal_coordinator_once(config)

    assert result.status == "degraded"
    assert result.reason == "sqlite_operational_error:database is locked"


def test_non_contention_sqlite_operational_error_is_persistent_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path, populated=False)
    monkeypatch.setattr(
        coordinator,
        "_validated_trial_window",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            sqlite3.OperationalError("no such table: trial_candidates")
        ),
    )

    result = coordinator.run_terminal_coordinator_once(config)

    assert result.status == "failed"
    assert result.reason == "sqlite_operational_error:no such table: trial_candidates"


@pytest.mark.parametrize(
    ("instant", "allowed"),
    [
        (datetime(2026, 1, 16, 1, 29, 59, tzinfo=UTC), False),
        (datetime(2026, 1, 16, 1, 30, 0, tzinfo=UTC), True),
        (datetime(2026, 1, 16, 4, 59, 59, 999999, tzinfo=UTC), True),
        (datetime(2026, 1, 16, 5, 0, 0, tzinfo=UTC), False),
        (datetime(2026, 7, 16, 0, 29, 59, tzinfo=UTC), False),
        (datetime(2026, 7, 16, 0, 30, 0, tzinfo=UTC), True),
        (datetime(2026, 7, 16, 3, 59, 59, 999999, tzinfo=UTC), True),
        (datetime(2026, 7, 16, 4, 0, 0, tzinfo=UTC), False),
    ],
)
def test_transition_window_uses_eastern_dst_and_exact_boundaries(
    instant: datetime, allowed: bool
) -> None:
    assert coordinator._transition_allowed(instant) is allowed


def test_naive_clock_is_invalid_without_touching_stores(tmp_path: Path) -> None:
    config = TerminalBuildConfig(
        trial_db=tmp_path / "trial.db",
        diagnostics_db=tmp_path / "diagnostics.db",
        canary_ledger_db=tmp_path / "canary.db",
        source_db=tmp_path / "source.db",
        registry_path=tmp_path / "registry.json",
        seal_db=tmp_path / "seals.db",
        artifact_root=tmp_path / "artifacts",
        activation_db=tmp_path / "activation.db",
    )

    result = coordinator.run_terminal_coordinator_once(config, now=datetime(2026, 1, 1))

    assert result.status == "invalid"
    assert result.reason == "terminal_coordinator_clock_naive"
    assert not config.seal_db.exists()


def test_safe_result_schema_cannot_emit_aggregate_outcomes() -> None:
    encoded = rfc8785.dumps(
        asdict(
            coordinator.TerminalCoordinatorResult(
                "decided",
                action="deadline_decide",
                reason="KILL",
                deadline_miss_receipt_sha256="a" * 64,
                decision_report_sha256="b" * 64,
            )
        )
    ).decode("utf-8")

    for forbidden in (
        "p_value",
        "confidence_interval",
        "gross_return",
        "spy_return",
        "profit_factor",
        "economic_gate",
        "mean_alpha",
    ):
        assert forbidden not in encoded


def test_main_treats_scientific_kill_as_operational_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "coordinator.log"
    error = tmp_path / "coordinator.err.log"
    monkeypatch.setattr(coordinator, "ensure_kill_on_close_process_tree", lambda: None)
    monkeypatch.setattr(coordinator, "_startup_validation_failure", lambda _config: None)
    monkeypatch.setattr(
        coordinator,
        "run_terminal_coordinator_once",
        lambda _config: coordinator.TerminalCoordinatorResult(
            "decided", action="deadline_decide", reason="KILL"
        ),
    )

    exit_code = coordinator.main(
        ["--output-log", str(output), "--error-log", str(error)]
    )

    assert exit_code == 0
    record = json.loads(output.read_text(encoding="utf-8"))
    assert record["status"] == "decided"
    assert record["reason"] == "KILL"
    assert not error.exists()


def test_main_logs_degradation_and_returns_retryable_exit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "coordinator.log"
    error = tmp_path / "coordinator.err.log"
    monkeypatch.setattr(coordinator, "ensure_kill_on_close_process_tree", lambda: None)
    monkeypatch.setattr(coordinator, "_startup_validation_failure", lambda _config: None)
    monkeypatch.setattr(
        coordinator,
        "run_terminal_coordinator_once",
        lambda _config: coordinator.TerminalCoordinatorResult(
            "degraded", reason="sqlite_operational_error:database is locked"
        ),
    )

    exit_code = coordinator.main(
        ["--output-log", str(output), "--error-log", str(error)]
    )

    assert exit_code == 2
    assert "database is locked" in error.read_text(encoding="utf-8")


@pytest.mark.parametrize(
    ("git_subcommand", "reason"),
    [
        ("show", "prospective_registry_invalid:activation_git_artifact_unverifiable"),
        ("merge-base", "prospective_registry_invalid:activation_git_commit_unverifiable"),
    ],
)
def test_startup_preflight_maps_real_git_timeouts_before_store_inspection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    git_subcommand: str,
    reason: str,
) -> None:
    config = _config(tmp_path, populated=False)
    real_run = subprocess.run
    faulted_commands: list[tuple[str, ...]] = []

    def timeout_on_command(args, *positional, **keywords):  # type: ignore[no-untyped-def]
        command = tuple(str(item) for item in args)
        if len(command) > 1 and command[0] == "git" and command[1] == git_subcommand:
            faulted_commands.append(command)
            raise subprocess.TimeoutExpired(command, timeout=5)
        return real_run(args, *positional, **keywords)

    monkeypatch.setattr(inference.subprocess, "run", timeout_on_command)

    result = coordinator._startup_validation_failure(config)

    assert result == coordinator.TerminalCoordinatorResult("invalid", reason=reason)
    assert len(faulted_commands) == 1
    assert not config.seal_db.exists()


def test_main_recovers_real_git_show_timeout_before_running_coordinator_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path, populated=False)
    output = tmp_path / "coordinator.log"
    error = tmp_path / "coordinator.err.log"
    real_subprocess_run = subprocess.run
    real_coordinator_run = coordinator.run_terminal_coordinator_once
    remaining_faults = 1
    body_calls = 0
    delays: list[float] = []

    def fail_first_show(args, *positional, **keywords):  # type: ignore[no-untyped-def]
        nonlocal remaining_faults
        command = tuple(str(item) for item in args)
        if command[:2] == ("git", "show") and remaining_faults:
            remaining_faults -= 1
            raise subprocess.TimeoutExpired(command, timeout=5)
        return real_subprocess_run(args, *positional, **keywords)

    def counted_body(
        body_config: TerminalBuildConfig,
    ) -> coordinator.TerminalCoordinatorResult:
        nonlocal body_calls
        body_calls += 1
        return real_coordinator_run(body_config)

    monkeypatch.setattr(inference.subprocess, "run", fail_first_show)
    monkeypatch.setattr(coordinator, "run_terminal_coordinator_once", counted_body)
    monkeypatch.setattr(coordinator, "ensure_kill_on_close_process_tree", lambda: None)
    monkeypatch.setattr(coordinator, "sleep", delays.append)

    exit_code = coordinator.main(_main_args(config, output=output, error=error))

    record = json.loads(output.read_text(encoding="utf-8"))
    assert exit_code == 0
    assert remaining_faults == 0
    assert delays == [coordinator.STARTUP_VALIDATION_RETRY_DELAYS_SECONDS[0]]
    assert body_calls == 1
    assert record["status"] == "collecting"
    assert not error.exists()


def test_repeated_real_git_unavailability_degrades_without_running_coordinator(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path, populated=False)
    output = tmp_path / "coordinator.log"
    error = tmp_path / "coordinator.err.log"
    real_subprocess_run = subprocess.run
    delays: list[float] = []
    timeout_count = 0

    def always_timeout(args, *positional, **keywords):  # type: ignore[no-untyped-def]
        nonlocal timeout_count
        command = tuple(str(item) for item in args)
        if command[:2] == ("git", "show"):
            timeout_count += 1
            raise subprocess.TimeoutExpired(command, timeout=5)
        return real_subprocess_run(args, *positional, **keywords)

    monkeypatch.setattr(inference.subprocess, "run", always_timeout)
    monkeypatch.setattr(
        coordinator,
        "run_terminal_coordinator_once",
        lambda _config: (_ for _ in ()).throw(AssertionError("coordinator body ran")),
    )
    monkeypatch.setattr(coordinator, "ensure_kill_on_close_process_tree", lambda: None)
    monkeypatch.setattr(coordinator, "sleep", delays.append)

    exit_code = coordinator.main(_main_args(config, output=output, error=error))

    record = json.loads(output.read_text(encoding="utf-8"))
    reason = "prospective_registry_invalid:activation_git_artifact_unverifiable"
    assert exit_code == 2
    assert timeout_count == 1 + len(coordinator.STARTUP_VALIDATION_RETRY_DELAYS_SECONDS)
    assert delays == list(coordinator.STARTUP_VALIDATION_RETRY_DELAYS_SECONDS)
    assert record["status"] == "degraded"
    assert record["action"] == "none"
    assert record["reason"] == f"startup_git_validation_unavailable:{reason}"
    assert "degraded: startup_git_validation_unavailable" in error.read_text(
        encoding="utf-8"
    )


def test_git_timeout_after_preflight_never_retries_coordinator_body(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path, populated=False)
    output = tmp_path / "coordinator.log"
    error = tmp_path / "coordinator.err.log"
    real_subprocess_run = subprocess.run
    real_coordinator_run = coordinator.run_terminal_coordinator_once
    body_active = False
    body_calls = 0

    def timeout_in_body(args, *positional, **keywords):  # type: ignore[no-untyped-def]
        command = tuple(str(item) for item in args)
        if body_active and command[:2] == ("git", "show"):
            raise subprocess.TimeoutExpired(command, timeout=5)
        return real_subprocess_run(args, *positional, **keywords)

    def marked_body(
        body_config: TerminalBuildConfig,
    ) -> coordinator.TerminalCoordinatorResult:
        nonlocal body_active, body_calls
        body_calls += 1
        body_active = True
        try:
            return real_coordinator_run(body_config)
        finally:
            body_active = False

    monkeypatch.setattr(inference.subprocess, "run", timeout_in_body)
    monkeypatch.setattr(coordinator, "run_terminal_coordinator_once", marked_body)
    monkeypatch.setattr(coordinator, "ensure_kill_on_close_process_tree", lambda: None)
    monkeypatch.setattr(
        coordinator,
        "sleep",
        lambda _seconds: (_ for _ in ()).throw(AssertionError("unexpected retry")),
    )

    exit_code = coordinator.main(_main_args(config, output=output, error=error))

    record = json.loads(output.read_text(encoding="utf-8"))
    assert exit_code == 3
    assert body_calls == 1
    assert record["status"] == "invalid"
    assert record["reason"] == (
        "prospective_registry_invalid:activation_git_artifact_unverifiable"
    )
    assert not config.seal_db.exists()


def test_git_timeout_during_decision_revalidation_preserves_single_seal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(inference, "TARGET_ENROLLED_TRADES", 2)
    monkeypatch.setattr(inference, "MINIMUM_DISTINCT_ENTRY_DATES", 2)
    monkeypatch.setattr(coordinator, "_transition_allowed", lambda _now: True)
    config = _config(tmp_path, populated=True)
    sealed = coordinator.run_terminal_coordinator_once(
        config, now=datetime(2026, 2, 10, 15, 0, tzinfo=UTC)
    )
    assert sealed.status == "sealed"
    store = inference.TrialSealStore(config.seal_db)
    receipt_before = store.receipt("terminal_seal")
    assert receipt_before is not None

    output = tmp_path / "coordinator.log"
    error = tmp_path / "coordinator.err.log"
    real_subprocess_run = subprocess.run
    real_decide = coordinator.decide_terminal_dataset
    decision_active = False
    decision_calls = 0

    def timeout_in_decision(args, *positional, **keywords):  # type: ignore[no-untyped-def]
        command = tuple(str(item) for item in args)
        if decision_active and command[:2] == ("git", "show"):
            raise subprocess.TimeoutExpired(command, timeout=5)
        return real_subprocess_run(args, *positional, **keywords)

    def marked_decide(*args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal decision_active, decision_calls
        decision_calls += 1
        decision_active = True
        try:
            return real_decide(*args, **kwargs)
        finally:
            decision_active = False

    monkeypatch.setattr(inference.subprocess, "run", timeout_in_decision)
    monkeypatch.setattr(coordinator, "decide_terminal_dataset", marked_decide)
    monkeypatch.setattr(coordinator, "ensure_kill_on_close_process_tree", lambda: None)
    monkeypatch.setattr(
        coordinator,
        "sleep",
        lambda _seconds: (_ for _ in ()).throw(AssertionError("unexpected retry")),
    )

    exit_code = coordinator.main(_main_args(config, output=output, error=error))

    record = json.loads(output.read_text(encoding="utf-8"))
    assert exit_code == 3
    assert decision_calls == 1
    assert record["status"] == "invalid"
    assert record["reason"] == (
        "prospective_registry_invalid:activation_git_artifact_unverifiable"
    )
    assert store.receipt("terminal_seal") == receipt_before
    assert store.existing_report() is None


def test_git_timeout_after_pending_stage_does_not_create_terminal_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(inference, "TARGET_ENROLLED_TRADES", 2)
    monkeypatch.setattr(inference, "MINIMUM_DISTINCT_ENTRY_DATES", 2)
    monkeypatch.setattr(coordinator, "_transition_allowed", lambda _now: True)
    config = _config(tmp_path, populated=True)
    store = inference.TrialSealStore(config.seal_db)
    real_subprocess_run = subprocess.run
    timeout_count = 0

    def timeout_after_pending(args, *positional, **keywords):  # type: ignore[no-untyped-def]
        nonlocal timeout_count
        command = tuple(str(item) for item in args)
        if command[:2] == ("git", "show") and store.pending_terminal() is not None:
            timeout_count += 1
            raise subprocess.TimeoutExpired(command, timeout=5)
        return real_subprocess_run(args, *positional, **keywords)

    monkeypatch.setattr(inference.subprocess, "run", timeout_after_pending)

    result = coordinator.run_terminal_coordinator_once(
        config, now=datetime(2026, 2, 10, 15, 0, tzinfo=UTC)
    )

    assert result.status == "invalid"
    assert result.action == "none"
    assert result.reason == (
        "terminal_preseal_validation_failed:activation_git_artifact_unverifiable"
    )
    assert timeout_count == 1
    assert store.pending_terminal() is not None
    assert store.receipt("terminal_seal") is None
    assert store.existing_report() is None


def test_git_timeout_after_deadline_receipt_preserves_blinded_partial_transition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path, populated=False)
    monkeypatch.setattr(coordinator, "_transition_allowed", lambda _now: True)
    store = inference.TrialSealStore(config.seal_db)
    real_subprocess_run = subprocess.run
    timeout_count = 0

    def timeout_after_receipt(args, *positional, **keywords):  # type: ignore[no-untyped-def]
        nonlocal timeout_count
        command = tuple(str(item) for item in args)
        if command[:2] == ("git", "show") and store.receipt("deadline_miss") is not None:
            timeout_count += 1
            raise subprocess.TimeoutExpired(command, timeout=5)
        return real_subprocess_run(args, *positional, **keywords)

    monkeypatch.setattr(inference.subprocess, "run", timeout_after_receipt)
    now = inference.enrollment_deadline(ACTIVATED_AT) + timedelta(minutes=1)

    result = coordinator.run_terminal_coordinator_once(config, now=now)

    receipt = store.receipt("deadline_miss")
    assert receipt is not None
    assert result.status == "invalid"
    assert result.action == "deadline_decide"
    assert result.reason == "activation_git_artifact_unverifiable"
    assert result.deadline_miss_receipt_sha256 == receipt["receipt_sha256"]
    assert timeout_count == 1
    assert store.existing_report() is None


def test_main_returns_persistent_failure_when_log_append_fails_after_seal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    error = tmp_path / "coordinator.err.log"
    monkeypatch.setattr(coordinator, "ensure_kill_on_close_process_tree", lambda: None)
    monkeypatch.setattr(coordinator, "_startup_validation_failure", lambda _config: None)
    monkeypatch.setattr(
        coordinator,
        "run_terminal_coordinator_once",
        lambda _config: coordinator.TerminalCoordinatorResult(
            "sealed", action="seal", terminal_seal_receipt_sha256="a" * 64
        ),
    )
    monkeypatch.setattr(
        coordinator,
        "_append_record",
        lambda *_args: (_ for _ in ()).throw(OSError("output disk unavailable")),
    )

    exit_code = coordinator.main(
        ["--output-log", str(tmp_path / "coordinator.log"), "--error-log", str(error)]
    )

    assert exit_code == 3
    assert "unexpected_OSError:output disk unavailable" in error.read_text(encoding="utf-8")


def test_activation_bound_artifacts_remain_byte_identical() -> None:
    registry = json.loads(
        (ROOT / "docs/research/registry/OPP-E07-V1.json").read_text(encoding="utf-8")
    )
    activation = registry["activation"]
    paths = {
        "inference_artifact_sha256": ROOT / "src/insider_alerts/research/inference.py",
        "terminal_builder_artifact_sha256": (
            ROOT / "src/insider_alerts/research/terminal_builder.py"
        ),
        "activation_artifact_sha256": ROOT / "src/insider_alerts/research/activation.py",
        "dependency_lock_sha256": ROOT / "uv.lock",
    }

    for field, path in paths.items():
        assert inference._file_sha256(path) == activation[field]


def test_worker_and_installer_are_order_incapable_hidden_and_singleton() -> None:
    module = (
        ROOT / "src/insider_alerts/research/terminal_coordinator.py"
    ).read_text(encoding="utf-8")
    installer = (
        ROOT / "ops/windows/install-research-terminal-coordinator-task.ps1"
    ).read_text(encoding="utf-8")

    assert "ib_async" not in module
    assert "placeOrder" not in module
    assert "ensure_kill_on_close_process_tree()" in module
    assert "pythonw.exe" in installer
    assert "-Hidden" in installer
    assert "-MultipleInstances IgnoreNew" in installer
    assert "-WindowStyle" not in installer
    assert "New-ScheduledTaskTrigger -Daily" in installer
    assert "New-ScheduledTaskTrigger -AtLogOn -User $user" in installer
    assert ".Date.AddHours(20).AddMinutes(30)" in installer
    assert '$localTimeZone.Id -ne "Eastern Standard Time"' in installer
    assert "ExecutionTimeLimit (New-TimeSpan -Minutes 60)" in installer
    assert "-LogonType S4U" in installer
    assert "-LogonType Interactive" in installer
    assert (
        '$_.FullyQualifiedErrorId -ne "HRESULT 0x80070005,Register-ScheduledTask"'
        in installer
    )
    assert "-Trigger @($dailyTrigger, $logonTrigger)" in installer
    assert 'RegistrationMode -NotePropertyValue $registrationMode' in installer
    assert installer.count("-ErrorAction Stop") == 2
    assert "-RestartCount" not in installer
    assert "-RestartInterval" not in installer

    trial_installer = (
        ROOT / "ops/windows/install-research-trial-task.ps1"
    ).read_text(encoding="utf-8")
    assert r'--seal-db `"$repoRoot\data\research\trial_seals.db`"' in trial_installer
