"""Blinded, order-incapable coordinator for the OPP-E07 terminal transitions."""

from __future__ import annotations

import argparse
import contextlib
import json
import sqlite3
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, time
from pathlib import Path
from time import sleep
from typing import Literal
from zoneinfo import ZoneInfo

import rfc8785

from insider_alerts.execution.windows_job import ensure_kill_on_close_process_tree
from insider_alerts.research.inference import TrialInvalid, TrialSealStore, evaluate_with_store
from insider_alerts.research.terminal_builder import (
    TerminalBuildConfig,
    TerminalBuildInvalid,
    TerminalBuildNotReady,
    TerminalBuildResult,
    _terminal_payload,
    _trial_snapshot,
    decide_terminal_dataset,
    seal_terminal_dataset,
    terminal_status,
)
from insider_alerts.research.trial_runtime import (
    TrialRuntimeConfig,
    TrialRuntimeInvalid,
    TrialStore,
    _validated_trial_window,
)

NEW_YORK = ZoneInfo("America/New_York")
TRANSITION_WINDOW_START_ET = time(20, 30)
TRANSITION_WINDOW_END_ET = time(23, 59, 59, 999999)
STARTUP_VALIDATION_RETRY_DELAYS_SECONDS = (5.0, 15.0, 30.0)
_RETRYABLE_STARTUP_VALIDATION_REASONS = frozenset(
    {
        "prospective_registry_invalid:activation_git_artifact_unverifiable",
        "prospective_registry_invalid:activation_git_commit_unverifiable",
    }
)


@dataclass(frozen=True, slots=True)
class TerminalCoordinatorResult:
    """Outcome-safe state emitted by the coordinator.

    The record deliberately has no return, p-value, confidence-interval, or gate fields.
    """

    status: Literal[
        "idle_registry_draft",
        "idle_registry_armed",
        "collecting",
        "sealed",
        "decided",
        "degraded",
        "failed",
        "invalid",
    ]
    action: Literal["none", "seal", "decide", "deadline_decide"] = "none"
    reason: str | None = None
    freeze_boundary_entry_date: str | None = None
    frozen_challenger_count: int = 0
    challenger_outcomes_waiting: int = 0
    control_membership_count: int = 0
    control_outcomes_waiting: int = 0
    routine_membership_count: int = 0
    terminal_dataset_sha256: str | None = None
    terminal_seal_receipt_sha256: str | None = None
    deadline_miss_receipt_sha256: str | None = None
    decision_report_sha256: str | None = None
    startup_retry_count: int = 0
    startup_retry_reason: str | None = None


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Advance at most one blinded OPP-E07 terminal transition"
    )
    parser.add_argument("--trial-db", type=Path, default=Path("data/research/trial.db"))
    parser.add_argument(
        "--diagnostics-db", type=Path, default=Path("data/research/diagnostics.db")
    )
    parser.add_argument(
        "--canary-ledger-db", type=Path, default=Path("data/live_canary.db")
    )
    parser.add_argument("--source-db", type=Path, default=Path("data/insider_alerts.db"))
    parser.add_argument(
        "--registry-path",
        type=Path,
        default=Path("docs/research/registry/OPP-E07-V1.json"),
    )
    parser.add_argument("--seal-db", type=Path, default=Path("data/research/trial_seals.db"))
    parser.add_argument(
        "--artifact-root", type=Path, default=Path("data/research/artifacts")
    )
    parser.add_argument(
        "--activation-db", type=Path, default=Path("data/research/activation.db")
    )
    parser.add_argument(
        "--output-log", type=Path, default=Path("logs/research-terminal-coordinator.log")
    )
    parser.add_argument(
        "--error-log",
        type=Path,
        default=Path("logs/research-terminal-coordinator.err.log"),
    )
    return parser


def _trial_config(config: TerminalBuildConfig) -> TrialRuntimeConfig:
    return TrialRuntimeConfig(
        trial_db=config.trial_db,
        evidence_db=config.trial_db.with_name("evidence.db"),
        bar_feed_db=config.trial_db.with_name("bar_feed.db"),
        session_feed_db=config.trial_db.with_name("session_feed.db"),
        registry_path=config.registry_path,
        activation_db=config.activation_db,
        seal_db=config.seal_db,
    )


def _from_build(
    result: TerminalBuildResult,
    *,
    action: Literal["none", "seal", "decide", "deadline_decide"] = "none",
    reason: str | None = None,
) -> TerminalCoordinatorResult:
    return TerminalCoordinatorResult(
        status=result.status,
        action=action,
        reason=reason if reason is not None else result.reason,
        freeze_boundary_entry_date=result.freeze_boundary_entry_date,
        frozen_challenger_count=result.frozen_challenger_count,
        challenger_outcomes_waiting=result.challenger_outcomes_waiting,
        control_membership_count=result.control_membership_count,
        control_outcomes_waiting=result.control_outcomes_waiting,
        routine_membership_count=result.routine_membership_count,
        terminal_dataset_sha256=result.terminal_dataset_sha256,
        terminal_seal_receipt_sha256=result.terminal_seal_receipt_sha256,
        decision_report_sha256=result.decision_report_sha256,
    )


def _transition_allowed(now: datetime) -> bool:
    local_time = now.astimezone(NEW_YORK).time()
    return TRANSITION_WINDOW_START_ET <= local_time <= TRANSITION_WINDOW_END_ET


def _is_sqlite_contention(exc: sqlite3.OperationalError) -> bool:
    error_code = getattr(exc, "sqlite_errorcode", None)
    if isinstance(error_code, int) and error_code & 0xFF in {
        sqlite3.SQLITE_BUSY,
        sqlite3.SQLITE_LOCKED,
    }:
        return True
    normalized = str(exc).lower()
    return "database is locked" in normalized or "database table is locked" in normalized


def _deadline_decision(
    config: TerminalBuildConfig, *, now: datetime
) -> TerminalCoordinatorResult:
    """Produce the frozen no-dataset deadline payload without exposing an outcome."""

    window = _validated_trial_window(_trial_config(config), now=now)
    if window.status != "active":
        return TerminalCoordinatorResult(
            "idle_registry_draft" if window.status == "draft" else "idle_registry_armed"
        )
    if window.activated_at_utc is None or window.enrollment_deadline_utc is None:
        raise TerminalBuildInvalid("active_window_missing_deadline")
    if now < window.enrollment_deadline_utc:
        return TerminalCoordinatorResult("collecting", reason="enrollment_deadline_not_reached")

    registry = json.loads(config.registry_path.read_text(encoding="utf-8"))
    if not isinstance(registry, dict):
        raise TerminalBuildInvalid("registry_not_object")
    seal_store = TrialSealStore(config.seal_db)
    trial_store = TrialStore(config.trial_db, initialize=False)
    trial_store.validate_integrity()
    with contextlib.closing(trial_store._connect()) as conn:
        conn.execute("BEGIN IMMEDIATE")
        snapshot = _trial_snapshot(conn)
        payload = _terminal_payload(
            snapshot,
            None,
            activated_at=window.activated_at_utc,
            evaluated_at=now,
        )
        report = evaluate_with_store(registry, payload, seal_store)

    state = report.get("state")
    reason_codes = report.get("reason_codes")
    reason = (
        str(reason_codes[0])
        if isinstance(reason_codes, list) and reason_codes
        else "deadline_decision_state_missing"
    )
    deadline_receipt = seal_store.receipt("deadline_miss")
    deadline_receipt_sha = (
        str(deadline_receipt["receipt_sha256"]) if deadline_receipt is not None else None
    )
    if state == "INVALID":
        return TerminalCoordinatorResult(
            "invalid",
            action="deadline_decide",
            reason=reason,
            deadline_miss_receipt_sha256=deadline_receipt_sha,
        )
    if state in {"KILL", "PROMOTE_RECOMMENDED"}:
        return TerminalCoordinatorResult(
            "decided",
            action="deadline_decide",
            reason=str(state),
            deadline_miss_receipt_sha256=deadline_receipt_sha,
            decision_report_sha256=str(report.get("report_sha256")),
        )
    if state != "COLLECTING":
        raise TerminalBuildInvalid("deadline_decision_state_invalid")
    return TerminalCoordinatorResult(
        "collecting",
        action="deadline_decide" if deadline_receipt_sha is not None else "none",
        reason=reason,
        deadline_miss_receipt_sha256=deadline_receipt_sha,
    )


def _run_terminal_coordinator_once(
    config: TerminalBuildConfig, *, now: datetime
) -> TerminalCoordinatorResult:
    if now.tzinfo is None:
        raise TerminalBuildInvalid("terminal_coordinator_clock_naive")
    evaluated_at = now.astimezone(UTC)
    window = _validated_trial_window(_trial_config(config), now=evaluated_at)
    if window.status != "active":
        return TerminalCoordinatorResult(
            "idle_registry_draft" if window.status == "draft" else "idle_registry_armed"
        )
    status = terminal_status(config)
    if status.status in {"decided", "invalid", "idle_registry_draft", "idle_registry_armed"}:
        return _from_build(status)
    if not _transition_allowed(evaluated_at):
        return _from_build(status, reason="transition_deferred_outside_after_hours_window")
    if status.status == "sealed":
        return _from_build(
            decide_terminal_dataset(config, now=evaluated_at),
            action="decide",
        )

    seal_store = TrialSealStore(config.seal_db)
    if seal_store.pending_terminal() is not None:
        return _from_build(
            seal_terminal_dataset(config, now=evaluated_at),
            action="seal",
        )

    if (
        window.enrollment_deadline_utc is not None
        and evaluated_at >= window.enrollment_deadline_utc
    ):
        deadline = _deadline_decision(config, now=evaluated_at)
        if deadline.status in {"decided", "invalid"}:
            return deadline
        if deadline.deadline_miss_receipt_sha256 is not None:
            return deadline

    ready_to_seal = (
        status.status == "collecting"
        and status.freeze_boundary_entry_date is not None
        and status.challenger_outcomes_waiting == 0
    )
    if ready_to_seal:
        return _from_build(
            seal_terminal_dataset(config, now=evaluated_at),
            action="seal",
        )
    return _from_build(status)


def run_terminal_coordinator_once(
    config: TerminalBuildConfig, *, now: datetime | None = None
) -> TerminalCoordinatorResult:
    """Advance at most one terminal state transition and classify retryable contention."""

    if now is not None and now.tzinfo is None:
        return TerminalCoordinatorResult("invalid", reason="terminal_coordinator_clock_naive")
    evaluated_at = (now or datetime.now(UTC)).astimezone(UTC)
    try:
        return _run_terminal_coordinator_once(config, now=evaluated_at)
    except TerminalBuildNotReady as exc:
        return TerminalCoordinatorResult("collecting", reason=exc.code)
    except sqlite3.OperationalError as exc:
        status: Literal["degraded", "failed"] = (
            "degraded" if _is_sqlite_contention(exc) else "failed"
        )
        return TerminalCoordinatorResult(status, reason=f"sqlite_operational_error:{exc}"[:500])
    except (
        TerminalBuildInvalid,
        TrialInvalid,
        TrialRuntimeInvalid,
        sqlite3.DatabaseError,
        json.JSONDecodeError,
        ValueError,
    ) as exc:
        return TerminalCoordinatorResult("invalid", reason=str(exc)[:500])
    except OSError as exc:
        return TerminalCoordinatorResult(
            "degraded", reason=f"{type(exc).__name__}:{exc}"[:500]
        )


def _startup_validation_failure(
    config: TerminalBuildConfig,
) -> TerminalCoordinatorResult | None:
    """Validate Git and activation custody before the transition-capable run begins."""

    try:
        _validated_trial_window(_trial_config(config))
    except sqlite3.OperationalError as exc:
        status: Literal["degraded", "failed"] = (
            "degraded" if _is_sqlite_contention(exc) else "failed"
        )
        return TerminalCoordinatorResult(
            status, reason=f"sqlite_operational_error:{exc}"[:500]
        )
    except (
        TerminalBuildInvalid,
        TrialInvalid,
        TrialRuntimeInvalid,
        sqlite3.DatabaseError,
        json.JSONDecodeError,
        ValueError,
    ) as exc:
        return TerminalCoordinatorResult("invalid", reason=str(exc)[:500])
    except OSError as exc:
        return TerminalCoordinatorResult(
            "degraded", reason=f"{type(exc).__name__}:{exc}"[:500]
        )
    return None


def _retryable_startup_validation_failure(
    result: TerminalCoordinatorResult | None,
) -> bool:
    """Return whether the isolated startup preflight had transient Git unavailability."""

    return result is not None and result.reason in _RETRYABLE_STARTUP_VALIDATION_REASONS


def _run_with_startup_validation_retries(
    config: TerminalBuildConfig,
) -> TerminalCoordinatorResult:
    startup_failure = _startup_validation_failure(config)
    retry_count = 0
    retry_reason: str | None = None
    for delay_seconds in STARTUP_VALIDATION_RETRY_DELAYS_SECONDS:
        if startup_failure is None:
            return replace(
                run_terminal_coordinator_once(config),
                startup_retry_count=retry_count,
                startup_retry_reason=retry_reason,
            )
        if not _retryable_startup_validation_failure(startup_failure):
            return replace(
                startup_failure,
                startup_retry_count=retry_count,
                startup_retry_reason=retry_reason,
            )
        retry_reason = retry_reason or startup_failure.reason
        sleep(delay_seconds)
        retry_count += 1
        startup_failure = _startup_validation_failure(config)
    if startup_failure is None:
        return replace(
            run_terminal_coordinator_once(config),
            startup_retry_count=retry_count,
            startup_retry_reason=retry_reason,
        )
    if _retryable_startup_validation_failure(startup_failure):
        return TerminalCoordinatorResult(
            "degraded",
            reason=f"startup_git_validation_unavailable:{startup_failure.reason}"[:500],
            startup_retry_count=retry_count,
            startup_retry_reason=retry_reason,
        )
    return replace(
        startup_failure,
        startup_retry_count=retry_count,
        startup_retry_reason=retry_reason,
    )


def _append_record(path: Path, result: TerminalCoordinatorResult) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {"recorded_at_utc": datetime.now(UTC).isoformat(), **asdict(result)}
    with path.open("ab") as stream:
        stream.write(rfc8785.dumps(record) + b"\n")


def _append_error(path: Path, result: TerminalCoordinatorResult) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(
            f"{datetime.now(UTC).isoformat()} {result.status}: {result.reason or 'unknown'}\n"
        )


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    config = TerminalBuildConfig(
        trial_db=args.trial_db,
        diagnostics_db=args.diagnostics_db,
        canary_ledger_db=args.canary_ledger_db,
        source_db=args.source_db,
        registry_path=args.registry_path,
        seal_db=args.seal_db,
        artifact_root=args.artifact_root,
        activation_db=args.activation_db,
    )
    try:
        ensure_kill_on_close_process_tree()
        result = _run_with_startup_validation_retries(config)
        _append_record(args.output_log, result)
        if result.status in {"degraded", "failed", "invalid"}:
            _append_error(args.error_log, result)
    except Exception as exc:
        failure = TerminalCoordinatorResult(
            "failed", reason=f"unexpected_{type(exc).__name__}:{exc}"[:500]
        )
        with contextlib.suppress(Exception):
            _append_record(args.output_log, failure)
        with contextlib.suppress(Exception):
            _append_error(args.error_log, failure)
        return 3
    if result.status == "invalid":
        return 3
    if result.status == "failed":
        return 3
    return 2 if result.status == "degraded" else 0


if __name__ == "__main__":
    raise SystemExit(main())
