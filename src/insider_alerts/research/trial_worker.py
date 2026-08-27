"""Hidden one-cycle worker for prospective candidate import and entry sealing."""

from __future__ import annotations

import argparse
import contextlib
import json
import sqlite3
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from insider_alerts.research.diagnostic_outcomes import (
    DiagnosticOutcomeResult,
    finalize_diagnostic_outcomes,
)
from insider_alerts.research.diagnostics import (
    DiagnosticConfig,
    DiagnosticRunResult,
    DiagnosticStore,
    run_diagnostics_once,
)
from insider_alerts.research.trial_finalizer import finalize_pending_entry_dates
from insider_alerts.research.trial_outcome_finalizer import finalize_trial_outcomes
from insider_alerts.research.trial_runtime import (
    TrialRuntimeConfig,
    TrialRuntimeRetryable,
    TrialStore,
    run_trial_once,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one prospective OPP-E07 trial cycle")
    parser.add_argument("--trial-db", type=Path, default=Path("data/research/trial.db"))
    parser.add_argument("--diagnostics-db", type=Path)
    parser.add_argument("--canary-ledger-db", type=Path, default=Path("data/live_canary.db"))
    parser.add_argument("--source-db", type=Path, default=Path("data/insider_alerts.db"))
    parser.add_argument("--evidence-db", type=Path, default=Path("data/research/evidence.db"))
    parser.add_argument("--bar-feed-db", type=Path, default=Path("data/research/bar_feed.db"))
    parser.add_argument(
        "--session-feed-db", type=Path, default=Path("data/research/session_feed.db")
    )
    parser.add_argument(
        "--registry-path",
        type=Path,
        default=Path("docs/research/registry/OPP-E07-V1.json"),
    )
    parser.add_argument(
        "--error-log", type=Path, default=Path("logs/research-trial-worker.err.log")
    )
    return parser


def _append_error(path: Path, exc: BaseException) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(f"{datetime.now(UTC).isoformat()} {type(exc).__name__}: {exc}\n")


def _append_isolated_error(path: Path, exc: BaseException) -> None:
    """Keep diagnostic logging failures from crossing into confirmatory execution."""

    with contextlib.suppress(Exception):
        _append_error(path, exc)


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    diagnostics_db = args.diagnostics_db or args.trial_db.with_name("diagnostics.db")
    config = TrialRuntimeConfig(
        trial_db=args.trial_db,
        evidence_db=args.evidence_db,
        bar_feed_db=args.bar_feed_db,
        session_feed_db=args.session_feed_db,
        registry_path=args.registry_path,
    )
    diagnostic_config = DiagnosticConfig(
        trial_db=args.trial_db,
        diagnostics_db=diagnostics_db,
        canary_ledger_db=args.canary_ledger_db,
        source_db=args.source_db,
        evidence_db=args.evidence_db,
        bar_feed_db=args.bar_feed_db,
        session_feed_db=args.session_feed_db,
        registry_path=args.registry_path,
    )
    try:
        imported = run_trial_once(config, now=datetime.now(UTC))
    except Exception as exc:
        _append_error(args.error_log, exc)
        return 2
    if imported.status in {"degraded", "invalid"}:
        error = RuntimeError(f"candidate runtime is {imported.status}: {imported.error}")
        _append_error(args.error_log, error)
        return 2
    try:
        finalized = finalize_pending_entry_dates(config)
        outcomes = finalize_trial_outcomes(config)
    except (TrialRuntimeRetryable, sqlite3.OperationalError, OSError) as exc:
        now = datetime.now(UTC)
        detail = f"{type(exc).__name__}: {exc}"[:2000]
        _append_error(args.error_log, exc)
        with contextlib.suppress(Exception):
            TrialStore(config.trial_db).write_health(
                now=now,
                result="degraded",
                error=detail,
                evidence_seen=0,
                unresolved_evidence=0,
            )
        return 2
    except Exception as exc:
        now = datetime.now(UTC)
        detail = f"{type(exc).__name__}: {exc}"[:2000]
        _append_error(args.error_log, exc)
        with contextlib.suppress(Exception):
            store = TrialStore(config.trial_db)
            store.record_fault(now=now, kind="TRIAL_FINALIZER_INVALID", detail=detail)
            store.write_health(
                now=now,
                result="invalid",
                error=detail,
                evidence_seen=0,
                unresolved_evidence=0,
            )
        return 2
    try:
        diagnostics = run_diagnostics_once(diagnostic_config, now=datetime.now(UTC))
    except Exception as exc:
        _append_isolated_error(args.error_log, RuntimeError(f"diagnostic phase isolated: {exc}"))
        diagnostics = DiagnosticRunResult("degraded", error=f"{type(exc).__name__}: {exc}"[:2000])
        with contextlib.suppress(Exception):
            DiagnosticStore(diagnostics_db).write_health(now=datetime.now(UTC), result=diagnostics)
    try:
        diagnostic_outcomes = finalize_diagnostic_outcomes(diagnostic_config, now=datetime.now(UTC))
    except Exception as exc:
        _append_isolated_error(
            args.error_log, RuntimeError(f"diagnostic outcome phase isolated: {exc}")
        )
        diagnostic_outcomes = DiagnosticOutcomeResult(
            "degraded", error=f"{type(exc).__name__}: {exc}"[:2000]
        )
        with contextlib.suppress(Exception):
            DiagnosticStore(diagnostics_db).write_outcome_health(
                now=datetime.now(UTC),
                status=diagnostic_outcomes.status,
                error=diagnostic_outcomes.error,
                candidates_seen=diagnostic_outcomes.candidates_seen,
                outcomes_waiting=diagnostic_outcomes.outcomes_waiting,
            )
    print(
        json.dumps(
            {
                "diagnostics": asdict(diagnostics),
                "diagnostic_outcomes": asdict(diagnostic_outcomes),
                "candidate_runtime": asdict(imported),
                "entry_finalizer": asdict(finalized),
                "outcome_finalizer": asdict(outcomes),
            },
            sort_keys=True,
            default=str,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
