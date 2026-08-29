"""Order-incapable one-shot worker for notification coverage reconciliation."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from insider_alerts.execution.windows_job import ensure_kill_on_close_process_tree
from insider_alerts.research.capture import resolve_git_commit
from insider_alerts.research.notification_coverage import (
    NotificationCoverageConfig,
    activate_notification_coverage,
    confined_notification_coverage_source,
    notification_coverage_status,
    run_notification_coverage_once,
)
from insider_alerts.research.notification_transport import NotificationJournalConfig
from insider_alerts.review.queue import ensure_review_tables


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reconcile operational notification acknowledgements to transport custody"
    )
    parser.add_argument("--source-db", type=Path, default=Path("data/insider_alerts.db"))
    parser.add_argument(
        "--journal-db", type=Path, default=Path("data/research/notification_transport.db")
    )
    parser.add_argument(
        "--coverage-db", type=Path, default=Path("data/research/notification_coverage.db")
    )
    parser.add_argument(
        "--journal-policy",
        type=Path,
        default=Path("docs/research/contracts/notification-transport-v1.json"),
    )
    parser.add_argument(
        "--coverage-policy",
        type=Path,
        default=Path("docs/research/contracts/notification-coverage-v1.json"),
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--initialize-source-schema", action="store_true")
    mode.add_argument("--activate", action="store_true")
    mode.add_argument("--status", action="store_true")
    parser.add_argument("--output-log", type=Path, default=Path("logs/notification-coverage.log"))
    parser.add_argument(
        "--error-log", type=Path, default=Path("logs/notification-coverage.err.log")
    )
    return parser


def _resolve(path: Path, *, repo_root: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def _append(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(value.rstrip() + "\n")


def _config(args: argparse.Namespace, *, repo_root: Path) -> NotificationCoverageConfig:
    git_commit = resolve_git_commit(repo_root)
    research_root = repo_root / "data" / "research"
    policy_root = repo_root / "docs" / "research" / "contracts"
    journal = NotificationJournalConfig(
        database=_resolve(args.journal_db, repo_root=repo_root),
        research_root=research_root,
        policy_path=_resolve(args.journal_policy, repo_root=repo_root),
        policy_root=policy_root,
        runtime_git_commit=git_commit,
    )
    return NotificationCoverageConfig(
        source_db=_resolve(args.source_db, repo_root=repo_root),
        source_root=repo_root / "data",
        coverage_db=_resolve(args.coverage_db, repo_root=repo_root),
        research_root=research_root,
        policy_path=_resolve(args.coverage_policy, repo_root=repo_root),
        policy_root=policy_root,
        journal=journal,
        runtime_git_commit=git_commit,
    )


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    repo_root = Path(__file__).resolve().parents[3]
    try:
        ensure_kill_on_close_process_tree()
        config = _config(args, repo_root=repo_root)
        if args.initialize_source_schema:
            ensure_review_tables(str(confined_notification_coverage_source(config)))
            result: dict[str, object] = {"initialized": True}
        else:
            if args.activate:
                activation = activate_notification_coverage(config)
                reconciliation = run_notification_coverage_once(config)
                result = {"activation": activation, "reconciliation": reconciliation}
            elif args.status:
                result = notification_coverage_status(config)
            else:
                result = run_notification_coverage_once(config)
        encoded = json.dumps(result, sort_keys=True)
        if not args.status and not args.initialize_source_schema:
            _append(
                _resolve(args.output_log, repo_root=repo_root),
                f"{datetime.now(UTC).isoformat()} {encoded}",
            )
        print(encoded)
        if args.status and result.get("valid") is not True:
            return 3
        if not args.status and result.get("valid") is False:
            return 2
        return 0
    except Exception as exc:
        _append(
            _resolve(args.error_log, repo_root=repo_root),
            f"{datetime.now(UTC).isoformat()} {type(exc).__name__}: {exc}",
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
