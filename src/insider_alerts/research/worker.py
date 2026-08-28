"""Order-incapable bounded entry point for prospective evidence capture."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from insider_alerts.research.capture import (
    CaptureConfig,
    record_worker_failure,
    resolve_git_commit,
    run_capture_once,
)
from insider_alerts.review.queue import ensure_review_tables


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Capture at most one prospective evidence job")
    parser.add_argument("--database-path", type=Path, default=Path("data/insider_alerts.db"))
    parser.add_argument("--evidence-db", type=Path, default=Path("data/research/evidence.db"))
    parser.add_argument("--activation-db", type=Path, default=Path("data/research/activation.db"))
    parser.add_argument("--artifact-root", type=Path, default=Path("data/research/artifacts"))
    parser.add_argument("--canary-ledger", type=Path, default=Path("data/live_canary.db"))
    parser.add_argument("--history-db", type=Path, default=Path("data/research/sec_history.db"))
    parser.add_argument("--history-snapshot-sha256", required=True)
    parser.add_argument("--alpha-python", type=Path, required=True)
    parser.add_argument("--alpha-script", type=Path, required=True)
    parser.add_argument("--alpha-historical-script", type=Path, required=True)
    parser.add_argument("--option-chain-store-db", type=Path, required=True)
    parser.add_argument("--historical-pacing-db", type=Path, required=True)
    parser.add_argument("--option-timeout", type=int, default=90)
    parser.add_argument("--historical-option-timeout", type=int, default=120)
    parser.add_argument("--error-log", type=Path, default=Path("logs/research-capture.err.log"))
    return parser


def _append_error(path: Path, exc: BaseException) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(f"{datetime.now(UTC).isoformat()} {type(exc).__name__}: {exc}\n")


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    repo_root = Path(__file__).resolve().parents[3]
    try:
        ensure_review_tables(str(args.database_path))
        config = CaptureConfig(
            source_db=args.database_path,
            evidence_db=args.evidence_db,
            artifact_root=args.artifact_root,
            research_root=repo_root / "data" / "research",
            alpha_python=args.alpha_python,
            alpha_script=args.alpha_script,
            alpha_historical_script=args.alpha_historical_script,
            option_chain_store_db=args.option_chain_store_db,
            historical_pacing_db=args.historical_pacing_db,
            canary_ledger=args.canary_ledger,
            history_db=args.history_db,
            history_snapshot_sha256=args.history_snapshot_sha256,
            insider_git_commit=resolve_git_commit(repo_root),
            policy_path=repo_root / "docs" / "research" / "registry" / "OPP-E07-V1.json",
            evidence_schema_path=(
                repo_root / "docs" / "research" / "contracts" / "evidence-snapshot.schema.json"
            ),
            activation_db=args.activation_db,
            option_timeout_seconds=args.option_timeout,
            historical_option_timeout_seconds=args.historical_option_timeout,
        )
        result = run_capture_once(config)
    except Exception as exc:
        try:
            record_worker_failure(args.evidence_db, exc)
        finally:
            _append_error(args.error_log, exc)
        return 2
    print(json.dumps(asdict(result), sort_keys=True))
    return 2 if result.status == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
