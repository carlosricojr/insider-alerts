"""Order-incapable one-shot entry point for prospective evidence capture."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from insider_alerts.research.capture import CaptureConfig, resolve_git_commit, run_capture_once
from insider_alerts.review.queue import ensure_review_tables


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Capture at most one prospective evidence job")
    parser.add_argument("--database-path", type=Path, default=Path("data/insider_alerts.db"))
    parser.add_argument("--evidence-db", type=Path, default=Path("data/research/evidence.db"))
    parser.add_argument(
        "--artifact-root", type=Path, default=Path("data/research/artifacts")
    )
    parser.add_argument("--canary-ledger", type=Path, default=Path("data/live_canary.db"))
    parser.add_argument("--alpha-python", type=Path, required=True)
    parser.add_argument("--alpha-script", type=Path, required=True)
    parser.add_argument("--option-timeout", type=int, default=90)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    repo_root = Path(__file__).resolve().parents[3]
    ensure_review_tables(str(args.database_path))
    config = CaptureConfig(
        source_db=args.database_path,
        evidence_db=args.evidence_db,
        artifact_root=args.artifact_root,
        alpha_python=args.alpha_python,
        alpha_script=args.alpha_script,
        canary_ledger=args.canary_ledger,
        insider_git_commit=resolve_git_commit(repo_root),
        policy_path=repo_root / "docs" / "research" / "registry" / "OPP-E07-V1.json",
        evidence_schema_path=(
            repo_root / "docs" / "research" / "contracts" / "evidence-snapshot.schema.json"
        ),
        option_timeout_seconds=args.option_timeout,
    )
    print(json.dumps(asdict(run_capture_once(config)), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
