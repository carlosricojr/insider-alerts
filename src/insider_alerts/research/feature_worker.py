"""Order-incapable one-shot worker for capture-only Companyfacts evidence."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from insider_alerts.config import get_settings
from insider_alerts.research.capture import resolve_git_commit
from insider_alerts.research.feature_capture import (
    FeatureCaptureConfig,
    feature_capture_status,
    initialize_feature_capture,
    run_feature_capture_once,
)
from insider_alerts.sec.client import SecHttpClient


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("activation timestamp must include a UTC offset")
    return parsed.astimezone(UTC)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Capture at most one point-in-time Companyfacts response"
    )
    parser.add_argument("--database-path", type=Path, default=Path("data/insider_alerts.db"))
    parser.add_argument(
        "--feature-db", type=Path, default=Path("data/research/feature_evidence.db")
    )
    parser.add_argument(
        "--artifact-root", type=Path, default=Path("data/research/artifacts/companyfacts")
    )
    parser.add_argument(
        "--policy-path",
        type=Path,
        default=Path("docs/research/contracts/companyfacts-capture-v1.json"),
    )
    parser.add_argument("--activation-at", type=_parse_utc)
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--initialize-only", action="store_true")
    parser.add_argument(
        "--error-log", type=Path, default=Path("logs/feature-capture.err.log")
    )
    return parser


def _append_error(path: Path, exc: BaseException) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(f"{datetime.now(UTC).isoformat()} {type(exc).__name__}: {exc}\n")


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    repo_root = Path(__file__).resolve().parents[3]
    try:
        if args.status:
            print(
                json.dumps(
                    feature_capture_status(args.feature_db, artifact_root=args.artifact_root),
                    sort_keys=True,
                )
            )
            return 0
        if args.activation_at is None:
            raise ValueError("--activation-at is required unless --status is used")
        config = FeatureCaptureConfig(
            source_db=args.database_path,
            feature_db=args.feature_db,
            artifact_root=args.artifact_root,
            research_root=repo_root / "data" / "research",
            policy_path=args.policy_path,
            activation_at_utc=args.activation_at,
            git_commit=resolve_git_commit(repo_root),
        )
        if args.initialize_only:
            print(json.dumps(initialize_feature_capture(config), sort_keys=True))
            return 0
        settings = get_settings().model_copy(update={"sec_retry_attempts": 1})
        result = run_feature_capture_once(
            config,
            client=SecHttpClient(settings=settings),
        )
    except Exception as exc:
        _append_error(args.error_log, exc)
        return 2
    print(json.dumps(asdict(result), sort_keys=True))
    return 2 if result.status == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
