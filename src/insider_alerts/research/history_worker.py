"""Order-incapable CLI for resumable SEC ownership archive synchronization."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from insider_alerts.config import Settings
from insider_alerts.research.sec_history import HistoryStore, RawObjectStore, sync_bulk_archives
from insider_alerts.sec.client import SecHttpClient


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Synchronize immutable SEC ownership archives")
    parser.add_argument("--database", type=Path, default=Path("data/research/sec_history.db"))
    parser.add_argument(
        "--raw-root", type=Path, default=Path("data/research/sec-history-raw")
    )
    parser.add_argument("--through-year", type=int)
    parser.add_argument("--through-quarter", type=int, choices=(1, 2, 3, 4))
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--timeout", type=float, default=120.0)
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--validate-snapshot")
    action.add_argument("--seal-existing-snapshot")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    store = HistoryStore(args.database)
    if args.validate_snapshot:
        now = datetime.now(UTC)
        classification_year = now.astimezone(ZoneInfo("America/New_York")).year
        metadata = store.verify_snapshot_material(str(args.validate_snapshot))
        _, coverage = store.coverage_for_classification(
            str(args.validate_snapshot), classification_year=classification_year
        )
        if metadata.created_at > now:
            raise SystemExit("snapshot creation timestamp is in the future")
        if coverage.missing_quarters:
            raise SystemExit(
                f"snapshot has classification coverage gaps: {coverage.missing_quarters}"
            )
        print(
            json.dumps(
                {
                    "snapshot_sha256": metadata.snapshot_sha256,
                    "created_at_utc": metadata.created_at.isoformat(),
                    "normalized_sha256": metadata.normalized_sha256,
                    "first_quarter": metadata.first_quarter,
                    "last_quarter": metadata.last_quarter,
                    "classification_year": classification_year,
                    "missing_quarters": list(coverage.missing_quarters),
                },
                sort_keys=True,
            )
        )
        return 0
    if args.seal_existing_snapshot:
        snapshot_sha256 = store.seal_existing_snapshot(
            str(args.seal_existing_snapshot), created_at=datetime.now(UTC)
        )
        print(json.dumps({"snapshot_sha256": snapshot_sha256}, sort_keys=True))
        return 0
    if (args.through_year is None) != (args.through_quarter is None):
        raise SystemExit("--through-year and --through-quarter must be supplied together")
    through = (
        (int(args.through_year), int(args.through_quarter))
        if args.through_year is not None
        else None
    )
    settings = Settings(SEC_TIMEOUT_SECONDS=args.timeout)
    result = sync_bulk_archives(
        client=SecHttpClient(settings),
        store=store,
        raw_store=RawObjectStore(args.raw_root),
        through=through,
        refresh=bool(args.refresh),
    )
    print(json.dumps(asdict(result), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
