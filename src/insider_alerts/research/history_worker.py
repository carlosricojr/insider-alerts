"""Order-incapable CLI for resumable SEC ownership archive synchronization."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

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
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
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
        store=HistoryStore(args.database),
        raw_store=RawObjectStore(args.raw_root),
        through=through,
        refresh=bool(args.refresh),
    )
    print(json.dumps(asdict(result), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
