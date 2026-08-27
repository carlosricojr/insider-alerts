"""Hidden one-cycle worker for the order-incapable completed-bar feed."""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path

from insider_alerts.research.bar_feed import BarFeedStore, collect_once, result_json
from insider_alerts.research.ibkr_bar_source import IbkrHistoricalBarSource


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Collect requested completed IBKR daily bars")
    parser.add_argument("--feed-db", type=Path, default=Path("data/research/bar_feed.db"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=4001)
    parser.add_argument("--client-id", type=int, default=176)
    parser.add_argument("--error-log", type=Path, default=Path("logs/research-bar-feed.err.log"))
    return parser


def _append_error(path: Path, exc: BaseException) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(f"{datetime.now(UTC).isoformat()} {type(exc).__name__}: {exc}\n")


async def _run(args: argparse.Namespace) -> dict[str, int]:
    store = BarFeedStore(args.feed_db)
    source = IbkrHistoricalBarSource(host=args.host, port=args.port, client_id=args.client_id)
    result = await collect_once(store, source, now=datetime.now(UTC))
    return result_json(result)


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = asyncio.run(_run(args))
    except Exception as exc:
        _append_error(args.error_log, exc)
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
