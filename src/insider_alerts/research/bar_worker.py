"""Hidden one-cycle worker for the order-incapable completed-bar feed."""

from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from insider_alerts.research.bar_feed import BarFeedStore, collect_once, result_json
from insider_alerts.research.ibkr_bar_source import IbkrHistoricalBarSource
from insider_alerts.research.session_feed import SessionFeedStore


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Collect requested completed IBKR daily bars")
    parser.add_argument("--feed-db", type=Path, default=Path("data/research/bar_feed.db"))
    parser.add_argument(
        "--session-feed-db",
        type=Path,
        default=Path("data/research/session_feed.db"),
    )
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
    now = datetime.now(UTC)
    store = BarFeedStore(args.feed_db)
    completed_through = None
    if args.session_feed_db.is_file():
        try:
            session_store = SessionFeedStore(args.session_feed_db, initialize=False)
            session_store.validate_integrity()
            completed_through = session_store.completed_through_date(now)
        except (sqlite3.DatabaseError, IndexError, KeyError, TypeError, ValueError) as exc:
            _append_error(args.error_log, exc)
            store.record_failure(
                now=now,
                symbol="SESSION-FEED",
                category="session_completion_proof_unavailable",
                detail=f"{type(exc).__name__}: {exc}",
            )
    source = IbkrHistoricalBarSource(host=args.host, port=args.port, client_id=args.client_id)
    result = await collect_once(
        store,
        source,
        now=now,
        completed_through_date=completed_through,
    )
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
