"""Fixed-path, hidden, one-shot worker for capture-only operational evidence."""

from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from insider_alerts.execution.observer import (
    Journal,
    parse,
    run_once,
    safe_path,
    status,
    worker_lock,
)
from insider_alerts.execution.windows_job import ensure_kill_on_close_process_tree
from insider_alerts.research.ibkr_bar_source import IbkrHistoricalBarSource


def revision(root: Path) -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    return result.stdout.strip()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Uncapped capture only; no returns or orders")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--activate-at", type=parse)
    mode.add_argument("--status", action="store_true")
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[3]
    try:
        ensure_kill_on_close_process_tree()
        folder = safe_path(root / "data" / "observer")
        path = safe_path(folder / "evidence.db")
        source = safe_path(root / "data" / "live_canary.db")
        if args.status:
            print(json.dumps(status(Journal(path, readonly=True), now=datetime.now(UTC))))
            return 0
        if args.activate_at:
            if not source.is_file():
                raise FileNotFoundError("canary source missing")
            Journal.activate(
                path, start=args.activate_at, now=datetime.now(UTC), revision=revision(root)
            )
            print(json.dumps({"activated_at": args.activate_at.isoformat()}))
            return 0
        if not path.is_file():
            raise FileNotFoundError("observer not activated; implicit activation is prohibited")
        with worker_lock(folder / "worker-lock.db"):
            result = asyncio.run(
                run_once(
                    Journal(path),
                    source,
                    IbkrHistoricalBarSource(host="127.0.0.1", port=4001, client_id=178),
                    revision=revision(root),
                )
            )
        print(json.dumps(result))
        return 2 if result["result"] in {"source_unavailable", "market_data_unavailable"} else 0
    except Exception as exc:
        error_log = safe_path(root / "logs" / "observer.err.log")
        error_log.parent.mkdir(parents=True, exist_ok=True)
        with error_log.open("a", encoding="utf-8") as stream:
            stream.write(f"{datetime.now(UTC).isoformat()} {type(exc).__name__}: {exc}\n")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
