"""Order-incapable annotations; never correct or replay the frozen operational ledger."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import tempfile
from collections import Counter
from collections.abc import Mapping, Sequence
from contextlib import closing
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import rfc8785

from insider_alerts.execution.calendar import CALENDAR_SHA256, bounds
from insider_alerts.execution.errors import IbkrExecutionError


def classification(row: Mapping[str, Any]) -> str:
    """No positive classification establishes an accurate price or a valid portfolio."""
    try:
        closed_at = datetime.fromisoformat(str(row["closed_at"]))
        session = bounds(date.fromisoformat(str(row["exit_session"])))
        if closed_at.tzinfo is None or session is None:
            return "unverifiable_exit_timestamp"
        if closed_at < session[1]:
            if row["exit_reason"] == "time":
                return "invalid_time_exit_before_session_close"
            return "unverified_intraday_barrier_result"
    except (KeyError, ValueError, IbkrExecutionError):
        return "unverifiable_exit_timestamp"
    return "unverified_completed_day_result"


def summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "closed_records": len(rows),
        "classifications": dict(sorted(Counter(classification(row) for row in rows).items())),
        "portfolio_performance_validated": False,
        "warning": (
            "Operational ghost bookkeeping is not validated profit evidence. "
            "Legacy price/selection defects and capacity omissions require separate review; "
            "completed-day timing alone does not validate a result."
        ),
    }


def build_report(ledger: Path, observed_at: datetime) -> dict[str, Any]:
    if observed_at.tzinfo is None:
        raise ValueError("audit clock must be timezone-aware")
    with closing(sqlite3.connect(ledger.resolve().as_uri() + "?mode=ro", uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        conn.execute("BEGIN")
        rows = [dict(row) for row in conn.execute("SELECT * FROM shadow_trades ORDER BY packet_id")]
        conn.rollback()
    annotations = [
        {
            "original_row": row,
            "original_row_sha256": hashlib.sha256(rfc8785.dumps(row)).hexdigest(),
            "classification": classification(row),
            "action": "preserve_original_no_replay_no_correction",
        }
        for row in rows
    ]
    return {
        "schema_version": 1,
        "scope": "operational_ghost_only_not_confirmatory",
        "observed_at_utc": observed_at.astimezone(UTC).isoformat(),
        "calendar_sha256": CALENDAR_SHA256,
        "summary": summary(rows),
        "annotations": annotations,
    }


def publish_report(report: Mapping[str, Any], output: Path) -> Path:
    """Publish a complete immutable artifact; never replace an existing path."""
    content = rfc8785.dumps(report)
    destination = output / f"shadow-audit-{hashlib.sha256(content).hexdigest()}.json"
    output.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=output, prefix=".shadow-audit-", delete=False
        ) as handle:
            temporary = Path(handle.name)
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, destination)
        except FileExistsError:
            if destination.read_bytes() != content:
                raise ValueError(
                    "existing audit artifact does not match its content address"
                ) from None
    finally:
        if temporary is not None:
            temporary.unlink()
    return destination


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    report = build_report(args.ledger, datetime.now(UTC))
    artifact = publish_report(report, args.output)
    print(json.dumps({"artifact": str(artifact), "summary": report["summary"]}, sort_keys=True))


if __name__ == "__main__":
    main()
