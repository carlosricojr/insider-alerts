"""Read-only field projection; never copy outcomes or mutate a canary store."""

from __future__ import annotations

import math
import sqlite3
from collections.abc import Callable
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import rfc8785

from insider_alerts.execution.observer import COLUMNS, safe_path
from insider_alerts.research import calendar_v2 as calendar
from insider_alerts.research.commissioning_journal import Journal, digest
from insider_alerts.research.commissioning_source import timestamp
from insider_alerts.research.commissioning_window import admit, parse, validate_manifest

METADATA = ("runtime_started_utc", "runtime_source_fingerprint")


def source_value(value: Any) -> Any:
    """Preserve invalid SQLite scalars explicitly instead of losing the whole poll."""
    if isinstance(value, float) and not math.isfinite(value):
        return {"sqlite_type": "REAL", "value_text": repr(value), "validation_error": "non_finite"}
    if type(value) is int and abs(value) > 2**53 - 1:
        return {
            "sqlite_type": "INTEGER",
            "value_text": str(value),
            "validation_error": "outside_canonical_integer_range",
        }
    if isinstance(value, bytes):
        return {"sqlite_type": "BLOB", "hex": value.hex(), "validation_error": "unexpected_blob"}
    return value


def snapshot(
    path: Path, *, clock: Callable[[], datetime] = lambda: datetime.now(UTC)
) -> dict[str, Any]:
    path = safe_path(path).resolve(strict=True)
    identity = path.stat()
    started = timestamp(clock())
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=2)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        conn.execute("BEGIN")
        rows = [
            dict(row)
            for row in conn.execute(
                f"SELECT {','.join(COLUMNS)} FROM candidates ORDER BY packet_id"
            )
        ]
        metadata = {
            row["key"]: row["value"]
            for row in conn.execute("SELECT key,value FROM metadata WHERE key IN (?,?)", METADATA)
        }
    # The transaction is closed before the output journal is opened.
    observed = timestamp(clock())
    after = path.stat()
    if (identity.st_dev, identity.st_ino) != (after.st_dev, after.st_ino):
        raise ValueError("source ledger replaced during custody read")
    if observed < started:
        raise ValueError("custody snapshot clock reversed")
    return {
        "source_path": str(path),
        "source_device": str(identity.st_dev),
        "source_inode": str(identity.st_ino),
        "read_started_at": started,
        "observed_at": observed,
        "metadata": metadata,
        "rows": rows,
    }


def inventory(observed: dict[str, Any]) -> dict[str, Any]:
    keys = [
        digest(rfc8785.dumps({"packet_id": source_value(row["packet_id"])}))
        for row in observed["rows"]
    ]
    if len(set(keys)) != len(keys):
        raise ValueError("duplicate source packet identities")
    return {
        key: observed[key]
        for key in (
            "source_path",
            "source_device",
            "source_inode",
            "read_started_at",
            "observed_at",
        )
    } | {"packet_id_hashes": sorted(keys)}


def capture(journal: Journal, version: calendar.Version, observed: dict[str, Any]) -> int:
    records = journal.records()
    frozen = records[0]["payload"]
    validate_manifest(frozen, version)
    when = parse(observed["observed_at"])
    if when >= parse(frozen["recovery_deadline"]):
        return 0
    if (
        parse(observed["read_started_at"]) > when
        or when > journal.clock()
        or when < parse(frozen["created_at"])
    ):
        raise ValueError("invalid custody observation clocks")
    prior_inventory = frozen["preexisting_inventory"]
    if any(
        observed[key] != prior_inventory[key]
        for key in ("source_path", "source_device", "source_inode")
    ):
        raise ValueError("source ledger identity differs from activation")
    preexisting = set(prior_inventory["packet_id_hashes"])
    existing = {r["entity"]: r for r in records if r["kind"] == "custody"}
    added = 0
    counts: dict[str, int] = {}
    for source_row in observed["rows"]:
        # Explicit projection again defends callers other than snapshot().
        row = {name: source_value(source_row[name]) for name in COLUMNS}
        entity = digest(rfc8785.dumps({"packet_id": row["packet_id"]}))
        if entity in preexisting:
            counts["preexisting"] = counts.get("preexisting", 0) + 1
            continue
        old = existing.get(entity)
        first = parse(old["payload"]["first_observed_at"]) if old else when
        revised_admission = admit(row, frozen, version, first_observed_at=when)
        admission = old["payload"]["admission"] if old else revised_admission
        if first < parse(frozen["boundary"]):
            admission = dict(admission, membership="preexisting", reason="observed_before_boundary")
        membership = admission["membership"]
        counts[membership] = counts.get(membership, 0) + 1
        if old and old["payload"]["row"] == row:
            continue
        producer = "unknown"
        source_errors = [name for name, value in row.items() if isinstance(value, dict)]
        try:
            if parse(row["created_at"]) > when:
                source_errors.append("created_at_in_future")
        except (TypeError, ValueError):
            source_errors.append("created_at_missing_or_invalid")
        try:
            if (
                parse(observed["metadata"]["runtime_started_utc"])
                <= parse(row["created_at"])
                <= when
            ):
                producer = "current_runtime_context_only"
        except (KeyError, TypeError, ValueError):
            pass
        payload = {
            "row": row,
            "first_observed_at": timestamp(first),
            "source_observed_at": timestamp(when),
            "read_started_at": observed["read_started_at"],
            "source_path": observed["source_path"],
            "runtime_context": {key: observed["metadata"].get(key) for key in METADATA},
            "producer_attribution": producer,
            "source_validation_errors": source_errors,
            "admission": admission,
            "revision_admission": revised_admission,
            "source_fields_changed": old is not None,
        }
        journal.append("custody", entity, payload)
        added += 1
    last_poll = next((r for r in reversed(records) if r["kind"] == "custody_poll"), None)
    if (
        added
        or last_poll is None
        or when - parse(last_poll["payload"]["source_observed_at"]) >= timedelta(minutes=5)
    ):
        journal.append(
            "custody_poll",
            "singleton",
            {
                "source_observed_at": timestamp(when),
                "read_started_at": observed["read_started_at"],
                "rows_seen": len(observed["rows"]),
                "membership_counts": counts,
                "new_revisions": added,
                "runtime_context": {key: observed["metadata"].get(key) for key in METADATA},
            },
        )
    return added
