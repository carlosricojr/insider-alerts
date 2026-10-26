"""Exclusive, append-only custody for the bounded commissioning window."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from collections.abc import Callable
from contextlib import closing
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any

import rfc8785

from insider_alerts.execution.observer import safe_path
from insider_alerts.research.commissioning_source import timestamp

FORMAT = "acquisition-commissioning-custody-v1"
APPLICATION_ID = 0x49414333
KINDS = frozenset({"activation", "custody", "custody_poll", "attempt", "response", "cycle", "seal"})
DDL = (
    "CREATE TABLE records(sequence INTEGER PRIMARY KEY, kind TEXT NOT NULL, "
    "entity TEXT NOT NULL, sha256 TEXT NOT NULL UNIQUE, envelope BLOB NOT NULL)",
    "CREATE TRIGGER records_no_update BEFORE UPDATE ON records "
    "BEGIN SELECT RAISE(ABORT,'append-only commissioning journal'); END",
    "CREATE TRIGGER records_no_delete BEFORE DELETE ON records "
    "BEGIN SELECT RAISE(ABORT,'append-only commissioning journal'); END",
)


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def schema(conn: sqlite3.Connection) -> list[tuple[str, str, str]]:
    return conn.execute(
        "SELECT type,name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
    ).fetchall()


@lru_cache(maxsize=1)
def expected_schema() -> list[tuple[str, str, str]]:
    with closing(sqlite3.connect(":memory:")) as conn:
        for sql in DDL:
            conn.execute(sql)
        return schema(conn)


def load(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    prior_entities: dict[tuple[str, str], str] = {}
    attempts: set[str] = set()
    try:
        for sequence, kind, entity, sha, raw in conn.execute(
            "SELECT sequence,kind,entity,sha256,envelope FROM records ORDER BY sequence"
        ):
            if not isinstance(raw, bytes) or len(raw) > 2_000_000 or digest(raw) != sha:
                raise ValueError("commissioning record digest/size mismatch")
            record = json.loads(raw)
            if (
                rfc8785.dumps(record) != raw
                or set(record)
                != {
                    "format",
                    "sequence",
                    "kind",
                    "entity",
                    "previous",
                    "supersedes",
                    "recorded_at",
                    "payload",
                }
                or record["format"] != FORMAT
                or sequence != len(records) + 1
                or type(record["sequence"]) is not int
                or record["sequence"] != sequence
                or record["kind"] != kind
                or record["entity"] != entity
                or kind not in KINDS
                or not isinstance(entity, str)
                or not 0 < len(entity) <= 256
                or not isinstance(record["payload"], dict)
                or record["previous"] != (records[-1]["sha256"] if records else None)
                or record["supersedes"] != prior_entities.get((kind, entity))
                or timestamp(datetime.fromisoformat(record["recorded_at"])) != record["recorded_at"]
                or (records and record["recorded_at"] < records[-1]["recorded_at"])
                or (kind == "activation") != (sequence == 1)
                or (kind in {"activation", "seal"} and entity != "singleton")
                or (
                    kind == "response"
                    and (entity not in attempts or (kind, entity) in prior_entities)
                )
                or (records and records[-1]["kind"] == "seal")
            ):
                raise ValueError("commissioning record invariant failed")
            records.append(dict(record, sha256=sha))
            prior_entities[(kind, entity)] = sha
            if kind == "attempt":
                attempts.add(sha)
    except (KeyError, TypeError, ArithmeticError, RecursionError) as exc:
        raise ValueError("malformed commissioning record") from exc
    if not records:
        raise ValueError("commissioning activation missing")
    return records


class Journal:
    def __init__(self, path: Path, *, clock: Callable[[], datetime] = lambda: datetime.now(UTC)):
        self.path = safe_path(path)
        self.clock = clock

    def connect(self, *, write: bool = False) -> sqlite3.Connection:
        path = safe_path(self.path).resolve(strict=True)
        conn = sqlite3.connect(
            path.as_uri() + ("?mode=rw" if write else "?mode=ro"), uri=True, timeout=2
        )
        try:
            if (
                conn.execute("PRAGMA application_id").fetchone()[0] != APPLICATION_ID
                or conn.execute("PRAGMA user_version").fetchone()[0] != 1
                or conn.execute("PRAGMA journal_mode").fetchone()[0] != "delete"
                or schema(conn) != expected_schema()
                or conn.execute("PRAGMA quick_check").fetchall() != [("ok",)]
            ):
                raise ValueError("foreign or corrupted commissioning journal")
            conn.execute("PRAGMA recursive_triggers=ON")
            conn.execute("PRAGMA synchronous=FULL" if write else "PRAGMA query_only=ON")
            return conn
        except BaseException:
            conn.close()
            raise

    def records(self) -> list[dict[str, Any]]:
        with closing(self.connect()) as conn:
            conn.execute("BEGIN")
            return load(conn)

    @staticmethod
    def _insert(
        conn: sqlite3.Connection,
        records: list[dict[str, Any]],
        kind: str,
        entity: str,
        payload: dict[str, Any],
        recorded_at: str,
    ) -> dict[str, Any]:
        if kind not in KINDS or not 0 < len(entity) <= 256 or not isinstance(payload, dict):
            raise ValueError("invalid commissioning event")
        if bool(records) == (kind == "activation"):
            raise ValueError("activation must be the first and only activation")
        if kind in {"activation", "seal"} and entity != "singleton":
            raise ValueError("invalid singleton event")
        if kind == "response" and not any(
            r["kind"] == "attempt" and r["sha256"] == entity for r in records
        ):
            raise ValueError("response has no preceding attempt")
        if records and (records[-1]["kind"] == "seal" or recorded_at < records[-1]["recorded_at"]):
            raise ValueError("journal sealed or record clock moved backwards")
        previous = next(
            (r for r in reversed(records) if (r["kind"], r["entity"]) == (kind, entity)), None
        )
        record = {
            "format": FORMAT,
            "sequence": len(records) + 1,
            "kind": kind,
            "entity": entity,
            "previous": records[-1]["sha256"] if records else None,
            "supersedes": previous["sha256"] if previous else None,
            "recorded_at": recorded_at,
            "payload": payload,
        }
        raw = rfc8785.dumps(record)
        if len(raw) > 2_000_000:
            raise ValueError("commissioning event too large")
        sha = digest(raw)
        conn.execute(
            "INSERT INTO records VALUES(?,?,?,?,?)", (record["sequence"], kind, entity, sha, raw)
        )
        return dict(record, sha256=sha)

    def append(self, kind: str, entity: str, payload: dict[str, Any]) -> dict[str, Any]:
        with closing(self.connect(write=True)) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            records = load(conn)
            if kind == "response":
                existing = next(
                    (r for r in records if (r["kind"], r["entity"]) == (kind, entity)), None
                )
                if existing is not None:
                    if existing["payload"] != payload:
                        raise ValueError("conflicting response for committed attempt")
                    return existing
            # Payload clocks describe source observation/dispatch. This clock is
            # journal construction inside the transaction, never source time or
            # a guarantee of the exact disk-commit instant.
            return self._insert(conn, records, kind, entity, payload, timestamp(self.clock()))

    @classmethod
    def create(
        cls,
        path: Path,
        manifest: dict[str, Any],
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> Journal:
        path = safe_path(path)
        if path.exists():
            raise FileExistsError("commissioning journal already exists")
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, name = tempfile.mkstemp(
            prefix=".commissioning-", suffix=".sqlite", dir=path.parent
        )
        os.close(descriptor)
        staging = Path(name)
        try:
            with closing(sqlite3.connect(staging)) as conn, conn:
                conn.execute("PRAGMA journal_mode=DELETE")
                conn.execute("PRAGMA synchronous=FULL")
                conn.execute(f"PRAGMA application_id={APPLICATION_ID}")
                conn.execute("PRAGMA user_version=1")
                for sql in DDL:
                    conn.execute(sql)
                cls._insert(conn, [], "activation", "singleton", manifest, timestamp(clock()))
                load(conn)
            with staging.open("r+b") as stream:
                os.fsync(stream.fileno())
            if os.name == "nt":
                os.rename(staging, path)  # Windows refuses replacement atomically.
            else:
                os.link(staging, path)
        finally:
            staging.unlink(missing_ok=True)
        return cls(path, clock=clock)
