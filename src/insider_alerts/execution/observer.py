"""Uncapped operational capture, deliberately without portfolio or outcome evaluation."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import sqlite3
import stat
from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import rfc8785

from insider_alerts.research.bar_feed import HistoricalBarSource

NY = ZoneInfo("America/New_York")
VERSION = "operational-observer-v1"
WINDOW_DAYS = 45
INTERVAL_SECONDS = 300
COLUMNS = (
    "packet_id",
    "accession_number",
    "cik",
    "symbol",
    "signal_at",
    "score",
    "entry_session",
    "lottery_rank",
    "eligible",
    "eligibility_reason",
    "prior_close",
    "median_dollar_volume_20d",
    "planned_quantity",
    "created_at",
)


def utc(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("naive clock")
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def parse(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    utc(result)
    return result.astimezone(UTC)


def digest(value: dict[str, Any]) -> str:
    return hashlib.sha256(rfc8785.dumps(value)).hexdigest()


def safe_path(path: Path) -> Path:
    """Reject symlinks and Windows junctions before resolving a fixed owned path."""
    for part in (path, *path.parents):
        if part.exists() or part.is_symlink():
            info = part.lstat()
            if part.is_symlink() or (
                getattr(info, "st_file_attributes", 0)
                & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
            ):
                raise ValueError("observer paths must not contain links or junctions")
            if part == path and part.is_file() and info.st_nlink != 1:
                raise ValueError("observer paths must not contain hard links")
    return path.resolve()


def snapshot(path: Path) -> list[dict[str, Any]]:
    """No schema initialization, outcomes, or locks retained across output/network work."""
    path = safe_path(path)
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=2)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        conn.execute("BEGIN")
        return [
            dict(row)
            for row in conn.execute(
                f"SELECT {','.join(COLUMNS)} FROM candidates ORDER BY packet_id"
            )
        ]


class Journal:
    def __init__(self, path: Path, *, readonly: bool = False) -> None:
        self.path = safe_path(path)
        self.readonly = readonly

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        mode = "ro" if self.readonly else "rw"
        with closing(
            sqlite3.connect(self.path.as_uri() + f"?mode={mode}", uri=True, timeout=2)
        ) as conn:
            conn.row_factory = sqlite3.Row
            if self.readonly:
                conn.execute("PRAGMA query_only=ON")
            with conn:
                yield conn

    @classmethod
    def activate(cls, path: Path, *, start: datetime, now: datetime, revision: str) -> Journal:
        if start < now + timedelta(hours=2):
            raise ValueError("activation must be at least two hours in the future")
        utc(start)
        utc(now)
        path = safe_path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        # Exclusive file creation: activation never resets or reuses an existing store.
        with path.open("xb"):
            pass
        with closing(sqlite3.connect(path)) as conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE records (
                    sequence INTEGER PRIMARY KEY,
                    kind TEXT NOT NULL,
                    entity TEXT NOT NULL,
                    envelope TEXT NOT NULL,
                    sha256 TEXT NOT NULL UNIQUE
                );
                CREATE INDEX records_entity ON records(kind, entity, sequence);
                CREATE TRIGGER records_no_update BEFORE UPDATE ON records
                    BEGIN SELECT RAISE(ABORT, 'append only'); END;
                CREATE TRIGGER records_no_delete BEFORE DELETE ON records
                    BEGIN SELECT RAISE(ABORT, 'append only'); END;
            """
            )
        journal = cls(path)
        journal.append(
            "activation",
            "singleton",
            {
                "start": utc(start),
                "revision": revision,
                "window_calendar_days": WINDOW_DAYS,
                "scope": "canary_ledger_candidates_only",
                "profit_reporting": False,
                "session_completion_proof": "unavailable_not_required_for_raw_capture",
            },
            now=now,
        )
        return journal

    def records(self, kind: str | None = None) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT envelope,sha256 FROM records WHERE (? IS NULL OR kind=?) ORDER BY sequence",
                (kind, kind),
            ).fetchall()
        return [dict(json.loads(row["envelope"]), sha256=row["sha256"]) for row in rows]

    def validate(self) -> None:
        previous = None
        revisions: dict[tuple[str, str], str] = {}
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM records ORDER BY sequence").fetchall()
        if not rows:
            raise ValueError("observer activation missing")
        for sequence, row in enumerate(rows, 1):
            envelope = json.loads(row["envelope"])
            key = (str(row["kind"]), str(row["entity"]))
            if (
                row["sequence"] != sequence
                or envelope["sequence"] != sequence
                or envelope["version"] != VERSION
                or envelope["previous"] != previous
                or envelope["supersedes"] != revisions.get(key)
                or envelope["kind"] != key[0]
                or envelope["entity"] != key[1]
                or digest(envelope) != row["sha256"]
            ):
                raise ValueError("observer journal integrity failure")
            parse(envelope["observed_at"])
            previous = row["sha256"]
            revisions[key] = previous
        first = json.loads(rows[0]["envelope"])
        if first["kind"] != "activation" or len(self.records("activation")) != 1:
            raise ValueError("invalid observer activation")

    def append(self, kind: str, entity: str, payload: dict[str, Any], *, now: datetime) -> bool:
        observed = utc(now)
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            same = conn.execute(
                "SELECT envelope,sha256 FROM records WHERE kind=? AND entity=? "
                "ORDER BY sequence DESC LIMIT 1",
                (kind, entity),
            ).fetchone()
            old_payload = json.loads(same["envelope"])["payload"] if same else None
            comparable = dict(payload)
            if kind == "bar" and old_payload is not None:
                # Repeated polls attest unchanged prices without duplicating the full path.
                old_payload.pop("attempt_started_at", None)
                comparable.pop("attempt_started_at", None)
                old_payload.pop("request_start_date", None)
                comparable.pop("request_start_date", None)
                old_payload.pop("capture_through_date", None)
                comparable.pop("capture_through_date", None)
            if kind not in {"cycle", "poll", "attempt"} and same and old_payload == comparable:
                return False
            last = conn.execute(
                "SELECT sequence,sha256 FROM records ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
            envelope = {
                "version": VERSION,
                "sequence": last["sequence"] + 1 if last else 1,
                "previous": last["sha256"] if last else None,
                "supersedes": same["sha256"] if same else None,
                "kind": kind,
                "entity": entity,
                "observed_at": observed,
                "payload": payload,
            }
            conn.execute(
                "INSERT INTO records VALUES (?,?,?,?,?)",
                (
                    envelope["sequence"],
                    kind,
                    entity,
                    rfc8785.dumps(envelope).decode(),
                    digest(envelope),
                ),
            )
        return True


@contextmanager
def worker_lock(path: Path) -> Iterator[None]:
    """Independent SQLite writer lock, released by OS on process death; no lease takeover."""
    path = safe_path(path)
    with closing(sqlite3.connect(path, timeout=0)) as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield
        finally:
            conn.rollback()


def valid_symbol(value: object) -> bool:
    return (
        isinstance(value, str)
        and 0 < len(value) <= 32
        and all(char in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-" for char in value)
    )


def capture_candidates(
    journal: Journal, rows: list[dict[str, Any]], *, now: datetime, start: datetime
) -> int:
    added = 0
    for row in rows:
        entity = str(row["packet_id"])
        try:
            signal = parse(str(row["signal_at"]))
            created = parse(str(row["created_at"]))
            if signal < start:
                continue
            # Canary created_at is its cycle-start clock, NOT the insertion/observation clock.
            # A signal arriving during broker awaits can legitimately be later than created_at.
            if signal > now or created > now:
                raise ValueError("source timestamp is in the future")
            # A finite projection can be hashed; malformed rows get a typed receipt, not coercion.
            digest(row)
        except (ValueError, TypeError, OverflowError) as exc:
            journal.append(
                "candidate_missing",
                entity,
                {
                    "reason": "invalid_source_projection",
                    "error_type": type(exc).__name__,
                },
                now=now,
            )
            continue
        added += int(journal.append("candidate", entity, row, now=now))
        if not valid_symbol(row["symbol"]):
            journal.append(
                "candidate_missing",
                entity,
                {
                    "reason": "invalid_symbol",
                },
                now=now,
            )
        if (now.astimezone(NY).date() - signal.astimezone(NY).date()).days > WINDOW_DAYS:
            journal.append(
                "window_closed",
                entity,
                {
                    "reason": "calendar_capture_window_elapsed",
                    "session_coverage_proven": False,
                },
                now=now,
            )
    return added


def io_timeout(now: datetime, maximum: float) -> float:
    local = now.astimezone(NY)
    if 8 <= local.hour < 18:
        raise TimeoutError("off-hours request window ended")
    boundary = local.replace(hour=8, minute=0, second=0, microsecond=0)
    if local.hour >= 18:
        boundary += timedelta(days=1)
    return min(maximum, (boundary.astimezone(UTC) - now.astimezone(UTC)).total_seconds())


async def run_once(
    journal: Journal,
    source_path: Path,
    source: HistoricalBarSource,
    *,
    revision: str,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> dict[str, Any]:
    """Caller owns worker_lock for the entire invocation, including connect and disconnect."""
    journal.validate()
    now = clock()
    utc(now)
    start = parse(journal.records("activation")[0]["payload"]["start"])
    result: dict[str, Any] = {"result": "waiting_activation", "candidates_added": 0}
    if now < start:
        journal.append("cycle", "singleton", dict(result, revision=revision), now=now)
        return result
    try:
        rows = snapshot(source_path)
        now = clock()  # observation time is after the source transaction, never before it
        result["candidates_added"] = capture_candidates(journal, rows, now=now, start=start)
    except (sqlite3.Error, OSError, ValueError) as exc:
        result.update(result="source_unavailable", error_type=type(exc).__name__)
        journal.append("cycle", "singleton", dict(result, revision=revision), now=clock())
        return result

    local = now.astimezone(NY)
    result["result"] = "off_hours_only" if 8 <= local.hour < 18 else "idle"
    attempts = journal.records("attempt")
    if attempts and now < parse(attempts[-1]["observed_at"]) + timedelta(seconds=INTERVAL_SECONDS):
        result["result"] = "paced"
    elif not 8 <= local.hour < 18:
        # First observed identity is authoritative. A changed projection is retained but cannot
        # silently replace candidate identity/eligibility for request planning.
        first: dict[str, dict[str, Any]] = {}
        for record in journal.records("candidate"):
            first.setdefault(record["entity"], record["payload"])
        windows: dict[str, tuple[datetime, datetime]] = {}
        for row in first.values():
            signal = parse(row["signal_at"])
            end = signal + timedelta(days=WINDOW_DAYS)
            symbol = row["symbol"]
            if (
                not valid_symbol(symbol)
                or signal.astimezone(NY).date() >= local.date()
                or local.date() > end.astimezone(NY).date() + timedelta(days=1)
            ):
                continue
            old = windows.get(symbol)
            windows[symbol] = (min(signal, old[0]), max(end, old[1])) if old else (signal, end)
        last_attempt = {r["entity"]: r["sequence"] for r in attempts}
        successes = journal.records("poll")
        done_today = {
            r["entity"]
            for r in successes
            if r["payload"]["request_local_date"] == local.date().isoformat()
            and r["payload"]["result"] == "received"
        }
        symbols = sorted(set(windows) - done_today, key=lambda s: (last_attempt.get(s, 0), s))
        if symbols:
            symbol = symbols[0]
            first_at, through_at = windows[symbol]
            requested = clock()
            request_date = requested.astimezone(NY).date()
            # Re-check the wall-clock window immediately before acquiring the connection.
            if 8 <= requested.astimezone(NY).hour < 18:
                result["result"] = "off_hours_only"
            else:
                start_date = first_at.astimezone(NY).date()
                end_date = min(through_at.astimezone(NY).date(), request_date - timedelta(days=1))
                attempt = {
                    "start_date": start_date.isoformat(),
                    "through_date": end_date.isoformat(),
                    "request_local_date": request_date.isoformat(),
                    "revision": revision,
                    "source": "IBKR_TRADES_RTH_1day",
                    "attempt_started_at": utc(requested),
                    "historical_request_at": None,
                    "request_clock_missing_reason": "adapter_does_not_expose_dispatch_clock",
                }
                journal.append("attempt", symbol, attempt, now=requested)
                rejected = accepted = 0
                accepted_hashes: list[str] = []
                try:
                    connect_timeout = io_timeout(clock(), 15)
                    await asyncio.wait_for(source.connect(), timeout=connect_timeout)
                    if 8 <= clock().astimezone(NY).hour < 18:
                        raise TimeoutError("off-hours request window ended during connection")
                    history_timeout = io_timeout(clock(), 45)
                    batch = await asyncio.wait_for(
                        source.daily_bars(symbol, start_date=start_date),
                        timeout=history_timeout,
                    )
                    received = clock()
                    utc(received)
                    if received < requested:
                        raise ValueError("clock moved backwards during request")
                    rejected = len(batch.rejections)
                    for index, reason in enumerate(batch.rejections):
                        journal.append(
                            "bar_missing",
                            f"{symbol}:{utc(requested)}:{index}",
                            {
                                "reason": "source_rejected",
                                "detail": reason,
                            },
                            now=received,
                        )
                    for bar in batch.bars:
                        values = (bar.open, bar.high, bar.low, bar.close, bar.volume)
                        if (
                            bar.symbol != symbol
                            or any(not math.isfinite(v) for v in values)
                            or min(values[:4]) <= 0
                            or bar.volume < 0
                            or bar.low > min(bar.open, bar.close)
                            or bar.high < max(bar.open, bar.close)
                        ):
                            rejected += 1
                            journal.append(
                                "bar_missing",
                                f"{symbol}:{bar.trade_date}",
                                {
                                    "reason": "invalid_ohlcv_or_symbol",
                                },
                                now=received,
                            )
                            continue
                        if not start_date <= bar.trade_date <= end_date:
                            continue
                        accepted += 1
                        accepted_hashes.append(
                            digest(
                                {
                                    "symbol": symbol,
                                    "date": bar.trade_date.isoformat(),
                                    "ohlcv": list(values),
                                }
                            )
                        )
                        journal.append(
                            "bar",
                            f"{symbol}:{bar.trade_date}",
                            {
                                "symbol": symbol,
                                "date": bar.trade_date.isoformat(),
                                "open": bar.open,
                                "high": bar.high,
                                "low": bar.low,
                                "close": bar.close,
                                "volume": bar.volume,
                                "source": attempt["source"],
                                "attempt_started_at": attempt["attempt_started_at"],
                                "request_start_date": attempt["start_date"],
                                "capture_through_date": attempt["through_date"],
                                "contract_id": None,
                                "contract_id_missing_reason": "adapter_does_not_expose_identity",
                            },
                            now=received,
                        )
                    result["result"] = "received" if accepted and not rejected else "partial"
                except Exception as exc:
                    result.update(result="market_data_unavailable", error_type=type(exc).__name__)
                finally:
                    source.disconnect()
                journal.append(
                    "poll",
                    symbol,
                    dict(
                        attempt,
                        result=result["result"],
                        accepted=accepted,
                        rejected=rejected,
                        accepted_bar_content_hashes=accepted_hashes,
                        error_type=result.get("error_type"),
                        session_coverage_proven=False,
                    ),
                    now=clock(),
                )
    journal.append("cycle", "singleton", dict(result, revision=revision), now=clock())
    return result


def status(journal: Journal, *, now: datetime) -> dict[str, Any]:
    journal.validate()
    records = journal.records()
    cycles = [r for r in records if r["kind"] == "cycle"]
    last = cycles[-1] if cycles else None
    age = (now - parse(last["observed_at"])).total_seconds() if last else None
    return {
        "version": VERSION,
        "activation": records[0]["payload"]["start"],
        "last_cycle_at": last["observed_at"] if last else None,
        "last_cycle": last["payload"] if last else None,
        "heartbeat_fresh": age is not None and 0 <= age <= 900,
        "candidates": len({r["entity"] for r in records if r["kind"] == "candidate"}),
        "bar_versions": sum(r["kind"] == "bar" for r in records),
        "missingness_records": sum(r["kind"].endswith("_missing") for r in records),
        "closed_capture_windows": sum(r["kind"] == "window_closed" for r in records),
        "market_data_failures": sum(
            r["kind"] == "poll" and r["payload"]["result"] != "received" for r in records
        ),
        "full_signal_universe_covered": False,
        "session_coverage_proven": False,
        "portfolio_performance_validated": False,
        "profit_reporting_enabled": False,
    }
