"""Append-only completed-bar feed for the prospective research trial."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import math
import sqlite3
import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol
from zoneinfo import ZoneInfo

import rfc8785

from insider_alerts.backtest.models import DailyBar

NEW_YORK = ZoneInfo("America/New_York")
_SOURCE_TIMEOUT_SECONDS = 40.0
BAR_FEED_VERSION = "ibkr-completed-rth-daily-v1"
BAR_POLL_RECEIPT_VERSION = "ibkr-completed-rth-daily-poll-v2"
BAR_FEED_SCHEMA_VERSION = 3


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("bar-feed timestamp cannot be naive")
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical(value: dict[str, Any]) -> bytes:
    return rfc8785.dumps(value)


def _normalized_symbol(symbol: str) -> str:
    normalized = symbol.strip().upper()
    if not normalized or len(normalized) > 32:
        raise ValueError("symbol must contain 1 through 32 characters")
    if any(character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-" for character in normalized):
        raise ValueError("symbol contains an unsupported character")
    return normalized


def _bar_validation_error(bar: DailyBar) -> str | None:
    values = (bar.open, bar.high, bar.low, bar.close, bar.volume)
    if any(not math.isfinite(float(value)) for value in values):
        return "non_finite_ohlcv"
    if min(bar.open, bar.high, bar.low, bar.close) <= 0 or bar.volume < 0:
        return "non_positive_price_or_negative_volume"
    if bar.low > min(bar.open, bar.close) or bar.high < max(bar.open, bar.close):
        return "inconsistent_ohlc_range"
    return None


@dataclass(frozen=True, slots=True)
class BarRequest:
    request_id: str
    symbol: str
    start_date: date
    through_date: date
    requested_at_utc: datetime
    requester: str


@dataclass(frozen=True, slots=True)
class BarFeedResult:
    requests: int
    symbols: int
    observations_added: int
    revisions_added: int
    rejected_bars: int
    failed_symbols: int


@dataclass(frozen=True, slots=True)
class SourceBarBatch:
    bars: tuple[DailyBar, ...]
    rejections: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class BarObservationRecord:
    sequence: int
    bar: DailyBar
    observed_at_utc: datetime
    record_sha256: str


@dataclass(frozen=True, slots=True)
class BarPollReceipt:
    sequence: int
    symbol: str
    polled_at_utc: datetime
    requested_start_date: date
    requested_through_date: date
    completed_through_date: date | None
    returned_bar_count: int
    in_range_bar_count: int
    source_rejection_count: int
    validation_rejection_count: int
    observation_watermark: int | None
    record_sha256: str


class HistoricalBarSource(Protocol):
    async def connect(self) -> None: ...

    async def daily_bars(self, symbol: str, *, start_date: date) -> SourceBarBatch: ...

    def disconnect(self) -> None: ...


class HistoricalBarSessionReset(RuntimeError):
    """The source session must be cleared before another symbol is attempted."""


class BarFeedStore:
    """Content-addressed requests and observations with immutable ledger rows."""

    def __init__(self, path: Path | str, *, initialize: bool = True) -> None:
        self.path = Path(path)
        if initialize:
            self._initialize()

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _initialize(self) -> None:
        with contextlib.closing(self._connect()) as conn, conn:
            schema_table_exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='bar_feed_schema'"
            ).fetchone()
            if schema_table_exists is not None:
                existing = conn.execute(
                    "SELECT schema_version FROM bar_feed_schema WHERE singleton=1"
                ).fetchone()
                if (
                    existing is not None
                    and int(existing["schema_version"]) > BAR_FEED_SCHEMA_VERSION
                ):
                    raise ValueError("bar feed schema is newer than this runtime supports")
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS bar_feed_schema (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    schema_version INTEGER NOT NULL CHECK(schema_version>=1)
                );
                CREATE TABLE IF NOT EXISTS bar_feed_requests (
                    sequence INTEGER NOT NULL UNIQUE,
                    request_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    start_date TEXT NOT NULL,
                    through_date TEXT NOT NULL,
                    requested_at_utc TEXT NOT NULL,
                    requester TEXT NOT NULL,
                    record_sha256 TEXT NOT NULL UNIQUE,
                    record_json BLOB NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS bar_feed_requests_sequence
                BEFORE INSERT ON bar_feed_requests
                WHEN NEW.sequence<>(SELECT COALESCE(MAX(sequence),0)+1 FROM bar_feed_requests)
                BEGIN SELECT RAISE(ABORT, 'bar feed request sequence must be gap-free'); END;
                CREATE TRIGGER IF NOT EXISTS bar_feed_requests_no_update
                BEFORE UPDATE ON bar_feed_requests
                BEGIN SELECT RAISE(ABORT, 'bar feed requests are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS bar_feed_requests_no_delete
                BEFORE DELETE ON bar_feed_requests
                BEGIN SELECT RAISE(ABORT, 'bar feed requests are immutable'); END;

                CREATE TABLE IF NOT EXISTS bar_observations (
                    sequence INTEGER NOT NULL UNIQUE,
                    observation_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    observed_at_utc TEXT NOT NULL,
                    value_sha256 TEXT NOT NULL UNIQUE,
                    record_sha256 TEXT NOT NULL UNIQUE,
                    record_json BLOB NOT NULL
                );
                CREATE INDEX IF NOT EXISTS bar_observations_symbol_date
                ON bar_observations(symbol, trade_date, sequence);
                CREATE TRIGGER IF NOT EXISTS bar_observations_sequence
                BEFORE INSERT ON bar_observations
                WHEN NEW.sequence<>(SELECT COALESCE(MAX(sequence),0)+1 FROM bar_observations)
                BEGIN SELECT RAISE(ABORT, 'bar observation sequence must be gap-free'); END;
                CREATE TRIGGER IF NOT EXISTS bar_observations_time_monotonic
                BEFORE INSERT ON bar_observations
                WHEN EXISTS(
                  SELECT 1 FROM bar_observations
                  WHERE observed_at_utc>NEW.observed_at_utc
                )
                BEGIN SELECT RAISE(ABORT, 'bar observation time cannot move backwards'); END;
                CREATE TRIGGER IF NOT EXISTS bar_observations_no_update
                BEFORE UPDATE ON bar_observations
                BEGIN SELECT RAISE(ABORT, 'bar observations are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS bar_observations_no_delete
                BEFORE DELETE ON bar_observations
                BEGIN SELECT RAISE(ABORT, 'bar observations are immutable'); END;

                CREATE TABLE IF NOT EXISTS bar_feed_health (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    last_worker_heartbeat_utc TEXT NOT NULL,
                    last_result TEXT NOT NULL,
                    last_error TEXT,
                    requests_seen INTEGER NOT NULL,
                    symbols_seen INTEGER NOT NULL,
                    observations_added INTEGER NOT NULL,
                    rejected_bars INTEGER NOT NULL,
                    failed_symbols INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS bar_poll_state (
                    symbol TEXT PRIMARY KEY,
                    last_success_local_date TEXT NOT NULL,
                    earliest_start_date TEXT NOT NULL,
                    last_success_utc TEXT NOT NULL,
                    completed_through_date TEXT
                );

                CREATE TABLE IF NOT EXISTS bar_poll_receipts (
                    sequence INTEGER NOT NULL UNIQUE,
                    receipt_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    polled_at_utc TEXT NOT NULL,
                    requested_start_date TEXT NOT NULL,
                    requested_through_date TEXT NOT NULL,
                    completed_through_date TEXT,
                    returned_bar_count INTEGER NOT NULL CHECK(returned_bar_count>=0),
                    in_range_bar_count INTEGER NOT NULL CHECK(in_range_bar_count>=0),
                    source_rejection_count INTEGER NOT NULL CHECK(source_rejection_count>=0),
                    validation_rejection_count INTEGER NOT NULL
                      CHECK(validation_rejection_count>=0),
                    record_sha256 TEXT NOT NULL UNIQUE,
                    record_json BLOB NOT NULL
                );
                CREATE INDEX IF NOT EXISTS bar_poll_receipts_symbol_time
                ON bar_poll_receipts(symbol, polled_at_utc, sequence);
                CREATE TRIGGER IF NOT EXISTS bar_poll_receipts_sequence
                BEFORE INSERT ON bar_poll_receipts
                WHEN NEW.sequence<>(SELECT COALESCE(MAX(sequence),0)+1 FROM bar_poll_receipts)
                BEGIN SELECT RAISE(ABORT, 'bar poll receipt sequence must be gap-free'); END;
                CREATE TRIGGER IF NOT EXISTS bar_poll_receipts_time_monotonic
                BEFORE INSERT ON bar_poll_receipts
                WHEN EXISTS(SELECT 1 FROM bar_poll_receipts WHERE polled_at_utc>NEW.polled_at_utc)
                BEGIN SELECT RAISE(ABORT, 'bar poll receipt time cannot move backwards'); END;
                CREATE TRIGGER IF NOT EXISTS bar_poll_receipts_no_update
                BEFORE UPDATE ON bar_poll_receipts
                BEGIN SELECT RAISE(ABORT, 'bar poll receipts are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS bar_poll_receipts_no_delete
                BEFORE DELETE ON bar_poll_receipts
                BEGIN SELECT RAISE(ABORT, 'bar poll receipts are immutable'); END;

                CREATE TABLE IF NOT EXISTS bar_feed_failures (
                    sequence INTEGER NOT NULL UNIQUE,
                    failure_id TEXT PRIMARY KEY,
                    occurred_at_utc TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    category TEXT NOT NULL,
                    record_sha256 TEXT NOT NULL UNIQUE,
                    record_json BLOB NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS bar_feed_failures_sequence
                BEFORE INSERT ON bar_feed_failures
                WHEN NEW.sequence<>(SELECT COALESCE(MAX(sequence),0)+1 FROM bar_feed_failures)
                BEGIN SELECT RAISE(ABORT, 'bar feed failure sequence must be gap-free'); END;
                CREATE TRIGGER IF NOT EXISTS bar_feed_failures_no_update
                BEFORE UPDATE ON bar_feed_failures
                BEGIN SELECT RAISE(ABORT, 'bar feed failures are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS bar_feed_failures_no_delete
                BEFORE DELETE ON bar_feed_failures
                BEGIN SELECT RAISE(ABORT, 'bar feed failures are immutable'); END;
                """
            )
            conn.execute(
                """
                INSERT INTO bar_feed_schema(singleton,schema_version) VALUES(1,?)
                ON CONFLICT(singleton) DO UPDATE SET
                  schema_version=MAX(bar_feed_schema.schema_version,excluded.schema_version)
                """,
                (BAR_FEED_SCHEMA_VERSION,),
            )
            poll_columns = {
                str(row["name"]) for row in conn.execute("PRAGMA table_info(bar_poll_state)")
            }
            if "completed_through_date" not in poll_columns:
                conn.execute("ALTER TABLE bar_poll_state ADD COLUMN completed_through_date TEXT")

    def request(self, request: BarRequest) -> str:
        symbol = _normalized_symbol(request.symbol)
        if not request.request_id.strip():
            raise ValueError("request_id cannot be empty")
        if request.start_date > request.through_date:
            raise ValueError("bar request start_date cannot follow through_date")
        if not request.requester.strip():
            raise ValueError("bar request requester cannot be empty")
        record: dict[str, object] = {
            "contract_version": BAR_FEED_VERSION,
            "request_id": request.request_id,
            "symbol": symbol,
            "start_date": request.start_date.isoformat(),
            "through_date": request.through_date.isoformat(),
            "requested_at_utc": _utc_text(request.requested_at_utc),
            "requester": request.requester,
        }
        encoded = _canonical(record)
        digest = _sha256(encoded)
        with contextlib.closing(self._connect()) as conn, conn:
            existing = conn.execute(
                "SELECT record_sha256 FROM bar_feed_requests WHERE request_id=?",
                (request.request_id,),
            ).fetchone()
            if existing is not None:
                if str(existing["record_sha256"]) != digest:
                    raise ValueError("request_id already binds different bar-feed content")
                return digest
            sequence = int(
                conn.execute(
                    "SELECT COALESCE(MAX(sequence),0)+1 FROM bar_feed_requests"
                ).fetchone()[0]
            )
            conn.execute(
                """
                INSERT INTO bar_feed_requests(
                    sequence,request_id,symbol,start_date,through_date,requested_at_utc,
                    requester,record_sha256,record_json
                ) VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (
                    sequence,
                    request.request_id,
                    symbol,
                    request.start_date.isoformat(),
                    request.through_date.isoformat(),
                    _utc_text(request.requested_at_utc),
                    request.requester,
                    digest,
                    encoded,
                ),
            )
        return digest

    def has_request(self, request_id: str) -> bool:
        """Return whether an immutable request identity already exists."""

        if not request_id.strip():
            raise ValueError("request_id cannot be empty")
        with contextlib.closing(self._connect()) as conn:
            return (
                conn.execute(
                    "SELECT 1 FROM bar_feed_requests WHERE request_id=?", (request_id,)
                ).fetchone()
                is not None
            )

    def pending_requests(
        self,
        *,
        as_of: date,
        completed_through_date: date | None = None,
    ) -> list[BarRequest]:
        with contextlib.closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT * FROM bar_feed_requests request
                WHERE NOT EXISTS(
                    SELECT 1 FROM bar_observations observation
                    WHERE observation.symbol=request.symbol
                      AND observation.trade_date=request.through_date
                  )
                ORDER BY sequence
                """
            ).fetchall()
            poll_rows = {
                str(row["symbol"]): row
                for row in conn.execute("SELECT * FROM bar_poll_state").fetchall()
            }
        requests = [self._verify_request_row(row) for row in rows]
        earliest_by_symbol: dict[str, date] = {}
        latest_by_symbol: dict[str, date] = {}
        for request in requests:
            earliest_by_symbol[request.symbol] = min(
                request.start_date,
                earliest_by_symbol.get(request.symbol, request.start_date),
            )
            latest_by_symbol[request.symbol] = max(
                request.through_date,
                latest_by_symbol.get(request.symbol, request.through_date),
            )
        due_symbols = {
            symbol
            for symbol, earliest in earliest_by_symbol.items()
            if symbol not in poll_rows
            or date.fromisoformat(str(poll_rows[symbol]["last_success_local_date"])) < as_of
            or earliest < date.fromisoformat(str(poll_rows[symbol]["earliest_start_date"]))
            or (
                completed_through_date is not None
                and earliest <= completed_through_date <= latest_by_symbol[symbol]
                and (
                    poll_rows[symbol]["completed_through_date"] is None
                    or date.fromisoformat(str(poll_rows[symbol]["completed_through_date"]))
                    < completed_through_date
                )
            )
        }
        return [request for request in requests if request.symbol in due_symbols]

    @staticmethod
    def _verify_request_row(row: sqlite3.Row) -> BarRequest:
        raw = bytes(row["record_json"])
        if _sha256(raw) != str(row["record_sha256"]):
            raise ValueError("bar-feed request stored bytes failed integrity check")
        record = json.loads(raw)
        if not isinstance(record, dict) or _canonical(record) != raw:
            raise ValueError("bar-feed request is not canonical JSON")
        expected = {
            "contract_version": BAR_FEED_VERSION,
            "request_id": str(row["request_id"]),
            "symbol": str(row["symbol"]),
            "start_date": str(row["start_date"]),
            "through_date": str(row["through_date"]),
            "requested_at_utc": str(row["requested_at_utc"]),
            "requester": str(row["requester"]),
        }
        if record != expected:
            raise ValueError("bar-feed request columns do not match immutable record")
        requested_at = datetime.fromisoformat(expected["requested_at_utc"].replace("Z", "+00:00"))
        if requested_at.tzinfo is None:
            raise ValueError("bar-feed request timestamp is naive")
        return BarRequest(
            request_id=expected["request_id"],
            symbol=expected["symbol"],
            start_date=date.fromisoformat(expected["start_date"]),
            through_date=date.fromisoformat(expected["through_date"]),
            requested_at_utc=requested_at.astimezone(UTC),
            requester=expected["requester"],
        )

    def record_successful_poll(
        self,
        symbol: str,
        *,
        local_date: date,
        earliest_start_date: date,
        requested_through_date: date,
        completed_through_date: date | None,
        now: datetime,
        returned_bar_count: int,
        in_range_bar_count: int,
        source_rejection_count: int,
        validation_rejection_count: int,
    ) -> str:
        normalized = _normalized_symbol(symbol)
        counts = (
            returned_bar_count,
            in_range_bar_count,
            source_rejection_count,
            validation_rejection_count,
        )
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in counts
        ):
            raise ValueError("bar poll receipt counts must be non-negative integers")
        if in_range_bar_count > returned_bar_count:
            raise ValueError("in-range bar count cannot exceed returned bar count")
        if earliest_start_date > requested_through_date:
            raise ValueError("bar poll receipt start date cannot follow through date")
        if local_date != now.astimezone(NEW_YORK).date():
            raise ValueError("bar poll receipt local date does not match poll timestamp")
        if completed_through_date is not None and completed_through_date > local_date:
            raise ValueError("bar poll receipt completed-through date cannot be in the future")
        with contextlib.closing(self._connect()) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            observation_watermark = int(
                conn.execute(
                    "SELECT COALESCE(MAX(sequence),0) FROM bar_observations"
                ).fetchone()[0]
            )
            record: dict[str, object] = {
                "contract_version": BAR_POLL_RECEIPT_VERSION,
                "symbol": normalized,
                "polled_at_utc": _utc_text(now),
                "requested_start_date": earliest_start_date.isoformat(),
                "requested_through_date": requested_through_date.isoformat(),
                "completed_through_date": (
                    completed_through_date.isoformat() if completed_through_date else None
                ),
                "returned_bar_count": returned_bar_count,
                "in_range_bar_count": in_range_bar_count,
                "source_rejection_count": source_rejection_count,
                "validation_rejection_count": validation_rejection_count,
                "observation_watermark": observation_watermark,
            }
            encoded = _canonical(record)
            digest = _sha256(encoded)
            sequence = int(
                conn.execute(
                    "SELECT COALESCE(MAX(sequence),0)+1 FROM bar_poll_receipts"
                ).fetchone()[0]
            )
            conn.execute(
                """
                INSERT INTO bar_poll_receipts(
                    sequence,receipt_id,symbol,polled_at_utc,requested_start_date,
                    requested_through_date,completed_through_date,returned_bar_count,
                    in_range_bar_count,source_rejection_count,validation_rejection_count,
                    record_sha256,record_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    sequence,
                    str(uuid.uuid5(uuid.NAMESPACE_URL, digest)),
                    normalized,
                    _utc_text(now),
                    earliest_start_date.isoformat(),
                    requested_through_date.isoformat(),
                    completed_through_date.isoformat() if completed_through_date else None,
                    returned_bar_count,
                    in_range_bar_count,
                    source_rejection_count,
                    validation_rejection_count,
                    digest,
                    encoded,
                ),
            )
            conn.execute(
                """
                INSERT INTO bar_poll_state(
                    symbol,last_success_local_date,earliest_start_date,last_success_utc,
                    completed_through_date
                ) VALUES(?,?,?,?,?)
                ON CONFLICT(symbol) DO UPDATE SET
                  last_success_local_date=excluded.last_success_local_date,
                  earliest_start_date=MIN(
                    bar_poll_state.earliest_start_date,excluded.earliest_start_date
                  ),
                  last_success_utc=excluded.last_success_utc,
                  completed_through_date=CASE
                    WHEN bar_poll_state.completed_through_date IS NULL
                      THEN excluded.completed_through_date
                    WHEN excluded.completed_through_date IS NULL
                      THEN bar_poll_state.completed_through_date
                    ELSE MAX(
                      bar_poll_state.completed_through_date,excluded.completed_through_date
                    )
                  END
                """,
                (
                    normalized,
                    local_date.isoformat(),
                    earliest_start_date.isoformat(),
                    _utc_text(now),
                    completed_through_date.isoformat() if completed_through_date else None,
                ),
            )
        return digest

    def fair_symbol_order(self, symbols: Sequence[str]) -> list[str]:
        """Prioritize never-attempted and least-recently-attempted symbols."""

        normalized = sorted({_normalized_symbol(symbol) for symbol in symbols})
        if not normalized:
            return []
        placeholders = ",".join("?" for _ in normalized)
        with contextlib.closing(self._connect()) as conn:
            failure_times = {
                str(row["symbol"]): str(row["attempted_at"])
                for row in conn.execute(
                    f"""
                    SELECT symbol,MAX(occurred_at_utc) attempted_at
                    FROM bar_feed_failures WHERE symbol IN ({placeholders}) GROUP BY symbol
                    """,
                    normalized,
                )
            }
            success_times = {
                str(row["symbol"]): str(row["last_success_utc"])
                for row in conn.execute(
                    f"SELECT symbol,last_success_utc FROM bar_poll_state "
                    f"WHERE symbol IN ({placeholders})",
                    normalized,
                )
            }

        def last_attempt(symbol: str) -> str:
            return max(failure_times.get(symbol, ""), success_times.get(symbol, ""))

        return sorted(normalized, key=lambda symbol: (last_attempt(symbol), symbol))

    @staticmethod
    def _insert_failure(
        conn: sqlite3.Connection,
        *,
        now: datetime,
        symbol: str,
        category: str,
        detail: str,
    ) -> None:
        normalized = _normalized_symbol(symbol)
        record: dict[str, Any] = {
            "contract_version": BAR_FEED_VERSION,
            "occurred_at_utc": _utc_text(now),
            "symbol": normalized,
            "category": category,
            "detail": detail[:2000],
        }
        encoded = _canonical(record)
        digest = _sha256(encoded)
        sequence = int(
            conn.execute("SELECT COALESCE(MAX(sequence),0)+1 FROM bar_feed_failures").fetchone()[0]
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO bar_feed_failures(
                sequence,failure_id,occurred_at_utc,symbol,category,record_sha256,record_json
            ) VALUES(?,?,?,?,?,?,?)
            """,
            (
                sequence,
                str(uuid.uuid5(uuid.NAMESPACE_URL, digest)),
                _utc_text(now),
                normalized,
                category,
                digest,
                encoded,
            ),
        )

    def record_failure(
        self,
        *,
        now: datetime,
        symbol: str,
        category: str,
        detail: str,
    ) -> None:
        with contextlib.closing(self._connect()) as conn, conn:
            self._insert_failure(
                conn,
                now=now,
                symbol=symbol,
                category=category,
                detail=detail,
            )

    def append_completed(
        self,
        bars: Sequence[DailyBar],
        *,
        observed_at_utc: datetime,
        completed_through_date: date | None = None,
    ) -> tuple[int, int, int]:
        """Append finite bars proven complete by date or strictly before today.

        The first exact value is de-duplicated forever. A changed value for the same symbol/date
        is an append-only revision and never replaces the first-observed value.
        """

        observed_text = _utc_text(observed_at_utc)
        completed_before = observed_at_utc.astimezone(NEW_YORK).date()
        if completed_through_date is not None and completed_through_date > completed_before:
            raise ValueError("completed-through date cannot be in the future")
        added = revisions = rejected = 0
        with contextlib.closing(self._connect()) as conn, conn:
            for bar in sorted(bars, key=lambda item: (item.symbol.upper(), item.trade_date)):
                if bar.trade_date >= completed_before and (
                    completed_through_date is None or bar.trade_date > completed_through_date
                ):
                    continue
                symbol = _normalized_symbol(bar.symbol)
                validation_error = _bar_validation_error(bar)
                if validation_error is not None:
                    self._insert_failure(
                        conn,
                        now=observed_at_utc,
                        symbol=symbol,
                        category="invalid_source_bar",
                        detail=f"{bar.trade_date.isoformat()}:{validation_error}",
                    )
                    rejected += 1
                    continue
                value_record: dict[str, object] = {
                    "source": "IBKR",
                    "bar_size": "1 day",
                    "what_to_show": "TRADES",
                    "regular_trading_hours_only": True,
                    "symbol": symbol,
                    "trade_date": bar.trade_date.isoformat(),
                    "open": float(bar.open),
                    "high": float(bar.high),
                    "low": float(bar.low),
                    "close": float(bar.close),
                    "volume": float(bar.volume),
                }
                value_sha = _sha256(_canonical(value_record))
                if conn.execute(
                    "SELECT 1 FROM bar_observations WHERE value_sha256=?", (value_sha,)
                ).fetchone():
                    continue
                prior = conn.execute(
                    "SELECT 1 FROM bar_observations WHERE symbol=? AND trade_date=? LIMIT 1",
                    (symbol, bar.trade_date.isoformat()),
                ).fetchone()
                record: dict[str, object] = {
                    "contract_version": BAR_FEED_VERSION,
                    "observed_at_utc": observed_text,
                    "value": value_record,
                    "value_sha256": value_sha,
                }
                record_sha = _sha256(_canonical(record))
                observation_id = str(uuid.uuid5(uuid.NAMESPACE_URL, record_sha))
                sequence = int(
                    conn.execute(
                        "SELECT COALESCE(MAX(sequence),0)+1 FROM bar_observations"
                    ).fetchone()[0]
                )
                conn.execute(
                    """
                    INSERT INTO bar_observations(
                        sequence,observation_id,symbol,trade_date,observed_at_utc,
                        value_sha256,record_sha256,record_json
                    ) VALUES(?,?,?,?,?,?,?,?)
                    """,
                    (
                        sequence,
                        observation_id,
                        symbol,
                        bar.trade_date.isoformat(),
                        observed_text,
                        value_sha,
                        record_sha,
                        _canonical(record),
                    ),
                )
                added += 1
                revisions += int(prior is not None)
        return added, revisions, rejected

    def first_observed_bars(
        self,
        symbol: str,
        *,
        start_date: date | None = None,
        through_date: date | None = None,
        max_sequence: int | None = None,
    ) -> list[DailyBar]:
        return [
            record.bar
            for record in self.first_observed_bar_records(
                symbol,
                start_date=start_date,
                through_date=through_date,
                max_sequence=max_sequence,
            )
        ]

    def observation_watermark(self) -> int:
        with contextlib.closing(self._connect()) as conn:
            return int(
                conn.execute("SELECT COALESCE(MAX(sequence),0) FROM bar_observations").fetchone()[0]
            )

    def first_observed_bar_records(
        self,
        symbol: str,
        *,
        start_date: date | None = None,
        through_date: date | None = None,
        max_sequence: int | None = None,
    ) -> list[BarObservationRecord]:
        normalized = _normalized_symbol(symbol)
        clauses = ["symbol=?"]
        parameters: list[object] = [normalized]
        if start_date is not None:
            clauses.append("trade_date>=?")
            parameters.append(start_date.isoformat())
        if through_date is not None:
            clauses.append("trade_date<=?")
            parameters.append(through_date.isoformat())
        if max_sequence is not None:
            if (
                isinstance(max_sequence, bool)
                or not isinstance(max_sequence, int)
                or max_sequence < 0
            ):
                raise ValueError("bar observation watermark must be a non-negative integer")
            clauses.append("sequence<=?")
            parameters.append(max_sequence)
        with contextlib.closing(self._connect()) as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM bar_observations
                WHERE {" AND ".join(clauses)}
                  AND sequence=(
                    SELECT MIN(first_seen.sequence) FROM bar_observations first_seen
                    WHERE first_seen.symbol=bar_observations.symbol
                      AND first_seen.trade_date=bar_observations.trade_date
                      {"AND first_seen.sequence<=?" if max_sequence is not None else ""}
                  )
                ORDER BY trade_date
                """,
                [*parameters, *([max_sequence] if max_sequence is not None else [])],
            ).fetchall()
        output: list[BarObservationRecord] = []
        for row in rows:
            output.append(
                BarObservationRecord(
                    sequence=int(row["sequence"]),
                    bar=self._verify_observation_row(row),
                    observed_at_utc=datetime.fromisoformat(
                        str(row["observed_at_utc"]).replace("Z", "+00:00")
                    ).astimezone(UTC),
                    record_sha256=str(row["record_sha256"]),
                )
            )
        return output

    @staticmethod
    def _verify_poll_receipt_row(row: sqlite3.Row) -> BarPollReceipt:
        raw = bytes(row["record_json"])
        digest = str(row["record_sha256"])
        if _sha256(raw) != digest:
            raise ValueError("bar poll receipt stored bytes failed integrity check")
        record = json.loads(raw)
        if not isinstance(record, dict) or _canonical(record) != raw:
            raise ValueError("bar poll receipt is not canonical JSON")
        contract_version = record.get("contract_version")
        if contract_version not in {BAR_FEED_VERSION, BAR_POLL_RECEIPT_VERSION}:
            raise ValueError("bar poll receipt contract version is unsupported")
        expected: dict[str, Any] = {
            "contract_version": contract_version,
            "symbol": str(row["symbol"]),
            "polled_at_utc": str(row["polled_at_utc"]),
            "requested_start_date": str(row["requested_start_date"]),
            "requested_through_date": str(row["requested_through_date"]),
            "completed_through_date": row["completed_through_date"],
            "returned_bar_count": int(row["returned_bar_count"]),
            "in_range_bar_count": int(row["in_range_bar_count"]),
            "source_rejection_count": int(row["source_rejection_count"]),
            "validation_rejection_count": int(row["validation_rejection_count"]),
        }
        if contract_version == BAR_POLL_RECEIPT_VERSION:
            expected["observation_watermark"] = record.get("observation_watermark")
        if (
            record != expected
            or str(uuid.uuid5(uuid.NAMESPACE_URL, digest)) != str(row["receipt_id"])
        ):
            raise ValueError("bar poll receipt columns do not match immutable record")
        count_keys = (
            "returned_bar_count",
            "in_range_bar_count",
            "source_rejection_count",
            "validation_rejection_count",
        )
        if any(
            isinstance(record.get(key), bool) or not isinstance(record.get(key), int)
            for key in count_keys
        ):
            raise ValueError("bar poll receipt count types are invalid")
        polled_at = datetime.fromisoformat(expected["polled_at_utc"].replace("Z", "+00:00"))
        if polled_at.tzinfo is None:
            raise ValueError("bar poll receipt timestamp is naive")
        start = date.fromisoformat(expected["requested_start_date"])
        through = date.fromisoformat(expected["requested_through_date"])
        if start > through:
            raise ValueError("bar poll receipt date range is invalid")
        completed_through = (
            date.fromisoformat(str(expected["completed_through_date"]))
            if expected["completed_through_date"] is not None
            else None
        )
        if (
            completed_through is not None
            and completed_through > polled_at.astimezone(NEW_YORK).date()
        ):
            raise ValueError("bar poll receipt completed-through date is in the future")
        counts = (
            expected["returned_bar_count"],
            expected["in_range_bar_count"],
            expected["source_rejection_count"],
            expected["validation_rejection_count"],
        )
        observation_watermark = expected.get("observation_watermark")
        if any(value < 0 for value in counts) or expected["in_range_bar_count"] > expected[
            "returned_bar_count"
        ] or (
            contract_version == BAR_POLL_RECEIPT_VERSION
            and (
                isinstance(observation_watermark, bool)
                or not isinstance(observation_watermark, int)
                or observation_watermark < 0
            )
        ):
            raise ValueError("bar poll receipt counts are invalid")
        return BarPollReceipt(
            sequence=int(row["sequence"]),
            symbol=expected["symbol"],
            polled_at_utc=polled_at.astimezone(UTC),
            requested_start_date=start,
            requested_through_date=through,
            completed_through_date=completed_through,
            returned_bar_count=expected["returned_bar_count"],
            in_range_bar_count=expected["in_range_bar_count"],
            source_rejection_count=expected["source_rejection_count"],
            validation_rejection_count=expected["validation_rejection_count"],
            observation_watermark=observation_watermark,
            record_sha256=digest,
        )

    def poll_receipt_watermark(self) -> int:
        with contextlib.closing(self._connect()) as conn:
            return int(
                conn.execute(
                    "SELECT COALESCE(MAX(sequence),0) FROM bar_poll_receipts"
                ).fetchone()[0]
            )

    def poll_receipts(
        self,
        symbol: str | None = None,
        *,
        max_sequence: int | None = None,
    ) -> list[BarPollReceipt]:
        parameters: tuple[object, ...] = ()
        clauses: list[str] = []
        if symbol is not None:
            clauses.append("symbol=?")
            parameters = (_normalized_symbol(symbol),)
        if max_sequence is not None:
            if (
                isinstance(max_sequence, bool)
                or not isinstance(max_sequence, int)
                or max_sequence < 0
            ):
                raise ValueError("bar poll receipt watermark must be a non-negative integer")
            clauses.append("sequence<=?")
            parameters = (*parameters, max_sequence)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with contextlib.closing(self._connect()) as conn:
            rows = conn.execute(
                f"SELECT * FROM bar_poll_receipts {where} ORDER BY sequence",
                parameters,
            ).fetchall()
        return [self._verify_poll_receipt_row(row) for row in rows]

    @staticmethod
    def _verify_observation_row(row: sqlite3.Row) -> DailyBar:
        raw = bytes(row["record_json"])
        record_sha = str(row["record_sha256"])
        if _sha256(raw) != record_sha:
            raise ValueError("bar observation stored bytes failed integrity check")
        record = json.loads(raw)
        if not isinstance(record, dict) or _canonical(record) != raw:
            raise ValueError("bar observation is not canonical JSON")
        if record.get("contract_version") != BAR_FEED_VERSION:
            raise ValueError("bar observation contract version mismatch")
        value = record.get("value")
        if not isinstance(value, dict):
            raise ValueError("bar observation value is missing")
        value_sha = _sha256(_canonical(value))
        if value_sha != record.get("value_sha256") or value_sha != str(row["value_sha256"]):
            raise ValueError("bar observation value digest mismatch")
        if str(uuid.uuid5(uuid.NAMESPACE_URL, record_sha)) != str(row["observation_id"]):
            raise ValueError("bar observation identity mismatch")
        if (
            value.get("source") != "IBKR"
            or value.get("bar_size") != "1 day"
            or value.get("what_to_show") != "TRADES"
            or value.get("regular_trading_hours_only") is not True
            or str(value.get("symbol")) != str(row["symbol"])
            or str(value.get("trade_date")) != str(row["trade_date"])
            or str(record.get("observed_at_utc")) != str(row["observed_at_utc"])
        ):
            raise ValueError("bar observation columns or provenance do not match immutable record")
        bar = DailyBar(
            symbol=str(value["symbol"]),
            trade_date=date.fromisoformat(str(value["trade_date"])),
            open=float(value["open"]),
            high=float(value["high"]),
            low=float(value["low"]),
            close=float(value["close"]),
            volume=float(value["volume"]),
        )
        if _bar_validation_error(bar) is not None:
            raise ValueError("bar observation contains invalid OHLCV values")
        return bar

    @staticmethod
    def _verify_failure_row(row: sqlite3.Row) -> None:
        raw = bytes(row["record_json"])
        digest = str(row["record_sha256"])
        if _sha256(raw) != digest:
            raise ValueError("bar-feed failure stored bytes failed integrity check")
        record = json.loads(raw)
        if not isinstance(record, dict) or _canonical(record) != raw:
            raise ValueError("bar-feed failure is not canonical JSON")
        if (
            record.get("contract_version") != BAR_FEED_VERSION
            or str(record.get("occurred_at_utc")) != str(row["occurred_at_utc"])
            or str(record.get("symbol")) != str(row["symbol"])
            or str(record.get("category")) != str(row["category"])
            or str(uuid.uuid5(uuid.NAMESPACE_URL, digest)) != str(row["failure_id"])
        ):
            raise ValueError("bar-feed failure columns do not match immutable record")

    @staticmethod
    def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
        return (
            conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
            is not None
        )

    @classmethod
    def _poll_receipt_rows(cls, conn: sqlite3.Connection) -> list[sqlite3.Row]:
        receipts_exist = cls._table_exists(conn, "bar_poll_receipts")
        schema_version: int | None = None
        if cls._table_exists(conn, "bar_feed_schema"):
            row = conn.execute(
                "SELECT schema_version FROM bar_feed_schema WHERE singleton=1"
            ).fetchone()
            if row is None:
                raise ValueError("bar feed schema marker is missing")
            schema_version = int(row["schema_version"])
            if schema_version < 1 or schema_version > BAR_FEED_SCHEMA_VERSION:
                raise ValueError("bar feed schema version is unsupported")
        if not receipts_exist:
            if schema_version is not None and schema_version >= 2:
                raise ValueError("bar poll receipt table is missing after schema migration")
            return []
        return conn.execute("SELECT * FROM bar_poll_receipts ORDER BY sequence").fetchall()

    def validate_integrity(self) -> None:
        with contextlib.closing(self._connect()) as conn:
            request_rows = conn.execute(
                "SELECT * FROM bar_feed_requests ORDER BY sequence"
            ).fetchall()
            observation_rows = conn.execute(
                "SELECT * FROM bar_observations ORDER BY sequence"
            ).fetchall()
            failure_rows = conn.execute(
                "SELECT * FROM bar_feed_failures ORDER BY sequence"
            ).fetchall()
            poll_receipt_rows = self._poll_receipt_rows(conn)
        if [int(row["sequence"]) for row in request_rows] != list(range(1, len(request_rows) + 1)):
            raise ValueError("bar-feed request sequence is not gap-free")
        if [int(row["sequence"]) for row in observation_rows] != list(
            range(1, len(observation_rows) + 1)
        ):
            raise ValueError("bar observation sequence is not gap-free")
        if [int(row["sequence"]) for row in failure_rows] != list(range(1, len(failure_rows) + 1)):
            raise ValueError("bar-feed failure sequence is not gap-free")
        if [int(row["sequence"]) for row in poll_receipt_rows] != list(
            range(1, len(poll_receipt_rows) + 1)
        ):
            raise ValueError("bar poll receipt sequence is not gap-free")
        for row in request_rows:
            self._verify_request_row(row)
        for row in observation_rows:
            self._verify_observation_row(row)
        observed_times = [str(row["observed_at_utc"]) for row in observation_rows]
        if observed_times != sorted(observed_times):
            raise ValueError("bar observation timestamps move backwards")
        for row in failure_rows:
            self._verify_failure_row(row)
        poll_times: list[datetime] = []
        for row in poll_receipt_rows:
            poll_times.append(self._verify_poll_receipt_row(row).polled_at_utc)
        if poll_times != sorted(poll_times):
            raise ValueError("bar poll receipt timestamps move backwards")

    def write_health(
        self,
        *,
        now: datetime,
        result: str,
        error: str | None,
        requests_seen: int,
        symbols_seen: int,
        observations_added: int,
        rejected_bars: int,
        failed_symbols: int,
    ) -> None:
        with contextlib.closing(self._connect()) as conn, conn:
            conn.execute(
                """
                INSERT INTO bar_feed_health(
                    singleton,last_worker_heartbeat_utc,last_result,last_error,
                    requests_seen,symbols_seen,observations_added,rejected_bars,failed_symbols
                ) VALUES(1,?,?,?,?,?,?,?,?)
                ON CONFLICT(singleton) DO UPDATE SET
                  last_worker_heartbeat_utc=excluded.last_worker_heartbeat_utc,
                  last_result=excluded.last_result,
                  last_error=excluded.last_error,
                  requests_seen=excluded.requests_seen,
                  symbols_seen=excluded.symbols_seen,
                  observations_added=excluded.observations_added,
                  rejected_bars=excluded.rejected_bars,
                  failed_symbols=excluded.failed_symbols
                """,
                (
                    _utc_text(now),
                    result,
                    error,
                    requests_seen,
                    symbols_seen,
                    observations_added,
                    rejected_bars,
                    failed_symbols,
                ),
            )

    def status(self, *, now: datetime | None = None) -> dict[str, object]:
        if now is not None and now.tzinfo is None:
            raise ValueError("bar-feed status time cannot be naive")
        today = (now or datetime.now(UTC)).astimezone(NEW_YORK).date()
        overdue_before = today - timedelta(days=30)
        with contextlib.closing(self._connect()) as conn:
            request_count = int(
                conn.execute("SELECT COUNT(*) FROM bar_feed_requests").fetchone()[0]
            )
            observation_count = int(
                conn.execute("SELECT COUNT(*) FROM bar_observations").fetchone()[0]
            )
            revision_count = int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM bar_observations current
                    WHERE EXISTS(
                      SELECT 1 FROM bar_observations prior
                      WHERE prior.symbol=current.symbol AND prior.trade_date=current.trade_date
                        AND prior.sequence<current.sequence
                    )
                    """
                ).fetchone()[0]
            )
            failure_count = int(
                conn.execute("SELECT COUNT(*) FROM bar_feed_failures").fetchone()[0]
            )
            poll_receipt_count = (
                int(conn.execute("SELECT COUNT(*) FROM bar_poll_receipts").fetchone()[0])
                if self._table_exists(conn, "bar_poll_receipts")
                else 0
            )
            unresolved_count = int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM bar_feed_requests request
                    WHERE NOT EXISTS(
                      SELECT 1 FROM bar_observations observation
                      WHERE observation.symbol=request.symbol
                        AND observation.trade_date=request.through_date
                    )
                    """
                ).fetchone()[0]
            )
            overdue_count = int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM bar_feed_requests request
                    WHERE request.through_date<? AND NOT EXISTS(
                      SELECT 1 FROM bar_observations observation
                      WHERE observation.symbol=request.symbol
                        AND observation.trade_date=request.through_date
                    )
                    """,
                    (overdue_before.isoformat(),),
                ).fetchone()[0]
            )
            health = conn.execute("SELECT * FROM bar_feed_health WHERE singleton=1").fetchone()
        try:
            self.validate_integrity()
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            integrity_status = "invalid"
        else:
            integrity_status = "valid"
        return {
            "request_count": request_count,
            "observation_count": observation_count,
            "revision_count": revision_count,
            "failure_count": failure_count,
            "poll_receipt_count": poll_receipt_count,
            "unresolved_request_count": unresolved_count,
            "overdue_request_count": overdue_count,
            "integrity_status": integrity_status,
            "health": dict(health) if health is not None else None,
        }


def bar_feed_status(path: Path | str) -> dict[str, object]:
    selected = Path(path)
    if not selected.is_file():
        return {
            "exists": False,
            "path": str(selected),
            "integrity_status": "missing",
        }
    try:
        status = BarFeedStore(selected, initialize=False).status()
    except sqlite3.DatabaseError:
        status = {"integrity_status": "invalid"}
    return {"exists": True, "path": str(selected), **status}


async def collect_once(
    store: BarFeedStore,
    source: HistoricalBarSource,
    *,
    now: datetime,
    minimum_interval_seconds: float = 11.0,
    max_symbols_per_cycle: int = 50,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    completed_through_date: date | None = None,
) -> BarFeedResult:
    if now.tzinfo is None:
        raise ValueError("bar-feed collection time cannot be naive")
    if minimum_interval_seconds < 0:
        raise ValueError("bar-feed request interval cannot be negative")
    if max_symbols_per_cycle < 1:
        raise ValueError("bar-feed symbol limit must be positive")
    requests = store.pending_requests(
        as_of=now.astimezone(NEW_YORK).date(),
        completed_through_date=completed_through_date,
    )
    symbols = store.fair_symbol_order([request.symbol for request in requests])[
        :max_symbols_per_cycle
    ]
    requests = [request for request in requests if request.symbol in symbols]
    if not symbols:
        result = BarFeedResult(0, 0, 0, 0, 0, 0)
        store.write_health(
            now=now,
            result="idle",
            error=None,
            requests_seen=0,
            symbols_seen=0,
            observations_added=0,
            rejected_bars=0,
            failed_symbols=0,
        )
        return result
    store.write_health(
        now=now,
        result="started",
        error=None,
        requests_seen=len(requests),
        symbols_seen=len(symbols),
        observations_added=0,
        rejected_bars=0,
        failed_symbols=0,
    )
    added = revisions = rejected = failed = 0
    try:
        await source.connect()
        for index, symbol in enumerate(symbols):
            if index:
                await sleep(minimum_interval_seconds)
            symbol_requests = [request for request in requests if request.symbol == symbol]
            start = min(request.start_date for request in symbol_requests)
            through = max(request.through_date for request in symbol_requests)
            try:
                batch = await asyncio.wait_for(
                    source.daily_bars(symbol, start_date=start),
                    timeout=_SOURCE_TIMEOUT_SECONDS,
                )
                for rejection in batch.rejections:
                    store.record_failure(
                        now=now,
                        symbol=symbol,
                        category="source_bar_rejected",
                        detail=rejection,
                    )
                if any(_normalized_symbol(bar.symbol) != symbol for bar in batch.bars):
                    raise ValueError("historical source returned a bar for the wrong symbol")
                bars = [bar for bar in batch.bars if start <= bar.trade_date <= through]
                symbol_added, symbol_revisions, symbol_rejected = store.append_completed(
                    bars,
                    observed_at_utc=now,
                    completed_through_date=completed_through_date,
                )
                store.record_successful_poll(
                    symbol,
                    local_date=now.astimezone(NEW_YORK).date(),
                    earliest_start_date=start,
                    requested_through_date=through,
                    completed_through_date=completed_through_date,
                    now=now,
                    returned_bar_count=len(batch.bars),
                    in_range_bar_count=len(bars),
                    source_rejection_count=len(batch.rejections),
                    validation_rejection_count=symbol_rejected,
                )
                added += symbol_added
                revisions += symbol_revisions
                rejected += len(batch.rejections) + symbol_rejected
            except (HistoricalBarSessionReset, TimeoutError) as exc:
                failed += 1
                store.record_failure(
                    now=now,
                    symbol=symbol,
                    category="symbol_collection_failed",
                    detail=f"{type(exc).__name__}: {exc}",
                )
                if index + 1 < len(symbols):
                    source.disconnect()
                    await source.connect()
                continue
            except Exception as exc:
                failed += 1
                store.record_failure(
                    now=now,
                    symbol=symbol,
                    category="symbol_collection_failed",
                    detail=f"{type(exc).__name__}: {exc}",
                )
                continue
    except Exception as exc:
        store.write_health(
            now=now,
            result="failed",
            error=f"{type(exc).__name__}: {exc}"[:2000],
            requests_seen=len(requests),
            symbols_seen=len(symbols),
            observations_added=added,
            rejected_bars=rejected,
            failed_symbols=failed,
        )
        raise
    finally:
        source.disconnect()
    store.write_health(
        now=now,
        result="partial" if failed or rejected else "completed",
        error=(f"failed_symbols={failed};rejected_bars={rejected}" if failed or rejected else None),
        requests_seen=len(requests),
        symbols_seen=len(symbols),
        observations_added=added,
        rejected_bars=rejected,
        failed_symbols=failed,
    )
    return BarFeedResult(len(requests), len(symbols), added, revisions, rejected, failed)


def result_json(result: BarFeedResult) -> dict[str, int]:
    return {str(key): int(value) for key, value in asdict(result).items()}
