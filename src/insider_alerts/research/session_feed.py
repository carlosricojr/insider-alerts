"""Append-only exchange-session schedule feed for prospective trial timing."""

from __future__ import annotations

import contextlib
import hashlib
import json
import sqlite3
import uuid
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol
from zoneinfo import ZoneInfo

import rfc8785

SESSION_FEED_VERSION = "ibkr-spy-rth-schedule-v1"
NEW_YORK = ZoneInfo("America/New_York")
SESSION_COMPLETION_MINIMUM_AGE = timedelta(minutes=1)


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("session-feed timestamp cannot be naive")
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("persisted session-feed timestamp is naive")
    return parsed.astimezone(UTC)


def _canonical(value: dict[str, Any]) -> bytes:
    return rfc8785.dumps(value)


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


@dataclass(frozen=True, slots=True)
class ExchangeSession:
    session_date: date
    opens_at_utc: datetime
    closes_at_utc: datetime

    def __post_init__(self) -> None:
        if self.opens_at_utc.tzinfo is None or self.closes_at_utc.tzinfo is None:
            raise ValueError("exchange-session boundaries cannot be naive")
        if self.closes_at_utc <= self.opens_at_utc:
            raise ValueError("exchange session must close after it opens")
        if (
            self.opens_at_utc.astimezone(NEW_YORK).date() != self.session_date
            or self.closes_at_utc.astimezone(NEW_YORK).date() != self.session_date
        ):
            raise ValueError("exchange-session boundary differs from its New York session date")


@dataclass(frozen=True, slots=True)
class SourceSessionBatch:
    sessions: tuple[ExchangeSession, ...]
    rejections: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class SessionFeedResult:
    sessions_seen: int
    observations_added: int
    revisions_added: int
    rejected_sessions: int


class ExchangeSessionSource(Protocol):
    async def connect(self) -> None: ...

    async def exchange_sessions(
        self, *, end: datetime, calendar_days: int
    ) -> SourceSessionBatch: ...

    def disconnect(self) -> None: ...


class SessionFeedStore:
    def __init__(self, path: Path | str, *, initialize: bool = True) -> None:
        self.path = Path(path)
        if initialize:
            self._initialize()

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _initialize(self) -> None:
        with contextlib.closing(self._connect()) as conn, conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS session_observations (
                    sequence INTEGER NOT NULL UNIQUE,
                    observation_id TEXT PRIMARY KEY,
                    session_date TEXT NOT NULL,
                    opens_at_utc TEXT NOT NULL,
                    closes_at_utc TEXT NOT NULL,
                    observed_at_utc TEXT NOT NULL,
                    value_sha256 TEXT NOT NULL,
                    record_sha256 TEXT NOT NULL UNIQUE,
                    record_json BLOB NOT NULL
                );
                CREATE INDEX IF NOT EXISTS session_observations_date
                ON session_observations(session_date, sequence);
                CREATE TRIGGER IF NOT EXISTS session_observations_sequence
                BEFORE INSERT ON session_observations
                WHEN NEW.sequence<>(SELECT COALESCE(MAX(sequence),0)+1 FROM session_observations)
                BEGIN SELECT RAISE(ABORT, 'session observation sequence must be gap-free'); END;
                CREATE TRIGGER IF NOT EXISTS session_observations_time_monotonic
                BEFORE INSERT ON session_observations
                WHEN EXISTS(
                  SELECT 1 FROM session_observations WHERE observed_at_utc>NEW.observed_at_utc
                )
                BEGIN SELECT RAISE(ABORT, 'session observation time cannot move backwards'); END;
                CREATE TRIGGER IF NOT EXISTS session_observations_no_update
                BEFORE UPDATE ON session_observations
                BEGIN SELECT RAISE(ABORT, 'session observations are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS session_observations_no_delete
                BEFORE DELETE ON session_observations
                BEGIN SELECT RAISE(ABORT, 'session observations are immutable'); END;

                CREATE TABLE IF NOT EXISTS session_feed_failures (
                    sequence INTEGER NOT NULL UNIQUE,
                    failure_id TEXT PRIMARY KEY,
                    occurred_at_utc TEXT NOT NULL,
                    category TEXT NOT NULL,
                    record_sha256 TEXT NOT NULL UNIQUE,
                    record_json BLOB NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS session_feed_failures_sequence
                BEFORE INSERT ON session_feed_failures
                WHEN NEW.sequence<>(SELECT COALESCE(MAX(sequence),0)+1 FROM session_feed_failures)
                BEGIN SELECT RAISE(ABORT, 'session failure sequence must be gap-free'); END;
                CREATE TRIGGER IF NOT EXISTS session_feed_failures_no_update
                BEFORE UPDATE ON session_feed_failures
                BEGIN SELECT RAISE(ABORT, 'session failures are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS session_feed_failures_no_delete
                BEFORE DELETE ON session_feed_failures
                BEGIN SELECT RAISE(ABORT, 'session failures are immutable'); END;

                CREATE TABLE IF NOT EXISTS session_feed_health (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    last_worker_heartbeat_utc TEXT NOT NULL,
                    last_result TEXT NOT NULL,
                    last_error TEXT,
                    sessions_seen INTEGER NOT NULL,
                    observations_added INTEGER NOT NULL,
                    rejected_sessions INTEGER NOT NULL
                );
                """
            )

    def append(
        self,
        sessions: Sequence[ExchangeSession],
        *,
        observed_at_utc: datetime,
    ) -> tuple[int, int]:
        observed_text = _utc_text(observed_at_utc)
        added = revisions = 0
        with contextlib.closing(self._connect()) as conn, conn:
            for session in sorted(sessions, key=lambda item: item.session_date):
                value: dict[str, Any] = {
                    "source": "IBKR",
                    "contract": "SPY:SMART:USD",
                    "regular_trading_hours_only": True,
                    "session_date": session.session_date.isoformat(),
                    "opens_at_utc": _utc_text(session.opens_at_utc),
                    "closes_at_utc": _utc_text(session.closes_at_utc),
                }
                value_sha = _sha256(_canonical(value))
                latest = conn.execute(
                    """
                    SELECT value_sha256 FROM session_observations
                    WHERE session_date=? ORDER BY sequence DESC LIMIT 1
                    """,
                    (session.session_date.isoformat(),),
                ).fetchone()
                if latest is not None and str(latest["value_sha256"]) == value_sha:
                    continue
                prior = conn.execute(
                    "SELECT 1 FROM session_observations WHERE session_date=? LIMIT 1",
                    (session.session_date.isoformat(),),
                ).fetchone()
                record: dict[str, Any] = {
                    "contract_version": SESSION_FEED_VERSION,
                    "observed_at_utc": observed_text,
                    "value_sha256": value_sha,
                    "value": value,
                }
                encoded = _canonical(record)
                record_sha = _sha256(encoded)
                sequence = int(
                    conn.execute(
                        "SELECT COALESCE(MAX(sequence),0)+1 FROM session_observations"
                    ).fetchone()[0]
                )
                conn.execute(
                    """
                    INSERT INTO session_observations(
                        sequence,observation_id,session_date,opens_at_utc,closes_at_utc,
                        observed_at_utc,value_sha256,record_sha256,record_json
                    ) VALUES(?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        sequence,
                        str(uuid.uuid5(uuid.NAMESPACE_URL, record_sha)),
                        session.session_date.isoformat(),
                        _utc_text(session.opens_at_utc),
                        _utc_text(session.closes_at_utc),
                        observed_text,
                        value_sha,
                        record_sha,
                        encoded,
                    ),
                )
                added += 1
                revisions += int(prior is not None)
        return added, revisions

    def record_failure(self, *, now: datetime, category: str, detail: str) -> None:
        record: dict[str, Any] = {
            "contract_version": SESSION_FEED_VERSION,
            "occurred_at_utc": _utc_text(now),
            "category": category,
            "detail": detail[:2000],
        }
        encoded = _canonical(record)
        digest = _sha256(encoded)
        with contextlib.closing(self._connect()) as conn, conn:
            sequence = int(
                conn.execute(
                    "SELECT COALESCE(MAX(sequence),0)+1 FROM session_feed_failures"
                ).fetchone()[0]
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO session_feed_failures(
                    sequence,failure_id,occurred_at_utc,category,record_sha256,record_json
                ) VALUES(?,?,?,?,?,?)
                """,
                (
                    sequence,
                    str(uuid.uuid5(uuid.NAMESPACE_URL, digest)),
                    _utc_text(now),
                    category,
                    digest,
                    encoded,
                ),
            )

    def _rows(self, *, through_observed_at: datetime | None = None) -> list[sqlite3.Row]:
        where = ""
        parameters: tuple[object, ...] = ()
        if through_observed_at is not None:
            where = "WHERE observed_at_utc<=?"
            parameters = (_utc_text(through_observed_at),)
        with contextlib.closing(self._connect()) as conn:
            return conn.execute(
                f"SELECT * FROM session_observations {where} ORDER BY sequence", parameters
            ).fetchall()

    @staticmethod
    def _verify_row(row: sqlite3.Row) -> ExchangeSession:
        raw = bytes(row["record_json"])
        record_sha = str(row["record_sha256"])
        if _sha256(raw) != record_sha:
            raise ValueError("session observation stored bytes failed integrity check")
        record = json.loads(raw)
        if not isinstance(record, dict) or _canonical(record) != raw:
            raise ValueError("session observation is not canonical JSON")
        value = record.get("value")
        if not isinstance(value, dict):
            raise ValueError("session observation value is missing")
        value_sha = _sha256(_canonical(value))
        if value_sha != record.get("value_sha256") or value_sha != str(row["value_sha256"]):
            raise ValueError("session observation value digest mismatch")
        if (
            record.get("contract_version") != SESSION_FEED_VERSION
            or str(uuid.uuid5(uuid.NAMESPACE_URL, record_sha)) != str(row["observation_id"])
            or value.get("source") != "IBKR"
            or value.get("contract") != "SPY:SMART:USD"
            or value.get("regular_trading_hours_only") is not True
            or str(value.get("session_date")) != str(row["session_date"])
            or str(value.get("opens_at_utc")) != str(row["opens_at_utc"])
            or str(value.get("closes_at_utc")) != str(row["closes_at_utc"])
            or str(record.get("observed_at_utc")) != str(row["observed_at_utc"])
        ):
            raise ValueError("session observation provenance or columns mismatch")
        return ExchangeSession(
            date.fromisoformat(str(value["session_date"])),
            _parse_utc(str(value["opens_at_utc"])),
            _parse_utc(str(value["closes_at_utc"])),
        )

    @staticmethod
    def _verify_failure_row(row: sqlite3.Row) -> None:
        raw = bytes(row["record_json"])
        digest = str(row["record_sha256"])
        if _sha256(raw) != digest:
            raise ValueError("session failure stored bytes failed integrity check")
        record = json.loads(raw)
        if not isinstance(record, dict) or _canonical(record) != raw:
            raise ValueError("session failure is not canonical JSON")
        if (
            record.get("contract_version") != SESSION_FEED_VERSION
            or str(record.get("occurred_at_utc")) != str(row["occurred_at_utc"])
            or str(record.get("category")) != str(row["category"])
            or str(uuid.uuid5(uuid.NAMESPACE_URL, digest)) != str(row["failure_id"])
        ):
            raise ValueError("session failure columns do not match immutable record")

    def schedule_as_known_at(self, as_of_utc: datetime) -> list[ExchangeSession]:
        rows = self._rows(through_observed_at=as_of_utc)
        latest: dict[date, tuple[int, ExchangeSession]] = {}
        for row in rows:
            session = self._verify_row(row)
            latest[session.session_date] = (int(row["sequence"]), session)
        return [latest[day][1] for day in sorted(latest)]

    def latest_schedule(self) -> list[ExchangeSession]:
        rows = self._rows()
        latest: dict[date, tuple[int, ExchangeSession]] = {}
        for row in rows:
            session = self._verify_row(row)
            latest[session.session_date] = (int(row["sequence"]), session)
        return [latest[day][1] for day in sorted(latest)]

    def completed_through_date(
        self,
        as_of_utc: datetime,
        *,
        minimum_age: timedelta = SESSION_COMPLETION_MINIMUM_AGE,
    ) -> date | None:
        if as_of_utc.tzinfo is None:
            raise ValueError("session completion boundary cannot be naive")
        if minimum_age < timedelta(0):
            raise ValueError("session completion minimum age cannot be negative")
        completed = [
            session.session_date
            for session in self.schedule_as_known_at(as_of_utc)
            if session.closes_at_utc + minimum_age <= as_of_utc
        ]
        return max(completed, default=None)

    def validate_integrity(self) -> None:
        rows = self._rows()
        with contextlib.closing(self._connect()) as conn:
            failure_rows = conn.execute(
                "SELECT * FROM session_feed_failures ORDER BY sequence"
            ).fetchall()
        if [int(row["sequence"]) for row in rows] != list(range(1, len(rows) + 1)):
            raise ValueError("session observation sequence is not gap-free")
        observed = [str(row["observed_at_utc"]) for row in rows]
        if observed != sorted(observed):
            raise ValueError("session observation timestamps move backwards")
        for row in rows:
            self._verify_row(row)
        if [int(row["sequence"]) for row in failure_rows] != list(range(1, len(failure_rows) + 1)):
            raise ValueError("session failure sequence is not gap-free")
        for row in failure_rows:
            self._verify_failure_row(row)

    def write_health(
        self,
        *,
        now: datetime,
        result: str,
        error: str | None,
        sessions_seen: int,
        observations_added: int,
        rejected_sessions: int,
    ) -> None:
        with contextlib.closing(self._connect()) as conn, conn:
            conn.execute(
                """
                INSERT INTO session_feed_health(
                    singleton,last_worker_heartbeat_utc,last_result,last_error,
                    sessions_seen,observations_added,rejected_sessions
                ) VALUES(1,?,?,?,?,?,?)
                ON CONFLICT(singleton) DO UPDATE SET
                  last_worker_heartbeat_utc=excluded.last_worker_heartbeat_utc,
                  last_result=excluded.last_result,
                  last_error=excluded.last_error,
                  sessions_seen=excluded.sessions_seen,
                  observations_added=excluded.observations_added,
                  rejected_sessions=excluded.rejected_sessions
                """,
                (
                    _utc_text(now),
                    result,
                    error,
                    sessions_seen,
                    observations_added,
                    rejected_sessions,
                ),
            )

    def status(self) -> dict[str, object]:
        with contextlib.closing(self._connect()) as conn:
            observations = int(
                conn.execute("SELECT COUNT(*) FROM session_observations").fetchone()[0]
            )
            revisions = int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM session_observations current
                    WHERE EXISTS(
                      SELECT 1 FROM session_observations prior
                      WHERE prior.session_date=current.session_date
                        AND prior.sequence<current.sequence
                    )
                    """
                ).fetchone()[0]
            )
            failures = int(conn.execute("SELECT COUNT(*) FROM session_feed_failures").fetchone()[0])
            health = conn.execute("SELECT * FROM session_feed_health WHERE singleton=1").fetchone()
        try:
            self.validate_integrity()
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            integrity = "invalid"
        else:
            integrity = "valid"
        return {
            "observation_count": observations,
            "revision_count": revisions,
            "failure_count": failures,
            "integrity_status": integrity,
            "health": dict(health) if health is not None else None,
        }


def session_feed_status(path: Path | str) -> dict[str, object]:
    selected = Path(path)
    if not selected.is_file():
        return {"exists": False, "path": str(selected), "integrity_status": "missing"}
    try:
        status = SessionFeedStore(selected, initialize=False).status()
    except sqlite3.DatabaseError:
        status = {"integrity_status": "invalid"}
    return {"exists": True, "path": str(selected), **status}


async def collect_sessions_once(
    store: SessionFeedStore,
    source: ExchangeSessionSource,
    *,
    now: datetime,
    calendar_days: int = 180,
) -> SessionFeedResult:
    if now.tzinfo is None:
        raise ValueError("session-feed collection time cannot be naive")
    if calendar_days < 90 or calendar_days > 365:
        raise ValueError("session-feed calendar_days must be in [90, 365]")
    store.write_health(
        now=now,
        result="started",
        error=None,
        sessions_seen=0,
        observations_added=0,
        rejected_sessions=0,
    )
    try:
        await source.connect()
        batch = await source.exchange_sessions(end=now, calendar_days=calendar_days)
        for rejection in batch.rejections:
            store.record_failure(
                now=now,
                category="source_session_rejected",
                detail=rejection,
            )
        added, revisions = store.append(batch.sessions, observed_at_utc=now)
    except Exception as exc:
        store.write_health(
            now=now,
            result="failed",
            error=f"{type(exc).__name__}: {exc}"[:2000],
            sessions_seen=0,
            observations_added=0,
            rejected_sessions=0,
        )
        raise
    finally:
        source.disconnect()
    rejected = len(batch.rejections)
    store.write_health(
        now=now,
        result="partial" if rejected else "completed",
        error=f"rejected_sessions={rejected}" if rejected else None,
        sessions_seen=len(batch.sessions),
        observations_added=added,
        rejected_sessions=rejected,
    )
    return SessionFeedResult(len(batch.sessions), added, revisions, rejected)


def result_json(result: SessionFeedResult) -> dict[str, int]:
    return {str(key): int(value) for key, value in asdict(result).items()}
