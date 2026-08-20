from __future__ import annotations

import asyncio
import sqlite3
import time as time_module
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

from insider_alerts.backtest.models import DailyBar
from insider_alerts.backtest.signal_study import NEW_YORK, DeliveredSignal, MinuteBar


def ensure_minute_bars_table(db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS price_bars_minute (
                symbol TEXT NOT NULL,
                bar_timestamp TEXT NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                source TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol, bar_timestamp)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS price_bar_minute_sessions (
                symbol TEXT NOT NULL,
                session_date TEXT NOT NULL,
                bar_count INTEGER NOT NULL,
                source TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                PRIMARY KEY (symbol, session_date)
            )
            """
        )
        conn.commit()


def upsert_minute_bars(
    db_path: str,
    *,
    bars: Sequence[MinuteBar],
    source: str = "ibkr_trades_rth",
) -> None:
    ensure_minute_bars_table(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO price_bars_minute (
                symbol, bar_timestamp, open, high, low, close, volume, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    bar.symbol.upper(),
                    bar.timestamp.astimezone(UTC).isoformat(),
                    bar.open,
                    bar.high,
                    bar.low,
                    bar.close,
                    bar.volume,
                    source,
                )
                for bar in bars
            ],
        )
        conn.commit()


def get_minute_bars(db_path: str, *, symbol: str) -> list[MinuteBar]:
    ensure_minute_bars_table(db_path)
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT symbol, bar_timestamp, open, high, low, close, volume
            FROM price_bars_minute
            WHERE symbol = ?
            ORDER BY bar_timestamp
            """,
            (symbol.upper(),),
        ).fetchall()
    return [
        MinuteBar(
            symbol=str(row[0]),
            timestamp=datetime.fromisoformat(str(row[1])).astimezone(UTC),
            open=float(row[2]),
            high=float(row[3]),
            low=float(row[4]),
            close=float(row[5]),
            volume=float(row[6]),
        )
        for row in rows
    ]


def build_intraday_requests(
    signals: Sequence[DeliveredSignal],
    *,
    benchmark_daily_bars: Sequence[DailyBar],
    benchmark_symbol: str = "SPY",
    as_of: datetime | None = None,
) -> list[tuple[str, date]]:
    now_local = (as_of or datetime.now(UTC)).astimezone(NEW_YORK)
    latest_complete_date = (
        now_local.date()
        if now_local.time() >= time(16, 0)
        else now_local.date() - timedelta(days=1)
    )
    sessions = sorted(
        {bar.trade_date for bar in benchmark_daily_bars if bar.trade_date <= latest_complete_date}
    )
    session_set = set(sessions)
    requests: set[tuple[str, date]] = set()
    for signal in signals:
        local = signal.signal_at.astimezone(NEW_YORK)
        same_session = local.date() in session_set and local.time() < time(16, 0)
        if same_session:
            requests.add((signal.symbol, local.date()))
        if not same_session or local.time() >= time(15, 30):
            next_session = next((day for day in sessions if day > local.date()), None)
            if next_session is not None:
                requests.add((signal.symbol, next_session))
    for session_date in {day for _, day in requests}:
        requests.add((benchmark_symbol, session_date))
    return sorted(requests, key=lambda item: (item[1], item[0]))


def _cached_session_count(db_path: str, *, symbol: str, session_date: date) -> int:
    ensure_minute_bars_table(db_path)
    start = datetime.combine(session_date, time(0, 0), tzinfo=NEW_YORK).astimezone(UTC)
    end = datetime.combine(session_date, time(23, 59, 59), tzinfo=NEW_YORK).astimezone(UTC)
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) FROM price_bars_minute
            WHERE symbol = ? AND bar_timestamp >= ? AND bar_timestamp <= ?
            """,
            (symbol.upper(), start.isoformat(), end.isoformat()),
        ).fetchone()
    return int(row[0]) if row is not None else 0


def _cached_session_complete(db_path: str, *, symbol: str, session_date: date) -> bool:
    ensure_minute_bars_table(db_path)
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT bar_count
            FROM price_bar_minute_sessions
            WHERE symbol = ? AND session_date = ?
            """,
            (symbol.upper(), session_date.isoformat()),
        ).fetchone()
    if row is None or int(row[0]) <= 0:
        return False
    return _cached_session_count(db_path, symbol=symbol, session_date=session_date) >= int(row[0])


def completed_minute_bar_sessions(
    db_path: str,
    *,
    requests: Iterable[tuple[str, date]],
) -> set[tuple[str, date]]:
    return {
        (symbol, session_date)
        for symbol, session_date in set(requests)
        if _cached_session_complete(db_path, symbol=symbol, session_date=session_date)
    }


def filter_completed_minute_bars(
    bars_by_symbol: Mapping[str, Sequence[MinuteBar]],
    *,
    completed_sessions: set[tuple[str, date]],
) -> dict[str, list[MinuteBar]]:
    normalized_sessions = {
        (symbol.upper(), session_date) for symbol, session_date in completed_sessions
    }
    return {
        symbol: [
            bar
            for bar in bars
            if (bar.symbol.upper(), bar.timestamp.astimezone(NEW_YORK).date())
            in normalized_sessions
        ]
        for symbol, bars in bars_by_symbol.items()
    }


def _mark_session_complete(
    db_path: str,
    *,
    symbol: str,
    session_date: date,
    bar_count: int,
    source: str = "ibkr_trades_rth",
) -> None:
    if bar_count <= 0:
        raise ValueError("bar_count must be positive")
    ensure_minute_bars_table(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO price_bar_minute_sessions (
                symbol, session_date, bar_count, source, completed_at
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(symbol, session_date) DO UPDATE SET
                bar_count=excluded.bar_count,
                source=excluded.source,
                completed_at=excluded.completed_at
            """,
            (
                symbol.upper(),
                session_date.isoformat(),
                bar_count,
                source,
                datetime.now(UTC).isoformat(),
            ),
        )
        conn.commit()


def refresh_ibkr_minute_bars(
    db_path: str,
    *,
    requests: Iterable[tuple[str, date]],
    host: str = "127.0.0.1",
    port: int = 4001,
    client_id: int = 172,
    pacing_seconds: float = 0.4,
    sleep_fn: Callable[[float], None] = time_module.sleep,
) -> dict[str, object]:
    """Fetch one RTH one-minute session per request through the local IB Gateway."""

    requested = list(dict.fromkeys(requests))
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())
    from ib_async import IB, Stock

    ib = IB()
    ib.RaiseRequestErrors = True
    fetched = 0
    reused = 0
    errors: list[str] = []
    contracts: dict[str, Any] = {}
    try:
        ib.connect(host, port, clientId=client_id, timeout=10, readonly=True)
        request_count = 0
        for symbol, session_date in requested:
            if _cached_session_complete(db_path, symbol=symbol, session_date=session_date):
                reused += 1
                continue
            contract = contracts.get(symbol)
            if contract is None:
                candidate = Stock(symbol, "SMART", "USD")
                try:
                    qualified = ib.qualifyContracts(candidate)
                except Exception as exc:  # noqa: BLE001 - record per-symbol provider failure
                    errors.append(f"{symbol}|{session_date}: qualification failed: {exc}")
                    continue
                if not qualified:
                    errors.append(f"{symbol}|{session_date}: contract not found")
                    continue
                contract = qualified[0]
                contracts[symbol] = contract
            try:
                if request_count > 0 and pacing_seconds > 0:
                    sleep_fn(pacing_seconds)
                request_count += 1
                raw_bars = ib.reqHistoricalData(
                    contract,
                    datetime.combine(session_date, time(23, 59), tzinfo=UTC),
                    "1 D",
                    "1 min",
                    "TRADES",
                    useRTH=True,
                    formatDate=2,
                    keepUpToDate=False,
                )
            except Exception as exc:  # noqa: BLE001 - continue coverage audit
                errors.append(f"{symbol}|{session_date}: history failed: {exc}")
                continue
            bars = [
                MinuteBar(
                    symbol=symbol,
                    timestamp=bar.date.astimezone(UTC),
                    open=float(bar.open),
                    high=float(bar.high),
                    low=float(bar.low),
                    close=float(bar.close),
                    volume=float(bar.volume),
                )
                for bar in raw_bars
                if isinstance(bar.date, datetime) and bar.date.tzinfo is not None
            ]
            if not bars:
                errors.append(f"{symbol}|{session_date}: no minute bars")
                continue
            upsert_minute_bars(db_path, bars=bars)
            _mark_session_complete(
                db_path,
                symbol=symbol,
                session_date=session_date,
                bar_count=len(bars),
            )
            fetched += 1
    finally:
        if ib.isConnected():
            ib.disconnect()
    return {
        "requested": len(requested),
        "fetched": fetched,
        "reused": reused,
        "errors": errors,
    }
