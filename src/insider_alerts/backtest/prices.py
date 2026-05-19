from __future__ import annotations

import csv
import io
import sqlite3
from datetime import date
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

from insider_alerts.backtest.models import DailyBar


class PriceDataError(RuntimeError):
    """Raised when price history retrieval fails."""


def ensure_price_bars_table(db_path: str) -> None:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS price_bars_daily (
                symbol TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                source TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(symbol, trade_date)
            )
            """
        )
        conn.commit()


class StooqPriceClient:
    def __init__(self, *, user_agent: str, timeout_seconds: float) -> None:
        self.user_agent = user_agent
        self.timeout_seconds = timeout_seconds

    def _download_csv(self, symbol: str) -> str:
        normalized = symbol.strip().lower()
        if not normalized:
            raise PriceDataError("empty symbol")
        url = f"https://stooq.com/q/d/l/?s={normalized}.us&i=d"
        req = Request(
            url,
            headers={
                "User-Agent": self.user_agent,
                "Accept-Encoding": "gzip, deflate",
            },
        )
        try:
            with urlopen(req, timeout=self.timeout_seconds) as response:
                body = response.read()
                if not isinstance(body, bytes):
                    raise PriceDataError(f"price response was not bytes for {symbol}")
                return body.decode("utf-8", "replace")
        except (OSError, URLError) as exc:
            raise PriceDataError(f"price request failed for {symbol}: {exc}") from exc

    def fetch_history(self, symbol: str) -> list[DailyBar]:
        csv_text = self._download_csv(symbol)
        rows = list(csv.DictReader(io.StringIO(csv_text)))
        bars: list[DailyBar] = []
        for row in rows:
            try:
                trade_date = date.fromisoformat(str(row["Date"]))
                open_price = float(row["Open"])
                high_price = float(row["High"])
                low_price = float(row["Low"])
                close_price = float(row["Close"])
                volume = float(row["Volume"])
            except (KeyError, TypeError, ValueError):
                continue
            if min(open_price, high_price, low_price, close_price, volume) <= 0:
                continue
            bars.append(
                DailyBar(
                    symbol=symbol.upper(),
                    trade_date=trade_date,
                    open=open_price,
                    high=high_price,
                    low=low_price,
                    close=close_price,
                    volume=volume,
                )
            )
        if not bars:
            raise PriceDataError(f"no valid price bars for {symbol}")
        return bars


def refresh_price_bars(
    db_path: str,
    *,
    symbol: str,
    bars: list[DailyBar],
    source: str = "stooq",
) -> None:
    ensure_price_bars_table(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO price_bars_daily (
                symbol, trade_date, open, high, low, close, volume, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    bar.symbol.upper(),
                    bar.trade_date.isoformat(),
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


def get_price_bars(
    db_path: str,
    *,
    symbol: str,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[DailyBar]:
    ensure_price_bars_table(db_path)
    conditions = ["symbol = ?"]
    params: list[str] = [symbol.upper()]
    if start_date is not None:
        conditions.append("trade_date >= ?")
        params.append(start_date.isoformat())
    if end_date is not None:
        conditions.append("trade_date <= ?")
        params.append(end_date.isoformat())
    where_clause = " AND ".join(conditions)
    query = f"""
        SELECT symbol, trade_date, open, high, low, close, volume
        FROM price_bars_daily
        WHERE {where_clause}
        ORDER BY trade_date ASC
    """
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(query, params).fetchall()
    return [
        DailyBar(
            symbol=str(row["symbol"]),
            trade_date=date.fromisoformat(str(row["trade_date"])),
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            volume=float(row["volume"]),
        )
        for row in rows
    ]
