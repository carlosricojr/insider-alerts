from __future__ import annotations

import csv
import io
import sqlite3
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen


class MarketContextError(RuntimeError):
    """Raised when market context lookup fails."""


@dataclass(slots=True)
class MarketSnapshot:
    symbol: str
    trade_date: date
    close: float
    volume: float
    dollar_turnover: float
    prior_close: float | None
    return_1d: float | None
    earnings_shock_flag: bool
    source: str = "stooq"


def ensure_market_snapshots_table(db_path: str) -> None:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS market_snapshots (
                symbol TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                dollar_turnover REAL NOT NULL,
                prior_close REAL,
                return_1d REAL,
                earnings_shock_flag INTEGER NOT NULL,
                source TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol, trade_date)
            )
            """
        )
        conn.commit()


def get_market_snapshot(db_path: str, *, symbol: str, trade_date: date) -> MarketSnapshot | None:
    ensure_market_snapshots_table(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT symbol, trade_date, close, volume, dollar_turnover, prior_close,
                   return_1d, earnings_shock_flag, source
            FROM market_snapshots
            WHERE symbol = ? AND trade_date = ?
            LIMIT 1
            """,
            (symbol.upper(), trade_date.isoformat()),
        ).fetchone()

    if row is None:
        return None

    prior_close_obj = row["prior_close"]
    return_1d_obj = row["return_1d"]
    return MarketSnapshot(
        symbol=str(row["symbol"]),
        trade_date=date.fromisoformat(str(row["trade_date"])),
        close=float(row["close"]),
        volume=float(row["volume"]),
        dollar_turnover=float(row["dollar_turnover"]),
        prior_close=float(prior_close_obj) if prior_close_obj is not None else None,
        return_1d=float(return_1d_obj) if return_1d_obj is not None else None,
        earnings_shock_flag=bool(int(row["earnings_shock_flag"])),
        source=str(row["source"]),
    )


def upsert_market_snapshot(db_path: str, snapshot: MarketSnapshot) -> None:
    ensure_market_snapshots_table(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO market_snapshots (
                symbol, trade_date, close, volume, dollar_turnover, prior_close,
                return_1d, earnings_shock_flag, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot.symbol.upper(),
                snapshot.trade_date.isoformat(),
                snapshot.close,
                snapshot.volume,
                snapshot.dollar_turnover,
                snapshot.prior_close,
                snapshot.return_1d,
                1 if snapshot.earnings_shock_flag else 0,
                snapshot.source,
            ),
        )
        conn.commit()


class DailyMarketDataClient:
    def __init__(
        self,
        *,
        user_agent: str,
        timeout_seconds: float,
        shock_drop_threshold: float = 0.08,
    ) -> None:
        self.user_agent = user_agent
        self.timeout_seconds = timeout_seconds
        self.shock_drop_threshold = shock_drop_threshold

    def _download_csv_text(self, symbol: str) -> str:
        normalized = symbol.strip().lower()
        if not normalized:
            raise MarketContextError("empty symbol")
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
                    raise MarketContextError(f"market data response was not bytes for {symbol}")
                return body.decode("utf-8", "replace")
        except (OSError, URLError) as exc:
            raise MarketContextError(f"market data request failed for {symbol}: {exc}") from exc

    def fetch_snapshot(self, symbol: str, *, trade_date: date) -> MarketSnapshot | None:
        text = self._download_csv_text(symbol)
        rows = list(csv.DictReader(io.StringIO(text)))
        if not rows:
            return None

        indexed: dict[date, dict[str, str]] = {}
        for raw_row in rows:
            row: dict[str, str] = {}
            for raw_key, raw_value in raw_row.items():
                if isinstance(raw_key, str) and isinstance(raw_value, str):
                    row[raw_key] = raw_value
            date_text = row.get("Date")
            if not date_text:
                continue
            try:
                key = date.fromisoformat(date_text)
            except ValueError:
                continue
            indexed[key] = row

        trade_row = indexed.get(trade_date)
        if trade_row is None:
            return None

        try:
            close = float(trade_row.get("Close", "0"))
            volume = float(trade_row.get("Volume", "0"))
        except ValueError as exc:
            raise MarketContextError(
                f"market data parse failed for {symbol} on {trade_date.isoformat()}: {exc}"
            ) from exc

        if close <= 0 or volume <= 0:
            return None

        prior_close: float | None = None
        return_1d: float | None = None
        prior_dates = sorted(d for d in indexed if d < trade_date)
        if prior_dates:
            prior_row = indexed[prior_dates[-1]]
            try:
                prior_close_value = float(prior_row.get("Close", "0"))
            except ValueError:
                prior_close_value = 0.0
            if prior_close_value > 0:
                prior_close = prior_close_value
                return_1d = (close / prior_close) - 1.0

        earnings_shock_flag = (
            return_1d is not None and return_1d <= -abs(self.shock_drop_threshold)
        )
        return MarketSnapshot(
            symbol=symbol.upper(),
            trade_date=trade_date,
            close=close,
            volume=volume,
            dollar_turnover=close * volume,
            prior_close=prior_close,
            return_1d=return_1d,
            earnings_shock_flag=earnings_shock_flag,
        )
