from __future__ import annotations

import csv
import gzip
import io
import json
import math
import re
import sqlite3
import time
import zlib
from collections.abc import Callable
from datetime import UTC, date, datetime
from email.utils import parsedate_to_datetime
from http.client import HTTPException
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from insider_alerts.backtest.models import DailyBar


class PriceDataError(RuntimeError):
    """Raised when price history retrieval fails."""


_TRADABLE_SYMBOL_RE = re.compile(r"^[A-Z][A-Z0-9]{0,5}(?:-[A-Z0-9]{1,2})?$")


def normalize_backtest_symbol(symbol: str) -> str | None:
    text = symbol.strip().upper()
    if not text:
        return None
    if text.startswith("(") and text.endswith(")") and len(text) > 2:
        text = text[1:-1].strip()
    if ":" in text:
        text = text.split(":")[-1].strip()
    text = text.replace(".", "-")
    if any(marker in text for marker in (" ", "/", ";", ",", "&")):
        return None
    if _TRADABLE_SYMBOL_RE.fullmatch(text) is None:
        return None
    return text


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
    def __init__(
        self,
        *,
        user_agent: str,
        timeout_seconds: float,
        rate_limit_per_second: float = 1.0,
        retry_attempts: int = 3,
        retry_min_seconds: float = 0.5,
        retry_max_seconds: float = 3.0,
        prefer_yahoo: bool = False,
        now_fn: Callable[[], float] = time.monotonic,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        self.user_agent = user_agent
        self.timeout_seconds = timeout_seconds
        self.rate_limit_per_second = rate_limit_per_second
        self.retry_attempts = max(1, retry_attempts)
        self.retry_min_seconds = max(0.0, retry_min_seconds)
        self.retry_max_seconds = max(self.retry_min_seconds, retry_max_seconds)
        self.prefer_yahoo = prefer_yahoo
        self.now_fn = now_fn
        self.sleep_fn = sleep_fn
        self._last_request_ts = 0.0

    def _enforce_rate_limit(self) -> None:
        if self.rate_limit_per_second <= 0:
            return
        interval = 1.0 / self.rate_limit_per_second
        now = self.now_fn()
        elapsed = now - self._last_request_ts
        if elapsed < interval:
            self.sleep_fn(interval - elapsed)
        self._last_request_ts = self.now_fn()

    def _retry_delay(self, attempt: int) -> float:
        if self.retry_min_seconds == 0:
            return 0.0
        return float(
            min(
                self.retry_min_seconds * (2 ** max(attempt - 1, 0)),
                self.retry_max_seconds,
            )
        )

    @staticmethod
    def _is_retryable_http_status(status_code: int) -> bool:
        return status_code in {403, 429} or status_code >= 500

    @staticmethod
    def _retry_after_seconds(error: HTTPError, *, now: datetime | None = None) -> float:
        headers = getattr(error, "headers", None)
        if headers is None:
            return 0.0
        value = headers.get("Retry-After")
        if value is None:
            return 0.0
        try:
            return max(0.0, float(value))
        except (TypeError, ValueError):
            try:
                retry_at = parsedate_to_datetime(str(value))
            except (TypeError, ValueError, OverflowError):
                return 0.0
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=UTC)
            reference = now or datetime.now(UTC)
            return max(0.0, (retry_at - reference).total_seconds())

    @staticmethod
    def _decode_response_bytes(raw: bytes, content_encoding: str) -> bytes:
        encodings = [part.strip().lower() for part in content_encoding.split(",") if part.strip()]
        decoded = raw
        for encoding in reversed(encodings):
            if encoding == "gzip":
                decoded = gzip.decompress(decoded)
            elif encoding == "deflate":
                try:
                    decoded = zlib.decompress(decoded)
                except zlib.error:
                    # Some servers emit raw DEFLATE streams without zlib wrapper.
                    decoded = zlib.decompress(decoded, -zlib.MAX_WBITS)
        return decoded

    def _download_text(self, *, url: str, symbol: str) -> str:
        req = Request(
            url,
            headers={
                "User-Agent": self.user_agent,
                "Accept-Encoding": "gzip, deflate",
            },
        )
        last_error: Exception | None = None
        for attempt in range(1, self.retry_attempts + 1):
            self._enforce_rate_limit()
            try:
                with urlopen(req, timeout=self.timeout_seconds) as response:
                    raw = response.read()
                    content_encoding = ""
                    headers = getattr(response, "headers", None)
                    if headers is not None:
                        get_header = getattr(headers, "get", None)
                        if callable(get_header):
                            value = get_header("Content-Encoding")
                            if isinstance(value, str):
                                content_encoding = value
                    if content_encoding:
                        try:
                            raw = self._decode_response_bytes(raw, content_encoding)
                        except (OSError, EOFError, zlib.error, ValueError) as exc:
                            raise PriceDataError(
                                f"price response decode failed for {symbol}: {exc}"
                            ) from exc
                    return str(raw.decode("utf-8", "replace"))
            except HTTPError as exc:
                last_error = exc
                if (
                    attempt < self.retry_attempts
                    and self._is_retryable_http_status(int(exc.code))
                ):
                    delay = self._retry_delay(attempt)
                    if int(exc.code) == 429:
                        delay = max(delay, self._retry_after_seconds(exc))
                    if delay > 0:
                        self.sleep_fn(delay)
                    continue
                raise PriceDataError(
                    f"price request failed for {symbol}: HTTP {exc.code} {exc.reason}"
                ) from exc
            except (OSError, URLError, HTTPException, ValueError) as exc:
                last_error = exc
                if attempt < self.retry_attempts:
                    delay = self._retry_delay(attempt)
                    if delay > 0:
                        self.sleep_fn(delay)
                    continue
                raise PriceDataError(f"price request failed for {symbol}: {exc}") from exc
        if last_error is not None:
            raise PriceDataError(f"price request failed for {symbol}: {last_error}") from last_error
        raise PriceDataError(f"price request failed for {symbol}: unknown network failure")

    def _download_yahoo_chart(self, symbol: str) -> str:
        params = urlencode(
            {
                "interval": "1d",
                # Yahoo silently changes range=max to monthly granularity for
                # long-lived symbols even when interval=1d is requested.
                "period1": 0,
                "period2": int(time.time()) + 86_400,
                "events": "div,splits",
                "includeAdjustedClose": "true",
            }
        )
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{quote(symbol)}?{params}"
        return self._download_text(url=url, symbol=symbol)

    def _download_csv(self, symbol: str) -> str:
        normalized = symbol.strip().lower()
        if not normalized:
            raise PriceDataError("empty symbol")
        query = urlencode({"s": f"{normalized}.us", "i": "d"})
        url = f"https://stooq.com/q/d/l/?{query}"
        return self._download_text(url=url, symbol=symbol)

    def _parse_stooq_csv(self, symbol: str, csv_text: str) -> list[DailyBar]:
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
        return bars

    def _parse_yahoo_chart(self, symbol: str, payload_text: str) -> list[DailyBar]:
        try:
            payload = json.loads(payload_text)
        except json.JSONDecodeError as exc:
            raise PriceDataError(f"invalid yahoo chart payload for {symbol}") from exc

        chart_obj = payload.get("chart")
        if not isinstance(chart_obj, dict):
            raise PriceDataError(f"missing yahoo chart payload for {symbol}")
        result_obj = chart_obj.get("result")
        if not isinstance(result_obj, list) or not result_obj:
            raise PriceDataError(f"empty yahoo chart result for {symbol}")
        result = result_obj[0]
        if not isinstance(result, dict):
            raise PriceDataError(f"invalid yahoo chart result for {symbol}")
        meta_obj = result.get("meta")
        if isinstance(meta_obj, dict):
            granularity = meta_obj.get("dataGranularity")
            if isinstance(granularity, str) and granularity != "1d":
                raise PriceDataError(
                    f"yahoo did not return daily granularity for {symbol}: {granularity}"
                )

        timestamps_obj = result.get("timestamp")
        indicators_obj = result.get("indicators")
        if (
            not isinstance(timestamps_obj, list)
            or not isinstance(indicators_obj, dict)
            or not timestamps_obj
        ):
            raise PriceDataError(f"incomplete yahoo chart data for {symbol}")
        quote_obj = indicators_obj.get("quote")
        if not isinstance(quote_obj, list) or not quote_obj:
            raise PriceDataError(f"missing yahoo quote data for {symbol}")
        quote_row = quote_obj[0]
        if not isinstance(quote_row, dict):
            raise PriceDataError(f"invalid yahoo quote data for {symbol}")

        opens = quote_row.get("open")
        highs = quote_row.get("high")
        lows = quote_row.get("low")
        closes = quote_row.get("close")
        volumes = quote_row.get("volume")
        if (
            not isinstance(opens, list)
            or not isinstance(highs, list)
            or not isinstance(lows, list)
            or not isinstance(closes, list)
            or not isinstance(volumes, list)
        ):
            raise PriceDataError(f"incomplete yahoo quote arrays for {symbol}")
        adjusted_closes: list[object] | None = None
        adjusted_obj = indicators_obj.get("adjclose")
        if isinstance(adjusted_obj, list) and adjusted_obj:
            adjusted_row = adjusted_obj[0]
            if isinstance(adjusted_row, dict):
                candidate = adjusted_row.get("adjclose")
                if isinstance(candidate, list):
                    adjusted_closes = candidate

        bars: list[DailyBar] = []
        max_len = min(
            len(timestamps_obj),
            len(opens),
            len(highs),
            len(lows),
            len(closes),
            len(volumes),
        )
        for idx in range(max_len):
            ts = timestamps_obj[idx]
            open_obj = opens[idx]
            high_obj = highs[idx]
            low_obj = lows[idx]
            close_obj = closes[idx]
            volume_obj = volumes[idx]
            try:
                ts_int = int(ts)
                trade_date = datetime.fromtimestamp(ts_int, tz=UTC).date()
                open_price = float(open_obj)
                high_price = float(high_obj)
                low_price = float(low_obj)
                close_price = float(close_obj)
                volume = float(volume_obj)
            except (TypeError, ValueError, OverflowError):
                continue
            if min(open_price, high_price, low_price, close_price, volume) <= 0:
                continue
            if adjusted_closes is not None and idx < len(adjusted_closes):
                adjusted_close_obj = adjusted_closes[idx]
                if not isinstance(adjusted_close_obj, (int, float, str)):
                    adjusted_close_obj = close_price
                try:
                    adjusted_close = float(adjusted_close_obj)
                    adjustment_factor = adjusted_close / close_price
                except (TypeError, ValueError, ZeroDivisionError):
                    adjustment_factor = 1.0
                if math.isfinite(adjustment_factor) and adjustment_factor > 0:
                    open_price *= adjustment_factor
                    high_price *= adjustment_factor
                    low_price *= adjustment_factor
                    close_price = adjusted_close
                    # Preserve raw dollar turnover while expressing OHLC on
                    # the adjusted price basis.
                    volume /= adjustment_factor
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
        return bars

    def _fetch_history_yahoo(self, symbol: str) -> list[DailyBar]:
        payload = self._download_yahoo_chart(symbol)
        bars = self._parse_yahoo_chart(symbol, payload)
        if not bars:
            raise PriceDataError(f"no valid yahoo price bars for {symbol}")
        return bars

    def _fetch_history_stooq(self, symbol: str) -> list[DailyBar]:
        csv_text = self._download_csv(symbol)
        bars = self._parse_stooq_csv(symbol, csv_text)
        if not bars:
            raise PriceDataError(f"no valid stooq price bars for {symbol}")
        return bars

    def fetch_history(self, symbol: str) -> list[DailyBar]:
        normalized = normalize_backtest_symbol(symbol)
        if normalized is None:
            raise PriceDataError(f"unsupported ticker format for {symbol}")

        if self.prefer_yahoo:
            yahoo_error: PriceDataError | None = None
            try:
                return self._fetch_history_yahoo(normalized)
            except PriceDataError as exc:
                yahoo_error = exc
            try:
                return self._fetch_history_stooq(normalized)
            except PriceDataError as exc:
                raise PriceDataError(
                    f"{yahoo_error}; fallback stooq failed for {normalized}: {exc}"
                ) from exc

        return self._fetch_history_stooq(normalized)


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


def get_price_bar_bounds(
    db_path: str,
    *,
    symbol: str,
) -> tuple[date | None, date | None]:
    ensure_price_bars_table(db_path)
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT MIN(trade_date), MAX(trade_date)
            FROM price_bars_daily
            WHERE symbol = ?
            """,
            (symbol.upper(),),
        ).fetchone()
    if row is None:
        return None, None
    min_date_obj, max_date_obj = row
    min_date = date.fromisoformat(str(min_date_obj)) if min_date_obj else None
    max_date = date.fromisoformat(str(max_date_obj)) if max_date_obj else None
    return min_date, max_date
