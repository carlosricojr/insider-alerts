from __future__ import annotations

import gzip
import json
import logging
import sqlite3
import threading
import time
import zlib
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, date, datetime
from http.client import HTTPException
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote as url_quote
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from insider_alerts.config import AUTOPILOT_IB_REQUEST_TIMEOUT_SECONDS

logger = logging.getLogger(__name__)

# Fallback-only HTTP source. Primary is IB Gateway (see _IBBarSource).
_YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart"

# IB Gateway is the authoritative feed on this host: it is authenticated, always-on, and the
# same source the rest of the stack trades against. Verified 2026-08-11 to cover the full
# insider universe including OTC pink sheets (RDGL @ $0.0538) which Yahoo prices as null.
class MarketContextError(RuntimeError):
    """Raised when market context lookup fails."""


class _IBBarSource:
    """Lazy, process-wide IB Gateway connection for daily bars.

    One connection is shared across the whole autopilot loop: reconnecting per symbol would
    dominate latency and burn client ids. ``ib_async`` is imported lazily so the module still
    imports (and the Yahoo fallback still works) on hosts without the dependency or Gateway.
    """

    _lock = threading.Lock()
    _ib: Any | None = None
    _unavailable_reason: str | None = None
    _host = "127.0.0.1"
    _port = 4001
    _client_id = 171

    @classmethod
    def configure(cls, *, host: str, port: int, client_id: int) -> None:
        endpoint = (host, int(port), int(client_id))
        if endpoint == (cls._host, cls._port, cls._client_id):
            return
        if cls._ib is not None:
            with suppress(Exception):
                cls._ib.disconnect()
        cls._ib = None
        cls._host, cls._port, cls._client_id = endpoint

    @classmethod
    def _connect(cls) -> Any | None:
        if cls._ib is not None and cls._ib.isConnected():
            return cls._ib
        try:
            import asyncio

            try:
                asyncio.get_event_loop()
            except RuntimeError:
                asyncio.set_event_loop(asyncio.new_event_loop())
            from ib_async import IB
        except ImportError as exc:  # pragma: no cover - depends on host deps
            cls._unavailable_reason = f"ib_async not installed: {exc}"
            return None
        ib: Any | None = None
        try:
            ib = IB()
            # ib_async otherwise lets synchronous requests wait forever. Keep both contract
            # qualification and historical bars inside the autopilot watchdog budget.
            ib.RequestTimeout = AUTOPILOT_IB_REQUEST_TIMEOUT_SECONDS
            ib.connect(
                cls._host,
                cls._port,
                clientId=cls._client_id,
                timeout=AUTOPILOT_IB_REQUEST_TIMEOUT_SECONDS,
                readonly=True,
            )
            ib.reqMarketDataType(1)
            cls._ib = ib
            cls._unavailable_reason = None
            return ib
        except Exception as exc:  # noqa: BLE001 - any connect failure degrades to fallback
            try:
                if ib is not None:
                    ib.disconnect()
            except Exception:  # noqa: BLE001 - preserve the original failure
                pass
            cls._unavailable_reason = f"IB Gateway {cls._host}:{cls._port} unreachable: {exc}"
            cls._ib = None
            return None

    @classmethod
    def fetch(cls, symbol: str) -> dict[date, tuple[float, float]]:
        """Return ``{trade_date: (close, volume)}``; empty dict if IB cannot serve the symbol."""
        with cls._lock:
            ib = cls._connect()
            if ib is None:
                return {}
            try:
                from ib_async import Stock

                contract = Stock(symbol.upper(), "SMART", "USD")
                qualified = ib.qualifyContracts(contract)
                if not qualified or not contract.conId:
                    cls._unavailable_reason = None
                    return {}
                bars = ib.reqHistoricalData(
                    contract,
                    "",
                    "10 D",
                    "1 day",
                    "TRADES",
                    useRTH=True,
                    formatDate=2,
                    timeout=AUTOPILOT_IB_REQUEST_TIMEOUT_SECONDS,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("IB bar fetch failed for %s: %s", symbol, exc)
                cls._unavailable_reason = f"IB bar request failed: {exc}"
                return {}
        cls._unavailable_reason = None
        out: dict[date, tuple[float, float]] = {}
        for bar in bars or []:
            bar_date = getattr(bar, "date", None)
            if isinstance(bar_date, datetime):
                bar_date = bar_date.date()
            if not isinstance(bar_date, date):
                continue
            try:
                close = float(bar.close)
                volume = float(bar.volume)
            except (TypeError, ValueError):
                continue
            if close > 0 and volume > 0:
                out[bar_date] = (close, volume)
        return out


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
    source: str = "ibkr"


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
        rate_limit_per_second: float = 1.0,
        retry_attempts: int = 3,
        retry_min_seconds: float = 0.5,
        retry_max_seconds: float = 3.0,
        shock_drop_threshold: float = 0.08,
        ib_gateway_host: str = "127.0.0.1",
        ib_gateway_port: int = 4001,
        ib_client_id: int = 171,
        now_fn: Callable[[], float] = time.monotonic,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        self.user_agent = user_agent
        self.timeout_seconds = timeout_seconds
        self.rate_limit_per_second = rate_limit_per_second
        self.retry_attempts = max(1, retry_attempts)
        self.retry_min_seconds = max(0.0, retry_min_seconds)
        self.retry_max_seconds = max(self.retry_min_seconds, retry_max_seconds)
        self.shock_drop_threshold = shock_drop_threshold
        self.now_fn = now_fn
        self.sleep_fn = sleep_fn
        self._last_request_ts = 0.0
        _IBBarSource.configure(
            host=ib_gateway_host,
            port=ib_gateway_port,
            client_id=ib_client_id,
        )

    def _enforce_rate_limit(self) -> None:
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

    def _download_text(self, url: str, *, symbol: str) -> str:
        """GET ``url`` with the configured retry/rate-limit policy and return the body.

        ``symbol`` is carried purely so failures name the instrument rather than a long URL.
        """
        if not url.strip():
            raise MarketContextError(f"empty url for {symbol}")
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
                    body = bytes(response.read())
                    headers = getattr(response, "headers", None)
                    encoding = headers.get("Content-Encoding", "") if headers is not None else ""
                    normalized_encoding = str(encoding).strip().lower()
                    if normalized_encoding == "gzip":
                        body = gzip.decompress(body)
                    elif normalized_encoding == "deflate":
                        body = zlib.decompress(body)
                    return body.decode("utf-8", "replace")
            except HTTPError as exc:
                last_error = exc
                if (
                    attempt < self.retry_attempts
                    and self._is_retryable_http_status(int(exc.code))
                ):
                    delay = self._retry_delay(attempt)
                    if delay > 0:
                        self.sleep_fn(delay)
                    continue
                raise MarketContextError(
                    f"market data request failed for {symbol}: HTTP {exc.code} {exc.reason}"
                ) from exc
            except (OSError, EOFError, URLError, HTTPException, ValueError, zlib.error) as exc:
                last_error = exc
                if attempt < self.retry_attempts:
                    delay = self._retry_delay(attempt)
                    if delay > 0:
                        self.sleep_fn(delay)
                    continue
                raise MarketContextError(f"market data request failed for {symbol}: {exc}") from exc
        if last_error is not None:
            raise MarketContextError(
                f"market data request failed for {symbol}: {last_error}"
            ) from last_error
        raise MarketContextError(
            f"market data request failed for {symbol}: unknown network failure"
        )

    def _fetch_bars(self, symbol: str) -> tuple[dict[date, tuple[float, float]], str]:
        """Return ``({trade_date: (close, volume)}, source_name)``.

        IB Gateway is primary; the Yahoo chart API is a fallback for the windows when Gateway
        is down (it needs a manual 2FA re-login weekly). The original stooq CSV feed went dark
        on 2026-02-12 when stooq put a JavaScript bot-challenge in front of the endpoint: it
        began returning an HTML "This site requires JavaScript" page with HTTP 200, which the
        CSV reader silently parsed to zero rows. That produced
        ``trade_pct_daily_turnover=None`` on 100% of packets for ~6 months and silently
        disabled every liquidity guard, so exhausting all sources now RAISES instead of
        returning empty -- a dead feed must never look like a quiet market.
        """
        bars: dict[date, tuple[float, float]] = {}
        for attempt in range(1, self.retry_attempts + 1):
            self._enforce_rate_limit()
            bars = _IBBarSource.fetch(symbol)
            if bars:
                return bars, "ibkr"
            if _IBBarSource._unavailable_reason is None or attempt >= self.retry_attempts:
                break
            delay = self._retry_delay(attempt)
            if delay > 0:
                self.sleep_fn(delay)
        ib_reason = _IBBarSource._unavailable_reason
        if ib_reason:
            logger.warning("IB unavailable (%s); falling back to yahoo for %s", ib_reason, symbol)
        try:
            return self._fetch_bars_yahoo(symbol), "yahoo"
        except MarketContextError:
            if ib_reason:
                raise MarketContextError(
                    f"no market data source available for {symbol}: {ib_reason}; yahoo also failed"
                ) from None
            raise

    def _fetch_bars_yahoo(self, symbol: str) -> dict[date, tuple[float, float]]:
        encoded_symbol = url_quote(symbol.upper(), safe="")
        url = f"{_YAHOO_CHART_URL}/{encoded_symbol}?{urlencode({'range': '10d', 'interval': '1d'})}"
        text = self._download_text(url, symbol=symbol)
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise MarketContextError(
                f"market data for {symbol} was not JSON (source may be challenging the client): "
                f"{text[:120]!r}"
            ) from exc
        chart = payload.get("chart") or {}
        error = chart.get("error")
        if error:
            raise MarketContextError(f"market data error for {symbol}: {error}")
        results = chart.get("result") or []
        if not results:
            return {}
        result = results[0]
        stamps = result.get("timestamp") or []
        quote_blocks = (result.get("indicators") or {}).get("quote") or [{}]
        quote = quote_blocks[0] if quote_blocks else {}
        closes = quote.get("close") or []
        volumes = quote.get("volume") or []
        bars: dict[date, tuple[float, float]] = {}
        for idx, stamp in enumerate(stamps):
            if idx >= len(closes) or idx >= len(volumes):
                break
            close_obj = closes[idx]
            volume_obj = volumes[idx]
            # Yahoo emits null for halted/incomplete sessions; skip rather than coerce to 0.
            if close_obj is None or volume_obj is None:
                continue
            try:
                bar_date = datetime.fromtimestamp(float(stamp), tz=UTC).date()
                bars[bar_date] = (float(close_obj), float(volume_obj))
            except (TypeError, ValueError, OSError):
                continue
        return bars

    def fetch_snapshot(self, symbol: str, *, trade_date: date) -> MarketSnapshot | None:
        indexed, source_name = self._fetch_bars(symbol)
        if not indexed:
            return None

        bar = indexed.get(trade_date)
        if bar is None:
            return None
        close, volume = bar

        if close <= 0 or volume <= 0:
            return None

        prior_close: float | None = None
        return_1d: float | None = None
        prior_dates = sorted(d for d in indexed if d < trade_date)
        if prior_dates:
            prior_close_value = indexed[prior_dates[-1]][0]
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
            source=source_name,
        )
