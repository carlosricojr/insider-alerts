"""Narrow IBKR adapter that can only retrieve historical daily bars."""

from __future__ import annotations

import asyncio
import contextlib
import math
from datetime import UTC, date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from insider_alerts.backtest.models import DailyBar
from insider_alerts.research.bar_feed import SourceBarBatch

_CONNECT_TIMEOUT_SECONDS = 10.0
_QUALIFY_TIMEOUT_SECONDS = 8.0
_HISTORICAL_TIMEOUT_SECONDS = 30.0
NEW_YORK = ZoneInfo("America/New_York")


class IbkrHistoricalBarSource:
    """A deliberately narrow, read-only surface for a separate feed process."""

    def __init__(self, *, host: str, port: int, client_id: int) -> None:
        self.host = host
        self.port = port
        self.client_id = client_id
        self.__ib: Any = None
        self.__contracts: dict[str, Any] = {}

    async def connect(self) -> None:
        if self.__ib is not None and self.__ib.isConnected():
            return
        from ib_async import IB

        self.__ib = IB()
        try:
            self.__ib.wrapper.clientId = self.client_id
            await self.__ib.client.connectAsync(
                self.host,
                self.port,
                self.client_id,
                _CONNECT_TIMEOUT_SECONDS,
            )
            if not self.__ib.client.isReady():
                raise ConnectionError("socket is not API-ready after handshake")
            self.__ib.connectedEvent.emit()
        except Exception as exc:
            with contextlib.suppress(Exception):
                self.__ib.disconnect()
            self.__ib = None
            detail = str(exc).strip() or type(exc).__name__
            raise ConnectionError(
                f"IBKR historical-bar source unavailable at {self.host}:{self.port}: {detail}"
            ) from exc

    def disconnect(self) -> None:
        if self.__ib is not None and self.__ib.isConnected():
            self.__ib.disconnect()
        self.__ib = None
        self.__contracts.clear()

    async def __contract(self, symbol: str) -> Any:
        normalized = symbol.upper()
        if normalized in self.__contracts:
            return self.__contracts[normalized]
        from ib_async import RequestError, Stock

        contract = Stock(normalized, "SMART", "USD")
        previous = bool(self.__ib.RaiseRequestErrors)
        self.__ib.RaiseRequestErrors = True
        try:
            qualified = await asyncio.wait_for(
                self.__ib.qualifyContractsAsync(contract),
                timeout=_QUALIFY_TIMEOUT_SECONDS,
            )
        except RequestError as exc:
            raise LookupError(f"IBKR could not qualify US stock symbol {normalized}") from exc
        finally:
            self.__ib.RaiseRequestErrors = previous
        if not qualified or not contract.conId:
            raise LookupError(f"IBKR could not qualify US stock symbol {normalized}")
        self.__contracts[normalized] = contract
        return contract

    async def daily_bars(self, symbol: str, *, start_date: date) -> SourceBarBatch:
        contract = await self.__contract(symbol)
        calendar_days = max(
            30,
            (datetime.now(UTC).astimezone(NEW_YORK).date() - start_date).days + 10,
        )
        duration = (
            f"{calendar_days} D" if calendar_days <= 365 else f"{math.ceil(calendar_days / 365)} Y"
        )
        bars = await self.__ib.reqHistoricalDataAsync(
            contract,
            "",
            duration,
            "1 day",
            "TRADES",
            True,
            2,
            timeout=_HISTORICAL_TIMEOUT_SECONDS,
        )
        if not bars:
            raise RuntimeError(
                f"IBKR historical bars unavailable for {symbol.upper()} "
                "(timeout, no data, permissions, or pacing)"
            )
        output: list[DailyBar] = []
        rejections: list[str] = []
        for bar in bars:
            trade_date = bar.date.date() if isinstance(bar.date, datetime) else bar.date
            if not isinstance(trade_date, date):
                rejections.append("unknown_date:invalid_trade_date")
                continue
            try:
                values = [
                    float(bar.open),
                    float(bar.high),
                    float(bar.low),
                    float(bar.close),
                    float(bar.volume),
                ]
            except (TypeError, ValueError):
                rejections.append(f"{trade_date.isoformat()}:non_numeric_ohlcv")
                continue
            output.append(
                DailyBar(
                    symbol=symbol.upper(),
                    trade_date=trade_date,
                    open=values[0],
                    high=values[1],
                    low=values[2],
                    close=values[3],
                    volume=values[4],
                )
            )
        return SourceBarBatch(
            tuple(sorted(output, key=lambda item: item.trade_date)), tuple(rejections)
        )
