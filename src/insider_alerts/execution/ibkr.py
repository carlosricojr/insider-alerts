from __future__ import annotations

import asyncio
import math
import sys
from collections import deque
from datetime import UTC, date, datetime, timedelta
from typing import Any, Literal

from insider_alerts.backtest.models import DailyBar
from insider_alerts.execution.canary import (
    AccountSnapshot,
    BrokerOrder,
    CommissionPreview,
)


class IbkrExecutionError(RuntimeError):
    """Raised when IBKR cannot safely complete a requested execution action."""


class IbkrBroker:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        client_id: int,
        account: str | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.client_id = client_id
        self.configured_account = account
        self.ib: Any = None
        self._contracts: dict[str, Any] = {}
        self._errors: deque[tuple[int, int, str]] = deque(maxlen=100)
        self._all_open_trades: list[Any] = []
        self._all_orders_checked_at = 0.0

    async def connect(self, *, readonly: bool) -> None:
        if self.ib is not None and self.ib.isConnected():
            return
        from ib_async import IB

        self.ib = IB()
        self.ib.errorEvent += self._on_error
        self._errors.clear()
        try:
            await self.ib.connectAsync(
                self.host,
                self.port,
                clientId=self.client_id,
                readonly=readonly,
                timeout=10,
            )
            self.ib.reqMarketDataType(1)
        except Exception as exc:  # noqa: BLE001
            try:
                self.ib.errorEvent -= self._on_error
                self.ib.disconnect()
            except Exception:  # noqa: BLE001 - preserve the original connection failure
                pass
            self.ib = None
            raise IbkrExecutionError(
                f"IB Gateway unavailable at {self.host}:{self.port}: {exc}"
            ) from exc

    def disconnect(self) -> None:
        if self.ib is not None and self.ib.isConnected():
            self.ib.disconnect()
        self.ib = None
        self._contracts.clear()
        self._all_open_trades = []
        self._all_orders_checked_at = 0.0
        self._errors.clear()

    def _on_error(self, req_id: int, code: int, message: str, contract: Any) -> None:
        if code not in {2104, 2106, 2107, 2108, 2158}:
            self._errors.append((req_id, code, message))

    async def _contract(self, symbol: str) -> Any:
        normalized = symbol.upper()
        if normalized in self._contracts:
            return self._contracts[normalized]
        from ib_async import Stock

        contract = Stock(normalized, "SMART", "USD")
        qualified = await self.ib.qualifyContractsAsync(contract)
        if not qualified or not contract.conId:
            raise IbkrExecutionError(f"IBKR could not qualify US stock symbol {normalized}")
        self._contracts[normalized] = contract
        return contract

    async def sessions(self, *, around: datetime, count: int = 90) -> list[date]:
        contract = await self._contract("SPY")
        schedule = await self.ib.reqHistoricalScheduleAsync(
            contract,
            count,
            around.astimezone(UTC) + timedelta(days=45),
            True,
        )
        return sorted({date.fromisoformat(str(item.refDate)) for item in schedule.sessions})

    async def daily_bars(self, symbol: str, *, duration: str = "6 M") -> list[DailyBar]:
        contract = await self._contract(symbol)
        bars = await self.ib.reqHistoricalDataAsync(
            contract,
            "",
            duration,
            "1 day",
            "TRADES",
            True,
            2,
            timeout=30,
        )
        output: list[DailyBar] = []
        for bar in bars:
            trade_date = bar.date.date() if isinstance(bar.date, datetime) else bar.date
            if not isinstance(trade_date, date):
                continue
            values = [bar.open, bar.high, bar.low, bar.close, bar.volume]
            if any(not math.isfinite(float(value)) for value in values):
                continue
            output.append(
                DailyBar(
                    symbol=symbol.upper(),
                    trade_date=trade_date,
                    open=float(bar.open),
                    high=float(bar.high),
                    low=float(bar.low),
                    close=float(bar.close),
                    volume=float(bar.volume),
                )
            )
        return sorted(output, key=lambda item: item.trade_date)

    def _account(self) -> str:
        accounts = list(self.ib.managedAccounts())
        if self.configured_account:
            if self.configured_account not in accounts:
                raise IbkrExecutionError("configured account is not exposed by this Gateway login")
            return self.configured_account
        if len(accounts) != 1:
            raise IbkrExecutionError(
                "Gateway exposes zero or multiple accounts; configure an explicit account"
            )
        return str(accounts[0])

    async def account_snapshot(self) -> AccountSnapshot:
        def _safe_float(value: Any) -> float:
            try:
                parsed = float(value)
            except (TypeError, ValueError):
                return float("nan")
            return parsed if math.isfinite(parsed) else float("nan")

        account = self._account()
        monotonic_now = asyncio.get_running_loop().time()
        if monotonic_now - self._all_orders_checked_at >= 15.0:
            self._all_open_trades = list(await self.ib.reqAllOpenOrdersAsync())
            self._all_orders_checked_at = monotonic_now
        values = {
            value.tag: value.value
            for value in self.ib.accountValues(account)
            if value.currency in {"USD", ""}
        }
        position_rows = [
            position for position in self.ib.positions(account) if float(position.position) != 0
        ]
        positions = {
            str(position.contract.symbol): float(position.position) for position in position_rows
        }
        average_costs = {
            str(position.contract.symbol): float(position.avgCost)
            for position in position_rows
            if math.isfinite(float(position.avgCost)) and float(position.avgCost) > 0
        }
        return AccountSnapshot(
            account=account,
            net_liquidation=_safe_float(values.get("NetLiquidation")),
            available_funds=_safe_float(values.get("AvailableFunds")),
            settled_cash=_safe_float(values.get("SettledCash")),
            positions=positions,
            open_order_count=max(
                len(self._all_open_trades),
                len(self.ib.openTrades()),
            ),
            average_costs=average_costs,
        )

    async def preview_entry(self, symbol: str, quantity: int) -> CommissionPreview:
        from ib_async import MarketOrder

        contract = await self._contract(symbol)
        self._errors.clear()
        order = MarketOrder("BUY", quantity, tif="OPG", account=self._account())
        try:
            state = await asyncio.wait_for(
                self.ib.whatIfOrderAsync(contract, order),
                timeout=8.0,
            )
        except TimeoutError:
            error_messages = ["what-if request timed out"]
            return CommissionPreview(
                commission=float("nan"),
                currency="USD",
                warning="; ".join(error_messages),
                commission_valid=False,
                commission_error="what-if preview timed out",
                estimate_source="unavailable",
            )
        except Exception as exc:  # noqa: BLE001
            error_messages = [f"what-if preview failed: {exc}"]
            return CommissionPreview(
                commission=float("nan"),
                currency="USD",
                warning="; ".join(error_messages),
                commission_valid=False,
                commission_error="what-if preview failed",
                estimate_source="unavailable",
            )

        errors = [message for _, code, message in self._errors if code >= 100]
        warning = "; ".join(part for part in [getattr(state, "warningText", ""), *errors] if part)
        if not hasattr(state, "commission"):
            error_messages = [
                part for part in [warning, "what-if payload was not an order state"] if part
            ]
            return CommissionPreview(
                commission=float("nan"),
                currency="USD",
                warning="; ".join(error_messages),
                commission_valid=False,
                commission_error="unexpected what-if payload",
                estimate_source="unavailable",
            )

        def valid_commission(value: Any) -> float | None:
            try:
                parsed = float(value)
            except (TypeError, ValueError):
                return None
            if not math.isfinite(parsed) or parsed < 0 or parsed >= (sys.float_info.max / 2):
                return None
            return parsed

        exact_commission = valid_commission(getattr(state, "commission", None))
        min_commission = valid_commission(getattr(state, "minCommission", None))
        max_commission = valid_commission(getattr(state, "maxCommission", None))
        range_is_valid = (
            min_commission is not None
            and max_commission is not None
            and min_commission <= max_commission
        )
        estimate_source: Literal["exact", "range_upper_bound", "unavailable"]
        if exact_commission is not None:
            commission = exact_commission
            estimate_source = "exact"
        elif range_is_valid:
            # Tiered pricing can leave the exact value unset while providing the
            # route-dependent execution range. Budget against its upper bound.
            assert max_commission is not None
            commission = max_commission
            estimate_source = "range_upper_bound"
        else:
            commission = float("nan")
            estimate_source = "unavailable"
        currency_value = getattr(state, "commissionCurrency", None)
        currency = str(currency_value or "USD")
        has_currency = isinstance(currency_value, str) and bool(currency_value.strip())
        is_valid = (exact_commission is not None or range_is_valid) and has_currency
        commission_error = (
            ""
            if is_valid
            else "invalid IBKR commission preview for preflight"
            if has_currency
            else "IBKR commission preview missing currency"
        )
        return CommissionPreview(
            commission=commission,
            currency=currency,
            warning=warning,
            commission_valid=is_valid,
            commission_error=commission_error,
            estimate_source=estimate_source,
            min_commission=min_commission,
            max_commission=max_commission,
        )

    async def submit_market_on_open(self, symbol: str, quantity: int, order_ref: str) -> int:
        from ib_async import MarketOrder

        existing = self._existing_trade(order_ref)
        if existing is not None:
            return int(existing.order.orderId)
        contract = await self._contract(symbol)
        order = MarketOrder(
            "BUY",
            quantity,
            tif="OPG",
            account=self._account(),
            orderRef=order_ref,
            transmit=True,
        )
        trade = self.ib.placeOrder(contract, order)
        await asyncio.sleep(0.5)
        if trade.orderStatus.status in {"Inactive", "Cancelled", "ApiCancelled"}:
            raise IbkrExecutionError(
                f"IBKR rejected entry for {symbol}: {trade.orderStatus.status} {trade.log}"
            )
        return int(trade.order.orderId)

    async def submit_protective_oca(
        self,
        symbol: str,
        quantity: int,
        *,
        stop_price: float,
        target_price: float,
        oca_group: str,
        order_ref_prefix: str,
    ) -> tuple[int, int]:
        from ib_async import LimitOrder, StopOrder

        contract = await self._contract(symbol)
        account = self._account()
        target_ref = f"{order_ref_prefix}-TARGET"
        stop_ref = f"{order_ref_prefix}-STOP"
        terminal = {"Inactive", "Cancelled", "ApiCancelled", "Filled"}
        existing = {
            str(trade.order.orderRef or ""): trade
            for trade in self.ib.trades()
            if str(trade.order.orderRef or "") in {target_ref, stop_ref}
            and trade.orderStatus.status not in terminal
        }
        target = LimitOrder(
            "SELL",
            quantity,
            target_price,
            tif="GTC",
            account=account,
            orderRef=target_ref,
            ocaGroup=oca_group,
            ocaType=1,
            transmit=True,
        )
        stop = StopOrder(
            "SELL",
            quantity,
            stop_price,
            tif="GTC",
            account=account,
            orderRef=stop_ref,
            ocaGroup=oca_group,
            ocaType=1,
            transmit=True,
        )
        # Put the stop on the server first. A target-only failure leaves the position
        # protected; OCA type 1 then blocks both exits from executing concurrently.
        stop_trade = existing.get(stop_ref) or self.ib.placeOrder(contract, stop)
        target_trade = existing.get(target_ref) or self.ib.placeOrder(contract, target)
        await asyncio.sleep(0.5)
        if any(
            trade.orderStatus.status in {"Inactive", "Cancelled", "ApiCancelled"}
            for trade in (target_trade, stop_trade)
        ):
            raise IbkrExecutionError(f"IBKR rejected protective OCA for {symbol}")
        return int(target_trade.order.orderId), int(stop_trade.order.orderId)

    async def submit_market_on_close(
        self,
        symbol: str,
        quantity: int,
        *,
        oca_group: str,
        order_ref: str,
    ) -> int:
        from ib_async import MarketOrder

        existing = self._existing_trade(order_ref)
        if existing is not None:
            return int(existing.order.orderId)
        contract = await self._contract(symbol)
        order = MarketOrder(
            "SELL",
            quantity,
            tif="DAY",
            account=self._account(),
            orderRef=order_ref,
            ocaGroup=oca_group,
            ocaType=1,
            transmit=True,
        )
        order.orderType = "MOC"
        trade = self.ib.placeOrder(contract, order)
        await asyncio.sleep(0.5)
        if trade.orderStatus.status in {"Inactive", "Cancelled", "ApiCancelled"}:
            raise IbkrExecutionError(f"IBKR rejected market-on-close for {symbol}")
        return int(trade.order.orderId)

    async def submit_market_exit(
        self,
        symbol: str,
        quantity: int,
        *,
        oca_group: str,
        order_ref: str,
    ) -> int:
        from ib_async import MarketOrder

        existing = self._existing_trade(order_ref)
        if existing is not None:
            return int(existing.order.orderId)
        contract = await self._contract(symbol)
        order = MarketOrder(
            "SELL",
            quantity,
            tif="DAY",
            account=self._account(),
            orderRef=order_ref,
            ocaGroup=oca_group,
            ocaType=1,
            outsideRth=False,
            transmit=True,
        )
        trade = self.ib.placeOrder(contract, order)
        await asyncio.sleep(0.5)
        if trade.orderStatus.status in {"Inactive", "Cancelled", "ApiCancelled"}:
            raise IbkrExecutionError(f"IBKR rejected emergency market exit for {symbol}")
        return int(trade.order.orderId)

    async def cancel_order(self, order_id: int) -> None:
        trade = next(
            (item for item in self.ib.trades() if int(item.order.orderId) == order_id),
            None,
        )
        if trade is None:
            raise IbkrExecutionError(f"cannot cancel unknown IBKR order id {order_id}")
        self.ib.cancelOrder(trade.order)
        await asyncio.sleep(0.5)

    def _existing_trade(self, order_ref: str) -> Any:
        rejected = {"Inactive", "Cancelled", "ApiCancelled"}
        return next(
            (
                trade
                for trade in self.ib.trades()
                if str(trade.order.orderRef or "") == order_ref
                and trade.orderStatus.status not in rejected
            ),
            None,
        )

    async def orders(self) -> list[BrokerOrder]:
        trades = list(self.ib.trades())
        output: list[BrokerOrder] = []
        for trade in trades:
            ref = str(trade.order.orderRef or "")
            if not ref.startswith("IA-E07-"):
                continue
            if ref.endswith("-ENTRY"):
                kind = "entry"
            elif ref.endswith("-STOP"):
                kind = "stop"
            elif ref.endswith("-TARGET"):
                kind = "target"
            else:
                kind = "time"
            commissions = [
                float(fill.commissionReport.commission)
                for fill in trade.fills
                if fill.commissionReport is not None
                and math.isfinite(float(fill.commissionReport.commission))
            ]
            output.append(
                BrokerOrder(
                    order_id=int(trade.order.orderId),
                    order_ref=ref,
                    symbol=str(trade.contract.symbol),
                    kind=kind,
                    status=str(trade.orderStatus.status),
                    filled=float(trade.orderStatus.filled),
                    remaining=float(trade.orderStatus.remaining),
                    average_fill_price=float(trade.orderStatus.avgFillPrice or 0.0),
                    commission=sum(commissions) if commissions else None,
                )
            )
        return output
