from __future__ import annotations

import asyncio
import math
import re
import sys
from collections import deque
from collections.abc import Callable
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from typing import Any, Literal

from insider_alerts.backtest.models import DailyBar
from insider_alerts.execution.calendar import (
    CALENDAR_SHA256,
    ENTRY_HORIZON_LIMIT,
    ENTRY_RENEWAL_START,
    FALLBACK_ENTRY_EXPIRY,
    MANAGEMENT_RECOVERY_START,
    NEW_YORK,
    SOURCE,
    bounds,
    calendar_dates,
    entry_approval,
    parse_native_schedule,
    validate_contract_hours,
    validate_future_projection_recovery,
    validate_native_schedule,
)
from insider_alerts.execution.canary import (
    AccountSnapshot,
    BrokerOrder,
    CommissionPreview,
)
from insider_alerts.execution.errors import (
    IbkrContractQualificationError,
    IbkrExecutionError,
)

__all__ = ["IbkrBroker", "IbkrExecutionError"]

_CONNECT_TIMEOUT_SECONDS = 10.0
_OPEN_ORDERS_TIMEOUT_SECONDS = 10.0
_SCHEDULE_TIMEOUT_SECONDS = 5.0


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
        self.exit_modification_intent: Callable[[dict[str, object]], None] | None = None
        self.ib: Any = None
        self._contracts: dict[str, Any] = {}
        self._errors: deque[tuple[int, int, str]] = deque(maxlen=100)
        self._all_open_trades: list[Any] = []
        self._all_orders_checked_at = 0.0
        self._allow_calendar_fallback = False
        self.schedule_evidence: dict[str, str] = {}

    async def connect(self, *, readonly: bool) -> None:
        if self.ib is not None and self.ib.isConnected():
            return
        from ib_async import IB

        self.ib = IB()
        self._allow_calendar_fallback = not readonly
        self.ib.errorEvent += self._on_error
        self._errors.clear()
        try:
            if readonly:
                # IB.connectAsync always requests positions, even when readonly=True and
                # fetchFields=0. Shadow-only operation needs only the API handshake plus
                # market/reference data, so do not create futures for account callbacks.
                self.ib.wrapper.clientId = self.client_id
                await self.ib.client.connectAsync(
                    self.host,
                    self.port,
                    self.client_id,
                    _CONNECT_TIMEOUT_SECONDS,
                )
                if not self.ib.client.isReady():
                    raise ConnectionError("socket is not API-ready after handshake")
                self.ib.connectedEvent.emit()
            else:
                await self.ib.connectAsync(
                    self.host,
                    self.port,
                    clientId=self.client_id,
                    readonly=False,
                    timeout=_CONNECT_TIMEOUT_SECONDS,
                    raiseSyncErrors=True,
                )
            self.ib.reqMarketDataType(1)
        except Exception as exc:  # noqa: BLE001
            try:
                self.ib.errorEvent -= self._on_error
                self.ib.disconnect()
            except Exception:  # noqa: BLE001 - preserve the original connection failure
                pass
            self.ib = None
            reason = (
                "IBKR_GATEWAY_HANDSHAKE_TIMEOUT"
                if isinstance(exc, TimeoutError)
                else "IBKR_GATEWAY_STARTUP_SYNC_FAILED"
            )
            detail = str(exc).strip() or type(exc).__name__
            raise IbkrExecutionError(
                f"{reason}: IB Gateway unavailable at {self.host}:{self.port}: {detail}"
            ) from exc

    def disconnect(self) -> None:
        if self.ib is not None and self.ib.isConnected():
            self.ib.disconnect()
        self.ib = None
        self._contracts.clear()
        self._all_open_trades = []
        self._all_orders_checked_at = 0.0
        self._errors.clear()
        self._allow_calendar_fallback = False
        self.schedule_evidence = {}

    def _on_error(self, req_id: int, code: int, message: str, contract: Any) -> None:
        if code not in {2104, 2106, 2107, 2108, 2158}:
            self._errors.append((req_id, code, message))

    async def _contract(self, symbol: str) -> Any:
        normalized = symbol.upper()
        if normalized in self._contracts:
            return self._contracts[normalized]
        from ib_async import RequestError, Stock

        contract = Stock(normalized, "SMART", "USD")
        previous_raise_request_errors = bool(self.ib.RaiseRequestErrors)
        self.ib.RaiseRequestErrors = True
        try:
            qualified = await self.ib.qualifyContractsAsync(contract)
        except RequestError as exc:
            if exc.code == 200:
                raise IbkrContractQualificationError(
                    f"IBKR could not qualify US stock symbol {normalized}"
                ) from exc
            raise IbkrExecutionError(
                f"IBKR contract lookup failed for {normalized}: {exc}"
            ) from exc
        finally:
            self.ib.RaiseRequestErrors = previous_raise_request_errors
        if not qualified or not contract.conId:
            raise IbkrContractQualificationError(
                f"IBKR could not qualify US stock symbol {normalized}"
            )
        self._contracts[normalized] = contract
        return contract

    async def sessions(self, *, around: datetime, count: int = 90) -> list[date]:
        from ib_async import RequestError

        self.schedule_evidence = {}
        if (
            not isinstance(around, datetime)
            or around.tzinfo is None
            or around.utcoffset() is None
            or isinstance(count, bool)
            or not isinstance(count, int)
            or not 60 <= count <= 365
        ):
            raise IbkrExecutionError("CALENDAR_INVALID_REQUEST")
        today = around.astimezone(NEW_YORK).date()
        end = today + timedelta(days=45)
        # Coverage includes the whole final NY date, even before today's open.
        # Resolve the target date's zone offset rather than carrying UTC time
        # across midnight or a DST transition.
        endpoint = datetime.combine(end, time(23, 59, 59), NEW_YORK).astimezone(UTC)
        request_ib = self.ib
        previous = bool(request_ib.RaiseRequestErrors)
        reason = ""
        projection_receipt: dict[str, str] = {}
        try:
            contract = await asyncio.wait_for(self._contract("SPY"), _SCHEDULE_TIMEOUT_SECONDS)
            self.ib.RaiseRequestErrors = True
            try:
                schedule = await asyncio.wait_for(
                    self.ib.reqHistoricalScheduleAsync(
                        contract, count, endpoint, True
                    ),
                    _SCHEDULE_TIMEOUT_SECONDS,
                )
            except RequestError as exc:
                if exc.code != 162 or "No data of type EODChart" not in str(exc):
                    raise IbkrExecutionError("CALENDAR_SCHEDULE_REQUEST_FAILED") from exc
                reason = "IBKR_162_EODCHART_UNAVAILABLE"
                schedule = None
            if isinstance(schedule, list) and not schedule:
                reason = "IBKR_EMPTY_SCHEDULE"
                schedule = None
            native_dates: list[date] = []
            if schedule is not None:
                try:
                    native_dates = validate_native_schedule(schedule)
                except IbkrExecutionError as native_error:
                    if not self._allow_calendar_fallback:
                        raise
                    try:
                        projection_receipt = validate_future_projection_recovery(
                            schedule, around, count
                        )
                    except IbkrExecutionError as recovery_error:
                        # Recovery eligibility is data-driven, not error-string matching.
                        # Retain the native reason AND why management recovery failed.
                        raise IbkrExecutionError(
                            f"{native_error}; projection_recovery_rejected={recovery_error}"
                        ) from native_error
                    reason = "IBKR_NATIVE_FUTURE_PROJECTION_REJECTED"
                    schedule = None
            if schedule is None:
                if not reason or not self._allow_calendar_fallback:
                    raise IbkrExecutionError("CALENDAR_FALLBACK_NOT_AUTHORIZED")
                source = SOURCE
                expected = calendar_dates(around, count)
                details = await asyncio.wait_for(
                    self.ib.reqContractDetailsAsync(contract), _SCHEDULE_TIMEOUT_SECONDS
                )
                receipt = validate_contract_hours(details, contract, around)
                receipt.update(projection_receipt)
                today_bounds = bounds(today)
                entry_basis = (
                    "projection_rejected" if projection_receipt else "fallback_unavailable"
                )
            else:
                expected = native_dates
                start = end - timedelta(days=count - 1)
                # IBKR can return a wider schedule than the requested date window.
                # Validate ALL rows above (including extras), then enforce coverage
                # and horizon minimums only within the window consumers requested.
                expected = [day for day in expected if start <= day <= end]
                known = {
                    day
                    for offset in range(count)
                    if (day := start + timedelta(days=offset)).year == 2026 and bounds(day)
                }
                if {day for day in expected if day.year == 2026} != known:
                    raise IbkrExecutionError("CALENDAR_SCHEDULE_COVERAGE_MISMATCH")
                if (
                    sum(day > today for day in expected) < 10
                    or sum(day < today for day in expected) < 20
                ):
                    raise IbkrExecutionError("CALENDAR_SCHEDULE_COVERAGE_MISMATCH")
                source = "IBKR-historical-schedule"
                receipt = {"validated_at_utc": around.astimezone(UTC).isoformat()}
                today_bounds = next(
                    ((opens, closes) for day, opens, closes in parse_native_schedule(schedule)
                     if day == today), None,
                )
                entry_basis = "native"
            future = [day for day in expected if today <= day <= ENTRY_HORIZON_LIMIT]
            approval = entry_approval(today)
            receipt.update(
                source=source,
                fallback_reason=reason,
                calendar_sha256=CALENDAR_SHA256,
                entry_expiry_et=str(
                    MANAGEMENT_RECOVERY_START if today < ENTRY_RENEWAL_START
                    else FALLBACK_ENTRY_EXPIRY
                ),
                entry_approval_id=approval or "",
                entry_basis=entry_basis,
                entry_horizon_exit=str(future[9]) if len(future) >= 10 and today in future else "",
                today_open_et=today_bounds[0].isoformat() if today_bounds else "",
                today_close_et=today_bounds[1].isoformat() if today_bounds else "",
                first_session=str(expected[0]),
                last_session=str(expected[-1]),
                today_open=str(around.astimezone(NEW_YORK).date() in expected),
            )
            self.schedule_evidence = receipt
            return expected
        except TimeoutError as exc:
            # wait_for cancels the future, not the IB request. A new connection
            # owns the next attempt, so late replies cannot leak into fallback.
            self.disconnect()
            raise IbkrExecutionError("CALENDAR_REQUEST_TIMEOUT_CONNECTION_RESET") from exc
        except asyncio.CancelledError:
            self.disconnect()
            raise
        except RequestError as exc:
            raise IbkrExecutionError("CALENDAR_CONTRACT_DETAILS_REQUEST_FAILED") from exc
        finally:
            request_ib.RaiseRequestErrors = previous

    def calendar_gate(self, now: datetime, *, for_entry: bool = False) -> str | None:
        """Expiry blocks new risk only; existing holdings keep reconciliation and exits."""
        if not self._allow_calendar_fallback or not for_entry:
            return None
        receipt = self.schedule_evidence
        if not receipt:
            return "calendar_validation_missing"
        validated = datetime.fromisoformat(receipt["validated_at_utc"])
        if (
            not 0 <= (now - validated).total_seconds() <= 60
            or now.astimezone(NEW_YORK).date() != validated.astimezone(NEW_YORK).date()
        ):
            return "calendar_validation_stale"
        if (
            for_entry
            and receipt["source"] == SOURCE
            and entry_approval(now.astimezone(NEW_YORK).date()) is None
        ):
            return "calendar_fallback_entry_approval_expired"
        if receipt.get("today_open") != "True":
            return "calendar_market_closed"
        if not receipt.get("entry_horizon_exit"):
            return "calendar_entry_horizon_incomplete"
        return None

    def session_bounds(self, now: datetime) -> tuple[datetime, datetime] | None:
        """Validated same-day bounds for management, independent of entry expiry.

        No guessed regular close and no entry-only 60-second age limit: discovery
        may take longer, but cannot turn yesterday's receipt into today's clock.
        """
        receipt = self.schedule_evidence
        try:
            validated = datetime.fromisoformat(receipt["validated_at_utc"])
            opens = datetime.fromisoformat(receipt["today_open_et"])
            closes = datetime.fromisoformat(receipt["today_close_et"])
            today = now.astimezone(NEW_YORK).date()
            if (
                now.tzinfo is None
                or any(item.tzinfo is None for item in (validated, opens, closes))
                or validated > now or validated.astimezone(NEW_YORK).date() != today
                or opens.astimezone(NEW_YORK).date() != today
                or closes.astimezone(NEW_YORK).date() != today
                or receipt.get("today_open") != "True" or not opens < closes
            ):
                return None
            return opens, closes
        except (KeyError, ValueError, TypeError):
            return None

    def _check_exit_window(self, window: tuple[datetime, datetime]) -> None:
        now = datetime.now(UTC)
        session = self.session_bounds(now)
        if session is None or not session[0] <= window[0] <= now < window[1] <= session[1]:
            raise IbkrExecutionError("CALENDAR_EXIT_DISPATCH_WINDOW_MISSED")

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
            try:
                open_trades = await asyncio.wait_for(
                    self.ib.reqAllOpenOrdersAsync(),
                    timeout=_OPEN_ORDERS_TIMEOUT_SECONDS,
                )
            except TimeoutError as exc:
                self.disconnect()
                raise IbkrExecutionError(
                    "IBKR_ALL_OPEN_ORDERS_TIMEOUT: all-open-orders reconciliation "
                    f"did not complete within {_OPEN_ORDERS_TIMEOUT_SECONDS:.0f}s; "
                    "broker order state is unknown"
                ) from exc
            except Exception as exc:  # noqa: BLE001
                self.disconnect()
                detail = str(exc).strip() or type(exc).__name__
                raise IbkrExecutionError(
                    "IBKR_ALL_OPEN_ORDERS_FAILED: all-open-orders reconciliation "
                    f"failed; broker order state is unknown: {detail}"
                ) from exc
            self._all_open_trades = list(open_trades)
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

    async def fresh_account_snapshot(self) -> AccountSnapshot:
        """Complete fresh position and all-client order requests before an entry."""
        try:
            try:
                positions = await asyncio.wait_for(
                    self.ib.reqPositionsAsync(), _OPEN_ORDERS_TIMEOUT_SECONDS,
                )
            except Exception as exc:
                self.disconnect()
                raise IbkrExecutionError(
                    "IBKR_ENTRY_POSITIONS_REFRESH_FAILED: broker position state is unknown"
                ) from exc
            self._all_orders_checked_at = float("-inf")
            snapshot = await self.account_snapshot()
            # A complete positions response can contain several updates for one
            # contract, including a final zero. Retain the last callback first.
            latest = {int(p.contract.conId): p for p in positions if p.account == snapshot.account}
            selected = [p for p in latest.values() if p.position != 0]
            if (any(conid <= 0 for conid in latest)
                or any(not math.isfinite(float(p.position)) for p in selected)
                or len({str(p.contract.symbol) for p in selected}) != len(selected)):
                self.disconnect()
                raise IbkrExecutionError("IBKR_ENTRY_POSITIONS_AMBIGUOUS")
            return replace(
                snapshot,
                positions={str(p.contract.symbol): float(p.position) for p in selected},
                average_costs={str(p.contract.symbol): float(p.avgCost) for p in selected
                               if math.isfinite(float(p.avgCost)) and float(p.avgCost) > 0},
            )
        except asyncio.CancelledError:
            self.disconnect()
            raise

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
            else (
                "invalid IBKR commission preview for preflight"
                if has_currency
                else "IBKR commission preview missing currency"
            )
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
        calendar_error = self.calendar_gate(datetime.now(UTC), for_entry=True)
        if calendar_error is not None:
            raise IbkrExecutionError(calendar_error)
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
        calendar_error = self.calendar_gate(datetime.now(UTC), for_entry=True)
        if calendar_error is not None:
            raise IbkrExecutionError(calendar_error)
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
        dispatch_window: tuple[datetime, datetime],
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
        self._check_exit_window(dispatch_window)
        trade = self.ib.placeOrder(contract, order)
        await asyncio.sleep(0.5)
        if trade.orderStatus.status in {"Inactive", "Cancelled", "ApiCancelled"}:
            raise IbkrExecutionError(
                f"IBKR rejected market-on-close for {symbol}: "
                + self._order_rejection_detail(
                    int(trade.order.orderId), order.account, trade.orderStatus.status,
                )
            )
        return int(trade.order.orderId)

    def _order_rejection_detail(self, order_id: int, account: str, status: str) -> str:
        # IB uses one monotonic request/order ID sequence per connection; connect
        # clears _errors. Match only this order, never connection/request errors.
        # errorEvent has the raw broker text; trade.log may append contract reprs.
        errors = [(code, message) for req, code, message in self._errors if req == order_id]
        details: list[str] = []
        for code, message in errors[-3:]:
            safe_code = str(code) if type(code) is int and 0 <= code <= 2**31 - 1 else "unknown"
            if isinstance(message, str):
                text = "".join(char for char in message if char.isprintable())
                for identifier in (account, self.configured_account):
                    if identifier:
                        text = text.replace(identifier, "[account]")
                text = re.sub(r"(?:DU|DF|U|F)\d{5,}", "[account]", text, flags=re.I)
                text = text[:240] + ("..." if len(text) > 240 else "")
            else:
                text = "broker message unavailable"
            details.append(f"code={safe_code} message={text}")
        reason = " | ".join(details) if details else "no broker error recorded"
        return f"order_id={order_id} status={status}; {reason}"

    async def submit_market_exit(
        self,
        symbol: str,
        quantity: int,
        *,
        oca_group: str,
        order_ref: str,
        dispatch_window: tuple[datetime, datetime] | None = None,
    ) -> int:
        from ib_async import MarketOrder

        contract = await self._contract(symbol)
        try:
            active = await asyncio.wait_for(
                self.ib.reqAllOpenOrdersAsync(), _OPEN_ORDERS_TIMEOUT_SECONDS,
            )
        except Exception as exc:
            self.disconnect()
            raise IbkrExecutionError("IBKR_EXIT_OPEN_ORDERS_FAILED") from exc
        matches = [t for t in active if t.order.orderRef == order_ref
                   and t.order.account == self._account()]
        if matches:
            if len(matches) != 1:
                raise IbkrExecutionError("IBKR_EXIT_DUPLICATE_REFERENCE")
            existing = matches[0]
            if (existing.contract.conId != contract.conId
                or existing.order.action != "SELL" or existing.order.ocaGroup != oca_group
                or existing.order.orderType != "MKT" or existing.order.ocaType != 1
                or float(existing.order.totalQuantity) != quantity
                or existing.orderStatus.status not in {
                    "Submitted", "PreSubmitted", "PendingSubmit",
                }):
                raise IbkrExecutionError("IBKR_EXIT_IDENTITY_CONFLICT")
            return int(existing.order.orderId)
        prefix = order_ref.rsplit("-", 1)[0]
        if not await self.confirm_missing_exit(
            symbol, quantity, oca_group=oca_group, order_ref_prefix=prefix,
            allow_partial_protection=order_ref.endswith("-QTYFAIL"),
        ):
            raise IbkrExecutionError("IBKR_EXIT_POSITION_OR_ORDER_CONFLICT")
        if order_ref.endswith("-OVERDUE"):
            targets = [t for t in self._all_open_trades
                       if t.order.account == self._account()
                       and str(t.contract.symbol) == symbol
                       and t.order.orderRef == prefix + "-TARGET"]
            if targets:
                stops = [t for t in self._all_open_trades
                         if t.order.account == self._account()
                         and str(t.contract.symbol) == symbol
                         and t.order.orderRef == prefix + "-STOP"]
                if (len(targets) != 1 or len(stops) != 1
                    or targets[0].order.clientId != self.client_id
                    or stops[0].order.clientId != self.client_id
                    or float(stops[0].orderStatus.remaining) != quantity
                    or float(targets[0].orderStatus.remaining) != quantity):
                    raise IbkrExecutionError("IBKR_EXIT_TARGET_OWNERSHIP_CONFLICT")
                return await self._replace_target_with_due_exit(
                    targets[0], contract, order_ref, dispatch_window,
                )
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
        if dispatch_window is not None:
            self._check_exit_window(dispatch_window)
        trade = self.ib.placeOrder(contract, order)
        await asyncio.sleep(0.5)
        if trade.orderStatus.status in {"Inactive", "Cancelled", "ApiCancelled"}:
            raise IbkrExecutionError(
                f"IBKR rejected emergency market exit for {symbol}: "
                + self._order_rejection_detail(
                    int(trade.order.orderId), order.account, trade.orderStatus.status,
                )
            )
        return int(trade.order.orderId)

    async def _replace_target_with_due_exit(
        self, target: Any, contract: Any, order_ref: str,
        dispatch_window: tuple[datetime, datetime] | None,
    ) -> int:
        """Make the existing sell marketable with a price-only modification."""
        if dispatch_window is None or target.order.orderId <= 0 or target.order.permId <= 0:
            raise IbkrExecutionError("IBKR_EXIT_TARGET_MODIFICATION_NOT_PROVEN")
        def identity(trade: Any) -> tuple[Any, ...]:
            order = trade.order
            return (order.orderId, order.permId, order.clientId, order.account,
                    trade.contract.conId, order.totalQuantity, order.ocaGroup,
                    order.ocaType, order.orderRef, order.orderType)

        expected_target = identity(target)
        stop_ref = order_ref.rsplit("-", 1)[0] + "-STOP"
        initial_stops = [t for t in self._all_open_trades
                         if t.order.account == target.order.account
                         and t.order.orderRef == stop_ref]
        if len(initial_stops) != 1:
            raise IbkrExecutionError("IBKR_EXIT_STOP_IDENTITY_MISSING")
        expected_stop = identity(initial_stops[0])
        request_ib = self.ib
        ticker = request_ib.reqMktData(contract, "", False, False)
        bid_observed_at: datetime | None = None
        observed_bid: float | None = None

        def on_quote(updated: Any) -> None:
            nonlocal bid_observed_at, observed_bid
            for tick in updated.ticks:
                if tick.tickType == 1 and math.isfinite(float(tick.price)) and tick.price > 0:
                    bid_observed_at = tick.time
                    observed_bid = float(tick.price)

        ticker.updateEvent += on_quote
        try:
            quote_deadline = asyncio.get_running_loop().time() + 5.0
            while asyncio.get_running_loop().time() < quote_deadline:
                await asyncio.sleep(0.1)
                if (ticker.marketDataType == 1 and observed_bid is not None
                    and float(ticker.bid) == observed_bid
                    and math.isfinite(float(ticker.bidSize)) and float(ticker.bidSize) > 0
                    and isinstance(bid_observed_at, datetime)
                    and bid_observed_at.tzinfo is not None):
                    break
            else:
                raise IbkrExecutionError("IBKR_EXIT_LIVE_BID_UNAVAILABLE")
            bid = observed_bid
            quote_time = bid_observed_at
        finally:
            ticker.updateEvent -= on_quote
            request_ib.cancelMktData(contract)
        prefix = order_ref.rsplit("-", 1)[0]
        if not await self.confirm_missing_exit(
            str(contract.symbol), int(target.order.totalQuantity),
            oca_group=target.order.ocaGroup, order_ref_prefix=prefix,
        ):
            self.disconnect()
            raise IbkrExecutionError("IBKR_EXIT_POST_QUOTE_RECONCILIATION_FAILED")
        if quote_time is None or not 0 <= (datetime.now(UTC) - quote_time).total_seconds() <= 3:
            raise IbkrExecutionError("IBKR_EXIT_LIVE_BID_STALE")
        current = [t for t in self._all_open_trades
                   if t.order.account == target.order.account
                   and t.order.orderRef == target.order.orderRef]
        current_stops = [t for t in self._all_open_trades
                         if t.order.account == target.order.account
                         and t.order.orderRef == stop_ref]
        if (len(current) != 1 or len(current_stops) != 1
            or identity(current[0]) != expected_target
            or identity(current_stops[0]) != expected_stop
            or current_stops[0].orderStatus.status not in {"Submitted", "PreSubmitted"}
            or float(current_stops[0].orderStatus.remaining) != float(target.order.totalQuantity)
            or not callable(self.exit_modification_intent)):
            self.disconnect()
            raise IbkrExecutionError("IBKR_EXIT_TARGET_INTENT_OR_IDENTITY_MISSING")
        target = current[0]
        if (target.orderStatus.status not in {"Submitted", "PreSubmitted"}
            or float(target.orderStatus.remaining) != float(target.order.totalQuantity)
            or not request_ib.isConnected()):
            self.disconnect()
            raise IbkrExecutionError("IBKR_EXIT_TARGET_STATE_CHANGED")
        self._check_exit_window(dispatch_window)
        self.exit_modification_intent({
            "target_order_id": int(target.order.orderId), "requested_limit_price": bid,
            "quote_asof": quote_time.isoformat(), "conid": int(contract.conId),
            "perm_id": int(target.order.permId),
        })
        if target.order.lmtPrice == bid:
            return int(target.order.orderId)
        replacement = deepcopy(target.order)
        replacement.lmtPrice = float(bid)
        self._errors = deque((e for e in self._errors if e[0] != replacement.orderId), maxlen=100)
        self._check_exit_window(dispatch_window)
        if (not request_ib.isConnected() or quote_time is None
            or not 0 <= (datetime.now(UTC) - quote_time).total_seconds() <= 3):
            raise IbkrExecutionError("IBKR_EXIT_BID_EXPIRED_BEFORE_DISPATCH")
        trade = self.ib.placeOrder(contract, replacement)
        deadline = asyncio.get_running_loop().time() + 5.0
        while asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.1)
            errors = [code for req, code, _ in self._errors if req == replacement.orderId]
            if errors or trade.orderStatus.status in {"Inactive", "Cancelled", "ApiCancelled"}:
                detail = self._order_rejection_detail(
                    replacement.orderId, replacement.account, trade.orderStatus.status,
                )
                self.disconnect()  # Rebuild SDK state; rejection need not cancel the original.
                raise IbkrExecutionError("IBKR_EXIT_TARGET_MODIFICATION_REJECTED: " + detail)
            if (trade.order.lmtPrice == replacement.lmtPrice
                and trade.order.orderId == replacement.orderId
                and trade.order.permId == replacement.permId
                and trade.orderStatus.status in {"Submitted", "PreSubmitted", "Filled"}):
                return int(replacement.orderId)
        self.disconnect()
        raise IbkrExecutionError("IBKR_EXIT_TARGET_MODIFICATION_ACK_TIMEOUT")

    async def confirm_missing_exit(
        self, symbol: str, quantity: int, *, oca_group: str, order_ref_prefix: str,
        allow_partial_protection: bool = False,
    ) -> bool:
        """Prove absence across all clients; never equate cached absence with expiry."""
        account = self._account()
        contract = await self._contract(symbol)
        try:
            trades = await asyncio.wait_for(
                self.ib.reqAllOpenOrdersAsync(), _OPEN_ORDERS_TIMEOUT_SECONDS,
            )
            positions = await asyncio.wait_for(
                self.ib.reqPositionsAsync(), _OPEN_ORDERS_TIMEOUT_SECONDS,
            )
        except Exception as exc:
            self.disconnect()
            raise IbkrExecutionError("IBKR_MISSING_EXIT_RECONCILIATION_FAILED") from exc
        matching_positions = [
            p for p in positions if p.account == account and str(p.contract.symbol) == symbol
        ]
        if any(p.contract.conId != contract.conId or p.contract.secType != "STK"
               or p.contract.currency != "USD" for p in matching_positions):
            return False
        current = sum(
            float(p.position) for p in positions
            if p.account == account and str(p.contract.symbol) == symbol
        )
        if not math.isfinite(current) or current != quantity or quantity <= 0:
            return False
        for trade in trades:
            order = trade.order
            if order.account != account or str(trade.contract.symbol) != symbol:
                continue
            if not (
                order.ocaGroup == oca_group and order.action == "SELL"
                and order.orderRef in {order_ref_prefix + "-STOP", order_ref_prefix + "-TARGET"}
                and trade.contract.conId == contract.conId
                and trade.contract.secType == "STK" and trade.contract.currency == "USD"
                and order.orderType == ("STP" if order.orderRef.endswith("-STOP") else "LMT")
                and order.ocaType == 1
                and (allow_partial_protection or float(order.totalQuantity) == quantity)
                and trade.orderStatus.status in {"Submitted", "PreSubmitted"}
                and (allow_partial_protection or float(trade.orderStatus.filled) == 0)
            ):
                return False
        self._all_open_trades = list(trades)
        self._all_orders_checked_at = asyncio.get_running_loop().time()
        return True

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
            order_id = int(trade.order.orderId)
            filled = float(trade.orderStatus.filled)
            remaining = float(trade.orderStatus.remaining)
            average_price = float(trade.orderStatus.avgFillPrice or 0.0)
            # completedOrder omits API orderId/status fill totals after reconnect.
            # Recover only a complete, uniquely identified execution set; never
            # infer a fill from the terminal status or from a current flat position.
            if order_id == 0 and trade.orderStatus.status == "Filled" and trade.fills:
                executions = [fill.execution for fill in trade.fills]
                ids = {execution.orderId for execution in executions}
                exec_ids = {execution.execId for execution in executions}
                valid = all(
                    fill.contract.conId == trade.contract.conId
                    and fill.execution.acctNumber == trade.order.account
                    and fill.execution.permId == trade.order.permId
                    and math.isfinite(float(fill.execution.shares))
                    and math.isfinite(float(fill.execution.price))
                    and float(fill.execution.shares) > 0 and float(fill.execution.price) > 0
                    for fill in trade.fills
                )
                shares = sum(float(execution.shares) for execution in executions)
                if (valid and len(ids) == 1 and next(iter(ids)) > 0
                    and len(exec_ids) == len(executions) and "" not in exec_ids
                    and shares == float(trade.order.totalQuantity)):
                    order_id = int(next(iter(ids)))
                    filled = shares
                    remaining = 0.0
                    average_price = sum(float(e.shares) * float(e.price)
                                        for e in executions) / shares
            commissions = [
                float(fill.commissionReport.commission)
                for fill in trade.fills
                if fill.commissionReport is not None
                and math.isfinite(float(fill.commissionReport.commission))
            ]
            output.append(
                BrokerOrder(
                    order_id=order_id,
                    order_ref=ref,
                    symbol=str(trade.contract.symbol),
                    kind=kind,
                    status=str(trade.orderStatus.status),
                    filled=filled,
                    remaining=remaining,
                    average_fill_price=average_price,
                    commission=sum(commissions) if commissions else None,
                    perm_id=int(trade.order.permId), conid=int(trade.contract.conId),
                    limit_price=(float(trade.order.lmtPrice)
                                 if trade.order.orderType == "LMT" else None),
                )
            )
        return output
