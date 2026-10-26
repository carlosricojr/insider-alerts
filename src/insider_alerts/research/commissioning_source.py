"""Bounded acquisition only: current contract identity and SDK response custody.

No broker account synchronization or order methods. Client-dispatch timestamps
identify the local SDK invocation, never a wire timestamp or exchange receipt.
The low-level history request mirrors the pinned ib_async 2.1 API so timeouts
cannot masquerade as a successful empty response.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import Any

EXCHANGES = frozenset({"NYSE", "NASDAQ", "AMEX", "ARCA", "BATS", "ISLAND", "IEX"})
SYMBOL_MAP = {"BRK.A": "BRK A", "BRK.B": "BRK B"}


def timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("naive acquisition clock")
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


class SourceFailure(ValueError):
    pass


class CommissioningSource:
    def __init__(
        self,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        budget: Callable[[], float],
        factory: Callable[[], Any] | None = None,
    ) -> None:
        self.clock, self.budget, self.factory = clock, budget, factory

    async def _bounded(self, call: Callable[[], Any], maximum: float) -> Any:
        timeout = min(maximum, self.budget())
        if timeout <= 0:
            raise TimeoutError("acquisition window ended")
        return await asyncio.wait_for(call(), timeout=timeout)

    async def acquire(
        self,
        symbol: str,
        *,
        start: date,
        end: datetime,
        expected_conid: int | None,
        schedule: bool = False,
    ) -> dict[str, Any]:
        from ib_async import IB, RequestError, Stock, util

        now = self.clock()
        timestamp(now)
        if (
            not symbol
            or len(symbol) > 32
            or any(ch not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-" for ch in symbol)
            or end.tzinfo is None
            or end > now
            or not 0 <= (end.date() - start).days <= 365
            or (schedule and symbol != "SPY")
        ):
            raise ValueError("invalid bounded acquisition request")
        result: dict[str, Any] = {
            "source": "IBKR_SDK_PROJECTION_V2",
            "symbol": symbol,
            "mapped_symbol": SYMBOL_MAP.get(symbol, symbol),
            "started_at": timestamp(now),
            "identity": None,
            "request": None,
            "client_dispatch_at": None,
            "response_at": None,
            "response": None,
            "result": "incomplete",
            "error_code": None,
            "stage": "connect",
        }
        ib = self.factory() if self.factory else IB()
        req_id: int | None = None
        future: Any = None
        try:
            ib.wrapper.clientId = 179
            ib.RaiseRequestErrors = True
            await self._bounded(lambda: ib.client.connectAsync("127.0.0.1", 4001, 179, 10), 10)
            if not ib.client.isReady():
                raise SourceFailure("api_not_ready")
            ib.connectedEvent.emit()
            result["stage"] = "qualify"
            details = await self._bounded(
                lambda: ib.reqContractDetailsAsync(Stock(result["mapped_symbol"], "SMART", "USD")),
                8,
            )
            if len(details) != 1:
                raise SourceFailure("contract_missing" if not details else "contract_ambiguous")
            contract = details[0].contract
            result["identity"] = {
                "conid": contract.conId,
                "symbol": contract.symbol,
                "local_symbol": contract.localSymbol,
                "primary_exchange": contract.primaryExchange,
                "security_type": contract.secType,
                "currency": contract.currency,
                "trading_class": contract.tradingClass,
                "observed_at": timestamp(self.clock()),
                "historical_issuer_identity_verified": False,
            }
            if (
                type(contract.conId) is not int
                or contract.conId <= 0
                or contract.symbol != result["mapped_symbol"]
                or contract.secType != "STK"
                or contract.currency != "USD"
                or contract.primaryExchange not in EXCHANGES
            ):
                raise SourceFailure("contract_not_supported")
            if expected_conid is not None and contract.conId != expected_conid:
                raise SourceFailure("contract_identity_changed")
            result["stage"] = "history"
            # Count calendar days conservatively; keep the exact needed range in
            # the request, and let the reader reject/ignore extra returned dates.
            duration = f"{max(2, (end.date() - start).days + 2)} D"
            result["request"] = {
                "start_date": str(start),
                "end_at": timestamp(end),
                "duration": duration,
                "bar_size": "1 day",
                "what_to_show": "SCHEDULE" if schedule else "TRADES",
                "use_rth": True,
                "format_date": 1 if schedule else 2,
            }
            timeout = min(30.0, self.budget())
            if timeout <= 0:
                raise TimeoutError("acquisition window ended")
            req_id = ib.client.getReqId()
            future = ib.wrapper.startReq(req_id, contract)
            result["client_dispatch_at"] = timestamp(self.clock())
            ib.client.reqHistoricalData(
                req_id,
                contract,
                util.formatIBDatetime(end),
                duration,
                "1 day",
                result["request"]["what_to_show"],
                True,
                1 if schedule else 2,
                False,
                [],
            )
            response = await asyncio.wait_for(future, timeout=timeout)
            result["response_at"] = timestamp(self.clock())
            if result["response_at"] < result["client_dispatch_at"]:
                raise SourceFailure("clock_reversed")
            if schedule:
                result["response"] = {
                    "start": str(response.startDateTime),
                    "end": str(response.endDateTime),
                    "timezone": str(response.timeZone),
                    "sessions": [
                        {
                            "date": str(s.refDate),
                            "open": str(s.startDateTime),
                            "close": str(s.endDateTime),
                        }
                        for s in response.sessions
                    ],
                }
                empty = not response.sessions
            else:
                # Strings preserve malformed/non-finite SDK values for later
                # validation without invalid RFC8785 numbers or invented zeros.
                result["response"] = [
                    {
                        name: str(getattr(bar, name))
                        for name in ("date", "open", "high", "low", "close", "volume")
                    }
                    for bar in response
                ]
                empty = not response
            result["result"] = "empty_response" if empty else "received"
        except RequestError as exc:
            result.update(result="broker_error", error_code=int(exc.code))
        except TimeoutError:
            result["result"] = "timeout"
        except SourceFailure as exc:
            result["result"] = str(exc)
        except Exception as exc:
            result.update(result="source_error", error_type=type(exc).__name__)
        finally:
            cleanup_errors = []
            try:
                if req_id is not None:
                    if future is not None and (future.cancelled() or not future.done()):
                        ib.client.cancelHistoricalData(req_id)
                    # Clear locally cancelled requests even if the server never
                    # sends a terminal callback. This connection is never shared.
                    ib.wrapper._endReq(req_id)
                    ib.wrapper._results.pop(req_id, None)
            except Exception as exc:
                cleanup_errors.append(type(exc).__name__)
            try:
                ib.disconnect()
            except Exception as exc:
                cleanup_errors.append(type(exc).__name__)
            if cleanup_errors:
                result.update(result="cleanup_error", cleanup_errors=cleanup_errors)
            result["finished_at"] = timestamp(self.clock())
            clocks = [result["started_at"]]
            if result["identity"]:
                clocks.append(result["identity"]["observed_at"])
            clocks.extend(
                result[key]
                for key in ("client_dispatch_at", "response_at", "finished_at")
                if result[key] is not None
            )
            if clocks != sorted(clocks):
                result["result"] = "clock_reversed"
        return result
