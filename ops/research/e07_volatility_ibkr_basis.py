"""Record one offline IBKR historical close-basis observation through Gateway."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import tempfile
from collections.abc import Awaitable, Callable
from datetime import UTC, date, datetime, time, timedelta
from functools import partial
from importlib.metadata import version
from pathlib import Path
from typing import Any, TypeVar

import rfc8785

from e07_volatility_exits import ibkr_basis
from e07_volatility_exits.ibkr_basis import (
    CLIENT_ID,
    HOST,
    ONE_SECOND,
    PAGE_LIMIT,
    PORT,
    ROUTING_EXCHANGE,
    RULE_SHA256,
    SCHEMA,
    SCOPE,
    TICK_COUNT,
    BasisRequest,
    audit_basis,
    daily_query,
    is_digest,
    sha256,
    tick_query,
)
from insider_alerts.execution.observer import safe_path
from insider_alerts.research.commissioning_source import SYMBOL_MAP, broker_category, error_message
from insider_alerts.strategy.e07 import NEW_YORK

T = TypeVar("T")


def recorder_sha256() -> str:
    """Bind the capture and receipt-reader source at observation time."""
    modules = (Path(__file__), Path(ibkr_basis.__file__))
    return sha256(
        rfc8785.dumps([sha256(path.read_bytes().replace(b"\r\n", b"\n")) for path in modules])
    )


def utc_now() -> datetime:
    return datetime.now(UTC)


def _closing_boundary(start: datetime) -> datetime:
    local = start.astimezone(NEW_YORK)
    early = time(8)
    evening = time(18)
    hour = local.time()
    if early <= hour < evening:
        raise ValueError("price-basis acquisition uses the research off-hours window")
    next_day = local.date() + timedelta(days=1) if hour >= evening else local.date()
    return datetime.combine(next_day, early, NEW_YORK).astimezone(UTC)


def _bar(raw: Any) -> dict[str, Any]:
    return {
        "date": raw.date.isoformat(),
        "open": str(raw.open),
        "high": str(raw.high),
        "low": str(raw.low),
        "close": str(raw.close),
        "volume": str(raw.volume),
        "count": raw.barCount,
        "wap": str(raw.average),
    }


def _trade(raw: Any) -> dict[str, Any]:
    at = raw.time
    if at.utcoffset() is None or at.microsecond:
        raise ValueError("trade time was not an aware whole second")
    return {
        "epoch_s": int(at.timestamp()),
        "price": str(raw.price),
        "size": str(raw.size),
        "exchange": raw.exchange,
        "conditions": raw.specialConditions,
        "past_limit": raw.tickAttribLast.pastLimit,
        "unreported": raw.tickAttribLast.unreported,
    }


async def capture_basis(
    request: BasisRequest,
    *,
    clock: Callable[[], datetime] = utc_now,
    factory: Callable[[], Any] | None = None,
    supersedes: str | None = None,
) -> bytes:
    """Capture a single completed session and preserve partial/error responses."""
    from ib_async import IB, Contract, RequestError

    def now() -> datetime:
        value = clock()
        if value.utcoffset() is None:
            raise ValueError("acquisition requires an aware clock")
        return value.astimezone(UTC)

    started = now()
    deadline = _closing_boundary(started)
    if started < request.daily_end:
        raise ValueError("daily session has not reached its historical request boundary")
    if supersedes is not None and not is_digest(supersedes):
        raise ValueError("invalid superseded price-basis digest")
    record: dict[str, Any] = {
        "schema": SCHEMA,
        "source_scope": SCOPE,
        "request": request.record(),
        "auction_rule_sha256": RULE_SHA256,
        "started_at": started.isoformat(),
        "ended_by": deadline.isoformat(),
        "supersedes": supersedes,
        "sdk_version": version("ib_async"),
        "recorder_sha256": recorder_sha256(),
        "connection": {"host": HOST, "port": PORT, "client_id": CLIENT_ID, "server_version": None},
        "contract": None,
        "daily": None,
        "closing_pages": [],
        "issue": None,
    }
    ib = factory() if factory is not None else IB()
    observed = started
    stage = "connection"

    async def receive(
        query: Callable[[], Awaitable[T]], limit: int
    ) -> tuple[T, datetime, datetime]:
        nonlocal observed
        sent = now()
        if sent < observed:
            raise RuntimeError("capture clock reversed")
        available = min(float(limit), (deadline - sent).total_seconds())
        if available <= 0:
            raise TimeoutError("research window closed")
        data = await asyncio.wait_for(query(), timeout=available)
        observed = now()
        if not sent <= observed <= deadline:
            raise RuntimeError("response exceeded the capture clock window")
        return data, sent, observed

    try:
        ib.wrapper.clientId = CLIENT_ID
        ib.RaiseRequestErrors = True
        await receive(lambda: ib.client.connectAsync(HOST, PORT, CLIENT_ID, 10), 10)
        if not ib.client.isReady():
            raise ConnectionError("Gateway did not finish the API handshake")
        record["connection"]["server_version"] = ib.client.serverVersion()

        stage = "contract"
        expected = Contract(conId=request.conid, exchange=ROUTING_EXCHANGE)
        details, _, acquired = await receive(lambda: ib.reqContractDetailsAsync(expected), 8)
        if len(details) != 1:
            raise LookupError("pinned contract unavailable or ambiguous")
        contract = details[0].contract
        record["contract"] = {
            "conid": contract.conId,
            "symbol": contract.symbol,
            "local_symbol": contract.localSymbol,
            "security_type": contract.secType,
            "currency": contract.currency,
            "primary_venue": contract.primaryExchange,
            "routing_exchange": contract.exchange,
            "historical_issuer_identity_verified": False,
            "observed_at": acquired.isoformat(),
        }
        if (
            contract.conId != request.conid
            or contract.symbol != SYMBOL_MAP.get(request.symbol, request.symbol)
            or contract.secType != "STK"
            or contract.currency != "USD"
            or contract.primaryExchange != request.primary_venue
            or contract.exchange != ROUTING_EXCHANGE
        ):
            raise LookupError("pinned contract does not match the requested listing")
        # The SDK normally returns an empty list on its own timeout. The outer
        # deadline owns cancellation here and the read-only connection closes.
        stage = "daily"
        daily, sent, acquired = await receive(
            lambda: ib.reqHistoricalDataAsync(
                contract, request.daily_end, "1 D", "1 day", "TRADES", True, 2, timeout=0
            ),
            20,
        )
        record["daily"] = {
            "query": daily_query(request),
            "sent_at": sent.isoformat(),
            "received_at": acquired.isoformat(),
            "rows": [_bar(item) for item in daily],
        }
        stage = "closing_tape"
        start = request.close
        for _ in range(PAGE_LIMIT):
            ticks, sent, acquired = await receive(
                partial(
                    ib.reqHistoricalTicksAsync,
                    contract,
                    start,
                    "",
                    TICK_COUNT,
                    "TRADES",
                    False,
                    False,
                ),
                12,
            )
            record["closing_pages"].append(
                {
                    "query": tick_query(start),
                    "sent_at": sent.isoformat(),
                    "received_at": acquired.isoformat(),
                    "rows": [_trade(item) for item in ticks],
                }
            )
            if not ticks or ticks[-1].time > request.through:
                break
            if ticks[-1].time < start:
                raise RuntimeError("closing tick pagination failed to advance")
            start = ticks[-1].time.astimezone(UTC) + ONE_SECOND
    except RequestError as exc:
        record["issue"] = {
            "stage": stage,
            "reason": broker_category(exc.code, exc.message),
            "detail": error_message(exc.message),
        }
    except (OSError, RuntimeError, LookupError, ValueError, AttributeError, TypeError) as exc:
        record["issue"] = {
            "stage": stage,
            "reason": f"{stage}_{type(exc).__name__}",
            "detail": error_message(str(exc)),
        }
    finally:
        try:
            ib.disconnect()
        except (OSError, RuntimeError) as exc:
            detail = error_message(str(exc))
            record["cleanup_issue"] = detail
            if record["issue"] is None:
                record["issue"] = {"stage": stage, "reason": "disconnect_error", "detail": detail}
    record["finished_at"] = now().isoformat()
    return rfc8785.dumps(record)


def publish_basis(directory: Path, receipt: bytes) -> Path:
    """Atomically publish complete bytes without replacing another receipt."""
    directory = safe_path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    name = sha256(receipt)
    destination = directory / f"{name}.json"
    handle, temporary = tempfile.mkstemp(dir=directory, prefix=f".{name}.", suffix=".capture")
    staging = Path(temporary)
    try:
        with os.fdopen(handle, "wb") as stream:
            stream.write(receipt)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(staging, destination)
        except FileExistsError:
            if safe_path(destination).read_bytes() != receipt:
                raise ValueError("price receipt conflicts with its content address") from None
        if os.name != "nt":
            descriptor = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    finally:
        staging.unlink(missing_ok=True)
    return destination


def _load_request(path: Path, expected: str) -> BasisRequest:
    raw = safe_path(path).read_bytes()
    if not is_digest(expected) or sha256(raw) != expected:
        raise ValueError("price-basis plan digest mismatch")
    record = json.loads(raw)
    request = BasisRequest(
        record["conid"],
        record["symbol"],
        record["primary_venue"],
        date.fromisoformat(record["session"]),
        datetime.fromisoformat(record["close"]),
        datetime.fromisoformat(record["through"]),
        record["calendar_sha256"],
        record["listing_sha256"],
    )
    if rfc8785.dumps(request.record()) != raw:
        raise ValueError("price-basis plan is not canonical")
    return request


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--control-plan", required=True, type=Path)
    parser.add_argument("--plan-sha256", required=True)
    parser.add_argument("--archive", required=True, type=Path)
    parser.add_argument("--supersedes")
    args = parser.parse_args()
    request = _load_request(args.control_plan, args.plan_sha256)
    grace = request.through - request.close
    if args.supersedes is not None:
        if not is_digest(args.supersedes):
            raise ValueError("invalid superseded price-basis digest")
        old = safe_path(args.archive / f"{args.supersedes}.json")
        audit_basis(request, old.read_bytes(), expected_sha256=args.supersedes, report_grace=grace)
    raw = asyncio.run(capture_basis(request, supersedes=args.supersedes))
    digest = sha256(raw)
    destination = publish_basis(args.archive, raw)
    audit = audit_basis(request, raw, expected_sha256=digest, report_grace=grace)
    print(json.dumps({"receipt": str(destination), "state": audit.state, "reason": audit.reason}))


if __name__ == "__main__":
    main()
