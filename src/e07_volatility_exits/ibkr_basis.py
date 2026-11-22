"""Read an IBKR price-basis receipt without admitting prices to the replay."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

import rfc8785

from insider_alerts.research.commissioning_source import SYMBOL_MAP
from insider_alerts.strategy.e07 import NEW_YORK

SCHEMA = "e07-ibkr-price-basis-sdk-v2"
SCOPE = "research_price_basis_control"
HOST = "127.0.0.1"
PORT = 4001
CLIENT_ID = 22954
ROUTING_EXCHANGE = "SMART"
TICK_COUNT = 1_000
PAGE_LIMIT = 2
ONE_SECOND = timedelta(seconds=1)

# A primary 6 print may carry the Nasdaq cross modifier.
# An unfamiliar modifier on a primary 6 print leaves the session unresolved.
VENUE_CONDITIONS = (
    ("AMEX", ("6",)),
    ("NASDAQ", ("6", "6 X")),
    ("NYSE", ("6",)),
)


def sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def is_digest(value: str) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[a-f0-9]{64}", value) is not None


RULE_SHA256 = sha256(
    rfc8785.dumps(
        [
            "e07-ibkr-close-condition-v1",
            VENUE_CONDITIONS,
            "positive reported trade with neither pastLimit nor unreported set",
        ]
    )
)


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("expected an aware source clock")
    return value.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class BasisRequest:
    """One pinned U.S. listing-session and its frozen price coverage window."""

    conid: int
    symbol: str
    primary_venue: str
    session: date
    close: datetime
    through: datetime
    calendar_sha256: str
    listing_sha256: str

    def __post_init__(self) -> None:
        close, through = _utc(self.close), _utc(self.through)
        if not (
            type(self.conid) is int
            and 0 < self.conid <= 2**31 - 1
            and re.fullmatch(r"[A-Z0-9][A-Z0-9.-]{0,31}", self.symbol)
            and self.primary_venue in dict(VENUE_CONDITIONS)
            and type(self.session) is date
            and not (close.microsecond or through.microsecond or close.second)
            and close <= through
            and close.astimezone(NEW_YORK).date()
            == through.astimezone(NEW_YORK).date()
            == self.session
            and is_digest(self.calendar_sha256)
            and is_digest(self.listing_sha256)
        ):
            raise ValueError("invalid price-basis request")
        object.__setattr__(self, "close", close)
        object.__setattr__(self, "through", through)

    @property
    def daily_end(self) -> datetime:
        tomorrow = self.session + timedelta(days=1)
        return datetime(tomorrow.year, tomorrow.month, tomorrow.day, tzinfo=NEW_YORK).astimezone(
            UTC
        )

    def record(self) -> dict[str, Any]:
        return {
            "conid": self.conid,
            "symbol": self.symbol,
            "primary_venue": self.primary_venue,
            "session": self.session.isoformat(),
            "close": self.close.isoformat(),
            "through": self.through.isoformat(),
            "calendar_sha256": self.calendar_sha256,
            "listing_sha256": self.listing_sha256,
        }


@dataclass(frozen=True, slots=True)
class BasisAudit:
    source_sha256: str
    state: Literal["close_prices_agree", "close_prices_conflict", "unresolved"]
    reason: str | None
    daily_close: Decimal | None = None
    auction_close: Decimal | None = None


class _Unresolved(ValueError):
    """Valid custody receipt without a usable basis cross-check."""


def _observation_clock(value: str) -> datetime:
    result = _utc(datetime.fromisoformat(value))
    if result.isoformat() != value:
        raise ValueError("noncanonical UTC observation clock")
    return result


def _positive(value: str) -> Decimal:
    if not isinstance(value, str):
        raise ValueError("expected the unmodified SDK price or size string")
    price = Decimal(value)
    if not price.is_finite() or price <= 0:
        raise _Unresolved("nonpositive_or_nonfinite_price_or_size")
    return price


def _rows(
    response: dict[str, Any], query: dict[str, Any], after: datetime, before: datetime
) -> tuple[list[dict[str, Any]], datetime]:
    observed = _observation_clock(response["received_at"])
    if (
        rfc8785.dumps(response["query"]) != rfc8785.dumps(query)
        or not after <= _observation_clock(response["sent_at"]) <= observed <= before
        or not isinstance(response["rows"], list)
    ):
        raise ValueError("receipt response differs from its request or clocks")
    return response["rows"], observed


def daily_query(request: BasisRequest) -> dict[str, Any]:
    return {
        "exchange": ROUTING_EXCHANGE,
        "end_utc": request.daily_end.isoformat(),
        "duration": "1 D",
        "bar_size": "1 day",
        "series": "TRADES",
        "rth": True,
        "format_date": 2,
    }


def tick_query(start: datetime) -> dict[str, Any]:
    return {
        "exchange": ROUTING_EXCHANGE,
        "from_epoch_s": int(start.timestamp()),
        "number": TICK_COUNT,
        "series": "TRADES",
        "rth": False,
        "ignore_size": False,
    }


def _daily_close(rows: list[dict[str, Any]], session: date) -> Decimal:
    if len(rows) != 1 or rows[0]["date"] != session.isoformat():
        raise _Unresolved("daily_session_missing_or_ambiguous")
    row = rows[0]
    opening, high, low, close, _ = (
        _positive(row[key]) for key in ("open", "high", "low", "close", "volume")
    )
    if not low <= min(opening, close) <= max(opening, close) <= high:
        raise _Unresolved("invalid_daily_ohlc")
    return close


def _auction_close(
    request: BasisRequest,
    pages: list[dict[str, Any]],
    cutoff: datetime,
    previous: datetime,
    finished: datetime,
) -> Decimal:
    if not isinstance(pages, list) or len(pages) > PAGE_LIMIT:
        raise ValueError("unexpected closing page count")
    start = request.close
    found: list[Decimal] = []
    allowed = dict(VENUE_CONDITIONS)[request.primary_venue]
    for index, page in enumerate(pages):
        rows, previous = _rows(page, tick_query(start), previous, finished)
        last: datetime | None = None
        for row in rows:
            if type(row["epoch_s"]) is not int:
                raise ValueError("trade timestamp must be epoch seconds")
            at = datetime.fromtimestamp(row["epoch_s"], UTC)
            if not start <= at <= previous or (last is not None and at < last):
                raise _Unresolved("unordered_or_overlapping_trade_pages")
            last = at
            conditions = " ".join(row["conditions"].split())
            if (
                at <= cutoff
                and row["exchange"] == request.primary_venue
                and "6" in conditions.split()
            ):
                if not (
                    conditions in allowed
                    and row["unreported"] is False
                    and row["past_limit"] is False
                ):
                    raise _Unresolved("unsupported_primary_closing_trade")
                found.append(_positive(row["price"]))
                _positive(row["size"])
        if last is None:
            reason = "closing_tape_empty" if index == 0 else "closing_window_unbracketed"
            raise _Unresolved(reason)
        if last > request.through:
            if index != len(pages) - 1:
                raise ValueError("unexpected page after closing coverage")
            if len(found) != 1:
                raise _Unresolved("primary_close_missing_or_ambiguous")
            return found[0]
        start = last + ONE_SECOND
    raise _Unresolved("closing_window_unbracketed")


def audit_basis(
    request: BasisRequest,
    raw: bytes,
    *,
    expected_sha256: str,
    report_grace: timedelta,
) -> BasisAudit:
    """Verify source identity and compare prices without adjusting either series."""
    if not is_digest(expected_sha256) or sha256(raw) != expected_sha256:
        raise ValueError("price-basis receipt digest mismatch")
    cutoff = request.close + report_grace
    if cutoff.microsecond or not request.close <= cutoff <= request.through:
        raise ValueError("closing convention exceeds the recorded window")
    try:
        record = json.loads(raw)
        if (
            rfc8785.dumps(record) != raw
            or record["schema"] != SCHEMA
            or record["auction_rule_sha256"] != RULE_SHA256
            or record["source_scope"] != SCOPE
            or rfc8785.dumps(record["request"]) != rfc8785.dumps(request.record())
        ):
            raise ValueError("price-basis receipt subject or schema mismatch")
        started = _observation_clock(record["started_at"])
        finished = _observation_clock(record["finished_at"])
        deadline = _observation_clock(record["ended_by"])
        if not (request.daily_end <= started <= finished and started < deadline):
            raise ValueError("price-basis capture predates the finished session")
        previous_receipt = record["supersedes"]
        if previous_receipt is not None and (
            not is_digest(previous_receipt) or previous_receipt == expected_sha256
        ):
            raise ValueError("invalid prior price-basis receipt")
        connection = record["connection"]
        if (
            not is_digest(record["recorder_sha256"])
            or connection["host"] != HOST
            or connection["port"] != PORT
            or connection["client_id"] != CLIENT_ID
        ):
            raise ValueError("price-basis source identity is missing or mismatched")
        if record["issue"] is not None:
            issue = record["issue"]
            if not isinstance(issue["reason"], str) or not issue["reason"]:
                raise ValueError("invalid price-basis failure receipt")
            return BasisAudit(expected_sha256, "unresolved", issue["reason"])
        identity = record["contract"]
        sdk = record["sdk_version"]
        server = connection["server_version"]
        if not isinstance(sdk, str) or not sdk or type(server) is not int or server <= 0:
            raise ValueError("receipt SDK or server identity is missing")

        if (
            type(identity["conid"]) is not int
            or identity["conid"] != request.conid
            or identity["symbol"] != SYMBOL_MAP.get(request.symbol, request.symbol)
            or identity["security_type"] != "STK"
            or identity["currency"] != "USD"
            or identity["primary_venue"] != request.primary_venue
            or identity["routing_exchange"] != ROUTING_EXCHANGE
            or identity["historical_issuer_identity_verified"] is not False
        ):
            raise _Unresolved("pinned_contract_mismatch")
        qualified = _observation_clock(identity["observed_at"])
        if not started <= qualified <= finished:
            raise ValueError("receipt contract observation clock is out of sequence")
        through = min(finished, deadline)
        daily, then = _rows(record["daily"], daily_query(request), qualified, through)
        bar_close = _daily_close(daily, request.session)
        auction = _auction_close(request, record["closing_pages"], cutoff, then, through)
    except _Unresolved as issue:
        return BasisAudit(expected_sha256, "unresolved", str(issue))
    except (
        KeyError,
        IndexError,
        TypeError,
        AttributeError,
        OverflowError,
        OSError,
        InvalidOperation,
    ) as exc:
        raise ValueError("malformed price-basis receipt") from exc
    if bar_close != auction:
        return BasisAudit(
            expected_sha256, "close_prices_conflict", "different_close_prices", bar_close, auction
        )
    return BasisAudit(expected_sha256, "close_prices_agree", None, bar_close, auction)
