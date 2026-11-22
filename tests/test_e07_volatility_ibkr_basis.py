from __future__ import annotations

import asyncio
import json
import sys
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import rfc8785
from ib_async import RequestError

from e07_volatility_exits.ibkr_basis import BasisAudit, BasisRequest, audit_basis, sha256
from insider_alerts.strategy.e07 import NEW_YORK
from ops.research import e07_volatility_ibkr_basis as recorder
from ops.research.e07_volatility_ibkr_basis import CLIENT_ID, capture_basis, publish_basis

CLOSE = datetime(2026, 11, 27, 13, 0, tzinfo=NEW_YORK)
CLOCK = datetime(2026, 11, 28, 7, 0, tzinfo=UTC)
GRACE = timedelta(seconds=15)
PLAN = BasisRequest(42, "ABC", "NASDAQ", CLOSE.date(), CLOSE, CLOSE + GRACE, "a" * 64, "b" * 64)


def tick(offset: int, price: float, venue: str = "NASDAQ", conditions: str = " 6 X") -> Any:
    return SimpleNamespace(
        time=CLOSE.astimezone(UTC) + timedelta(seconds=offset),
        price=price,
        size=100,
        exchange=venue,
        specialConditions=conditions,
        tickAttribLast=SimpleNamespace(pastLimit=False, unreported=False),
    )


class Gateway:
    def __init__(self, close: float, pages: list[list[Any]]):
        self.client = self
        self.wrapper = SimpleNamespace(clientId=None)
        self.close = close
        self.pages = iter(pages)
        self.events: list[Any] = []
        self.daily_problem: Exception | None = None
        self.tape_problem: Exception | None = None
        self.conid = 42
        self.venue = "NASDAQ"
        self.routing = "SMART"
        self.cleanup_problem: Exception | None = None

    async def connectAsync(self, *args: Any) -> None:
        self.events.append(("connect", *args))

    def isReady(self) -> bool:
        return True

    def serverVersion(self) -> int:
        return 187

    async def reqContractDetailsAsync(self, request: Any) -> list[Any]:
        self.events.append(("details", request.conId, request.exchange))
        return [
            SimpleNamespace(
                contract=SimpleNamespace(
                    conId=self.conid,
                    symbol="ABC",
                    localSymbol="ABC",
                    secType="STK",
                    currency="USD",
                    primaryExchange=self.venue,
                    exchange=self.routing,
                )
            )
        ]

    async def reqHistoricalDataAsync(self, contract: Any, *args: Any, **kwargs: Any) -> list[Any]:
        self.events.append(("bars", contract.conId, *args, kwargs))
        if self.daily_problem is not None:
            raise self.daily_problem
        return [
            SimpleNamespace(
                date=PLAN.session,
                open=self.close,
                high=self.close + 1,
                low=self.close - 1,
                close=self.close,
                volume=123_456,
                barCount=123,
                average=self.close,
            )
        ]

    async def reqHistoricalTicksAsync(self, contract: Any, *args: Any) -> list[Any]:
        self.events.append(("ticks", contract.conId, *args))
        if self.tape_problem is not None:
            raise self.tape_problem
        return next(self.pages)

    def disconnect(self) -> None:
        self.events.append(("disconnect",))
        if self.cleanup_problem is not None:
            raise self.cleanup_problem


def run_capture(gateway: Gateway, **kwargs: Any) -> bytes:
    return asyncio.run(capture_basis(PLAN, clock=lambda: CLOCK, factory=lambda: gateway, **kwargs))


def evaluate(raw: bytes) -> BasisAudit:
    return audit_basis(PLAN, raw, expected_sha256=sha256(raw), report_grace=GRACE)


def test_early_close_late_report_and_continuation_binding() -> None:
    gateway = Gateway(94.39, [[tick(15, 94.39)], [tick(16, 90, "NYSE", "6")]])
    raw = run_capture(gateway)
    audit = evaluate(raw)
    assert audit.state == "close_prices_agree"
    assert audit.daily_close == audit.auction_close == Decimal("94.39")
    assert gateway.RaiseRequestErrors and gateway.wrapper.clientId == CLIENT_ID
    assert gateway.events == [
        ("connect", "127.0.0.1", 4001, CLIENT_ID, 10),
        ("details", 42, "SMART"),
        ("bars", 42, PLAN.daily_end, "1 D", "1 day", "TRADES", True, 2, {"timeout": 0}),
        ("ticks", 42, PLAN.close, "", 1000, "TRADES", False, False),
        ("ticks", 42, PLAN.close + timedelta(seconds=16), "", 1000, "TRADES", False, False),
        ("disconnect",),
    ]
    tighter = audit_basis(
        PLAN, raw, expected_sha256=sha256(raw), report_grace=timedelta(seconds=14)
    )
    assert tighter.reason == "primary_close_missing_or_ambiguous"


def test_pre_split_bars_conflict_with_raw_tape_and_stay_unmodified(tmp_path: Path) -> None:
    raw = run_capture(Gateway(94.39, [[tick(1, 755.16), tick(16, 755.5, conditions="")]]))
    path = publish_basis(tmp_path, raw)
    assert publish_basis(tmp_path, raw) == path
    result = evaluate(path.read_bytes())
    assert result.state == "close_prices_conflict"
    assert (result.daily_close, result.auction_close) == (Decimal("94.39"), Decimal("755.16"))
    assert path.stem == result.source_sha256


@pytest.mark.parametrize(
    ("pages", "reason"),
    [
        ([[]], "closing_tape_empty"),
        ([[tick(2, 94.39)], []], "closing_window_unbracketed"),
        ([[tick(2, 94.39)], [tick(15, 94.39, conditions="")]], "closing_window_unbracketed"),
        (
            [[tick(1, 94.39)], [tick(1, 94.39), tick(16, 94.39)]],
            "unordered_or_overlapping_trade_pages",
        ),
        (
            [[tick(2, 94.39), tick(1, 94.39), tick(16, 94.39)]],
            "unordered_or_overlapping_trade_pages",
        ),
        ([[tick(2, 94.39, "NYSE"), tick(16, 94.39)]], "primary_close_missing_or_ambiguous"),
        ([[tick(2, 94.39, conditions="M"), tick(16, 94.39)]], "primary_close_missing_or_ambiguous"),
        ([[tick(1, 94.39), tick(1, 94.39), tick(16, 94.39)]], "primary_close_missing_or_ambiguous"),
        (
            [[tick(2, 94.39, conditions="6 Z"), tick(16, 94.39)]],
            "unsupported_primary_closing_trade",
        ),
        ([[tick(2, 0), tick(16, 94.39)]], "nonpositive_or_nonfinite_price_or_size"),
    ],
)
def test_distinguishes_unavailable_sources(pages: list[list[Any]], reason: str) -> None:
    assert evaluate(run_capture(Gateway(94.39, pages))).reason == reason


@pytest.mark.parametrize("attribute", ["unreported", "pastLimit"])
def test_reported_primary_print_is_required(attribute: str) -> None:
    auction = tick(2, 94.39)
    setattr(auction.tickAttribLast, attribute, True)
    raw = run_capture(Gateway(94.39, [[auction, tick(16, 94.39)]]))
    assert evaluate(raw).reason == "unsupported_primary_closing_trade"


@pytest.mark.parametrize(
    ("daily_problem", "tape_problem", "reason", "daily_present"),
    [
        (TimeoutError("simulated timeout"), None, "daily_TimeoutError", False),
        (None, RequestError(7, 162, "HMDS query returned no data"), "hmds_162_no_data", True),
    ],
)
def test_partial_error_is_archived_not_reclassified_as_empty(
    daily_problem: Exception | None,
    tape_problem: Exception | None,
    reason: str,
    daily_present: bool,
) -> None:
    gateway = Gateway(94.39, [])
    gateway.daily_problem, gateway.tape_problem = daily_problem, tape_problem
    receipt = run_capture(gateway)
    assert evaluate(receipt).reason == reason
    parsed = json.loads(receipt)
    assert (parsed["daily"] is not None) is daily_present
    assert parsed["closing_pages"] == []
    assert gateway.events[-1] == ("disconnect",)


@pytest.mark.parametrize(
    ("field", "value"), [("conid", 99), ("venue", "NYSE"), ("routing", "NYSE")]
)
def test_wrong_contract_stops_before_price_requests(field: str, value: object) -> None:
    gateway = Gateway(94.39, [])
    setattr(gateway, field, value)
    assert evaluate(run_capture(gateway)).reason == "contract_LookupError"
    assert [event[0] for event in gateway.events] == ["connect", "details", "disconnect"]


@pytest.mark.parametrize("venue", ["NYSE", "AMEX"])
def test_non_nasdaq_primary_close_matches_its_own_venue(venue: str) -> None:
    gateway = Gateway(94.39, [[tick(1, 94.39, venue, " 6 "), tick(16, 94.39)]])
    gateway.venue = venue
    plan = replace(PLAN, primary_venue=venue)
    raw = asyncio.run(capture_basis(plan, clock=lambda: CLOCK, factory=lambda: gateway))
    audit = audit_basis(plan, raw, expected_sha256=sha256(raw), report_grace=GRACE)
    assert audit.state == "close_prices_agree"


def test_wrong_hash_and_listing_are_rejected() -> None:
    raw = run_capture(Gateway(94.39, [[tick(1, 94.39), tick(16, 94.39)]]))
    with pytest.raises(ValueError, match="digest mismatch"):
        audit_basis(PLAN, raw, expected_sha256="0" * 64, report_grace=GRACE)
    with pytest.raises(ValueError, match="subject"):
        audit_basis(replace(PLAN, conid=88), raw, expected_sha256=sha256(raw), report_grace=GRACE)
    with pytest.raises(ValueError, match="exceeds"):
        audit_basis(
            PLAN, raw, expected_sha256=sha256(raw), report_grace=GRACE + timedelta(seconds=1)
        )


@pytest.mark.parametrize(
    "mutate",
    [
        lambda row: row["daily"]["query"].update(rth=False),
        lambda row: row["closing_pages"][0]["query"].update(rth=True),
        lambda row: row["closing_pages"][0]["query"].update(exchange="NYSE"),
        lambda row: row["closing_pages"][0].update(sent_at="2026-11-28T06:59:00+00:00"),
    ],
)
def test_inconsistent_request_or_response_clock_is_rejected(
    mutate: Callable[[dict[str, Any]], None],
) -> None:
    record = json.loads(run_capture(Gateway(94.39, [[tick(1, 94.39), tick(16, 94.39)]])))
    mutate(record)
    with pytest.raises(ValueError, match="request or clocks"):
        evaluate(rfc8785.dumps(record))


def test_revision_links_original_and_corrupt_address_is_never_replaced(tmp_path: Path) -> None:
    old = run_capture(Gateway(94.39, [[]]))
    original = publish_basis(tmp_path, old)
    new = run_capture(Gateway(94.39, [[tick(1, 94.39), tick(16, 94.39)]]), supersedes=original.stem)
    revision = publish_basis(tmp_path, new)
    assert original != revision
    assert original.read_bytes() == old
    assert json.loads(revision.read_bytes())["supersedes"] == original.stem

    original.write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="conflicts"):
        publish_basis(tmp_path, old)
    assert original.read_bytes() == b"corrupt"


def test_missing_daily_bar_and_invalid_daily_range_are_unresolved() -> None:
    capture = run_capture(Gateway(94.39, [[tick(1, 94.39), tick(16, 94.39)]]))
    record = json.loads(capture)
    record["daily"]["rows"][0]["low"] = "96"
    assert evaluate(rfc8785.dumps(record)).reason == "invalid_daily_ohlc"
    record["daily"]["rows"] = []
    assert evaluate(rfc8785.dumps(record)).reason == "daily_session_missing_or_ambiguous"


def test_incorrect_source_or_revision_binding_is_rejected() -> None:
    raw = run_capture(Gateway(94.39, [[tick(1, 94.39), tick(16, 94.39)]]))
    record = json.loads(raw)
    assert record["recorder_sha256"] == recorder.recorder_sha256()
    record["supersedes"] = "malformed"
    with pytest.raises(ValueError, match="invalid prior"):
        evaluate(rfc8785.dumps(record))
    record["supersedes"] = None
    record["connection"]["client_id"] = 0
    with pytest.raises(ValueError, match="source identity"):
        evaluate(rfc8785.dumps(record))


def test_extra_tape_page_after_proven_coverage_is_rejected() -> None:
    raw = run_capture(Gateway(94.39, [[tick(1, 94.39), tick(16, 94.39)]]))
    record = json.loads(raw)
    record["closing_pages"].append(record["closing_pages"][0])
    with pytest.raises(ValueError, match="unexpected page"):
        evaluate(rfc8785.dumps(record))


def test_cleanup_failure_retains_the_transferred_rows() -> None:
    gateway = Gateway(94.39, [[tick(1, 94.39), tick(16, 94.39)]])
    gateway.cleanup_problem = OSError("simulated disconnect issue")
    raw = run_capture(gateway)
    record = json.loads(raw)
    assert evaluate(raw).reason == "disconnect_error"
    assert record["closing_pages"] and record["daily"]


def test_cli_validates_the_plan_and_links_an_existing_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    plan_bytes = rfc8785.dumps(PLAN.record())
    plan = publish_basis(tmp_path / "plans", plan_bytes)
    archive = tmp_path / "receipts"
    old = publish_basis(archive, run_capture(Gateway(94.39, [[]])))
    gateway = Gateway(94.39, [[tick(1, 94.39), tick(16, 94.39)]])

    async def simulated_capture(plan: BasisRequest, *, supersedes: str) -> bytes:
        assert plan == PLAN and supersedes == old.stem
        return await capture_basis(
            plan, clock=lambda: CLOCK, factory=lambda: gateway, supersedes=supersedes
        )

    args = [
        "recorder",
        "--control-plan",
        str(plan),
        "--plan-sha256",
        "0" * 64,
        "--archive",
        str(archive),
        "--supersedes",
        old.stem,
    ]
    monkeypatch.setattr(recorder, "capture_basis", simulated_capture)
    monkeypatch.setattr(sys, "argv", args)
    with pytest.raises(ValueError, match="plan digest"):
        recorder.main()
    assert not gateway.events

    args[4] = plan.stem
    recorder.main()
    summary = json.loads(capsys.readouterr().out)
    assert summary["state"] == "close_prices_agree"
    assert json.loads(Path(summary["receipt"]).read_bytes())["supersedes"] == old.stem


def test_off_hours_and_completed_historical_session_hold_before_connecting() -> None:
    gateway = Gateway(94.39, [])
    with pytest.raises(ValueError, match="off-hours"):
        asyncio.run(
            capture_basis(PLAN, clock=lambda: CLOCK + timedelta(hours=8), factory=lambda: gateway)
        )
    with pytest.raises(ValueError, match="not reached"):
        asyncio.run(
            capture_basis(PLAN, clock=lambda: CLOCK - timedelta(hours=8), factory=lambda: gateway)
        )
    assert not gateway.events
