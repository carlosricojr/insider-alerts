from __future__ import annotations

import ast
import asyncio
import json
import sqlite3
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from typer.testing import CliRunner

from insider_alerts import cli
from insider_alerts.backtest.models import DailyBar
from insider_alerts.research.bar_feed import (
    BarFeedStore,
    BarRequest,
    SourceBarBatch,
    bar_feed_status,
    collect_once,
)
from insider_alerts.research.ibkr_bar_source import IbkrHistoricalBarSource

NEW_YORK = ZoneInfo("America/New_York")


def _bar(day: date, *, close: float = 10.0) -> DailyBar:
    return DailyBar("TEST", day, 10.0, 11.0, 9.0, close, 100_000.0)


class FakeSource:
    def __init__(
        self,
        bars: list[DailyBar],
        *,
        error: Exception | None = None,
        symbol_errors: set[str] | None = None,
        rejections: tuple[str, ...] = (),
    ) -> None:
        self.bars = bars
        self.error = error
        self.symbol_errors = symbol_errors or set()
        self.rejections = rejections
        self.connected = 0
        self.disconnected = 0
        self.symbols: list[str] = []

    async def connect(self) -> None:
        self.connected += 1
        if self.error is not None:
            raise self.error

    async def daily_bars(self, symbol: str, *, start_date: date) -> SourceBarBatch:
        self.symbols.append(symbol)
        if symbol in self.symbol_errors:
            raise LookupError(f"cannot qualify {symbol}")
        bars = tuple(
            DailyBar(
                symbol,
                bar.trade_date,
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.volume,
            )
            for bar in self.bars
            if bar.trade_date >= start_date
        )
        return SourceBarBatch(bars, self.rejections)

    def disconnect(self) -> None:
        self.disconnected += 1


def _request(now: datetime, *, through: date | None = None) -> BarRequest:
    return BarRequest(
        request_id="OPP-E07-V1:packet-1",
        symbol="test",
        start_date=now.date() - timedelta(days=40),
        through_date=through or now.date() + timedelta(days=20),
        requested_at_utc=now,
        requester="OPP-E07-V1",
    )


def test_feed_is_append_only_idempotent_and_preserves_revisions(tmp_path: Path) -> None:
    now = datetime(2026, 8, 27, 7, 0, tzinfo=UTC)
    store = BarFeedStore(tmp_path / "feed.db")
    digest = store.request(_request(now))
    assert store.request(_request(now)) == digest
    with pytest.raises(ValueError, match="different"):
        store.request(replace(_request(now), symbol="OTHER"))

    first = _bar(date(2026, 8, 25))
    current_partial = _bar(now.astimezone(NEW_YORK).date())
    assert store.append_completed([first, current_partial], observed_at_utc=now) == (1, 0, 0)
    assert store.append_completed([first], observed_at_utc=now + timedelta(minutes=1)) == (0, 0, 0)
    revision = _bar(first.trade_date, close=10.25)
    assert store.append_completed([revision], observed_at_utc=now + timedelta(minutes=2)) == (
        1,
        1,
        0,
    )
    invalid = _bar(first.trade_date - timedelta(days=1), close=20.0)
    assert store.append_completed([invalid], observed_at_utc=now + timedelta(minutes=3)) == (
        0,
        0,
        1,
    )
    assert store.first_observed_bars("TEST") == [first]
    assert store.status()["revision_count"] == 1
    assert store.status()["failure_count"] == 1

    with sqlite3.connect(store.path) as conn:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            conn.execute("UPDATE bar_observations SET symbol='X'")
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            conn.execute("DELETE FROM bar_feed_requests")


def test_worker_stays_offline_when_idle_and_collects_only_requested_range(tmp_path: Path) -> None:
    now = datetime(2026, 8, 27, 7, 0, tzinfo=UTC)
    store = BarFeedStore(tmp_path / "feed.db")
    idle_source = FakeSource([])
    idle = asyncio.run(collect_once(store, idle_source, now=now))
    assert (idle.requests, idle.symbols, idle.observations_added) == (0, 0, 0)
    assert idle_source.connected == idle_source.disconnected == 0

    request = _request(now)
    store.request(request)
    before = _bar(request.start_date - timedelta(days=1))
    wanted = _bar(request.start_date)
    current_partial = _bar(now.astimezone(NEW_YORK).date())
    source = FakeSource([before, wanted, current_partial])
    result = asyncio.run(collect_once(store, source, now=now))
    assert (result.requests, result.symbols, result.observations_added) == (1, 1, 1)
    assert source.symbols == ["TEST"]
    assert source.connected == source.disconnected == 1
    assert store.first_observed_bars("TEST") == [wanted]
    assert store.status()["health"]["last_result"] == "completed"
    second_source = FakeSource([wanted])
    second = asyncio.run(collect_once(store, second_source, now=now + timedelta(minutes=1)))
    assert second.requests == 0
    assert second_source.connected == 0


def test_worker_failure_is_durable_and_disconnects(tmp_path: Path) -> None:
    now = datetime(2026, 8, 27, 7, 0, tzinfo=UTC)
    store = BarFeedStore(tmp_path / "feed.db")
    store.request(_request(now))
    source = FakeSource([], error=ConnectionError("gateway unavailable"))
    with pytest.raises(ConnectionError, match="gateway unavailable"):
        asyncio.run(collect_once(store, source, now=now))
    assert source.connected == source.disconnected == 1
    health = store.status()["health"]
    assert health["last_result"] == "failed"
    assert "gateway unavailable" in health["last_error"]


def test_final_horizon_remains_collectible_on_following_day(tmp_path: Path) -> None:
    now = datetime(2026, 8, 27, 7, 0, tzinfo=UTC)
    store = BarFeedStore(tmp_path / "feed.db")
    store.request(_request(now, through=date(2026, 8, 26)))
    assert len(store.pending_requests(as_of=date(2026, 8, 27))) == 1
    assert len(store.pending_requests(as_of=date(2026, 9, 26))) == 1
    assert store.status(now=now)["overdue_request_count"] == 0
    store.append_completed([_bar(date(2026, 8, 26))], observed_at_utc=now)
    assert store.pending_requests(as_of=date(2026, 9, 26)) == []


def test_bad_symbol_does_not_block_later_symbols(tmp_path: Path) -> None:
    now = datetime(2026, 8, 27, 7, 0, tzinfo=UTC)
    store = BarFeedStore(tmp_path / "feed.db")
    first = replace(_request(now), request_id="a", symbol="AAA")
    second = replace(_request(now), request_id="b", symbol="BBB")
    store.request(first)
    store.request(second)
    source = FakeSource([_bar(first.start_date)], symbol_errors={"AAA"})
    result = asyncio.run(collect_once(store, source, now=now, minimum_interval_seconds=0))
    assert source.symbols == ["AAA", "BBB"]
    assert result.failed_symbols == 1
    assert result.observations_added == 1
    assert store.first_observed_bars("BBB")
    assert store.status()["health"]["last_result"] == "partial"


def test_source_timeout_is_durable_and_does_not_suppress_retry(tmp_path: Path) -> None:
    class TimeoutSource(FakeSource):
        async def daily_bars(self, symbol: str, *, start_date: date) -> SourceBarBatch:
            self.symbols.append(symbol)
            raise TimeoutError("simulated application timeout")

    now = datetime(2026, 8, 27, 7, 0, tzinfo=UTC)
    store = BarFeedStore(tmp_path / "feed.db")
    store.request(_request(now))
    source = TimeoutSource([])

    result = asyncio.run(collect_once(store, source, now=now))

    assert result.failed_symbols == 1
    assert source.connected == source.disconnected == 1
    assert len(store.pending_requests(as_of=now.astimezone(NEW_YORK).date())) == 1
    assert store.status(now=now)["health"]["last_result"] == "partial"


def test_ibkr_empty_response_fails_closed() -> None:
    class EmptyIb:
        async def reqHistoricalDataAsync(self, *_args: object, **_kwargs: object) -> list[object]:
            return []

    source = IbkrHistoricalBarSource(host="127.0.0.1", port=4001, client_id=176)
    source._IbkrHistoricalBarSource__ib = EmptyIb()  # type: ignore[attr-defined]
    source._IbkrHistoricalBarSource__contracts["TEST"] = object()  # type: ignore[attr-defined]

    with pytest.raises(RuntimeError, match="historical bars unavailable"):
        asyncio.run(source.daily_bars("TEST", start_date=date(2026, 8, 1)))


def test_ibkr_source_has_no_account_execution_or_order_api() -> None:
    import insider_alerts.research.ibkr_bar_source as source_module

    source = Path(source_module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    attributes = {node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)}
    forbidden = {
        "placeOrder",
        "cancelOrder",
        "reqOpenOrders",
        "reqAllOpenOrders",
        "reqExecutions",
        "reqPositions",
        "accountValues",
        "accountSummary",
    }
    assert attributes.isdisjoint(forbidden)
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")
        elif isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
    assert not any(module.startswith("insider_alerts.execution") for module in imported)
    string_literals = {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }
    assert "6 M" not in string_literals


def test_windows_task_uses_direct_hidden_pythonw() -> None:
    import insider_alerts.research.bar_feed as bar_feed_module
    import insider_alerts.research.ibkr_bar_source as source_module

    installer = (
        Path(__file__).parents[1] / "ops" / "windows" / "install-research-bar-feed-task.ps1"
    ).read_text(encoding="utf-8")
    assert ".venv\\Scripts\\pythonw.exe" in installer
    action = installer.split("$action =", maxsplit=1)[1].split("$logonTrigger", maxsplit=1)[0]
    assert "-Execute $pythonExe" in action
    assert "powershell" not in action.lower()
    assert "cmd.exe" not in action.lower()
    assert "-Hidden" in installer
    assert "New-TimeSpan -Minutes 60" in installer
    assert (
        source_module._QUALIFY_TIMEOUT_SECONDS  # type: ignore[attr-defined]
        + source_module._HISTORICAL_TIMEOUT_SECONDS  # type: ignore[attr-defined]
        < bar_feed_module._SOURCE_TIMEOUT_SECONDS  # type: ignore[attr-defined]
    )
    worst_case_seconds = (
        50 * bar_feed_module._SOURCE_TIMEOUT_SECONDS  # type: ignore[attr-defined]
        + 49 * 11
        + 10
    )
    assert worst_case_seconds < 60 * 60


def test_status_record_json_is_canonical_and_self_hashing(tmp_path: Path) -> None:
    now = datetime(2026, 8, 27, 7, 0, tzinfo=UTC)
    store = BarFeedStore(tmp_path / "feed.db")
    store.request(_request(now))
    store.append_completed([_bar(date(2026, 8, 25))], observed_at_utc=now)
    with sqlite3.connect(store.path) as conn:
        row = conn.execute("SELECT record_json,record_sha256 FROM bar_observations").fetchone()
    assert json.loads(bytes(row[0]))["contract_version"] == "ibkr-completed-rth-daily-v1"
    import hashlib

    assert hashlib.sha256(bytes(row[0])).hexdigest() == row[1]

    with sqlite3.connect(store.path) as conn:
        conn.execute("DROP TRIGGER bar_observations_no_update")
        conn.execute("UPDATE bar_observations SET record_json=?", (b"{}",))
    assert store.status()["integrity_status"] == "invalid"


def test_status_does_not_create_a_missing_database(tmp_path: Path) -> None:
    missing = tmp_path / "wrong.db"
    assert bar_feed_status(missing)["integrity_status"] == "missing"
    assert not missing.exists()
    corrupt = tmp_path / "corrupt.db"
    corrupt.write_bytes(b"not sqlite")
    assert bar_feed_status(corrupt)["integrity_status"] == "invalid"


def test_status_rejects_naive_time_and_cli_uses_integrity_exit_code(tmp_path: Path) -> None:
    store = BarFeedStore(tmp_path / "feed.db")
    with pytest.raises(ValueError, match="cannot be naive"):
        store.status(now=datetime(2026, 8, 27, 12, 0))

    runner = CliRunner()
    valid = runner.invoke(
        cli.app,
        ["ops", "research-bar-feed-status", "--feed-db", str(store.path)],
    )
    assert valid.exit_code == 0
    missing = runner.invoke(
        cli.app,
        ["ops", "research-bar-feed-status", "--feed-db", str(tmp_path / "missing.db")],
    )
    assert missing.exit_code == 3
