from __future__ import annotations

import argparse
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

import insider_alerts.research.bar_worker as bar_worker
import insider_alerts.research.session_feed as session_feed_module
from insider_alerts import cli
from insider_alerts.backtest.models import DailyBar
from insider_alerts.research.bar_feed import (
    BarFeedStore,
    BarRequest,
    HistoricalBarSessionReset,
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


def test_current_date_requires_explicit_official_close_proof(tmp_path: Path) -> None:
    now = datetime(2026, 8, 27, 20, 1, tzinfo=UTC)
    today = now.astimezone(NEW_YORK).date()
    store = BarFeedStore(tmp_path / "feed.db")
    current = _bar(today)

    assert store.append_completed([current], observed_at_utc=now) == (0, 0, 0)
    assert store.append_completed(
        [current],
        observed_at_utc=now,
        completed_through_date=today,
    ) == (1, 0, 0)
    with pytest.raises(ValueError, match="future"):
        store.append_completed(
            [current],
            observed_at_utc=now,
            completed_through_date=today + timedelta(days=1),
        )


def test_preclose_poll_is_due_again_once_current_session_is_proven_complete(
    tmp_path: Path,
) -> None:
    preclose = datetime(2026, 8, 27, 19, 0, tzinfo=UTC)
    postclose = datetime(2026, 8, 27, 20, 1, tzinfo=UTC)
    today = preclose.astimezone(NEW_YORK).date()
    store = BarFeedStore(tmp_path / "feed.db")
    store.request(_request(preclose, through=today + timedelta(days=10)))
    current = _bar(today)

    first_source = FakeSource([current])
    second_source = FakeSource([current])
    third_source = FakeSource([current])
    first = asyncio.run(
        collect_once(
            store,
            first_source,
            now=preclose,
            minimum_interval_seconds=0,
        )
    )
    second = asyncio.run(
        collect_once(
            store,
            second_source,
            now=postclose,
            minimum_interval_seconds=0,
            completed_through_date=today,
        )
    )
    third = asyncio.run(
        collect_once(
            store,
            third_source,
            now=postclose + timedelta(minutes=1),
            minimum_interval_seconds=0,
            completed_through_date=today,
        )
    )

    assert first.observations_added == 0
    assert second.observations_added == 1
    assert second_source.symbols == ["TEST"]
    assert third.symbols == 0
    assert third_source.symbols == []
    assert store.first_observed_bars("TEST", start_date=today) == [current]


def test_worker_keeps_historical_feed_available_when_session_store_is_missing(
    tmp_path: Path,
) -> None:
    missing = tmp_path / "missing-sessions.db"
    args = argparse.Namespace(
        feed_db=tmp_path / "feed.db",
        session_feed_db=missing,
        host="127.0.0.1",
        port=4001,
        client_id=176,
        error_log=tmp_path / "error.log",
    )

    assert asyncio.run(bar_worker._run(args))["requests"] == 0
    assert not missing.exists()


def test_worker_degrades_corrupt_session_proof_without_stopping_historical_feed(
    tmp_path: Path,
) -> None:
    corrupt = tmp_path / "corrupt-sessions.db"
    corrupt.write_bytes(b"not sqlite")
    args = argparse.Namespace(
        feed_db=tmp_path / "feed.db",
        session_feed_db=corrupt,
        host="127.0.0.1",
        port=4001,
        client_id=176,
        error_log=tmp_path / "error.log",
    )

    assert asyncio.run(bar_worker._run(args))["requests"] == 0
    status = BarFeedStore(args.feed_db).status()
    assert status["failure_count"] == 1
    assert "file is not a database" in args.error_log.read_text(encoding="utf-8")


@pytest.mark.parametrize("error_type", [TypeError, IndexError, KeyError, ValueError])
def test_structurally_corrupt_session_proof_uses_non_tradable_failure_sentinel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    error_type: type[Exception],
) -> None:
    corrupt = tmp_path / "corrupt-sessions.db"
    corrupt.touch()

    def fail_validation(_store: object) -> None:
        raise error_type("corrupt cell structure")

    monkeypatch.setattr(
        session_feed_module.SessionFeedStore,
        "validate_integrity",
        fail_validation,
    )
    args = argparse.Namespace(
        feed_db=tmp_path / "feed.db",
        session_feed_db=corrupt,
        host="127.0.0.1",
        port=4001,
        client_id=176,
        error_log=tmp_path / "error.log",
    )

    assert asyncio.run(bar_worker._run(args))["requests"] == 0
    with sqlite3.connect(args.feed_db) as conn:
        assert conn.execute("SELECT symbol FROM bar_feed_failures").fetchone()[0] == "SESSION-FEED"


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
    receipts = store.poll_receipts("TEST")
    assert len(receipts) == 1
    assert receipts[0].returned_bar_count == 2
    assert receipts[0].in_range_bar_count == 2
    assert receipts[0].source_rejection_count == 0
    assert receipts[0].validation_rejection_count == 0
    assert store.poll_receipt_watermark() == 1
    assert store.poll_receipts("TEST", max_sequence=0) == []
    with pytest.raises(ValueError, match="receipt watermark"):
        store.poll_receipts(max_sequence=True)  # type: ignore[arg-type]
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
    assert store.poll_receipts() == []


def test_successful_poll_receipt_is_append_only_and_binds_rejections(tmp_path: Path) -> None:
    now = datetime(2026, 8, 27, 7, 0, tzinfo=UTC)
    store = BarFeedStore(tmp_path / "feed.db")
    request = _request(now)
    store.request(request)
    valid = _bar(request.start_date)
    invalid = _bar(request.start_date + timedelta(days=1), close=20.0)
    source = FakeSource([valid, invalid], rejections=("source rejected one row",))

    result = asyncio.run(collect_once(store, source, now=now, minimum_interval_seconds=0))

    assert result.rejected_bars == 2
    receipt = store.poll_receipts("test")[0]
    assert receipt.symbol == "TEST"
    assert receipt.polled_at_utc == now
    assert receipt.requested_start_date == request.start_date
    assert receipt.requested_through_date == request.through_date
    assert receipt.completed_through_date is None
    assert receipt.returned_bar_count == 2
    assert receipt.in_range_bar_count == 2
    assert receipt.source_rejection_count == 1
    assert receipt.validation_rejection_count == 1
    assert receipt.observation_watermark == 1
    assert len(receipt.record_sha256) == 64
    assert store.status()["poll_receipt_count"] == 1
    store.validate_integrity()
    with sqlite3.connect(store.path) as conn:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            conn.execute("UPDATE bar_poll_receipts SET symbol='OTHER'")
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            conn.execute("DELETE FROM bar_poll_receipts")


def test_bar_observation_watermark_freezes_first_observed_snapshot(tmp_path: Path) -> None:
    observed = datetime(2026, 8, 27, 12, 0, tzinfo=UTC)
    store = BarFeedStore(tmp_path / "feed.db")
    first_day = date(2026, 8, 25)
    second_day = date(2026, 8, 26)
    first = _bar(first_day)
    assert store.append_completed([first], observed_at_utc=observed) == (1, 0, 0)
    watermark = store.observation_watermark()
    assert watermark == 1
    assert store.append_completed(
        [_bar(first_day, close=10.25), _bar(second_day)],
        observed_at_utc=observed + timedelta(minutes=1),
    ) == (2, 1, 0)

    frozen = store.first_observed_bar_records("TEST", max_sequence=watermark)
    assert [(item.sequence, item.bar) for item in frozen] == [(1, first)]
    assert frozen[0].observed_at_utc == observed
    assert len(frozen[0].record_sha256) == 64
    assert store.first_observed_bars("TEST") == [first, _bar(second_day)]
    with pytest.raises(ValueError, match="watermark"):
        store.first_observed_bar_records("TEST", max_sequence=True)  # type: ignore[arg-type]


def test_integrity_detects_poll_receipt_byte_tampering(tmp_path: Path) -> None:
    now = datetime(2026, 8, 27, 7, 0, tzinfo=UTC)
    store = BarFeedStore(tmp_path / "feed.db")
    store.request(_request(now))
    asyncio.run(collect_once(store, FakeSource([_bar(now.date() - timedelta(days=1))]), now=now))
    with sqlite3.connect(store.path) as conn:
        conn.execute("DROP TRIGGER bar_poll_receipts_no_update")
        conn.execute("UPDATE bar_poll_receipts SET record_json=?", (b"{}",))

    assert store.status()["integrity_status"] == "invalid"


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


def test_session_reset_failure_reconnects_before_later_symbol(tmp_path: Path) -> None:
    class ResetSource(FakeSource):
        async def daily_bars(self, symbol: str, *, start_date: date) -> SourceBarBatch:
            if symbol == "AAA":
                self.symbols.append(symbol)
                raise HistoricalBarSessionReset("qualification timed out")
            return await super().daily_bars(symbol, start_date=start_date)

    now = datetime(2026, 8, 27, 7, 0, tzinfo=UTC)
    store = BarFeedStore(tmp_path / "feed.db")
    store.request(replace(_request(now), request_id="a", symbol="AAA"))
    store.request(replace(_request(now), request_id="b", symbol="BBB"))
    source = ResetSource([_bar(now.date() - timedelta(days=1))])

    result = asyncio.run(collect_once(store, source, now=now, minimum_interval_seconds=0))

    assert result.failed_symbols == 1
    assert result.observations_added == 1
    assert source.symbols == ["AAA", "BBB"]
    assert source.connected == source.disconnected == 2
    assert store.first_observed_bars("BBB")


def test_source_timeout_is_durable_and_does_not_suppress_retry(tmp_path: Path) -> None:
    class TimeoutSource(FakeSource):
        async def daily_bars(self, symbol: str, *, start_date: date) -> SourceBarBatch:
            if symbol == "AAA":
                self.symbols.append(symbol)
                raise TimeoutError("simulated application timeout")
            return await super().daily_bars(symbol, start_date=start_date)

    now = datetime(2026, 8, 27, 7, 0, tzinfo=UTC)
    store = BarFeedStore(tmp_path / "feed.db")
    store.request(replace(_request(now), request_id="a", symbol="AAA"))
    store.request(replace(_request(now), request_id="b", symbol="BBB"))
    source = TimeoutSource([_bar(now.date() - timedelta(days=1))])

    result = asyncio.run(collect_once(store, source, now=now, minimum_interval_seconds=0))

    assert result.failed_symbols == 1
    assert result.observations_added == 1
    assert source.symbols == ["AAA", "BBB"]
    assert source.connected == source.disconnected == 2
    assert len(store.pending_requests(as_of=now.astimezone(NEW_YORK).date())) == 1
    assert store.first_observed_bars("BBB")
    assert store.status(now=now)["health"]["last_result"] == "partial"


def test_ibkr_qualification_timeout_requires_session_reset(monkeypatch: pytest.MonkeyPatch) -> None:
    import insider_alerts.research.ibkr_bar_source as source_module

    class HungQualificationIb:
        RaiseRequestErrors = False

        async def qualifyContractsAsync(self, *_args: object) -> list[object]:
            await asyncio.sleep(60)
            return []

    monkeypatch.setattr(source_module, "_QUALIFY_TIMEOUT_SECONDS", 0.001)
    source = IbkrHistoricalBarSource(host="127.0.0.1", port=4001, client_id=176)
    source._IbkrHistoricalBarSource__ib = HungQualificationIb()  # type: ignore[attr-defined]

    with pytest.raises(HistoricalBarSessionReset, match="qualification timed out for TEST"):
        asyncio.run(source.daily_bars("TEST", start_date=date(2026, 8, 1)))


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


def test_status_accepts_legacy_schema_until_non_mutating_migration(tmp_path: Path) -> None:
    path = tmp_path / "legacy.db"
    BarFeedStore(path)
    with sqlite3.connect(path) as conn:
        conn.execute("DROP TABLE bar_poll_receipts")
        conn.execute("DROP TABLE bar_feed_schema")

    assert bar_feed_status(path)["integrity_status"] == "valid"
    runner = CliRunner()
    legacy = runner.invoke(
        cli.app,
        ["ops", "research-bar-feed-status", "--feed-db", str(path)],
    )
    assert legacy.exit_code == 0
    with sqlite3.connect(path) as conn:
        assert conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='bar_poll_receipts'"
        ).fetchone() is None

    BarFeedStore(path)
    assert bar_feed_status(path)["integrity_status"] == "valid"
    with sqlite3.connect(path) as conn:
        conn.execute("DROP TABLE bar_poll_receipts")
    assert bar_feed_status(path)["integrity_status"] == "invalid"


def test_initializer_refuses_to_downgrade_newer_schema_marker(tmp_path: Path) -> None:
    path = tmp_path / "future.db"
    BarFeedStore(path)
    with sqlite3.connect(path) as conn:
        conn.execute("UPDATE bar_feed_schema SET schema_version=4 WHERE singleton=1")

    with pytest.raises(ValueError, match="newer than this runtime"):
        BarFeedStore(path)
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT schema_version FROM bar_feed_schema").fetchone()[0] == 4
    assert bar_feed_status(path)["integrity_status"] == "invalid"


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
