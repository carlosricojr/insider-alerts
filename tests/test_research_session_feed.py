from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from typer.testing import CliRunner

from insider_alerts import cli
from insider_alerts.research.session_feed import (
    ExchangeSession,
    SessionFeedStore,
    SourceSessionBatch,
    collect_sessions_once,
    session_feed_status,
)

NEW_YORK = ZoneInfo("America/New_York")


def _session(day: date, *, close_hour_local: int = 16) -> ExchangeSession:
    return ExchangeSession(
        day,
        datetime(day.year, day.month, day.day, 9, 30, tzinfo=NEW_YORK).astimezone(UTC),
        datetime(day.year, day.month, day.day, close_hour_local, 0, tzinfo=NEW_YORK).astimezone(
            UTC
        ),
    )


class FakeSessionSource:
    def __init__(
        self,
        batch: SourceSessionBatch,
        *,
        error: Exception | None = None,
    ) -> None:
        self.batch = batch
        self.error = error
        self.connected = 0
        self.disconnected = 0
        self.calls: list[tuple[datetime, int]] = []

    async def connect(self) -> None:
        self.connected += 1
        if self.error is not None:
            raise self.error

    async def exchange_sessions(
        self,
        *,
        end: datetime,
        calendar_days: int,
    ) -> SourceSessionBatch:
        self.calls.append((end, calendar_days))
        return self.batch

    def disconnect(self) -> None:
        self.disconnected += 1


def test_schedule_is_append_only_idempotent_and_point_in_time(tmp_path: Path) -> None:
    store = SessionFeedStore(tmp_path / "sessions.db")
    observed = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)
    regular = _session(date(2026, 11, 27))
    early = _session(date(2026, 11, 27), close_hour_local=13)
    assert store.append([regular], observed_at_utc=observed) == (1, 0)
    assert store.append([regular], observed_at_utc=observed + timedelta(hours=1)) == (0, 0)
    assert store.append([early], observed_at_utc=observed + timedelta(days=1)) == (1, 1)
    assert store.append([regular], observed_at_utc=observed + timedelta(days=2)) == (1, 1)
    assert store.schedule_as_known_at(observed) == [regular]
    assert store.schedule_as_known_at(observed + timedelta(days=1)) == [early]
    assert store.schedule_as_known_at(observed + timedelta(days=2)) == [regular]
    assert store.latest_schedule() == [regular]
    assert store.status()["revision_count"] == 2
    assert store.status()["integrity_status"] == "valid"

    with sqlite3.connect(store.path) as conn:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            conn.execute("UPDATE session_observations SET session_date='2026-01-01'")
        with pytest.raises(sqlite3.IntegrityError, match="gap-free"):
            conn.execute(
                """
                INSERT INTO session_observations VALUES(
                  99,'id','2026-01-01','x','x','x','value','record',X'00'
                )
                """
            )


def test_worker_records_rejections_and_failure_health(tmp_path: Path) -> None:
    now = datetime(2026, 8, 27, 7, 0, tzinfo=UTC)
    store = SessionFeedStore(tmp_path / "sessions.db")
    session = _session(date(2026, 8, 27))
    source = FakeSessionSource(SourceSessionBatch((session,), ("bad-date:invalid_schedule",)))
    result = asyncio.run(collect_sessions_once(store, source, now=now))
    assert result.sessions_seen == result.observations_added == 1
    assert result.rejected_sessions == 1
    assert source.connected == source.disconnected == 1
    status = store.status()
    assert status["failure_count"] == 1
    assert status["health"]["last_result"] == "partial"

    failed_store = SessionFeedStore(tmp_path / "failed.db")
    failed_source = FakeSessionSource(
        SourceSessionBatch(()), error=ConnectionError("gateway unavailable")
    )
    with pytest.raises(ConnectionError, match="gateway unavailable"):
        asyncio.run(collect_sessions_once(failed_store, failed_source, now=now))
    assert failed_source.connected == failed_source.disconnected == 1
    assert failed_store.status()["health"]["last_result"] == "failed"


def test_completed_through_date_uses_schedule_as_known_at_that_instant(tmp_path: Path) -> None:
    store = SessionFeedStore(tmp_path / "sessions.db")
    observed = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)
    session = _session(date(2026, 8, 27), close_hour_local=13)
    store.append([session], observed_at_utc=observed)

    assert store.completed_through_date(session.closes_at_utc) is None
    assert (
        store.completed_through_date(session.closes_at_utc + timedelta(minutes=1))
        == session.session_date
    )


def test_completed_through_ignores_schedule_revision_observed_after_boundary(
    tmp_path: Path,
) -> None:
    store = SessionFeedStore(tmp_path / "sessions.db")
    day = date(2026, 8, 27)
    regular = _session(day)
    early = _session(day, close_hour_local=13)
    store.append([regular], observed_at_utc=datetime(2026, 8, 20, 12, 0, tzinfo=UTC))
    correction_at = datetime(2026, 8, 27, 18, 2, tzinfo=UTC)
    store.append([early], observed_at_utc=correction_at)

    assert store.completed_through_date(correction_at - timedelta(minutes=1)) is None
    assert store.completed_through_date(correction_at + timedelta(minutes=1)) == day


def test_status_does_not_create_missing_database(tmp_path: Path) -> None:
    missing = tmp_path / "wrong.db"
    assert session_feed_status(missing)["integrity_status"] == "missing"
    assert not missing.exists()
    corrupt = tmp_path / "corrupt.db"
    corrupt.write_bytes(b"not sqlite")
    assert session_feed_status(corrupt)["integrity_status"] == "invalid"


def test_status_cli_uses_integrity_exit_code(tmp_path: Path) -> None:
    store = SessionFeedStore(tmp_path / "sessions.db")
    runner = CliRunner()

    valid = runner.invoke(
        cli.app,
        ["ops", "research-session-feed-status", "--feed-db", str(store.path)],
    )
    assert valid.exit_code == 0
    missing_path = tmp_path / "missing.db"
    missing = runner.invoke(
        cli.app,
        ["ops", "research-session-feed-status", "--feed-db", str(missing_path)],
    )
    assert missing.exit_code == 3
    assert not missing_path.exists()


def test_windows_session_task_is_direct_hidden_pythonw() -> None:
    installer = (
        Path(__file__).parents[1] / "ops" / "windows" / "install-research-session-feed-task.ps1"
    ).read_text(encoding="utf-8")
    assert ".venv\\Scripts\\pythonw.exe" in installer
    action = installer.split("$action =", maxsplit=1)[1].split("$logonTrigger", maxsplit=1)[0]
    assert "-Execute $pythonExe" in action
    assert "powershell" not in action.lower()
    assert "cmd.exe" not in action.lower()
    assert "-Hidden" in installer
    bar_installer = (
        Path(__file__).parents[1] / "ops" / "windows" / "install-research-bar-feed-task.ps1"
    ).read_text(encoding="utf-8")
    assert "--session-feed-db" in bar_installer


def test_endpoint_convention_is_frozen_in_registry_and_preregistration() -> None:
    root = Path(__file__).parents[1]
    registry = (root / "docs" / "research" / "registry" / "OPP-E07-V1.json").read_text(
        encoding="utf-8"
    )
    prereg = (
        root / "docs" / "research" / "OPPORTUNISTIC-PROSPECTIVE-TRIAL-2026-08-26-PREREG.md"
    ).read_text(encoding="utf-8")
    assert "spy_entry_session_rth_open_to_exit_session_rth_close" in registry
    assert "official_exchange_rth_open_and_close_session_boundaries" in registry
    assert "no unobserved intraday hit time is imputed" in prereg
