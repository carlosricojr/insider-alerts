from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import subprocess
import sys
from contextlib import closing
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from insider_alerts.backtest.models import DailyBar
from insider_alerts.execution import observer as obs
from insider_alerts.execution import observer_worker as worker
from insider_alerts.research.bar_feed import SourceBarBatch

NOW = datetime(2026, 9, 15, 0, 0, tzinfo=UTC)  # Monday 20:00 New York
START = NOW - timedelta(days=2)


def candidate(packet: str = "p1", symbol: str = "ABC", **updates: object) -> dict:
    row = dict.fromkeys(obs.COLUMNS)
    row.update(
        packet_id=packet,
        symbol=symbol,
        accession_number="accession",
        cik="123",
        signal_at=obs.utc(NOW - timedelta(days=1)),
        created_at=obs.utc(NOW - timedelta(days=1)),
        eligible=1,
        entry_session="2026-09-15",
        eligibility_reason="eligible",
        score=1.0,
        planned_quantity=10,
        lottery_rank=packet,
    )
    row.update(updates)
    return row


def source_db(path: Path, rows: list[dict]) -> Path:
    with closing(sqlite3.connect(path)) as conn:
        conn.execute(f"CREATE TABLE candidates ({','.join(obs.COLUMNS)})")
        conn.executemany(
            f"INSERT INTO candidates VALUES ({','.join('?' for _ in obs.COLUMNS)})",
            [tuple(row[c] for c in obs.COLUMNS) for row in rows],
        )
        conn.commit()
    return path


@pytest.fixture
def journal(tmp_path: Path) -> obs.Journal:
    return obs.Journal.activate(
        tmp_path / "evidence.db", start=START, now=START - timedelta(hours=3), revision="a" * 40
    )


def bar(symbol: str = "ABC", day: date = date(2026, 9, 13), **kwargs: float) -> DailyBar:
    values = dict(open=10.0, high=11.0, low=9.0, close=10.5, volume=100.0)
    values.update(kwargs)
    return DailyBar(symbol=symbol, trade_date=day, **values)


class Source:
    def __init__(self, bars: tuple[DailyBar, ...] = ()) -> None:
        self.bars = bars
        self.calls: list[str] = []
        self.connected = False
        self.failure = False
        self.rejections: tuple[str, ...] = ()

    async def connect(self) -> None:
        self.connected = True

    def disconnect(self) -> None:
        self.connected = False

    async def daily_bars(self, symbol: str, *, start_date: date) -> SourceBarBatch:
        self.calls.append(symbol)
        if self.failure:
            raise TimeoutError("expected market boundary")
        return SourceBarBatch(self.bars, self.rejections)


def run(journal: obs.Journal, path: Path, source: Source, now: datetime = NOW) -> dict:
    with obs.worker_lock(journal.path.parent / "worker-lock.db"):
        return asyncio.run(
            obs.run_once(journal, path, source, revision="b" * 40, clock=lambda: now)
        )


def test_activation_exclusive_and_future(tmp_path: Path) -> None:
    path = tmp_path / "evidence.db"
    with pytest.raises(ValueError):
        obs.Journal.activate(path, start=NOW, now=NOW, revision="a")
    assert not path.exists()
    obs.Journal.activate(path, start=NOW + timedelta(hours=2), now=NOW, revision="a")
    original = path.read_bytes()
    with pytest.raises(FileExistsError):
        obs.Journal.activate(path, start=NOW + timedelta(hours=3), now=NOW, revision="a")
    assert path.read_bytes() == original


@pytest.mark.parametrize("stage", ["append", "validate", "fsync"])
def test_activation_failure_never_publishes_and_can_retry(
    tmp_path: Path, monkeypatch, stage: str
) -> None:
    path = tmp_path / "evidence.db"

    def failure(*args, **kwargs):
        assert not path.exists()
        raise OSError("injected activation failure")

    with monkeypatch.context() as patch:
        patch.setattr(obs.os if stage == "fsync" else obs.Journal, stage, failure)
        with pytest.raises(OSError, match="injected"):
            obs.Journal.activate(path, start=START, now=START - timedelta(hours=3), revision="a")
    assert not list(tmp_path.iterdir())
    obs.Journal.activate(path, start=START, now=START - timedelta(hours=3), revision="a").validate()


def test_activation_publication_race_never_overwrites(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "evidence.db"
    operation = "rename" if os.name == "nt" else "link"
    original = getattr(obs.os, operation)

    def collision(source, target):
        Path(target).write_bytes(b"concurrently published")
        original(source, target)

    monkeypatch.setattr(obs.os, operation, collision)
    with pytest.raises(FileExistsError):
        obs.Journal.activate(path, start=START, now=START - timedelta(hours=3), revision="a")
    assert path.read_bytes() == b"concurrently published"
    assert list(tmp_path.iterdir()) == [path]


def test_readonly_source_and_no_outcome_columns(tmp_path: Path) -> None:
    path = source_db(tmp_path / "source.db", [candidate()])
    before = path.read_bytes()
    assert obs.snapshot(path) == [candidate()]
    assert path.read_bytes() == before
    with pytest.raises(sqlite3.OperationalError):
        obs.snapshot(tmp_path / "missing.db")
    assert not (tmp_path / "missing.db").exists()
    assert not any("live_" in c or "exit" in c or "shadow" in c for c in obs.COLUMNS)


def test_append_chain_revisions_and_triggers(journal: obs.Journal) -> None:
    assert journal.append("candidate", "p1", {"eligible": 1}, now=NOW)
    assert not journal.append("candidate", "p1", {"eligible": 1}, now=NOW)
    assert journal.append("candidate", "p1", {"eligible": 0}, now=NOW)
    rows = journal.records("candidate")
    assert rows[1]["supersedes"] == rows[0]["sha256"]
    journal.validate()
    with journal.connect() as conn:
        for sql in ("UPDATE records SET entity='x'", "DELETE FROM records"):
            with pytest.raises(sqlite3.IntegrityError, match="append only"):
                conn.execute(sql)


def test_tamper_detected(journal: obs.Journal) -> None:
    with journal.connect() as conn:
        conn.execute("DROP TRIGGER records_no_update")
        conn.execute("UPDATE records SET sha256=?", ("0" * 64,))
    with pytest.raises(ValueError, match="integrity"):
        journal.validate()


def test_worker_lock_rejects_overlap_and_recovers(tmp_path: Path) -> None:
    path = tmp_path / "lock.db"
    with obs.worker_lock(path), pytest.raises(sqlite3.OperationalError), obs.worker_lock(path):
        pytest.fail("overlapping worker")
    with obs.worker_lock(path):
        pass


def test_future_activation_does_not_read_source_or_connect(
    journal: obs.Journal, tmp_path: Path
) -> None:
    source = Source()
    assert (
        run(journal, tmp_path / "absent.db", source, START - timedelta(seconds=1))["result"]
        == "waiting_activation"
    )
    assert not source.connected and not source.calls


def test_all_ledger_candidates_no_capacity_or_eligibility_filter(
    journal: obs.Journal, tmp_path: Path
) -> None:
    rows = [candidate(str(i), eligible=i % 2) for i in range(100)]
    path = source_db(tmp_path / "source.db", rows)
    run(journal, path, Source())
    assert len(journal.records("candidate")) == 100
    assert obs.status(journal, now=NOW)["candidates"] == 100


def test_preboundary_excluded_and_bad_timestamp_typed(journal: obs.Journal) -> None:
    rows = [
        candidate("old", signal_at=obs.utc(START - timedelta(seconds=1))),
        candidate("bad", signal_at="not a timestamp"),
        candidate("future", created_at=obs.utc(NOW + timedelta(seconds=1))),
        candidate("reverse", signal_at=obs.utc(NOW)),
        candidate("new"),
    ]
    assert obs.capture_candidates(journal, rows, now=NOW, start=START) == 2
    assert len(journal.records("candidate_missing")) == 2


def test_invalid_symbol_and_nonfinite_projection(journal: obs.Journal) -> None:
    rows = [candidate("bad", symbol="not/a/symbol"), candidate("nan", score=float("nan"))]
    assert obs.capture_candidates(journal, rows, now=NOW, start=START) == 1
    assert len(journal.records("candidate_missing")) == 2


@pytest.mark.parametrize("hour", [8, 9, 16, 17])
def test_no_market_calls_during_day(journal: obs.Journal, tmp_path: Path, hour: int) -> None:
    path = source_db(tmp_path / "source.db", [candidate()])
    source = Source((bar(),))
    result = run(journal, path, source, datetime(2026, 9, 14, hour, tzinfo=obs.NY))
    assert result["result"] == "off_hours_only"
    assert not source.calls
    assert len(journal.records("candidate")) == 1


def test_current_future_and_outside_window_bars_not_captured(
    journal: obs.Journal, tmp_path: Path
) -> None:
    path = source_db(tmp_path / "source.db", [candidate()])
    source = Source(
        (bar(), bar(day=date(2026, 9, 14)), bar(day=date(2026, 9, 15)), bar(day=date(2026, 9, 1)))
    )
    assert run(journal, path, source)["result"] == "received"
    assert [r["payload"]["date"] for r in journal.records("bar")] == ["2026-09-13"]
    assert not source.connected
    assert journal.records("bar")[0]["payload"]["attempt_started_at"] == obs.utc(NOW)


@pytest.mark.parametrize(
    "bad",
    [
        bar(close=float("nan")),
        bar(low=11.0),
        bar(high=9.0),
        bar(open=0.0),
        bar(volume=-1.0),
        bar(symbol="OTHER"),
    ],
)
def test_invalid_bars_are_typed_and_good_bars_isolated(
    journal: obs.Journal, tmp_path: Path, bad: DailyBar
) -> None:
    path = source_db(tmp_path / "source.db", [candidate()])
    assert run(journal, path, Source((bad, bar())))["result"] == "partial"
    assert len(journal.records("bar_missing")) == 1
    assert len(journal.records("bar")) == 1


def test_source_unavailable_does_not_create_source(journal: obs.Journal, tmp_path: Path) -> None:
    source = Source()
    path = tmp_path / "missing.db"
    assert run(journal, path, source)["result"] == "source_unavailable"
    assert not path.exists() and not source.calls


def test_empty_data_not_success_and_failure_retry_paced(
    journal: obs.Journal, tmp_path: Path
) -> None:
    path = source_db(tmp_path / "source.db", [candidate()])
    source = Source()
    assert run(journal, path, source)["result"] == "partial"
    source.failure = True
    assert run(journal, path, source, NOW + timedelta(seconds=299))["result"] == "paced"
    assert (
        run(journal, path, source, NOW + timedelta(seconds=300))["result"]
        == "market_data_unavailable"
    )
    assert not source.connected


@pytest.mark.parametrize("primary_failure", [False, True])
def test_disconnect_failure_keeps_poll_and_cycle_receipts(
    journal: obs.Journal, tmp_path: Path, primary_failure: bool
) -> None:
    path = source_db(tmp_path / "source.db", [candidate()])

    class BrokenCleanup(Source):
        def disconnect(self) -> None:
            super().disconnect()
            raise RuntimeError("cleanup failed")

    source = BrokenCleanup((bar(),))
    source.failure = primary_failure
    assert run(journal, path, source)["result"] == "market_data_unavailable"
    poll = journal.records("poll")[0]["payload"]
    assert poll["cleanup_error_type"] == "RuntimeError"
    assert poll["error_type"] == ("TimeoutError" if primary_failure else None)
    assert journal.records("cycle")[-1]["payload"]["cleanup_error_type"] == "RuntimeError"
    journal.validate()


def test_persisted_crash_attempt_and_fairness(journal: obs.Journal, tmp_path: Path) -> None:
    path = source_db(tmp_path / "source.db", [candidate("a", "AAA"), candidate("b", "BBB")])
    journal.append("attempt", "AAA", {"crashed": True}, now=NOW)
    source = Source()
    assert run(journal, path, source)["result"] == "paced"
    run(journal, path, source, NOW + timedelta(minutes=5))
    assert source.calls == ["BBB"]


def test_success_not_repeated_same_day_and_new_candidate_symbol_is_seen(
    journal: obs.Journal, tmp_path: Path
) -> None:
    path = source_db(tmp_path / "source.db", [candidate()])
    source = Source((bar(),))
    run(journal, path, source)
    run(journal, path, source, NOW + timedelta(minutes=5))
    assert source.calls == ["ABC"]
    assert len(journal.records("cycle")) == 2
    assert obs.status(journal, now=NOW + timedelta(minutes=5))["heartbeat_fresh"]


def test_source_mutation_preserved_not_replanned(journal: obs.Journal, tmp_path: Path) -> None:
    path = source_db(tmp_path / "source.db", [candidate()])
    source = Source()
    run(journal, path, source)
    with closing(sqlite3.connect(path)) as conn:
        conn.execute("UPDATE candidates SET symbol='XYZ'")
        conn.commit()
    run(journal, path, source, NOW + timedelta(minutes=5))
    assert source.calls == ["ABC", "ABC"]
    assert [r["payload"]["symbol"] for r in journal.records("candidate")] == ["ABC", "XYZ"]


def test_expired_window_typed_no_retrospective_request(
    journal: obs.Journal, tmp_path: Path
) -> None:
    path = source_db(tmp_path / "source.db", [candidate()])
    source = Source()
    run(journal, path, source, NOW + timedelta(days=47))
    assert not source.calls
    assert len(journal.records("window_closed")) == 1
    assert journal.records("candidate")[0]["observed_at"] == obs.utc(NOW + timedelta(days=47))


def test_status_blinded_readonly_and_staleness(journal: obs.Journal, tmp_path: Path) -> None:
    path = source_db(tmp_path / "source.db", [candidate()])
    run(journal, path, Source((bar(),)))
    readonly = obs.Journal(journal.path, readonly=True)
    before = journal.records()
    report = obs.status(readonly, now=NOW + timedelta(minutes=16))
    assert not report["heartbeat_fresh"]
    assert not report["profit_reporting_enabled"]
    assert not report["session_coverage_proven"]
    assert "10.5" not in json.dumps(report)
    assert journal.records() == before
    with pytest.raises(sqlite3.OperationalError):
        readonly.append("bad", "bad", {}, now=NOW)


def test_hardlink_path_rejected(tmp_path: Path) -> None:
    path = tmp_path / "source.db"
    path.touch()
    alias = tmp_path / "alias.db"
    os.link(path, alias)
    with pytest.raises(ValueError, match="hard links"):
        obs.safe_path(alias)


def test_naive_time_rejected() -> None:
    with pytest.raises(ValueError):
        obs.utc(datetime(2026, 1, 1))
    with pytest.raises(ValueError):
        obs.parse("2026-01-01")


def test_bar_repeat_receipt_and_revision(journal: obs.Journal, tmp_path: Path) -> None:
    path = source_db(tmp_path / "source.db", [candidate()])
    source = Source((bar(),))
    run(journal, path, source)
    run(journal, path, source, NOW + timedelta(days=1))
    assert len(journal.records("bar")) == 1
    assert len(journal.records("poll")) == 2
    assert journal.records("poll")[0]["payload"]["accepted_bar_content_hashes"]
    source.bars = (bar(close=10.6),)
    run(journal, path, source, NOW + timedelta(days=2))
    assert len(journal.records("bar")) == 2
    assert journal.records("bar")[1]["supersedes"] == journal.records("bar")[0]["sha256"]
    journal.validate()


def test_source_rejections_persisted(journal: obs.Journal, tmp_path: Path) -> None:
    path = source_db(tmp_path / "source.db", [candidate()])
    source = Source((bar(),))
    source.rejections = ("unknown_date:invalid_trade_date",)
    assert run(journal, path, source)["result"] == "partial"
    assert journal.records("bar_missing")[0]["payload"]["reason"] == "source_rejected"


def test_crossing_daytime_during_connect_prevents_history(
    journal: obs.Journal, tmp_path: Path
) -> None:
    path = source_db(tmp_path / "source.db", [candidate()])
    at = datetime(2026, 9, 15, 7, 59, tzinfo=obs.NY)
    clock = [at]

    class CrossingSource(Source):
        async def connect(self) -> None:
            await super().connect()
            clock[0] += timedelta(minutes=1)

    source = CrossingSource((bar(),))
    asyncio.run(obs.run_once(journal, path, source, revision="r", clock=lambda: clock[0]))
    assert not source.calls and not source.connected
    assert journal.records("poll")[0]["payload"]["result"] == "market_data_unavailable"


def test_qualification_canceled_at_cutoff(journal: obs.Journal, tmp_path: Path) -> None:
    path = source_db(tmp_path / "source.db", [candidate()])
    at = datetime(2026, 9, 15, 7, 59, 59, 990000, tzinfo=obs.NY)

    class SlowQualification(Source):
        async def daily_bars(self, symbol: str, *, start_date: date) -> SourceBarBatch:
            await asyncio.sleep(1)  # qualification awaits before history dispatch
            return await super().daily_bars(symbol, start_date=start_date)

    source = SlowQualification((bar(),))
    result = run(journal, path, source, at)
    assert result["result"] == "market_data_unavailable"
    assert not source.calls and not source.connected
    assert obs.io_timeout(at, 45) == pytest.approx(0.01)


def test_signal_arriving_during_canary_cycle_is_preserved(journal: obs.Journal) -> None:
    row = candidate(
        signal_at=obs.utc(START + timedelta(seconds=5)),
        created_at=obs.utc(START - timedelta(seconds=5)),
    )
    assert obs.capture_candidates(journal, [row], now=NOW, start=START) == 1
    assert journal.records("candidate")[0]["payload"] == row
    assert not journal.records("candidate_missing")


def test_capture_window_uses_local_dates_across_dst(journal: obs.Journal, tmp_path: Path) -> None:
    signal = datetime(2026, 10, 1, 0, 30, tzinfo=obs.NY)
    path = source_db(
        tmp_path / "source.db", [candidate(signal_at=obs.utc(signal), created_at=obs.utc(signal))]
    )
    # 45 local calendar days ends Nov15, not Nov14 after subtracting the DST hour.
    at = datetime(2026, 11, 16, 7, 0, tzinfo=obs.NY)
    source = Source((bar(day=date(2026, 11, 15)),))
    assert run(journal, path, source, at)["result"] == "received"
    assert journal.records("attempt")[0]["payload"]["through_date"] == "2026-11-15"


def test_capture_window_spring_dst_does_not_extend_one_day(tmp_path: Path) -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    journal = obs.Journal.activate(
        tmp_path / "evidence.db", start=start, now=start - timedelta(hours=3), revision="a"
    )
    signal = datetime(2026, 2, 1, 23, 30, tzinfo=obs.NY)
    path = source_db(
        tmp_path / "source.db", [candidate(signal_at=obs.utc(signal), created_at=obs.utc(signal))]
    )
    source = Source((bar(day=date(2026, 3, 18)), bar(day=date(2026, 3, 19))))
    assert (
        run(journal, path, source, datetime(2026, 3, 19, 7, tzinfo=obs.NY))["result"] == "received"
    )
    assert journal.records("attempt")[0]["payload"]["through_date"] == "2026-03-18"
    assert [r["payload"]["date"] for r in journal.records("bar")] == ["2026-03-18"]
    assert run(journal, path, source, datetime(2026, 3, 20, 7, tzinfo=obs.NY))["result"] == "idle"
    assert source.calls == ["ABC"]


def test_cli_help_no_runpy_warning() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-W",
            "error::RuntimeWarning",
            "-m",
            "insider_alerts.execution.observer_worker",
            "--help",
        ],
        capture_output=True,
        text=True,
        timeout=30,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    assert result.returncode == 0, result.stderr
    assert "--activate-at" in result.stdout


def test_worker_paths_modes_and_no_implicit_activation(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        worker, "__file__", str(tmp_path / "src/insider_alerts/execution/observer_worker.py")
    )
    monkeypatch.setattr(worker, "ensure_kill_on_close_process_tree", lambda: None)
    monkeypatch.setattr(worker, "revision", lambda root: "a" * 40)
    assert worker.main([]) == 2
    assert not (tmp_path / "data/observer/evidence.db").exists()
    (tmp_path / "data").mkdir()
    source_db(tmp_path / "data/live_canary.db", [candidate()])
    activate = obs.utc(datetime.now(UTC) + timedelta(hours=3))
    assert worker.main(["--activate-at", activate]) == 0
    assert worker.main([]) == 0
    assert worker.main(["--status"]) == 0
    assert '"waiting_activation"' in capsys.readouterr().out
    assert worker.main(["--activate-at", activate]) == 2


def test_revision_probe_is_bounded_and_invisible(tmp_path: Path, monkeypatch) -> None:
    calls = []

    def probe(*args, **kwargs):
        calls.append((args, kwargs))
        return subprocess.CompletedProcess(args[0], 0, stdout="abc\n")

    monkeypatch.setattr(worker.subprocess, "run", probe)
    assert worker.revision(tmp_path) == "abc"
    assert calls[0][1]["timeout"] == 10
    assert calls[0][1]["creationflags"] == getattr(subprocess, "CREATE_NO_WINDOW", 0)
