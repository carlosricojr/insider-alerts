from __future__ import annotations

import asyncio
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from contextlib import closing
from datetime import timedelta
from pathlib import Path

import pytest

from insider_alerts.execution.observer import COLUMNS, worker_lock
from insider_alerts.research import calendar_v2 as cal
from insider_alerts.research import commissioning_worker as worker
from insider_alerts.research.commissioning_custody import inventory, snapshot
from insider_alerts.research.commissioning_journal import Journal
from insider_alerts.research.commissioning_source import timestamp
from insider_alerts.research.commissioning_window import manifest, parse
from tests.test_commissioning_receipts import receipts
from tests.test_commissioning_window import BOUNDARY, CREATED
from tests.test_research_calendar_v2 import PDF, notice

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def setup(tmp_path, monkeypatch):
    clock = [CREATED]
    fingerprint = ["c" * 64]
    ticks = [0.0]
    calls = []
    source = tmp_path / "data/live_canary.db"
    source.parent.mkdir()
    with closing(sqlite3.connect(source)) as conn, conn:
        conn.execute(f"CREATE TABLE candidates({','.join(COLUMNS)}, outcome)")
        conn.execute("CREATE TABLE metadata(key,value)")
    runtime = worker.Runtime(
        tmp_path, clock=lambda: clock[0], ticks=lambda: ticks[0], fingerprint=lambda: fingerprint[0]
    )
    runtime.folder.mkdir()
    monkeypatch.setattr(cal, "_now", lambda: clock[0])
    cal.create_journal(
        runtime.calendar.path,
        baseline_pdf=PDF.read_bytes(),
        review_reference="synthetic",
        observed_at=CREATED,
    )
    bound = runtime.calendar.versions()[0]
    frozen = manifest(
        bound,
        boundary=BOUNDARY,
        created_at=CREATED,
        revision="b" * 40,
        review_reference="synthetic",
        implementation_sha256=fingerprint[0],
        inventory=inventory(snapshot(source, clock=lambda: clock[0])),
    )
    runtime.journal = Journal.create(runtime.journal.path, frozen, clock=lambda: clock[0])

    class Source:
        def __init__(self, *, clock, budget):
            self.clock, self.budget = clock, budget

        async def acquire(self, symbol, *, start, end, expected_conid, schedule):
            calls.append(
                {
                    "symbol": symbol,
                    "start": start,
                    "end": end,
                    "conid": expected_conid,
                    "schedule": schedule,
                    "budget": self.budget(),
                }
            )
            return source_response(symbol, start, end, schedule, self.clock(), bound)

    runtime.source_factory = Source
    return runtime, clock, fingerprint, ticks, calls


def add_candidate(
    runtime,
    clock,
    *,
    packet="candidate",
    symbol="TEST",
    signal="2026-10-27T12:00:00Z",
    entry="2026-10-27",
):
    row = {name: None for name in COLUMNS} | {
        "packet_id": packet,
        "symbol": symbol,
        "signal_at": signal,
        "created_at": timestamp(clock[0]),
        "entry_session": entry,
        "eligible": 1,
    }
    with closing(sqlite3.connect(runtime.source)) as conn, conn:
        conn.execute(
            f"INSERT INTO candidates VALUES({','.join('?' for _ in range(len(COLUMNS) + 1))})",
            [*(row[name] for name in COLUMNS), "OUTCOME_MUST_NOT_BE_READ"],
        )


def source_response(symbol, start, end, schedule, now, bound):
    _, fixture = receipts(schedule=schedule)
    result = fixture["payload"]
    for key in ("started_at", "client_dispatch_at", "response_at", "finished_at"):
        result[key] = timestamp(now)
    result.update(symbol=symbol, sdk_version="fixture", server_version=178, notices=[])
    result["identity"].update(
        symbol=symbol, conid=123 if symbol == "SPY" else 456, observed_at=timestamp(now)
    )
    result["request"].update(start_date=str(start), end_at=timestamp(end))
    days = [d for d in bound.days if d.day >= start and d.closes_at and d.closes_at <= end]
    if schedule:
        result["response"] = {
            "timezone": "America/New_York",
            "sessions": [
                {
                    "date": d.day.strftime("%Y%m%d"),
                    "open": d.opens_at.strftime("%Y%m%d-%H:%M:%S"),
                    "close": d.closes_at.strftime("%Y%m%d-%H:%M:%S"),
                }
                for d in days
            ],
        }
    else:
        result["response"] = [
            {
                "date": str(d.day),
                "open": "10",
                "high": "11",
                "low": "9",
                "close": "10",
                "volume": "100",
            }
            for d in days
        ]
    return result


def test_capture_runs_during_day_and_only_offhours_dispatches(setup):
    runtime, clock, _, _, calls = setup
    clock[0] = parse("2026-10-27T13:00:00Z")
    add_candidate(runtime, clock)
    assert asyncio.run(runtime.run_once())["result"] == "idle"
    assert not calls
    report = runtime.status()
    assert report["admitted_cases"] == 1
    assert report["cases"][0]["source_observed_before_decision"]
    assert not report["cases"][0]["precutoff_durable_read_verified"]
    assert "OUTCOME_MUST_NOT_BE_READ" not in str(runtime.journal.records())
    clock[0] = parse("2026-10-27T22:00:00Z")
    assert asyncio.run(runtime.run_once())["result"] == "received"
    assert len(calls) == 1 and 0 < calls[0]["budget"] <= 50
    assert asyncio.run(runtime.run_once())["result"] == "idle"
    clock[0] += timedelta(seconds=300)
    assert asyncio.run(runtime.run_once())["result"] == "received"
    assert len(calls) == 2


def test_new_calendar_notice_does_not_rebind_or_stop_capture(setup):
    runtime, clock, _, _, _ = setup
    clock[0] = parse("2026-10-27T13:00:00Z")
    add_candidate(runtime, clock)
    original = runtime.calendar.versions()[0].digest
    runtime.calendar.append_notice(notice(), expected_parent=original, observed_at=clock[0])
    assert asyncio.run(runtime.run_once())["result"] == "idle"
    result = runtime.status()
    assert result["calendar"]["frozen_version"] == original
    assert result["calendar"]["conflicted_dates"] == ["2026-11-02"]
    assert result["cases"][0]["calendar_conflicted_dates"] == ["2026-11-02"]
    assert result["cases"][0]["admission_reason"] == "included"


def test_two_invocations_capture_while_network_is_held(setup):
    runtime, clock, _, _, calls = setup
    clock[0] = parse("2026-10-27T22:00:00Z")
    real_factory = runtime.source_factory

    async def scenario():
        entered, release = asyncio.Event(), asyncio.Event()

        class SlowSource(real_factory):
            async def acquire(self, *args, **kwargs):
                entered.set()
                await release.wait()
                return await super().acquire(*args, **kwargs)

        runtime.source_factory = SlowSource
        running = asyncio.create_task(runtime.run_once())
        await asyncio.wait_for(entered.wait(), timeout=2)
        try:
            add_candidate(runtime, clock)
            assert (await runtime.run_once())["result"] == "network_busy"
            assert runtime.status()["admitted_cases"] == 1
        finally:
            release.set()
            await running

    asyncio.run(scenario())
    assert len(calls) == 1


def test_implementation_change_stops_custody_and_network(setup):
    runtime, clock, fingerprint, _, calls = setup
    before = runtime.journal.path.read_bytes()
    clock[0] = parse("2026-10-27T22:00:00Z")
    fingerprint[0] = "d" * 64
    with pytest.raises(worker.BoundaryFailure, match="implementation_changed"):
        asyncio.run(runtime.run_once())
    assert not calls and runtime.journal.path.read_bytes() == before


def test_mid_request_implementation_change_leaves_unanswered_attempt(setup):
    runtime, clock, fingerprint, _, _ = setup
    clock[0] = parse("2026-10-27T22:00:00Z")
    real_factory = runtime.source_factory

    class ChangedSource(real_factory):
        async def acquire(self, *args, **kwargs):
            result = await super().acquire(*args, **kwargs)
            fingerprint[0] = "d" * 64
            return result

    runtime.source_factory = ChangedSource
    with pytest.raises(worker.BoundaryFailure, match="implementation_changed"):
        asyncio.run(runtime.run_once())
    records = runtime.journal.records()
    assert records[-1]["kind"] == "attempt"
    assert not any(r["kind"] == "response" for r in records)


def test_source_replacement_stops_before_any_custody_or_network(setup):
    runtime, clock, _, _, calls = setup
    clock[0] = parse("2026-10-27T22:00:00Z")
    copy = runtime.source.with_name("replacement.db")
    copy.write_bytes(runtime.source.read_bytes())
    copy.replace(runtime.source)
    before = runtime.journal.path.read_bytes()
    with pytest.raises(worker.BoundaryFailure, match="source_identity_changed"):
        asyncio.run(runtime.run_once())
    assert not calls and runtime.journal.path.read_bytes() == before


def test_late_seal_is_idempotent_and_report_is_fixed_even_after_calendar_notice(setup):
    runtime, clock, _, _, calls = setup
    deadline = parse(runtime.journal.records()[0]["payload"]["recovery_deadline"])
    clock[0] = deadline + timedelta(days=2)
    runtime.source.unlink()  # Historical sealing must not require a still-live source.
    result = asyncio.run(runtime.run_once())
    assert result["result"] == "sealed" and not calls
    original = runtime.journal.path.read_bytes()
    report = runtime.status()
    assert report["report_state"] == "sealed" and not report["strategy_trial_ready"]
    clock[0] += timedelta(days=1)
    runtime.calendar.append_notice(
        notice(), expected_parent=runtime.calendar.versions()[-1].digest, observed_at=clock[0]
    )
    assert asyncio.run(runtime.run_once()) == result
    assert runtime.journal.path.read_bytes() == original
    assert runtime.status() == report
    with pytest.raises(ValueError, match="sealed"):
        runtime.journal.append("cycle", "singleton", {"result": "late"})


def test_sealing_defers_while_custody_lock_is_held(setup):
    runtime, clock, _, _, _ = setup
    clock[0] = parse(runtime.journal.records()[0]["payload"]["recovery_deadline"])
    with worker_lock(runtime.folder / "custody-lock.db"):
        assert asyncio.run(runtime.run_once())["result"] == "seal_deferred_custody_busy"
        assert runtime.journal.records()[-1]["kind"] != "seal"
    assert asyncio.run(runtime.run_once())["result"] == "sealed"


def test_status_clock_regression_is_typed_and_readonly(setup, monkeypatch, capsys):
    runtime, clock, _, _, _ = setup
    before = runtime.journal.path.read_bytes()
    clock[0] -= timedelta(seconds=1)
    monkeypatch.setattr(worker, "Runtime", lambda root: runtime)
    monkeypatch.chdir(runtime.root)
    monkeypatch.setattr(worker, "ensure_kill_on_close_process_tree", lambda: None)
    monkeypatch.setattr(
        worker,
        "__file__",
        str(runtime.root / "src/insider_alerts/research/commissioning_worker.py"),
    )
    assert worker.main(["--status"]) == 2
    assert json.loads(capsys.readouterr().out)["result"] == "clock_regression"
    assert runtime.journal.path.read_bytes() == before


def test_default_worker_cannot_initialize_missing_journals(tmp_path):
    runtime = worker.Runtime(tmp_path, fingerprint=lambda: "c" * 64)
    with pytest.raises(FileNotFoundError):
        asyncio.run(runtime.run_once())
    assert not runtime.folder.exists()


def test_wall_cutoff_and_monotonic_budget_both_bound_io(setup):
    runtime, clock, _, ticks, calls = setup
    clock[0] = parse("2026-10-28T11:59:55Z")
    real_factory = runtime.source_factory

    class BudgetSource(real_factory):
        async def acquire(self, *args, **kwargs):
            assert 0 < self.budget() <= 5
            ticks[0] += 6
            assert self.budget() == 0
            raise TimeoutError("simulated deadline")

    runtime.source_factory = BudgetSource
    assert asyncio.run(runtime.run_once())["result"] == "worker_source_exception"
    assert not calls
    clock[0] = parse("2026-10-28T12:00:00Z")
    assert asyncio.run(runtime.run_once())["result"] == "idle"
    assert runtime.status()["attempt_count"] == 1


def test_initial_preboundary_custody_does_not_make_network_requests(setup):
    runtime, clock, _, _, calls = setup
    add_candidate(runtime, clock)
    assert asyncio.run(runtime.run_once())["result"] == "idle"
    result = runtime.status()
    assert not calls and result["preexisting_records"] == 1 and result["unassignable_records"] == 0


def test_hard_process_exit_releases_network_lock_and_keeps_attempt(setup):
    runtime, clock, _, _, calls = setup
    clock[0] = parse("2026-10-27T22:00:00Z")
    marker = runtime.folder / "child-ready"
    lock = runtime.folder / "network-lock.db"
    executable = Path(sys.executable)
    if os.name == "nt":
        executable = executable.with_name("pythonw.exe")
    code = (
        "import sys,time\nfrom pathlib import Path\n"
        f"sys.path.insert(0,{str(ROOT / 'src')!r})\n"
        "from insider_alerts.research.commissioning_journal import Journal\n"
        "from insider_alerts.research.commissioning_planner import Evidence\n"
        "from insider_alerts.research.commissioning_worker import exclusive_lock\n"
        "from insider_alerts.research.commissioning_window import parse\n"
        "from insider_alerts.research.calendar_v2 import CalendarJournal\n"
        f"now=parse({timestamp(clock[0])!r})\n"
        f"journal=Journal(Path({str(runtime.journal.path)!r}),clock=lambda:now)\n"
        f"with exclusive_lock(Path({str(lock)!r})):\n"
        f" version=CalendarJournal(Path({str(runtime.calendar.path)!r})).versions()[0]\n"
        " evidence=Evidence(journal.records(),version,now)\n"
        " jobs,_=evidence.jobs_and_report()\n"
        " journal.append('attempt','network',evidence.next_request(jobs))\n"
        f" Path({str(marker)!r}).write_text('committed')\n"
        " time.sleep(60)\n"
    )
    child = subprocess.Popen(
        [str(executable), "-c", code], creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0)
    )
    try:
        limit = time.monotonic() + 15
        while not marker.exists() and child.poll() is None and time.monotonic() < limit:
            time.sleep(0.05)
        assert marker.exists(), f"child failed before lock/attempt: {child.poll()}"
        assert asyncio.run(runtime.run_once())["result"] == "network_busy"
    finally:
        child.kill()
        child.wait(timeout=10)
    assert runtime.status()["attempts_without_response"] == 1
    assert asyncio.run(runtime.run_once())["result"] == "idle"  # Durable 300s pacing.
    clock[0] += timedelta(seconds=300)
    assert asyncio.run(runtime.run_once())["result"] == "received"
    assert len(calls) == 1 and runtime.status()["attempts_without_response"] == 1


def test_client_326_is_infrastructure_and_recovers_without_exhausting_job(setup):
    runtime, clock, _, _, calls = setup
    clock[0] = parse("2026-10-27T22:00:00Z")
    real_factory = runtime.source_factory

    class OccupiedSource(real_factory):
        async def acquire(self, *args, **kwargs):
            result = await super().acquire(*args, **kwargs)
            result.update(
                result="broker_error",
                error_code=326,
                error_category="broker_326",
                response=None,
                response_at=None,
            )
            return result

    runtime.source_factory = OccupiedSource
    for index in range(7):
        assert asyncio.run(runtime.run_once())["result"] == "broker_error"
        clock[0] += timedelta(seconds=min(1800, 300 * 2**index))
    runtime.source_factory = real_factory
    assert asyncio.run(runtime.run_once())["result"] == "received"
    status = runtime.status()
    assert len(calls) == 8 and status["broker_error_code_counts"] == {"326": 7}


def test_journal_busy_is_not_misreported_as_network_lock_busy(setup, monkeypatch):
    runtime, clock, _, _, _ = setup
    clock[0] = parse("2026-10-27T22:00:00Z")
    append = runtime.journal.append

    def failing_append(kind, entity, payload):
        if kind == "response":
            exc = sqlite3.OperationalError("database is locked")
            exc.sqlite_errorcode = sqlite3.SQLITE_BUSY
            raise exc
        return append(kind, entity, payload)

    monkeypatch.setattr(runtime.journal, "append", failing_append)
    with pytest.raises(sqlite3.OperationalError):
        asyncio.run(runtime.run_once())
    assert runtime.status()["attempts_without_response"] == 1


def test_activation_is_explicit_future_exclusive_and_binds_inventory(tmp_path, monkeypatch):
    clock = [CREATED]
    monkeypatch.setattr(cal, "_now", lambda: clock[0])
    checks = []
    monkeypatch.setattr(worker, "clean_main", lambda root, fetch: checks.append(fetch) or "b" * 40)
    runtime = worker.Runtime(tmp_path, clock=lambda: clock[0], fingerprint=lambda: "c" * 64)
    runtime.source.parent.mkdir()
    with closing(sqlite3.connect(runtime.source)) as conn, conn:
        conn.execute(f"CREATE TABLE candidates({','.join(COLUMNS)}, outcome)")
        conn.execute("CREATE TABLE metadata(key,value)")
    archive = tmp_path / "docs/research/calendar-sources/nyse-holidays-2026-2028.pdf"
    archive.parent.mkdir(parents=True)
    archive.write_bytes(PDF.read_bytes())
    with pytest.raises(worker.BoundaryFailure, match="two_hours"):
        runtime.activate(CREATED + timedelta(minutes=119), "synthetic")
    assert not runtime.folder.exists()
    assert runtime.activate(BOUNDARY, "synthetic")["result"] == "activated"
    assert checks == [True, True, False]
    records = runtime.journal.records()
    assert records[0]["payload"]["preexisting_inventory"]["source_path"] == str(runtime.source)
    before = runtime.journal.path.read_bytes()
    with pytest.raises(FileExistsError):
        runtime.activate(BOUNDARY, "synthetic")
    assert runtime.journal.path.read_bytes() == before


@pytest.mark.parametrize("problem", ["branch", "dirty", "remote"])
def test_activation_rejects_noncanonical_checkout(tmp_path, monkeypatch, problem):
    def fake_git(root, *args):
        if args[0] == "fetch":
            return ""
        if args == ("branch", "--show-current"):
            return "research" if problem == "branch" else "main"
        if args == ("status", "--porcelain"):
            return "M source.py" if problem == "dirty" else ""
        return "c" * 40 if problem == "remote" and args[-1] == "origin/main" else "b" * 40

    monkeypatch.setattr(worker, "git", fake_git)
    runtime = worker.Runtime(tmp_path, clock=lambda: CREATED, fingerprint=lambda: "c" * 64)
    with pytest.raises(worker.BoundaryFailure, match="clean_synced_main"):
        runtime.activate(BOUNDARY, "synthetic")
    assert not runtime.folder.exists()


def test_implementation_digest_covers_shared_code_lock_runtime_and_hours(tmp_path, monkeypatch):
    shutil.copytree(ROOT / "src", tmp_path / "src", ignore=shutil.ignore_patterns("__pycache__"))
    shutil.copytree(
        ROOT / "docs/research/calendar-sources", tmp_path / "docs/research/calendar-sources"
    )
    for name in ("uv.lock", "pyproject.toml"):
        shutil.copyfile(ROOT / name, tmp_path / name)
    original = worker.implementation_digest(tmp_path)
    (tmp_path / "README.md").write_text("unrelated review", encoding="utf-8")
    assert worker.implementation_digest(tmp_path) == original
    for relative in (
        "src/insider_alerts/strategy/e07.py",
        "src/insider_alerts/research/capture.py",
        "src/insider_alerts/backtest/engine.py",
        "uv.lock",
    ):
        path = tmp_path / relative
        prior = path.read_bytes()
        path.write_bytes(prior + b"\n# changed\n")
        assert worker.implementation_digest(tmp_path) != original
        path.write_bytes(prior)
    actual_version = worker.version
    monkeypatch.setattr(
        worker, "version", lambda name: "changed" if name == "ib_async" else actual_version(name)
    )
    assert worker.implementation_digest(tmp_path) != original
    archive = tmp_path / "docs/research/calendar-sources/nyse-hours-2026-10-26.html"
    archive.write_bytes(b"changed")
    with pytest.raises(worker.BoundaryFailure, match="hours_archive_changed"):
        worker.implementation_digest(tmp_path)


def test_digest_file_set_contains_every_loaded_project_module():
    included = set(worker.implementation_files(ROOT))
    for name, module in tuple(sys.modules.items()):
        source = getattr(module, "__file__", None)
        if name.startswith("insider_alerts") and source is not None:
            assert Path(source) in included, name


@pytest.mark.parametrize("racing", [False, True])
def test_backdated_calendar_notice_cannot_change_sealed_report(setup, monkeypatch, racing):
    runtime, clock, _, _, _ = setup
    clock[0] = parse(runtime.journal.records()[0]["payload"]["recovery_deadline"]) + timedelta(
        days=2
    )
    original = runtime.calendar.versions
    initial = original()
    calls = [0]

    def racing_versions():
        versions = original()
        calls[0] += 1
        if calls[0] == 3:
            runtime.calendar.append_notice(
                notice(),
                expected_parent=initial[-1].digest,
                observed_at=clock[0] - timedelta(hours=1),
            )
        return versions

    if racing:
        monkeypatch.setattr(runtime.calendar, "versions", racing_versions)
    assert asyncio.run(runtime.run_once())["result"] == "sealed"
    if not racing:
        runtime.calendar.append_notice(
            notice(), expected_parent=initial[-1].digest, observed_at=clock[0] - timedelta(hours=1)
        )
    result = runtime.status()
    assert result["seal"]["calendar_head"] == {"sequence": 1, "sha256": initial[-1].digest}
    assert result["calendar"]["latest_known_version"] == initial[-1].digest
    assert not result["calendar"]["conflicted_dates"]


def test_full_synthetic_window_captures_first_and_last_enrollment_horizons(setup):
    runtime, clock, _, _, calls = setup
    frozen = runtime.journal.records()[0]["payload"]
    for index, item in enumerate(frozen["sessions"]):
        clock[0] = parse(item["open"]) - timedelta(minutes=30)
        if index in {0, 9}:
            add_candidate(
                runtime,
                clock,
                packet=f"case-{index}",
                entry=item["date"],
                signal=timestamp(clock[0] - timedelta(hours=1)),
            )
        assert asyncio.run(runtime.run_once())["result"] == "idle"
        clock[0] = parse(item["close"]) + timedelta(hours=2)
        for _ in range(12):
            outcome = asyncio.run(runtime.run_once())["result"]
            if outcome == "idle":
                break
            assert outcome == "received"
            clock[0] += timedelta(minutes=5)
        else:
            pytest.fail("bounded daily jobs failed to drain")
    interim = runtime.status()
    assert interim["admitted_cases"] == 2
    assert all(case["collection_complete"] for case in interim["cases"])
    assert interim["benchmark"]["projection_stable"]
    assert interim["benchmark"]["W10_checkpoint"]["complete"]
    assert all(day["complete"] for day in interim["schedule_coverage"])
    assert all(
        case["pin_source_receipt"] and case["original_conid"] == 456 for case in interim["cases"]
    )
    assert interim["benchmark"]["original_conid"] == 123
    assert all(
        case["stage_details"]["R2"]["receipt_lag_after_session_close_seconds"]
        for case in interim["cases"]
    )
    assert not interim["strategy_trial_ready"] and len(calls) < 45
    clock[0] = parse(frozen["recovery_deadline"])
    assert asyncio.run(runtime.run_once())["result"] == "sealed"
    final = runtime.status()
    assert final["acquisition_counts"]["collection_complete"] == 2
    assert final["report_state"] == "sealed" and not final["strategy_trial_ready"]
