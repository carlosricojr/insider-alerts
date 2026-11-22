from __future__ import annotations

import asyncio
import json
import sqlite3
from contextlib import closing
from datetime import date, timedelta

import pytest

from insider_alerts.execution.observer import COLUMNS
from insider_alerts.research import calendar_v2
from insider_alerts.research.commissioning_source import timestamp
from insider_alerts.research.commissioning_window import parse
from ops.research import e07_volatility_rehearsal as runtime
from ops.research.e07_volatility_rehearsal_contract import issuer_directory
from tests.test_commissioning_worker import source_response
from tests.test_research_calendar_v2 import PDF

DIRECTORY = json.dumps(
    {
        "fields": ["cik", "name", "ticker", "exchange"],
        "data": [[123, "Test Inc.", "TEST", "Nasdaq"], [234, "Other Inc.", "OTHER", "NYSE"]],
    }
)


@pytest.fixture
def system(tmp_path, monkeypatch):
    initial = parse("2026-10-26T21:00:00Z")
    clock = [initial]

    def ticks():
        return (clock[0] - initial).total_seconds()

    ledger = tmp_path / "data/live_canary.db"
    ledger.parent.mkdir()
    with closing(sqlite3.connect(ledger)) as conn, conn:
        conn.execute(f"CREATE TABLE candidates({','.join(COLUMNS)}, forbidden_outcomes)")
        conn.execute("CREATE TABLE metadata(key,value)")
    calendar_path = ledger.parent / "research-commissioning-v2/calendar.db"
    calendar_path.parent.mkdir()
    (calendar_path.parent / "network-lock.db").touch()
    monkeypatch.setattr(calendar_v2, "_now", lambda: clock[0])
    calendar_v2.create_journal(
        calendar_path,
        baseline_pdf=PDF.read_bytes(),
        review_reference="fixture",
        observed_at=initial,
    )
    bound = calendar_v2.CalendarJournal(calendar_path).versions()[0]
    monkeypatch.setattr(runtime, "clean_main", lambda *args, **kwargs: "a" * 40)
    calls = []
    mode = ["ok"]

    class Source:
        def __init__(self, *, clock, budget):
            self.clock, self.budget = clock, budget

        async def acquire(self, symbol, *, start, end, expected_conid):
            calls.append((symbol, start, end, expected_conid, self.budget()))
            response = source_response(symbol, start, end, False, self.clock(), bound)
            response["identity"]["primary_exchange"] = "NASDAQ"
            response["symbol_resolution_changed"] = False
            for index, row in enumerate(response["response"]):
                row["close"] = "10.01" if index % 2 else "10.05"
            if mode[0] == "failure":
                response.update(result="timeout", response=None)
            return response

    def make():
        return runtime.Rehearsal(
            tmp_path,
            clock=lambda: clock[0],
            ticks=ticks,
            digest_for=lambda root: "b" * 64,
            source=Source,
            directory=lambda _: DIRECTORY,
            network_idle=lambda: True,
        )

    instance = make()
    instance.activate(date(2026, 10, 27), "fixture", sec_contact="fixture@example.net")
    return instance, make, clock, calls, mode


def add_case(instance, clock, *, packet="candidate", cik="0000000123", entry="2026-10-27"):
    row = dict.fromkeys(COLUMNS) | {
        "packet_id": packet,
        "accession_number": "fixture-accession",
        "cik": cik,
        "symbol": "TEST",
        "signal_at": timestamp(clock[0]),
        "created_at": timestamp(clock[0]),
        "entry_session": entry,
        "eligible": 1,
    }
    with closing(sqlite3.connect(instance.ledger)) as conn, conn:
        placeholders = ",".join("?" for _ in range(len(COLUMNS) + 1))
        conn.execute(
            f"INSERT INTO candidates VALUES ({placeholders})",
            [*(row[key] for key in COLUMNS), "OUTCOME_NOT_IN_PROJECTION"],
        )


def run(instance):
    asyncio.run(instance.cycle())
    return instance.load().report(instance.now())


def test_offhours_capture_durable_read_and_daily_contract_pin(system):
    instance, make, clock, calls, _ = system
    clock[0] = parse("2026-10-27T11:05:00Z")
    add_case(instance, clock)
    before = instance.ledger.read_bytes()
    report = run(instance)
    assert report["custody"] == {"admitted": 1}
    assert report["preopen"] == {"waiting_for_cutoff": 1}
    assert len(calls) == 1 and calls[0][3] is None and calls[0][-1] > 0
    assert before == instance.ledger.read_bytes()
    assert "OUTCOME_NOT_IN_PROJECTION" not in str(instance.journal.records())

    # A new process reads a committed response. Arrival after the network window
    # stays in the denominator and receives a fallback.
    clock[0] = parse("2026-10-27T13:17:00Z")
    instance = make()
    add_case(instance, clock, packet="late-candidate")
    report = run(instance)
    assert report["preopen"] == {
        "features_observed": 1,
        "no_complete_preopen_bar_receipt": 1,
    }
    assert len(calls) == 1
    reads = [rec for rec in instance.journal.records() if rec["entity"].startswith("read:")]
    assert any(len(rec["payload"].get("bar_digests", [])) == 21 for rec in reads)
    assert any(rec["payload"]["fallback_width"] == 0.10 for rec in reads)

    clock[0] = parse("2026-10-27T22:15:00Z")
    report = run(instance)
    assert calls[-1][3] == 456
    assert calls[-1][1].isoformat() == "2026-10-27"
    assert report["daily_projections"]["provider_projection_observed"] == 1
    assert report["outcome_price_basis"] == "as_traded_unverified"


def test_pacing_persists_across_restarts_and_stops_after_bounded_attempts(system):
    instance, make, clock, calls, mode = system
    clock[0] = parse("2026-10-27T11:00:00Z")
    add_case(instance, clock)
    mode[0] = "failure"
    run(instance)
    for minutes, expected in [(4, 1), (1, 2), (5, 3), (5, 3)]:
        clock[0] += timedelta(minutes=minutes)
        run(make())
        assert len(calls) == expected


def test_interrupted_preopen_read_is_never_resealed(system, monkeypatch):
    instance, make, clock, _, _ = system
    clock[0] = parse("2026-10-27T11:00:00Z")
    add_case(instance, clock)
    run(instance)
    clock[0] = parse("2026-10-27T13:17:00Z")
    append = instance.journal.append

    def interrupted(kind, entity, payload):
        if entity.startswith("commit:"):
            raise RuntimeError("interrupted after durable read")
        return append(kind, entity, payload)

    monkeypatch.setattr(instance.journal, "append", interrupted)
    with pytest.raises(RuntimeError, match="interrupted"):
        run(instance)
    run(make())
    clock[0] = parse("2026-10-27T13:18:10Z")
    assert run(make())["preopen"] == {"durability_not_proven_before_cutoff": 1}


def test_mismatched_issuer_keeps_raw_receipt_and_uses_fallback(system):
    instance, _, clock, _, _ = system
    clock[0] = parse("2026-10-27T11:00:00Z")
    add_case(instance, clock, cik="99999")
    run(instance)
    clock[0] = parse("2026-10-27T13:17:00Z")
    assert run(instance)["preopen"] == {"issuer_venue_unverified": 1}
    payload = next(
        record["payload"]
        for record in instance.journal.records()
        if record["entity"].startswith("read:")
    )
    assert payload["conid"] == 456 and payload["fallback_width"] == 0.10
    assert "daily_volatility" not in payload


def test_prior_close_is_early_close_for_first_session_after_thanksgiving(system):
    instance, _, _, _, _ = system
    version = instance.load().version
    dates, ready = runtime._history(version, date(2026, 11, 30))
    assert max(dates) == "2026-11-27"
    assert ready == parse("2026-11-27T18:01:00Z")


def test_directory_contract_rejects_reordered_columns_and_tracks_ambiguity():
    body = json.loads(DIRECTORY)
    body["data"].append([345, "Repeat", "TEST", "Nasdaq"])
    assert len(issuer_directory(json.dumps(body))["TEST"]) == 2
    body["fields"] = body["fields"][::-1]
    with pytest.raises(ValueError, match="unrecognized"):
        issuer_directory(json.dumps(body))


def test_clock_discontinuity_is_not_a_valid_decision_clock():
    wall = [parse("2026-11-27T14:16:00Z")]
    ticks = [0.0]
    clock = runtime.checked_clock(lambda: wall[0], lambda: ticks[0])
    wall[0] += timedelta(minutes=1)
    with pytest.raises(ValueError, match="discontinuity"):
        clock()


def test_legacy_network_lock_prevents_dispatched_attempt(system):
    instance, make, clock, calls, _ = system
    clock[0] = parse("2026-10-27T11:00:00Z")
    add_case(instance, clock)
    lock = instance.calendar.path.parent / "network-lock.db"
    with runtime.exclusive_lock(lock), pytest.raises(runtime.LockBusy):
        run(instance)
    assert not calls
    assert not any(row["kind"] == "attempt" for row in instance.journal.records())
    assert run(make())["custody"] == {"admitted": 1}
    assert len(calls) == 1


def test_commit_crossing_cutoff_is_explicitly_unverified(system, monkeypatch):
    instance, make, clock, _, _ = system
    clock[0] = parse("2026-10-27T11:00:00Z")
    add_case(instance, clock)
    run(instance)
    clock[0] = parse("2026-10-27T13:17:59Z")
    append = instance.journal.append

    def delayed(kind, entity, payload):
        value = append(kind, entity, payload)
        if entity.startswith("read:"):
            clock[0] += timedelta(seconds=1)
        return value

    monkeypatch.setattr(instance.journal, "append", delayed)
    assert run(instance)["preopen"] == {"durability_not_proven_before_cutoff": 1}
    assert run(make())["preopen"] == {"durability_not_proven_before_cutoff": 1}


def test_rehearsal_seals_at_fixed_drain_even_after_restart(system):
    instance, make, clock, _, _ = system
    expiry = parse(instance.load().frozen["expiry"])
    clock[0] = expiry + timedelta(minutes=5)
    sealed = run(make())
    assert sealed["sealed"] and not sealed["trading_enabled"]
    assert run(make())["journal_head"] == sealed["journal_head"]


@pytest.mark.parametrize(
    "contact", ["\nforged@example.com", "missing", "x@example.com\r\nHeader: x"]
)
def test_sec_contact_rejects_invalid_header_material(contact):
    with pytest.raises(ValueError, match="invalid rehearsal SEC contact"):
        runtime.sec_user_agent(contact)
