from __future__ import annotations

import asyncio
import json
import sqlite3
import sys
from contextlib import closing
from datetime import date, timedelta

import pytest

from insider_alerts.execution.observer import COLUMNS
from insider_alerts.research import calendar_v2
from insider_alerts.research.commissioning_custody import inventory, snapshot
from insider_alerts.research.commissioning_journal import Journal, digest
from insider_alerts.research.commissioning_source import timestamp
from insider_alerts.research.commissioning_window import parse
from insider_alerts.review.queue import ensure_review_tables
from ops.research import e07_volatility_rehearsal as runtime
from ops.research.e07_volatility_approval import approval_database, read_approvals
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
    review_db = ledger.parent / "insider_alerts.db"
    ensure_review_tables(str(review_db))
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
            if mode[0] == "clock":
                raise runtime.ClockDiscontinuity("simulated clock discontinuity")
            if mode[0] == "exception":
                raise ValueError("simulated source schema exception")
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
            approval_db=review_db,
        )

    instance = make()
    instance.activate(date(2026, 10, 27), "fixture", sec_contact="fixture@example.net")
    return instance, make, clock, calls, mode


def add_case(
    instance,
    clock,
    *,
    packet="candidate",
    cik="0000000123",
    entry="2026-10-27",
    symbol="TEST",
    signal=None,
    issuer=None,
    approval=True,
    approval_symbol=None,
    approval_time=None,
    form="4",
):
    accession = f"{packet}-accession"
    packet_id = f"{accession}|{cik}|{form}"
    row = dict.fromkeys(COLUMNS) | {
        "packet_id": packet_id,
        "accession_number": accession,
        "cik": cik,
        "symbol": symbol,
        "signal_at": signal or timestamp(clock[0]),
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
    if approval:
        approved = approval_time or row["signal_at"]
        payload = {
            "issuer_cik": cik if issuer is None else issuer,
            "issuer_symbol": symbol if approval_symbol is None else approval_symbol,
            "irrelevant": "DO_NOT_RETAIN_RAW_APPROVAL",
        }
        with closing(sqlite3.connect(instance.approval_db)) as conn, conn:
            conn.execute(
                "INSERT INTO review_packets "
                "(packet_id, accession_number, cik, form_type, payload_json, "
                "created_at, updated_at)"
                " VALUES (?,?,?,?,?,?,?)",
                (packet_id, accession, cik, form, json.dumps(payload), approved, approved),
            )
            conn.execute(
                "UPDATE review_packets SET status='approve', decision_json=? WHERE packet_id=?",
                (json.dumps({"packet_id": packet_id, "decision": "approve"}), packet_id),
            )
    return row


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
    add_case(instance, clock, packet="late-candidate", symbol="LATE")
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
    assert report["daily_projections"]["issuer_linked_projection_observed"] == 1
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
    add_case(instance, clock, issuer="99999")
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
    with pytest.raises(runtime.ClockDiscontinuity, match="discontinuity"):
        clock()


def test_legacy_network_lock_prevents_dispatched_attempt(system):
    instance, make, clock, calls, _ = system
    clock[0] = parse("2026-10-27T11:00:00Z")
    add_case(instance, clock)
    lock = instance.calendar.path.parent / "network-lock.db"
    with runtime.exclusive_lock(lock):
        run(instance)
    heartbeat = json.loads((instance.folder / "heartbeat.json").read_bytes())
    assert heartbeat["acquisition_issue"] == "legacy_network_lock_busy"
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


def test_duplicate_scoped_cases_share_one_request_but_keep_both_reads(system):
    instance, make, clock, calls, _ = system
    clock[0] = parse("2026-10-27T11:00:00Z")
    add_case(instance, clock)
    add_case(instance, clock, packet="same-instrument")
    run(instance)
    clock[0] += timedelta(minutes=6)
    run(make())
    assert len(calls) == 1
    clock[0] = parse("2026-10-27T13:17:00Z")
    assert run(make())["preopen"] == {"features_observed": 2}
    clock[0] = parse("2026-10-27T22:30:00Z")
    assert run(make())["daily_projections"]["issuer_linked_projection_observed"] == 2
    assert len(calls) == 2


def test_one_bad_case_does_not_abandon_other_durable_reads(system, monkeypatch):
    instance, _, clock, _, _ = system
    clock[0] = parse("2026-10-27T11:00:00Z")
    add_case(instance, clock, packet="bad")
    add_case(instance, clock, packet="good")
    run(instance)
    actual = runtime.decision_material

    def material(case, *args, **kwargs):
        if case["row"]["packet_id"].startswith("bad-accession|"):
            raise ValueError("candidate-level schema failure")
        return actual(case, *args, **kwargs)

    monkeypatch.setattr(runtime, "decision_material", material)
    clock[0] = parse("2026-10-27T13:17:00Z")
    assert run(instance)["preopen"] == {"candidate_input_invalid": 1, "features_observed": 1}


def test_source_exception_is_a_durable_typed_response(system):
    instance, _, clock, _, mode = system
    clock[0] = parse("2026-10-27T11:00:00Z")
    add_case(instance, clock)
    mode[0] = "exception"
    report = run(instance)
    assert report["provider_result_counts"] == {"source_exception": 1}
    assert report["unanswered_requests"] == 0


def test_directory_failure_can_retry_without_blocking_bars(system):
    instance, make, clock, calls, _ = system
    clock[0] = parse("2026-10-27T11:00:00Z")
    add_case(instance, clock)

    def unavailable(_):
        raise OSError("simulated upstream failure")

    instance.directory = unavailable
    run(instance)
    assert len(calls) == 1
    clock[0] += timedelta(minutes=16)
    run(make())
    clock[0] = parse("2026-10-27T13:17:00Z")
    assert run(make())["preopen"] == {"features_observed": 1}


def test_heartbeat_checks_quiet_clock_regression_across_restart(system):
    instance, make, clock, _, _ = system
    clock[0] = parse("2026-10-27T11:00:00Z")
    run(instance)
    clock[0] -= timedelta(minutes=1)
    with pytest.raises(ValueError, match="heartbeat clock ahead"):
        make().load()


def test_daytime_and_legacy_client_contention_leave_no_requests(system):
    instance, make, clock, calls, _ = system
    clock[0] = parse("2026-10-27T11:00:00Z")
    add_case(instance, clock)
    instance.network_idle = lambda: False
    run(instance)
    assert not calls
    heartbeat = json.loads((instance.folder / "heartbeat.json").read_bytes())
    assert heartbeat["acquisition_issue"] == "legacy_client_not_idle"
    clock[0] = parse("2026-10-27T12:00:00Z")
    run(make())
    assert not calls


def test_probe_budget_stops_at_0800_new_york(system):
    instance, _, clock, calls, _ = system
    clock[0] = parse("2026-10-27T11:59:57Z")
    add_case(instance, clock)
    run(instance)
    assert 0 < calls[0][-1] <= 3


def test_late_insert_with_older_signal_remains_in_exclusions(system):
    instance, _, clock, calls, _ = system
    clock[0] = parse("2026-10-27T11:00:00Z")
    add_case(instance, clock, signal="2026-10-26T20:59:00Z")
    report = run(instance)
    assert report["custody"] == {"candidate_predates_activation": 1}
    assert not calls
    assert run(instance)["custody"] == report["custody"]


def test_empty_window_records_worker_availability_without_candidate_claim(system):
    instance, _, clock, _, _ = system
    clock[0] = parse("2026-10-27T13:17:00Z")
    report = run(instance)
    assert report["read_windows_seen"] == ["2026-10-27"]
    assert report["preopen"] == {}


def test_clock_disagreement_escapes_acquisition_and_preserves_intent(system):
    instance, _, clock, _, mode = system
    clock[0] = parse("2026-10-27T11:00:00Z")
    add_case(instance, clock)
    mode[0] = "clock"
    with pytest.raises(runtime.ClockDiscontinuity):
        run(instance)
    assert instance.load().report(instance.now())["unanswered_requests"] == 1


def test_construction_failure_reports_worker_error(system, monkeypatch):
    instance, _, _, _, _ = system

    def unavailable(_root):
        raise ValueError("deliberate construction failure")

    monkeypatch.setattr(
        runtime, "__file__", str(instance.root / "ops/research/e07_volatility_rehearsal.py")
    )
    monkeypatch.setattr(runtime, "ensure_kill_on_close_process_tree", lambda: None)
    monkeypatch.setattr(runtime, "Rehearsal", unavailable)
    monkeypatch.setattr(sys, "argv", ["rehearsal", "--cycle"])
    with pytest.raises(SystemExit) as exc:
        runtime.main()
    assert exc.value.code == 1
    report = json.loads((instance.folder / "worker-errors.log").read_bytes())
    assert report["error"] == "ValueError"


def test_owner_routed_amendment_uses_its_original_issuer_and_does_not_copy_raw_payload(system):
    instance, _, clock, _, _ = system
    clock[0] = parse("2026-10-27T11:00:00Z")
    row = add_case(instance, clock, cik="0000000999", issuer="0000000123", form="4/A")
    run(instance)
    source_before = instance.approval_db.read_bytes()
    clock[0] = parse("2026-10-27T13:17:00Z")
    assert run(instance)["preopen"] == {"features_observed": 1}
    packet_evidence = next(
        record["payload"]["issuer_approval"]
        for record in instance.journal.records()
        if record["entity"].startswith("read:")
    )
    assert packet_evidence["issuer_cik"] == 123
    assert packet_evidence["routing_cik"] == row["cik"]
    assert packet_evidence["form_type"] == "4/A"
    assert packet_evidence["read_at"] == timestamp(clock[0])
    assert len(packet_evidence["approval_sha256"]) == 64
    assert instance.approval_db.read_bytes() == source_before
    assert "DO_NOT_RETAIN_RAW_APPROVAL" not in str(instance.journal.records())


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"approval": False}, "approval_copy_missing"),
        ({"approval_symbol": "OTHER"}, "approval_symbol_mismatch"),
        ({"issuer": "0000000000"}, "approval_issuer_cik_invalid"),
        ({"issuer": "123BAD"}, "approval_issuer_cik_invalid"),
        ({"approval_time": "2026-10-27T11:01:00Z"}, "approval_copy_clock_mismatch"),
    ],
)
def test_approval_errors_leave_fixed_width_with_raw_price_receipt(system, overrides, reason):
    instance, _, clock, _, _ = system
    clock[0] = parse("2026-10-27T11:00:00Z")
    add_case(instance, clock, **overrides)
    run(instance)
    clock[0] = parse("2026-10-27T13:17:00Z")
    assert run(instance)["preopen"] == {reason: 1}
    payload = next(
        record["payload"]
        for record in instance.journal.records()
        if record["entity"].startswith("read:")
    )
    assert payload["fallback_width"] == 0.10 and len(payload["bar_digests"]) == 21
    assert "issuer_linked" not in payload


def test_approval_digest_ignores_mutable_capture_job_work_state(system):
    instance, _, clock, _, _ = system
    clock[0] = parse("2026-10-27T11:00:00Z")
    case = add_case(instance, clock)
    cutoff = parse("2026-10-27T13:18:00Z")
    source = instance.approval_db
    identity = approval_database(source)
    first = read_approvals(source, identity, [case["packet_id"]], instance.now)
    with closing(sqlite3.connect(source)) as db, db:
        db.execute("UPDATE research_capture_jobs SET attempt_count=1, status='retry'")
    clock[0] += timedelta(minutes=1)
    next_read = read_approvals(source, identity, [case["packet_id"]], instance.now)
    first_proof = first.evidence(case, cutoff)
    next_proof = next_read.evidence(case, cutoff)
    assert next_proof["read_at"] != first_proof["read_at"]
    assert next_proof["approval_sha256"] == first_proof["approval_sha256"]


def test_approval_database_replacement_fails_isolated(system):
    instance, _, clock, _, _ = system
    clock[0] = parse("2026-10-27T11:00:00Z")
    add_case(instance, clock)
    run(instance)
    another_db = instance.approval_db.with_suffix(".replacement.db")
    ensure_review_tables(str(another_db))
    instance.approval_db = another_db
    clock[0] = parse("2026-10-27T13:17:00Z")
    assert run(instance)["preopen"] == {"approval_source_replaced": 1}


def test_v2_references_unused_v1_without_rewriting_it(system):
    instance, make, _, _, _ = system
    current = instance.load()
    old = current.frozen | {"contract": runtime.PRIOR_CONTRACT}
    old_path = instance.root / runtime.PRIOR_FOLDER / "evidence.db"
    prior = Journal.create(old_path, old, clock=instance.now)
    original_bytes = old_path.read_bytes()
    fresh = make()
    fresh.folder = instance.folder.with_name("new-rehearsal-fixture")
    fresh.journal = Journal(fresh.folder / "evidence.db", clock=fresh.now)
    fresh.activate(date.fromisoformat(old["entries"][0]), "new-fixture")
    frozen = fresh.load().frozen
    assert frozen["supersedes"] == {
        "contract": runtime.PRIOR_CONTRACT,
        "activation_sha256": prior.records()[0]["sha256"],
        "journal_sha256": digest(original_bytes),
    }
    assert frozen["sec_user_agent"] == old["sec_user_agent"]
    assert old_path.read_bytes() == original_bytes


@pytest.mark.parametrize("kind", ["different_inventory", "different_calendar", "used"])
def test_v2_declines_incompatible_or_used_predecessor(system, kind):
    instance, _, _, _, _ = system
    current = instance.load()
    old = current.frozen | {"contract": runtime.PRIOR_CONTRACT}
    base = inventory(snapshot(instance.ledger, clock=instance.now))
    if kind == "different_inventory":
        old["source_inventory"] = base | {"packet_id_hashes": ["f" * 64]}
    if kind == "different_calendar":
        old["calendar_sha256"] = "e" * 64
    prior = Journal.create(
        instance.root / runtime.PRIOR_FOLDER / "evidence.db", old, clock=instance.now
    )
    if kind == "used":
        prior.append("cycle", "prior-test", {})
    with pytest.raises(ValueError, match="predecessor rehearsal was used or its frozen"):
        instance.unused_predecessor(base, current.frozen["entries"], current.version)
