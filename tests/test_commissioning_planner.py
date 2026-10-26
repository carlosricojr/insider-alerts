from __future__ import annotations

import copy
from datetime import timedelta

import pytest

from insider_alerts.execution.observer import COLUMNS
from insider_alerts.research.commissioning_custody import capture
from insider_alerts.research.commissioning_journal import Journal
from insider_alerts.research.commissioning_planner import Evidence
from insider_alerts.research.commissioning_source import timestamp
from insider_alerts.research.commissioning_window import parse
from tests.test_commissioning_receipts import receipts
from tests.test_commissioning_window import CREATED, VERSION, frozen


@pytest.fixture
def setup(tmp_path):
    clock = [CREATED]
    journal = Journal.create(tmp_path / "commission.db", frozen(), clock=lambda: clock[0])
    clock[0] = parse("2026-10-27T13:00:00Z")
    add_candidate(journal, clock, "first", "TEST")
    clock[0] = parse("2026-10-27T22:00:00Z")
    return journal, clock


def add_candidate(journal, clock, packet, symbol, *, signal="2026-10-27T12:00:00Z"):
    row = {name: None for name in COLUMNS} | {
        "packet_id": packet,
        "symbol": symbol,
        "signal_at": signal,
        "entry_session": "2026-10-27",
        "created_at": signal,
    }
    capture(
        journal,
        VERSION,
        {
            "rows": [row],
            "metadata": {},
            "source_path": "synthetic",
            "source_device": "1",
            "source_inode": "1",
            "read_started_at": timestamp(clock[0]),
            "observed_at": timestamp(clock[0]),
        },
    )


def evidence(journal, clock):
    result = Evidence(journal.records(), VERSION, clock[0])
    jobs, report = result.jobs_and_report()
    return result, jobs, report


def job_request(job, clock):
    return {
        "symbol": job.symbol,
        "expected_conid": job.conid,
        "schedule": job.schedule,
        "start_date": min(job.dates),
        "end_at": timestamp(clock[0]),
        "jobs": [job.key],
        "required_dates": list(job.dates),
        "planned_at": timestamp(clock[0]),
    }


def respond(journal, clock, request, *, conid=123, changed=False, failure=None):
    attempt = journal.append("attempt", "network", request)
    _, response = receipts(schedule=request["schedule"])
    source = response["payload"]
    source.update(symbol=request["symbol"])
    for key in ("started_at", "client_dispatch_at", "response_at", "finished_at"):
        source[key] = timestamp(clock[0])
    source["identity"].update(
        conid=conid, symbol=request["symbol"], observed_at=timestamp(clock[0])
    )
    source["request"].update(start_date=request["start_date"], end_at=request["end_at"])
    source["response"] = [
        {
            "date": day,
            "open": "10.0",
            "high": "11.0",
            "low": "9.0",
            "close": "10.6" if changed else "10.5",
            "volume": "100",
        }
        for day in request["required_dates"]
    ]
    if failure:
        source.update(result=failure, response=None, response_at=None)
    journal.append("response", attempt["sha256"], source)
    return attempt


def candidate_job(jobs, stage="R1"):
    return next(job for job in jobs if job.owner.startswith("case:") and job.stage == stage)


def test_event_driven_history_horizon_and_later_session_repeat(setup):
    journal, clock = setup
    state, jobs, report = evidence(journal, clock)
    assert report["admitted_cases"] == 1 and not report["strategy_trial_ready"]
    assert candidate_job(jobs).conid is None
    respond(journal, clock, job_request(candidate_job(jobs), clock))
    state, jobs, report = evidence(journal, clock)
    assert report["cases"][0]["R1_complete"]
    assert not report["cases"][0]["R1_received_before_decision"]
    assert candidate_job(jobs).conid == 123
    assert state.success(candidate_job(jobs))
    assert not any(job.stage == "R3" for job in jobs)
    assert candidate_job(jobs, "R2").ready == parse("2026-11-09T21:00:00Z")
    clock[0] = parse("2026-11-09T23:00:00Z")
    _, jobs, _ = evidence(journal, clock)
    respond(journal, clock, job_request(candidate_job(jobs, "R2"), clock))
    _, jobs, report = evidence(journal, clock)
    assert candidate_job(jobs, "R3").ready == parse("2026-11-10T21:00:00Z")
    clock[0] = parse("2026-11-10T23:00:00Z")
    respond(journal, clock, job_request(candidate_job(jobs, "R3"), clock))
    _, _, report = evidence(journal, clock)
    assert report["cases"][0]["collection_complete"]
    assert report["report_state"] == "interim" and not report["strategy_trial_ready"]


def test_stability_revision_cannot_be_retried_until_a_matching_pair_appears(setup):
    journal, clock = setup
    clock[0] = parse("2026-11-09T23:00:00Z")
    _, jobs, _ = evidence(journal, clock)
    respond(journal, clock, job_request(candidate_job(jobs, "R2"), clock))
    clock[0] = parse("2026-11-10T23:00:00Z")
    _, jobs, _ = evidence(journal, clock)
    repeat = candidate_job(jobs, "R3")
    respond(journal, clock, job_request(repeat, clock), changed=True)
    state, jobs, report = evidence(journal, clock)
    assert report["cases"][0]["R3_complete"] and not report["cases"][0]["projection_stable"]
    first_repeat = report["cases"][0]["receipt_bindings"]["R3"]
    clock[0] += timedelta(minutes=10)
    respond(journal, clock, job_request(repeat, clock), changed=False)
    state, jobs, report = evidence(journal, clock)
    assert not report["cases"][0]["projection_stable"]
    assert report["cases"][0]["receipt_bindings"]["R3"] == first_repeat
    assert state.next_request([repeat]) is None


def test_initial_qualification_pins_case_even_if_first_history_times_out(setup):
    journal, clock = setup
    _, jobs, _ = evidence(journal, clock)
    respond(journal, clock, job_request(candidate_job(jobs), clock), failure="timeout")
    clock[0] += timedelta(minutes=5)
    state, jobs, report = evidence(journal, clock)
    assert candidate_job(jobs).conid == 123
    assert not report["cases"][0]["R1_complete"]
    request = state.next_request([candidate_job(jobs)])
    assert request["expected_conid"] == 123


def test_reused_symbol_new_candidate_is_not_pinned_to_older_candidate(setup):
    journal, clock = setup
    _, jobs, _ = evidence(journal, clock)
    first = candidate_job(jobs)
    respond(journal, clock, job_request(first, clock), conid=123)
    clock[0] += timedelta(minutes=5)
    add_candidate(journal, clock, "second", "TEST")
    state, jobs, _ = evidence(journal, clock)
    initial = [job for job in jobs if job.owner.startswith("case:") and job.stage == "R1"]
    assert {job.conid for job in initial} == {None, 123}
    new_job = next(job for job in initial if job.conid is None)
    request = state.next_request([new_job])
    respond(journal, clock, request, conid=456)
    _, jobs, _ = evidence(journal, clock)
    assert {job.conid for job in jobs if job.owner.startswith("case:")} == {123, 456}


def test_durable_pacing_crash_backoff_does_not_exhaust_jobs(setup):
    journal, clock = setup
    _, jobs, _ = evidence(journal, clock)
    original = candidate_job(jobs)
    for index in range(6):
        state, _, _ = evidence(journal, clock)
        request = state.next_request([original])
        assert request is not None
        journal.append("attempt", "network", request)  # Crash before any response.
        clock[0] += timedelta(seconds=299)
        state, _, report = evidence(journal, clock)
        assert state.next_request([original]) is None
        assert report["attempts_without_response"] == index + 1
        clock[0] += timedelta(seconds=max(300, 300 * 2**index) - 299)
    state, _, _ = evidence(journal, clock)
    request = state.next_request([original])
    assert request is not None
    respond(journal, clock, request)
    _, _, report = evidence(journal, clock)
    assert report["cases"][0]["R1_complete"]


def test_merge_due_ranges_but_not_incompatible_pinned_contracts(setup):
    journal, clock = setup
    add_candidate(journal, clock, "second", "TEST")
    state, jobs, _ = evidence(journal, clock)
    initial = [job for job in jobs if job.owner.startswith("case:") and job.stage == "R1"]
    request = state.next_request(initial)
    assert len(request["jobs"]) == 2 and len(request["required_dates"]) == 20
    respond(journal, clock, request)
    _, _, report = evidence(journal, clock)
    assert all(case["R1_complete"] for case in report["cases"])


@pytest.mark.parametrize(
    "when", ["2026-10-27T12:00:00Z", "2026-10-27T21:59:59Z", "2026-11-26T13:00:00Z"]
)
def test_daytime_and_fixed_recovery_deadline_never_dispatch(setup, when):
    journal, clock = setup
    # Snapshot predates all cases except first test, which uses a separate later date.
    if when == "2026-10-27T12:00:00Z":
        when = "2026-10-28T12:00:00Z"
    clock[0] = parse(when)
    state, jobs, _ = evidence(journal, clock)
    assert state.next_request(jobs) is None


def test_invalid_symbols_and_unassignable_source_records_are_visible(setup):
    journal, clock = setup
    add_candidate(journal, clock, "invalid", "?")
    add_candidate(journal, clock, "unknown", "TEST", signal="bad")
    state, jobs, report = evidence(journal, clock)
    assert report["admitted_cases"] == 2 and report["unassignable_records"] == 1
    assert len([job for job in jobs if job.owner.startswith("case:")]) == 2
    assert report["admission_counts"]["invalid_symbol"] == 1
    assert not report["strategy_trial_ready"]


def test_benchmark_reservation_does_not_starve_candidate_work(setup):
    journal, clock = setup
    state, jobs, _ = evidence(journal, clock)
    request = state.next_request(jobs)
    assert request["jobs"][0].startswith("benchmark:")
    # A failed benchmark request still consumes its reservation.
    respond(journal, clock, request, failure="timeout")
    clock[0] += timedelta(minutes=5)
    state, jobs, _ = evidence(journal, clock)
    request = state.next_request(jobs)
    assert all(key.startswith("case:") for key in request["jobs"])


def test_future_journal_clock_and_altered_manifest_fail_closed(setup):
    journal, clock = setup
    with pytest.raises(ValueError, match="current clock"):
        Evidence(journal.records(), VERSION, CREATED)
    records = copy.deepcopy(journal.records())
    records[0]["payload"]["maximum_attempts_per_job"] = 7
    with pytest.raises(ValueError, match="frozen derivation"):
        Evidence(records, VERSION, clock[0])


def test_reachable_decision_deadline_precedes_expired_backlog(setup):
    journal, clock = setup
    clock[0] = parse("2026-10-28T11:00:00Z")
    add_candidate(journal, clock, "new", "NEW", signal="2026-10-28T10:30:00Z")
    state, jobs, _ = evidence(journal, clock)
    initial = [job for job in jobs if job.owner.startswith("case:") and job.stage == "R1"]
    assert len(initial) == 2
    assert state.next_request(initial)["symbol"] == "NEW"
