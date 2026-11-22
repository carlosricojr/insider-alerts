from __future__ import annotations

import hashlib
from datetime import date

import pytest
import rfc8785

from e07_volatility_exits.audit_commissioning import _history, audit_archive
from insider_alerts.research.commissioning_journal import Journal
from insider_alerts.research.commissioning_window import parse
from tests.test_commissioning_planner import add_candidate, evidence, job_request, respond
from tests.test_commissioning_window import CREATED, VERSION, frozen


def test_archive_qualification_is_blinded_complete_and_read_only(tmp_path):
    clock = [CREATED]
    path = tmp_path / "archived.db"
    journal = Journal.create(path, frozen(), clock=lambda: clock[0])
    clock[0] = parse("2026-10-27T11:30:00Z")
    for index, symbol in enumerate(("GOOD", "LATE", "PART", "NONE")):
        add_candidate(journal, clock, str(index), symbol, signal="2026-10-27T11:00:00Z")
    _, jobs, _ = evidence(journal, clock)
    initial = {
        job.symbol: job for job in jobs if job.owner.startswith("case:") and job.stage == "R1"
    }
    expected, finalization = _history(VERSION, date(2026, 10, 27))
    assert len(expected) == 21 and clock[0] > finalization

    request = job_request(initial["GOOD"], clock)
    complete = {"required_dates": sorted(expected), "start_date": min(expected)}
    respond(journal, clock, request | complete)
    request = job_request(initial["PART"], clock)
    respond(journal, clock, request | complete | {"required_dates": sorted(expected)[1:]})

    clock[0] = parse("2026-10-27T22:00:00Z")
    respond(journal, clock, job_request(initial["LATE"], clock) | complete)

    horizon = next(job for job in jobs if job.owner == initial["NONE"].owner and job.stage == "R2")
    unrelated = journal.append("attempt", "network", job_request(horizon, clock))
    # A terminal payload cannot enter the archive coverage calculation.
    journal.append("response", unrelated["sha256"], {"sentinel": "not an R1 response"})

    before = path.read_bytes()
    records = journal.records()
    result = audit_archive(records, [VERSION])
    assert result["status"] == "descriptive_only"
    assert result["scientific_reuse"] == "prohibited"
    assert result["membership_counts"] == {"admitted": 4}
    assert result["admitted_receipt_counts"] == {
        "response_and_case_recorded_before_cutoff": 1,
        "first_complete_response_recorded_after_cutoff": 1,
        "no_usable_complete_response": 1,
        "no_response": 1,
    }
    assert result["watermark_sha256"] == records[-1]["sha256"]
    sha = result.pop("audit_sha256")
    assert hashlib.sha256(rfc8785.dumps(result)).hexdigest() == sha
    assert path.read_bytes() == before


def test_archive_qualification_requires_the_frozen_calendar(tmp_path):
    journal = Journal.create(tmp_path / "archive.db", frozen(), clock=lambda: CREATED)
    with pytest.raises(ValueError, match="activation calendar is absent"):
        audit_archive(journal.records(), [])
