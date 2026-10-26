from __future__ import annotations

import hashlib
import sqlite3
from contextlib import closing
from datetime import timedelta

import pytest

from insider_alerts.execution.observer import COLUMNS
from insider_alerts.research.commissioning_custody import capture, inventory, snapshot
from insider_alerts.research.commissioning_journal import Journal
from insider_alerts.research.commissioning_window import parse
from tests.test_commissioning_window import CREATED, VERSION, frozen


@pytest.fixture
def setup(tmp_path):
    clock = [CREATED]
    source = tmp_path / "canary.db"
    with closing(sqlite3.connect(source)) as conn, conn:
        conn.execute(f"CREATE TABLE candidates({','.join(COLUMNS)}, outcome, secret)")
        conn.execute("CREATE TABLE metadata(key,value)")
        conn.executemany(
            "INSERT INTO metadata VALUES(?,?)",
            [
                ("runtime_started_utc", "2026-10-27T12:30:00Z"),
                ("runtime_source_fingerprint", "f" * 64),
                ("account", "DO_NOT_CAPTURE"),
            ],
        )
        values = {name: None for name in COLUMNS} | {
            "packet_id": "packet-1",
            "symbol": "TEST",
            "signal_at": "2026-10-27T12:00:00Z",
            "created_at": "2026-10-27T12:01:00Z",
            "entry_session": "2026-10-27",
            "eligible": 1,
        }
        conn.execute(
            f"INSERT INTO candidates VALUES({','.join('?' for _ in range(len(COLUMNS) + 2))})",
            [*(values[name] for name in COLUMNS), "DO_NOT_CAPTURE", "DO_NOT_CAPTURE"],
        )
    baseline = inventory(snapshot(source, clock=lambda: CREATED))
    baseline["packet_id_hashes"] = []  # Model this packet arriving after activation.
    journal = Journal.create(
        tmp_path / "journal.db", frozen(inventory=baseline), clock=lambda: clock[0]
    )
    clock[0] = parse("2026-10-27T13:00:00Z")
    return source, journal, clock


def test_snapshot_readonly_projection_and_context_is_not_row_producer(setup):
    source, journal, clock = setup
    original = hashlib.sha256(source.read_bytes()).hexdigest()
    observed = snapshot(source, clock=lambda: clock[0])
    assert "DO_NOT_CAPTURE" not in str(observed)
    assert set(observed["rows"][0]) == set(COLUMNS)
    assert capture(journal, VERSION, observed) == 1
    assert original == hashlib.sha256(source.read_bytes()).hexdigest()
    row = journal.records()[1]["payload"]
    assert row["producer_attribution"] == "unknown"
    assert row["first_observed_at"] == "2026-10-27T13:00:00.000000Z"
    assert row["admission"]["observed_before_decision"]


def test_revisions_do_not_backdate_corrected_features_or_rewindow_membership(setup):
    source, journal, clock = setup
    capture(journal, VERSION, snapshot(source, clock=lambda: clock[0]))
    clock[0] = parse("2026-10-27T14:00:00Z")
    with closing(sqlite3.connect(source)) as conn, conn:
        conn.execute("UPDATE candidates SET eligible=0,signal_at='2026-10-27T13:30:00Z'")
    assert capture(journal, VERSION, snapshot(source, clock=lambda: clock[0])) == 1
    records = [r for r in journal.records() if r["kind"] == "custody"]
    assert records[1]["supersedes"] == records[0]["sha256"]
    revised = records[1]["payload"]
    assert revised["admission"] == records[0]["payload"]["admission"]
    assert revised["admission"]["entry_session"] == "2026-10-27"
    assert revised["revision_admission"]["entry_session"] == "2026-10-28"
    assert revised["source_observed_at"] > revised["first_observed_at"]
    assert revised["source_fields_changed"]


def test_no_duplicate_custody_and_compact_five_minute_poll_receipts(setup):
    source, journal, clock = setup
    capture(journal, VERSION, snapshot(source, clock=lambda: clock[0]))
    count = len(journal.records())
    for _ in range(4):
        clock[0] += timedelta(minutes=1)
        assert capture(journal, VERSION, snapshot(source, clock=lambda: clock[0])) == 0
        assert len(journal.records()) == count
    clock[0] += timedelta(minutes=1)
    capture(journal, VERSION, snapshot(source, clock=lambda: clock[0]))
    assert len(journal.records()) == count + 1


def test_missing_source_never_created_and_reversed_clock_rejected(tmp_path, setup):
    with pytest.raises(FileNotFoundError):
        snapshot(tmp_path / "missing.db")
    assert not (tmp_path / "missing.db").exists()
    source, _, clock = setup
    times = iter([clock[0], clock[0] - timedelta(seconds=1)])
    with pytest.raises(ValueError, match="reversed"):
        snapshot(source, clock=lambda: next(times))


def test_future_snapshot_fails_and_preboundary_observation_is_preexisting(setup):
    source, journal, clock = setup
    original = journal.path.read_bytes()
    observation = snapshot(source, clock=lambda: clock[0] + timedelta(seconds=1))
    with pytest.raises(ValueError, match="observation clocks"):
        capture(journal, VERSION, observation)
    assert journal.path.read_bytes() == original
    observation = snapshot(source, clock=lambda: CREATED)
    assert capture(journal, VERSION, observation) == 1
    assert journal.records()[1]["payload"]["admission"]["membership"] == "preexisting"


@pytest.mark.parametrize("value", [float("inf"), -(2**63), b"malformed"])
def test_invalid_scalar_is_explicit_and_does_not_lose_candidate(setup, value):
    source, journal, clock = setup
    with closing(sqlite3.connect(source)) as conn, conn:
        conn.execute("UPDATE candidates SET prior_close=?", (value,))
    assert capture(journal, VERSION, snapshot(source, clock=lambda: clock[0])) == 1
    raw = journal.records()[1]["payload"]["row"]["prior_close"]
    assert raw["validation_error"]
    assert journal.records()[1]["payload"]["admission"]["membership"] == "admitted"


def test_inventory_excludes_existing_malformed_signals_from_fresh_unknown_count(tmp_path, setup):
    source, _, clock = setup
    with closing(sqlite3.connect(source)) as conn, conn:
        conn.execute("UPDATE candidates SET signal_at='malformed historical clock'")
    baseline = inventory(snapshot(source, clock=lambda: CREATED))
    journal = Journal.create(
        tmp_path / "fresh.db", frozen(inventory=baseline), clock=lambda: CREATED
    )
    journal.clock = lambda: clock[0]
    assert capture(journal, VERSION, snapshot(source, clock=lambda: clock[0])) == 0
    assert journal.records()[-1]["payload"]["membership_counts"] == {"preexisting": 1}


def test_replaced_source_file_is_not_silently_accepted(tmp_path, setup):
    source, journal, clock = setup
    replacement = tmp_path / "replacement.db"
    replacement.write_bytes(source.read_bytes())
    replacement.replace(source)
    with pytest.raises(ValueError, match="source ledger identity"):
        capture(journal, VERSION, snapshot(source, clock=lambda: clock[0]))


def test_outside_row_cannot_enter_window_through_later_revision(setup):
    source, journal, clock = setup
    with closing(sqlite3.connect(source)) as conn, conn:
        conn.execute("UPDATE candidates SET signal_at='2026-10-27T09:00:00Z'")
    assert capture(journal, VERSION, snapshot(source, clock=lambda: clock[0])) == 1
    first = journal.records()[1]
    assert first["payload"]["admission"]["membership"] == "outside"
    clock[0] += timedelta(minutes=1)
    with closing(sqlite3.connect(source)) as conn, conn:
        conn.execute("UPDATE candidates SET signal_at='2026-10-27T12:00:00Z'")
    assert capture(journal, VERSION, snapshot(source, clock=lambda: clock[0])) == 1
    current = next(r for r in reversed(journal.records()) if r["kind"] == "custody")
    assert current["supersedes"] == first["sha256"]
    assert current["payload"]["admission"]["membership"] == "outside"
    assert current["payload"]["revision_admission"]["membership"] == "admitted"
    assert current["payload"]["source_fields_changed"]
