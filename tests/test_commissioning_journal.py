from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import rfc8785

from insider_alerts.research.commissioning_journal import DDL, Journal, digest

NOW = datetime(2026, 10, 26, 23, tzinfo=UTC)


@pytest.fixture
def journal(tmp_path: Path) -> Journal:
    return Journal.create(tmp_path / "evidence.sqlite", {"fixture": True}, clock=lambda: NOW)


def test_chain_supersession_response_retry_and_seal(journal: Journal) -> None:
    a = journal.append("custody", "candidate", {"value": 1})
    b = journal.append("custody", "candidate", {"value": 2})
    assert b["supersedes"] == b["previous"] == a["sha256"]
    attempt = journal.append("attempt", "job", {"symbol": "SPY"})
    receipt = journal.append("response", attempt["sha256"], {"raw": "fixture"})
    assert journal.append("response", attempt["sha256"], {"raw": "fixture"}) == receipt
    with pytest.raises(ValueError, match="conflicting"):
        journal.append("response", attempt["sha256"], {"raw": "changed"})
    with pytest.raises(ValueError, match="preceding attempt"):
        journal.append("response", "not-an-attempt", {})
    seal = journal.append("seal", "singleton", {"cutoff": "fixture"})
    assert len(journal.records()) == 6 and seal["sequence"] == 6
    with pytest.raises(ValueError, match="sealed"):
        journal.append("cycle", "singleton", {})


def test_readers_and_exclusive_creation_do_not_modify_files(
    journal: Journal, tmp_path: Path
) -> None:
    before = journal.path.read_bytes()
    journal.records()
    assert journal.path.read_bytes() == before
    with pytest.raises(FileExistsError):
        Journal.create(journal.path, {}, clock=lambda: NOW)
    absent = tmp_path / "missing.sqlite"
    with pytest.raises(FileNotFoundError):
        Journal(absent).records()
    assert not absent.exists()
    with closing(sqlite3.connect(absent)) as conn:
        conn.execute("CREATE TABLE user_data(value)")
    foreign = absent.read_bytes()
    with pytest.raises(ValueError, match="foreign"):
        Journal(absent).append("cycle", "singleton", {})
    assert absent.read_bytes() == foreign


def test_clock_reversal_rolls_back(journal: Journal) -> None:
    journal.clock = lambda: NOW - timedelta(seconds=1)
    with pytest.raises(ValueError, match="clock"):
        journal.append("cycle", "singleton", {})
    assert len(journal.records()) == 1


def test_concurrent_appends_have_one_chain(journal: Journal) -> None:
    def add(index: int) -> None:
        Journal(journal.path, clock=lambda: NOW).append("custody", str(index), {"index": index})

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(add, range(12)))
    rows = journal.records()
    assert [r["sequence"] for r in rows] == list(range(1, 14))


@pytest.mark.parametrize(
    "defect", ["digest", "rehashed_previous", "boolean_sequence", "missing_trigger"]
)
def test_corruption_is_rejected(journal: Journal, defect: str) -> None:
    if defect != "boolean_sequence":
        journal.append("custody", "candidate", {"fixture": True})
    sequence = 1 if defect == "boolean_sequence" else 2
    with closing(sqlite3.connect(journal.path)) as conn, conn:
        conn.execute("DROP TRIGGER records_no_update")
        raw = conn.execute("SELECT envelope FROM records WHERE sequence=?", (sequence,)).fetchone()[
            0
        ]
        record = json.loads(raw)
        if defect == "digest":
            conn.execute("UPDATE records SET envelope=X'7B7D' WHERE sequence=2")
        elif defect != "missing_trigger":
            if defect == "rehashed_previous":
                record["previous"] = "0" * 64
            else:
                record["sequence"] = True
            altered = rfc8785.dumps(record)
            conn.execute(
                "UPDATE records SET envelope=?,sha256=? WHERE sequence=?",
                (altered, digest(altered), sequence),
            )
        if defect != "missing_trigger":
            conn.execute(DDL[1])
    with pytest.raises(ValueError):
        journal.records()


def test_write_triggers_reject_mutation(journal: Journal) -> None:
    with (
        closing(journal.connect(write=True)) as conn,
        pytest.raises(sqlite3.IntegrityError, match="append-only"),
    ):
        conn.execute("DELETE FROM records")


def test_publication_failure_cleans_owned_staging(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import insider_alerts.research.commissioning_journal as module

    def fail(*args: object) -> None:
        raise OSError("injected publication failure")

    monkeypatch.setattr(module.os, "rename" if module.os.name == "nt" else "link", fail)
    with pytest.raises(OSError, match="injected"):
        Journal.create(tmp_path / "never.sqlite", {}, clock=lambda: NOW)
    assert list(tmp_path.iterdir()) == []
