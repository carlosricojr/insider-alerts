from __future__ import annotations

import ast
import contextlib
import hashlib
import json
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import rfc8785

from insider_alerts.research import calendar_v2 as cal

ROOT = Path(__file__).resolve().parents[1]
PDF = ROOT / "docs/research/calendar-sources/nyse-holidays-2026-2028.pdf"
OBSERVED = datetime(2026, 10, 26, 20, tzinfo=UTC)


@pytest.fixture(autouse=True)
def clock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cal, "_now", lambda: OBSERVED + timedelta(days=1))


@pytest.fixture
def journal(tmp_path: Path) -> cal.CalendarJournal:
    path = tmp_path / "journal.sqlite"
    cal.create_journal(
        path,
        baseline_pdf=PDF.read_bytes(),
        review_reference="synthetic-reviewed-fixture",
        observed_at=OBSERVED,
    )
    return cal.CalendarJournal(path)


def notice(kind: cal.NoticeKind = "closed", day: date = date(2026, 11, 2)) -> cal.Notice:
    return cal.Notice(
        kind,
        (day,),
        "synthetic emergency notice",
        b"synthetic-not-real-notice",
        "https://www.nyse.com/synthetic-test",
        date(2026, 10, 26),
        "fixture-review",
    )


def test_archived_source_bytes() -> None:
    assert hashlib.sha256(PDF.read_bytes()).hexdigest() == (
        "76775c27848d19dc15d7cac8927f6e85f63605aa715fe5401ed486aa722e1d23"
    )


@pytest.mark.parametrize(
    "value",
    [
        "2026-01-01",
        "2026-01-19",
        "2026-02-16",
        "2026-04-03",
        "2026-05-25",
        "2026-06-19",
        "2026-07-03",
        "2026-09-07",
        "2026-11-26",
        "2026-12-25",
        "2027-01-01",
        "2027-01-18",
        "2027-02-15",
        "2027-03-26",
        "2027-05-31",
        "2027-06-18",
        "2027-07-05",
        "2027-09-06",
        "2027-11-25",
        "2027-12-24",
        "2028-01-17",
        "2028-02-21",
        "2028-04-14",
        "2028-05-29",
        "2028-06-19",
        "2028-07-04",
        "2028-09-04",
        "2028-11-23",
        "2028-12-25",
    ],
)
def test_separately_transcribed_published_closures(
    journal: cal.CalendarJournal, value: str
) -> None:
    day = journal.versions()[0].day(date.fromisoformat(value))
    assert day.state == "closed" and day.opens_at is None and day.closes_at is None


@pytest.mark.parametrize(
    "value",
    [
        "2026-11-27",
        "2026-12-24",
        "2027-11-26",
        "2028-07-03",
        "2028-11-24",
    ],
)
def test_published_early_closes(journal: cal.CalendarJournal, value: str) -> None:
    day = journal.versions()[0].day(date.fromisoformat(value))
    assert day.closes_at is not None and day.closes_at.astimezone(cal.NY).hour == 13


@pytest.mark.parametrize("value", ["2026-07-02", "2027-12-31", "2028-02-29", "2028-12-22"])
def test_no_invented_holidays_or_early_closes(journal: cal.CalendarJournal, value: str) -> None:
    day = journal.versions()[0].day(date.fromisoformat(value))
    assert day.state == "open"
    assert day.closes_at is not None and day.closes_at.astimezone(cal.NY).hour == 16


@pytest.mark.parametrize(
    "value,hour",
    [
        ("2026-03-06", 14),
        ("2026-03-09", 13),
        ("2026-10-30", 13),
        ("2026-11-02", 14),
        ("2027-03-12", 14),
        ("2027-03-15", 13),
        ("2027-11-05", 13),
        ("2027-11-08", 14),
        ("2028-03-10", 14),
        ("2028-03-13", 13),
        ("2028-11-03", 13),
        ("2028-11-06", 14),
    ],
)
def test_dst_boundaries(journal: cal.CalendarJournal, value: str, hour: int) -> None:
    day = journal.versions()[0].day(date.fromisoformat(value))
    assert day.opens_at is not None and day.opens_at.astimezone(UTC).hour == hour


def test_explicit_daily_coverage_and_unknown_year(journal: cal.CalendarJournal) -> None:
    version = journal.versions()[0]
    assert len(version.days) == 1096
    assert version.day(date(2026, 10, 31)).state == "closed"
    assert version.day(date(2029, 1, 2)).state == "unknown"
    assert version.day(date(2025, 12, 31)).state == "unknown"


def test_tenth_session_and_year_rollover(journal: cal.CalendarJournal) -> None:
    version = journal.versions()[0]
    binding = cal.bind(version, date(2026, 8, 31))
    assert len(binding.sessions) == 10
    assert binding.sessions[-1].day == date(2026, 9, 14)
    assert cal.bind(version, date(2026, 12, 24), 6).sessions[-1].day == date(2027, 1, 4)
    with pytest.raises(cal.CalendarError, match="unknown"):
        cal.bind(version, date(2028, 12, 29), 10)
    with pytest.raises(cal.CalendarError, match="entry"):
        cal.bind(version, date(2026, 9, 7))
    with pytest.raises(cal.CalendarError, match="count"):
        cal.bind(version, date(2026, 9, 8), True)


def test_as_of_uses_observation_not_publication(journal: cal.CalendarJournal) -> None:
    assert journal.as_of(OBSERVED - timedelta(microseconds=1)) is None
    assert journal.as_of(OBSERVED) == journal.versions()[0]
    with pytest.raises(cal.CalendarError, match="naive"):
        journal.as_of(datetime(2026, 10, 26))


@pytest.mark.parametrize("kind", ["closed", "unknown"])
def test_notice_preserves_binding_and_history(
    journal: cal.CalendarJournal, kind: cal.NoticeKind
) -> None:
    old = journal.versions()[0]
    binding = cal.bind(old, date(2026, 10, 26))
    updated = journal.append_notice(
        notice(kind), expected_parent=old.digest, observed_at=OBSERVED + timedelta(seconds=1)
    )
    assert updated.parent == old.digest
    assert journal.as_of(OBSERVED) == old
    assert binding == cal.bind(journal.versions()[0], date(2026, 10, 26))
    assert cal.binding_conflicts(binding, updated, bound_version=old) == (date(2026, 11, 2),)
    assert updated.day(date(2026, 11, 2)).state == kind
    if kind == "unknown":
        with pytest.raises(cal.CalendarError, match="unknown"):
            cal.bind(updated, date(2026, 10, 26))


def test_dates_only_coverage_does_not_make_missing_bars_zero(journal: cal.CalendarJournal) -> None:
    binding = cal.bind(journal.versions()[0], date(2026, 8, 31))
    dates = frozenset(item.day for item in binding.sessions)
    assert cal.missing_bar_dates(binding, dates) == ()
    assert cal.missing_bar_dates(binding, dates - {date(2026, 9, 14)}) == (date(2026, 9, 14),)


def test_unknown_on_skipped_closed_date_conflicts(journal: cal.CalendarJournal) -> None:
    old = journal.versions()[0]
    binding = cal.bind(old, date(2026, 11, 23))
    holiday = date(2026, 11, 26)
    assert holiday not in {item.day for item in binding.sessions}
    updated = journal.append_notice(
        notice("unknown", holiday),
        expected_parent=old.digest,
        observed_at=OBSERVED + timedelta(seconds=1),
    )
    assert cal.binding_conflicts(binding, updated, bound_version=old) == (holiday,)
    with pytest.raises(cal.CalendarError, match="unknown"):
        cal.bind(updated, binding.anchor)


def test_replay_idempotence_and_concurrent_parent_check(journal: cal.CalendarJournal) -> None:
    old = journal.versions()[0]
    new = journal.append_notice(
        notice(), expected_parent=old.digest, observed_at=OBSERVED + timedelta(seconds=1)
    )
    assert (
        journal.append_notice(
            notice(), expected_parent=old.digest, observed_at=OBSERVED + timedelta(seconds=2)
        )
        == new
    )
    assert len(journal.versions()) == 2

    def append(day: int) -> str:
        try:
            journal.append_notice(
                notice(day=date(2026, 11, day)),
                expected_parent=new.digest,
                observed_at=OBSERVED + timedelta(seconds=3),
            )
        except cal.CalendarError as exc:
            return str(exc)
        return "success"

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(append, [3, 4]))
    assert sorted(results) == ["expected parent changed", "success"]
    assert len(journal.versions()) == 3


def test_notice_validation_and_transaction_rollback(journal: cal.CalendarJournal) -> None:
    old = journal.versions()[0]
    with pytest.raises(cal.CalendarError, match="clock"):
        journal.append_notice(notice(), expected_parent=old.digest, observed_at=OBSERVED)
    with pytest.raises(cal.CalendarError, match="nothing"):
        journal.append_notice(
            notice(day=date(2026, 11, 26)),
            expected_parent=old.digest,
            observed_at=OBSERVED + timedelta(seconds=1),
        )
    bad = cal.Notice(
        "closed",
        (date(2026, 11, 2),),
        "future",
        b"future",
        "https://www.nyse.com/test",
        date(2027, 1, 1),
        "fixture",
    )
    with pytest.raises(cal.CalendarError, match="publication"):
        journal.append_notice(
            bad, expected_parent=old.digest, observed_at=OBSERVED + timedelta(seconds=1)
        )
    assert journal.versions() == (old,)
    with contextlib.closing(sqlite3.connect(journal.path)) as conn, conn:
        assert conn.execute("SELECT count(*) FROM calendar_sources").fetchone()[0] == 1


@pytest.mark.parametrize(
    "url",
    [
        "http://www.nyse.com/test",
        "https://evil.example/test",
        "https://user:password@www.nyse.com/test",
        "https://www.nyse.com/test?token=secret",
        "https://www.nyse.com/test#fragment",
        "https://www.nyse.com:8443/test",
        "https://s2.q4cdn.com/another-company/notice.pdf",
    ],
)
def test_invalid_source_metadata(url: str) -> None:
    value = notice()
    with pytest.raises(cal.CalendarError, match="source"):
        cal.Notice(
            value.kind,
            value.dates,
            value.reason,
            value.source_bytes,
            url,
            value.published_on,
            value.review_reference,
        ).material()


def test_no_implicit_creation_and_foreign_database_untouched(tmp_path: Path) -> None:
    path = tmp_path / "absent.sqlite"
    with pytest.raises(FileNotFoundError):
        cal.CalendarJournal(path).versions()
    assert not path.exists()
    with contextlib.closing(sqlite3.connect(path)) as conn, conn:
        conn.execute("CREATE TABLE important(data)")
    original = path.read_bytes()
    with pytest.raises(cal.CalendarError, match="foreign"):
        cal.CalendarJournal(path).versions()
    with pytest.raises(FileExistsError):
        cal.create_journal(
            path, baseline_pdf=PDF.read_bytes(), review_reference="fixture", observed_at=OBSERVED
        )
    assert path.read_bytes() == original


def test_creation_bad_hash_and_publication_are_not_published(tmp_path: Path) -> None:
    path = tmp_path / "absent.sqlite"
    with pytest.raises(cal.CalendarError, match="PDF"):
        cal.create_journal(
            path, baseline_pdf=b"wrong", review_reference="fixture", observed_at=OBSERVED
        )
    assert not path.exists()
    with pytest.raises(cal.CalendarError, match="publication"):
        cal.create_journal(
            path,
            baseline_pdf=PDF.read_bytes(),
            review_reference="fixture",
            observed_at=datetime(2025, 1, 1, tzinfo=UTC),
        )
    assert list(tmp_path.iterdir()) == []


def test_schema_triggers_and_raw_integrity(journal: cal.CalendarJournal) -> None:
    with contextlib.closing(sqlite3.connect(journal.path)) as conn, conn:
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            conn.execute("DELETE FROM calendar_versions")
        conn.execute("DROP TRIGGER calendar_versions_no_update")
    with pytest.raises(cal.CalendarError, match="schema"):
        journal.versions()
    with contextlib.closing(sqlite3.connect(journal.path)) as conn, conn:
        conn.execute("UPDATE calendar_versions SET payload=X'7B7D'")
        conn.execute(next(sql for sql in cal.TRIGGERS if "calendar_versions_no_update" in sql))
    with pytest.raises(cal.CalendarError, match="digest"):
        journal.versions()


def test_read_only_access_leaves_file_unchanged(journal: cal.CalendarJournal) -> None:
    original = journal.path.read_bytes()
    journal.versions()
    assert journal.path.read_bytes() == original
    assert not journal.path.with_name(journal.path.name + "-wal").exists()


def test_creation_publication_failure_cleans_staging(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail(source: object, destination: object) -> None:
        raise OSError("injected publication failure")

    monkeypatch.setattr(cal.os, "link", fail)
    with pytest.raises(OSError, match="injected"):
        cal.create_journal(
            tmp_path / "new.sqlite",
            baseline_pdf=PDF.read_bytes(),
            review_reference="fixture",
            observed_at=OBSERVED,
        )
    assert list(tmp_path.iterdir()) == []


def test_future_observation_cannot_poison_journal(
    journal: cal.CalendarJournal, tmp_path: Path
) -> None:
    future = OBSERVED + timedelta(days=2)
    old = journal.versions()[0]
    with pytest.raises(cal.CalendarError, match="clock allowance"):
        journal.append_notice(notice(), expected_parent=old.digest, observed_at=future)
    with pytest.raises(cal.CalendarError, match="clock allowance"):
        cal.create_journal(
            tmp_path / "future.sqlite",
            baseline_pdf=PDF.read_bytes(),
            review_reference="fixture",
            observed_at=future,
        )
    assert journal.versions() == (old,)
    assert not (tmp_path / "future.sqlite").exists()


def test_binding_authentication_and_lineage(journal: cal.CalendarJournal, tmp_path: Path) -> None:
    old = journal.versions()[0]
    binding = cal.bind(old, date(2026, 10, 26))
    with pytest.raises(cal.CalendarError, match="binding differs"):
        cal.binding_conflicts(replace(binding, digest="forged"), old, bound_version=old)
    with pytest.raises(cal.CalendarError, match="binding differs"):
        cal.binding_conflicts(
            replace(binding, sessions=binding.sessions[1:]), old, bound_version=old
        )
    other = tmp_path / "other.sqlite"
    cal.create_journal(
        other, baseline_pdf=PDF.read_bytes(), review_reference="other", observed_at=OBSERVED
    )
    with pytest.raises(cal.CalendarError, match="descendant"):
        cal.binding_conflicts(binding, cal.CalendarJournal(other).versions()[0], bound_version=old)
    assert cal.binding_conflicts(binding, old, bound_version=old) == ()


@pytest.mark.parametrize(
    "defect,expected",
    [
        ("parent", "parent/clock"),
        ("clock", "parent/clock"),
        ("canonical", "noncanonical/unsupported"),
        ("timestamp", "noncanonical observation"),
        ("material", "notice material"),
        ("facts", "facts mismatch"),
        ("baseline", "invalid baseline"),
        ("unbound", "unbound sources"),
        ("malformed", "malformed calendar"),
        ("text_payload", "malformed calendar"),
        ("overflow", "malformed calendar"),
        ("sequence", "sequence/digest"),
    ],
)
def test_rehashed_inconsistent_records_fail(
    journal: cal.CalendarJournal, defect: str, expected: str
) -> None:
    old = journal.versions()[0]
    journal.append_notice(
        replace(notice(), dates=(date(2026, 11, 2), date(2026, 11, 3))),
        expected_parent=old.digest,
        observed_at=OBSERVED + timedelta(seconds=1),
    )
    with contextlib.closing(sqlite3.connect(journal.path)) as conn, conn:
        if defect == "unbound":
            payload = b"orphaned source"
            conn.execute(
                "INSERT INTO calendar_sources VALUES(?,?)",
                (hashlib.sha256(payload).hexdigest(), payload),
            )
        else:
            sequence = 1 if defect == "baseline" else 2
            raw = conn.execute(
                "SELECT payload FROM calendar_versions WHERE sequence=?", (sequence,)
            ).fetchone()[0]
            record: dict[str, Any] = json.loads(raw)
            if defect == "parent":
                record["parent"] = "0" * 64
            elif defect == "clock":
                record["observed_at_utc"] = "2026-10-26T20:00:00.000000Z"
            elif defect == "timestamp":
                record["observed_at_utc"] = "2026-10-26T20:00:01+00:00"
            elif defect == "material":
                record["material"]["dates"].reverse()
            elif defect == "facts":
                record["days"][0]["state"] = "open"
            elif defect == "baseline":
                record["material"]["reason"] = "altered baseline"
            elif defect == "malformed":
                record["material"] = None
            elif defect == "overflow":
                record["observed_at_utc"] = "0001-01-01T00:00:00.000000Z"
            raw = rfc8785.dumps(record)
            if defect == "canonical":
                raw = json.dumps(record, indent=2).encode()
            conn.execute("DROP TRIGGER calendar_versions_no_update")
            conn.execute(
                "UPDATE calendar_versions SET payload=?,sha256=? WHERE sequence=?",
                (
                    raw.decode() if defect == "text_payload" else raw,
                    hashlib.sha256(raw).hexdigest(),
                    sequence,
                ),
            )
            conn.execute(next(sql for sql in cal.TRIGGERS if "calendar_versions_no_update" in sql))
            if defect == "sequence":
                conn.execute("DROP TRIGGER calendar_versions_no_update")
                conn.execute("UPDATE calendar_versions SET sequence=3 WHERE sequence=2")
                conn.execute(
                    next(sql for sql in cal.TRIGGERS if "calendar_versions_no_update" in sql)
                )
    with pytest.raises(cal.CalendarError, match=expected):
        journal.versions()


@pytest.mark.parametrize("defect", ["missing", "rehashed", "digest"])
def test_source_corruption(journal: cal.CalendarJournal, defect: str) -> None:
    with contextlib.closing(sqlite3.connect(journal.path)) as conn, conn:
        operation = "delete" if defect == "missing" else "update"
        conn.execute(f"DROP TRIGGER calendar_sources_no_{operation}")
        if defect == "missing":
            conn.execute("DELETE FROM calendar_sources")
        else:
            raw = b"not the reviewed source"
            if defect == "rehashed":
                conn.execute(
                    "UPDATE calendar_sources SET payload=?,sha256=?",
                    (raw, hashlib.sha256(raw).hexdigest()),
                )
            else:
                conn.execute("UPDATE calendar_sources SET payload=?", (raw,))
        conn.execute(next(sql for sql in cal.TRIGGERS if f"calendar_sources_no_{operation}" in sql))
    with pytest.raises(cal.CalendarError, match="missing source|source digest"):
        journal.versions()


def test_clock_allowance_boundary(journal: cal.CalendarJournal) -> None:
    old = journal.versions()[0]
    exact = OBSERVED + timedelta(days=1, seconds=5)
    new = journal.append_notice(notice(), expected_parent=old.digest, observed_at=exact)
    with pytest.raises(cal.CalendarError, match="clock allowance"):
        journal.append_notice(
            notice(day=date(2026, 11, 3)),
            expected_parent=new.digest,
            observed_at=exact + timedelta(microseconds=1),
        )
    assert len(journal.versions()) == 2


def test_quick_check_failure(journal: cal.CalendarJournal, monkeypatch: pytest.MonkeyPatch) -> None:
    real_connect = sqlite3.connect

    class ConnectionProxy:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.conn = real_connect(*args, **kwargs)

        def execute(self, sql: str) -> Any:
            if sql == "PRAGMA quick_check":
                return self.conn.execute("SELECT 'injected corruption'")
            return self.conn.execute(sql)

        def close(self) -> None:
            self.conn.close()

    monkeypatch.setattr(cal.sqlite3, "connect", ConnectionProxy)
    with pytest.raises(cal.CalendarError, match="integrity check"):
        journal.versions()


def test_datetime_is_not_a_session_date(journal: cal.CalendarJournal) -> None:
    with pytest.raises(cal.CalendarError, match="anchor must"):
        cal.bind(journal.versions()[0], OBSERVED)
    with pytest.raises(cal.CalendarError, match="notice dates"):
        replace(notice(), dates=(OBSERVED,)).material()


def test_cli_is_explicit_and_only_reports_baseline(
    journal: cal.CalendarJournal,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(sys, "argv", ["calendar_v2", "--journal", str(journal.path)])
    cal.main()
    report = json.loads(capsys.readouterr().out)
    assert report["readiness"] == "BASELINE_ONLY" and not report["trial_activated"]


def test_no_production_consumer_or_frozen_module_dependency() -> None:
    module = ROOT / "src/insider_alerts/research/calendar_v2.py"
    tree = ast.parse(module.read_text(encoding="utf-8"))
    assert not any(
        isinstance(node, ast.ImportFrom)
        and node.module
        and node.module.startswith("insider_alerts")
        for node in ast.walk(tree)
    )
    for path in (ROOT / "src").rglob("*.py"):
        if path != module:
            assert "calendar_v2" not in path.read_text(encoding="utf-8")
