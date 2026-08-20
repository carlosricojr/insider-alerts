import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from shutil import rmtree
from uuid import uuid4

import pytest

from insider_alerts.sec import store as store_module
from insider_alerts.sec.models import FilingRef
from insider_alerts.sec.store import (
    get_filing_date_bounds,
    list_filings_missing_xml,
    update_form4_xml_url,
    update_form4_xml_urls,
    upsert_filing_refs,
)


def _ref(
    detail_url: str,
    *,
    accession_number: str = "0000320193-24-000123",
) -> FilingRef:
    return FilingRef(
        source="sec_rss",
        cik="0000320193",
        accession_number=accession_number,
        form_type="4",
        filed_at=datetime(2026, 2, 11, 1, 0, tzinfo=UTC),
        filing_detail_url=detail_url,
        primary_doc_url=None,
        raw_rss_entry={"title": "x"},
    )


def test_list_and_update_missing_xml() -> None:
    tmp_dir = Path(".tmp_testdata") / f"sec_store_{uuid4().hex}"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    try:
        db = str(tmp_dir / "db.sqlite3")
        upsert_filing_refs(db, [_ref("https://www.sec.gov/a-index.htm")])

        rows = list_filings_missing_xml(db, limit=10)
        assert len(rows) == 1

        updated = update_form4_xml_url(
            db,
            accession_number=rows[0].accession_number,
            cik=rows[0].cik,
            form_type=rows[0].form_type,
            xml_url="https://www.sec.gov/a.xml",
        )
        assert updated == 1

        updated_again = update_form4_xml_url(
            db,
            accession_number=rows[0].accession_number,
            cik=rows[0].cik,
            form_type=rows[0].form_type,
            xml_url="https://www.sec.gov/b.xml",
        )
        assert updated_again == 0

        with sqlite3.connect(db) as conn:
            value = conn.execute("SELECT form4_xml_url FROM filings").fetchone()[0]
        assert value == "https://www.sec.gov/a.xml"
    finally:
        rmtree(tmp_dir, ignore_errors=True)


def test_get_filing_date_bounds() -> None:
    tmp_dir = Path(".tmp_testdata") / f"sec_store_{uuid4().hex}"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    try:
        db = str(tmp_dir / "db.sqlite3")
        assert get_filing_date_bounds(db) == (None, None)

        first = _ref("https://www.sec.gov/a-index.htm")
        second = FilingRef(
            source=first.source,
            cik=first.cik,
            accession_number="0000320193-24-000124",
            form_type=first.form_type,
            filed_at=datetime(2026, 2, 12, 1, 0, tzinfo=UTC),
            filing_detail_url=first.filing_detail_url,
            primary_doc_url=first.primary_doc_url,
            raw_rss_entry=first.raw_rss_entry,
        )
        upsert_filing_refs(db, [first, second])

        min_date, max_date = get_filing_date_bounds(db)
        assert str(min_date) == "2026-02-11"
        assert str(max_date) == "2026-02-12"
    finally:
        rmtree(tmp_dir, ignore_errors=True)


def test_update_form4_xml_urls_batch_updates_multiple_rows() -> None:
    tmp_dir = Path(".tmp_testdata") / f"sec_store_{uuid4().hex}"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    try:
        db = str(tmp_dir / "db.sqlite3")
        upsert_filing_refs(
            db,
            [
                _ref(
                    "https://www.sec.gov/a-index.htm",
                    accession_number="0000320193-24-000123",
                ),
                _ref(
                    "https://www.sec.gov/b-index.htm",
                    accession_number="0000320193-24-000124",
                ),
            ],
        )

        updated = update_form4_xml_urls(
            db,
            updates=[
                (
                    "0000320193-24-000123",
                    "0000320193",
                    "4",
                    "https://www.sec.gov/a.xml",
                ),
                (
                    "0000320193-24-000124",
                    "0000320193",
                    "4",
                    "https://www.sec.gov/b.xml",
                ),
            ],
        )
        assert updated == 2

        with sqlite3.connect(db) as conn:
            urls = {
                row[0]
                for row in conn.execute(
                    "SELECT form4_xml_url FROM filings WHERE form4_xml_url IS NOT NULL"
                ).fetchall()
            }
        assert urls == {"https://www.sec.gov/a.xml", "https://www.sec.gov/b.xml"}
    finally:
        rmtree(tmp_dir, ignore_errors=True)


def test_update_form4_xml_urls_retries_locked_error(monkeypatch) -> None:
    tmp_dir = Path(".tmp_testdata") / f"sec_store_{uuid4().hex}"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    try:
        db = str(tmp_dir / "db.sqlite3")
        ref = _ref("https://www.sec.gov/a-index.htm")
        upsert_filing_refs(db, [ref])

        original = store_module._update_form4_xml_urls_once
        calls = {"count": 0}

        def _flaky_once(db_path: str, updates):  # type: ignore[no-untyped-def]
            calls["count"] += 1
            if calls["count"] == 1:
                raise sqlite3.OperationalError("database is locked")
            return original(db_path, updates)

        monkeypatch.setattr(store_module, "_update_form4_xml_urls_once", _flaky_once)
        monkeypatch.setattr(store_module, "SQLITE_LOCK_RETRY_BASE_SLEEP_SECONDS", 0.0)

        updated = update_form4_xml_urls(
            db,
            updates=[
                (
                    ref.accession_number,
                    ref.cik,
                    ref.form_type,
                    "https://www.sec.gov/a.xml",
                )
            ],
        )

        assert updated == 1
        assert calls["count"] == 2
    finally:
        rmtree(tmp_dir, ignore_errors=True)


def test_update_form4_xml_urls_raises_non_lock_operational_error(monkeypatch) -> None:
    tmp_dir = Path(".tmp_testdata") / f"sec_store_{uuid4().hex}"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    try:
        db = str(tmp_dir / "db.sqlite3")
        ref = _ref("https://www.sec.gov/a-index.htm")
        upsert_filing_refs(db, [ref])

        def _bad_once(db_path: str, updates):  # type: ignore[no-untyped-def]
            raise sqlite3.OperationalError("no such table: filings")

        monkeypatch.setattr(store_module, "_update_form4_xml_urls_once", _bad_once)
        with pytest.raises(sqlite3.OperationalError, match="no such table"):
            update_form4_xml_urls(
                db,
                updates=[
                    (
                        ref.accession_number,
                        ref.cik,
                        ref.form_type,
                        "https://www.sec.gov/a.xml",
                    )
                ],
            )
    finally:
        rmtree(tmp_dir, ignore_errors=True)
