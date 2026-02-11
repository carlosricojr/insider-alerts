import sqlite3
from datetime import UTC, datetime

from insider_alerts.sec.models import FilingRef
from insider_alerts.sec.store import (
    list_filings_missing_xml,
    update_form4_xml_url,
    upsert_filing_refs,
)


def _ref(detail_url: str) -> FilingRef:
    return FilingRef(
        source="sec_rss",
        cik="0000320193",
        accession_number="0000320193-24-000123",
        form_type="4",
        filed_at=datetime(2026, 2, 11, 1, 0, tzinfo=UTC),
        filing_detail_url=detail_url,
        primary_doc_url=None,
        raw_rss_entry={"title": "x"},
    )


def test_list_and_update_missing_xml(tmp_path) -> None:
    db = str(tmp_path / "db.sqlite3")
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
