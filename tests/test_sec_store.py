from datetime import UTC, datetime

from insider_alerts.sec.models import FilingRef
from insider_alerts.sec.store import upsert_filing_refs


def test_store_dedupes_filing_refs(tmp_path) -> None:
    db_path = tmp_path / "insider_alerts.db"
    ref = FilingRef(
        source="sec_rss",
        cik="0000320193",
        accession_number="0000320193-24-000123",
        form_type="4",
        filed_at=datetime(2026, 2, 11, tzinfo=UTC),
        filing_detail_url="https://www.sec.gov/Archives/edgar/data/320193/000032019324000123",
        primary_doc_url=None,
        raw_rss_entry={"title": "x"},
    )

    first = upsert_filing_refs(str(db_path), [ref])
    second = upsert_filing_refs(str(db_path), [ref])

    assert first.inserted == 1
    assert first.skipped_existing == 0
    assert second.inserted == 0
    assert second.skipped_existing == 1
