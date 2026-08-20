from datetime import UTC, date, datetime
from pathlib import Path
from shutil import rmtree
from uuid import uuid4

from pytest_httpx import HTTPXMock

from insider_alerts.config import Settings
from insider_alerts.sec.backfill import iter_quarter_range, parse_form4_master_idx
from insider_alerts.sec.pipeline import backfill_form4_filings


def test_iter_quarter_range() -> None:
    quarters = list(iter_quarter_range(date(2025, 12, 15), date(2026, 4, 1)))
    assert quarters == [(2025, 4), (2026, 1), (2026, 2)]


def test_parse_form4_master_idx_filters_window() -> None:
    index_text = """Description: Master Index of EDGAR Dissemination Feed
CIK|Company Name|Form Type|Date Filed|Filename
320193|APPLE INC|4|2025-02-12|edgar/data/320193/000032019325000010/0000320193-25-000010.txt
320193|APPLE INC|4/A|2025-02-13|edgar/data/320193/000032019325000011/0000320193-25-000011.txt
320193|APPLE INC|10-K|2025-02-12|edgar/data/320193/000032019325000012/0000320193-25-000012.txt
"""

    refs = parse_form4_master_idx(
        index_text,
        start_date=date(2025, 2, 12),
        end_date=date(2025, 2, 12),
    )
    assert len(refs) == 1

    ref = refs[0]
    assert ref.source == "sec_master_index"
    assert ref.cik == "0000320193"
    assert ref.accession_number == "0000320193-25-000010"
    assert ref.form_type == "4"
    assert ref.filed_at == datetime(2025, 2, 12, tzinfo=UTC)
    assert (
        ref.filing_detail_url
        == "https://www.sec.gov/Archives/edgar/data/320193/000032019325000010/"
        "0000320193-25-000010-index.html"
    )
    assert (
        ref.primary_doc_url
        == "https://www.sec.gov/Archives/edgar/data/320193/000032019325000010/"
        "0000320193-25-000010.txt"
    )


def test_backfill_form4_filings_upserts_deduped_refs(httpx_mock: HTTPXMock) -> None:
    q4_text = """Description: Master Index of EDGAR Dissemination Feed
CIK|Company Name|Form Type|Date Filed|Filename
320193|APPLE INC|4|2025-12-20|edgar/data/320193/000032019325000010/0000320193-25-000010.txt
"""
    q1_text = """Description: Master Index of EDGAR Dissemination Feed
CIK|Company Name|Form Type|Date Filed|Filename
85961|BARNES GROUP INC|4/A|2026-01-08|edgar/data/85961/000121693126000004/0001216931-26-000004.txt
"""

    httpx_mock.add_response(
        status_code=200,
        text=q4_text,
        url="https://www.sec.gov/Archives/edgar/full-index/2025/QTR4/master.idx",
    )
    httpx_mock.add_response(
        status_code=200,
        text=q1_text,
        url="https://www.sec.gov/Archives/edgar/full-index/2026/QTR1/master.idx",
    )

    tmp_dir = Path(".tmp_testdata") / f"sec_backfill_{uuid4().hex}"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    try:
        settings = Settings(
            DATABASE_PATH=str(tmp_dir / "db.sqlite3"),
            SEC_RATE_LIMIT_PER_SECOND=10,
        )
        first = backfill_form4_filings(
            settings,
            start_date=date(2025, 12, 1),
            end_date=date(2026, 2, 1),
        )

        assert first.requested_quarters == 2
        assert first.fetched_quarters == 2
        assert first.matched_filings == 2
        assert first.inserted == 2
        assert first.skipped_existing == 0

        httpx_mock.add_response(
            status_code=200,
            text=q4_text,
            url="https://www.sec.gov/Archives/edgar/full-index/2025/QTR4/master.idx",
        )
        httpx_mock.add_response(
            status_code=200,
            text=q1_text,
            url="https://www.sec.gov/Archives/edgar/full-index/2026/QTR1/master.idx",
        )
        second = backfill_form4_filings(
            settings,
            start_date=date(2025, 12, 1),
            end_date=date(2026, 2, 1),
        )
        assert second.inserted == 0
        assert second.skipped_existing == 2
    finally:
        rmtree(tmp_dir, ignore_errors=True)
