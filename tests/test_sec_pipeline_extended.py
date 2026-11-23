import sqlite3
from contextlib import closing
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from pytest_httpx import HTTPXMock

from insider_alerts.config import Settings
from insider_alerts.review.market_context import MarketSnapshot
from insider_alerts.review.queue import list_pending_review_packets
from insider_alerts.sec.models import FilingRef
from insider_alerts.sec.pipeline import (
    enqueue_review_packets,
    enrich_filings_with_xml_url,
    run_sec_poll_once,
)
from insider_alerts.sec.store import (
    list_filings_missing_xml,
    update_form4_xml_url,
    upsert_filing_refs,
)


def _seed_ref(
    db_path: str,
    *,
    accession_number: str,
    filed_at: datetime,
    xml_url: str,
    cik: str = "0000320193",
) -> None:
    ref = FilingRef(
        source="sec_rss",
        cik=cik,
        accession_number=accession_number,
        form_type="4",
        filed_at=filed_at,
        filing_detail_url="https://www.sec.gov/Archives/edgar/data/320193/000032019324000123-index.htm",
        primary_doc_url=None,
        raw_rss_entry={"title": "4 - Apple Inc"},
    )
    upsert_filing_refs(db_path, [ref])
    updated = update_form4_xml_url(
        db_path,
        accession_number=ref.accession_number,
        cik=ref.cik,
        form_type=ref.form_type,
        xml_url=xml_url,
    )
    assert updated == 1


def test_sec_poll_reports_source_boundary_diagnostics(
    httpx_mock: HTTPXMock,
    tmp_path,
) -> None:
    rss = Path("tests/fixtures_form4_rss.xml").read_text(encoding="utf-8")
    httpx_mock.add_response(status_code=200, text=rss)
    settings = Settings(DATABASE_PATH=str(tmp_path / "db.sqlite3"), SEC_RATE_LIMIT_PER_SECOND=10)

    result = run_sec_poll_once(settings, max_items=10, dry_run=True)

    assert result.fetched == 2
    assert result.source_items_seen == 3
    assert result.source_boundary_rejected == 0
    assert result.source_invalid_items == 1


def test_enrich_filings_updates_missing_xml(httpx_mock: HTTPXMock, tmp_path) -> None:
    rss = Path("tests/fixtures_form4_rss.xml").read_text(encoding="utf-8")
    httpx_mock.add_response(status_code=200, text=rss)

    settings = Settings(DATABASE_PATH=str(tmp_path / "db.sqlite3"), SEC_RATE_LIMIT_PER_SECOND=10)
    run_sec_poll_once(settings, max_items=1, dry_run=False)

    progress: list[str] = []
    result = enrich_filings_with_xml_url(
        settings,
        limit=10,
        progress_callback=progress.append,
    )
    assert result.scanned == 1
    assert result.updated == 1
    assert progress == ["enrichment_item_0_started", "enrichment_items_completed"]


def test_enrich_filings_preserves_successes_when_later_detail_fetch_fails(
    httpx_mock: HTTPXMock, tmp_path
) -> None:
    settings = Settings(
        DATABASE_PATH=str(tmp_path / "db.sqlite3"),
        SEC_RATE_LIMIT_PER_SECOND=10,
        SEC_RETRY_MIN_SECONDS=0,
        SEC_RETRY_MAX_SECONDS=0,
    )
    good = FilingRef(
        source="sec_rss",
        cik="1",
        accession_number="0000000001-26-000001",
        form_type="4",
        filed_at=datetime(2026, 2, 12, 2, 0, tzinfo=UTC),
        filing_detail_url="https://www.sec.gov/good.xml",
        primary_doc_url=None,
        raw_rss_entry={},
    )
    bad = FilingRef(
        source="sec_rss",
        cik="2",
        accession_number="0000000002-26-000002",
        form_type="4",
        filed_at=datetime(2026, 2, 12, 1, 0, tzinfo=UTC),
        filing_detail_url="https://www.sec.gov/missing-index.htm",
        primary_doc_url=None,
        raw_rss_entry={},
    )
    upsert_filing_refs(settings.database_path, [good, bad])
    httpx_mock.add_response(url=bad.filing_detail_url, status_code=404)

    result = enrich_filings_with_xml_url(settings, limit=10)

    assert result.scanned == 2
    assert result.updated == 1
    with sqlite3.connect(settings.database_path) as conn:
        value = conn.execute(
            "SELECT form4_xml_url FROM filings WHERE accession_number = ?",
            (good.accession_number,),
        ).fetchone()
    assert value == (good.filing_detail_url,)


@pytest.mark.parametrize(
    "filing_url",
    [
        "https://attacker.example/form4.xml",
        "http://www.sec.gov/form4.xml",
        "https://www.sec.gov.attacker.example/form4.xml",
        "https://[invalid/form4.xml",
        "https://attacker.example/form4-index.htm",
        "https://www.sec.gov:notaport/form4.xml",
        "https://www.sec.gov:/form4.xml",
        "https://www.sec.gov:8443/form4.xml",
        "https://user:password@www.sec.gov/form4.xml",
        "https://.sec.gov/form4.xml",
        "https:///form4.xml",
        "/form4.xml",
        "form4.xml",
    ],
)
def test_enrich_filings_rejects_direct_xml_outside_sec_boundary(
    httpx_mock: HTTPXMock,
    tmp_path,
    filing_url: str,
) -> None:
    settings = Settings(DATABASE_PATH=str(tmp_path / "db.sqlite3"), SEC_RATE_LIMIT_PER_SECOND=10)
    ref = FilingRef(
        source="sec_rss",
        cik="1",
        accession_number="0000000001-26-000001",
        form_type="4",
        filed_at=datetime(2026, 2, 12, 2, 0, tzinfo=UTC),
        filing_detail_url=filing_url,
        primary_doc_url=None,
        raw_rss_entry={},
    )
    upsert_filing_refs(settings.database_path, [ref])

    result = enrich_filings_with_xml_url(settings, limit=10)

    assert result.scanned == 1
    assert result.updated == 0
    assert result.xml_not_found == 1
    assert httpx_mock.get_requests() == []
    with sqlite3.connect(settings.database_path) as conn:
        value = conn.execute(
            "SELECT form4_xml_url FROM filings WHERE accession_number = ?",
            (ref.accession_number,),
        ).fetchone()
    assert value == (None,)


@pytest.mark.parametrize(
    "xml_url",
    [
        "https://attacker.example/form4.xml",
        "https://xsl.attacker.example/www.sec.gov/form4.xml",
        "https:///form4.xml",
        "/form4.xml",
        "form4.xml",
    ],
)
def test_enqueue_review_packets_rejects_stored_xml_outside_sec_boundary(
    httpx_mock: HTTPXMock,
    tmp_path,
    xml_url: str,
) -> None:
    settings = Settings(DATABASE_PATH=str(tmp_path / "db.sqlite3"), SEC_RATE_LIMIT_PER_SECOND=10)
    _seed_ref(
        settings.database_path,
        accession_number="0000320193-24-000123",
        filed_at=datetime(2026, 2, 11, 1, 0, tzinfo=UTC),
        xml_url=xml_url,
    )

    result = enqueue_review_packets(settings, limit=5)

    assert result.processed == 1
    assert result.enqueued == 0
    assert result.http_failed == 1
    assert httpx_mock.get_requests() == []


def test_enrichment_rejection_cannot_starve_older_valid_row(
    httpx_mock: HTTPXMock,
    tmp_path,
) -> None:
    settings = Settings(DATABASE_PATH=str(tmp_path / "db.sqlite3"), SEC_RATE_LIMIT_PER_SECOND=10)
    rejected = FilingRef(
        source="sec_rss",
        cik="0000000002",
        accession_number="0000000002-26-000002",
        form_type="4",
        filed_at=datetime(2026, 2, 12, 2, 0, tzinfo=UTC),
        filing_detail_url="https://www.sec.gov/no-form4-index.htm",
        primary_doc_url=None,
        raw_rss_entry={"title": "4 - Rejected Issuer"},
    )
    valid = FilingRef(
        source="sec_rss",
        cik="0000000001",
        accession_number="0000000001-26-000001",
        form_type="4",
        filed_at=datetime(2026, 2, 12, 1, 0, tzinfo=UTC),
        filing_detail_url="https://www.sec.gov/valid-form4.xml",
        primary_doc_url=None,
        raw_rss_entry={"title": "4 - Valid Issuer"},
    )
    upsert_filing_refs(settings.database_path, [rejected, valid])
    httpx_mock.add_response(
        url=rejected.filing_detail_url,
        text="""
        <table summary="Document Format Files">
          <tr><th>Document</th><th>Type</th></tr>
          <tr><td><a href="taxonomy.xml">taxonomy</a></td><td>EX-101</td></tr>
        </table>
        """,
    )

    first = enrich_filings_with_xml_url(settings, limit=1)
    second = enrich_filings_with_xml_url(settings, limit=1)
    third = enrich_filings_with_xml_url(settings, limit=1)

    assert (first.scanned, first.updated, first.xml_not_found) == (1, 0, 1)
    assert (second.scanned, second.updated) == (1, 1)
    assert third.scanned == 0
    with sqlite3.connect(settings.database_path) as conn:
        rejection = conn.execute(
            "SELECT stage, reason FROM sec_processing_rejections"
        ).fetchone()
        selected = conn.execute(
            "SELECT form4_xml_url FROM filings WHERE accession_number = ?",
            (valid.accession_number,),
        ).fetchone()
    assert rejection == ("xml_enrichment", "xml_not_found")
    assert selected == (valid.filing_detail_url,)


def test_enrichment_unrecognized_page_remains_retryable(
    httpx_mock: HTTPXMock,
    tmp_path,
) -> None:
    settings = Settings(DATABASE_PATH=str(tmp_path / "db.sqlite3"), SEC_RATE_LIMIT_PER_SECOND=10)
    ref = FilingRef(
        source="sec_rss",
        cik="0000000002",
        accession_number="0000000002-26-000002",
        form_type="4",
        filed_at=datetime(2026, 2, 12, 2, 0, tzinfo=UTC),
        filing_detail_url="https://www.sec.gov/transient-index.htm",
        primary_doc_url=None,
        raw_rss_entry={"title": "4 - Transient Issuer"},
    )
    upsert_filing_refs(settings.database_path, [ref])
    for _ in range(2):
        httpx_mock.add_response(url=ref.filing_detail_url, text="<html>try again</html>")

    first = enrich_filings_with_xml_url(settings, limit=1)
    second = enrich_filings_with_xml_url(settings, limit=1)

    assert (first.scanned, first.updated, first.xml_not_found) == (1, 0, 1)
    assert (second.scanned, second.updated, second.xml_not_found) == (1, 0, 1)
    assert len(httpx_mock.get_requests()) == 2
    with sqlite3.connect(settings.database_path) as conn:
        rejection_count = conn.execute(
            "SELECT COUNT(*) FROM sec_processing_rejections"
        ).fetchone()
    assert rejection_count == (0,)


def test_enrichment_truncated_document_table_remains_retryable(
    httpx_mock: HTTPXMock,
    tmp_path,
) -> None:
    settings = Settings(DATABASE_PATH=str(tmp_path / "db.sqlite3"), SEC_RATE_LIMIT_PER_SECOND=10)
    ref = FilingRef(
        source="sec_rss",
        cik="0000000002",
        accession_number="0000000002-26-000002",
        form_type="4",
        filed_at=datetime(2026, 2, 12, 2, 0, tzinfo=UTC),
        filing_detail_url="https://www.sec.gov/truncated-index.htm",
        primary_doc_url=None,
        raw_rss_entry={"title": "4 - Transient Issuer"},
    )
    upsert_filing_refs(settings.database_path, [ref])
    for _ in range(2):
        httpx_mock.add_response(
            url=ref.filing_detail_url,
            text='<table summary="Document Format Files"><tr><th>Type</th></tr>',
        )

    first = enrich_filings_with_xml_url(settings, limit=1)
    second = enrich_filings_with_xml_url(settings, limit=1)

    assert (first.scanned, first.xml_not_found) == (1, 1)
    assert (second.scanned, second.xml_not_found) == (1, 1)
    assert len(httpx_mock.get_requests()) == 2
    with sqlite3.connect(settings.database_path) as conn:
        rejection_count = conn.execute(
            "SELECT COUNT(*) FROM sec_processing_rejections"
        ).fetchone()
    assert rejection_count == (0,)


@pytest.mark.parametrize(
    "unrelated_xml",
    [
        "<xbrl></xbrl>",
        '<link:linkbase xmlns:link="http://www.xbrl.org/2003/linkbase"></link:linkbase>',
        '<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema"></xsd:schema>',
    ],
)
def test_review_parse_rejection_cannot_starve_older_valid_row(
    httpx_mock: HTTPXMock,
    tmp_path,
    unrelated_xml: str,
) -> None:
    settings = Settings(DATABASE_PATH=str(tmp_path / "db.sqlite3"), SEC_RATE_LIMIT_PER_SECOND=10)
    rejected_url = "https://www.sec.gov/rejected-form4.xml"
    valid_url = "https://www.sec.gov/valid-form4.xml"
    _seed_ref(
        settings.database_path,
        accession_number="0000320193-24-000124",
        filed_at=datetime(2026, 2, 11, 2, 0, tzinfo=UTC),
        xml_url=rejected_url,
    )
    _seed_ref(
        settings.database_path,
        accession_number="0000320193-24-000123",
        filed_at=datetime(2026, 2, 11, 1, 0, tzinfo=UTC),
        xml_url=valid_url,
    )
    httpx_mock.add_response(url=rejected_url, text=unrelated_xml)
    httpx_mock.add_response(
        url=valid_url,
        text=Path("tests/fixtures_form4.xml").read_text(encoding="utf-8"),
    )

    first = enqueue_review_packets(settings, limit=1)
    second = enqueue_review_packets(settings, limit=1)

    assert (first.processed, first.enqueued, first.parse_failed) == (1, 0, 1)
    assert (second.processed, second.enqueued) == (1, 1)
    assert [str(request.url) for request in httpx_mock.get_requests()] == [
        rejected_url,
        valid_url,
    ]
    with sqlite3.connect(settings.database_path) as conn:
        rejection = conn.execute(
            "SELECT stage, reason FROM sec_processing_rejections"
        ).fetchone()
    assert rejection == ("review_xml", "parse_failed")


@pytest.mark.parametrize(
    "xml_text",
    [
        "<ownershipDocument>",
        "<ownershipDocument></ownershipDocument>",
        "<Error>rate limited</Error>",
        "<html>try again</html>",
    ],
)
def test_review_unrecognized_xml_remains_retryable(
    httpx_mock: HTTPXMock,
    tmp_path,
    xml_text: str,
) -> None:
    settings = Settings(DATABASE_PATH=str(tmp_path / "db.sqlite3"), SEC_RATE_LIMIT_PER_SECOND=10)
    xml_url = "https://www.sec.gov/transient-form4.xml"
    _seed_ref(
        settings.database_path,
        accession_number="0000320193-24-000124",
        filed_at=datetime(2026, 2, 11, 2, 0, tzinfo=UTC),
        xml_url=xml_url,
    )
    for _ in range(2):
        httpx_mock.add_response(url=xml_url, text=xml_text)

    first = enqueue_review_packets(settings, limit=1)
    second = enqueue_review_packets(settings, limit=1)

    assert (first.processed, first.enqueued, first.parse_failed) == (1, 0, 1)
    assert (second.processed, second.enqueued, second.parse_failed) == (1, 0, 1)
    assert len(httpx_mock.get_requests()) == 2
    with sqlite3.connect(settings.database_path) as conn:
        rejection_count = conn.execute(
            "SELECT COUNT(*) FROM sec_processing_rejections"
        ).fetchone()
    assert rejection_count == (0,)


def test_enqueue_review_packets_from_xml_urls(httpx_mock: HTTPXMock, tmp_path) -> None:
    rss = Path("tests/fixtures_form4_rss.xml").read_text(encoding="utf-8")
    form4 = Path("tests/fixtures_form4.xml").read_text(encoding="utf-8")
    httpx_mock.add_response(status_code=200, text=rss)

    settings = Settings(DATABASE_PATH=str(tmp_path / "db.sqlite3"), SEC_RATE_LIMIT_PER_SECOND=10)
    run_sec_poll_once(settings, max_items=1, dry_run=False)
    enrich_filings_with_xml_url(settings, limit=5)

    httpx_mock.add_response(status_code=200, text=form4)
    progress: list[str] = []
    result = enqueue_review_packets(
        settings,
        limit=5,
        progress_callback=progress.append,
    )
    assert result.processed == 1
    assert result.enqueued == 1
    assert progress == ["review_item_0_started", "review_items_completed"]


def test_enqueue_review_packets_skips_existing_packets(httpx_mock: HTTPXMock, tmp_path) -> None:
    rss = Path("tests/fixtures_form4_rss.xml").read_text(encoding="utf-8")
    form4 = Path("tests/fixtures_form4.xml").read_text(encoding="utf-8")
    httpx_mock.add_response(status_code=200, text=rss)

    settings = Settings(DATABASE_PATH=str(tmp_path / "db.sqlite3"), SEC_RATE_LIMIT_PER_SECOND=10)
    run_sec_poll_once(settings, max_items=1, dry_run=False)
    enrich_filings_with_xml_url(settings, limit=5)

    httpx_mock.add_response(status_code=200, text=form4)
    first = enqueue_review_packets(settings, limit=5)
    second = enqueue_review_packets(settings, limit=5)

    assert first.processed == 1
    assert first.enqueued == 1
    assert second.processed == 0
    assert second.enqueued == 0
    assert len(httpx_mock.get_requests()) == 2


def test_enqueue_review_packets_normalizes_xsl_urls(httpx_mock: HTTPXMock, tmp_path) -> None:
    settings = Settings(DATABASE_PATH=str(tmp_path / "db.sqlite3"), SEC_RATE_LIMIT_PER_SECOND=10)
    xsl_url = (
        "https://www.sec.gov/Archives/edgar/data/85961/000121693126000004/"
        "xslF345X05/wk-form4_1770852089.xml"
    )
    raw_url = "https://www.sec.gov/Archives/edgar/data/85961/000121693126000004/wk-form4_1770852089.xml"
    form4 = Path("tests/fixtures_form4.xml").read_text(encoding="utf-8")

    _seed_ref(
        settings.database_path,
        accession_number="0000320193-24-000123",
        filed_at=datetime(2026, 2, 11, 1, 0, tzinfo=UTC),
        xml_url=xsl_url,
    )
    httpx_mock.add_response(status_code=200, text=form4, url=raw_url)

    result = enqueue_review_packets(settings, limit=5)
    assert result.processed == 1
    assert result.enqueued == 1
    assert len(httpx_mock.get_requests()) == 1
    assert str(httpx_mock.get_requests()[0].url) == raw_url


def test_enqueue_review_packets_skips_bad_xml_and_continues(
    httpx_mock: HTTPXMock, tmp_path
) -> None:
    settings = Settings(DATABASE_PATH=str(tmp_path / "db.sqlite3"), SEC_RATE_LIMIT_PER_SECOND=10)
    bad_xsl_url = (
        "https://www.sec.gov/Archives/edgar/data/85961/000121693126000004/"
        "xslF345X05/wk-form4_1770852089.xml"
    )
    bad_raw_url = (
        "https://www.sec.gov/Archives/edgar/data/85961/000121693126000004/"
        "wk-form4_1770852089.xml"
    )
    good_raw_url = (
        "https://www.sec.gov/Archives/edgar/data/320193/000032019324000124/"
        "wk-form4_1770852090.xml"
    )
    form4 = Path("tests/fixtures_form4.xml").read_text(encoding="utf-8")

    _seed_ref(
        settings.database_path,
        accession_number="0000320193-24-000123",
        filed_at=datetime(2026, 2, 11, 1, 1, tzinfo=UTC),
        xml_url=bad_xsl_url,
    )
    _seed_ref(
        settings.database_path,
        accession_number="0000320193-24-000124",
        filed_at=datetime(2026, 2, 11, 1, 0, tzinfo=UTC),
        xml_url=good_raw_url,
    )

    httpx_mock.add_response(status_code=200, text="<html>not xml</html>", url=bad_raw_url)
    httpx_mock.add_response(status_code=200, text=form4, url=good_raw_url)

    result = enqueue_review_packets(settings, limit=5)
    assert result.processed == 2
    assert result.enqueued == 1


def test_enqueue_review_packets_dedupes_same_accession_across_cik(
    httpx_mock: HTTPXMock, tmp_path
) -> None:
    settings = Settings(DATABASE_PATH=str(tmp_path / "db.sqlite3"), SEC_RATE_LIMIT_PER_SECOND=10)
    raw_url = (
        "https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/"
        "wk-form4_1770852090.xml"
    )
    form4 = Path("tests/fixtures_form4.xml").read_text(encoding="utf-8")

    _seed_ref(
        settings.database_path,
        accession_number="0000320193-24-000123",
        filed_at=datetime(2026, 2, 11, 1, 1, tzinfo=UTC),
        xml_url=raw_url,
        cik="0000320193",
    )
    _seed_ref(
        settings.database_path,
        accession_number="0000320193-24-000123",
        filed_at=datetime(2026, 2, 11, 1, 0, tzinfo=UTC),
        xml_url=raw_url,
        cik="0000000001",
    )

    httpx_mock.add_response(status_code=200, text=form4, url=raw_url)
    result = enqueue_review_packets(settings, limit=10)
    assert result.processed == 1
    assert result.enqueued == 1
    assert len(httpx_mock.get_requests()) == 1


def test_enqueue_review_packets_adds_market_context_fields(
    httpx_mock: HTTPXMock,
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        DATABASE_PATH=str(tmp_path / "db.sqlite3"),
        SEC_RATE_LIMIT_PER_SECOND=10,
        MARKET_CONTEXT_ENABLED=True,
    )
    raw_url = (
        "https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/"
        "wk-form4_1770852090.xml"
    )
    form4 = Path("tests/fixtures_form4.xml").read_text(encoding="utf-8")
    _seed_ref(
        settings.database_path,
        accession_number="0000320193-24-000123",
        filed_at=datetime(2026, 2, 11, 1, 1, tzinfo=UTC),
        xml_url=raw_url,
    )
    httpx_mock.add_response(status_code=200, text=form4, url=raw_url)

    monkeypatch.setattr(
        "insider_alerts.sec.pipeline.get_market_snapshot",
        lambda db_path, *, symbol, trade_date: None,
    )
    monkeypatch.setattr(
        "insider_alerts.sec.pipeline.upsert_market_snapshot",
        lambda db_path, snapshot: None,
    )

    class _FakeMarketClient:
        def fetch_snapshot(self, symbol: str, *, trade_date: date) -> MarketSnapshot:
            return MarketSnapshot(
                symbol=symbol.upper(),
                trade_date=trade_date,
                close=100.0,
                volume=2_000_000.0,
                dollar_turnover=200_000_000.0,
                prior_close=101.0,
                return_1d=-0.00990099009900991,
                earnings_shock_flag=False,
            )

    monkeypatch.setattr(
        "insider_alerts.sec.pipeline.DailyMarketDataClient",
        lambda **kwargs: _FakeMarketClient(),
    )

    result = enqueue_review_packets(settings, limit=5)
    assert result.processed == 1
    assert result.enqueued == 1

    with sqlite3.connect(settings.database_path) as conn:
        trade_turnover = conn.execute(
            """
            SELECT json_extract(payload_json, '$.rationale.trade_pct_daily_turnover')
            FROM review_packets
            LIMIT 1
            """
        ).fetchone()[0]
    assert trade_turnover is not None


def test_enqueue_review_packets_excludes_legacy_false_rss_before_limit(
    httpx_mock: HTTPXMock,
    tmp_path,
) -> None:
    settings = Settings(DATABASE_PATH=str(tmp_path / "db.sqlite3"), SEC_RATE_LIMIT_PER_SECOND=10)
    valid_xml_url = "https://www.sec.gov/valid-form4.xml"
    false_xml_url = "https://www.sec.gov/taxonomy.xml"
    form4 = Path("tests/fixtures_form4.xml").read_text(encoding="utf-8")

    _seed_ref(
        settings.database_path,
        accession_number="0000320193-24-000123",
        filed_at=datetime(2026, 2, 11, 1, 0, tzinfo=UTC),
        xml_url=valid_xml_url,
    )
    false = FilingRef(
        source="sec_rss",
        cik="0001000001",
        accession_number="0001000001-26-000124",
        form_type="4",
        filed_at=datetime(2026, 8, 28, 20, 0, tzinfo=UTC),
        filing_detail_url="https://www.sec.gov/false-index.htm",
        primary_doc_url=None,
        raw_rss_entry={"title": "497 - Example Fund"},
    )
    upsert_filing_refs(settings.database_path, [false])
    assert update_form4_xml_url(
        settings.database_path,
        accession_number=false.accession_number,
        cik=false.cik,
        form_type=false.form_type,
        xml_url=false_xml_url,
    ) == 1
    httpx_mock.add_response(status_code=200, text=form4, url=valid_xml_url)

    result = enqueue_review_packets(settings, limit=1)

    assert result.processed == 1
    assert result.enqueued == 1
    assert [str(request.url) for request in httpx_mock.get_requests()] == [valid_xml_url]



def test_rss_cutover_filters_precise_acceptance_time(httpx_mock: HTTPXMock, tmp_path) -> None:
    settings = Settings(
        _env_file=None,
        DATABASE_PATH=str(tmp_path / "cutover.sqlite3"),
        SEC_RSS_CUTOVER_AT="2026-02-11T06:01:00.000001Z",
    )
    times = (
        "2026-02-10T23:59:59Z",
        "2026-02-11T01:01:00-05:00",
        "2026-02-11T01:01:00.000001-05:00",
        "2026-02-11T06:01:01Z",
        "2099-02-11T06:01:01Z",
        "2026-02-11T06:01:01",
        "0001-01-01T00:00:00+01:00",
    )
    entries = [
        f"""<entry><title>4 - Control (0000320193)</title>
        <id>0000320193-26-{index:06d}</id><updated>{at}</updated>
        <link href="https://www.sec.gov/Archives/edgar/data/320193/{index}/form4.xml" />
        <category term="4" /></entry>"""
        for index, at in enumerate(times)
    ]
    page = '<feed xmlns="http://www.w3.org/2005/Atom">' + "".join(entries) + "</feed>"
    httpx_mock.add_response(url=settings.sec_rss_url, text=page, is_reusable=True)

    dry_run = run_sec_poll_once(settings, max_items=1, dry_run=True)
    assert dry_run.fetched == 1
    assert not Path(settings.database_path).exists()
    limited = run_sec_poll_once(settings, max_items=1, dry_run=False)
    assert limited.fetched == limited.inserted == 1
    assert limited.cutover_excluded == dry_run.cutover_excluded == 3
    with closing(sqlite3.connect(settings.database_path)) as db:
        assert db.execute("SELECT filed_at FROM filings").fetchall() == [
            ("2026-02-11T06:01:00.000001+00:00",)
        ]

    result = run_sec_poll_once(settings, max_items=20, dry_run=False)
    assert result.source_items_seen == len(times)
    assert result.source_invalid_items == 2
    assert result.cutover_excluded == 3
    assert result.fetched == 2
    assert result.inserted == result.skipped_existing == 1
    with closing(sqlite3.connect(settings.database_path)) as db:
        admitted = db.execute("SELECT filed_at FROM filings ORDER BY filed_at").fetchall()
    assert admitted == [
        ("2026-02-11T06:01:00.000001+00:00",),
        ("2026-02-11T06:01:01+00:00",),
    ]


def test_rss_cutover_checks_existing_rows_before_stage_limits(
    httpx_mock: HTTPXMock, tmp_path
) -> None:
    path = str(tmp_path / "cutover.sqlite3")
    cutoff = datetime(2026, 2, 11, 6, 1, 0, 1, tzinfo=UTC)
    legacy = Settings(_env_file=None, DATABASE_PATH=path, MARKET_CONTEXT_ENABLED=False)
    guarded = Settings(
        _env_file=None,
        DATABASE_PATH=path,
        MARKET_CONTEXT_ENABLED=False,
        SEC_RSS_CUTOVER_AT=cutoff,
    )
    xml_url = "https://www.sec.gov/Archives/edgar/data/320193/000032019326000123/form4.xml"
    xml = Path("tests/fixtures_form4.xml").read_text(encoding="utf-8")
    httpx_mock.add_response(url=xml_url, text=xml, is_reusable=True)

    stale = "0000320193-26-000010"
    _seed_ref(
        path, accession_number=stale,
        filed_at=cutoff - timedelta(microseconds=1), xml_url=xml_url,
    )
    assert enqueue_review_packets(legacy, limit=1).enqueued == 1
    fresh = "0000320193-26-000013"
    published_after_insert = "0000320193-26-000015"
    for accession, filed_at in (
        ("0000320193-26-000011", cutoff - timedelta(microseconds=1)),
        ("0000320193-26-000012", cutoff.replace(tzinfo=None)),
        (fresh, cutoff),
        ("0000320193-26-000014", datetime(2099, 2, 11, tzinfo=UTC)),
        (published_after_insert, cutoff + timedelta(seconds=20)),
    ):
        _seed_ref(path, accession_number=accession, filed_at=filed_at, xml_url=xml_url)

    with closing(sqlite3.connect(path)) as db, db:
        db.execute(
            "UPDATE filings SET created_at = '2026-02-11 06:01:00' "
            "WHERE accession_number IN (?, ?)",
            (fresh, published_after_insert),
        )

    # Model an older filing and one valid filing still waiting for XML enrichment.
    enrichment_ref = FilingRef(
        source="sec_rss",
        cik="0000320193",
        accession_number="0000320193-26-000016",
        form_type="4",
        filed_at=cutoff,
        filing_detail_url=xml_url,
        primary_doc_url=None,
        raw_rss_entry={"title": "4 - Control"},
    )
    assert upsert_filing_refs(
        path,
        [
            enrichment_ref,
            replace(
                enrichment_ref,
                accession_number="0000320193-26-000017",
                filed_at=datetime(2099, 2, 11, tzinfo=UTC),
            ),
            replace(
                enrichment_ref,
                accession_number="0000320193-26-000018",
                filed_at=cutoff - timedelta(microseconds=1),
            ),
            replace(
                enrichment_ref,
                accession_number="0000320193-26-000019",
                filed_at=cutoff + timedelta(seconds=1),
                source="sec_master_index",
            ),
        ],
    ).inserted == 4
    assert [ref.accession_number for ref in list_filings_missing_xml(path, limit=1)] == [
        "0000320193-26-000017"
    ]
    assert [
        ref.accession_number
        for ref in list_filings_missing_xml(path, limit=1, rss_cutover_at=cutoff)
    ] == [enrichment_ref.accession_number]

    assert enqueue_review_packets(guarded, limit=1).enqueued == 1
    assert [row["accession_number"] for row in list_pending_review_packets(path, limit=1)] == [
        stale
    ]
    assert [
        row["accession_number"]
        for row in list_pending_review_packets(path, limit=1, rss_cutover_at=cutoff)
    ] == [fresh]

    # Keep the same accession and form but remove the exact originating CIK.
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("UPDATE review_packets SET cik='0000000001' WHERE accession_number=?", (fresh,))
    assert list_pending_review_packets(path, limit=1, rss_cutover_at=cutoff) == []

    with closing(sqlite3.connect(path)) as db, db:
        db.execute(
            "UPDATE review_packets SET cik='0000320193', form_type='4/A' WHERE accession_number=?",
            (fresh,),
        )
    assert list_pending_review_packets(path, limit=1, rss_cutover_at=cutoff) == []

    with closing(sqlite3.connect(path)) as db, db:
        db.execute("UPDATE review_packets SET form_type='4' WHERE accession_number=?", (fresh,))
        db.execute(
            "UPDATE filings SET source='sec_master_index' WHERE accession_number=?", (fresh,)
        )
    assert list_pending_review_packets(path, limit=1, rss_cutover_at=cutoff) == []
