import sqlite3
from datetime import UTC, date, datetime
from pathlib import Path

from pytest_httpx import HTTPXMock

from insider_alerts.config import Settings
from insider_alerts.review.market_context import MarketSnapshot
from insider_alerts.sec.models import FilingRef
from insider_alerts.sec.pipeline import (
    enqueue_review_packets,
    enrich_filings_with_xml_url,
    run_sec_poll_once,
)
from insider_alerts.sec.store import update_form4_xml_url, upsert_filing_refs


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


def test_enrich_filings_updates_missing_xml(httpx_mock: HTTPXMock, tmp_path) -> None:
    rss = Path("tests/fixtures_form4_rss.xml").read_text(encoding="utf-8")
    httpx_mock.add_response(status_code=200, text=rss)

    settings = Settings(DATABASE_PATH=str(tmp_path / "db.sqlite3"), SEC_RATE_LIMIT_PER_SECOND=10)
    run_sec_poll_once(settings, max_items=1, dry_run=False)

    result = enrich_filings_with_xml_url(settings, limit=10)
    assert result.scanned == 1
    assert result.updated == 1


def test_enqueue_review_packets_from_xml_urls(httpx_mock: HTTPXMock, tmp_path) -> None:
    rss = Path("tests/fixtures_form4_rss.xml").read_text(encoding="utf-8")
    form4 = Path("tests/fixtures_form4.xml").read_text(encoding="utf-8")
    httpx_mock.add_response(status_code=200, text=rss)

    settings = Settings(DATABASE_PATH=str(tmp_path / "db.sqlite3"), SEC_RATE_LIMIT_PER_SECOND=10)
    run_sec_poll_once(settings, max_items=1, dry_run=False)
    enrich_filings_with_xml_url(settings, limit=5)

    httpx_mock.add_response(status_code=200, text=form4)
    result = enqueue_review_packets(settings, limit=5)
    assert result.processed == 1
    assert result.enqueued == 1


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
