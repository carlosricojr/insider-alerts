from pathlib import Path

from pytest_httpx import HTTPXMock

from insider_alerts.config import Settings
from insider_alerts.sec.pipeline import (
    enqueue_review_packets,
    enrich_filings_with_xml_url,
    run_sec_poll_once,
)


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

    # idempotency is covered by queue tests; this test focuses on happy-path enqueue.
