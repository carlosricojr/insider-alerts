from pathlib import Path

from pytest_httpx import HTTPXMock

from insider_alerts.config import Settings
from insider_alerts.sec.pipeline import run_sec_poll_once


def test_sec_pipeline_poll_once(httpx_mock: HTTPXMock, tmp_path) -> None:
    rss = Path("tests/fixtures_form4_rss.xml").read_text(encoding="utf-8")
    httpx_mock.add_response(status_code=200, text=rss)
    httpx_mock.add_response(status_code=200, text=rss)

    settings = Settings(
        DATABASE_PATH=str(tmp_path / "insider_alerts.db"),
        SEC_RATE_LIMIT_PER_SECOND=10,
    )

    first = run_sec_poll_once(settings, max_items=40, dry_run=False)
    second = run_sec_poll_once(settings, max_items=40, dry_run=False)

    assert first.fetched == 2
    assert first.inserted == 2
    assert second.inserted == 0
    assert second.skipped_existing == 2
