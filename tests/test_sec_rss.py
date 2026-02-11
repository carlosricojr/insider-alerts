from pathlib import Path

import pytest

from insider_alerts.sec.rss import SecRssParseError, parse_form4_rss


def _fixture_text() -> str:
    return Path("tests/fixtures_form4_rss.xml").read_text(encoding="utf-8")


def test_parse_rss_extracts_form4_refs() -> None:
    refs = parse_form4_rss(_fixture_text())

    assert len(refs) == 2
    assert refs[0].form_type == "4"
    assert refs[0].cik == "0000320193"
    assert refs[1].form_type == "4/A"
    assert refs[1].accession_number == "0000789019-24-000987"


def test_parse_rss_honors_max_items() -> None:
    refs = parse_form4_rss(_fixture_text(), max_items=1)
    assert len(refs) == 1


def test_parse_rss_invalid_payload_raises() -> None:
    with pytest.raises(SecRssParseError):
        parse_form4_rss("<rss><channel><item>")
