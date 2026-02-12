from pathlib import Path

import pytest

from insider_alerts.sec.rss import SecRssParseError, parse_form4_rss


def _fixture_text() -> str:
    return Path("tests/fixtures_form4_rss.xml").read_text(encoding="utf-8")


def _atom_fixture_text() -> str:
    return Path("tests/fixtures_form4_atom.xml").read_text(encoding="utf-8")


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


def test_parse_atom_extracts_form4_refs() -> None:
    refs = parse_form4_rss(_atom_fixture_text())

    assert len(refs) == 2
    assert refs[0].form_type == "4"
    assert refs[0].cik == "0000320193"
    assert refs[0].accession_number == "0000320193-24-000123"
    assert refs[1].form_type == "4/A"
    assert refs[1].accession_number == "0000789019-24-000987"


def test_parse_rss_invalid_payload_raises() -> None:
    with pytest.raises(SecRssParseError):
        parse_form4_rss("<rss><channel><item>")


def test_parse_rss_unsupported_root_raises() -> None:
    with pytest.raises(SecRssParseError):
        parse_form4_rss("<root><item /></root>")


def test_parse_rss_prefers_cik_from_link_over_text() -> None:
    xml = """
    <rss version="2.0">
      <channel>
        <item>
          <title>4 - Example (0001216931)</title>
          <link>https://www.sec.gov/Archives/edgar/data/85961/000121693126000004/wk-form4.xml</link>
          <guid>0001216931-26-000004</guid>
          <pubDate>Tue, 11 Feb 2026 01:01:00 GMT</pubDate>
          <description>FORM 4; accession 0001216931-26-000004</description>
        </item>
      </channel>
    </rss>
    """
    refs = parse_form4_rss(xml)
    assert len(refs) == 1
    assert refs[0].cik == "0000085961"
