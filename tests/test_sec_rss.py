from pathlib import Path

import pytest

from insider_alerts.sec.rss import (
    SecRssParseError,
    parse_form4_rss,
    parse_form4_rss_with_diagnostics,
)


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


def test_parse_rss_rejects_non_form4_entry_with_size_4_mb() -> None:
    xml = """
    <rss version="2.0">
      <channel>
        <item>
          <title>485BPOS - World Funds Trust (0001396092)</title>
          <link>https://www.sec.gov/Archives/edgar/data/1396092/000139609226000123/a-index.htm</link>
          <guid>0001396092-26-000123</guid>
          <pubDate>Fri, 28 Aug 2026 20:01:00 GMT</pubDate>
          <description>&lt;b&gt;Size:&lt;/b&gt; 4 MB</description>
        </item>
        <item>
          <title>497 - BNY Mellon Funds Trust (0001000001)</title>
          <link>https://www.sec.gov/Archives/edgar/data/1000001/000100000126000124/b-index.htm</link>
          <guid>0001000001-26-000124</guid>
          <pubDate>Fri, 28 Aug 2026 20:02:00 GMT</pubDate>
          <description>Size: 4 MB</description>
        </item>
      </channel>
    </rss>
    """

    result = parse_form4_rss_with_diagnostics(xml)

    assert result.refs == []
    assert result.items_seen == 2
    assert result.source_boundary_rejected == 2
    assert result.invalid_items == 0


def test_parse_atom_rejects_conflicting_non_form4_title() -> None:
    xml = """
    <feed xmlns="http://www.w3.org/2005/Atom">
      <entry>
        <title>424B3 - Example Fund (0001000001)</title>
        <link
          href="https://www.sec.gov/Archives/edgar/data/1000001/000100000126000125/c-index.htm"
        />
        <id>0001000001-26-000125</id>
        <updated>2026-08-28T20:03:00Z</updated>
        <summary>Size: 4 MB</summary>
        <category term="424B3" />
      </entry>
    </feed>
    """

    assert parse_form4_rss(xml) == []


def test_parse_rss_limit_counts_only_accepted_form4_entries() -> None:
    xml = """
    <rss version="2.0">
      <channel>
        <item>
          <title>497 - Not Form 4 (0001000001)</title>
          <link>https://www.sec.gov/Archives/edgar/data/1000001/000100000126000126/a-index.htm</link>
          <guid>0001000001-26-000126</guid>
          <pubDate>Fri, 28 Aug 2026 20:04:00 GMT</pubDate>
          <description>Size: 4 MB</description>
        </item>
        <item>
          <title>4/A - Accepted Issuer (0001000002)</title>
          <link>https://www.sec.gov/Archives/edgar/data/1000002/000100000226000127/b-index.htm</link>
          <guid>0001000002-26-000127</guid>
          <pubDate>Fri, 28 Aug 2026 20:05:00 GMT</pubDate>
        </item>
      </channel>
    </rss>
    """

    refs = parse_form4_rss(xml, max_items=1)

    assert len(refs) == 1
    assert refs[0].form_type == "4/A"
    assert refs[0].accession_number == "0001000002-26-000127"


def test_parse_rss_accepts_exact_text_category_with_unstructured_title() -> None:
    xml = """
    <rss version="2.0">
      <channel>
        <item>
          <title>Accepted Issuer filing</title>
          <link>https://www.sec.gov/Archives/edgar/data/1000003/000100000326000128/c-index.htm</link>
          <guid>0001000003-26-000128</guid>
          <pubDate>Fri, 28 Aug 2026 20:06:00 GMT</pubDate>
          <category>4</category>
        </item>
      </channel>
    </rss>
    """

    refs = parse_form4_rss(xml)

    assert len(refs) == 1
    assert refs[0].form_type == "4"
    assert refs[0].raw_rss_entry["category"] == "4"
    assert refs[0].raw_rss_entry["feed_form_type"] == "4"


def test_parse_atom_explicit_non_form4_title_overrides_form4_category() -> None:
    xml = """
    <feed xmlns="http://www.w3.org/2005/Atom">
      <entry>
        <title>485BPOS - Example Fund (0001000004)</title>
        <link
          href="https://www.sec.gov/Archives/edgar/data/1000004/000100000426000129/d-index.htm"
        />
        <id>0001000004-26-000129</id>
        <updated>2026-08-28T20:07:00Z</updated>
        <category term="4" />
      </entry>
    </feed>
    """

    assert parse_form4_rss(xml) == []


def test_parse_rss_reports_source_boundary_rejections() -> None:
    result = parse_form4_rss_with_diagnostics(_fixture_text())

    assert len(result.refs) == 2
    assert result.items_seen == 3
    assert result.source_boundary_rejected == 0
    assert result.invalid_items == 1
