from pathlib import Path

from insider_alerts.sec.index import locate_form4_xml_url


def test_locate_form4_xml_url_from_filing_detail_fixture() -> None:
    html = Path("tests/fixtures_filing_detail.html").read_text(encoding="utf-8")
    url = locate_form4_xml_url(html)
    assert url == "https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/wk-form4.xml"


def test_locate_form4_xml_url_with_nonpreferred_xml_fallback() -> None:
    html = '<html><body><a href="/Archives/edgar/data/a/other.xml">other.xml</a></body></html>'
    url = locate_form4_xml_url(html)
    assert url == "https://www.sec.gov/Archives/edgar/data/a/other.xml"


def test_locate_form4_xml_url_preserves_absolute() -> None:
    html = '<html><body><a href="https://www.sec.gov/Archives/edgar/data/a/form4.xml">x</a></body></html>'
    url = locate_form4_xml_url(html)
    assert url == "https://www.sec.gov/Archives/edgar/data/a/form4.xml"


def test_locate_form4_xml_url_returns_none_when_missing() -> None:
    url = locate_form4_xml_url("<html><body><a href='/a.txt'>a.txt</a></body></html>")
    assert url is None


def test_locate_form4_xml_url_prefers_non_xsl_xml() -> None:
    html = """
    <html><body>
      <a href="/Archives/edgar/data/a/0001/xslF345X05/wk-form4_1.xml">xsl</a>
      <a href="/Archives/edgar/data/a/0001/wk-form4_1.xml">raw</a>
    </body></html>
    """
    url = locate_form4_xml_url(html)
    assert url == "https://www.sec.gov/Archives/edgar/data/a/0001/wk-form4_1.xml"
