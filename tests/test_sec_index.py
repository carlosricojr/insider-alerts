from pathlib import Path

from insider_alerts.sec.index import locate_form4_xml_url


def test_locate_form4_xml_url_from_filing_detail_fixture() -> None:
    html = Path("tests/fixtures_filing_detail.html").read_text(encoding="utf-8")
    url = locate_form4_xml_url(html)
    assert url == "https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/wk-form4.xml"


def test_locate_form4_xml_url_rejects_arbitrary_xml_fallback() -> None:
    html = '<html><body><a href="/Archives/edgar/data/a/other.xml">other.xml</a></body></html>'
    url = locate_form4_xml_url(html)
    assert url is None


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


def test_locate_form4_xml_url_accepts_generic_document_in_exact_type_row() -> None:
    html = """
    <table class="tableFile" summary="Document Format Files">
      <tr><th>Seq</th><th>Description</th><th>Document</th><th>Type</th></tr>
      <tr>
        <td>1</td><td>PRIMARY DOCUMENT</td>
        <td><a href="/Archives/edgar/data/a/0001/xslF345X05/primary_doc.xml">view</a>
            <a href="/Archives/edgar/data/a/0001/primary_doc.xml">primary_doc.xml</a></td>
        <td>4</td>
      </tr>
    </table>
    """

    assert locate_form4_xml_url(html) == (
        "https://www.sec.gov/Archives/edgar/data/a/0001/primary_doc.xml"
    )


def test_locate_form4_xml_url_rejects_taxonomy_xml_in_non_form4_table() -> None:
    html = """
    <table class="tableFile" summary="Document Format Files">
      <tr><th>Seq</th><th>Description</th><th>Document</th><th>Type</th></tr>
      <tr>
        <td>1</td><td>Prospectus</td>
        <td><a href="primary.htm">primary.htm</a></td><td>485BPOS</td>
      </tr>
      <tr>
        <td>2</td><td>Schema</td>
        <td><a href="fund_def.xml">fund_def.xml</a></td><td>EX-101.SCH</td>
      </tr>
      <tr>
        <td>3</td><td>Labels</td>
        <td><a href="fund_lab.xml">fund_lab.xml</a></td><td>EX-101.LAB</td>
      </tr>
    </table>
    """

    assert locate_form4_xml_url(html) is None


def test_locate_form4_xml_url_accepts_exact_form4_amendment_row() -> None:
    html = """
    <table summary="Document Format Files">
      <tr><th>Document</th><th>Type</th></tr>
      <tr><td><a href="/Archives/edgar/data/a/0001/rdgdoc.xml">rdgdoc.xml</a></td><td>4/A</td></tr>
    </table>
    """

    assert locate_form4_xml_url(html) == (
        "https://www.sec.gov/Archives/edgar/data/a/0001/rdgdoc.xml"
    )


def test_locate_form4_xml_url_ignores_malformed_and_off_domain_links() -> None:
    html = """
    <a href="http://[broken/form4.xml">broken</a>
    <a href="javascript:form4.xml">script</a>
    <a href="https://evil.example/form4.xml">external</a>
    """

    assert locate_form4_xml_url(html) is None
