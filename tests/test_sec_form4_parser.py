from pathlib import Path

import pytest

from insider_alerts.sec.form4 import Form4ParseError, parse_form4_xml


def test_parse_form4_xml_fixture_to_canonical_facts() -> None:
    xml = Path("tests/fixtures_form4.xml").read_text(encoding="utf-8")
    facts = parse_form4_xml(xml)
    assert facts.issuer_cik == "0000320193"
    assert facts.issuer_symbol == "AAPL"
    assert facts.reporting_owner_name == "DOE JOHN"
    assert len(facts.transactions) == 2
    assert facts.transactions[0].direction == "buy"
    assert facts.transactions[1].direction == "sell"


def test_parse_form4_xml_handles_missing_numeric_fields() -> None:
    xml = """
    <ownershipDocument>
      <issuer><issuerCik>0001</issuerCik></issuer>
      <nonDerivativeTable>
        <nonDerivativeTransaction>
          <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
          <transactionAmounts><transactionShares><value>bad</value></transactionShares></transactionAmounts>
          <transactionDate><value>bad-date</value></transactionDate>
        </nonDerivativeTransaction>
      </nonDerivativeTable>
    </ownershipDocument>
    """
    facts = parse_form4_xml(xml)
    assert facts.transactions[0].shares == 0.0
    assert facts.transactions[0].transaction_date is None


def test_parse_form4_xml_invalid_payload_raises() -> None:
    with pytest.raises(Form4ParseError):
        parse_form4_xml("<ownershipDocument>")


def test_parse_form4_xml_missing_issuer_raises() -> None:
    with pytest.raises(Form4ParseError):
        parse_form4_xml("<ownershipDocument><issuer></issuer></ownershipDocument>")
