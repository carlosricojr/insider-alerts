from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date


class Form4ParseError(RuntimeError):
    """Raised when Form 4 XML cannot be parsed."""


@dataclass(slots=True)
class Form4Transaction:
    transaction_date: date | None
    transaction_code: str
    direction: str
    shares: float
    price_per_share: float | None
    shares_following: float | None
    security_title: str | None


@dataclass(slots=True)
class Form4Facts:
    issuer_cik: str
    issuer_name: str | None
    issuer_symbol: str | None
    reporting_owner_name: str | None
    reporting_owner_cik: str | None
    is_director: bool
    is_officer: bool
    officer_title: str | None
    transactions: list[Form4Transaction]


def _text(root: ET.Element, path: str) -> str | None:
    node = root.find(path)
    if node is None or node.text is None:
        return None
    text = node.text.strip()
    return text if text else None


def _to_float(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _to_date(value: str | None) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def parse_form4_xml(xml_text: str) -> Form4Facts:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise Form4ParseError(f"invalid Form 4 XML: {exc}") from exc

    issuer_cik = _text(root, "issuer/issuerCik")
    if issuer_cik is None:
        raise Form4ParseError("missing issuer CIK")

    transactions: list[Form4Transaction] = []
    for txn in root.findall("nonDerivativeTable/nonDerivativeTransaction"):
        code = _text(txn, "transactionCoding/transactionCode") or ""
        ad = (_text(txn, "transactionAmounts/transactionAcquiredDisposedCode/value") or "").upper()
        direction = "buy" if ad == "A" else "sell" if ad == "D" else "unknown"
        shares = _to_float(_text(txn, "transactionAmounts/transactionShares/value")) or 0.0
        transactions.append(
            Form4Transaction(
                transaction_date=_to_date(_text(txn, "transactionDate/value")),
                transaction_code=code,
                direction=direction,
                shares=shares,
                price_per_share=_to_float(
                    _text(txn, "transactionAmounts/transactionPricePerShare/value")
                ),
                shares_following=_to_float(
                    _text(txn, "postTransactionAmounts/sharesOwnedFollowingTransaction/value")
                ),
                security_title=_text(txn, "securityTitle/value"),
            )
        )

    return Form4Facts(
        issuer_cik=issuer_cik,
        issuer_name=_text(root, "issuer/issuerName"),
        issuer_symbol=_text(root, "issuer/issuerTradingSymbol"),
        reporting_owner_name=_text(root, "reportingOwner/reportingOwnerId/rptOwnerName"),
        reporting_owner_cik=_text(root, "reportingOwner/reportingOwnerId/rptOwnerCik"),
        is_director=_text(root, "reportingOwner/reportingOwnerRelationship/isDirector") == "1",
        is_officer=_text(root, "reportingOwner/reportingOwnerRelationship/isOfficer") == "1",
        officer_title=_text(root, "reportingOwner/reportingOwnerRelationship/officerTitle"),
        transactions=transactions,
    )
