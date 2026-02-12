from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
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
    is_ten_percent_owner: bool = False
    is_other: bool = False
    other_text: str | None = None
    remarks: str | None = None
    footnotes: list[str] = field(default_factory=list)
    has_10b5_1_plan: bool = False
    has_13d_reference: bool = False
    has_equity_comp_event: bool = False
    has_tax_withholding_language: bool = False


TEN_B_FIVE_ONE_RE = re.compile(r"\b10b5\s*[- ]?\s*1\b", re.IGNORECASE)
THIRTEEN_D_RE = re.compile(r"schedule\s+13d|rule\s+13d|13d-5", re.IGNORECASE)
EQUITY_COMP_RE = re.compile(
    r"restricted\s+stock\s+unit|rsu|equity\s+incentive\s+plan|vesting|stock\s+option|performance-?based",
    re.IGNORECASE,
)
TAX_WITHHOLDING_RE = re.compile(
    r"tax\s+withholding|cover\s+tax|solely\s+to\s+cover\s+tax|withholding\s+obligation",
    re.IGNORECASE,
)


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


def _to_bool(value: str | None) -> bool:
    if value is None:
        return False
    lowered = value.strip().lower()
    return lowered in {"1", "true", "yes", "y"}


def _collect_footnotes(root: ET.Element) -> list[str]:
    notes: list[str] = []
    for note in root.findall("footnotes/footnote"):
        if note.text is None:
            continue
        text = note.text.strip()
        if text:
            notes.append(text)
    return notes


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

    remarks = _text(root, "remarks")
    footnotes = _collect_footnotes(root)
    joined_notes = " ".join([remarks or "", *footnotes])

    return Form4Facts(
        issuer_cik=issuer_cik,
        issuer_name=_text(root, "issuer/issuerName"),
        issuer_symbol=_text(root, "issuer/issuerTradingSymbol"),
        reporting_owner_name=_text(root, "reportingOwner/reportingOwnerId/rptOwnerName"),
        reporting_owner_cik=_text(root, "reportingOwner/reportingOwnerId/rptOwnerCik"),
        is_director=_to_bool(_text(root, "reportingOwner/reportingOwnerRelationship/isDirector")),
        is_officer=_to_bool(_text(root, "reportingOwner/reportingOwnerRelationship/isOfficer")),
        officer_title=_text(root, "reportingOwner/reportingOwnerRelationship/officerTitle"),
        transactions=transactions,
        is_ten_percent_owner=_to_bool(
            _text(root, "reportingOwner/reportingOwnerRelationship/isTenPercentOwner")
        ),
        is_other=_to_bool(_text(root, "reportingOwner/reportingOwnerRelationship/isOther")),
        other_text=_text(root, "reportingOwner/reportingOwnerRelationship/otherText"),
        remarks=remarks,
        footnotes=footnotes,
        has_10b5_1_plan=TEN_B_FIVE_ONE_RE.search(joined_notes) is not None,
        has_13d_reference=THIRTEEN_D_RE.search(joined_notes) is not None,
        has_equity_comp_event=EQUITY_COMP_RE.search(joined_notes) is not None,
        has_tax_withholding_language=TAX_WITHHOLDING_RE.search(joined_notes) is not None,
    )
