from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

from insider_alerts.sec.models import FilingRef

ACCESSION_RE = re.compile(r"\d{10}-\d{2}-\d{6}")
CIK_RE = re.compile(r"\b(\d{10})\b")
FORM_TYPE_RE = re.compile(r"\b4(?:/A)?\b", re.IGNORECASE)


class SecRssParseError(RuntimeError):
    """Raised for malformed RSS payloads."""


def _extract_text(parent: ET.Element, tag_name: str) -> str | None:
    child = parent.find(tag_name)
    if child is None or child.text is None:
        return None
    text = child.text.strip()
    return text if text else None


def _parse_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        dt = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def parse_form4_rss(xml_text: str, *, max_items: int | None = None) -> list[FilingRef]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise SecRssParseError(f"invalid RSS payload: {exc}") from exc

    channel = root.find("channel")
    if channel is None:
        raise SecRssParseError("missing channel element")

    refs: list[FilingRef] = []
    items = channel.findall("item")
    for item in items:
        title = _extract_text(item, "title")
        link = _extract_text(item, "link")
        pub_date = _extract_text(item, "pubDate")
        description = _extract_text(item, "description") or ""
        guid = _extract_text(item, "guid") or ""

        joined = " ".join(part for part in [title, link, description, guid] if part)

        accession_match = ACCESSION_RE.search(joined)
        cik_match = CIK_RE.search(joined)
        form_match = FORM_TYPE_RE.search(joined)

        if not accession_match or not cik_match or not form_match or not link:
            continue

        filed_at = _parse_datetime(pub_date)
        if filed_at is None:
            continue

        refs.append(
            FilingRef(
                source="sec_rss",
                cik=cik_match.group(1),
                accession_number=accession_match.group(0),
                form_type=form_match.group(0).upper(),
                filed_at=filed_at,
                filing_detail_url=link,
                primary_doc_url=None,
                raw_rss_entry={
                    "title": title or "",
                    "link": link,
                    "pubDate": pub_date or "",
                    "description": description,
                    "guid": guid,
                },
            )
        )

        if max_items is not None and len(refs) >= max_items:
            break

    return refs
