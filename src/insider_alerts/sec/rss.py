from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

from insider_alerts.sec.models import FilingRef

ACCESSION_RE = re.compile(r"\d{10}-\d{2}-\d{6}")
ACCESSION_COMPACT_RE = re.compile(r"\b(\d{18})\b")
CIK_LABELED_RE = re.compile(r"\bCIK\s*[:=]?\s*(\d{1,10})\b", re.IGNORECASE)
CIK_ANY_RE = re.compile(r"\b(\d{10})\b")
CIK_IN_URL_RE = re.compile(r"/data/(\d{1,10})/", re.IGNORECASE)
FORM_TYPE_RE = re.compile(r"\b4(?:/A)?\b", re.IGNORECASE)


class SecRssParseError(RuntimeError):
    """Raised for malformed RSS payloads."""


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


def _children_by_name(parent: ET.Element, tag_name: str) -> list[ET.Element]:
    return [child for child in list(parent) if _local_name(child.tag) == tag_name]


def _first_child_text(parent: ET.Element, *tag_names: str) -> str | None:
    for tag_name in tag_names:
        for child in _children_by_name(parent, tag_name):
            if child.text is None:
                continue
            text = child.text.strip()
            if text:
                return text
    return None


def _extract_link(parent: ET.Element) -> str | None:
    for link_node in _children_by_name(parent, "link"):
        href = link_node.attrib.get("href")
        if href is not None:
            href = href.strip()
            if href:
                return href
        if link_node.text is not None:
            text = link_node.text.strip()
            if text:
                return text
    return None


def _parse_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _normalize_accession(value: str) -> str | None:
    if ACCESSION_RE.fullmatch(value):
        return value
    if len(value) == 18 and value.isdigit():
        return f"{value[0:10]}-{value[10:12]}-{value[12:18]}"
    return None


def _extract_accession(*parts: str | None) -> str | None:
    joined = " ".join(part for part in parts if part)
    match = ACCESSION_RE.search(joined)
    if match is not None:
        return match.group(0)
    compact = ACCESSION_COMPACT_RE.search(joined)
    if compact is None:
        return None
    return _normalize_accession(compact.group(1))


def _extract_cik(joined: str, link: str | None) -> str | None:
    if link is not None:
        in_url = CIK_IN_URL_RE.search(link)
        if in_url is not None:
            return in_url.group(1).zfill(10)

    labeled = CIK_LABELED_RE.search(joined)
    if labeled is not None:
        return labeled.group(1).zfill(10)

    any_cik = CIK_ANY_RE.search(joined)
    if any_cik is not None:
        return any_cik.group(1).zfill(10)

    return None


def _iter_feed_items(root: ET.Element) -> list[ET.Element]:
    root_name = _local_name(root.tag).lower()
    if root_name == "feed":
        return _children_by_name(root, "entry")
    if root_name == "rss":
        channels = _children_by_name(root, "channel")
        if not channels:
            raise SecRssParseError("missing channel element")
        return _children_by_name(channels[0], "item")

    channels = _children_by_name(root, "channel")
    if channels:
        return _children_by_name(channels[0], "item")
    entries = _children_by_name(root, "entry")
    if entries:
        return entries
    raise SecRssParseError(f"unsupported feed root element: {root_name}")


def parse_form4_rss(xml_text: str, *, max_items: int | None = None) -> list[FilingRef]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise SecRssParseError(f"invalid RSS payload: {exc}") from exc

    items = _iter_feed_items(root)

    refs: list[FilingRef] = []
    for item in items:
        title = _first_child_text(item, "title")
        link = _extract_link(item)
        pub_date = _first_child_text(item, "pubDate", "updated", "published")
        description = _first_child_text(item, "description", "summary", "content") or ""
        guid = _first_child_text(item, "guid", "id") or ""
        category_terms = " ".join(
            term
            for node in _children_by_name(item, "category")
            for term in [node.attrib.get("term", "").strip()]
            if term
        )

        joined = " ".join(part for part in [title, description, guid, category_terms] if part)
        accession = _extract_accession(title, description, guid, link)
        cik = _extract_cik(joined, link)
        form_match = FORM_TYPE_RE.search(" ".join([joined, link or ""]))

        if accession is None or cik is None or form_match is None or link is None:
            continue

        filed_at = _parse_datetime(pub_date)
        if filed_at is None:
            continue

        refs.append(
            FilingRef(
                source="sec_rss",
                cik=cik,
                accession_number=accession,
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
