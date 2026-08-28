from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

from insider_alerts.sec.models import FilingRef

ACCESSION_RE = re.compile(r"\d{10}-\d{2}-\d{6}")
ACCESSION_COMPACT_RE = re.compile(r"\b(\d{18})\b")
CIK_LABELED_RE = re.compile(r"\bCIK\s*[:=]?\s*(\d{1,10})\b", re.IGNORECASE)
CIK_ANY_RE = re.compile(r"\b(\d{10})\b")
CIK_IN_URL_RE = re.compile(r"/data/(\d{1,10})/", re.IGNORECASE)
FORM_TITLE_PREFIX_RE = re.compile(
    r"^\s*(?P<form_type>[A-Z0-9][A-Z0-9./-]*)\s*-",
    re.IGNORECASE,
)
FORM4_TYPES = frozenset({"4", "4/A"})


class SecRssParseError(RuntimeError):
    """Raised for malformed RSS payloads."""


@dataclass(frozen=True, slots=True)
class RssParseResult:
    refs: list[FilingRef]
    items_seen: int
    source_boundary_rejected: int
    invalid_items: int


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


def _extract_form4_type(title: str | None, category_terms: list[str]) -> str | None:
    """Return an exact Form 4 identity from authoritative feed fields.

    SEC's ``type=4`` current-filings endpoint uses prefix matching, so the feed can contain
    forms such as 424B3, 485BPOS, and 497. Descriptions are metadata and may contain strings
    such as ``Size: 4 MB``; they must never determine the filing type.
    """
    if title:
        title_match = FORM_TITLE_PREFIX_RE.match(title)
        if title_match is not None:
            title_form = title_match.group("form_type").upper()
            return title_form if title_form in FORM4_TYPES else None

    exact_category_types = {
        term.strip().upper() for term in category_terms if term.strip().upper() in FORM4_TYPES
    }
    if len(exact_category_types) == 1:
        return next(iter(exact_category_types))
    return None


def _has_explicit_form_identity(title: str | None, category_terms: list[str]) -> bool:
    return bool((title and FORM_TITLE_PREFIX_RE.match(title)) or category_terms)


def parse_form4_rss_with_diagnostics(
    xml_text: str,
    *,
    max_items: int | None = None,
) -> RssParseResult:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise SecRssParseError(f"invalid RSS payload: {exc}") from exc

    items = _iter_feed_items(root)

    refs: list[FilingRef] = []
    items_seen = 0
    source_boundary_rejected = 0
    invalid_items = 0
    for item in items:
        items_seen += 1
        title = _first_child_text(item, "title")
        link = _extract_link(item)
        pub_date = _first_child_text(item, "pubDate", "updated", "published")
        description = _first_child_text(item, "description", "summary", "content") or ""
        guid = _first_child_text(item, "guid", "id") or ""
        category_terms = [
            value
            for node in _children_by_name(item, "category")
            for value in [
                node.attrib.get("term", "").strip(),
                (node.text or "").strip(),
            ]
            if value
        ]

        joined = " ".join(part for part in [title, description, guid, *category_terms] if part)
        accession = _extract_accession(title, description, guid, link)
        cik = _extract_cik(joined, link)
        form_type = _extract_form4_type(title, category_terms)

        if form_type is None:
            if _has_explicit_form_identity(title, category_terms):
                source_boundary_rejected += 1
            else:
                invalid_items += 1
            continue
        if accession is None or cik is None or link is None:
            invalid_items += 1
            continue

        filed_at = _parse_datetime(pub_date)
        if filed_at is None:
            invalid_items += 1
            continue

        refs.append(
            FilingRef(
                source="sec_rss",
                cik=cik,
                accession_number=accession,
                form_type=form_type,
                filed_at=filed_at,
                filing_detail_url=link,
                primary_doc_url=None,
                raw_rss_entry={
                    "title": title or "",
                    "link": link,
                    "pubDate": pub_date or "",
                    "description": description,
                    "guid": guid,
                    "category": " ".join(category_terms),
                    "feed_form_type": form_type,
                },
            )
        )

        if max_items is not None and len(refs) >= max_items:
            break

    return RssParseResult(
        refs=refs,
        items_seen=items_seen,
        source_boundary_rejected=source_boundary_rejected,
        invalid_items=invalid_items,
    )


def parse_form4_rss(xml_text: str, *, max_items: int | None = None) -> list[FilingRef]:
    return parse_form4_rss_with_diagnostics(xml_text, max_items=max_items).refs
