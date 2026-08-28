from __future__ import annotations

import re
from dataclasses import dataclass, field
from html.parser import HTMLParser
from urllib.parse import urljoin, urlsplit

XML_LINK_RE = re.compile(r"href=[\"'](?P<href>[^\"']+\.xml)[\"']", re.IGNORECASE)
HOST_LABEL_RE = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", re.IGNORECASE)


@dataclass(slots=True)
class _Cell:
    text_parts: list[str] = field(default_factory=list)
    hrefs: list[str] = field(default_factory=list)

    @property
    def text(self) -> str:
        return " ".join(" ".join(self.text_parts).split())


@dataclass(frozen=True, slots=True)
class Form4XmlLocation:
    url: str | None
    recognized_document_table: bool


class _DocumentTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tables: list[list[list[_Cell]]] = []
        self.all_hrefs: list[str] = []
        self._current_table: list[list[_Cell]] | None = None
        self._current_row: list[_Cell] | None = None
        self._current_cell: _Cell | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        normalized_tag = tag.casefold()
        attr_map = {name.casefold(): value or "" for name, value in attrs}
        if normalized_tag == "table" and self._current_table is None:
            summary = " ".join(attr_map.get("summary", "").split()).casefold()
            if summary == "document format files":
                self._current_table = []
                self.tables.append(self._current_table)
        elif normalized_tag == "tr" and self._current_table is not None:
            self._current_row = []
        elif normalized_tag in {"td", "th"} and self._current_row is not None:
            self._current_cell = _Cell()
            self._current_row.append(self._current_cell)

        if normalized_tag == "a":
            href = attr_map.get("href", "").strip()
            if href:
                self.all_hrefs.append(href)
                if self._current_cell is not None:
                    self._current_cell.hrefs.append(href)

    def handle_data(self, data: str) -> None:
        if self._current_cell is not None and data.strip():
            self._current_cell.text_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        normalized_tag = tag.casefold()
        if normalized_tag in {"td", "th"}:
            self._current_cell = None
        elif normalized_tag == "tr" and self._current_table is not None:
            if self._current_row:
                self._current_table.append(self._current_row)
            self._current_row = None
            self._current_cell = None
        elif normalized_tag == "table" and self._current_table is not None:
            self._current_table = None
            self._current_row = None
            self._current_cell = None


def _absolute_sec_url(href: str, filing_detail_url: str | None) -> str | None:
    try:
        absolute = urljoin(filing_detail_url or "https://www.sec.gov/", href)
        parsed = urlsplit(absolute)
        hostname = (parsed.hostname or "").casefold()
        port = parsed.port
    except ValueError:
        return None
    if parsed.scheme.casefold() != "https":
        return None
    if hostname == "sec.gov":
        valid_hostname = True
    elif hostname.endswith(".sec.gov"):
        subdomain = hostname.removesuffix(".sec.gov")
        valid_hostname = bool(subdomain) and all(
            HOST_LABEL_RE.fullmatch(label) for label in subdomain.split(".")
        )
    else:
        valid_hostname = False
    if not valid_hostname:
        return None
    if port not in {None, 443} or parsed.username is not None or parsed.password is not None:
        return None
    canonical_authority = hostname if port is None else f"{hostname}:{port}"
    if parsed.netloc.casefold() != canonical_authority:
        return None
    return absolute


def validate_sec_url(url: str) -> str | None:
    """Return a normalized HTTPS SEC URL, or ``None`` when it crosses the boundary."""

    try:
        parsed = urlsplit(url)
    except ValueError:
        return None
    if not parsed.scheme or not parsed.netloc:
        return None
    return _absolute_sec_url(url, None)


def _is_form4_like(url: str) -> bool:
    lowered = url.lower()
    return "ownership" in lowered or "form4" in lowered or "f345" in lowered


def _is_xsl_transformed(url: str) -> bool:
    return "/xsl" in url.lower()


def _is_xml_link(href: str) -> bool:
    try:
        return urlsplit(href).path.lower().endswith(".xml")
    except ValueError:
        return False


def _prefer_raw(candidates: list[str]) -> str | None:
    for candidate in candidates:
        if not _is_xsl_transformed(candidate):
            return candidate
    return candidates[0] if candidates else None


def _table_form4_candidates(parser: _DocumentTableParser) -> tuple[list[str], bool]:
    candidates: list[str] = []
    recognized_document_table = False
    for table in parser.tables:
        type_index: int | None = None
        header_index: int | None = None
        for row_index, row in enumerate(table):
            for cell_index, cell in enumerate(row):
                if cell.text.strip().casefold() == "type":
                    type_index = cell_index
                    header_index = row_index
                    break
            if type_index is not None:
                break
        if type_index is None or header_index is None:
            continue
        if len(table) <= header_index + 1:
            continue
        recognized_document_table = True

        for row in table[header_index + 1 :]:
            if len(row) <= type_index or row[type_index].text.strip().upper() not in {"4", "4/A"}:
                continue
            candidates.extend(href for cell in row for href in cell.hrefs if _is_xml_link(href))
    return candidates, recognized_document_table


def locate_form4_xml(
    filing_detail_html: str,
    *,
    filing_detail_url: str | None = None,
) -> Form4XmlLocation:
    parser = _DocumentTableParser()
    parser.feed(filing_detail_html)

    if parser.tables:
        table_candidates, recognized_document_table = _table_form4_candidates(parser)
        candidates = [
            absolute
            for href in table_candidates
            for absolute in [_absolute_sec_url(href, filing_detail_url)]
            if absolute is not None
        ]
        return Form4XmlLocation(
            url=_prefer_raw(candidates),
            recognized_document_table=recognized_document_table,
        )

    candidates = [
        absolute
        for href in parser.all_hrefs
        if _is_xml_link(href) and _is_form4_like(href)
        for absolute in [_absolute_sec_url(href, filing_detail_url)]
        if absolute is not None
    ]
    if not candidates:
        # Retain compatibility with malformed-but-parseable HTML in sparse legacy fixtures.
        candidates = [
            absolute
            for match in XML_LINK_RE.finditer(filing_detail_html)
            if _is_form4_like(match.group("href"))
            for absolute in [_absolute_sec_url(match.group("href"), filing_detail_url)]
            if absolute is not None
        ]

    return Form4XmlLocation(
        url=_prefer_raw(candidates),
        recognized_document_table=False,
    )


def locate_form4_xml_url(
    filing_detail_html: str,
    *,
    filing_detail_url: str | None = None,
) -> str | None:
    return locate_form4_xml(
        filing_detail_html,
        filing_detail_url=filing_detail_url,
    ).url
