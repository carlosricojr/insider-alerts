from __future__ import annotations

import re

XML_LINK_RE = re.compile(r"href=[\"'](?P<href>[^\"']+\.xml)[\"']", re.IGNORECASE)


def _absolute_sec_url(href: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return f"https://www.sec.gov{href}"
    return f"https://www.sec.gov/{href}"


def locate_form4_xml_url(filing_detail_html: str) -> str | None:
    for match in XML_LINK_RE.finditer(filing_detail_html):
        href = match.group("href")
        lowered = href.lower()
        if "ownership" in lowered or "form4" in lowered or "f345" in lowered:
            return _absolute_sec_url(href)
    fallback_match = XML_LINK_RE.search(filing_detail_html)
    if fallback_match:
        return _absolute_sec_url(fallback_match.group("href"))
    return None
