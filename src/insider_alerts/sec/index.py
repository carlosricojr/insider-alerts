from __future__ import annotations

import re

XML_LINK_RE = re.compile(r"href=[\"'](?P<href>[^\"']+\.xml)[\"']", re.IGNORECASE)


def _absolute_sec_url(href: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return f"https://www.sec.gov{href}"
    return f"https://www.sec.gov/{href}"


def _is_form4_like(url: str) -> bool:
    lowered = url.lower()
    return "ownership" in lowered or "form4" in lowered or "f345" in lowered


def _is_xsl_transformed(url: str) -> bool:
    return "/xsl" in url.lower()


def locate_form4_xml_url(filing_detail_html: str) -> str | None:
    candidates = [
        _absolute_sec_url(match.group("href"))
        for match in XML_LINK_RE.finditer(filing_detail_html)
    ]
    if not candidates:
        return None

    for candidate in candidates:
        if _is_form4_like(candidate) and not _is_xsl_transformed(candidate):
            return candidate

    for candidate in candidates:
        if not _is_xsl_transformed(candidate):
            return candidate

    for candidate in candidates:
        if _is_form4_like(candidate):
            return candidate

    if candidates:
        return candidates[0]
    return None
