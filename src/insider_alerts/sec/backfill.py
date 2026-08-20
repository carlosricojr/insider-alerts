from __future__ import annotations

import re
from datetime import UTC, date, datetime

from insider_alerts.sec.models import FilingRef

FORM4_TYPES = {"4", "4/A"}
ACCESSION_RE = re.compile(r"\b(\d{10}-\d{2}-\d{6})\b")


def _quarter_for_day(day: date) -> tuple[int, int]:
    quarter = ((day.month - 1) // 3) + 1
    return day.year, quarter


def iter_quarter_range(start_date: date, end_date: date) -> list[tuple[int, int]]:
    if start_date > end_date:
        return []

    year, quarter = _quarter_for_day(start_date)
    end_year, end_quarter = _quarter_for_day(end_date)

    quarters: list[tuple[int, int]] = []
    while (year, quarter) <= (end_year, end_quarter):
        quarters.append((year, quarter))
        quarter += 1
        if quarter > 4:
            quarter = 1
            year += 1
    return quarters


def master_index_url(year: int, quarter: int) -> str:
    return f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.idx"


def _normalize_cik(raw: str) -> str | None:
    digits = "".join(char for char in raw if char.isdigit())
    if not digits:
        return None
    if len(digits) > 10:
        digits = digits[-10:]
    return digits.zfill(10)


def _filing_detail_url_from_filename(filename: str) -> str:
    normalized = filename.strip().lstrip("/")
    if normalized.lower().endswith(".txt"):
        normalized = normalized[:-4]
    return f"https://www.sec.gov/Archives/{normalized}-index.html"


def _primary_doc_url_from_filename(filename: str) -> str:
    normalized = filename.strip().lstrip("/")
    return f"https://www.sec.gov/Archives/{normalized}"


def parse_form4_master_idx(
    index_text: str,
    *,
    start_date: date,
    end_date: date,
) -> list[FilingRef]:
    refs: list[FilingRef] = []

    for raw_line in index_text.splitlines():
        line = raw_line.strip()
        if not line or "|" not in line:
            continue
        columns = [value.strip() for value in line.split("|", 4)]
        if len(columns) != 5:
            continue

        cik_raw, company_name, form_type_raw, filed_on_raw, filename_raw = columns
        form_type = form_type_raw.upper()
        if form_type not in FORM4_TYPES:
            continue

        try:
            filed_on = date.fromisoformat(filed_on_raw)
        except ValueError:
            continue
        if filed_on < start_date or filed_on > end_date:
            continue

        cik = _normalize_cik(cik_raw)
        if cik is None:
            continue

        accession_match = ACCESSION_RE.search(filename_raw)
        if accession_match is None:
            continue
        accession = accession_match.group(1)

        refs.append(
            FilingRef(
                source="sec_master_index",
                cik=cik,
                accession_number=accession,
                form_type=form_type,
                filed_at=datetime(
                    filed_on.year,
                    filed_on.month,
                    filed_on.day,
                    tzinfo=UTC,
                ),
                filing_detail_url=_filing_detail_url_from_filename(filename_raw),
                primary_doc_url=_primary_doc_url_from_filename(filename_raw),
                raw_rss_entry={
                    "company_name": company_name,
                    "form_type": form_type,
                    "date_filed": filed_on.isoformat(),
                    "filename": filename_raw.strip(),
                },
            )
        )

    return refs
