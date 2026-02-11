from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class FilingRef:
    source: str
    cik: str
    accession_number: str
    form_type: str
    filed_at: datetime
    filing_detail_url: str
    primary_doc_url: str | None
    raw_rss_entry: dict[str, str]
