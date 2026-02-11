from __future__ import annotations

from dataclasses import dataclass

from insider_alerts.config import Settings
from insider_alerts.sec.client import SecHttpClient
from insider_alerts.sec.rss import parse_form4_rss
from insider_alerts.sec.store import StoreResult, upsert_filing_refs


@dataclass(slots=True)
class PollResult:
    fetched: int
    inserted: int
    skipped_existing: int


def run_sec_poll_once(settings: Settings, *, max_items: int, dry_run: bool) -> PollResult:
    client = SecHttpClient(settings)
    rss_text = client.get_text(settings.sec_rss_url)
    refs = parse_form4_rss(rss_text, max_items=max_items)

    if dry_run:
        return PollResult(fetched=len(refs), inserted=0, skipped_existing=0)

    result: StoreResult = upsert_filing_refs(settings.database_path, refs)
    return PollResult(
        fetched=len(refs),
        inserted=result.inserted,
        skipped_existing=result.skipped_existing,
    )
