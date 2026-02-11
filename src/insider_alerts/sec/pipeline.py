from __future__ import annotations

from dataclasses import dataclass

from insider_alerts.config import Settings
from insider_alerts.review.queue import enqueue_review_packet
from insider_alerts.review.scoring import score_form4_signal
from insider_alerts.sec.client import SecHttpClient
from insider_alerts.sec.form4 import parse_form4_xml
from insider_alerts.sec.index import locate_form4_xml_url
from insider_alerts.sec.rss import parse_form4_rss
from insider_alerts.sec.store import (
    StoreResult,
    list_filings_missing_xml,
    update_form4_xml_url,
    upsert_filing_refs,
)


@dataclass(slots=True)
class PollResult:
    fetched: int
    inserted: int
    skipped_existing: int


@dataclass(slots=True)
class EnrichResult:
    scanned: int
    updated: int


@dataclass(slots=True)
class QueueResult:
    processed: int
    enqueued: int


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


def enrich_filings_with_xml_url(settings: Settings, *, limit: int) -> EnrichResult:
    client = SecHttpClient(settings)
    refs = list_filings_missing_xml(settings.database_path, limit=limit)

    updated = 0
    for ref in refs:
        if ref.filing_detail_url.lower().endswith(".xml"):
            xml_url = ref.filing_detail_url
        else:
            html = client.get_text(ref.filing_detail_url)
            maybe = locate_form4_xml_url(html)
            if maybe is None:
                continue
            xml_url = maybe
        updated += update_form4_xml_url(
            settings.database_path,
            accession_number=ref.accession_number,
            cik=ref.cik,
            form_type=ref.form_type,
            xml_url=xml_url,
        )

    return EnrichResult(scanned=len(refs), updated=updated)


def enqueue_review_packets(settings: Settings, *, limit: int) -> QueueResult:
    from sqlite3 import connect

    with connect(settings.database_path) as conn:
        conn.row_factory = __import__("sqlite3").Row
        rows = conn.execute(
            """
            SELECT source, cik, accession_number, form_type, filed_at, filing_detail_url,
                   primary_doc_url, raw_rss_entry, form4_xml_url
            FROM filings
            WHERE form4_xml_url IS NOT NULL
            ORDER BY filed_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    client = SecHttpClient(settings)
    processed = 0
    enqueued = 0
    import json
    from datetime import datetime

    from insider_alerts.sec.models import FilingRef

    for row in rows:
        processed += 1
        xml_url = str(row["form4_xml_url"])
        xml_text = client.get_text(xml_url)
        facts = parse_form4_xml(xml_text)
        score = score_form4_signal(facts)
        ref = FilingRef(
            source=str(row["source"]),
            cik=str(row["cik"]),
            accession_number=str(row["accession_number"]),
            form_type=str(row["form_type"]),
            filed_at=datetime.fromisoformat(str(row["filed_at"])),
            filing_detail_url=str(row["filing_detail_url"]),
            primary_doc_url=str(row["primary_doc_url"]) if row["primary_doc_url"] else None,
            raw_rss_entry=json.loads(str(row["raw_rss_entry"])),
        )
        packet = {
            "xml_url": xml_url,
            "score": score.score,
            "rationale": score.rationale,
            "issuer_symbol": facts.issuer_symbol,
            "owner": facts.reporting_owner_name,
        }
        if enqueue_review_packet(settings.database_path, ref, packet):
            enqueued += 1

    return QueueResult(processed=processed, enqueued=enqueued)
