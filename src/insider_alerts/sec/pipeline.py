from __future__ import annotations

import re
from dataclasses import dataclass

from insider_alerts.config import Settings
from insider_alerts.review.queue import enqueue_review_packet, ensure_review_tables
from insider_alerts.review.scoring import score_form4_signal
from insider_alerts.sec.client import SecHttpClient, SecHttpError
from insider_alerts.sec.form4 import Form4ParseError, parse_form4_xml
from insider_alerts.sec.index import locate_form4_xml_url
from insider_alerts.sec.rss import parse_form4_rss
from insider_alerts.sec.store import (
    StoreResult,
    list_filings_missing_xml,
    update_form4_xml_url,
    upsert_filing_refs,
)

XSL_SEGMENT_RE = re.compile(r"/xsl[^/]+/", re.IGNORECASE)


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


def _normalize_form4_xml_url(url: str) -> str:
    return XSL_SEGMENT_RE.sub("/", url, count=1)


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
            xml_url = _normalize_form4_xml_url(ref.filing_detail_url)
        else:
            html = client.get_text(ref.filing_detail_url)
            maybe = locate_form4_xml_url(html)
            if maybe is None:
                continue
            xml_url = _normalize_form4_xml_url(maybe)
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

    ensure_review_tables(settings.database_path)
    with connect(settings.database_path) as conn:
        conn.row_factory = __import__("sqlite3").Row
        rows = conn.execute(
            """
            SELECT f.source, f.cik, f.accession_number, f.form_type, f.filed_at,
                   f.filing_detail_url,
                   f.primary_doc_url, f.raw_rss_entry, f.form4_xml_url
            FROM filings AS f
            LEFT JOIN review_packets AS rp
              ON rp.packet_id = f.accession_number || '|' || f.cik || '|' || f.form_type
            WHERE f.form4_xml_url IS NOT NULL
              AND rp.packet_id IS NULL
            ORDER BY f.filed_at DESC, f.cik ASC
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

    seen_filing_keys: set[tuple[str, str]] = set()
    for row in rows:
        accession_number = str(row["accession_number"])
        form_type = str(row["form_type"])
        filing_key = (accession_number, form_type)
        if filing_key in seen_filing_keys:
            continue
        seen_filing_keys.add(filing_key)

        processed += 1
        xml_url = _normalize_form4_xml_url(str(row["form4_xml_url"]))
        try:
            xml_text = client.get_text(xml_url)
            facts = parse_form4_xml(xml_text)
        except (SecHttpError, Form4ParseError):
            continue
        score = score_form4_signal(facts)
        ref = FilingRef(
            source=str(row["source"]),
            cik=str(row["cik"]),
            accession_number=accession_number,
            form_type=form_type,
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
