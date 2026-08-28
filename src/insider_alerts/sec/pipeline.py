from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from urllib.parse import urlsplit, urlunsplit

from insider_alerts.config import Settings
from insider_alerts.review.market_context import (
    DailyMarketDataClient,
    MarketContextError,
    MarketSnapshot,
    get_market_snapshot,
    upsert_market_snapshot,
)
from insider_alerts.review.queue import (
    enqueue_review_packets_batch,
    ensure_review_tables,
)
from insider_alerts.review.scoring import score_form4_signal
from insider_alerts.sec.backfill import iter_quarter_range, master_index_url, parse_form4_master_idx
from insider_alerts.sec.client import SecHttpClient, SecHttpError
from insider_alerts.sec.form4 import Form4ParseError, parse_form4_xml
from insider_alerts.sec.index import locate_form4_xml, validate_sec_url
from insider_alerts.sec.rss import parse_form4_rss_with_diagnostics
from insider_alerts.sec.store import (
    StoreResult,
    form4_source_boundary_sql,
    init_db,
    list_filings_missing_xml,
    record_sec_processing_rejections,
    update_form4_xml_urls,
    upsert_filing_refs,
)

logger = logging.getLogger(__name__)

XSL_SEGMENT_RE = re.compile(r"/xsl[^/]+/", re.IGNORECASE)
TERMINAL_NON_FORM4_XML_ROOTS = frozenset({"linkbase", "schema", "xbrl"})


@dataclass(slots=True)
class PollResult:
    fetched: int
    inserted: int
    skipped_existing: int
    source_items_seen: int = 0
    source_boundary_rejected: int = 0
    source_invalid_items: int = 0


@dataclass(slots=True)
class EnrichResult:
    scanned: int
    updated: int
    http_failed: int = 0
    xml_not_found: int = 0


@dataclass(slots=True)
class QueueResult:
    processed: int
    enqueued: int
    skipped_existing: int = 0
    http_failed: int = 0
    parse_failed: int = 0
    market_failed: int = 0


@dataclass(slots=True)
class BackfillResult:
    requested_quarters: int
    fetched_quarters: int
    matched_filings: int
    inserted: int
    skipped_existing: int


def _normalize_form4_xml_url(url: str) -> str:
    parsed = urlsplit(url)
    normalized_path = XSL_SEGMENT_RE.sub("/", parsed.path, count=1)
    return urlunsplit(
        (parsed.scheme, parsed.netloc, normalized_path, parsed.query, parsed.fragment)
    )


def _is_terminal_non_form4_xml(xml_text: str) -> bool:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return False
    local_name = root.tag.rsplit("}", 1)[-1].casefold()
    return local_name in TERMINAL_NON_FORM4_XML_ROOTS


def run_sec_poll_once(settings: Settings, *, max_items: int, dry_run: bool) -> PollResult:
    client = SecHttpClient(settings)
    rss_text = client.get_text(settings.sec_rss_url)
    parsed = parse_form4_rss_with_diagnostics(rss_text, max_items=max_items)
    refs = parsed.refs

    if dry_run:
        return PollResult(
            fetched=len(refs),
            inserted=0,
            skipped_existing=0,
            source_items_seen=parsed.items_seen,
            source_boundary_rejected=parsed.source_boundary_rejected,
            source_invalid_items=parsed.invalid_items,
        )

    result: StoreResult = upsert_filing_refs(settings.database_path, refs)
    return PollResult(
        fetched=len(refs),
        inserted=result.inserted,
        skipped_existing=result.skipped_existing,
        source_items_seen=parsed.items_seen,
        source_boundary_rejected=parsed.source_boundary_rejected,
        source_invalid_items=parsed.invalid_items,
    )


def enrich_filings_with_xml_url(
    settings: Settings,
    *,
    limit: int,
    progress_callback: Callable[[str], None] | None = None,
) -> EnrichResult:
    client = SecHttpClient(settings)
    refs = list_filings_missing_xml(settings.database_path, limit=limit)

    pending_updates: list[tuple[str, str, str, str]] = []
    terminal_rejections: list[tuple[str, str, str, str, str]] = []
    http_failed = 0
    xml_not_found = 0
    for index, ref in enumerate(refs):
        if progress_callback is not None:
            progress_callback(f"enrichment_item_{index}_started")
        validated_detail_url = validate_sec_url(ref.filing_detail_url)
        if validated_detail_url is None:
            xml_not_found += 1
            logger.warning(
                "SEC filing URL rejected for accession=%s url=%s",
                ref.accession_number,
                ref.filing_detail_url,
            )
            terminal_rejections.append(
                (
                    ref.accession_number,
                    ref.cik,
                    ref.form_type,
                    ref.filing_detail_url,
                    "url_rejected",
                )
            )
            continue
        if urlsplit(validated_detail_url).path.lower().endswith(".xml"):
            xml_url = _normalize_form4_xml_url(validated_detail_url)
        else:
            try:
                html = client.get_text(validated_detail_url)
            except SecHttpError as exc:
                http_failed += 1
                logger.warning(
                    "SEC detail enrichment failed for accession=%s url=%s: %s",
                    ref.accession_number,
                    ref.filing_detail_url,
                    exc,
                )
                continue
            location = locate_form4_xml(html, filing_detail_url=validated_detail_url)
            if location.url is None:
                xml_not_found += 1
                if location.recognized_document_table:
                    terminal_rejections.append(
                        (
                            ref.accession_number,
                            ref.cik,
                            ref.form_type,
                            ref.filing_detail_url,
                            "xml_not_found",
                        )
                    )
                continue
            xml_url = _normalize_form4_xml_url(location.url)
        pending_updates.append(
            (
                ref.accession_number,
                ref.cik,
                ref.form_type,
                xml_url,
            )
        )

    if progress_callback is not None:
        progress_callback("enrichment_items_completed")

    record_sec_processing_rejections(
        settings.database_path,
        stage="xml_enrichment",
        rejections=terminal_rejections,
    )
    updated = update_form4_xml_urls(settings.database_path, updates=pending_updates)
    return EnrichResult(
        scanned=len(refs),
        updated=updated,
        http_failed=http_failed,
        xml_not_found=xml_not_found,
    )


def enqueue_review_packets(
    settings: Settings,
    *,
    limit: int,
    oldest_first: bool = False,
    start_date: date | None = None,
    end_date: date | None = None,
    progress_callback: Callable[[str], None] | None = None,
) -> QueueResult:
    from sqlite3 import connect

    init_db(settings.database_path)
    ensure_review_tables(settings.database_path)
    where_parts = [
        "f.form4_xml_url IS NOT NULL",
        "f.form4_xml_url <> ''",
        "f.form_type IN ('4', '4/A')",
        form4_source_boundary_sql("f"),
        """
        NOT EXISTS (
            SELECT 1
            FROM sec_processing_rejections AS rejection
            WHERE rejection.stage = 'review_xml'
              AND rejection.accession_number = f.accession_number
              AND rejection.cik = f.cik
              AND rejection.form_type = f.form_type
              AND rejection.input_url = f.form4_xml_url
        )
        """,
    ]
    params: list[object] = []
    if start_date is not None:
        where_parts.append("date(f.filed_at) >= ?")
        params.append(start_date.isoformat())
    if end_date is not None:
        where_parts.append("date(f.filed_at) <= ?")
        params.append(end_date.isoformat())
    where_clause = " AND ".join(where_parts)
    order_direction = "ASC" if oldest_first else "DESC"

    with connect(settings.database_path) as conn:
        conn.row_factory = __import__("sqlite3").Row
        rows = conn.execute(
            f"""
            WITH deduped AS (
                SELECT f.source, f.cik, f.accession_number, f.form_type, f.filed_at,
                       f.filing_detail_url, f.primary_doc_url, f.raw_rss_entry, f.form4_xml_url,
                       ROW_NUMBER() OVER (
                           PARTITION BY f.accession_number, f.form_type
                           ORDER BY f.filed_at DESC, f.cik ASC, f.source ASC
                       ) AS rn
                FROM filings AS f
                WHERE {where_clause}
            )
            SELECT d.source, d.cik, d.accession_number, d.form_type, d.filed_at,
                   d.filing_detail_url, d.primary_doc_url, d.raw_rss_entry, d.form4_xml_url
            FROM deduped AS d
            WHERE d.rn = 1
              AND NOT EXISTS (
                  SELECT 1
                  FROM review_packets AS rp
                  WHERE rp.accession_number = d.accession_number
                    AND rp.form_type = d.form_type
              )
            ORDER BY d.filed_at {order_direction}, d.cik ASC
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()

    client = SecHttpClient(settings)
    market_client = DailyMarketDataClient(
        user_agent=settings.sec_user_agent,
        timeout_seconds=settings.market_data_timeout_seconds,
        rate_limit_per_second=settings.market_data_rate_limit_per_second,
        retry_attempts=settings.market_data_retry_attempts,
        retry_min_seconds=settings.market_data_retry_min_seconds,
        retry_max_seconds=settings.market_data_retry_max_seconds,
        shock_drop_threshold=settings.market_earnings_shock_drop_threshold,
        ib_gateway_host=settings.ib_gateway_host,
        ib_gateway_port=settings.ib_gateway_port,
        ib_client_id=settings.insider_ib_client_id,
    )
    market_cache: dict[tuple[str, str], MarketSnapshot | None] = {}
    processed = 0
    enqueued = 0
    import json

    from insider_alerts.sec.models import FilingRef

    skipped_existing = 0
    http_failed = 0
    parse_failed = 0
    market_failed = 0
    terminal_rejections: list[tuple[str, str, str, str, str]] = []
    packets_to_enqueue: list[tuple[FilingRef, dict[str, object]]] = []
    for row_index, row in enumerate(rows):
        if progress_callback is not None:
            progress_callback(f"review_item_{row_index}_started")
        accession_number = str(row["accession_number"])
        form_type = str(row["form_type"])

        processed += 1
        stored_xml_url = str(row["form4_xml_url"])
        validated_stored_xml_url = validate_sec_url(stored_xml_url)
        xml_url = (
            validate_sec_url(_normalize_form4_xml_url(validated_stored_xml_url))
            if validated_stored_xml_url is not None
            else None
        )
        if xml_url is None:
            http_failed += 1
            logger.warning(
                "SEC review XML URL rejected for accession=%s url=%s",
                accession_number,
                stored_xml_url,
            )
            terminal_rejections.append(
                (
                    accession_number,
                    str(row["cik"]),
                    form_type,
                    stored_xml_url,
                    "url_rejected",
                )
            )
            continue
        try:
            xml_text = client.get_text(xml_url)
            facts = parse_form4_xml(xml_text)
        except SecHttpError:
            http_failed += 1
            continue
        except Form4ParseError:
            parse_failed += 1
            if _is_terminal_non_form4_xml(xml_text):
                terminal_rejections.append(
                    (
                        accession_number,
                        str(row["cik"]),
                        form_type,
                        stored_xml_url,
                        "parse_failed",
                    )
                )
            continue
        market_snapshot: MarketSnapshot | None = None
        if settings.market_context_enabled and facts.issuer_symbol:
            trade_dates = [
                tx.transaction_date for tx in facts.transactions if tx.transaction_date is not None
            ]
            fallback_dt = datetime.fromisoformat(str(row["filed_at"])).date()
            trade_date = max(trade_dates) if trade_dates else fallback_dt
            symbol = facts.issuer_symbol.upper()
            cache_key = (symbol, trade_date.isoformat())
            if cache_key in market_cache:
                market_snapshot = market_cache[cache_key]
            else:
                market_snapshot = get_market_snapshot(
                    settings.database_path,
                    symbol=symbol,
                    trade_date=trade_date,
                )
                if market_snapshot is None:
                    try:
                        market_snapshot = market_client.fetch_snapshot(
                            symbol,
                            trade_date=trade_date,
                        )
                    except MarketContextError as exc:
                        market_failed += 1
                        # Never swallow this silently. A dead price feed zeroes out
                        # trade_pct_daily_turnover, which silently disables every liquidity
                        # guard downstream -- exactly how the stooq outage (2026-02-12 to
                        # 2026-08-11) went unnoticed for six months with a clean error log.
                        logger.warning(
                            "market context unavailable for %s on %s: %s",
                            symbol,
                            trade_date.isoformat(),
                            exc,
                        )
                        market_snapshot = None
                    if market_snapshot is not None:
                        upsert_market_snapshot(settings.database_path, market_snapshot)
                market_cache[cache_key] = market_snapshot

        score = score_form4_signal(facts, market_snapshot=market_snapshot)
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
            "issuer_cik": facts.issuer_cik,
            "owner": facts.reporting_owner_name,
            "reporting_owner_cik": facts.reporting_owner_cik,
            "reporting_owner_ciks": list(facts.reporting_owner_ciks),
            "reporting_owner_count": facts.reporting_owner_count,
        }
        packets_to_enqueue.append((ref, packet))

    if progress_callback is not None:
        progress_callback("review_items_completed")

    record_sec_processing_rejections(
        settings.database_path,
        stage="review_xml",
        rejections=terminal_rejections,
    )
    enqueued = enqueue_review_packets_batch(settings.database_path, packets_to_enqueue)
    skipped_existing = len(packets_to_enqueue) - enqueued

    return QueueResult(
        processed=processed,
        enqueued=enqueued,
        skipped_existing=skipped_existing,
        http_failed=http_failed,
        parse_failed=parse_failed,
        market_failed=market_failed,
    )


def backfill_form4_filings(
    settings: Settings,
    *,
    start_date: date,
    end_date: date,
) -> BackfillResult:
    effective_end = min(end_date, datetime.now(UTC).date())
    if start_date > effective_end:
        return BackfillResult(
            requested_quarters=0,
            fetched_quarters=0,
            matched_filings=0,
            inserted=0,
            skipped_existing=0,
        )

    client = SecHttpClient(settings)
    quarters = iter_quarter_range(start_date, effective_end)

    fetched_quarters = 0
    matched_filings = 0
    inserted = 0
    skipped_existing = 0

    for year, quarter in quarters:
        url = master_index_url(year, quarter)
        index_text = client.get_text(url)
        fetched_quarters += 1
        refs = parse_form4_master_idx(
            index_text,
            start_date=start_date,
            end_date=effective_end,
        )
        matched_filings += len(refs)
        if not refs:
            continue
        result = upsert_filing_refs(settings.database_path, refs)
        inserted += result.inserted
        skipped_existing += result.skipped_existing

    return BackfillResult(
        requested_quarters=len(quarters),
        fetched_quarters=fetched_quarters,
        matched_filings=matched_filings,
        inserted=inserted,
        skipped_existing=skipped_existing,
    )
