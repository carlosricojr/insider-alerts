from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, date, datetime

from insider_alerts.backtest.prices import normalize_backtest_symbol
from insider_alerts.review.queue import ensure_review_tables
from insider_alerts.sec.store import init_db


@dataclass(slots=True)
class CanonicalEvent:
    packet_id: str
    accession_number: str
    cik: str
    form_type: str
    symbol: str
    filed_at: datetime
    score: float
    rationale: dict[str, object]
    cluster_packet_count: int
    cluster_max_score: float


def _parse_iso_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


@dataclass(slots=True)
class _ClusterAccumulator:
    winner: CanonicalEvent
    count: int
    max_score: float


def _build_candidate_event(row: sqlite3.Row) -> CanonicalEvent | None:
    try:
        payload_obj = json.loads(str(row["payload_json"]))
    except (TypeError, ValueError):
        return None
    if not isinstance(payload_obj, dict):
        return None
    symbol_obj = payload_obj.get("issuer_symbol")
    score_obj = payload_obj.get("score")
    if not isinstance(symbol_obj, str):
        return None
    symbol = normalize_backtest_symbol(symbol_obj)
    if symbol is None:
        return None
    if not isinstance(score_obj, (int, float, str)):
        return None
    try:
        score = float(score_obj)
    except (TypeError, ValueError):
        return None
    rationale_obj = payload_obj.get("rationale")
    rationale = rationale_obj if isinstance(rationale_obj, dict) else {}
    try:
        filed_at = _parse_iso_datetime(str(row["filed_at"]))
    except ValueError:
        return None
    return CanonicalEvent(
        packet_id=str(row["packet_id"]),
        accession_number=str(row["accession_number"]),
        cik=str(row["cik"]),
        form_type=str(row["form_type"]),
        symbol=symbol,
        filed_at=filed_at,
        score=score,
        rationale=rationale,
        cluster_packet_count=1,
        cluster_max_score=score,
    )


def load_canonical_events(
    db_path: str,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[CanonicalEvent]:
    init_db(db_path)
    ensure_review_tables(db_path)

    where_parts = ["json_extract(rp.payload_json, '$.issuer_symbol') IS NOT NULL"]
    params: list[str] = []
    if start_date is not None:
        where_parts.append("date(f.filed_at) >= ?")
        params.append(start_date.isoformat())
    if end_date is not None:
        where_parts.append("date(f.filed_at) <= ?")
        params.append(end_date.isoformat())
    where_clause = " AND ".join(where_parts)

    query = f"""
        SELECT
            rp.packet_id,
            rp.payload_json,
            rp.accession_number,
            rp.cik,
            rp.form_type,
            f.filed_at
        FROM review_packets AS rp
        INNER JOIN filings AS f
            ON f.accession_number = rp.accession_number
            AND f.cik = rp.cik
            AND f.form_type = rp.form_type
        WHERE {where_clause}
        ORDER BY f.filed_at ASC, rp.packet_id ASC
    """

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(query, params).fetchall()

    clusters: dict[tuple[str, str, date], _ClusterAccumulator] = {}
    for row in rows:
        event = _build_candidate_event(row)
        if event is None:
            continue
        key = (event.accession_number, event.symbol, event.filed_at.date())
        existing = clusters.get(key)
        if existing is None:
            clusters[key] = _ClusterAccumulator(
                winner=event,
                count=1,
                max_score=event.score,
            )
            continue

        existing.count += 1
        if event.score > existing.max_score:
            existing.max_score = event.score
        if event.score > existing.winner.score or (
            event.score == existing.winner.score and event.packet_id < existing.winner.packet_id
        ):
            existing.winner = event

    canonical_events: list[CanonicalEvent] = []
    for cluster in clusters.values():
        winner = cluster.winner
        winner.cluster_packet_count = cluster.count
        winner.cluster_max_score = cluster.max_score
        canonical_events.append(winner)

    canonical_events.sort(key=lambda event: (event.filed_at, event.packet_id))
    return canonical_events
