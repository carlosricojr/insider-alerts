from __future__ import annotations

import json
import sqlite3
from datetime import UTC, date, datetime

from insider_alerts.backtest.models import SignalEvent
from insider_alerts.review.queue import ensure_review_tables
from insider_alerts.sec.store import init_db


def _parse_iso_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def load_scored_signals(
    db_path: str,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[SignalEvent]:
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

    signals: list[SignalEvent] = []
    for row in rows:
        payload = json.loads(str(row["payload_json"]))
        if not isinstance(payload, dict):
            continue
        rationale = payload.get("rationale")
        if not isinstance(rationale, dict):
            rationale = {}

        symbol_obj = payload.get("issuer_symbol")
        score_obj = payload.get("score")
        if not isinstance(symbol_obj, str) or not symbol_obj.strip():
            continue
        try:
            score = float(score_obj)
        except (TypeError, ValueError):
            continue

        def _float(name: str, rationale_dict: dict[str, object] = rationale) -> float:
            value = rationale_dict.get(name)
            try:
                return float(value)
            except (TypeError, ValueError):
                return 0.0

        def _bool(name: str, rationale_dict: dict[str, object] = rationale) -> bool:
            value = rationale_dict.get(name)
            if isinstance(value, bool):
                return value
            if isinstance(value, (int, float)):
                return value != 0
            if isinstance(value, str):
                return value.strip().lower() in {"1", "true", "yes", "y"}
            return False

        role_tier_obj = rationale.get("role_tier")
        role_tier = role_tier_obj if isinstance(role_tier_obj, str) else "unknown"

        signals.append(
            SignalEvent(
                packet_id=str(row["packet_id"]),
                symbol=symbol_obj.strip().upper(),
                filed_at=_parse_iso_datetime(str(row["filed_at"])),
                score=score,
                open_market_buy_shares=_float("open_market_buy_shares"),
                open_market_net_shares=_float("open_market_net_shares"),
                has_10b5_1_plan=_bool("has_10b5_1_plan"),
                has_equity_comp_event=_bool("has_equity_comp_event"),
                has_tax_withholding_language=_bool("has_tax_withholding_language"),
                role_tier=role_tier,
            )
        )
    return signals
