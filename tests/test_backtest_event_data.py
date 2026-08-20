from __future__ import annotations

import json
import sqlite3
from datetime import UTC, date, datetime
from pathlib import Path
from shutil import rmtree
from uuid import uuid4

from insider_alerts.backtest.event_data import load_canonical_events
from insider_alerts.review.queue import ensure_review_tables
from insider_alerts.sec.store import init_db


def _make_db_path() -> tuple[str, Path]:
    root = Path("data/.tmp_pytests")
    root.mkdir(parents=True, exist_ok=True)
    case_dir = root / f"case_{uuid4().hex}"
    case_dir.mkdir(parents=True, exist_ok=True)
    return str(case_dir / "db.sqlite3"), case_dir


def _insert_filing(
    conn: sqlite3.Connection,
    *,
    accession_number: str,
    cik: str,
    filed_at: datetime,
) -> None:
    conn.execute(
        """
        INSERT INTO filings (
            source, cik, accession_number, form_type, filed_at,
            filing_detail_url, primary_doc_url, raw_rss_entry
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "sec_master_index",
            cik,
            accession_number,
            "4",
            filed_at.isoformat(),
            f"https://www.sec.gov/Archives/{accession_number}-index.html",
            None,
            json.dumps({"form_type": "4"}),
        ),
    )


def _insert_packet(
    conn: sqlite3.Connection,
    *,
    packet_id: str,
    accession_number: str,
    cik: str,
    filed_at: datetime,
    symbol: str,
    score: float,
    created_at: datetime,
) -> None:
    _insert_filing(
        conn,
        accession_number=accession_number,
        cik=cik,
        filed_at=filed_at,
    )
    conn.execute(
        """
        INSERT INTO review_packets (
            packet_id, accession_number, cik, form_type, payload_json, status,
            decision_json, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            packet_id,
            accession_number,
            cik,
            "4",
            json.dumps(
                {
                    "issuer_symbol": symbol,
                    "score": score,
                    "rationale": {
                        "open_market_buy_shares": 1000.0,
                        "open_market_net_shares": 1000.0,
                    },
                }
            ),
            "pending",
            None,
            created_at.isoformat(),
            created_at.isoformat(),
        ),
    )


def test_load_canonical_events_dedupes_by_accession_symbol_and_date() -> None:
    db_path, case_dir = _make_db_path()
    try:
        init_db(db_path)
        ensure_review_tables(db_path)

        filed_at = datetime(2025, 6, 10, 13, 45, tzinfo=UTC)
        with sqlite3.connect(db_path) as conn:
            _insert_packet(
                conn,
                packet_id="0000000001-25-000001|0000000001|4",
                accession_number="0000000001-25-000001",
                cik="0000000001",
                filed_at=filed_at,
                symbol="MAT",
                score=70.0,
                created_at=datetime(2025, 6, 10, 14, 0, tzinfo=UTC),
            )
            _insert_packet(
                conn,
                packet_id="0000000001-25-000001|0000000002|4",
                accession_number="0000000001-25-000001",
                cik="0000000002",
                filed_at=filed_at,
                symbol="MAT",
                score=90.0,
                created_at=datetime(2025, 6, 10, 14, 5, tzinfo=UTC),
            )
            conn.commit()

        events = load_canonical_events(
            db_path,
            start_date=date(2025, 6, 1),
            end_date=date(2025, 6, 30),
        )
        assert len(events) == 1
        assert events[0].packet_id == "0000000001-25-000001|0000000002|4"
        assert events[0].score == 90.0
        assert events[0].cluster_packet_count == 2
        assert events[0].cluster_max_score == 90.0
    finally:
        rmtree(case_dir, ignore_errors=True)


def test_load_canonical_events_tie_breaks_on_packet_id() -> None:
    db_path, case_dir = _make_db_path()
    try:
        init_db(db_path)
        ensure_review_tables(db_path)

        filed_at = datetime(2025, 7, 1, 12, 0, tzinfo=UTC)
        with sqlite3.connect(db_path) as conn:
            _insert_packet(
                conn,
                packet_id="0000000002-25-000001|0000000002|4",
                accession_number="0000000002-25-000001",
                cik="0000000002",
                filed_at=filed_at,
                symbol="SPGI",
                score=85.0,
                created_at=datetime(2025, 7, 1, 12, 5, tzinfo=UTC),
            )
            _insert_packet(
                conn,
                packet_id="0000000002-25-000001|0000000001|4",
                accession_number="0000000002-25-000001",
                cik="0000000001",
                filed_at=filed_at,
                symbol="SPGI",
                score=85.0,
                created_at=datetime(2025, 7, 1, 12, 6, tzinfo=UTC),
            )
            conn.commit()

        events = load_canonical_events(
            db_path,
            start_date=date(2025, 7, 1),
            end_date=date(2025, 7, 31),
        )
        assert len(events) == 1
        assert events[0].packet_id == "0000000002-25-000001|0000000001|4"
        assert events[0].cluster_packet_count == 2
    finally:
        rmtree(case_dir, ignore_errors=True)


def test_load_canonical_events_skips_unsupported_symbols() -> None:
    db_path, case_dir = _make_db_path()
    try:
        init_db(db_path)
        ensure_review_tables(db_path)

        filed_at = datetime(2025, 8, 1, 10, 0, tzinfo=UTC)
        with sqlite3.connect(db_path) as conn:
            _insert_packet(
                conn,
                packet_id="0000000003-25-000001|0000000001|4",
                accession_number="0000000003-25-000001",
                cik="0000000001",
                filed_at=filed_at,
                symbol="Z AND ZG",
                score=95.0,
                created_at=datetime(2025, 8, 1, 10, 1, tzinfo=UTC),
            )
            conn.commit()

        events = load_canonical_events(
            db_path,
            start_date=date(2025, 8, 1),
            end_date=date(2025, 8, 31),
        )
        assert events == []
    finally:
        rmtree(case_dir, ignore_errors=True)
