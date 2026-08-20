from __future__ import annotations

import json
import sqlite3
from datetime import UTC, date, datetime
from pathlib import Path
from shutil import rmtree
from uuid import uuid4

from insider_alerts.backtest.models import DailyBar
from insider_alerts.backtest.prices import ensure_price_bars_table, refresh_price_bars
from insider_alerts.backtest.readiness import (
    EventStudyReadinessConfig,
    audit_event_study_readiness,
)
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


def _event(
    *,
    packet_id: str,
    symbol: str,
    filed_at: datetime,
    score: float = 90.0,
    rationale: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "packet_id": packet_id,
        "accession_number": packet_id.split("|", 1)[0],
        "symbol": symbol,
        "filed_at": filed_at,
        "score": score,
        "rationale": rationale or {},
    }


def test_readiness_handles_empty_window() -> None:
    db_path, case_dir = _make_db_path()
    try:
        init_db(db_path)
        ensure_review_tables(db_path)
        ensure_price_bars_table(db_path)

        report = audit_event_study_readiness(
            db_path,
            start_date=date(2025, 1, 1),
            end_date=date(2025, 12, 31),
            canonical_events=[],
            config=EventStudyReadinessConfig(
                min_total_canonical_events=1,
                min_monthly_canonical_events=1,
            ),
        )

        assert report.is_ready is False
        assert "insufficient_canonical_events" in report.hard_failure_codes
    finally:
        rmtree(case_dir, ignore_errors=True)


def test_readiness_detects_missing_internal_month() -> None:
    db_path, case_dir = _make_db_path()
    try:
        init_db(db_path)
        ensure_review_tables(db_path)
        ensure_price_bars_table(db_path)

        with sqlite3.connect(db_path) as conn:
            _insert_filing(
                conn,
                accession_number="0000000001-25-000001",
                cik="0000000001",
                filed_at=datetime(2025, 1, 15, tzinfo=UTC),
            )
            _insert_filing(
                conn,
                accession_number="0000000002-25-000001",
                cik="0000000002",
                filed_at=datetime(2025, 3, 12, tzinfo=UTC),
            )
            conn.commit()

        events = [
            _event(
                packet_id="0000000001-25-000001|0000000001|4",
                symbol="AAA",
                filed_at=datetime(2025, 1, 15, tzinfo=UTC),
            ),
            _event(
                packet_id="0000000002-25-000001|0000000002|4",
                symbol="AAA",
                filed_at=datetime(2025, 3, 12, tzinfo=UTC),
            ),
        ]

        # Provide sufficient price coverage so month continuity is the failing gate.
        refresh_price_bars(
            db_path,
            symbol="AAA",
            bars=[
                DailyBar(
                    symbol="AAA",
                    trade_date=date(2025, 1, 2),
                    open=10.0,
                    high=10.5,
                    low=9.5,
                    close=10.1,
                    volume=1_000_000.0,
                ),
                DailyBar(
                    symbol="AAA",
                    trade_date=date(2025, 3, 28),
                    open=11.0,
                    high=11.2,
                    low=10.8,
                    close=11.1,
                    volume=1_100_000.0,
                ),
            ],
        )

        report = audit_event_study_readiness(
            db_path,
            start_date=date(2025, 1, 1),
            end_date=date(2025, 3, 31),
            canonical_events=events,
            config=EventStudyReadinessConfig(
                min_total_canonical_events=2,
                min_monthly_canonical_events=1,
                min_symbol_price_coverage_rate=0.0,
            ),
        )

        assert report.is_ready is False
        assert "missing_internal_months" in report.hard_failure_codes
        assert "2025-02" in report.missing_internal_months
    finally:
        rmtree(case_dir, ignore_errors=True)


def test_readiness_detects_missing_price_coverage() -> None:
    db_path, case_dir = _make_db_path()
    try:
        init_db(db_path)
        ensure_review_tables(db_path)
        ensure_price_bars_table(db_path)

        with sqlite3.connect(db_path) as conn:
            _insert_filing(
                conn,
                accession_number="0000000010-25-000001",
                cik="0000000010",
                filed_at=datetime(2025, 5, 15, tzinfo=UTC),
            )
            _insert_filing(
                conn,
                accession_number="0000000011-25-000001",
                cik="0000000011",
                filed_at=datetime(2025, 6, 20, tzinfo=UTC),
            )
            conn.commit()

        events = [
            _event(
                packet_id="0000000010-25-000001|0000000010|4",
                symbol="BBB",
                filed_at=datetime(2025, 5, 15, tzinfo=UTC),
                rationale={"open_market_gross_value": 1_000_000.0},
            ),
            _event(
                packet_id="0000000011-25-000001|0000000011|4",
                symbol="CCC",
                filed_at=datetime(2025, 6, 20, tzinfo=UTC),
                rationale={"open_market_gross_value": 2_000_000.0},
            ),
        ]

        # Only BBB has bars, CCC remains uncovered.
        refresh_price_bars(
            db_path,
            symbol="BBB",
            bars=[
                DailyBar(
                    symbol="BBB",
                    trade_date=date(2025, 5, 1),
                    open=20.0,
                    high=21.0,
                    low=19.5,
                    close=20.5,
                    volume=2_000_000.0,
                ),
                DailyBar(
                    symbol="BBB",
                    trade_date=date(2025, 6, 30),
                    open=21.0,
                    high=21.5,
                    low=20.5,
                    close=21.2,
                    volume=2_100_000.0,
                ),
            ],
        )

        report = audit_event_study_readiness(
            db_path,
            start_date=date(2025, 5, 1),
            end_date=date(2025, 6, 30),
            canonical_events=events,
            config=EventStudyReadinessConfig(
                min_total_canonical_events=2,
                min_monthly_canonical_events=1,
                min_symbol_price_coverage_rate=1.0,
            ),
        )

        assert report.is_ready is False
        assert "insufficient_price_coverage" in report.hard_failure_codes
        assert report.symbol_price_coverage_rate == 0.5
    finally:
        rmtree(case_dir, ignore_errors=True)
