from __future__ import annotations

import json
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from shutil import rmtree
from uuid import uuid4

from insider_alerts.backtest.models import DailyBar
from insider_alerts.backtest.signal_study import (
    DAILY_EXECUTION_RULES,
    DeliveredSignal,
    MinuteBar,
    compute_point_in_time_features,
    holm_adjust,
    load_delivered_signals,
    moving_block_null_p_value,
    simulate_daily_rule,
    simulate_intraday_rule,
)
from insider_alerts.review.queue import ensure_review_tables
from insider_alerts.sec.store import init_db


def _db_path() -> tuple[str, Path]:
    root = Path("data/.tmp_pytests")
    root.mkdir(parents=True, exist_ok=True)
    case_dir = root / f"signal_{uuid4().hex}"
    case_dir.mkdir(parents=True, exist_ok=True)
    return str(case_dir / "db.sqlite3"), case_dir


def _insert(
    conn: sqlite3.Connection,
    *,
    packet_id: str,
    accession: str,
    symbol: str,
    source: str,
    status: str,
    filed_at: datetime,
    updated_at: datetime,
) -> None:
    cik = packet_id.split("|")[1]
    conn.execute(
        """
        INSERT INTO filings (
            source, cik, accession_number, form_type, filed_at,
            filing_detail_url, raw_rss_entry
        ) VALUES (?, ?, ?, '4', ?, ?, '{}')
        """,
        (source, cik, accession, filed_at.isoformat(), f"https://example.test/{accession}"),
    )
    conn.execute(
        """
        INSERT INTO review_packets (
            packet_id, accession_number, cik, form_type, payload_json, status,
            decision_json, created_at, updated_at
        ) VALUES (?, ?, ?, '4', ?, ?, ?, ?, ?)
        """,
        (
            packet_id,
            accession,
            cik,
            json.dumps(
                {
                    "issuer_symbol": symbol,
                    "score": 100.0,
                    "rationale": {
                        "open_market_gross_value": 250_000.0,
                        "role_tier": "chief_exec",
                    },
                }
            ),
            status,
            json.dumps({"decision": status}),
            updated_at.isoformat(),
            updated_at.isoformat(),
        ),
    )


def _signal(signal_at: datetime) -> DeliveredSignal:
    return DeliveredSignal(
        packet_id="a|1|4",
        accession_number="a",
        cik="1",
        symbol="MAT",
        filed_at=signal_at,
        signal_at=signal_at,
        score=100.0,
        rationale={"open_market_gross_value": 250_000.0, "role_tier": "chief_exec"},
    )


def _bar(symbol: str, day: date, price: float, *, volume: float = 1_000_000.0) -> DailyBar:
    return DailyBar(
        symbol=symbol,
        trade_date=day,
        open=price,
        high=price * 1.02,
        low=price * 0.98,
        close=price * 1.01,
        volume=volume,
    )


def test_load_delivered_signals_keeps_only_live_approvals_and_earliest_duplicate() -> None:
    db_path, case_dir = _db_path()
    try:
        init_db(db_path)
        ensure_review_tables(db_path)
        filed = datetime(2026, 5, 1, 14, 0, tzinfo=UTC)
        with sqlite3.connect(db_path) as conn:
            _insert(
                conn,
                packet_id="acc1|1|4",
                accession="acc1",
                symbol="MAT",
                source="sec_rss",
                status="approve",
                filed_at=filed,
                updated_at=datetime(2026, 5, 1, 14, 7, tzinfo=UTC),
            )
            _insert(
                conn,
                packet_id="acc1|2|4",
                accession="acc1",
                symbol="MAT",
                source="sec_rss",
                status="approve",
                filed_at=filed,
                updated_at=datetime(2026, 5, 1, 14, 8, tzinfo=UTC),
            )
            _insert(
                conn,
                packet_id="acc2|3|4",
                accession="acc2",
                symbol="MAT",
                source="sec_master_index",
                status="approve",
                filed_at=filed,
                updated_at=datetime(2026, 5, 1, 14, 9, tzinfo=UTC),
            )
            _insert(
                conn,
                packet_id="acc3|4|4",
                accession="acc3",
                symbol="MAT",
                source="sec_rss",
                status="reject",
                filed_at=filed,
                updated_at=datetime(2026, 5, 1, 14, 10, tzinfo=UTC),
            )
            conn.commit()

        events = load_delivered_signals(db_path)
        assert len(events) == 1
        assert events[0].packet_id == "acc1|1|4"
        assert events[0].signal_at == datetime(2026, 5, 1, 14, 7, tzinfo=UTC)
    finally:
        rmtree(case_dir, ignore_errors=True)


def test_point_in_time_features_do_not_use_unfinished_signal_day_bar() -> None:
    signal = _signal(datetime(2026, 5, 4, 15, 0, tzinfo=UTC))  # 11:00 New York
    bars = [_bar("MAT", date(2026, 3, 1) + timedelta(days=i), 10.0 + i) for i in range(31)]
    # A same-day bar with an extreme close must not enter an intraday signal's features.
    bars.append(_bar("MAT", date(2026, 5, 4), 1_000.0))
    spy = [_bar("SPY", date(2026, 3, 1) + timedelta(days=i), 100.0 + i) for i in range(31)]

    features = compute_point_in_time_features(signal, symbol_bars=bars, benchmark_bars=spy)
    assert features.prior_close < 100.0
    assert features.stock_return_20d is not None


def test_simulate_daily_rule_uses_same_day_open_for_premarket_signal() -> None:
    signal = _signal(datetime(2026, 5, 4, 12, 0, tzinfo=UTC))  # 08:00 New York
    bars = [
        _bar("MAT", date(2026, 5, 1), 10.0),
        _bar("MAT", date(2026, 5, 4), 11.0),
        _bar("MAT", date(2026, 5, 5), 12.0),
    ]
    spy = [
        _bar("SPY", date(2026, 5, 1), 100.0),
        _bar("SPY", date(2026, 5, 4), 101.0),
        _bar("SPY", date(2026, 5, 5), 102.0),
    ]
    result = simulate_daily_rule(
        signal,
        rule=DAILY_EXECUTION_RULES[0],
        symbol_bars=bars,
        benchmark_bars=spy,
        cost_fraction=0.002,
        min_price=0.0,
        min_median_dollar_volume_20d=0.0,
    )
    assert result is not None
    assert result.entry_date == date(2026, 5, 4)


def test_moving_block_null_p_value_is_deterministic_and_detects_large_positive_mean() -> None:
    dated = [(date(2026, 1, day), 0.01 + day / 10_000) for day in range(1, 21)]
    first = moving_block_null_p_value(dated, block_length=3, iterations=2_000, seed=17)
    second = moving_block_null_p_value(dated, block_length=3, iterations=2_000, seed=17)
    assert first == second
    assert first < 0.01


def test_simulate_intraday_rule_enters_first_bar_after_delay() -> None:
    signal = _signal(datetime(2026, 5, 4, 14, 0, 30, tzinfo=UTC))
    daily = [_bar("MAT", date(2026, 5, 1), 10.0), _bar("MAT", date(2026, 5, 4), 10.0)]
    spy_daily = [
        _bar("SPY", date(2026, 5, 1), 100.0),
        _bar("SPY", date(2026, 5, 4), 100.0),
    ]
    minute = [
        MinuteBar("MAT", datetime(2026, 5, 4, 14, 5, tzinfo=UTC), 10, 10, 10, 10, 100),
        MinuteBar("MAT", datetime(2026, 5, 4, 14, 6, tzinfo=UTC), 11, 11, 11, 11, 100),
        MinuteBar("MAT", datetime(2026, 5, 4, 19, 59, tzinfo=UTC), 12, 12, 12, 12, 100),
    ]
    spy_minute = [
        MinuteBar("SPY", datetime(2026, 5, 4, 14, 6, tzinfo=UTC), 100, 100, 100, 100, 100),
        MinuteBar("SPY", datetime(2026, 5, 4, 19, 59, tzinfo=UTC), 101, 101, 101, 101, 100),
    ]
    result = simulate_intraday_rule(
        signal,
        delay_minutes=5,
        symbol_daily_bars=daily,
        benchmark_daily_bars=spy_daily,
        symbol_minute_bars=minute,
        benchmark_minute_bars=spy_minute,
        cost_fraction=0.002,
        min_price=0.0,
        min_median_dollar_volume_20d=0.0,
    )
    assert result is not None
    assert result.entry_timestamp == datetime(2026, 5, 4, 14, 6, tzinfo=UTC)
    assert result.entry_price == 11
    assert result.exit_price == 12


def test_holm_adjust_counts_unavailable_hypotheses_in_family() -> None:
    adjusted = holm_adjust({"a": 0.01, "b": 0.04, "c": None}, family_size=3)
    assert adjusted["a"] == 0.03
    assert adjusted["b"] == 0.08
    assert adjusted["c"] is None
