from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any

from insider_alerts.backtest.prices import get_price_bar_bounds
from insider_alerts.sec.store import init_db


@dataclass(slots=True, frozen=True)
class EventStudyReadinessConfig:
    min_total_canonical_events: int = 500
    min_monthly_canonical_events: int = 20
    min_symbol_price_coverage_rate: float = 0.75
    conviction_feature_coverage_min: float = 0.80
    conviction_feature_keys: tuple[str, ...] = (
        "holding_change_ratio",
        "open_market_gross_value",
        "trade_pct_daily_turnover",
    )


@dataclass(slots=True)
class EventStudyReadinessReport:
    requested_start_date: date
    requested_end_date: date
    filing_min_date: date | None
    filing_max_date: date | None
    canonical_event_count: int
    symbol_count: int
    full_months_evaluated: list[str]
    monthly_filing_counts: dict[str, int]
    monthly_canonical_counts: dict[str, int]
    missing_internal_months: list[str]
    insufficient_monthly_event_months: list[str]
    rationale_feature_coverage: dict[str, float]
    conviction_feature_coverage_ready: bool
    symbol_price_coverage_rate: float
    covered_symbol_count: int
    uncovered_symbols: list[str]
    hard_failure_codes: list[str] = field(default_factory=list)

    @property
    def is_ready(self) -> bool:
        return len(self.hard_failure_codes) == 0


def _as_date(value: object) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text).date()
        except ValueError:
            pass
        try:
            return date.fromisoformat(text)
        except ValueError:
            return None
    return None


def _month_key(value: date) -> str:
    return value.strftime("%Y-%m")


def _next_month_start(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


def _iter_full_month_keys(*, start_date: date, end_date: date) -> list[str]:
    current = date(start_date.year, start_date.month, 1)
    if start_date.day != 1:
        current = _next_month_start(current)
    month_keys: list[str] = []
    while True:
        next_month = _next_month_start(current)
        month_end = next_month - timedelta(days=1)
        if month_end > end_date:
            break
        month_keys.append(_month_key(current))
        current = next_month
    return month_keys


def _canonical_event_month_counts(
    canonical_events: list[dict[str, Any]],
    *,
    start_date: date,
    end_date: date,
) -> tuple[dict[str, int], list[dict[str, Any]]]:
    counts: dict[str, int] = {}
    filtered_events: list[dict[str, Any]] = []
    for event in canonical_events:
        filed_at = _as_date(event.get("filed_at"))
        if filed_at is None or filed_at < start_date or filed_at > end_date:
            continue
        filtered_events.append(event)
        key = _month_key(filed_at)
        counts[key] = counts.get(key, 0) + 1
    return counts, filtered_events


def _rationale_feature_coverage(
    canonical_events: list[dict[str, Any]],
    *,
    feature_keys: tuple[str, ...],
) -> dict[str, float]:
    if not canonical_events:
        return {key: 0.0 for key in feature_keys}
    total = len(canonical_events)
    coverage: dict[str, float] = {}
    for key in feature_keys:
        present = 0
        for event in canonical_events:
            rationale = event.get("rationale")
            if not isinstance(rationale, dict):
                continue
            if key in rationale and rationale[key] is not None:
                present += 1
        coverage[key] = present / total
    return coverage


def _filing_month_counts(
    db_path: str,
    *,
    start_date: date,
    end_date: date,
) -> dict[str, int]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT substr(date(filed_at), 1, 7) AS month_key, COUNT(*) AS count
            FROM filings
            WHERE form_type = '4'
              AND date(filed_at) >= ?
              AND date(filed_at) <= ?
            GROUP BY month_key
            ORDER BY month_key
            """,
            (start_date.isoformat(), end_date.isoformat()),
        ).fetchall()
    return {str(row[0]): int(row[1]) for row in rows}


def _filing_date_bounds(db_path: str) -> tuple[date | None, date | None]:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT MIN(date(filed_at)), MAX(date(filed_at))
            FROM filings
            WHERE form_type = '4'
            """
        ).fetchone()
    if row is None:
        return None, None
    min_obj, max_obj = row
    min_date = date.fromisoformat(str(min_obj)) if min_obj is not None else None
    max_date = date.fromisoformat(str(max_obj)) if max_obj is not None else None
    return min_date, max_date


def _symbol_price_coverage(
    db_path: str,
    *,
    symbols: set[str],
    start_date: date,
    end_date: date,
) -> tuple[int, list[str]]:
    covered = 0
    uncovered: list[str] = []
    for symbol in sorted(symbols):
        min_date, max_date = get_price_bar_bounds(db_path, symbol=symbol)
        has_full_range = (
            min_date is not None
            and max_date is not None
            and min_date <= start_date
            and max_date >= end_date
        )
        if has_full_range:
            covered += 1
        else:
            uncovered.append(symbol)
    return covered, uncovered


def audit_event_study_readiness(
    db_path: str,
    *,
    start_date: date,
    end_date: date,
    canonical_events: list[dict[str, Any]],
    config: EventStudyReadinessConfig | None = None,
) -> EventStudyReadinessReport:
    if start_date > end_date:
        raise ValueError("start_date cannot be after end_date")
    cfg = config or EventStudyReadinessConfig()
    init_db(db_path)

    filing_min_date, filing_max_date = _filing_date_bounds(db_path)
    monthly_filing_counts = _filing_month_counts(
        db_path,
        start_date=start_date,
        end_date=end_date,
    )
    monthly_canonical_counts, filtered_events = _canonical_event_month_counts(
        canonical_events,
        start_date=start_date,
        end_date=end_date,
    )
    full_months = _iter_full_month_keys(start_date=start_date, end_date=end_date)

    missing_internal_months = [
        month_key
        for month_key in full_months
        if (
            monthly_filing_counts.get(month_key, 0) <= 0
            or monthly_canonical_counts.get(month_key, 0) <= 0
        )
    ]
    insufficient_monthly_event_months = [
        month_key
        for month_key in full_months
        if monthly_canonical_counts.get(month_key, 0) < cfg.min_monthly_canonical_events
    ]

    symbol_set = {
        str(event.get("symbol")).strip().upper()
        for event in filtered_events
        if isinstance(event.get("symbol"), str) and str(event.get("symbol")).strip()
    }
    covered_symbol_count, uncovered_symbols = _symbol_price_coverage(
        db_path,
        symbols=symbol_set,
        start_date=start_date,
        end_date=end_date,
    )
    symbol_count = len(symbol_set)
    symbol_price_coverage_rate = (covered_symbol_count / symbol_count) if symbol_count > 0 else 0.0

    feature_coverage = _rationale_feature_coverage(
        filtered_events,
        feature_keys=cfg.conviction_feature_keys,
    )
    conviction_ready = (
        bool(feature_coverage)
        and all(value >= cfg.conviction_feature_coverage_min for value in feature_coverage.values())
    )

    hard_failures: list[str] = []
    if len(filtered_events) < cfg.min_total_canonical_events:
        hard_failures.append("insufficient_canonical_events")
    if missing_internal_months:
        hard_failures.append("missing_internal_months")
    if insufficient_monthly_event_months:
        hard_failures.append("insufficient_monthly_events")
    if symbol_count > 0 and symbol_price_coverage_rate < cfg.min_symbol_price_coverage_rate:
        hard_failures.append("insufficient_price_coverage")

    return EventStudyReadinessReport(
        requested_start_date=start_date,
        requested_end_date=end_date,
        filing_min_date=filing_min_date,
        filing_max_date=filing_max_date,
        canonical_event_count=len(filtered_events),
        symbol_count=symbol_count,
        full_months_evaluated=full_months,
        monthly_filing_counts=monthly_filing_counts,
        monthly_canonical_counts=monthly_canonical_counts,
        missing_internal_months=missing_internal_months,
        insufficient_monthly_event_months=insufficient_monthly_event_months,
        rationale_feature_coverage=feature_coverage,
        conviction_feature_coverage_ready=conviction_ready,
        symbol_price_coverage_rate=symbol_price_coverage_rate,
        covered_symbol_count=covered_symbol_count,
        uncovered_symbols=uncovered_symbols,
        hard_failure_codes=hard_failures,
    )
