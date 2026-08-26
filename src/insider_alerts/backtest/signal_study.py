from __future__ import annotations

import json
import math
import random
import sqlite3
import statistics
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from insider_alerts.backtest.models import DailyBar
from insider_alerts.backtest.prices import normalize_backtest_symbol
from insider_alerts.review.queue import ensure_review_tables
from insider_alerts.sec.store import init_db

NEW_YORK = ZoneInfo("America/New_York")
_SEC_MISSING_TRADING_SYMBOLS = frozenset({"NONE"})


@dataclass(slots=True, frozen=True)
class DeliveredSignal:
    packet_id: str
    accession_number: str
    cik: str
    symbol: str
    filed_at: datetime
    signal_at: datetime
    score: float
    rationale: dict[str, object]


@dataclass(slots=True, frozen=True)
class ExecutionRule:
    rule_id: str
    hold_sessions: int
    stop_loss_pct: float | None = None
    take_profit_pct: float | None = None


DAILY_EXECUTION_RULES: tuple[ExecutionRule, ...] = (
    ExecutionRule("E01", 1),
    ExecutionRule("E02", 5),
    ExecutionRule("E03", 10),
    ExecutionRule("E04", 20),
    ExecutionRule("E05", 10, 0.05, 0.05),
    ExecutionRule("E06", 10, 0.05, 0.10),
    ExecutionRule("E07", 10, 0.10, 0.10),
    ExecutionRule("E08", 10, 0.10, 0.20),
)

INTRADAY_RULE_IDS: tuple[str, ...] = ("E09", "E10", "E11", "E12")
FILTER_IDS: tuple[str, ...] = tuple(f"F{index:02d}" for index in range(14))
CONFIRMATORY_FAMILY_SIZE = 168


@dataclass(slots=True, frozen=True)
class PointInTimeFeatures:
    prior_close: float | None
    median_dollar_volume_20d: float | None
    stock_return_20d: float | None
    stock_above_sma50: bool | None
    realized_volatility_20d: float | None
    benchmark_above_sma50: bool | None
    open_market_gross_value: float | None
    trade_pct_daily_turnover: float | None
    role_tier: str | None
    market_cap: float | None = None


@dataclass(slots=True, frozen=True)
class MinuteBar:
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(slots=True, frozen=True)
class DailyStrategyObservation:
    packet_id: str
    accession_number: str
    symbol: str
    signal_at: datetime
    entry_date: date
    exit_date: date
    entry_price: float
    exit_price: float
    exit_reason: str
    net_return: float
    benchmark_return: float
    alpha_return: float
    features: PointInTimeFeatures
    entry_timestamp: datetime | None = None
    exit_timestamp: datetime | None = None


def _parse_datetime(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _finite_float(value: object) -> float | None:
    if not isinstance(value, (str, int, float)):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def load_delivered_signals(
    db_path: str,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[DeliveredSignal]:
    """Load alerts produced by the live RSS path, using the decision timestamp.

    The earliest approved packet for an accession/symbol pair is retained. Later
    packets cannot be used to enrich the first alert without introducing lookahead.
    """

    init_db(db_path)
    ensure_review_tables(db_path)
    where = [
        "rp.status = 'approve'",
        "f.source = 'sec_rss'",
        "json_extract(rp.payload_json, '$.issuer_symbol') IS NOT NULL",
    ]
    params: list[str] = []
    if start_date is not None:
        where.append("date(rp.updated_at) >= ?")
        params.append(start_date.isoformat())
    if end_date is not None:
        where.append("date(rp.updated_at) <= ?")
        params.append(end_date.isoformat())
    query = f"""
        SELECT rp.packet_id, rp.accession_number, rp.cik, rp.payload_json,
               rp.updated_at, f.filed_at
        FROM review_packets AS rp
        INNER JOIN filings AS f
          ON f.accession_number = rp.accession_number
         AND f.cik = rp.cik
         AND f.form_type = rp.form_type
        WHERE {' AND '.join(where)}
        ORDER BY rp.updated_at ASC, rp.packet_id ASC
    """
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(query, params).fetchall()

    events: list[DeliveredSignal] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        try:
            payload = json.loads(str(row["payload_json"]))
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        raw_symbol = payload.get("issuer_symbol")
        if not isinstance(raw_symbol, str):
            continue
        symbol = normalize_backtest_symbol(raw_symbol)
        if symbol is None or symbol in _SEC_MISSING_TRADING_SYMBOLS:
            continue
        key = (str(row["accession_number"]), symbol)
        if key in seen:
            continue
        score = _finite_float(payload.get("score"))
        if score is None:
            continue
        rationale_obj = payload.get("rationale")
        rationale = rationale_obj if isinstance(rationale_obj, dict) else {}
        events.append(
            DeliveredSignal(
                packet_id=str(row["packet_id"]),
                accession_number=key[0],
                cik=str(row["cik"]),
                symbol=symbol,
                filed_at=_parse_datetime(row["filed_at"]),
                signal_at=_parse_datetime(row["updated_at"]),
                score=score,
                rationale=rationale,
            )
        )
        seen.add(key)
    return events


def load_historical_approved_replay(
    db_path: str,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[DeliveredSignal]:
    """Load retrospective master-index approvals with a conservative next-open clock.

    These approvals were produced after the historical filing dates and are not a
    substitute for live evidence. Setting the synthetic signal clock to the filing
    date's regular-session close ensures daily rules cannot enter on the filing day.
    """

    init_db(db_path)
    ensure_review_tables(db_path)
    where = [
        "rp.status = 'approve'",
        "f.source = 'sec_master_index'",
        "json_extract(rp.payload_json, '$.issuer_symbol') IS NOT NULL",
    ]
    params: list[str] = []
    if start_date is not None:
        where.append("date(f.filed_at) >= ?")
        params.append(start_date.isoformat())
    if end_date is not None:
        where.append("date(f.filed_at) <= ?")
        params.append(end_date.isoformat())
    query = f"""
        SELECT rp.packet_id, rp.accession_number, rp.cik, rp.payload_json,
               rp.updated_at, f.filed_at
        FROM review_packets AS rp
        INNER JOIN filings AS f
          ON f.accession_number = rp.accession_number
         AND f.cik = rp.cik
         AND f.form_type = rp.form_type
        WHERE {' AND '.join(where)}
        ORDER BY f.filed_at ASC, rp.packet_id ASC
    """
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(query, params).fetchall()
    events: list[DeliveredSignal] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        try:
            payload = json.loads(str(row["payload_json"]))
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        raw_symbol = payload.get("issuer_symbol")
        symbol = normalize_backtest_symbol(raw_symbol) if isinstance(raw_symbol, str) else None
        score = _finite_float(payload.get("score"))
        if symbol is None or symbol in _SEC_MISSING_TRADING_SYMBOLS or score is None:
            continue
        key = (str(row["accession_number"]), symbol)
        if key in seen:
            continue
        filed_at = _parse_datetime(row["filed_at"])
        synthetic_signal_at = datetime.combine(
            filed_at.date(),
            time(16, 0),
            tzinfo=NEW_YORK,
        ).astimezone(UTC)
        rationale_obj = payload.get("rationale")
        events.append(
            DeliveredSignal(
                packet_id=str(row["packet_id"]),
                accession_number=key[0],
                cik=str(row["cik"]),
                symbol=symbol,
                filed_at=filed_at,
                signal_at=synthetic_signal_at,
                score=score,
                rationale=rationale_obj if isinstance(rationale_obj, dict) else {},
            )
        )
        seen.add(key)
    return events


def _completed_bars(bars: Sequence[DailyBar], signal_at: datetime) -> list[DailyBar]:
    local = signal_at.astimezone(NEW_YORK)
    same_day_complete = local.time() >= time(16, 0)
    return [
        bar
        for bar in bars
        if bar.trade_date < local.date()
        or (same_day_complete and bar.trade_date == local.date())
    ]


def _median_dollar_volume(bars: Sequence[DailyBar], sessions: int = 20) -> float | None:
    if len(bars) < sessions:
        return None
    values = [bar.close * bar.volume for bar in bars[-sessions:]]
    if any(not math.isfinite(value) or value <= 0 for value in values):
        return None
    return statistics.median(values)


def _return_over_sessions(bars: Sequence[DailyBar], sessions: int) -> float | None:
    if len(bars) < sessions + 1 or bars[-sessions - 1].close <= 0:
        return None
    return bars[-1].close / bars[-sessions - 1].close - 1.0


def _above_sma(bars: Sequence[DailyBar], sessions: int) -> bool | None:
    if len(bars) < sessions:
        return None
    closes = [bar.close for bar in bars[-sessions:]]
    if any(not math.isfinite(value) or value <= 0 for value in closes):
        return None
    return closes[-1] > statistics.fmean(closes)


def _realized_volatility(bars: Sequence[DailyBar], sessions: int = 20) -> float | None:
    if len(bars) < sessions + 1:
        return None
    closes = [bar.close for bar in bars[-sessions - 1 :]]
    if any(not math.isfinite(value) or value <= 0 for value in closes):
        return None
    log_returns = [
        math.log(current / previous)
        for previous, current in zip(closes, closes[1:], strict=False)
    ]
    if len(log_returns) < 2:
        return None
    return statistics.stdev(log_returns) * math.sqrt(252.0)


def compute_point_in_time_features(
    signal: DeliveredSignal,
    *,
    symbol_bars: Sequence[DailyBar],
    benchmark_bars: Sequence[DailyBar],
    market_cap: float | None = None,
) -> PointInTimeFeatures:
    completed = _completed_bars(symbol_bars, signal.signal_at)
    benchmark_completed = _completed_bars(benchmark_bars, signal.signal_at)
    prior_close = completed[-1].close if completed else None
    role_obj = signal.rationale.get("role_tier")
    return PointInTimeFeatures(
        prior_close=prior_close,
        median_dollar_volume_20d=_median_dollar_volume(completed),
        stock_return_20d=_return_over_sessions(completed, 20),
        stock_above_sma50=_above_sma(completed, 50),
        realized_volatility_20d=_realized_volatility(completed, 20),
        benchmark_above_sma50=_above_sma(benchmark_completed, 50),
        open_market_gross_value=_finite_float(
            signal.rationale.get("open_market_gross_value")
        ),
        trade_pct_daily_turnover=_finite_float(
            signal.rationale.get("trade_pct_daily_turnover")
        ),
        role_tier=role_obj if isinstance(role_obj, str) else None,
        market_cap=market_cap,
    )


def _entry_index(bars: Sequence[DailyBar], signal_at: datetime) -> int | None:
    local = signal_at.astimezone(NEW_YORK)
    allow_same_day = local.time() < time(9, 30)
    for index, bar in enumerate(bars):
        if bar.trade_date > local.date() or (allow_same_day and bar.trade_date == local.date()):
            return index
    return None


def _bar_exact(bars: Sequence[DailyBar], target: date) -> DailyBar | None:
    return next((bar for bar in bars if bar.trade_date == target), None)


def simulate_daily_rule(
    signal: DeliveredSignal,
    *,
    rule: ExecutionRule,
    symbol_bars: Sequence[DailyBar],
    benchmark_bars: Sequence[DailyBar],
    cost_fraction: float,
    min_price: float,
    min_median_dollar_volume_20d: float,
    market_cap: float | None = None,
) -> DailyStrategyObservation | None:
    features = compute_point_in_time_features(
        signal,
        symbol_bars=symbol_bars,
        benchmark_bars=benchmark_bars,
        market_cap=market_cap,
    )
    if min_median_dollar_volume_20d > 0 and (
        features.median_dollar_volume_20d is None
        or features.median_dollar_volume_20d < min_median_dollar_volume_20d
    ):
        return None
    entry_index = _entry_index(symbol_bars, signal.signal_at)
    if entry_index is None:
        return None
    entry_bar = symbol_bars[entry_index]
    if not math.isfinite(entry_bar.open) or entry_bar.open < min_price:
        return None
    final_index = entry_index + rule.hold_sessions - 1
    if final_index >= len(symbol_bars):
        return None
    exit_index = final_index
    exit_price = symbol_bars[final_index].close
    exit_reason = "time"
    stop = (
        entry_bar.open * (1.0 - rule.stop_loss_pct)
        if rule.stop_loss_pct is not None
        else None
    )
    target = (
        entry_bar.open * (1.0 + rule.take_profit_pct)
        if rule.take_profit_pct is not None
        else None
    )
    for index in range(entry_index, final_index + 1):
        bar = symbol_bars[index]
        hit_stop = stop is not None and bar.low <= stop
        hit_target = target is not None and bar.high >= target
        if hit_stop:
            assert stop is not None
            exit_index = index
            exit_price = min(stop, bar.open)
            exit_reason = "stop_and_target_same_day_stop_assumed" if hit_target else "stop"
            break
        if hit_target:
            assert target is not None
            exit_index = index
            exit_price = max(target, bar.open)
            exit_reason = "target"
            break
    exit_bar = symbol_bars[exit_index]
    benchmark_entry = _bar_exact(benchmark_bars, entry_bar.trade_date)
    benchmark_exit = _bar_exact(benchmark_bars, exit_bar.trade_date)
    if benchmark_entry is None or benchmark_exit is None or benchmark_entry.open <= 0:
        return None
    net_return = exit_price / entry_bar.open - 1.0 - cost_fraction
    benchmark_return = benchmark_exit.close / benchmark_entry.open - 1.0
    return DailyStrategyObservation(
        packet_id=signal.packet_id,
        accession_number=signal.accession_number,
        symbol=signal.symbol,
        signal_at=signal.signal_at,
        entry_date=entry_bar.trade_date,
        exit_date=exit_bar.trade_date,
        entry_price=entry_bar.open,
        exit_price=exit_price,
        exit_reason=exit_reason,
        net_return=net_return,
        benchmark_return=benchmark_return,
        alpha_return=net_return - benchmark_return,
        features=features,
    )


def _intraday_session_and_target(
    signal_at: datetime,
    *,
    benchmark_daily_bars: Sequence[DailyBar],
    delay_minutes: int,
) -> tuple[date, datetime] | None:
    local = signal_at.astimezone(NEW_YORK)
    session_dates = sorted({bar.trade_date for bar in benchmark_daily_bars})
    if not session_dates:
        return None
    if local.date() in session_dates and local.time() < time(16, 0):
        session_open = datetime.combine(local.date(), time(9, 30), tzinfo=NEW_YORK)
        base = max(local, session_open)
        target = base + timedelta(minutes=delay_minutes)
        if target.time() < time(16, 0):
            return local.date(), target.astimezone(UTC)
    next_session = next((day for day in session_dates if day > local.date()), None)
    if next_session is None:
        return None
    target = datetime.combine(next_session, time(9, 30), tzinfo=NEW_YORK) + timedelta(
        minutes=delay_minutes
    )
    return next_session, target.astimezone(UTC)


def simulate_intraday_rule(
    signal: DeliveredSignal,
    *,
    delay_minutes: int,
    symbol_daily_bars: Sequence[DailyBar],
    benchmark_daily_bars: Sequence[DailyBar],
    symbol_minute_bars: Sequence[MinuteBar],
    benchmark_minute_bars: Sequence[MinuteBar],
    cost_fraction: float,
    min_price: float,
    min_median_dollar_volume_20d: float,
    market_cap: float | None = None,
) -> DailyStrategyObservation | None:
    if delay_minutes < 0:
        raise ValueError("delay_minutes must be non-negative")
    features = compute_point_in_time_features(
        signal,
        symbol_bars=symbol_daily_bars,
        benchmark_bars=benchmark_daily_bars,
        market_cap=market_cap,
    )
    if min_median_dollar_volume_20d > 0 and (
        features.median_dollar_volume_20d is None
        or features.median_dollar_volume_20d < min_median_dollar_volume_20d
    ):
        return None
    session_target = _intraday_session_and_target(
        signal.signal_at,
        benchmark_daily_bars=benchmark_daily_bars,
        delay_minutes=delay_minutes,
    )
    if session_target is None:
        return None
    session_date, target = session_target
    symbol_session = sorted(
        (
            bar
            for bar in symbol_minute_bars
            if bar.timestamp.astimezone(NEW_YORK).date() == session_date
        ),
        key=lambda bar: bar.timestamp,
    )
    benchmark_session = sorted(
        (
            bar
            for bar in benchmark_minute_bars
            if bar.timestamp.astimezone(NEW_YORK).date() == session_date
        ),
        key=lambda bar: bar.timestamp,
    )
    entry = next((bar for bar in symbol_session if bar.timestamp >= target), None)
    if entry is None or entry.open < min_price or not math.isfinite(entry.open):
        return None
    exit_bar = symbol_session[-1]
    benchmark_entry = next(
        (bar for bar in benchmark_session if bar.timestamp >= entry.timestamp),
        None,
    )
    benchmark_exit = benchmark_session[-1] if benchmark_session else None
    if (
        benchmark_entry is None
        or benchmark_exit is None
        or benchmark_entry.open <= 0
        or exit_bar.timestamp < entry.timestamp
        or exit_bar.timestamp.astimezone(NEW_YORK).time() < time(15, 59)
        or benchmark_exit.timestamp.astimezone(NEW_YORK).time() < time(15, 59)
    ):
        return None
    net_return = exit_bar.close / entry.open - 1.0 - cost_fraction
    benchmark_return = benchmark_exit.close / benchmark_entry.open - 1.0
    return DailyStrategyObservation(
        packet_id=signal.packet_id,
        accession_number=signal.accession_number,
        symbol=signal.symbol,
        signal_at=signal.signal_at,
        entry_date=session_date,
        exit_date=session_date,
        entry_price=entry.open,
        exit_price=exit_bar.close,
        exit_reason="session_close",
        net_return=net_return,
        benchmark_return=benchmark_return,
        alpha_return=net_return - benchmark_return,
        features=features,
        entry_timestamp=entry.timestamp,
        exit_timestamp=exit_bar.timestamp,
    )


def moving_block_null_p_value(
    dated_values: Sequence[tuple[date, float]],
    *,
    block_length: int,
    iterations: int,
    seed: int,
) -> float | None:
    """One-sided moving-block bootstrap p-value for a positive clustered mean."""

    finite = [(day, value) for day, value in dated_values if math.isfinite(value)]
    if len(finite) < 2 or iterations <= 0:
        return None
    by_date: dict[date, list[float]] = defaultdict(list)
    for day, value in finite:
        by_date[day].append(value)
    dates = sorted(by_date)
    observed = statistics.fmean(value for _, value in finite)
    centered = {
        day: [value - observed for value in by_date[day]]
        for day in dates
    }
    rng = random.Random(seed)
    length = max(1, min(block_length, len(dates)))
    exceedances = 0
    for _ in range(iterations):
        sample: list[float] = []
        sampled_dates = 0
        while sampled_dates < len(dates):
            start = rng.randrange(len(dates))
            for offset in range(length):
                if sampled_dates >= len(dates):
                    break
                day = dates[(start + offset) % len(dates)]
                sample.extend(centered[day])
                sampled_dates += 1
        if sample and statistics.fmean(sample) >= observed:
            exceedances += 1
    return (exceedances + 1.0) / (iterations + 1.0)


def holm_adjust(
    p_values: Mapping[str, float | None],
    *,
    family_size: int,
) -> dict[str, float | None]:
    if family_size < len(p_values):
        raise ValueError("family_size cannot be smaller than supplied hypotheses")
    available = sorted(
        ((key, value) for key, value in p_values.items() if value is not None),
        key=lambda item: (float(item[1]), item[0]),
    )
    adjusted: dict[str, float | None] = {key: None for key in p_values}
    running = 0.0
    for rank, (key, value) in enumerate(available):
        candidate = min(1.0, float(value) * (family_size - rank))
        running = max(running, candidate)
        adjusted[key] = running
    return adjusted


def passes_filter(filter_id: str, features: PointInTimeFeatures) -> bool:
    if filter_id == "F00":
        return True
    if filter_id == "F01":
        return bool(
            features.open_market_gross_value is not None
            and features.open_market_gross_value >= 100_000.0
        )
    if filter_id == "F02":
        return bool(
            features.open_market_gross_value is not None
            and features.open_market_gross_value >= 500_000.0
        )
    if filter_id == "F03":
        return features.role_tier == "chief_exec"
    if filter_id == "F04":
        return bool(
            features.trade_pct_daily_turnover is not None
            and features.trade_pct_daily_turnover >= 1.0
        )
    if filter_id == "F05":
        return bool(features.stock_return_20d is not None and features.stock_return_20d > 0)
    if filter_id == "F06":
        return bool(features.stock_return_20d is not None and features.stock_return_20d <= 0)
    if filter_id == "F07":
        return features.stock_above_sma50 is True
    if filter_id == "F08":
        return features.stock_above_sma50 is False
    if filter_id == "F09":
        return bool(
            features.realized_volatility_20d is not None
            and features.realized_volatility_20d <= 0.40
        )
    if filter_id == "F10":
        return bool(
            features.realized_volatility_20d is not None
            and features.realized_volatility_20d > 0.40
        )
    if filter_id == "F11":
        return features.benchmark_above_sma50 is True
    if filter_id == "F12":
        return features.benchmark_above_sma50 is False
    if filter_id == "F13":
        return bool(features.market_cap is not None and features.market_cap >= 2_000_000_000.0)
    raise ValueError(f"unknown filter: {filter_id}")


def collect_daily_strategy_observations(
    signals: Sequence[DeliveredSignal],
    *,
    rule: ExecutionRule,
    filter_id: str,
    bars_by_symbol: Mapping[str, Sequence[DailyBar]],
    benchmark_symbol: str = "SPY",
    cost_fraction: float = 0.002,
    min_price: float = 2.0,
    min_median_dollar_volume_20d: float = 500_000.0,
    market_caps: Mapping[str, float] | None = None,
) -> list[DailyStrategyObservation]:
    caps = market_caps or {}
    benchmark_bars = bars_by_symbol.get(benchmark_symbol, ())
    observations: list[DailyStrategyObservation] = []
    for signal in signals:
        observation = simulate_daily_rule(
            signal,
            rule=rule,
            symbol_bars=bars_by_symbol.get(signal.symbol, ()),
            benchmark_bars=benchmark_bars,
            cost_fraction=cost_fraction,
            min_price=min_price,
            min_median_dollar_volume_20d=min_median_dollar_volume_20d,
            market_cap=caps.get(signal.packet_id),
        )
        if observation is not None and passes_filter(filter_id, observation.features):
            observations.append(observation)
    return _without_overlapping_symbol_positions(observations)


def matched_random_date_control(
    signals: Sequence[DeliveredSignal],
    *,
    rule: ExecutionRule,
    filter_id: str,
    bars_by_symbol: Mapping[str, Sequence[DailyBar]],
    study_start: date,
    study_end: date,
    iterations: int = 5_000,
    seed: int = 20260817,
    benchmark_symbol: str = "SPY",
    cost_fraction: float = 0.002,
    min_price: float = 2.0,
    min_median_dollar_volume_20d: float = 500_000.0,
) -> dict[str, object]:
    """Compare signal timing with random eligible dates in the same symbols."""

    actual = collect_daily_strategy_observations(
        signals,
        rule=rule,
        filter_id=filter_id,
        bars_by_symbol=bars_by_symbol,
        benchmark_symbol=benchmark_symbol,
        cost_fraction=cost_fraction,
        min_price=min_price,
        min_median_dollar_volume_20d=min_median_dollar_volume_20d,
    )
    if not actual or iterations <= 0:
        return {"status": "insufficient_sample", "trade_count": len(actual)}
    by_packet = {signal.packet_id: signal for signal in signals}
    symbol_counts: dict[str, int] = defaultdict(int)
    template_by_symbol: dict[str, DeliveredSignal] = {}
    for observation in actual:
        symbol_counts[observation.symbol] += 1
        template_by_symbol.setdefault(observation.symbol, by_packet[observation.packet_id])
    benchmark_bars = bars_by_symbol.get(benchmark_symbol, ())
    pools: dict[str, list[float]] = {}
    for symbol in symbol_counts:
        template = template_by_symbol[symbol]
        pool: list[float] = []
        for bar in bars_by_symbol.get(symbol, ()):
            if not study_start <= bar.trade_date <= study_end:
                continue
            synthetic_at = datetime.combine(
                bar.trade_date,
                time(16, 0),
                tzinfo=NEW_YORK,
            ).astimezone(UTC)
            synthetic = DeliveredSignal(
                packet_id=template.packet_id,
                accession_number=template.accession_number,
                cik=template.cik,
                symbol=symbol,
                filed_at=synthetic_at,
                signal_at=synthetic_at,
                score=template.score,
                rationale=template.rationale,
            )
            candidate_observation = simulate_daily_rule(
                synthetic,
                rule=rule,
                symbol_bars=bars_by_symbol.get(symbol, ()),
                benchmark_bars=benchmark_bars,
                cost_fraction=cost_fraction,
                min_price=min_price,
                min_median_dollar_volume_20d=min_median_dollar_volume_20d,
            )
            if candidate_observation is not None and passes_filter(
                filter_id, candidate_observation.features
            ):
                pool.append(candidate_observation.alpha_return)
        pools[symbol] = pool
    covered_trade_count = sum(
        count for symbol, count in symbol_counts.items() if pools.get(symbol)
    )
    if covered_trade_count == 0:
        return {"status": "no_matched_dates", "trade_count": len(actual)}
    actual_mean = statistics.fmean(item.alpha_return for item in actual)
    rng = random.Random(seed)
    null_means: list[float] = []
    for _ in range(iterations):
        sampled = [
            rng.choice(pools[symbol])
            for symbol, count in symbol_counts.items()
            if pools.get(symbol)
            for _index in range(count)
        ]
        null_means.append(statistics.fmean(sampled))
    null_mean = statistics.fmean(null_means)
    p_value = (1.0 + sum(value >= actual_mean for value in null_means)) / (
        iterations + 1.0
    )
    ordered = sorted(null_means)
    return {
        "status": "tested",
        "trade_count": len(actual),
        "covered_trade_count": covered_trade_count,
        "coverage_rate": covered_trade_count / len(actual),
        "actual_mean_alpha": actual_mean,
        "null_mean_alpha": null_mean,
        "signal_timing_uplift": actual_mean - null_mean,
        "one_sided_p_value": p_value,
        "null_95th_percentile": ordered[min(len(ordered) - 1, int(0.95 * len(ordered)))],
        "iterations": iterations,
        "seed": seed,
    }


def fixed_slot_portfolio_summary(
    observations: Sequence[DailyStrategyObservation],
    *,
    slots: int = 20,
) -> dict[str, object]:
    if slots <= 0:
        raise ValueError("slots must be positive")
    active: list[DailyStrategyObservation] = []
    retained: list[DailyStrategyObservation] = []
    skipped = 0
    for observation in sorted(
        observations,
        key=lambda item: (item.entry_date, item.signal_at, item.packet_id),
    ):
        active = [item for item in active if item.exit_date >= observation.entry_date]
        if len(active) >= slots:
            skipped += 1
            continue
        active.append(observation)
        retained.append(observation)
    equity = 1.0
    peak = 1.0
    max_realized_drawdown = 0.0
    for observation in sorted(retained, key=lambda item: (item.exit_date, item.packet_id)):
        equity += observation.net_return / slots
        peak = max(peak, equity)
        max_realized_drawdown = max(max_realized_drawdown, (peak - equity) / peak)
    return {
        "slot_count": slots,
        "candidate_trade_count": len(observations),
        "retained_trade_count": len(retained),
        "capacity_skipped_trade_count": skipped,
        "realized_return": equity - 1.0,
        "max_realized_only_drawdown": max_realized_drawdown,
        "mean_retained_trade_return": (
            statistics.fmean(item.net_return for item in retained) if retained else None
        ),
        "mean_retained_trade_alpha": (
            statistics.fmean(item.alpha_return for item in retained) if retained else None
        ),
    }


def _without_overlapping_symbol_positions(
    observations: Sequence[DailyStrategyObservation],
) -> list[DailyStrategyObservation]:
    retained: list[DailyStrategyObservation] = []
    last_exit: dict[str, date] = {}
    for observation in sorted(
        observations,
        key=lambda item: (item.entry_date, item.signal_at, item.packet_id),
    ):
        prior_exit = last_exit.get(observation.symbol)
        if prior_exit is not None and observation.entry_date <= prior_exit:
            continue
        retained.append(observation)
        last_exit[observation.symbol] = observation.exit_date
    return retained


def _profit_factor(values: Sequence[float]) -> float | None:
    wins = sum(value for value in values if value > 0)
    losses = abs(sum(value for value in values if value < 0))
    if losses > 0:
        return wins / losses
    return None


def _mean_or_none(values: Sequence[float]) -> float | None:
    return statistics.fmean(values) if values else None


def _robustness_metrics(
    observations: Sequence[DailyStrategyObservation],
    *,
    primary_cost_fraction: float,
    stress_cost_fraction: float,
    split_date: date,
) -> dict[str, object]:
    if not observations:
        return {
            "stress_mean_return": None,
            "stress_mean_alpha": None,
            "pre_split_mean_alpha": None,
            "post_split_mean_alpha": None,
            "mean_alpha_without_best_trade": None,
            "mean_alpha_without_best_month": None,
            "top_symbol_pnl_share": None,
        }
    extra_cost = stress_cost_fraction - primary_cost_fraction
    stress_returns = [item.net_return - extra_cost for item in observations]
    stress_alphas = [item.alpha_return - extra_cost for item in observations]
    pre = [item.alpha_return for item in observations if item.entry_date < split_date]
    post = [item.alpha_return for item in observations if item.entry_date >= split_date]
    sorted_alpha = sorted(item.alpha_return for item in observations)
    without_best_trade = sorted_alpha[:-1]
    by_month: dict[str, list[float]] = defaultdict(list)
    by_symbol: dict[str, float] = defaultdict(float)
    for item in observations:
        by_month[item.entry_date.strftime("%Y-%m")].append(item.alpha_return)
        by_symbol[item.symbol] += item.alpha_return
    best_month = max(by_month, key=lambda month: sum(by_month[month]))
    without_best_month = [
        item.alpha_return
        for item in observations
        if item.entry_date.strftime("%Y-%m") != best_month
    ]
    total_positive_pnl = sum(max(value, 0.0) for value in by_symbol.values())
    top_symbol_share = (
        max((max(value, 0.0) for value in by_symbol.values()), default=0.0)
        / total_positive_pnl
        if total_positive_pnl > 0
        else None
    )
    return {
        "stress_mean_return": _mean_or_none(stress_returns),
        "stress_mean_alpha": _mean_or_none(stress_alphas),
        "split_date": split_date.isoformat(),
        "pre_split_mean_alpha": _mean_or_none(pre),
        "post_split_mean_alpha": _mean_or_none(post),
        "mean_alpha_without_best_trade": _mean_or_none(without_best_trade),
        "mean_alpha_without_best_month": _mean_or_none(without_best_month),
        "best_month_removed": best_month,
        "top_symbol_pnl_share": top_symbol_share,
    }


def evaluate_daily_hypothesis_family(
    signals: Sequence[DeliveredSignal],
    *,
    bars_by_symbol: Mapping[str, Sequence[DailyBar]],
    benchmark_symbol: str = "SPY",
    primary_cost_fraction: float = 0.002,
    stress_cost_fraction: float = 0.005,
    min_price: float = 2.0,
    min_median_dollar_volume_20d: float = 500_000.0,
    bootstrap_iterations: int = 10_000,
    random_seed: int = 20260817,
    market_caps: Mapping[str, float] | None = None,
    minute_bars_by_symbol: Mapping[str, Sequence[MinuteBar]] | None = None,
    robustness_split_date: date = date(2026, 7, 1),
) -> dict[str, object]:
    """Evaluate the locked 168-test family, marking unavailable tiers explicitly."""

    benchmark_bars = bars_by_symbol.get(benchmark_symbol, ())
    caps = market_caps or {}
    observations_by_rule: dict[str, list[DailyStrategyObservation]] = {}
    for rule in DAILY_EXECUTION_RULES:
        rule_observations: list[DailyStrategyObservation] = []
        for signal in signals:
            observation = simulate_daily_rule(
                signal,
                rule=rule,
                symbol_bars=bars_by_symbol.get(signal.symbol, ()),
                benchmark_bars=benchmark_bars,
                cost_fraction=primary_cost_fraction,
                min_price=min_price,
                min_median_dollar_volume_20d=min_median_dollar_volume_20d,
                market_cap=caps.get(signal.packet_id),
            )
            if observation is not None:
                rule_observations.append(observation)
        observations_by_rule[rule.rule_id] = rule_observations

    results: list[dict[str, object]] = []
    p_values: dict[str, float | None] = {}
    for rule in DAILY_EXECUTION_RULES:
        for filter_id in FILTER_IDS:
            key = f"{rule.rule_id}|{filter_id}"
            eligible = [
                item
                for item in observations_by_rule[rule.rule_id]
                if passes_filter(filter_id, item.features)
            ]
            selected = _without_overlapping_symbol_positions(eligible)
            returns = [item.net_return for item in selected]
            alphas = [item.alpha_return for item in selected]
            cluster_count = len({item.entry_date for item in selected})
            evidence_floor_pass = len(selected) >= 40 and cluster_count >= 30
            raw_p = (
                moving_block_null_p_value(
                    [(item.entry_date, item.alpha_return) for item in selected],
                    block_length=rule.hold_sessions,
                    iterations=bootstrap_iterations,
                    seed=random_seed
                    + sum((index + 1) * ord(char) for index, char in enumerate(key)),
                )
                if evidence_floor_pass
                else None
            )
            p_values[key] = raw_p
            result: dict[str, object] = {
                "hypothesis_id": key,
                "execution_rule": asdict(rule),
                "filter_id": filter_id,
                "status": "tested" if evidence_floor_pass else "insufficient_sample",
                "trade_count": len(selected),
                "entry_date_cluster_count": cluster_count,
                "mean_return": _mean_or_none(returns),
                "median_return": statistics.median(returns) if returns else None,
                "mean_alpha": _mean_or_none(alphas),
                "median_alpha": statistics.median(alphas) if alphas else None,
                "win_rate": (
                    sum(value > 0 for value in returns) / len(returns) if returns else None
                ),
                "profit_factor": _profit_factor(returns),
                "raw_p_value": raw_p,
                "bonferroni_p_value": (
                    min(1.0, raw_p * CONFIRMATORY_FAMILY_SIZE)
                    if raw_p is not None
                    else None
                ),
                "robustness": _robustness_metrics(
                    selected,
                    primary_cost_fraction=primary_cost_fraction,
                    stress_cost_fraction=stress_cost_fraction,
                    split_date=robustness_split_date,
                ),
            }
            results.append(result)

    intraday_observations: dict[str, list[DailyStrategyObservation]] = {}
    intraday_delays = dict(zip(INTRADAY_RULE_IDS, (0, 5, 15, 30), strict=True))
    if minute_bars_by_symbol is not None:
        for rule_id, delay in intraday_delays.items():
            items: list[DailyStrategyObservation] = []
            for signal in signals:
                observation = simulate_intraday_rule(
                    signal,
                    delay_minutes=delay,
                    symbol_daily_bars=bars_by_symbol.get(signal.symbol, ()),
                    benchmark_daily_bars=benchmark_bars,
                    symbol_minute_bars=minute_bars_by_symbol.get(signal.symbol, ()),
                    benchmark_minute_bars=minute_bars_by_symbol.get(benchmark_symbol, ()),
                    cost_fraction=primary_cost_fraction,
                    min_price=min_price,
                    min_median_dollar_volume_20d=min_median_dollar_volume_20d,
                    market_cap=caps.get(signal.packet_id),
                )
                if observation is not None:
                    items.append(observation)
            intraday_observations[rule_id] = items

    coverage_denominator = max(len(observations_by_rule.get("E01", [])), 1)
    intraday_coverage = {
        rule_id: len(intraday_observations.get(rule_id, []))
        for rule_id in INTRADAY_RULE_IDS
    }
    intraday_coverage_pass = bool(
        minute_bars_by_symbol is not None
        and all(
            intraday_coverage[rule_id] / coverage_denominator >= 0.80
            for rule_id in INTRADAY_RULE_IDS
        )
    )
    for rule_id in INTRADAY_RULE_IDS:
        for filter_id in FILTER_IDS:
            key = f"{rule_id}|{filter_id}"
            if minute_bars_by_symbol is None:
                intraday_selected: list[DailyStrategyObservation] = []
                status = "unavailable_intraday_data"
            else:
                intraday_selected = _without_overlapping_symbol_positions(
                    [
                        item
                        for item in intraday_observations.get(rule_id, [])
                        if passes_filter(filter_id, item.features)
                    ]
                )
                status = (
                    "pending_sample_check"
                    if intraday_coverage_pass
                    else "non_decision_grade_intraday_coverage"
                )
            returns = [item.net_return for item in intraday_selected]
            alphas = [item.alpha_return for item in intraday_selected]
            cluster_count = len({item.entry_date for item in intraday_selected})
            evidence_floor_pass = (
                intraday_coverage_pass
                and len(intraday_selected) >= 40
                and cluster_count >= 30
            )
            if evidence_floor_pass:
                status = "tested"
            elif status == "pending_sample_check":
                status = "insufficient_sample"
            raw_p = (
                moving_block_null_p_value(
                    [(item.entry_date, item.alpha_return) for item in intraday_selected],
                    block_length=1,
                    iterations=bootstrap_iterations,
                    seed=random_seed
                    + sum((index + 1) * ord(char) for index, char in enumerate(key)),
                )
                if evidence_floor_pass
                else None
            )
            p_values[key] = raw_p
            results.append(
                {
                    "hypothesis_id": key,
                    "execution_rule": {
                        "rule_id": rule_id,
                        "delay_minutes": intraday_delays[rule_id],
                        "exit": "session_close",
                    },
                    "filter_id": filter_id,
                    "status": status,
                    "trade_count": len(intraday_selected),
                    "entry_date_cluster_count": cluster_count,
                    "mean_return": _mean_or_none(returns),
                    "median_return": statistics.median(returns) if returns else None,
                    "mean_alpha": _mean_or_none(alphas),
                    "median_alpha": statistics.median(alphas) if alphas else None,
                    "win_rate": (
                        sum(value > 0 for value in returns) / len(returns) if returns else None
                    ),
                    "profit_factor": _profit_factor(returns),
                    "raw_p_value": raw_p,
                    "bonferroni_p_value": (
                        min(1.0, raw_p * CONFIRMATORY_FAMILY_SIZE)
                        if raw_p is not None
                        else None
                    ),
                    "robustness": _robustness_metrics(
                        intraday_selected,
                        primary_cost_fraction=primary_cost_fraction,
                        stress_cost_fraction=stress_cost_fraction,
                        split_date=robustness_split_date,
                    ),
                }
            )

    adjusted = holm_adjust(p_values, family_size=CONFIRMATORY_FAMILY_SIZE)
    survivors: list[str] = []
    for result in results:
        key = str(result["hypothesis_id"])
        result["holm_adjusted_p_value"] = adjusted[key]
        robustness = result.get("robustness")
        robust = robustness if isinstance(robustness, dict) else {}
        raw_p_value = _finite_float(result.get("raw_p_value"))
        mean_return = _finite_float(result.get("mean_return"))
        profit_factor = _finite_float(result.get("profit_factor"))
        stress_mean_alpha = _finite_float(robust.get("stress_mean_alpha"))
        pre_split_mean_alpha = _finite_float(robust.get("pre_split_mean_alpha"))
        post_split_mean_alpha = _finite_float(robust.get("post_split_mean_alpha"))
        alpha_without_best_trade = _finite_float(
            robust.get("mean_alpha_without_best_trade")
        )
        alpha_without_best_month = _finite_float(
            robust.get("mean_alpha_without_best_month")
        )
        top_symbol_pnl_share = _finite_float(robust.get("top_symbol_pnl_share"))
        statistical_pass = bool(
            raw_p_value is not None
            and raw_p_value <= 0.05 / CONFIRMATORY_FAMILY_SIZE
        )
        economic_pass = bool(
            mean_return is not None
            and mean_return > 0
            and profit_factor is not None
            and profit_factor > 1
        )
        robustness_pass = bool(
            stress_mean_alpha is not None
            and stress_mean_alpha > 0
            and pre_split_mean_alpha is not None
            and pre_split_mean_alpha > 0
            and post_split_mean_alpha is not None
            and post_split_mean_alpha > 0
            and alpha_without_best_trade is not None
            and alpha_without_best_trade > 0
            and alpha_without_best_month is not None
            and alpha_without_best_month > 0
            and top_symbol_pnl_share is not None
            and top_symbol_pnl_share <= 0.25
        )
        result["statistical_pass"] = statistical_pass
        result["economic_pass"] = economic_pass
        result["robustness_pass"] = robustness_pass
        if statistical_pass and economic_pass and robustness_pass:
            survivors.append(key)

    results.sort(
        key=lambda item: (
            item["raw_p_value"] is None,
            _finite_float(item["raw_p_value"])
            if item["raw_p_value"] is not None
            else math.inf,
            str(item["hypothesis_id"]),
        )
    )
    return {
        "schema_version": "signal-study-v1",
        "family_size": CONFIRMATORY_FAMILY_SIZE,
        "bonferroni_raw_p_threshold": 0.05 / CONFIRMATORY_FAMILY_SIZE,
        "signal_count": len(signals),
        "benchmark_symbol": benchmark_symbol,
        "primary_cost_fraction": primary_cost_fraction,
        "stress_cost_fraction": stress_cost_fraction,
        "bootstrap_iterations": bootstrap_iterations,
        "random_seed": random_seed,
        "daily_execution_coverage": {
            rule_id: len(items) for rule_id, items in observations_by_rule.items()
        },
        "intraday_status": (
            "decision_grade"
            if intraday_coverage_pass
            else (
                "non_decision_grade_coverage"
                if minute_bars_by_symbol is not None
                else "unavailable_intraday_data"
            )
        ),
        "intraday_execution_coverage": intraday_coverage,
        "intraday_coverage_denominator": coverage_denominator,
        "surviving_hypotheses": survivors,
        "conclusion": (
            "candidate_edge_requires_further_falsification"
            if survivors
            else "no_demonstrated_tradable_edge"
        ),
        "hypotheses": results,
    }
