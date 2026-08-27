"""Pure E07/F00 policy shared by live-control and research shadow books."""

from __future__ import annotations

import hashlib
import math
import statistics
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Protocol
from zoneinfo import ZoneInfo

from insider_alerts.backtest.models import DailyBar

NEW_YORK = ZoneInfo("America/New_York")


class E07Config(Protocol):
    @property
    def lottery_salt(self) -> str: ...

    @property
    def entry_submission_deadline(self) -> time: ...

    @property
    def min_price(self) -> float: ...

    @property
    def max_price(self) -> float: ...

    @property
    def min_median_dollar_volume_20d(self) -> float: ...

    @property
    def slot_budget(self) -> float: ...

    @property
    def stop_loss_pct(self) -> float: ...

    @property
    def take_profit_pct(self) -> float: ...

    @property
    def max_sessions(self) -> int: ...


class E07Signal(Protocol):
    @property
    def packet_id(self) -> str: ...

    @property
    def accession_number(self) -> str: ...

    @property
    def symbol(self) -> str: ...

    @property
    def signal_at(self) -> datetime: ...


@dataclass(frozen=True, slots=True)
class ShadowEvaluation:
    entry_bar: DailyBar | None
    stop_price: float | None
    target_price: float | None
    exit_bar: DailyBar | None
    exit_price: float | None
    exit_reason: str | None


def deterministic_rank(config: E07Config, signal: E07Signal, session: date) -> str:
    material = (
        f"{config.lottery_salt}|{session.isoformat()}|{signal.packet_id}|"
        f"{signal.accession_number}|{signal.symbol}"
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def entry_session(
    config: E07Config,
    signal_at: datetime,
    sessions: Sequence[date],
    *,
    now: datetime,
) -> date | None:
    """Choose the first executable RTH open without chasing a missed auction."""

    local_signal = signal_at.astimezone(NEW_YORK)
    local_now = now.astimezone(NEW_YORK)
    candidates = [session for session in sorted(set(sessions)) if session >= local_signal.date()]
    for session in candidates:
        if (
            session == local_signal.date()
            and local_signal.time() >= config.entry_submission_deadline
        ):
            continue
        if session == local_now.date() and local_now.time() >= config.entry_submission_deadline:
            continue
        if session < local_now.date():
            continue
        return session
    return None


def completed_bars(bars: Sequence[DailyBar], signal_at: datetime) -> list[DailyBar]:
    local = signal_at.astimezone(NEW_YORK)
    same_day_complete = local.time() >= time(16, 0)
    return [
        bar
        for bar in bars
        if bar.trade_date < local.date() or (same_day_complete and bar.trade_date == local.date())
    ]


def eligibility(
    config: E07Config,
    signal: E07Signal,
    bars: Sequence[DailyBar],
) -> tuple[bool, str, float | None, float | None]:
    return eligibility_from_completed_bars(config, completed_bars(bars, signal.signal_at))


def eligibility_from_completed_bars(
    config: E07Config,
    completed: Sequence[DailyBar],
) -> tuple[bool, str, float | None, float | None]:
    """Evaluate E07 eligibility from an already point-in-time-completed history."""

    if len(completed) < 20:
        return False, "fewer_than_20_completed_daily_bars", None, None
    prior_close = completed[-1].close
    dollar_volumes = [bar.close * bar.volume for bar in completed[-20:]]
    if not math.isfinite(prior_close) or not config.min_price <= prior_close <= config.max_price:
        return False, "prior_close_outside_price_bounds", prior_close, None
    if any(not math.isfinite(value) or value <= 0 for value in dollar_volumes):
        return False, "invalid_dollar_volume_history", prior_close, None
    median_dollar_volume = statistics.median(dollar_volumes)
    if median_dollar_volume < config.min_median_dollar_volume_20d:
        return False, "median_20d_dollar_volume_below_floor", prior_close, median_dollar_volume
    return True, "eligible_E07_F00", prior_close, median_dollar_volume


def planned_quantity(config: E07Config, reference_price: float) -> int:
    if not math.isfinite(reference_price) or reference_price <= 0:
        return 0
    return max(0, math.floor(config.slot_budget / reference_price))


def evaluate_shadow(
    config: E07Config,
    bars: Sequence[DailyBar],
    entry_day: date,
) -> ShadowEvaluation:
    """Evaluate only completed daily bars under the frozen stop-first E07 policy."""

    post_entry = [bar for bar in bars if bar.trade_date >= entry_day]
    if not post_entry or post_entry[0].trade_date != entry_day:
        return ShadowEvaluation(None, None, None, None, None, None)
    entry_bar = post_entry[0]
    stop = entry_bar.open * (1.0 - config.stop_loss_pct)
    target = entry_bar.open * (1.0 + config.take_profit_pct)
    for index, bar in enumerate(post_entry[: config.max_sessions]):
        stop_hit = bar.low <= stop
        target_hit = bar.high >= target
        if stop_hit:
            return ShadowEvaluation(
                entry_bar,
                stop,
                target,
                bar,
                min(stop, bar.open),
                "stop_and_target_same_day_stop_assumed" if target_hit else "stop",
            )
        if target_hit:
            return ShadowEvaluation(entry_bar, stop, target, bar, max(target, bar.open), "target")
        if index == config.max_sessions - 1:
            return ShadowEvaluation(entry_bar, stop, target, bar, bar.close, "time")
    return ShadowEvaluation(entry_bar, stop, target, None, None, None)
