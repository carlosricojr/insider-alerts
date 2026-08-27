"""Shared point-in-time proof for frozen E07 research outcomes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal, cast

from insider_alerts.research.bar_feed import BarFeedStore, BarObservationRecord, BarPollReceipt
from insider_alerts.research.session_feed import SessionFeedStore, SessionObservationRecord
from insider_alerts.research.trial_runtime import MAX_SESSIONS, TrialRuntimeInvalid
from insider_alerts.strategy.e07 import E07Config, evaluate_shadow


@dataclass(frozen=True, slots=True)
class FrozenScheduleBinding:
    """Immutable session-feed coordinates for one ten-session outcome horizon."""

    entry_date: date
    final_session_date: date
    as_of_utc: datetime
    observation_watermark: int
    record_sha256s: tuple[str, ...]
    expected_entry_opens_at_utc: datetime | None = None


@dataclass(frozen=True, slots=True)
class ResearchOutcomeProof:
    """Economic result and every research-feed coordinate needed to reproduce it."""

    symbol: str
    entry_date: date
    entry_at_utc: datetime
    exit_date: date
    exit_at_utc: datetime
    entry_price: float
    stop_price: float
    target_price: float
    exit_price: float
    exit_reason: Literal["stop", "target", "time", "stop_and_target_same_day_stop_assumed"]
    gross_return: float
    spy_entry_price: float
    spy_exit_price: float
    spy_return: float
    schedule_observation_watermark: int
    schedule_record_sha256s: tuple[str, ...]
    bar_observation_watermark: int
    bar_poll_receipt_watermark: int
    bar_record_sha256s: tuple[str, ...]
    bar_poll_receipt_sha256s: tuple[str, ...]


def bound_horizon(
    session_store: SessionFeedStore,
    binding: FrozenScheduleBinding,
) -> tuple[SessionObservationRecord, ...]:
    """Reconstruct exactly the frozen schedule horizon or fail closed."""

    bound_digests = set(binding.record_sha256s)
    records = session_store.schedule_records_as_known_at(
        binding.as_of_utc,
        max_sequence=binding.observation_watermark,
    )
    horizon = tuple(
        record
        for record in records
        if binding.entry_date <= record.session.session_date <= binding.final_session_date
    )
    if (
        len(horizon) != MAX_SESSIONS
        or horizon[0].session.session_date != binding.entry_date
        or horizon[-1].session.session_date != binding.final_session_date
        or (
            binding.expected_entry_opens_at_utc is not None
            and horizon[0].session.opens_at_utc != binding.expected_entry_opens_at_utc
        )
        or len(bound_digests) != len(binding.record_sha256s)
        or any(record.record_sha256 not in bound_digests for record in horizon)
    ):
        raise TrialRuntimeInvalid("outcome_frozen_schedule_binding_invalid")
    return horizon


def _terminal_receipt(
    receipts: tuple[BarPollReceipt, ...] | list[BarPollReceipt],
    *,
    entry_date: date,
    final_session: SessionObservationRecord,
    now: datetime,
    bar_watermark: int,
) -> BarPollReceipt | None:
    qualified = [
        receipt
        for receipt in receipts
        if receipt.observation_watermark is not None
        and receipt.observation_watermark <= bar_watermark
        and receipt.requested_start_date <= entry_date
        and receipt.requested_through_date >= final_session.session.session_date
        and receipt.completed_through_date is not None
        and receipt.completed_through_date >= final_session.session.session_date
        and receipt.polled_at_utc >= final_session.session.closes_at_utc
        and receipt.polled_at_utc <= now
        and receipt.source_rejection_count == 0
        and receipt.validation_rejection_count == 0
    ]
    return max(qualified, key=lambda item: item.sequence, default=None)


def _horizon_record_maps(
    bar_store: BarFeedStore,
    *,
    symbol: str,
    horizon_dates: tuple[date, ...],
    receipt: BarPollReceipt,
    current_bar_watermark: int,
) -> tuple[dict[date, BarObservationRecord], dict[date, BarObservationRecord]]:
    if receipt.observation_watermark is None:
        raise TrialRuntimeInvalid("outcome_poll_receipt_observation_watermark_missing")
    current = bar_store.first_observed_bar_records(
        symbol,
        start_date=horizon_dates[0],
        through_date=horizon_dates[-1],
        max_sequence=current_bar_watermark,
    )
    proven = bar_store.first_observed_bar_records(
        symbol,
        start_date=horizon_dates[0],
        through_date=horizon_dates[-1],
        max_sequence=receipt.observation_watermark,
    )
    return (
        {record.bar.trade_date: record for record in current},
        {record.bar.trade_date: record for record in proven},
    )


def materialize_research_outcome(
    *,
    symbol: str,
    schedule_binding: FrozenScheduleBinding,
    session_store: SessionFeedStore,
    bar_store: BarFeedStore,
    policy: E07Config,
    now: datetime,
) -> ResearchOutcomeProof | None:
    """Return one terminal research-feed outcome, or ``None`` while proof is pending."""

    horizon = bound_horizon(session_store, schedule_binding)
    final_session = horizon[-1]
    if now < final_session.session.closes_at_utc:
        return None
    bar_watermark = bar_store.observation_watermark()
    receipt_watermark = bar_store.poll_receipt_watermark()
    stock_receipt = _terminal_receipt(
        bar_store.poll_receipts(symbol, max_sequence=receipt_watermark),
        entry_date=schedule_binding.entry_date,
        final_session=final_session,
        now=now,
        bar_watermark=bar_watermark,
    )
    spy_receipt = _terminal_receipt(
        bar_store.poll_receipts("SPY", max_sequence=receipt_watermark),
        entry_date=schedule_binding.entry_date,
        final_session=final_session,
        now=now,
        bar_watermark=bar_watermark,
    )
    if stock_receipt is None or spy_receipt is None:
        return None
    horizon_dates = tuple(record.session.session_date for record in horizon)
    stock_current, stock_proven = _horizon_record_maps(
        bar_store,
        symbol=symbol,
        horizon_dates=horizon_dates,
        receipt=stock_receipt,
        current_bar_watermark=bar_watermark,
    )
    spy_current, spy_proven = _horizon_record_maps(
        bar_store,
        symbol="SPY",
        horizon_dates=horizon_dates,
        receipt=spy_receipt,
        current_bar_watermark=bar_watermark,
    )
    stock_prefix: list[BarObservationRecord] = []
    for session_date in horizon_dates:
        record = stock_current.get(session_date)
        if record is None:
            break
        stock_prefix.append(record)
    shadow = evaluate_shadow(
        policy,
        [record.bar for record in stock_prefix],
        schedule_binding.entry_date,
    )
    if (
        shadow.entry_bar is None
        or shadow.stop_price is None
        or shadow.target_price is None
        or shadow.exit_bar is None
        or shadow.exit_price is None
        or shadow.exit_reason is None
    ):
        raise TrialRuntimeInvalid("outcome_terminal_stock_path_incomplete")
    exit_session = next(
        (record for record in horizon if record.session.session_date == shadow.exit_bar.trade_date),
        None,
    )
    if exit_session is None:
        raise TrialRuntimeInvalid("outcome_exit_session_missing")
    exit_index = horizon_dates.index(shadow.exit_bar.trade_date)
    required_stock_dates = horizon_dates[: exit_index + 1]
    if any(session_date not in stock_proven for session_date in required_stock_dates):
        return None
    required_spy_dates = {schedule_binding.entry_date, shadow.exit_bar.trade_date}
    if any(session_date not in spy_current for session_date in required_spy_dates):
        raise TrialRuntimeInvalid("outcome_terminal_spy_benchmark_incomplete")
    if any(session_date not in spy_proven for session_date in required_spy_dates):
        return None
    stock_records = tuple(stock_proven[session_date] for session_date in required_stock_dates)
    spy_records = tuple(spy_proven[session_date] for session_date in sorted(required_spy_dates))
    spy_entry = spy_proven[schedule_binding.entry_date].bar
    spy_exit = spy_proven[shadow.exit_bar.trade_date].bar
    entry_price = shadow.entry_bar.open
    gross_return = shadow.exit_price / entry_price - 1.0
    spy_return = spy_exit.close / spy_entry.open - 1.0
    return ResearchOutcomeProof(
        symbol=symbol,
        entry_date=schedule_binding.entry_date,
        entry_at_utc=horizon[0].session.opens_at_utc,
        exit_date=shadow.exit_bar.trade_date,
        exit_at_utc=exit_session.session.closes_at_utc,
        entry_price=entry_price,
        stop_price=shadow.stop_price,
        target_price=shadow.target_price,
        exit_price=shadow.exit_price,
        exit_reason=cast(
            Literal["stop", "target", "time", "stop_and_target_same_day_stop_assumed"],
            shadow.exit_reason,
        ),
        gross_return=gross_return,
        spy_entry_price=spy_entry.open,
        spy_exit_price=spy_exit.close,
        spy_return=spy_return,
        schedule_observation_watermark=schedule_binding.observation_watermark,
        schedule_record_sha256s=tuple(sorted(record.record_sha256 for record in horizon)),
        bar_observation_watermark=bar_watermark,
        bar_poll_receipt_watermark=receipt_watermark,
        bar_record_sha256s=tuple(
            sorted(
                {
                    *(record.record_sha256 for record in stock_records),
                    *(record.record_sha256 for record in spy_records),
                }
            )
        ),
        bar_poll_receipt_sha256s=tuple(
            sorted({stock_receipt.record_sha256, spy_receipt.record_sha256})
        ),
    )
