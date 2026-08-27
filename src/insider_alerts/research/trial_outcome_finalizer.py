"""Outcome materializer for the blinded OPP-E07-V1 prospective trial."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Literal, cast

from insider_alerts.research.bar_feed import BarFeedStore, BarObservationRecord, BarPollReceipt
from insider_alerts.research.session_feed import SessionFeedStore, SessionObservationRecord
from insider_alerts.research.trial_finalizer import E07
from insider_alerts.research.trial_runtime import (
    MAX_SESSIONS,
    TrialCandidate,
    TrialOutcomeInputs,
    TrialResolution,
    TrialRuntimeConfig,
    TrialRuntimeInvalid,
    TrialStore,
    _parse_utc,
    _validated_trial_window,
)
from insider_alerts.strategy.e07 import evaluate_shadow


@dataclass(frozen=True, slots=True)
class OutcomeFinalizationResult:
    status: str
    outcomes_added: int = 0
    outcomes_waiting: int = 0
    reason: str | None = None


def _bound_horizon(
    session_store: SessionFeedStore,
    completion: dict[str, Any],
    candidate: TrialCandidate,
) -> tuple[SessionObservationRecord, ...]:
    watermark = int(completion["schedule_observation_watermark"])
    decision_at = _parse_utc(str(completion["decision_clock_at_utc"]))
    bound_digests = {str(value) for value in completion["schedule_record_sha256s"]}
    records = session_store.schedule_records_as_known_at(
        decision_at,
        max_sequence=watermark,
    )
    horizon = tuple(
        record
        for record in records
        if candidate.planned_entry_date
        <= record.session.session_date
        <= candidate.final_session_date
    )
    if (
        len(horizon) != MAX_SESSIONS
        or horizon[0].session.session_date != candidate.planned_entry_date
        or horizon[-1].session.session_date != candidate.final_session_date
        or horizon[0].session.opens_at_utc != _parse_utc(str(completion["entry_opens_at_utc"]))
        or any(record.record_sha256 not in bound_digests for record in horizon)
    ):
        raise TrialRuntimeInvalid("outcome_frozen_schedule_binding_invalid")
    return horizon


def _terminal_receipt(
    receipts: Sequence[BarPollReceipt],
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


def _materialize_one(
    *,
    candidate: TrialCandidate,
    resolution: TrialResolution,
    completion: dict[str, Any],
    session_store: SessionFeedStore,
    bar_store: BarFeedStore,
    trial_store: TrialStore,
    now: datetime,
) -> str | None:
    horizon = _bound_horizon(session_store, completion, candidate)
    final_session = horizon[-1]
    if now < final_session.session.closes_at_utc:
        return None
    bar_watermark = bar_store.observation_watermark()
    receipt_watermark = bar_store.poll_receipt_watermark()
    stock_receipt = _terminal_receipt(
        bar_store.poll_receipts(candidate.symbol, max_sequence=receipt_watermark),
        entry_date=candidate.planned_entry_date,
        final_session=final_session,
        now=now,
        bar_watermark=bar_watermark,
    )
    spy_receipt = _terminal_receipt(
        bar_store.poll_receipts("SPY", max_sequence=receipt_watermark),
        entry_date=candidate.planned_entry_date,
        final_session=final_session,
        now=now,
        bar_watermark=bar_watermark,
    )
    if stock_receipt is None or spy_receipt is None:
        return None
    horizon_dates = tuple(record.session.session_date for record in horizon)
    stock_current, stock_proven = _horizon_record_maps(
        bar_store,
        symbol=candidate.symbol,
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
        E07,
        [record.bar for record in stock_prefix],
        candidate.planned_entry_date,
    )
    if (
        shadow.entry_bar is None
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
    required_spy_dates = {candidate.planned_entry_date, shadow.exit_bar.trade_date}
    if any(session_date not in spy_current for session_date in required_spy_dates):
        raise TrialRuntimeInvalid("outcome_terminal_spy_benchmark_incomplete")
    if any(session_date not in spy_proven for session_date in required_spy_dates):
        return None
    stock_records = tuple(stock_proven[session_date] for session_date in required_stock_dates)
    spy_records = tuple(spy_proven[session_date] for session_date in sorted(required_spy_dates))
    spy_entry = spy_proven[candidate.planned_entry_date].bar
    spy_exit = spy_proven[shadow.exit_bar.trade_date].bar
    schedule_digests = tuple(sorted(record.record_sha256 for record in horizon))
    bar_digests = tuple(
        sorted(
            {
                *(record.record_sha256 for record in stock_records),
                *(record.record_sha256 for record in spy_records),
            }
        )
    )
    receipt_digests = tuple(sorted({stock_receipt.record_sha256, spy_receipt.record_sha256}))
    entry_price = shadow.entry_bar.open
    gross_return = shadow.exit_price / entry_price - 1.0
    spy_return = spy_exit.close / spy_entry.open - 1.0
    return trial_store.append_outcome(
        TrialOutcomeInputs(
            candidate_id=candidate.candidate_id,
            confirmatory_enrollment_sequence=resolution.confirmatory_enrollment_sequence or 0,
            evidence_record_sha256=candidate.evidence_record_sha256,
            entry_rank_sha256=candidate.entry_rank_sha256,
            symbol=candidate.symbol,
            entry_date=candidate.planned_entry_date,
            entry_at_utc=horizon[0].session.opens_at_utc,
            exit_date=shadow.exit_bar.trade_date,
            exit_at_utc=exit_session.session.closes_at_utc,
            entry_price=entry_price,
            exit_price=shadow.exit_price,
            exit_reason=cast(
                Literal["stop", "target", "time", "stop_and_target_same_day_stop_assumed"],
                shadow.exit_reason,
            ),
            gross_return=gross_return,
            spy_entry_price=spy_entry.open,
            spy_exit_price=spy_exit.close,
            spy_return=spy_return,
            recorded_at_utc=now,
            schedule_observation_watermark=int(completion["schedule_observation_watermark"]),
            schedule_record_sha256s=schedule_digests,
            bar_observation_watermark=bar_watermark,
            bar_poll_receipt_watermark=receipt_watermark,
            bar_record_sha256s=bar_digests,
            bar_poll_receipt_sha256s=receipt_digests,
        )
    )


def finalize_trial_outcomes(
    config: TrialRuntimeConfig,
    *,
    clock: Callable[[], datetime] | None = None,
) -> OutcomeFinalizationResult:
    """Materialize individual outcomes without calculating or reporting aggregates."""

    clock = clock or (lambda: datetime.now(UTC))
    now = clock()
    if now.tzinfo is None:
        raise ValueError("trial outcome finalizer clock cannot be naive")
    now = now.astimezone(UTC)
    window = _validated_trial_window(config)
    if window.status == "draft":
        return OutcomeFinalizationResult("idle_registry_draft")
    trial_store = TrialStore(config.trial_db)
    session_store = SessionFeedStore(config.session_feed_db, initialize=False)
    bar_store = BarFeedStore(config.bar_feed_db, initialize=False)
    trial_store.validate_integrity()
    session_store.validate_integrity()
    bar_store.validate_integrity()
    candidates = {candidate.candidate_id: candidate for candidate in trial_store.candidates()}
    completions = {
        str(completion["entry_date"]): completion
        for completion in trial_store.entry_completion_records()
    }
    existing = trial_store.outcome_candidate_ids()
    enrolled = sorted(
        (
            resolution
            for resolution in trial_store.resolutions()
            if resolution.enrollment_state == "enrolled" and resolution.candidate_id not in existing
        ),
        key=lambda item: item.confirmatory_enrollment_sequence or 0,
    )
    added = 0
    waiting = 0
    for resolution in enrolled:
        candidate = candidates.get(resolution.candidate_id)
        if candidate is None:
            raise TrialRuntimeInvalid("outcome_candidate_missing")
        completion = completions.get(candidate.planned_entry_date.isoformat())
        if completion is None:
            raise TrialRuntimeInvalid("outcome_entry_completion_missing")
        digest = _materialize_one(
            candidate=candidate,
            resolution=resolution,
            completion=completion,
            session_store=session_store,
            bar_store=bar_store,
            trial_store=trial_store,
            now=now,
        )
        if digest is None:
            waiting += 1
        else:
            added += 1
    if waiting:
        return OutcomeFinalizationResult(
            "waiting",
            outcomes_added=added,
            outcomes_waiting=waiting,
            reason="terminal_bar_or_receipt_proof_unavailable",
        )
    return OutcomeFinalizationResult("complete", added)
