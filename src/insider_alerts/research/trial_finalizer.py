"""Point-in-time entry-date finalizer for the OPP-E07-V1 shadow trial."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Literal

from insider_alerts.research.bar_feed import BarFeedStore, BarPollReceipt
from insider_alerts.research.session_feed import SessionFeedStore, SessionObservationRecord
from insider_alerts.research.trial_runtime import (
    BAR_LOOKBACK_CALENDAR_DAYS,
    MAX_CHALLENGER_SLOTS,
    MAX_SESSIONS,
    MAX_TRANSIENT_CLOCK_REGRESSION,
    NEW_YORK,
    SIGNAL_CUTOFF,
    EntryCompletionDecisionAfterOpen,
    EntryCompletionInputs,
    EntryEligibility,
    EntryLapseInputs,
    EvidenceNotReady,
    PriorBookPosition,
    TrialCandidate,
    TrialRuntimeConfig,
    TrialRuntimeInvalid,
    TrialRuntimeRetryable,
    TrialStore,
    _validated_trial_window,
    resolve_ranked_entry_date,
)
from insider_alerts.strategy.e07 import eligibility_from_completed_bars, evaluate_shadow


@dataclass(frozen=True, slots=True)
class FinalizationResult:
    status: str
    dates_completed: int = 0
    dates_lapsed: int = 0
    pending_date: date | None = None
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class _E07Config:
    lottery_salt: str = "OPP-E07-V1"
    entry_submission_deadline: time = SIGNAL_CUTOFF
    min_price: float = 2.0
    max_price: float = 200.0
    min_median_dollar_volume_20d: float = 500_000.0
    slot_budget: float = 200.0
    stop_loss_pct: float = 0.10
    take_profit_pct: float = 0.10
    max_sessions: int = MAX_SESSIONS


E07 = _E07Config()


def _official_horizon(
    records: Sequence[SessionObservationRecord], entry_date: date
) -> tuple[SessionObservationRecord, date, tuple[str, ...]]:
    ordered = sorted(
        (record for record in records if record.session.session_date >= entry_date),
        key=lambda record: record.session.session_date,
    )
    if not ordered or ordered[0].session.session_date != entry_date or len(ordered) < MAX_SESSIONS:
        raise EvidenceNotReady("official_schedule_does_not_cover_entry_horizon")
    horizon = ordered[:MAX_SESSIONS]
    return (
        horizon[0],
        horizon[-1].session.session_date,
        tuple(sorted(record.record_sha256 for record in horizon)),
    )


def _healthy_receipt(
    receipts: Sequence[BarPollReceipt],
    *,
    requested_start: date,
    required_session: SessionObservationRecord,
    decision_at: datetime,
    observation_watermark: int,
) -> BarPollReceipt:
    qualifying = [
        receipt
        for receipt in receipts
        if receipt.polled_at_utc <= decision_at
        and receipt.observation_watermark is not None
        and receipt.observation_watermark <= observation_watermark
        and receipt.polled_at_utc >= required_session.session.closes_at_utc + timedelta(minutes=1)
        and receipt.requested_start_date <= requested_start
        and receipt.requested_through_date >= required_session.session.session_date
        and receipt.completed_through_date is not None
        and receipt.completed_through_date >= required_session.session.session_date
        and receipt.source_rejection_count == 0
        and receipt.validation_rejection_count == 0
    ]
    if not qualifying:
        raise EvidenceNotReady("healthy_bar_poll_proof_unavailable")
    return min(qualifying, key=lambda receipt: receipt.sequence)


def _candidate_eligibility(
    candidate: TrialCandidate,
    *,
    schedule_records: Sequence[SessionObservationRecord],
    bar_store: BarFeedStore,
    bar_watermark: int,
    receipt_watermark: int,
    decision_at: datetime,
) -> tuple[EntryEligibility, tuple[str, ...], str]:
    cutoff = datetime.combine(candidate.planned_entry_date, SIGNAL_CUTOFF, tzinfo=NEW_YORK)
    cutoff = cutoff.astimezone(UTC)
    if candidate.evidence_recorded_at_utc >= cutoff or candidate.imported_at_utc >= cutoff:
        return EntryEligibility(False, "not_evaluated_timestamp_missed"), (), ""
    completed_sessions = sorted(
        (
            record
            for record in schedule_records
            if record.session.closes_at_utc < candidate.source_first_observed_at_utc
        ),
        key=lambda record: record.session.session_date,
    )
    if not completed_sessions:
        raise EvidenceNotReady("no_completed_pre_signal_session_schedule")
    last_required = completed_sessions[-1]
    requested_start = candidate.source_first_observed_at_utc.astimezone(
        NEW_YORK
    ).date() - timedelta(days=BAR_LOOKBACK_CALENDAR_DAYS)
    receipt = _healthy_receipt(
        bar_store.poll_receipts(candidate.symbol, max_sequence=receipt_watermark),
        requested_start=requested_start,
        required_session=last_required,
        decision_at=decision_at,
        observation_watermark=bar_watermark,
    )
    if receipt.observation_watermark is None:
        raise TrialRuntimeInvalid("healthy_receipt_missing_observation_watermark")
    completed_dates = {record.session.session_date for record in completed_sessions}
    records = [
        record
        for record in bar_store.first_observed_bar_records(
            candidate.symbol,
            start_date=requested_start,
            through_date=last_required.session.session_date,
            max_sequence=receipt.observation_watermark,
        )
        if record.bar.trade_date in completed_dates
    ]
    eligible, reason, _prior_close, _median_dv = eligibility_from_completed_bars(
        E07, [record.bar for record in records]
    )
    return (
        EntryEligibility(eligible, reason),
        tuple(record.record_sha256 for record in records),
        receipt.record_sha256,
    )


def _prior_book(
    trial_store: TrialStore,
    bar_store: BarFeedStore,
    *,
    entry_date: date,
    bar_watermark: int,
) -> tuple[tuple[PriorBookPosition, ...], frozenset[str], tuple[str, ...]]:
    candidates = {candidate.candidate_id: candidate for candidate in trial_store.candidates()}
    completions = {
        date.fromisoformat(str(record["entry_date"])): record
        for record in trial_store.entry_completion_records()
    }
    positions: list[PriorBookPosition] = []
    all_bar_digests: set[str] = set()
    for resolution in trial_store.resolutions():
        if resolution.enrollment_state != "enrolled" or resolution.entry_date >= entry_date:
            continue
        candidate = candidates[resolution.candidate_id]
        completion = completions.get(resolution.entry_date)
        if completion is None:
            raise TrialRuntimeInvalid("enrolled_position_missing_entry_completion")
        final_session = date.fromisoformat(str(completion["final_session_date"]))
        if final_session < entry_date:
            continue
        records = bar_store.first_observed_bar_records(
            candidate.symbol,
            start_date=resolution.entry_date,
            through_date=entry_date - timedelta(days=1),
            max_sequence=bar_watermark,
        )
        digests = tuple(record.record_sha256 for record in records)
        all_bar_digests.update(digests)
        bars = [record.bar for record in records]
        basis: Literal["bars_no_exit_before_entry_open", "missing_bars_conservative"] = (
            "missing_bars_conservative"
        )
        if bars and bars[0].trade_date == resolution.entry_date:
            shadow = evaluate_shadow(E07, bars, resolution.entry_date)
            if shadow.exit_bar is not None and shadow.exit_bar.trade_date < entry_date:
                continue
            basis = "bars_no_exit_before_entry_open"
        positions.append(
            PriorBookPosition(
                candidate_id=candidate.candidate_id,
                symbol=candidate.symbol,
                occupied_through_date=final_session,
                basis=basis,
                bar_record_sha256s=digests,
            )
        )
    positions.sort(key=lambda position: (position.symbol, position.candidate_id))
    symbols = frozenset(position.symbol for position in positions)
    if len(symbols) != len(positions) or len(positions) > MAX_CHALLENGER_SLOTS:
        raise TrialRuntimeInvalid("prior_book_occupancy_invalid")
    return tuple(positions), symbols, tuple(sorted(all_bar_digests))


def finalize_pending_entry_dates(
    config: TrialRuntimeConfig,
    *,
    clock: Callable[[], datetime] | None = None,
) -> FinalizationResult:
    """Seal complete entry dates without consulting mutable outcome rows."""

    clock = clock or (lambda: datetime.now(UTC))
    now = clock()
    if now.tzinfo is None:
        raise ValueError("trial finalizer clock cannot be naive")
    now = now.astimezone(UTC)
    window = _validated_trial_window(config)
    if window.status == "draft":
        return FinalizationResult("idle_registry_draft")
    trial_store = TrialStore(config.trial_db, clock=clock)
    session_store = SessionFeedStore(config.session_feed_db, initialize=False)
    bar_store = BarFeedStore(config.bar_feed_db, initialize=False)
    trial_store.validate_integrity(include_outcomes=False)
    session_store.validate_integrity()
    bar_store.validate_integrity()
    frozen = trial_store.cohort_freeze()
    if frozen is not None:
        return FinalizationResult(
            "cohort_frozen",
            reason=f"freeze_boundary_entry_date={frozen[0].isoformat()}",
        )
    resolved_ids = {resolution.candidate_id for resolution in trial_store.resolutions()}
    pending = [
        candidate
        for candidate in trial_store.candidates()
        if candidate.candidate_id not in resolved_ids
    ]
    dates = sorted({candidate.planned_entry_date for candidate in pending})
    completed = lapsed = 0
    for entry_date in dates:
        candidates = [
            candidate for candidate in pending if candidate.planned_entry_date == entry_date
        ]
        schedule_watermark = session_store.observation_watermark()
        schedule_records = session_store.schedule_records_as_known_at(
            now, max_sequence=schedule_watermark
        )
        try:
            entry_record, final_session, horizon_digests = _official_horizon(
                schedule_records, entry_date
            )
        except EvidenceNotReady as exc:
            return FinalizationResult("waiting", completed, lapsed, entry_date, str(exc))
        entry_open = entry_record.session.opens_at_utc
        cutoff = datetime.combine(entry_date, SIGNAL_CUTOFF, tzinfo=NEW_YORK).astimezone(UTC)
        used_schedule_digests = set(horizon_digests)
        if now < cutoff:
            return FinalizationResult("waiting", completed, lapsed, entry_date, "before_cutoff")
        if now >= entry_open:
            trial_store.append_entry_lapse(
                EntryLapseInputs(
                    entry_date=entry_date,
                    lapsed_at_utc=now,
                    reason="valid_completion_not_committed_before_official_open",
                    entry_opens_at_utc=entry_open,
                    schedule_observation_watermark=schedule_watermark,
                    schedule_record_sha256s=tuple(sorted(used_schedule_digests)),
                )
            )
            lapsed += 1
            continue
        bar_watermark = bar_store.observation_watermark()
        receipt_watermark = bar_store.poll_receipt_watermark()
        eligibility: dict[str, EntryEligibility] = {}
        bar_digests: set[str] = set()
        poll_digests: set[str] = set()
        try:
            for candidate in candidates:
                decision, candidate_bars, receipt_digest = _candidate_eligibility(
                    candidate,
                    schedule_records=schedule_records,
                    bar_store=bar_store,
                    bar_watermark=bar_watermark,
                    receipt_watermark=receipt_watermark,
                    decision_at=now,
                )
                eligibility[candidate.candidate_id] = decision
                bar_digests.update(candidate_bars)
                if receipt_digest:
                    poll_digests.add(receipt_digest)
                used_schedule_digests.update(
                    record.record_sha256
                    for record in schedule_records
                    if record.session.closes_at_utc < candidate.source_first_observed_at_utc
                )
            prior_positions, occupied_symbols, prior_bar_digests = _prior_book(
                trial_store,
                bar_store,
                entry_date=entry_date,
                bar_watermark=bar_watermark,
            )
        except EvidenceNotReady as exc:
            return FinalizationResult("waiting", completed, lapsed, entry_date, str(exc))
        bar_digests.update(prior_bar_digests)
        next_sequence = 1 + max(
            (
                resolution.confirmatory_enrollment_sequence or 0
                for resolution in trial_store.resolutions()
            ),
            default=0,
        )
        resolutions = resolve_ranked_entry_date(
            candidates,
            eligibility=eligibility,
            occupied_symbols=occupied_symbols,
            occupied_slots=len(occupied_symbols),
            next_enrollment_sequence=next_sequence,
            completed_at_utc=now,
            entry_opens_at_utc=entry_open,
        )
        completion_inputs = EntryCompletionInputs(
            entry_date=entry_date,
            completed_at_utc=now,
            entry_opens_at_utc=entry_open,
            final_session_date=final_session,
            schedule_observation_watermark=schedule_watermark,
            schedule_record_sha256s=tuple(sorted(used_schedule_digests)),
            bar_observation_watermark=bar_watermark,
            bar_poll_receipt_watermark=receipt_watermark,
            bar_record_sha256s=tuple(sorted(bar_digests)),
            bar_poll_receipt_sha256s=tuple(sorted(poll_digests)),
            prior_book_positions=prior_positions,
        )
        try:
            trial_store.append_entry_completion(completion_inputs, resolutions)
        except EntryCompletionDecisionAfterOpen as exc:
            rolled_at = clock()
            if rolled_at.tzinfo is None:
                raise ValueError("trial finalizer clock cannot be naive") from exc
            rolled_at = rolled_at.astimezone(UTC)
            if rolled_at < entry_open:
                regression = exc.decision_clock_at_utc - rolled_at
                if regression <= MAX_TRANSIENT_CLOCK_REGRESSION:
                    raise TrialRuntimeRetryable(
                        "entry_completion_clock_moved_backwards_across_open"
                    ) from exc
                raise TrialRuntimeInvalid(
                    "entry_completion_clock_regression_exceeds_limit"
                ) from exc
            trial_store.append_entry_lapse(
                EntryLapseInputs(
                    entry_date=entry_date,
                    lapsed_at_utc=rolled_at,
                    reason="decision_clock_reached_official_open_before_seal",
                    entry_opens_at_utc=entry_open,
                    schedule_observation_watermark=schedule_watermark,
                    schedule_record_sha256s=tuple(sorted(used_schedule_digests)),
                )
            )
            lapsed += 1
            continue
        completed += 1
        if trial_store.cohort_freeze() is not None:
            break
    return FinalizationResult("complete", completed, lapsed)
