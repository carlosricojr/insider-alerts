"""Outcome materializer for the blinded OPP-E07-V1 prospective trial."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from insider_alerts.research.bar_feed import BarFeedStore
from insider_alerts.research.outcome_proof import (
    FrozenScheduleBinding,
    bound_horizon,
    materialize_research_outcome,
)
from insider_alerts.research.session_feed import SessionFeedStore, SessionObservationRecord
from insider_alerts.research.trial_finalizer import E07
from insider_alerts.research.trial_runtime import (
    TrialCandidate,
    TrialOutcomeInputs,
    TrialResolution,
    TrialRuntimeConfig,
    TrialRuntimeInvalid,
    TrialStore,
    _parse_utc,
    _validated_trial_window,
)


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
    return bound_horizon(
        session_store,
        FrozenScheduleBinding(
            entry_date=candidate.planned_entry_date,
            final_session_date=candidate.final_session_date,
            as_of_utc=_parse_utc(str(completion["decision_clock_at_utc"])),
            observation_watermark=int(completion["schedule_observation_watermark"]),
            record_sha256s=tuple(str(value) for value in completion["schedule_record_sha256s"]),
            expected_entry_opens_at_utc=_parse_utc(str(completion["entry_opens_at_utc"])),
            digest_scope="known_schedule_superset",
        ),
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
    proof = materialize_research_outcome(
        symbol=candidate.symbol,
        schedule_binding=FrozenScheduleBinding(
            entry_date=candidate.planned_entry_date,
            final_session_date=candidate.final_session_date,
            as_of_utc=_parse_utc(str(completion["decision_clock_at_utc"])),
            observation_watermark=int(completion["schedule_observation_watermark"]),
            record_sha256s=tuple(str(value) for value in completion["schedule_record_sha256s"]),
            expected_entry_opens_at_utc=_parse_utc(str(completion["entry_opens_at_utc"])),
            digest_scope="known_schedule_superset",
        ),
        session_store=session_store,
        bar_store=bar_store,
        policy=E07,
        now=now,
    )
    if proof is None:
        return None
    return trial_store.append_outcome(
        TrialOutcomeInputs(
            candidate_id=candidate.candidate_id,
            confirmatory_enrollment_sequence=resolution.confirmatory_enrollment_sequence or 0,
            evidence_record_sha256=candidate.evidence_record_sha256,
            entry_rank_sha256=candidate.entry_rank_sha256,
            symbol=candidate.symbol,
            entry_date=candidate.planned_entry_date,
            entry_at_utc=proof.entry_at_utc,
            exit_date=proof.exit_date,
            exit_at_utc=proof.exit_at_utc,
            entry_price=proof.entry_price,
            exit_price=proof.exit_price,
            exit_reason=proof.exit_reason,
            gross_return=proof.gross_return,
            spy_entry_price=proof.spy_entry_price,
            spy_exit_price=proof.spy_exit_price,
            spy_return=proof.spy_return,
            recorded_at_utc=now,
            schedule_observation_watermark=proof.schedule_observation_watermark,
            schedule_record_sha256s=proof.schedule_record_sha256s,
            bar_observation_watermark=proof.bar_observation_watermark,
            bar_poll_receipt_watermark=proof.bar_poll_receipt_watermark,
            bar_record_sha256s=proof.bar_record_sha256s,
            bar_poll_receipt_sha256s=proof.bar_poll_receipt_sha256s,
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
