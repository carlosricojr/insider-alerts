"""Isolated outcome materializer for prospective canary-control diagnostics."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Literal

from insider_alerts.research.bar_feed import BarFeedStore
from insider_alerts.research.diagnostics import (
    DIAGNOSTIC_CONTRACT_VERSION,
    DiagnosticConfig,
    DiagnosticStore,
    _diagnostic_trade_id,
    _parse_utc,
    _utc_text,
)
from insider_alerts.research.inference import HYPOTHESIS_ID
from insider_alerts.research.outcome_proof import (
    FrozenScheduleBinding,
    ResearchOutcomeProof,
    materialize_research_outcome,
)
from insider_alerts.research.session_feed import SessionFeedStore
from insider_alerts.research.trial_finalizer import E07
from insider_alerts.research.trial_runtime import (
    TrialRuntimeConfig,
    TrialRuntimeInvalid,
    _validated_trial_window,
)


@dataclass(frozen=True, slots=True)
class DiagnosticOutcomeResult:
    status: Literal["idle_registry_draft", "collecting", "degraded"]
    candidates_seen: int = 0
    outcomes_added: int = 0
    receipts_added: int = 0
    outcomes_waiting: int = 0
    unavailable_total: int = 0
    reconciliations_added: int = 0
    error: str | None = None


def _agreement(
    proof: ResearchOutcomeProof,
    shadow_trade: dict[str, Any],
) -> dict[str, Any]:
    expected: dict[str, object] = {
        "symbol": proof.symbol,
        "entry_session": proof.entry_date.isoformat(),
        "entry_price": proof.entry_price,
        "exit_session": proof.exit_date.isoformat(),
        "exit_price": proof.exit_price,
        "exit_reason": proof.exit_reason,
        "gross_return": proof.gross_return,
    }
    mismatches: list[str] = []
    for name, value in expected.items():
        observed = shadow_trade.get(name)
        if isinstance(value, float):
            try:
                matches = math.isclose(float(str(observed)), value, rel_tol=1e-12, abs_tol=1e-12)
            except (TypeError, ValueError):
                matches = False
        else:
            matches = observed == value
        if not matches:
            mismatches.append(name)
    return {
        "status": "match" if not mismatches else "mismatch",
        "mismatched_fields": sorted(mismatches),
    }


def _outcome_record(
    *,
    packet_id: str,
    candidate_id: str,
    candidate_record_sha256: str,
    state_binding_record_sha256: str,
    proof: ResearchOutcomeProof,
    agreement: dict[str, Any],
    now: datetime,
) -> dict[str, Any]:
    return {
        "contract_version": DIAGNOSTIC_CONTRACT_VERSION,
        "hypothesis_id": HYPOTHESIS_ID,
        "packet_id": packet_id,
        "candidate_id": candidate_id,
        "trade_id": _diagnostic_trade_id(packet_id),
        "candidate_record_sha256": candidate_record_sha256,
        "state_binding_record_sha256": state_binding_record_sha256,
        "symbol": proof.symbol,
        "entry_date": proof.entry_date.isoformat(),
        "entry_at_utc": _utc_text(proof.entry_at_utc),
        "exit_date": proof.exit_date.isoformat(),
        "exit_at_utc": _utc_text(proof.exit_at_utc),
        "entry_price": proof.entry_price,
        "exit_price": proof.exit_price,
        "exit_reason": proof.exit_reason,
        "gross_return": proof.gross_return,
        "spy_entry_price": proof.spy_entry_price,
        "spy_exit_price": proof.spy_exit_price,
        "spy_return": proof.spy_return,
        "schedule_observation_watermark": proof.schedule_observation_watermark,
        "schedule_record_sha256s": list(proof.schedule_record_sha256s),
        "bar_observation_watermark": proof.bar_observation_watermark,
        "bar_poll_receipt_watermark": proof.bar_poll_receipt_watermark,
        "bar_record_sha256s": list(proof.bar_record_sha256s),
        "bar_poll_receipt_sha256s": list(proof.bar_poll_receipt_sha256s),
        "canary_agreement": agreement,
        "recorded_at_utc": _utc_text(now),
    }


def _receipt_record(
    *,
    packet_id: str,
    candidate_id: str,
    candidate_record_sha256: str,
    state_binding_record_sha256: str,
    evidence_binding_record_sha256: str | None,
    disposition: Literal["available", "not_traded", "unavailable"],
    reason: str,
    now: datetime,
) -> dict[str, Any]:
    return {
        "contract_version": DIAGNOSTIC_CONTRACT_VERSION,
        "hypothesis_id": HYPOTHESIS_ID,
        "packet_id": packet_id,
        "candidate_id": candidate_id,
        "candidate_record_sha256": candidate_record_sha256,
        "state_binding_record_sha256": state_binding_record_sha256,
        "evidence_binding_record_sha256": evidence_binding_record_sha256,
        "disposition": disposition,
        "reason": reason,
        "recorded_at_utc": _utc_text(now),
    }


def _schedule_binding(candidate_record: dict[str, Any]) -> FrozenScheduleBinding:
    selection = candidate_record.get("canary_selection")
    schedule = candidate_record.get("schedule_binding")
    if not isinstance(selection, dict) or not isinstance(schedule, dict):
        raise TrialRuntimeInvalid("diagnostic_outcome_candidate_binding_missing")
    entry_text = selection.get("entry_session")
    final_text = schedule.get("final_session")
    if not isinstance(entry_text, str) or not isinstance(final_text, str):
        raise TrialRuntimeInvalid("diagnostic_outcome_schedule_dates_missing")
    try:
        entry_date = date.fromisoformat(entry_text)
        final_date = date.fromisoformat(final_text)
        signal_at = _parse_utc(str(selection["signal_at_utc"]))
        watermark = int(schedule["observation_watermark"])
        digests = tuple(str(value) for value in schedule["record_sha256s"])
    except (KeyError, TypeError, ValueError) as exc:
        raise TrialRuntimeInvalid("diagnostic_outcome_schedule_binding_invalid") from exc
    return FrozenScheduleBinding(
        entry_date=entry_date,
        final_session_date=final_date,
        as_of_utc=signal_at,
        observation_watermark=watermark,
        record_sha256s=digests,
    )


def finalize_diagnostic_outcomes(
    config: DiagnosticConfig,
    *,
    now: datetime | None = None,
) -> DiagnosticOutcomeResult:
    """Materialize every ready control outcome without blocking independent candidates."""

    now = (now or datetime.now(UTC)).astimezone(UTC)
    store = DiagnosticStore(config.diagnostics_db)
    window = _validated_trial_window(
        TrialRuntimeConfig(
            trial_db=config.diagnostics_db.with_name("unused-diagnostic-trial.db"),
            evidence_db=config.evidence_db,
            bar_feed_db=config.bar_feed_db,
            session_feed_db=config.session_feed_db,
            registry_path=config.registry_path,
        )
    )
    if window.status == "draft":
        result = DiagnosticOutcomeResult("idle_registry_draft")
        store.write_outcome_health(
            now=now,
            status=result.status,
            error=None,
            candidates_seen=0,
            outcomes_waiting=0,
        )
        return result
    store.validate_integrity()
    session_store = SessionFeedStore(config.session_feed_db, initialize=False)
    bar_store = BarFeedStore(config.bar_feed_db, initialize=False)
    session_store.validate_integrity()
    bar_store.validate_integrity()
    candidates = store.candidates()
    outcomes_added = receipts_added = waiting = reconciliations = 0
    for candidate in candidates:
        packet_id = str(candidate["packet_id"])
        if store.outcome_receipt(packet_id) is not None:
            continue
        candidate_record = json.loads(bytes(candidate["record_json"]))
        if not isinstance(candidate_record, dict):
            raise ValueError("diagnostic outcome candidate is not an object")
        state = store.state_binding(packet_id)
        if state is None:
            waiting += 1
            continue
        state_record = json.loads(bytes(state["record_json"]))
        if not isinstance(state_record, dict):
            raise ValueError("diagnostic outcome state is not an object")
        evidence = store.evidence_binding(packet_id)
        evidence_sha = str(evidence["record_sha256"]) if evidence is not None else None
        candidate_id = str(candidate["candidate_id"])
        candidate_sha = str(candidate["record_sha256"])
        state_sha = str(state["record_sha256"])

        def receipt(
            disposition: Literal["available", "not_traded", "unavailable"],
            reason: str,
            *,
            bound_packet_id: str = packet_id,
            bound_candidate_id: str = candidate_id,
            bound_candidate_sha: str = candidate_sha,
            bound_state_sha: str = state_sha,
            bound_evidence_sha: str | None = evidence_sha,
        ) -> dict[str, Any]:
            return _receipt_record(
                packet_id=bound_packet_id,
                candidate_id=bound_candidate_id,
                candidate_record_sha256=bound_candidate_sha,
                state_binding_record_sha256=bound_state_sha,
                evidence_binding_record_sha256=bound_evidence_sha,
                disposition=disposition,
                reason=reason,
                now=now,
            )

        shadow_state = str(state["shadow_state"])
        if shadow_state != "closed":
            _, receipt_added = store.append_outcome_receipt(
                outcome_record=None,
                receipt_record=receipt("not_traded", f"canary_shadow_{shadow_state}"),
            )
            receipts_added += int(receipt_added)
            continue
        canary_state = state_record.get("canary_state")
        shadow_trade = canary_state.get("shadow_trade") if isinstance(canary_state, dict) else None
        if not isinstance(shadow_trade, dict):
            terminal_disposition: Literal["unavailable"] = "unavailable"
            reason = "diagnostic_closed_state_shadow_trade_missing"
            proof = None
        else:
            try:
                selection = candidate_record["canary_selection"]
                if not isinstance(selection, dict):
                    raise TrialRuntimeInvalid("diagnostic_outcome_selection_missing")
                proof = materialize_research_outcome(
                    symbol=str(selection["symbol"]),
                    schedule_binding=_schedule_binding(candidate_record),
                    session_store=session_store,
                    bar_store=bar_store,
                    policy=E07,
                    now=now,
                )
            except (KeyError, TypeError, ValueError, TrialRuntimeInvalid) as exc:
                proof = None
                terminal_disposition = "unavailable"
                reason = f"{type(exc).__name__}:{exc}"[:1000]
            else:
                if proof is None:
                    waiting += 1
                    continue
                agreement = _agreement(proof, shadow_trade)
                outcome = _outcome_record(
                    packet_id=packet_id,
                    candidate_id=candidate_id,
                    candidate_record_sha256=candidate_sha,
                    state_binding_record_sha256=state_sha,
                    proof=proof,
                    agreement=agreement,
                    now=now,
                )
                if agreement["status"] == "match":
                    outcome_disposition: Literal["available", "unavailable"] = "available"
                    reason = "research_e07_outcome_available"
                else:
                    outcome_disposition = "unavailable"
                    reason = "canary_research_outcome_mismatch"
                outcome_added, receipt_added = store.append_outcome_receipt(
                    outcome_record=outcome,
                    receipt_record=receipt(outcome_disposition, reason),
                )
                outcomes_added += int(outcome_added)
                receipts_added += int(receipt_added)
                if agreement["status"] == "mismatch":
                    reconciliations += int(
                        store.add_reconciliation(
                            packet_id=packet_id,
                            category="canary_research_outcome_mismatch",
                            detail={
                                "state_binding_record_sha256": str(state["record_sha256"]),
                                "mismatched_fields": agreement["mismatched_fields"],
                            },
                            now=now,
                        )
                    )
                continue
        _, receipt_added = store.append_outcome_receipt(
            outcome_record=None,
            receipt_record=receipt(terminal_disposition, reason),
        )
        receipts_added += int(receipt_added)
    store.validate_integrity()
    unavailable = store.outcome_disposition_counts().get("unavailable", 0)
    status: Literal["collecting", "degraded"] = "degraded" if unavailable else "collecting"
    result = DiagnosticOutcomeResult(
        status,
        candidates_seen=len(candidates),
        outcomes_added=outcomes_added,
        receipts_added=receipts_added,
        outcomes_waiting=waiting,
        unavailable_total=unavailable,
        reconciliations_added=reconciliations,
        error="diagnostic_outcomes_unavailable" if unavailable else None,
    )
    store.write_outcome_health(
        now=now,
        status=result.status,
        error=result.error,
        candidates_seen=result.candidates_seen,
        outcomes_waiting=result.outcomes_waiting,
    )
    return result
