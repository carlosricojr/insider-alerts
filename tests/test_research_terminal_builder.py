from __future__ import annotations

import hashlib
import json
import sqlite3
import subprocess
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

import insider_alerts.research.inference as inference
import insider_alerts.research.terminal_builder as builder
from insider_alerts.research.diagnostics import DiagnosticStore
from insider_alerts.research.trial_runtime import (
    EntryCompletionInputs,
    TrialCandidate,
    TrialOutcomeInputs,
    TrialResolution,
    TrialStore,
)

ROOT = Path(__file__).resolve().parents[1]
ACTIVATED_AT = datetime(2026, 1, 31, 15, 0, tzinfo=UTC)


def _rank(entry_date: date, packet_id: str, accession: str, symbol: str) -> str:
    material = (
        f"{inference.CAPACITY_RANK_SALT}|{entry_date.isoformat()}|{packet_id}|{accession}|{symbol}"
    )
    return hashlib.sha256(material.encode()).hexdigest()


def _candidate(candidate_id: str, entry_date: date, symbol: str) -> TrialCandidate:
    packet_id = f"packet-{candidate_id}"
    accession = f"accession-{candidate_id}"
    observed = datetime.combine(entry_date - timedelta(days=1), datetime.min.time(), UTC)
    observed += timedelta(hours=21)
    return TrialCandidate(
        candidate_id=candidate_id,
        evidence_snapshot_id=f"snapshot-{candidate_id}",
        evidence_record_sha256=hashlib.sha256(f"evidence-{candidate_id}".encode()).hexdigest(),
        packet_id=packet_id,
        accession_number=accession,
        symbol=symbol,
        source_first_observed_at_utc=observed,
        evidence_recorded_at_utc=observed + timedelta(seconds=1),
        classification_state="opportunistic",
        transaction_owner_mapping="exact",
        history_coverage_complete=True,
        planned_entry_date=entry_date,
        entry_opens_at_utc=datetime.combine(entry_date, datetime.min.time(), UTC)
        + timedelta(hours=14, minutes=30),
        final_session_date=entry_date + timedelta(days=14),
        entry_rank_sha256=_rank(entry_date, packet_id, accession, symbol),
        imported_at_utc=observed + timedelta(seconds=2),
    )


def _install_trial(path: Path) -> None:
    current_clock = [datetime(2026, 2, 2, 14, 21, tzinfo=UTC)]
    store = TrialStore(path, clock=lambda: current_clock[0])
    groups = (
        (date(2026, 2, 2), [_candidate("a", date(2026, 2, 2), "AAA")]),
        (
            date(2026, 2, 3),
            [
                _candidate("b", date(2026, 2, 3), "BBB"),
                _candidate("c", date(2026, 2, 3), "CCC"),
            ],
        ),
    )
    sequence = 1
    for entry_date, candidates in groups:
        completed_at = datetime.combine(entry_date, datetime.min.time(), UTC) + timedelta(
            hours=14, minutes=20
        )
        current_clock[0] = completed_at + timedelta(seconds=1)
        for candidate in candidates:
            assert store.append_candidate(candidate)
        resolutions = []
        for candidate in sorted(candidates, key=lambda item: item.entry_rank_sha256):
            resolutions.append(
                TrialResolution(
                    candidate_id=candidate.candidate_id,
                    entry_date=entry_date,
                    enrollment_state="enrolled",
                    reason="eligible_E07_F00",
                    confirmatory_enrollment_sequence=sequence,
                    resolved_at_utc=completed_at,
                )
            )
            sequence += 1
        store.append_entry_completion(
            EntryCompletionInputs(
                entry_date=entry_date,
                completed_at_utc=completed_at,
                entry_opens_at_utc=datetime.combine(entry_date, datetime.min.time(), UTC)
                + timedelta(hours=14, minutes=30),
                final_session_date=entry_date + timedelta(days=14),
                schedule_observation_watermark=1,
                schedule_record_sha256s=("a" * 64,),
                bar_observation_watermark=1,
                bar_poll_receipt_watermark=1,
                bar_record_sha256s=("b" * 64,),
                bar_poll_receipt_sha256s=("c" * 64,),
                prior_book_positions=(),
            ),
            resolutions,
        )
    candidates = {item.candidate_id: item for item in store.candidates()}
    for resolution in store.resolutions():
        candidate = candidates[resolution.candidate_id]
        entry_at = candidate.entry_opens_at_utc
        exit_at = entry_at + timedelta(days=1, hours=6, minutes=30)
        store.append_outcome(
            TrialOutcomeInputs(
                candidate_id=candidate.candidate_id,
                confirmatory_enrollment_sequence=resolution.confirmatory_enrollment_sequence or 0,
                evidence_record_sha256=candidate.evidence_record_sha256,
                entry_rank_sha256=candidate.entry_rank_sha256,
                symbol=candidate.symbol,
                entry_date=candidate.planned_entry_date,
                entry_at_utc=entry_at,
                exit_date=exit_at.date(),
                exit_at_utc=exit_at,
                entry_price=100.0,
                exit_price=103.0,
                exit_reason="time",
                gross_return=0.03,
                spy_entry_price=100.0,
                spy_exit_price=101.0,
                spy_return=0.01,
                recorded_at_utc=exit_at + timedelta(seconds=1),
                schedule_observation_watermark=1,
                schedule_record_sha256s=("a" * 64,),
                bar_observation_watermark=1,
                bar_poll_receipt_watermark=1,
                bar_record_sha256s=("b" * 64,),
                bar_poll_receipt_sha256s=("c" * 64,),
            )
        )
    store.validate_integrity()


def _empty_external_stores(tmp_path: Path) -> tuple[Path, Path, Path]:
    diagnostics = tmp_path / "diagnostics.db"
    DiagnosticStore(diagnostics).validate_integrity()
    canary = tmp_path / "canary.db"
    with sqlite3.connect(canary) as conn:
        conn.execute("CREATE TABLE candidates(packet_id TEXT,signal_at TEXT,entry_session TEXT)")
    source = tmp_path / "source.db"
    with sqlite3.connect(source) as conn:
        conn.execute(
            "CREATE TABLE research_capture_jobs(packet_id TEXT,"
            "source_first_observed_at_utc TEXT,decision_at_utc TEXT)"
        )
    return diagnostics, canary, source


def _active_registry_path(tmp_path: Path) -> Path:
    registry = json.loads(
        (ROOT / "docs/research/registry/OPP-E07-V1.json").read_text(encoding="utf-8")
    )
    registry["status"] = "active"
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    file_sha = inference._file_sha256
    registry["activation"] = {
        "activated_at_utc": builder._utc_text(ACTIVATED_AT),
        "activation_git_commit": commit,
        "registry_definition_sha256": inference.registry_definition_sha256(registry),
        "preregistration_sha256": file_sha(ROOT / registry["preregistration"]),
        "hypothesis_schema_sha256": file_sha(
            ROOT / "docs/research/contracts/hypothesis-registry.schema.json"
        ),
        "evidence_schema_sha256": file_sha(
            ROOT / "docs/research/contracts/evidence-snapshot.schema.json"
        ),
        "inference_artifact_sha256": inference.inference_artifact_sha256(),
        "terminal_builder_artifact_sha256": file_sha(
            ROOT / "src/insider_alerts/research/terminal_builder.py"
        ),
        "dependency_lock_sha256": file_sha(ROOT / "uv.lock"),
        "policy_sha256": file_sha(ROOT / registry["strategy"]["policy_artifact"]),
        "classifier_version": inference.CLASSIFIER_VERSION,
        "enrollment_start_sequence": 1,
    }
    path = tmp_path / "active-registry.json"
    path.write_text(json.dumps(registry), encoding="utf-8")
    return path


def test_builder_includes_every_boundary_date_trade_and_prevalidates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(inference, "TARGET_ENROLLED_TRADES", 2)
    monkeypatch.setattr(inference, "MINIMUM_DISTINCT_ENTRY_DATES", 2)
    trial = tmp_path / "trial.db"
    _install_trial(trial)
    diagnostics, canary, source = _empty_external_stores(tmp_path)
    connections = []
    try:
        for path in (trial, diagnostics, canary, source):
            conn = sqlite3.connect(path)
            conn.row_factory = sqlite3.Row
            conn.execute("BEGIN IMMEDIATE")
            connections.append(conn)
        snapshot, terminal, counts = builder._build_dataset_locked(
            *connections,
            activated_at=ACTIVATED_AT,
        )
    finally:
        for conn in reversed(connections):
            conn.rollback()
            conn.close()

    assert terminal["freeze_boundary_entry_date"] == "2026-02-03"
    assert counts == {"frozen": 3, "control": 0, "routine": 0}
    assert len(terminal["challenger_trades"]) == 3
    assert terminal["diagnostic_group_status"]["control"]["status"] == "available"
    evaluated = datetime.fromisoformat(terminal["sealed_at_utc"].replace("Z", "+00:00"))
    payload = builder._terminal_payload(
        snapshot,
        terminal,
        activated_at=ACTIVATED_AT,
        evaluated_at=evaluated,
    )
    registry = json.loads(
        (ROOT / "docs/research/registry/OPP-E07-V1.json").read_text(encoding="utf-8")
    )
    report = inference.evaluate_trial(
        registry,
        payload,
        allow_draft=True,
        terminal_validation_only=True,
    )
    assert report["state"] == "COLLECTING"
    assert report["reason_codes"] == ["terminal_payload_validated_not_evaluated"]


def test_builder_waits_for_every_frozen_challenger_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(inference, "TARGET_ENROLLED_TRADES", 2)
    monkeypatch.setattr(inference, "MINIMUM_DISTINCT_ENTRY_DATES", 2)
    trial = tmp_path / "trial.db"
    _install_trial(trial)
    with sqlite3.connect(trial) as conn:
        conn.execute("DROP TRIGGER trial_outcomes_no_delete")
        conn.execute("DELETE FROM trial_outcomes WHERE sequence=3")
    diagnostics, canary, source = _empty_external_stores(tmp_path)
    connections = []
    try:
        for path in (trial, diagnostics, canary, source):
            conn = sqlite3.connect(path)
            conn.row_factory = sqlite3.Row
            conn.execute("BEGIN IMMEDIATE")
            connections.append(conn)
        with pytest.raises(builder.TerminalBuildNotReady, match="challenger_outcomes_pending"):
            builder._build_dataset_locked(*connections, activated_at=ACTIVATED_AT)
    finally:
        for conn in reversed(connections):
            conn.rollback()
            conn.close()


def test_public_seal_status_and_single_decision_are_crash_idempotent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(inference, "TARGET_ENROLLED_TRADES", 2)
    monkeypatch.setattr(inference, "MINIMUM_DISTINCT_ENTRY_DATES", 2)
    trial = tmp_path / "trial.db"
    _install_trial(trial)
    diagnostics, canary, source = _empty_external_stores(tmp_path)
    config = builder.TerminalBuildConfig(
        trial_db=trial,
        diagnostics_db=diagnostics,
        canary_ledger_db=canary,
        source_db=source,
        registry_path=_active_registry_path(tmp_path),
        seal_db=tmp_path / "seals.db",
        artifact_root=tmp_path / "artifacts",
    )
    now = datetime(2026, 2, 10, 15, 0, tzinfo=UTC)

    first = builder.seal_terminal_dataset(config, now=now)
    replay = builder.seal_terminal_dataset(config, now=now + timedelta(minutes=1))
    sealed_status = builder.terminal_status(config)
    decision = builder.decide_terminal_dataset(config, now=now + timedelta(minutes=2))
    decision_replay = builder.decide_terminal_dataset(config, now=now + timedelta(minutes=3))

    assert first.status == replay.status == sealed_status.status == "sealed"
    assert first.terminal_dataset_sha256 == replay.terminal_dataset_sha256
    assert first.terminal_seal_receipt_sha256 == replay.terminal_seal_receipt_sha256
    assert decision.status == decision_replay.status == "decided"
    assert decision.decision_report_sha256 == decision_replay.decision_report_sha256
    assert builder.terminal_status(config).decision_report_sha256 == decision.decision_report_sha256
