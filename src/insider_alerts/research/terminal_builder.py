"""Blinded terminal-dataset construction and single-look orchestration for OPP-E07-V1."""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import sqlite3
import tempfile
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal

import rfc8785

from insider_alerts.research.diagnostics import (
    DiagnosticStore,
    _parse_utc,
    _selection_projection,
)
from insider_alerts.research.inference import (
    HYPOTHESIS_ID,
    TERMINAL_DATASET_SCHEMA_VERSION,
    TrialInvalid,
    TrialSealStore,
    _candidate_projection_sha256,
    _parse_candidate,
    cohort_freeze_boundary,
    evaluate_with_store,
)
from insider_alerts.research.trial_runtime import (
    TrialCandidate,
    TrialOutcomeInputs,
    TrialResolution,
    TrialRuntimeConfig,
    TrialRuntimeInvalid,
    TrialStore,
    _validated_trial_window,
)
from insider_alerts.research.trial_runtime import (
    _parse_utc as _parse_trial_utc,
)

TRIAL_SNAPSHOT_TABLES = (
    "trial_candidates",
    "trial_resolutions",
    "trial_entry_date_completions",
    "trial_entry_date_lapses",
    "trial_outcomes",
)
DIAGNOSTIC_SNAPSHOT_TABLES = (
    "diagnostic_candidates",
    "diagnostic_evidence_bindings",
    "diagnostic_state_bindings",
    "diagnostic_reconciliations",
    "diagnostic_outcomes",
    "diagnostic_outcome_receipts",
)


class TerminalBuildNotReady(RuntimeError):
    """The immutable terminal cohort is not complete yet."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class TerminalBuildInvalid(RuntimeError):
    """Terminal construction found a fail-closed contract violation."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True, slots=True)
class TerminalBuildConfig:
    trial_db: Path
    diagnostics_db: Path
    canary_ledger_db: Path
    source_db: Path
    registry_path: Path
    seal_db: Path
    artifact_root: Path


@dataclass(frozen=True, slots=True)
class TerminalBuildResult:
    status: Literal["idle_registry_draft", "collecting", "sealed", "decided", "invalid"]
    freeze_boundary_entry_date: str | None = None
    frozen_challenger_count: int = 0
    challenger_outcomes_waiting: int = 0
    control_membership_count: int = 0
    control_outcomes_waiting: int = 0
    routine_membership_count: int = 0
    terminal_dataset_sha256: str | None = None
    terminal_seal_receipt_sha256: str | None = None
    decision_report_sha256: str | None = None
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class _TrialSnapshot:
    candidates: tuple[TrialCandidate, ...]
    resolutions: tuple[TrialResolution, ...]
    completions: tuple[dict[str, Any], ...]
    outcomes: tuple[TrialOutcomeInputs, ...]


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None:
        raise TerminalBuildInvalid("terminal_timestamp_naive")
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _canonical(value: Mapping[str, Any]) -> bytes:
    return rfc8785.dumps(dict(value))


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _table_fingerprint(conn: sqlite3.Connection, tables: Sequence[str]) -> str:
    material: list[dict[str, Any]] = []
    for table in tables:
        rows = conn.execute(
            f"SELECT sequence,record_sha256 FROM {table} ORDER BY sequence"
        ).fetchall()
        material.append(
            {
                "table": table,
                "records": [[int(row["sequence"]), str(row["record_sha256"])] for row in rows],
            }
        )
    return _sha256(rfc8785.dumps(material))


def _fingerprint(path: Path, tables: Sequence[str]) -> str:
    uri = f"file:{path.resolve().as_posix()}?mode=ro"
    with contextlib.closing(sqlite3.connect(uri, uri=True, timeout=30)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("BEGIN")
        return _table_fingerprint(conn, tables)


@contextlib.contextmanager
def _locked_inputs(config: TerminalBuildConfig) -> Iterator[tuple[sqlite3.Connection, ...]]:
    """Hold fixed snapshots across all moving membership stores until the seal commits."""

    paths = (
        config.trial_db,
        config.diagnostics_db,
        config.canary_ledger_db,
        config.source_db,
    )
    connections: list[sqlite3.Connection] = []
    try:
        for path in paths:
            conn = sqlite3.connect(path, timeout=30)
            connections.append(conn)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute("BEGIN IMMEDIATE")
        yield tuple(connections)
    finally:
        for conn in reversed(connections):
            with contextlib.suppress(sqlite3.Error):
                conn.rollback()
            conn.close()


def _trial_snapshot(conn: sqlite3.Connection) -> _TrialSnapshot:
    candidates = tuple(
        TrialStore._verify_candidate_row(row)
        for row in conn.execute("SELECT * FROM trial_candidates ORDER BY sequence")
    )
    resolutions = tuple(
        TrialStore._verify_resolution_row(row)
        for row in conn.execute("SELECT * FROM trial_resolutions ORDER BY sequence")
    )
    completions = tuple(
        TrialStore._verify_completion_row(row)
        for row in conn.execute("SELECT * FROM trial_entry_date_completions ORDER BY sequence")
    )
    outcomes = tuple(
        TrialStore._verify_outcome_row(row)
        for row in conn.execute("SELECT * FROM trial_outcomes ORDER BY sequence")
    )
    return _TrialSnapshot(candidates, resolutions, completions, outcomes)


def _candidate_records(snapshot: _TrialSnapshot) -> list[dict[str, Any]]:
    resolutions = {item.candidate_id: item for item in snapshot.resolutions}
    output: list[dict[str, Any]] = []
    for candidate in snapshot.candidates:
        resolution = resolutions.get(candidate.candidate_id)
        output.append(
            {
                "candidate_id": candidate.candidate_id,
                "packet_id": candidate.packet_id,
                "accession_number": candidate.accession_number,
                "symbol": candidate.symbol,
                "evidence_record_sha256": candidate.evidence_record_sha256,
                "source_first_observed_at_utc": _utc_text(candidate.source_first_observed_at_utc),
                "entry_date": candidate.planned_entry_date.isoformat(),
                "entry_rank_sha256": candidate.entry_rank_sha256,
                "enrollment_state": (
                    resolution.enrollment_state if resolution else "pending_entry_selection"
                ),
                "confirmatory_enrollment_sequence": (
                    resolution.confirmatory_enrollment_sequence if resolution else None
                ),
            }
        )
    return sorted(
        output, key=lambda item: (item["source_first_observed_at_utc"], item["candidate_id"])
    )


def _completion_records(snapshot: _TrialSnapshot) -> list[dict[str, str]]:
    return [
        {
            "entry_date": str(record["entry_date"]),
            "completed_at_utc": str(record["completed_at_utc"]),
        }
        for record in sorted(snapshot.completions, key=lambda item: str(item["entry_date"]))
    ]


def _challenger_trade(outcome: TrialOutcomeInputs) -> dict[str, Any]:
    return {
        "trade_id": outcome.candidate_id,
        "confirmatory_enrollment_sequence": outcome.confirmatory_enrollment_sequence,
        "evidence_record_sha256": outcome.evidence_record_sha256,
        "entry_rank_sha256": outcome.entry_rank_sha256,
        "symbol": outcome.symbol,
        "entry_date": outcome.entry_date.isoformat(),
        "entry_at_utc": _utc_text(outcome.entry_at_utc),
        "exit_at_utc": _utc_text(outcome.exit_at_utc),
        "gross_return": outcome.gross_return,
        "spy_return": outcome.spy_return,
    }


def _diagnostic_trade(record: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "trade_id": str(record["trade_id"]),
        "confirmatory_enrollment_sequence": None,
        "evidence_record_sha256": None,
        "entry_rank_sha256": None,
        "symbol": str(record["symbol"]),
        "entry_date": str(record["entry_date"]),
        "entry_at_utc": str(record["entry_at_utc"]),
        "exit_at_utc": str(record["exit_at_utc"]),
        "gross_return": record["gross_return"],
        "spy_return": record["spy_return"],
    }


def _trade_order(record: Mapping[str, Any]) -> tuple[str, str, int, str]:
    sequence = record.get("confirmatory_enrollment_sequence")
    return (
        str(record["entry_date"]),
        str(record["entry_at_utc"]),
        int(sequence) if sequence is not None else 2**63,
        str(record["trade_id"]),
    )


def _unavailable_status(membership_count: int, code: str) -> dict[str, Any]:
    return {
        "status": "unavailable",
        "error_code": code,
        "membership_count": membership_count,
        "available_trade_count": 0,
        "not_traded_count": 0,
        "unavailable_count": membership_count,
    }


def _available_status(
    membership_count: int, available_count: int, not_traded_count: int
) -> dict[str, Any]:
    return {
        "status": "available",
        "error_code": None,
        "membership_count": membership_count,
        "available_trade_count": available_count,
        "not_traded_count": not_traded_count,
        "unavailable_count": 0,
    }


def _diagnostic_material(
    diagnostic_conn: sqlite3.Connection,
    canary_conn: sqlite3.Connection,
    source_conn: sqlite3.Connection,
    *,
    activated_at: datetime,
    freeze_boundary: date,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, dict[str, Any]], datetime | None]:
    stored_rows = diagnostic_conn.execute(
        "SELECT * FROM diagnostic_candidates WHERE entry_session IS NOT NULL "
        "AND entry_session<=? ORDER BY sequence",
        (freeze_boundary.isoformat(),),
    ).fetchall()
    stored = {str(row["packet_id"]): row for row in stored_rows}
    canary_rows = canary_conn.execute(
        "SELECT * FROM candidates WHERE signal_at>=? AND entry_session IS NOT NULL "
        "AND entry_session<=? ORDER BY signal_at,packet_id",
        (activated_at.astimezone(UTC).isoformat(), freeze_boundary.isoformat()),
    ).fetchall()
    expected: dict[str, sqlite3.Row] = {}
    in_scope: set[str] = set()
    membership_error: str | None = None

    def record_membership_error(code: str) -> None:
        nonlocal membership_error
        if membership_error is None:
            membership_error = code

    for row in canary_rows:
        packet_id = str(row["packet_id"])
        job = source_conn.execute(
            "SELECT source_first_observed_at_utc,decision_at_utc FROM research_capture_jobs "
            "WHERE packet_id=?",
            (packet_id,),
        ).fetchone()
        if job is None:
            in_scope.add(packet_id)
            record_membership_error("control_source_capture_job_missing")
            continue
        source_at = _parse_utc(str(job["source_first_observed_at_utc"]))
        signal_at = _parse_utc(str(row["signal_at"]))
        decision_at = _parse_utc(str(job["decision_at_utc"]))
        if source_at < activated_at:
            continue
        in_scope.add(packet_id)
        if source_at > signal_at or decision_at != signal_at:
            record_membership_error("control_source_timestamp_reconciliation_failed")
            continue
        expected[packet_id] = row
    if set(expected) != set(stored):
        record_membership_error("control_candidate_membership_mismatch")
    for packet_id in set(expected) & set(stored):
        projected = _selection_projection(expected[packet_id])
        if _sha256(rfc8785.dumps(projected)) != str(stored[packet_id]["canary_selection_sha256"]):
            record_membership_error("control_canary_selection_changed")
            break
    relevant_reconciliations = diagnostic_conn.execute(
        "SELECT 1 FROM diagnostic_reconciliations WHERE packet_id IS NULL OR packet_id IN ("
        "SELECT packet_id FROM diagnostic_candidates WHERE entry_session IS NOT NULL "
        "AND entry_session<=?) LIMIT 1",
        (freeze_boundary.isoformat(),),
    ).fetchone()
    if relevant_reconciliations is not None:
        record_membership_error("control_reconciliation_present")

    membership_count = len(stored)
    if membership_error is not None:
        membership_count = len(set(stored) | in_scope)
        status = _unavailable_status(membership_count, membership_error)
        routine_status = _unavailable_status(
            membership_count, f"routine_membership_unavailable:{membership_error}"
        )
        return [], [], {"control": status, "routine": routine_status}, None

    receipts = {
        str(row["packet_id"]): row
        for row in diagnostic_conn.execute(
            "SELECT * FROM diagnostic_outcome_receipts WHERE packet_id IN ("
            "SELECT packet_id FROM diagnostic_candidates WHERE entry_session IS NOT NULL "
            "AND entry_session<=?)",
            (freeze_boundary.isoformat(),),
        )
    }
    missing_receipts = set(stored) - set(receipts)
    if missing_receipts:
        timestamps = [_parse_utc(str(row["recorded_at_utc"])) for row in receipts.values()]
        control_status = _unavailable_status(
            membership_count, "control_terminal_receipts_incomplete"
        )
        routine_status = _unavailable_status(
            membership_count, "routine_terminal_receipts_incomplete"
        )
        return (
            [],
            [],
            {"control": control_status, "routine": routine_status},
            max(timestamps) if timestamps else None,
        )
    outcomes = {
        str(row["packet_id"]): json.loads(bytes(row["record_json"]))
        for row in diagnostic_conn.execute(
            "SELECT * FROM diagnostic_outcomes WHERE packet_id IN ("
            "SELECT packet_id FROM diagnostic_candidates WHERE entry_session IS NOT NULL "
            "AND entry_session<=?)",
            (freeze_boundary.isoformat(),),
        )
    }
    control_available = sum(str(row["disposition"]) == "available" for row in receipts.values())
    control_not_traded = sum(str(row["disposition"]) == "not_traded" for row in receipts.values())
    control_unavailable = sum(str(row["disposition"]) == "unavailable" for row in receipts.values())
    if control_unavailable:
        control_status = _unavailable_status(
            membership_count, "control_terminal_outcome_unavailable"
        )
        control_trades: list[dict[str, Any]] = []
    else:
        control_status = _available_status(membership_count, control_available, control_not_traded)
        control_trades = [
            _diagnostic_trade(outcomes[packet_id])
            for packet_id, receipt in receipts.items()
            if str(receipt["disposition"]) == "available"
        ]

    evidence = {
        str(row["packet_id"]): bool(row["routine_eligible"])
        for row in diagnostic_conn.execute(
            "SELECT packet_id,routine_eligible FROM diagnostic_evidence_bindings WHERE packet_id "
            "IN (SELECT packet_id FROM diagnostic_candidates WHERE entry_session IS NOT NULL "
            "AND entry_session<=?)",
            (freeze_boundary.isoformat(),),
        )
    }
    if set(evidence) != set(stored):
        routine_status = _unavailable_status(
            membership_count, "routine_membership_classification_incomplete"
        )
        routine_trades: list[dict[str, Any]] = []
    else:
        routine_packets = {packet_id for packet_id, eligible in evidence.items() if eligible}
        routine_available = sum(
            str(receipts[packet_id]["disposition"]) == "available" for packet_id in routine_packets
        )
        routine_not_traded = sum(
            str(receipts[packet_id]["disposition"]) == "not_traded" for packet_id in routine_packets
        )
        routine_unavailable = sum(
            str(receipts[packet_id]["disposition"]) == "unavailable"
            for packet_id in routine_packets
        )
        if routine_unavailable:
            routine_status = _unavailable_status(
                len(routine_packets), "routine_terminal_outcome_unavailable"
            )
            routine_trades = []
        else:
            routine_status = _available_status(
                len(routine_packets), routine_available, routine_not_traded
            )
            routine_trades = [
                _diagnostic_trade(outcomes[packet_id])
                for packet_id in routine_packets
                if str(receipts[packet_id]["disposition"]) == "available"
            ]

    timestamps = [_parse_utc(str(row["recorded_at_utc"])) for row in receipts.values()]
    timestamps.extend(_parse_utc(str(record["recorded_at_utc"])) for record in outcomes.values())
    return (
        sorted(control_trades, key=_trade_order),
        sorted(routine_trades, key=_trade_order),
        {"control": control_status, "routine": routine_status},
        max(timestamps) if timestamps else None,
    )


def _terminal_payload(
    snapshot: _TrialSnapshot,
    terminal_dataset: Mapping[str, Any] | None,
    *,
    activated_at: datetime,
    evaluated_at: datetime,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "hypothesis_id": HYPOTHESIS_ID,
        "activated_at_utc": _utc_text(activated_at),
        "evaluated_at_utc": _utc_text(evaluated_at),
        "entry_date_completions": _completion_records(snapshot),
        "candidates": _candidate_records(snapshot),
        "integrity_checks": {
            "timestamp_ordering": True,
            "snapshot_hashes": True,
            "sec_archive_coverage": True,
            "classification_provenance": True,
            "enrollment_reconciliation": True,
            "outcome_blinding": True,
            "outcome_completeness": True if terminal_dataset is not None else None,
            "shadow_book_reconciliation": True if terminal_dataset is not None else None,
        },
        "terminal_dataset": dict(terminal_dataset) if terminal_dataset is not None else None,
    }


def _build_dataset_locked(
    trial_conn: sqlite3.Connection,
    diagnostic_conn: sqlite3.Connection,
    canary_conn: sqlite3.Connection,
    source_conn: sqlite3.Connection,
    *,
    activated_at: datetime,
) -> tuple[_TrialSnapshot, dict[str, Any], dict[str, int]]:
    snapshot = _trial_snapshot(trial_conn)
    completion_map = {
        date.fromisoformat(str(record["entry_date"])): _parse_trial_utc(
            str(record["completed_at_utc"])
        )
        for record in snapshot.completions
    }
    enrolled = [
        resolution
        for resolution in snapshot.resolutions
        if resolution.enrollment_state == "enrolled"
    ]
    freeze = cohort_freeze_boundary([item.entry_date for item in enrolled], completion_map)
    if freeze is None:
        raise TerminalBuildNotReady("cohort_not_frozen")
    freeze_boundary, freeze_completed_at = freeze
    frozen = [item for item in enrolled if item.entry_date <= freeze_boundary]
    frozen_ids = {item.candidate_id for item in frozen}
    outcomes = {item.candidate_id: item for item in snapshot.outcomes}
    waiting = frozen_ids - set(outcomes)
    unexpected = set(outcomes) - frozen_ids
    if unexpected:
        raise TerminalBuildInvalid("challenger_outcome_outside_frozen_cohort")
    if waiting:
        raise TerminalBuildNotReady("challenger_outcomes_pending")
    challenger_trades = sorted(
        (_challenger_trade(outcomes[item.candidate_id]) for item in frozen), key=_trade_order
    )
    control, routine, diagnostic_status, diagnostic_recorded_at = _diagnostic_material(
        diagnostic_conn,
        canary_conn,
        source_conn,
        activated_at=activated_at,
        freeze_boundary=freeze_boundary,
    )
    candidate_records = _candidate_records(snapshot)
    parsed_candidates = [
        _parse_candidate(record, index) for index, record in enumerate(candidate_records)
    ]
    timestamps = [freeze_completed_at]
    timestamps.extend(item.recorded_at_utc for item in outcomes.values())
    if diagnostic_recorded_at is not None:
        timestamps.append(diagnostic_recorded_at)
    sealed_at = max(timestamps)
    terminal: dict[str, Any] = {
        "schema_version": TERMINAL_DATASET_SCHEMA_VERSION,
        "hypothesis_id": HYPOTHESIS_ID,
        "freeze_boundary_entry_date": freeze_boundary.isoformat(),
        "sealed_at_utc": _utc_text(sealed_at),
        "candidate_projection_sha256": _candidate_projection_sha256(parsed_candidates),
        "challenger_trades": challenger_trades,
        "control_trades": control,
        "routine_trades": routine,
        "diagnostic_group_status": diagnostic_status,
        "dataset_sha256": "",
    }
    unsigned = dict(terminal)
    unsigned.pop("dataset_sha256")
    terminal["dataset_sha256"] = _sha256(rfc8785.dumps(unsigned))
    counts = {
        "frozen": len(frozen),
        "control": int(diagnostic_status["control"]["membership_count"]),
        "routine": int(diagnostic_status["routine"]["membership_count"]),
    }
    return snapshot, terminal, counts


def _publish_dataset(root: Path, terminal: Mapping[str, Any]) -> Path:
    digest = str(terminal["dataset_sha256"])
    encoded = _canonical(terminal)
    unsigned = dict(terminal)
    unsigned.pop("dataset_sha256", None)
    if _sha256(rfc8785.dumps(unsigned)) != digest:
        raise TerminalBuildInvalid("terminal_artifact_digest_mismatch")
    directory = root / "terminal-datasets"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{digest}.json"
    descriptor, temporary_name = tempfile.mkstemp(
        dir=directory, prefix=f".{digest}.", suffix=".staging"
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary_path, path)
        except FileExistsError:
            if path.read_bytes() != encoded:
                raise TerminalBuildInvalid("terminal_artifact_path_collision") from None
        if os.name != "nt":
            directory_descriptor = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
    finally:
        temporary_path.unlink(missing_ok=True)
    return path


def seal_terminal_dataset(
    config: TerminalBuildConfig, *, now: datetime | None = None
) -> TerminalBuildResult:
    """Build and seal one complete terminal dataset without calculating any aggregate."""

    if now is not None and now.tzinfo is None:
        raise TerminalBuildInvalid("terminal_builder_clock_naive")
    requested_at = now.astimezone(UTC) if now is not None else None
    trial_config = TrialRuntimeConfig(
        trial_db=config.trial_db,
        evidence_db=config.trial_db.with_name("evidence.db"),
        bar_feed_db=config.trial_db.with_name("bar_feed.db"),
        session_feed_db=config.trial_db.with_name("session_feed.db"),
        registry_path=config.registry_path,
    )
    window = _validated_trial_window(trial_config)
    if window.status == "draft":
        return TerminalBuildResult("idle_registry_draft")
    if window.activated_at_utc is None:
        raise TerminalBuildInvalid("active_window_missing_activation")
    registry = json.loads(config.registry_path.read_text(encoding="utf-8"))
    if not isinstance(registry, dict):
        raise TerminalBuildInvalid("registry_not_object")
    seal_store = TrialSealStore(config.seal_db)
    trial_store = TrialStore(config.trial_db, initialize=False)
    diagnostic_store = DiagnosticStore(config.diagnostics_db, initialize=False)
    before_trial = _fingerprint(config.trial_db, TRIAL_SNAPSHOT_TABLES)
    before_diagnostic = _fingerprint(config.diagnostics_db, DIAGNOSTIC_SNAPSHOT_TABLES)
    trial_store.validate_integrity()
    diagnostic_store.validate_integrity()
    expected_trial = _fingerprint(config.trial_db, TRIAL_SNAPSHOT_TABLES)
    expected_diagnostic = _fingerprint(config.diagnostics_db, DIAGNOSTIC_SNAPSHOT_TABLES)
    if before_trial != expected_trial or before_diagnostic != expected_diagnostic:
        raise TerminalBuildNotReady("terminal_inputs_changed_during_validation")
    with _locked_inputs(config) as connections:
        trial_conn, diagnostic_conn, canary_conn, source_conn = connections
        if (
            _table_fingerprint(trial_conn, TRIAL_SNAPSHOT_TABLES) != expected_trial
            or _table_fingerprint(diagnostic_conn, DIAGNOSTIC_SNAPSHOT_TABLES)
            != expected_diagnostic
        ):
            raise TerminalBuildNotReady("terminal_inputs_changed_during_validation")
        pending = seal_store.pending_terminal()
        if pending is None:
            snapshot, terminal, counts = _build_dataset_locked(
                trial_conn,
                diagnostic_conn,
                canary_conn,
                source_conn,
                activated_at=window.activated_at_utc,
            )
        else:
            snapshot = _trial_snapshot(trial_conn)
            terminal = pending
            diagnostic_status = terminal["diagnostic_group_status"]
            counts = {
                "frozen": len(terminal["challenger_trades"]),
                "control": int(diagnostic_status["control"]["membership_count"]),
                "routine": int(diagnostic_status["routine"]["membership_count"]),
            }
        sealed_at = requested_at or datetime.now(UTC)
        payload = _terminal_payload(
            snapshot,
            terminal,
            activated_at=window.activated_at_utc,
            evaluated_at=sealed_at,
        )
        terminal = seal_store.stage_terminal(registry, payload)
        _publish_dataset(config.artifact_root, terminal)
        receipt = seal_store.seal_terminal(registry, payload, recorded_at=sealed_at)
    return TerminalBuildResult(
        "sealed",
        freeze_boundary_entry_date=str(terminal["freeze_boundary_entry_date"]),
        frozen_challenger_count=counts["frozen"],
        control_membership_count=counts["control"],
        routine_membership_count=counts["routine"],
        terminal_dataset_sha256=str(terminal["dataset_sha256"]),
        terminal_seal_receipt_sha256=str(receipt["receipt_sha256"]),
    )


def terminal_status(config: TerminalBuildConfig) -> TerminalBuildResult:
    """Return readiness and receipt counts without materializing or aggregating returns."""

    trial_config = TrialRuntimeConfig(
        trial_db=config.trial_db,
        evidence_db=config.trial_db.with_name("evidence.db"),
        bar_feed_db=config.trial_db.with_name("bar_feed.db"),
        session_feed_db=config.trial_db.with_name("session_feed.db"),
        registry_path=config.registry_path,
    )
    window = _validated_trial_window(trial_config)
    if window.status == "draft":
        return TerminalBuildResult("idle_registry_draft")
    seal_store = TrialSealStore(config.seal_db)
    report = seal_store.existing_report()
    if report is not None:
        return TerminalBuildResult(
            "decided",
            freeze_boundary_entry_date=report.get("freeze_boundary_entry_date"),
            terminal_dataset_sha256=report.get("terminal_dataset_sha256"),
            terminal_seal_receipt_sha256=report.get("terminal_seal_receipt_sha256"),
            decision_report_sha256=report.get("report_sha256"),
            reason=str(report.get("state")),
        )
    receipt = seal_store.receipt("terminal_seal")
    if receipt is not None:
        digest = str(receipt["terminal_dataset_sha256"])
        artifact = config.artifact_root / "terminal-datasets" / f"{digest}.json"
        return TerminalBuildResult(
            "sealed" if artifact.is_file() else "invalid",
            terminal_dataset_sha256=digest,
            terminal_seal_receipt_sha256=str(receipt["receipt_sha256"]),
            reason="awaiting_single_look" if artifact.is_file() else "sealed_artifact_missing",
        )
    trial_store = TrialStore(config.trial_db, initialize=False)
    diagnostic_store = DiagnosticStore(config.diagnostics_db, initialize=False)
    trial_store.validate_integrity()
    diagnostic_store.validate_integrity()
    freeze = trial_store.cohort_freeze()
    if freeze is None:
        return TerminalBuildResult("collecting", reason="cohort_not_frozen")
    boundary = freeze[0]
    frozen_ids = {
        item.candidate_id
        for item in trial_store.resolutions()
        if item.enrollment_state == "enrolled" and item.entry_date <= boundary
    }
    challenger_waiting = len(frozen_ids - trial_store.outcome_candidate_ids())
    with contextlib.closing(diagnostic_store._connect()) as conn:
        control_count = int(
            conn.execute(
                "SELECT COUNT(*) FROM diagnostic_candidates WHERE entry_session IS NOT NULL "
                "AND entry_session<=?",
                (boundary.isoformat(),),
            ).fetchone()[0]
        )
        control_receipts = int(
            conn.execute(
                "SELECT COUNT(*) FROM diagnostic_outcome_receipts WHERE packet_id IN ("
                "SELECT packet_id FROM diagnostic_candidates WHERE entry_session IS NOT NULL "
                "AND entry_session<=?)",
                (boundary.isoformat(),),
            ).fetchone()[0]
        )
        routine_count = int(
            conn.execute(
                "SELECT COUNT(*) FROM diagnostic_evidence_bindings WHERE routine_eligible=1 "
                "AND packet_id IN (SELECT packet_id FROM diagnostic_candidates WHERE "
                "entry_session IS NOT NULL AND entry_session<=?)",
                (boundary.isoformat(),),
            ).fetchone()[0]
        )
    return TerminalBuildResult(
        "collecting",
        freeze_boundary_entry_date=boundary.isoformat(),
        frozen_challenger_count=len(frozen_ids),
        challenger_outcomes_waiting=challenger_waiting,
        control_membership_count=control_count,
        control_outcomes_waiting=max(0, control_count - control_receipts),
        routine_membership_count=routine_count,
        reason=(
            "challenger_outcomes_pending"
            if challenger_waiting
            else "ready_to_seal_diagnostics_assessed_nonblocking_at_seal"
        ),
    )


def decide_terminal_dataset(
    config: TerminalBuildConfig, *, now: datetime | None = None
) -> TerminalBuildResult:
    """Perform the separately invoked, append-only single terminal look."""

    if now is not None and now.tzinfo is None:
        raise TerminalBuildInvalid("terminal_decision_clock_naive")
    evaluated_at = (now or datetime.now(UTC)).astimezone(UTC)
    store = TrialSealStore(config.seal_db)
    existing = store.existing_report()
    if existing is not None:
        return TerminalBuildResult(
            "decided",
            freeze_boundary_entry_date=existing.get("freeze_boundary_entry_date"),
            terminal_dataset_sha256=existing.get("terminal_dataset_sha256"),
            terminal_seal_receipt_sha256=existing.get("terminal_seal_receipt_sha256"),
            decision_report_sha256=existing.get("report_sha256"),
            reason=str(existing.get("state")),
        )
    receipt = store.receipt("terminal_seal")
    if receipt is None:
        raise TerminalBuildNotReady("terminal_seal_receipt_missing")
    dataset_digest = str(receipt["terminal_dataset_sha256"])
    artifact = config.artifact_root / "terminal-datasets" / f"{dataset_digest}.json"
    if not artifact.is_file():
        raise TerminalBuildInvalid("sealed_terminal_artifact_missing")
    raw = artifact.read_bytes()
    terminal = json.loads(raw)
    if not isinstance(terminal, dict) or _canonical(terminal) != raw:
        raise TerminalBuildInvalid("sealed_terminal_artifact_not_canonical")
    unsigned = dict(terminal)
    unsigned.pop("dataset_sha256", None)
    if (
        terminal.get("dataset_sha256") != dataset_digest
        or _sha256(rfc8785.dumps(unsigned)) != dataset_digest
    ):
        raise TerminalBuildInvalid("sealed_terminal_artifact_digest_mismatch")
    trial_config = TrialRuntimeConfig(
        trial_db=config.trial_db,
        evidence_db=config.trial_db.with_name("evidence.db"),
        bar_feed_db=config.trial_db.with_name("bar_feed.db"),
        session_feed_db=config.trial_db.with_name("session_feed.db"),
        registry_path=config.registry_path,
    )
    window = _validated_trial_window(trial_config)
    if window.status != "active" or window.activated_at_utc is None:
        raise TerminalBuildInvalid("decision_registry_not_active")
    registry = json.loads(config.registry_path.read_text(encoding="utf-8"))
    if not isinstance(registry, dict):
        raise TerminalBuildInvalid("registry_not_object")
    trial_store = TrialStore(config.trial_db, initialize=False)
    trial_store.validate_integrity()
    with contextlib.closing(trial_store._connect()) as conn:
        conn.execute("BEGIN IMMEDIATE")
        snapshot = _trial_snapshot(conn)
        payload = _terminal_payload(
            snapshot,
            terminal,
            activated_at=window.activated_at_utc,
            evaluated_at=evaluated_at,
        )
        report = evaluate_with_store(registry, payload, store)
    return TerminalBuildResult(
        "decided",
        freeze_boundary_entry_date=report.get("freeze_boundary_entry_date"),
        frozen_challenger_count=int(report.get("counts", {}).get("enrolled", 0)),
        terminal_dataset_sha256=report.get("terminal_dataset_sha256"),
        terminal_seal_receipt_sha256=report.get("terminal_seal_receipt_sha256"),
        decision_report_sha256=report.get("report_sha256"),
        reason=str(report.get("state")),
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Seal or decide the OPP-E07 terminal dataset")
    parser.add_argument("action", choices=("status", "seal", "decide"))
    parser.add_argument("--trial-db", type=Path, default=Path("data/research/trial.db"))
    parser.add_argument("--diagnostics-db", type=Path, default=Path("data/research/diagnostics.db"))
    parser.add_argument("--canary-ledger-db", type=Path, default=Path("data/live_canary.db"))
    parser.add_argument("--source-db", type=Path, default=Path("data/insider_alerts.db"))
    parser.add_argument(
        "--registry-path",
        type=Path,
        default=Path("docs/research/registry/OPP-E07-V1.json"),
    )
    parser.add_argument("--seal-db", type=Path, default=Path("data/research/trial_seals.db"))
    parser.add_argument("--artifact-root", type=Path, default=Path("data/research/artifacts"))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    config = TerminalBuildConfig(
        trial_db=args.trial_db,
        diagnostics_db=args.diagnostics_db,
        canary_ledger_db=args.canary_ledger_db,
        source_db=args.source_db,
        registry_path=args.registry_path,
        seal_db=args.seal_db,
        artifact_root=args.artifact_root,
    )
    try:
        if args.action == "status":
            result = terminal_status(config)
        elif args.action == "seal":
            result = seal_terminal_dataset(config)
        else:
            result = decide_terminal_dataset(config)
    except TerminalBuildNotReady as exc:
        result = TerminalBuildResult("collecting", reason=exc.code)
    except (
        OSError,
        sqlite3.DatabaseError,
        TerminalBuildInvalid,
        TrialInvalid,
        TrialRuntimeInvalid,
        ValueError,
    ) as exc:
        result = TerminalBuildResult("invalid", reason=str(exc))
    print(rfc8785.dumps(asdict(result)).decode("utf-8"))
    if result.status == "invalid":
        return 3
    if result.status == "decided" and result.reason == "KILL":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
