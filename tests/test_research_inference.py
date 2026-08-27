from __future__ import annotations

import ast
import hashlib
import json
import subprocess
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any

import pytest
import rfc8785

import insider_alerts.research.inference as inference

ROOT = Path(__file__).resolve().parents[1]
REGISTRY_PATH = ROOT / "docs/research/registry/OPP-E07-V1.json"
ACTIVATED_AT = datetime(2026, 1, 31, 15, 0, tzinfo=UTC)


def _registry() -> dict[str, Any]:
    return json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))


def _active_registry() -> dict[str, Any]:
    registry = _registry()
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
        "activated_at_utc": _utc_text(ACTIVATED_AT),
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
        "dependency_lock_sha256": file_sha(ROOT / "uv.lock"),
        "policy_sha256": file_sha(ROOT / registry["strategy"]["policy_artifact"]),
        "classifier_version": inference.CLASSIFIER_VERSION,
        "enrollment_start_sequence": 1,
    }
    return registry


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _candidate_rows(counts_by_date: list[int]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    observed = ACTIVATED_AT + timedelta(seconds=1)
    for day_index, count in enumerate(counts_by_date):
        entry_day = date(2026, 2, 1) + timedelta(days=day_index * 2)
        for within_day in range(count):
            candidate_id = f"candidate-{day_index:03d}-{within_day:02d}"
            packet_id = f"packet-{day_index:03d}-{within_day:02d}"
            accession = f"0000000001-26-{day_index * 10 + within_day:06d}"
            symbol = f"SYM{len(candidates) % 6}"
            rank = hashlib.sha256(
                (
                    f"{inference.CAPACITY_RANK_SALT}|{entry_day.isoformat()}|{packet_id}|"
                    f"{accession}|{symbol}"
                ).encode()
            ).hexdigest()
            candidates.append(
                {
                    "candidate_id": candidate_id,
                    "packet_id": packet_id,
                    "accession_number": accession,
                    "symbol": symbol,
                    "evidence_record_sha256": hashlib.sha256(
                        f"evidence:{candidate_id}".encode()
                    ).hexdigest(),
                    "source_first_observed_at_utc": _utc_text(observed),
                    "entry_date": entry_day.isoformat(),
                    "entry_rank_sha256": rank,
                    "enrollment_state": "enrolled",
                    "confirmatory_enrollment_sequence": None,
                }
            )
            observed += timedelta(seconds=1)
    enrollment_order = sorted(
        candidates,
        key=lambda item: (
            item["entry_date"],
            item["entry_rank_sha256"],
            item["candidate_id"],
        ),
    )
    for sequence, candidate in enumerate(enrollment_order, start=1):
        candidate["confirmatory_enrollment_sequence"] = sequence
    return sorted(
        candidates,
        key=lambda item: (item["source_first_observed_at_utc"], item["candidate_id"]),
    )


def _pending_candidate() -> dict[str, Any]:
    entry_day = date(2026, 6, 1)
    packet_id = "pending-packet"
    accession = "0000000001-26-999999"
    symbol = "PEND"
    rank = hashlib.sha256(
        (
            f"{inference.CAPACITY_RANK_SALT}|{entry_day.isoformat()}|{packet_id}|"
            f"{accession}|{symbol}"
        ).encode()
    ).hexdigest()
    return {
        "candidate_id": "pending",
        "packet_id": packet_id,
        "accession_number": accession,
        "symbol": symbol,
        "evidence_record_sha256": hashlib.sha256(b"pending-evidence").hexdigest(),
        "source_first_observed_at_utc": _utc_text(ACTIVATED_AT + timedelta(minutes=1)),
        "entry_date": entry_day.isoformat(),
        "entry_rank_sha256": rank,
        "enrollment_state": "pending_entry_selection",
        "confirmatory_enrollment_sequence": None,
    }


def _integrity(*, terminal: bool, false_check: str | None = None) -> dict[str, Any]:
    result = {
        "timestamp_ordering": True,
        "snapshot_hashes": True,
        "sec_archive_coverage": True,
        "classification_provenance": True,
        "enrollment_reconciliation": True,
        "outcome_blinding": True,
        "outcome_completeness": True if terminal else None,
        "shadow_book_reconciliation": True if terminal else None,
    }
    if false_check:
        result[false_check] = False
    return result


def _trade_rows(
    candidates: list[dict[str, Any]], *, gross_return: float, spy_return: float = 0.0
) -> list[dict[str, Any]]:
    trades: list[dict[str, Any]] = []
    for candidate in candidates:
        if candidate["enrollment_state"] != "enrolled":
            continue
        entry_day = date.fromisoformat(candidate["entry_date"])
        entry_at = datetime.combine(entry_day, time(14, 30), tzinfo=UTC)
        trades.append(
            {
                "trade_id": candidate["candidate_id"],
                "confirmatory_enrollment_sequence": candidate["confirmatory_enrollment_sequence"],
                "evidence_record_sha256": candidate["evidence_record_sha256"],
                "entry_rank_sha256": candidate["entry_rank_sha256"],
                "symbol": candidate["symbol"],
                "entry_date": candidate["entry_date"],
                "entry_at_utc": _utc_text(entry_at),
                "exit_at_utc": _utc_text(entry_at + timedelta(days=10, hours=6)),
                "gross_return": gross_return,
                "spy_return": spy_return,
            }
        )
    return sorted(
        trades,
        key=lambda item: (
            item["entry_date"],
            item["entry_at_utc"],
            item["confirmatory_enrollment_sequence"],
            item["trade_id"],
        ),
    )


def _terminal_dataset(candidates: list[dict[str, Any]], *, gross_return: float) -> dict[str, Any]:
    trades = _trade_rows(candidates, gross_return=gross_return)
    freeze_boundary = max(item["entry_date"] for item in candidates if item["entry_date"])
    latest_exit = max(
        datetime.fromisoformat(item["exit_at_utc"].replace("Z", "+00:00")) for item in trades
    )
    terminal: dict[str, Any] = {
        "schema_version": 1,
        "hypothesis_id": "OPP-E07-V1",
        "freeze_boundary_entry_date": freeze_boundary,
        "sealed_at_utc": _utc_text(latest_exit + timedelta(seconds=1)),
        "candidate_projection_sha256": inference._candidate_projection_sha256(
            [
                inference._parse_candidate(candidate, index)
                for index, candidate in enumerate(candidates)
            ]
        ),
        "challenger_trades": trades,
        "control_trades": [],
        "routine_trades": [],
        "dataset_sha256": "",
    }
    unsigned = dict(terminal)
    unsigned.pop("dataset_sha256")
    terminal["dataset_sha256"] = hashlib.sha256(rfc8785.dumps(unsigned)).hexdigest()
    return terminal


def _payload(
    candidates: list[dict[str, Any]],
    *,
    evaluated_at: datetime,
    complete_through: date | None,
    terminal: dict[str, Any] | None = None,
    false_check: str | None = None,
) -> dict[str, Any]:
    completion_rows: list[dict[str, str]] = []
    if complete_through is not None:
        entry_dates = sorted(
            {
                date.fromisoformat(candidate["entry_date"])
                for candidate in candidates
                if candidate["entry_date"] is not None
                and date.fromisoformat(candidate["entry_date"]) <= complete_through
            }
        )
        completion_rows = [
            {
                "entry_date": entry_day.isoformat(),
                "completed_at_utc": _utc_text(datetime.combine(entry_day, time(22, 0), tzinfo=UTC)),
            }
            for entry_day in entry_dates
        ]
    return {
        "schema_version": 1,
        "hypothesis_id": "OPP-E07-V1",
        "activated_at_utc": _utc_text(ACTIVATED_AT),
        "evaluated_at_utc": _utc_text(evaluated_at),
        "entry_date_completions": completion_rows,
        "candidates": candidates,
        "integrity_checks": _integrity(terminal=terminal is not None, false_check=false_check),
        "terminal_dataset": terminal,
    }


def _evaluate(payload: dict[str, Any]) -> dict[str, Any]:
    evaluated_at = datetime.fromisoformat(payload["evaluated_at_utc"].replace("Z", "+00:00"))
    activated_at = datetime.fromisoformat(payload["activated_at_utc"].replace("Z", "+00:00"))
    deadline = inference.enrollment_deadline(activated_at)
    parsed_candidates = [
        inference._parse_candidate(candidate, index)
        for index, candidate in enumerate(payload["candidates"])
    ]
    completions = inference._entry_date_completions(payload["entry_date_completions"], evaluated_at)
    freeze = inference._freeze_boundary(parsed_candidates, completions)
    deadline_receipt = None
    if evaluated_at >= deadline and (freeze is None or freeze[1] >= deadline):
        deadline_receipt = inference._build_receipt(
            kind="deadline_miss",
            recorded_at=evaluated_at,
            deadline=deadline,
            terminal_dataset_sha256=None,
            candidate_projection_sha256=inference._candidate_projection_sha256(parsed_candidates),
            candidate_universe_sha256=inference._candidate_universe_sha256(parsed_candidates),
        )
    terminal_receipt = None
    if payload["terminal_dataset"] is not None:
        terminal_receipt = inference._build_receipt(
            kind="terminal_seal",
            recorded_at=evaluated_at,
            deadline=deadline,
            terminal_dataset_sha256=payload["terminal_dataset"]["dataset_sha256"],
            candidate_projection_sha256=inference._candidate_projection_sha256(parsed_candidates),
            candidate_universe_sha256=inference._candidate_universe_sha256(parsed_candidates),
        )
    return inference.evaluate_trial(
        _registry(),
        payload,
        allow_draft=True,
        terminal_receipt=terminal_receipt,
        deadline_miss_receipt=deadline_receipt,
    )


def test_sha256_counter_rng_has_frozen_vector() -> None:
    rng = inference.Sha256CounterRng(inference.BOOTSTRAP_SEED, inference.PRNG_DOMAIN)

    assert [rng.bounded(60) for _ in range(8)] == [14, 25, 43, 9, 22, 5, 8, 20]


def test_rng_rejects_biased_tail_and_circular_blocks_wrap_and_truncate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    limit = 2**64 - (2**64 % 10)
    values = iter((limit, 7))

    class FakeDigest:
        def __init__(self, value: int) -> None:
            self.value = value

        def digest(self) -> bytes:
            return self.value.to_bytes(8, "big") + bytes(24)

    monkeypatch.setattr(inference.hashlib, "sha256", lambda _payload: FakeDigest(next(values)))
    rng = inference.Sha256CounterRng(inference.BOOTSTRAP_SEED, b"fixture")
    assert rng.bounded(10) == 7

    class Starts:
        def __init__(self) -> None:
            self.values = iter((10, 5))

        def bounded(self, _upper: int) -> int:
            return next(self.values)

    assert inference._sampled_cluster_indices(Starts(), 12) == [
        10,
        11,
        0,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        5,
        6,
    ]
    assert inference._percentile_type7([0.0, 10.0], 0.25) == 2.5


def test_enrollment_deadline_freezes_month_end_and_dst_rules() -> None:
    month_end_activation = datetime(2026, 8, 31, 16, 0, tzinfo=UTC)
    assert inference.enrollment_deadline(month_end_activation) == datetime(
        2028, 2, 29, 17, 0, tzinfo=UTC
    )

    gap_activation = datetime(2025, 9, 14, 6, 30, tzinfo=UTC)
    assert inference.enrollment_deadline(gap_activation) == datetime(2027, 3, 14, 7, 0, tzinfo=UTC)

    fold_activation = datetime(2026, 5, 7, 5, 30, tzinfo=UTC)
    assert inference.enrollment_deadline(fold_activation) == datetime(
        2027, 11, 7, 6, 30, tzinfo=UTC
    )


def test_boundary_date_includes_every_trade_after_both_thresholds() -> None:
    candidates = _candidate_rows([2] * 40 + [1] * 19 + [5])
    boundary = date.fromisoformat(candidates[-1]["entry_date"])
    report = _evaluate(
        _payload(
            candidates,
            evaluated_at=ACTIVATED_AT + timedelta(days=150),
            complete_through=boundary,
        )
    )

    assert report["state"] == "COLLECTING"
    assert report["freeze_boundary_entry_date"] == boundary.isoformat()
    assert report["reason_codes"] == ["awaiting_frozen_outcomes_and_terminal_seal"]
    assert report["inference"] is None


def test_positive_fixture_deterministically_recommends_promotion() -> None:
    candidates = _candidate_rows([2] * 60)
    terminal = _terminal_dataset(candidates, gross_return=0.03)
    evaluated = datetime.fromisoformat(terminal["sealed_at_utc"].replace("Z", "+00:00"))

    report = _evaluate(
        _payload(
            candidates,
            evaluated_at=evaluated,
            complete_through=date.fromisoformat(terminal["freeze_boundary_entry_date"]),
            terminal=terminal,
        )
    )

    assert report["state"] == "PROMOTE_RECOMMENDED"
    assert report["reason_codes"] == ["primary_and_all_cogates_passed"]
    assert report["inference"]["p_value"] == 1 / 10_001
    assert report["inference"]["exceedances"] == 0
    assert report["inference"]["confidence_interval_95"] == [0.027999999999999994] * 2
    assert all(report["economic_gates"].values())
    assert report["falsification_context"]["control"]["status"] == "available"
    assert report["falsification_context"]["control"]["trade_count"] == 0
    unsigned = dict(report)
    digest = unsigned.pop("report_sha256")
    assert hashlib.sha256(rfc8785.dumps(unsigned)).hexdigest() == digest


def test_null_fixture_kills_at_the_only_terminal_look() -> None:
    candidates = _candidate_rows([2] * 60)
    terminal = _terminal_dataset(candidates, gross_return=0.002)
    evaluated = datetime.fromisoformat(terminal["sealed_at_utc"].replace("Z", "+00:00"))

    report = _evaluate(
        _payload(
            candidates,
            evaluated_at=evaluated,
            complete_through=date.fromisoformat(terminal["freeze_boundary_entry_date"]),
            terminal=terminal,
        )
    )

    assert report["state"] == "KILL"
    assert report["inference"]["p_value"] == 1.0
    assert "primary_p_value_above_0.025" in report["reason_codes"]
    assert report["economic_gates"]["positive_mean_alpha_at_50bps"] is False


def test_clustered_bootstrap_is_deterministic_and_trade_weighted() -> None:
    candidates = _candidate_rows([1, 3] * 30)
    terminal = _terminal_dataset(candidates, gross_return=0.02)
    for index, trade in enumerate(terminal["challenger_trades"]):
        trade["gross_return"] = 0.04 if index % 5 else -0.02
    unsigned = dict(terminal)
    unsigned.pop("dataset_sha256")
    terminal["dataset_sha256"] = hashlib.sha256(rfc8785.dumps(unsigned)).hexdigest()
    evaluated = datetime.fromisoformat(terminal["sealed_at_utc"].replace("Z", "+00:00"))

    first = _evaluate(
        _payload(
            candidates,
            evaluated_at=evaluated,
            complete_through=date.fromisoformat(terminal["freeze_boundary_entry_date"]),
            terminal=terminal,
        )
    )
    second = _evaluate(
        _payload(
            candidates,
            evaluated_at=evaluated,
            complete_through=date.fromisoformat(terminal["freeze_boundary_entry_date"]),
            terminal=terminal,
        )
    )

    assert first == second
    assert first["inference"]["trade_count"] == 120
    assert first["inference"]["distinct_entry_dates"] == 60
    assert first["inference"]["mean"] == 0.026000000000000002
    assert first["inference"]["exceedances"] == 0


def test_deadline_waits_for_pending_then_kills_without_outcome_access() -> None:
    candidates = _candidate_rows([1] * 10)
    deadline = inference.enrollment_deadline(ACTIVATED_AT)
    pending = sorted(
        candidates + [_pending_candidate()],
        key=lambda item: item["source_first_observed_at_utc"],
    )

    draining = _evaluate(
        _payload(
            pending,
            evaluated_at=deadline,
            complete_through=None,
        )
    )
    killed = _evaluate(
        _payload(
            candidates,
            evaluated_at=deadline,
            complete_through=date.fromisoformat(candidates[-1]["entry_date"]),
        )
    )

    assert draining["state"] == "COLLECTING"
    assert draining["reason_codes"] == ["draining_predeadline_pending_entry_selection"]
    assert killed["state"] == "KILL"
    assert killed["reason_codes"] == ["insufficient_enrollment"]
    assert killed["inference"] is None
    assert killed["economic_metrics"] is None


def test_append_only_deadline_receipt_prevents_postdeadline_rescue(tmp_path: Path) -> None:
    candidates = _candidate_rows([2] * 39 + [1] * 21)
    pending = _pending_candidate()
    rows = sorted(candidates + [pending], key=lambda item: item["source_first_observed_at_utc"])
    deadline = inference.enrollment_deadline(ACTIVATED_AT)
    store = inference.TrialSealStore(tmp_path / "seals.db")

    draining = inference.evaluate_with_store(
        _registry(),
        _payload(rows, evaluated_at=deadline, complete_through=None),
        store,
        allow_draft=True,
    )

    omitted = inference.evaluate_with_store(
        _registry(),
        _payload(
            rows[:-1],
            evaluated_at=deadline + timedelta(hours=1),
            complete_through=None,
        ),
        store,
        allow_draft=True,
    )
    assert store.existing_report() is None

    pending["enrollment_state"] = "enrolled"
    resolved = sorted(candidates + [pending], key=lambda item: item["source_first_observed_at_utc"])
    for sequence, candidate in enumerate(
        sorted(
            resolved,
            key=lambda item: (item["entry_date"], item["entry_rank_sha256"], item["candidate_id"]),
        ),
        start=1,
    ):
        candidate["confirmatory_enrollment_sequence"] = sequence
    killed = inference.evaluate_with_store(
        _registry(),
        _payload(
            resolved,
            evaluated_at=deadline + timedelta(days=1),
            complete_through=date.fromisoformat(pending["entry_date"]),
        ),
        store,
        allow_draft=True,
    )

    assert draining["state"] == "COLLECTING"
    assert store.receipt("deadline_miss") is not None

    assert omitted["state"] == "INVALID"
    assert omitted["reason_codes"] == ["deadline_receipt_candidate_universe_mismatch"]

    assert killed["state"] == "KILL"
    assert killed["reason_codes"] == ["insufficient_enrollment"]
    assert killed["deadline_miss_receipt_sha256"] is not None


def test_complete_entry_date_cannot_skip_an_unresolved_candidate() -> None:
    candidates = _candidate_rows([2] * 60)
    pending = _pending_candidate()
    pending["entry_date"] = candidates[-1]["entry_date"]
    material = (
        f"{inference.CAPACITY_RANK_SALT}|{pending['entry_date']}|{pending['packet_id']}|"
        f"{pending['accession_number']}|{pending['symbol']}"
    )
    pending["entry_rank_sha256"] = hashlib.sha256(material.encode()).hexdigest()
    rows = sorted(candidates + [pending], key=lambda item: item["source_first_observed_at_utc"])

    report = _evaluate(
        _payload(
            rows,
            evaluated_at=ACTIVATED_AT + timedelta(days=150),
            complete_through=date.fromisoformat(candidates[-1]["entry_date"]),
        )
    )

    assert report["state"] == "INVALID"
    assert report["reason_codes"] == ["complete_entry_dates_skip_pending_candidate"]


def test_integrity_failure_is_invalid_without_calculating_outcomes() -> None:
    candidates = _candidate_rows([2] * 60)
    terminal = _terminal_dataset(candidates, gross_return=0.03)
    evaluated = datetime.fromisoformat(terminal["sealed_at_utc"].replace("Z", "+00:00"))

    report = _evaluate(
        _payload(
            candidates,
            evaluated_at=evaluated,
            complete_through=date.fromisoformat(terminal["freeze_boundary_entry_date"]),
            terminal=terminal,
            false_check="snapshot_hashes",
        )
    )

    assert report["state"] == "INVALID"
    assert report["reason_codes"] == ["integrity_failed:snapshot_hashes"]
    assert report["inference"] is None


def test_tampered_or_incomplete_terminal_dataset_fails_closed() -> None:
    candidates = _candidate_rows([2] * 60)
    terminal = _terminal_dataset(candidates, gross_return=0.03)
    evaluated = datetime.fromisoformat(terminal["sealed_at_utc"].replace("Z", "+00:00"))
    terminal["challenger_trades"][0]["gross_return"] = 99.0

    tampered = _evaluate(
        _payload(
            candidates,
            evaluated_at=evaluated,
            complete_through=date.fromisoformat(terminal["freeze_boundary_entry_date"]),
            terminal=terminal,
        )
    )
    assert tampered["state"] == "INVALID"
    assert tampered["reason_codes"] == ["terminal_dataset_digest_mismatch"]
    assert tampered["inference"] is None

    terminal = _terminal_dataset(candidates, gross_return=0.03)
    terminal["challenger_trades"].pop()
    unsigned = dict(terminal)
    unsigned.pop("dataset_sha256")
    terminal["dataset_sha256"] = hashlib.sha256(rfc8785.dumps(unsigned)).hexdigest()
    incomplete = _evaluate(
        _payload(
            candidates,
            evaluated_at=evaluated,
            complete_through=date.fromisoformat(terminal["freeze_boundary_entry_date"]),
            terminal=terminal,
        )
    )
    assert incomplete["state"] == "INVALID"
    assert incomplete["reason_codes"] == ["challenger_outcomes_do_not_match_frozen_cohort"]

    terminal = _terminal_dataset(candidates, gross_return=0.03)
    changed_candidates = json.loads(json.dumps(candidates))
    changed_candidates[0]["evidence_record_sha256"] = hashlib.sha256(
        b"alternate-evidence"
    ).hexdigest()
    projection_mismatch = _evaluate(
        _payload(
            changed_candidates,
            evaluated_at=evaluated,
            complete_through=date.fromisoformat(terminal["freeze_boundary_entry_date"]),
            terminal=terminal,
        )
    )
    assert projection_mismatch["state"] == "INVALID"
    assert projection_mismatch["reason_codes"] == ["terminal_candidate_projection_digest_mismatch"]


def test_terminal_entry_cannot_precede_the_source_observation() -> None:
    candidates = _candidate_rows([2] * 60)
    for index, candidate in enumerate(candidates):
        entry_day = date.fromisoformat(candidate["entry_date"])
        candidate["source_first_observed_at_utc"] = _utc_text(
            datetime.combine(entry_day, time(14, 31), tzinfo=UTC) + timedelta(microseconds=index)
        )
    candidates.sort(key=lambda item: (item["source_first_observed_at_utc"], item["candidate_id"]))
    terminal = _terminal_dataset(candidates, gross_return=0.03)
    evaluated = datetime.fromisoformat(terminal["sealed_at_utc"].replace("Z", "+00:00"))

    report = _evaluate(
        _payload(
            candidates,
            evaluated_at=evaluated,
            complete_through=date.fromisoformat(terminal["freeze_boundary_entry_date"]),
            terminal=terminal,
        )
    )

    assert report["state"] == "INVALID"
    assert report["reason_codes"] == ["challenger_entry_not_after_source_observation"]


def test_append_only_terminal_seal_and_report_prohibit_a_second_look(
    tmp_path: Path,
) -> None:
    candidates = _candidate_rows([2] * 60)
    terminal = _terminal_dataset(candidates, gross_return=0.03)
    evaluated = datetime.fromisoformat(terminal["sealed_at_utc"].replace("Z", "+00:00"))
    payload = _payload(
        candidates,
        evaluated_at=evaluated,
        complete_through=date.fromisoformat(terminal["freeze_boundary_entry_date"]),
        terminal=terminal,
    )
    store = inference.TrialSealStore(tmp_path / "seals.db")

    missing_seal = inference.evaluate_with_store(_registry(), payload, store, allow_draft=True)
    assert missing_seal["state"] == "INVALID"
    assert missing_seal["reason_codes"] == ["terminal_seal_receipt_required"]
    assert store.existing_report() is None

    store.seal_terminal(payload, recorded_at=evaluated)
    first = inference.evaluate_with_store(_registry(), payload, store, allow_draft=True)

    altered = json.loads(json.dumps(payload))
    altered["terminal_dataset"]["challenger_trades"][0]["gross_return"] = 0.5
    unsigned = dict(altered["terminal_dataset"])
    unsigned.pop("dataset_sha256")
    altered["terminal_dataset"]["dataset_sha256"] = hashlib.sha256(
        rfc8785.dumps(unsigned)
    ).hexdigest()

    with pytest.raises(inference.TrialInvalid, match="alternate_terminal_seal"):
        store.seal_terminal(altered, recorded_at=evaluated)
    assert inference.evaluate_with_store(_registry(), altered, store, allow_draft=True) == first


def test_diagnostic_corruption_is_typed_and_cannot_change_primary_decision() -> None:
    candidates = _candidate_rows([2] * 60)
    terminal = _terminal_dataset(candidates, gross_return=0.03)
    diagnostic = dict(terminal["challenger_trades"][0])
    diagnostic["confirmatory_enrollment_sequence"] = None
    diagnostic["evidence_record_sha256"] = None
    diagnostic["entry_rank_sha256"] = None
    terminal["control_trades"] = [diagnostic, dict(diagnostic)]
    unsigned = dict(terminal)
    unsigned.pop("dataset_sha256")
    terminal["dataset_sha256"] = hashlib.sha256(rfc8785.dumps(unsigned)).hexdigest()
    evaluated = datetime.fromisoformat(terminal["sealed_at_utc"].replace("Z", "+00:00"))

    report = _evaluate(
        _payload(
            candidates,
            evaluated_at=evaluated,
            complete_through=date.fromisoformat(terminal["freeze_boundary_entry_date"]),
            terminal=terminal,
        )
    )

    assert report["state"] == "PROMOTE_RECOMMENDED"
    assert report["falsification_context"]["control"] == {
        "status": "unavailable",
        "error_code": "control_trade_id_duplicate",
    }


def test_extreme_finite_returns_fail_closed_instead_of_crashing() -> None:
    candidates = _candidate_rows([2] * 60)
    terminal = _terminal_dataset(candidates, gross_return=1e308)
    evaluated = datetime.fromisoformat(terminal["sealed_at_utc"].replace("Z", "+00:00"))

    report = _evaluate(
        _payload(
            candidates,
            evaluated_at=evaluated,
            complete_through=date.fromisoformat(terminal["freeze_boundary_entry_date"]),
            terminal=terminal,
        )
    )

    assert report["state"] == "INVALID"
    assert report["reason_codes"] == ["numeric_arithmetic_invalid"]


def test_active_registry_requires_every_bound_artifact() -> None:
    candidates = _candidate_rows([1])
    payload = _payload(
        candidates,
        evaluated_at=ACTIVATED_AT + timedelta(days=2),
        complete_through=date.fromisoformat(candidates[0]["entry_date"]),
    )
    active = _active_registry()

    accepted = inference.evaluate_trial(active, payload)
    assert accepted["state"] == "COLLECTING"

    del active["activation"]["dependency_lock_sha256"]
    rejected = inference.evaluate_trial(active, payload)
    assert rejected["state"] == "INVALID"
    assert rejected["reason_codes"] == ["registry_schema_validation_failed"]


def test_production_evaluation_refuses_unactivated_draft_and_has_no_order_code() -> None:
    candidates = _candidate_rows([1])
    payload = _payload(
        candidates,
        evaluated_at=ACTIVATED_AT + timedelta(days=1),
        complete_through=date.fromisoformat(candidates[0]["entry_date"]),
    )

    report = inference.evaluate_trial(_registry(), payload)
    source = Path(inference.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_modules = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}

    assert report["state"] == "INVALID"
    assert report["reason_codes"] == ["registry_not_active"]
    assert "execution.ibkr" not in source
    assert "placeOrder" not in source
    assert "submit_market" not in source
    assert "cancel_order" not in source
    forbidden_roots = ("ib_async", "insider_alerts.execution", "socket", "httpx")
    assert not any(
        module == root or module.startswith(root + ".")
        for module in imported_modules
        for root in forbidden_roots
    )
