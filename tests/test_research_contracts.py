from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator, FormatChecker, ValidationError

ROOT = Path(__file__).resolve().parents[1]
CONTRACTS = ROOT / "docs" / "research" / "contracts"
REGISTRY = ROOT / "docs" / "research" / "registry" / "OPP-E07-V1.json"


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def test_research_contracts_are_draft_2020_12_json_schemas() -> None:
    schemas = sorted(CONTRACTS.glob("*.schema.json"))

    assert [path.name for path in schemas] == [
        "evidence-snapshot.schema.json",
        "hypothesis-registry.schema.json",
    ]
    for schema in map(_load, schemas):
        assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
        assert schema["type"] == "object"
        assert schema["additionalProperties"] is False
        Draft202012Validator.check_schema(schema)


def test_draft_registry_matches_frozen_contract() -> None:
    schema = _load(CONTRACTS / "hypothesis-registry.schema.json")
    registry = _load(REGISTRY)

    Draft202012Validator(schema, format_checker=FormatChecker()).validate(registry)

    assert set(registry) == set(schema["required"])
    assert registry["schema_version"] == 1
    assert re.fullmatch(r"[A-Z0-9]+(?:-[A-Z0-9]+)*-V[0-9]+", registry["hypothesis_id"])
    assert registry["status"] == "draft"
    assert registry["activation"] is None

    preregistration = ROOT / registry["preregistration"]
    assert preregistration.is_file()
    prereg_text = preregistration.read_text(encoding="utf-8")
    assert f"Registry ID: `{registry['hypothesis_id']}`" in prereg_text

    family = registry["family"]
    assert family["correction"] == "bonferroni"
    assert family["threshold"] == family["alpha_budget"] / family["hypothesis_count"]
    assert family["threshold"] == 0.025

    strategy = registry["strategy"]
    assert strategy["base_policy"] == "E07/F00"
    assert strategy["challenger_mode"] == "shadow_only"
    assert strategy["primary_round_trip_bps"] == 20
    assert strategy["stress_round_trip_bps"] == 50

    classifier = registry["classifier"]
    assert classifier["history_observation_start_date"] == "2006-01-01"
    assert classifier["history_source_snapshot_required"] is True
    assert classifier["pre_observation_history"] == "left_censored_measurement_limitation"
    assert classifier["opportunistic_persists_until_routine"] is True

    assert registry["feature_policy"]["confirmatory_features"] == ["owner_classification"]
    assert registry["inference"]["interim_looks"] == 0
    assert registry["terminal_information_time"] == {
        "target_enrolled_trades": 100,
        "minimum_distinct_entry_dates": 60,
        "freeze_at_complete_entry_date": True,
        "include_all_boundary_date_entries": True,
        "all_frozen_outcomes_required": True,
        "max_enrollment_calendar_months": 18,
        "look_count": 1,
        "seal_dataset_before_outcomes": True,
        "entry_date_completion_proof": "append_only_pre_open_cutoff_completion_v1",
        "decision_readiness": "evidence_recorded_and_trial_imported_strictly_before_cutoff",
        "entry_date_lapse_policy": "append_only_mass_missed_no_backdating",
        "bar_input_binding": "observation_sequence_watermark_and_record_digests",
        "bar_poll_receipt_binding": (
            "receipt_sequence_watermark_digests_and_observation_watermarks"
        ),
        "schedule_input_binding": "observation_sequence_watermark_and_point_in_time_record_digests",
        "eligibility_history": "exact_shared_e07_completed_pre_signal_sessions",
        "healthy_bar_poll_proof": (
            "successful_zero_rejection_receipt_after_prior_required_session_close"
        ),
        "outcome_materialization": "after_frozen_tenth_session_healthy_stock_and_spy_polls_v1",
        "outcome_bar_binding": "first_observed_watermark_and_record_digests",
        "outcome_schedule_binding": "entry_completion_frozen_ten_session_records",
        "outcome_missingness": (
            "terminal_healthy_poll_missing_required_pre_exit_or_benchmark_bar_invalid"
        ),
        "outcome_visibility": "individual_immutable_records_no_aggregate_before_terminal_seal",
        "prior_book_binding": "watermark_bounded_first_observed_records_and_occupancy_digest",
        "seal_clock_rule": "last_in_transaction_decision_clock_strictly_before_open",
        "missing_prior_position_policy": (
            "occupy_through_frozen_final_session_then_expire_unconditionally"
        ),
        "candidate_universe_binding": "immutable_identity_provenance_digest",
        "seal_store": "append_only_sqlite_singleton_receipts",
        "terminal_seal_command_separate": True,
    }
    assert registry["diagnostics"] == {
        "groups": ["full_e07_f00_control", "routine_control_subset"],
        "selection_authority": "existing_live_canary_e07_f00_shadow_ledger_only",
        "membership_window": (
            "source_and_canary_signal_at_gte_activation_through_challenger_freeze_entry_date"
        ),
        "timestamp_binding": "source_first_observed_at_lte_canary_approval_signal_at",
        "candidate_provenance": "content_addressed_canary_row_and_activation_metadata",
        "control_outcome_authority": (
            "research_feeds_and_shared_e07_kernel_after_frozen_tenth_session"
        ),
        "canary_outcome_role": "diagnostic_agreement_evidence_only",
        "routine_membership": (
            "first_valid_pre_cutoff_snapshot_routine_exact_single_owner_complete_history"
        ),
        "storage": "separate_append_only_diagnostics_sqlite",
        "missingness": "typed_per_trade_or_group_unavailable",
        "confirmatory_effect": "cannot_veto_rescue_delay_or_mutate_challenger",
        "shadow_book_reconciliation_scope": "challenger_only",
    }

    fixed_arrays = (
        ("classifier", "qualifying_transaction_codes"),
        ("classifier", "excluded_states"),
        ("diagnostics", "groups"),
        ("feature_policy", "confirmatory_features"),
    )
    for parent, field in fixed_arrays:
        truncated = json.loads(json.dumps(registry))
        truncated[parent][field] = truncated[parent][field][:-1]
        with pytest.raises(ValidationError):
            Draft202012Validator(schema).validate(truncated)
    truncated = json.loads(json.dumps(registry))
    truncated["decision_states"] = truncated["decision_states"][:-1]
    with pytest.raises(ValidationError):
        Draft202012Validator(schema).validate(truncated)


def test_evidence_contract_requires_point_in_time_and_failure_evidence() -> None:
    schema = _load(CONTRACTS / "evidence-snapshot.schema.json")
    payload = schema["properties"]["payload"]
    required_payload_fields = set(payload["required"])

    assert {
        "signal",
        "timing",
        "versions",
        "classification",
        "observations",
        "errors",
        "provenance",
    } <= required_payload_fields

    timing_required = set(payload["properties"]["timing"]["required"])
    assert {
        "source_first_observed_at_utc",
        "decision_at_utc",
        "notification_requested_at_utc",
        "notification_responded_at_utc",
        "client_received_at_utc",
        "monotonic_capture_duration_ms",
    } <= timing_required

    version_required = set(payload["properties"]["versions"]["required"])
    assert {
        "git_commit",
        "source_fingerprint_sha256",
        "policy_sha256",
        "classifier_version",
        "model_id",
        "prompt_sha256",
        "configuration_sha256",
    } <= version_required

    observations = payload["properties"]["observations"]
    assert set(observations["required"]) == {
        "sec_source",
        "market_context",
        "options_surface",
        "owner_history",
        "notification_transport",
    }

    observation_status = schema["$defs"]["observation"]["properties"]["status"]["enum"]
    assert observation_status == ["captured", "missing", "error", "not_applicable"]


def test_evidence_contract_accepts_explicit_missingness_and_rejects_omission() -> None:
    schema = _load(CONTRACTS / "evidence-snapshot.schema.json")
    validator = Draft202012Validator(schema, format_checker=FormatChecker())

    captured = {
        "status": "captured",
        "as_of_utc": "2026-08-26T20:00:00Z",
        "observed_at_utc": "2026-08-26T20:00:01Z",
        "source": "fixture",
        "artifact_ref": None,
        "artifact_sha256": None,
        "values": {"fixture": True},
        "error": None,
    }
    missing = {
        "status": "missing",
        "as_of_utc": None,
        "observed_at_utc": "2026-08-26T20:00:01Z",
        "source": "fixture",
        "artifact_ref": None,
        "artifact_sha256": None,
        "values": None,
        "error": None,
    }
    snapshot = {
        "schema_version": 2,
        "snapshot_id": "0f1ebdfa-c859-4a29-998e-837e64cb82f0",
        "hypothesis_id": "OPP-E07-V1",
        "recorded_at_utc": "2026-08-26T20:00:02Z",
        "enrollment_state": "enrolled",
        "confirmatory_enrollment_sequence": 1,
        "supersedes_snapshot_id": None,
        "record_sha256": "0" * 64,
        "payload": {
            "signal": {
                "packet_id": "fixture-packet",
                "accession_number": "0000000000-26-000001",
                "issuer_cik": "1",
                "issuer_symbol": "TEST",
                "form_type": "4",
                "decision": "approve",
                "reporting_owner_ciks": ["2"],
            },
            "timing": {
                "sec_filed_at_utc": "2026-08-26T19:59:00Z",
                "source_first_observed_at_utc": "2026-08-26T20:00:00Z",
                "decision_at_utc": "2026-08-26T20:00:00Z",
                "notification_requested_at_utc": None,
                "notification_responded_at_utc": None,
                "client_received_at_utc": None,
                "monotonic_capture_duration_ms": 1,
                "clock_skew_status": "valid",
            },
            "versions": {
                "git_commit": "0" * 40,
                "source_fingerprint_sha256": "0" * 64,
                "policy_sha256": "0" * 64,
                "classifier_version": "fixture-v1",
                "model_id": None,
                "prompt_sha256": None,
                "configuration_sha256": "0" * 64,
            },
            "classification": {
                "state": "opportunistic",
                "owner_cik": "2",
                "classification_year": 2026,
                "cutoff_at_utc": "2026-01-01T05:00:00Z",
                "transaction_owner_mapping": "exact",
                "history_coverage_complete": True,
                "left_censored": True,
                "history_observation_start_date": "2006-01-01",
                "history_source_snapshot_sha256": "0" * 64,
                "history_input_sha256": "0" * 64,
            },
            "observations": {
                "sec_source": captured,
                "market_context": captured,
                "options_surface": missing,
                "owner_history": captured,
                "notification_transport": missing,
            },
            "errors": [],
            "provenance": {
                "host_id_sha256": "0" * 64,
                "process_id": 1,
                "writer": "fixture",
                "append_only_sequence": 1,
            },
        },
    }

    validator.validate(snapshot)

    legacy = json.loads(json.dumps(snapshot))
    legacy["schema_version"] = 1
    legacy["payload"]["classification"]["left_censored"] = False
    del legacy["payload"]["classification"]["history_observation_start_date"]
    del legacy["payload"]["classification"]["history_source_snapshot_sha256"]
    validator.validate(legacy)

    snapshot["payload"]["signal"]["decision"] = "reject"
    with pytest.raises(ValidationError):
        validator.validate(snapshot)
    snapshot["payload"]["signal"]["decision"] = "approve"

    snapshot["payload"]["classification"]["state"] = "routine"
    with pytest.raises(ValidationError):
        validator.validate(snapshot)
    snapshot["payload"]["classification"]["state"] = "opportunistic"

    snapshot["enrollment_state"] = "capacity_suppressed"
    with pytest.raises(ValidationError):
        validator.validate(snapshot)
    snapshot["confirmatory_enrollment_sequence"] = None
    validator.validate(snapshot)

    snapshot["payload"]["classification"]["left_censored"] = False
    with pytest.raises(ValidationError):
        validator.validate(snapshot)
    snapshot["payload"]["classification"]["left_censored"] = True

    snapshot["payload"]["classification"]["history_observation_start_date"] = "2007-01-01"
    with pytest.raises(ValidationError):
        validator.validate(snapshot)
    snapshot["payload"]["classification"]["history_observation_start_date"] = "2006-01-01"

    snapshot["payload"]["classification"]["history_source_snapshot_sha256"] = None
    with pytest.raises(ValidationError):
        validator.validate(snapshot)
    snapshot["payload"]["classification"]["history_source_snapshot_sha256"] = "0" * 64

    snapshot["confirmatory_enrollment_sequence"] = 1
    snapshot["enrollment_state"] = "enrolled"

    snapshot["payload"]["observations"]["owner_history"] = missing
    with pytest.raises(ValidationError):
        validator.validate(snapshot)
    snapshot["payload"]["observations"]["owner_history"] = captured

    snapshot["payload"]["signal"]["reporting_owner_ciks"] = ["2", "3"]
    with pytest.raises(ValidationError):
        validator.validate(snapshot)
    snapshot["payload"]["signal"]["reporting_owner_ciks"] = ["2"]

    snapshot["payload"]["classification"]["left_censored"] = False
    with pytest.raises(ValidationError):
        validator.validate(snapshot)
    snapshot["payload"]["classification"]["left_censored"] = True

    snapshot["payload"]["classification"]["history_coverage_complete"] = False
    with pytest.raises(ValidationError):
        validator.validate(snapshot)
    snapshot["payload"]["classification"]["history_coverage_complete"] = True

    snapshot["payload"]["classification"]["history_observation_start_date"] = "2007-01-01"
    with pytest.raises(ValidationError):
        validator.validate(snapshot)
    snapshot["payload"]["classification"]["history_observation_start_date"] = "2006-01-01"

    snapshot["payload"]["classification"]["history_source_snapshot_sha256"] = None
    with pytest.raises(ValidationError):
        validator.validate(snapshot)
    snapshot["payload"]["classification"]["history_source_snapshot_sha256"] = "0" * 64

    snapshot["recorded_at_utc"] = "2026-08-26T16:00:02-04:00"
    with pytest.raises(ValidationError):
        validator.validate(snapshot)
    snapshot["recorded_at_utc"] = "2026-08-26T20:00:02Z"

    del snapshot["payload"]["observations"]["options_surface"]
    with pytest.raises(ValidationError):
        validator.validate(snapshot)
