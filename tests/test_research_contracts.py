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
    }


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
        "schema_version": 1,
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
                "left_censored": False,
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
    snapshot["confirmatory_enrollment_sequence"] = 1
    snapshot["enrollment_state"] = "enrolled"

    snapshot["payload"]["observations"]["owner_history"] = missing
    with pytest.raises(ValidationError):
        validator.validate(snapshot)
    snapshot["payload"]["observations"]["owner_history"] = captured

    snapshot["recorded_at_utc"] = "2026-08-26T16:00:02-04:00"
    with pytest.raises(ValidationError):
        validator.validate(snapshot)
    snapshot["recorded_at_utc"] = "2026-08-26T20:00:02Z"

    del snapshot["payload"]["observations"]["options_surface"]
    with pytest.raises(ValidationError):
        validator.validate(snapshot)
