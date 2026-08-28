from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

import insider_alerts.research.feature_capture as feature_capture_module
from insider_alerts.research.feature_capture import (
    FeatureCaptureConfig,
    FeatureCaptureConfigurationError,
    feature_capture_status,
    run_feature_capture_once,
)
from insider_alerts.sec.client import SecHttpError, SecResource

ROOT = Path(__file__).resolve().parents[1]
POLICY = ROOT / "docs" / "research" / "contracts" / "companyfacts-capture-v1.json"
DECISION = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)


class _Client:
    def __init__(self, resource: SecResource | None = None, error: str | None = None) -> None:
        self.resource = resource
        self.error = error
        self.urls: list[str] = []

    def get_resource(self, url: str) -> SecResource:
        self.urls.append(url)
        if self.error is not None:
            raise SecHttpError(self.error)
        assert self.resource is not None
        return self.resource


def _clock(*values: datetime) -> Iterator[datetime]:
    return iter(values)


def _source(path: Path, *, decision: datetime = DECISION, cik: str = "1234") -> None:
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE research_capture_jobs (
              job_id TEXT,packet_id TEXT,contract_version TEXT,accession_number TEXT,
              issuer_cik TEXT,form_type TEXT,
              payload_json TEXT,decision_json TEXT,source_first_observed_at_utc TEXT,
              decision_at_utc TEXT,created_at_utc TEXT
            )
            """
        )
        conn.execute(
            "INSERT INTO research_capture_jobs VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                "job-1",
                "packet-1",
                "insider-evidence-capture-v1",
                "0001-26-000001",
                cik,
                "4",
                f'{{"issuer_cik":"{cik}","issuer_symbol":"TEST"}}',
                '{"decision":"approve"}',
                (decision - timedelta(seconds=10)).isoformat(),
                decision.isoformat(),
                decision.isoformat(),
            ),
        )


def _config(tmp_path: Path, *, policy: Path = POLICY) -> FeatureCaptureConfig:
    research_root = tmp_path / "data" / "research"
    research_root.mkdir(parents=True)
    return FeatureCaptureConfig(
        source_db=tmp_path / "data" / "insider_alerts.db",
        feature_db=research_root / "feature_evidence.db",
        artifact_root=research_root / "artifacts" / "companyfacts",
        research_root=research_root,
        policy_path=policy,
        activation_at_utc=DECISION - timedelta(hours=1),
        git_commit="a" * 40,
    )


def _resource(payload: object, *, cik: str = "1234") -> SecResource:
    content = json.dumps(payload, separators=(",", ":")).encode()
    return SecResource(
        content=content,
        status_code=200,
        final_url=f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik.zfill(10)}.json",
        etag='"etag"',
        last_modified="Fri, 28 Aug 2026 12:00:00 GMT",
        content_type="application/json",
        upstream_digest="sha-256=:abc:",
    )


def _payload() -> dict[str, object]:
    return {
        "cik": 1234,
        "facts": {
            "dei": {
                "EntityCommonStockSharesOutstanding": {
                    "units": {
                        "shares": [
                            {
                                "filed": "2026-08-27",
                                "end": "2026-08-20",
                                "val": 1000000,
                                "accn": "0001-26-000010",
                                "form": "10-Q",
                            },
                            {
                                "filed": "2026-08-28",
                                "end": "2026-08-27",
                                "val": 9999999,
                                "accn": "0001-26-000011",
                                "form": "8-K",
                            },
                        ]
                    }
                }
            },
            "us-gaap": {
                "CommonStockSharesOutstanding": {
                    "units": {
                        "shares": [
                            {
                                "filed": "2026-08-26",
                                "end": "2026-08-19",
                                "val": 2000000,
                                "accn": "0001-26-000009",
                            }
                        ]
                    }
                }
            },
        },
    }


def test_captures_raw_response_and_conservative_selected_fact(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _source(config.source_db)
    resource = _resource(_payload())
    client = _Client(resource)
    times = _clock(
        DECISION + timedelta(seconds=30),
        DECISION + timedelta(seconds=31),
        DECISION + timedelta(seconds=32),
    )

    result = run_feature_capture_once(config, client=client, now_fn=lambda: next(times))

    assert result.status == "completed"
    assert result.missing_reason is None
    assert client.urls == [
        "https://data.sec.gov/api/xbrl/companyfacts/CIK0000001234.json"
    ]
    with sqlite3.connect(config.feature_db) as conn:
        receipt_bytes = bytes(
            conn.execute("SELECT record_json FROM companyfacts_receipts").fetchone()[0]
        )
        receipt = json.loads(receipt_bytes)
        configuration = conn.execute(
            "SELECT selection_code_sha256 FROM feature_capture_configuration"
        ).fetchone()
        assert configuration is not None and len(str(configuration[0])) == 64
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            conn.execute("UPDATE companyfacts_receipts SET status='missing'")
    assert hashlib.sha256(receipt_bytes).hexdigest() == result.receipt_sha256
    assert receipt["selection"]["selected"]["value"] == 1000000.0
    rejected = receipt["selection"]["candidates"][1]
    assert rejected["rejection_reason"] == "filed_not_strictly_before_cutoff_date"
    raw_path = config.artifact_root / receipt["raw_artifact_ref"]
    assert raw_path.suffix == ".bin"
    assert raw_path.read_bytes() == resource.content
    assert hashlib.sha256(resource.content).hexdigest() == receipt["raw_artifact_sha256"]


def test_preactivation_job_is_never_ingested_or_requested(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _source(config.source_db, decision=config.activation_at_utc - timedelta(seconds=1))
    client = _Client(_resource(_payload()))
    times = _clock(DECISION, DECISION)

    result = run_feature_capture_once(config, client=client, now_fn=lambda: next(times))

    assert result.status == "idle"
    assert client.urls == []
    assert feature_capture_status(config.feature_db)["receipts"] == 0


def test_expired_capture_is_terminal_missing_without_sec_request(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _source(config.source_db)
    client = _Client(_resource(_payload()))
    late = DECISION + timedelta(seconds=901)
    times = _clock(late, late)

    result = run_feature_capture_once(config, client=client, now_fn=lambda: next(times))

    assert result.status == "completed"
    assert result.missing_reason == "request_window_expired"
    assert client.urls == []
    with sqlite3.connect(config.feature_db) as conn:
        assert conn.execute("SELECT status FROM feature_capture_jobs").fetchone()[0] == "complete"


def test_request_failure_retries_then_records_terminal_missing(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _source(config.source_db)
    client = _Client(error="offline")

    for attempt in range(1, 4):
        base = DECISION + timedelta(seconds=attempt * 10)
        times = _clock(base, base, base + timedelta(seconds=1))
        result = run_feature_capture_once(
            config, client=client, now_fn=lambda times=times: next(times)
        )

    assert result.status == "completed"
    assert result.missing_reason == "request_failed"
    with sqlite3.connect(config.feature_db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM feature_capture_attempts").fetchone()[0] == 3
        assert conn.execute("SELECT status FROM feature_capture_jobs").fetchone()[0] == "complete"
        assert conn.execute("SELECT status FROM companyfacts_receipts").fetchone()[0] == "missing"


def test_raw_artifact_failure_retries_then_records_terminal_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    _source(config.source_db)
    real_publish = feature_capture_module._publish

    def fail_raw(root: Path, data: bytes, *, suffix: str) -> tuple[Path, str]:
        if root.name == "raw":
            raise OSError("disk unavailable")
        return real_publish(root, data, suffix=suffix)

    monkeypatch.setattr(feature_capture_module, "_publish", fail_raw)
    for attempt in range(1, 4):
        base = DECISION + timedelta(seconds=attempt * 10)
        times = _clock(base, base, base + timedelta(seconds=1))
        result = run_feature_capture_once(
            config,
            client=_Client(_resource(_payload())),
            now_fn=lambda times=times: next(times),
        )

    assert result.status == "completed"
    assert result.missing_reason == "artifact_publish_failed"
    with sqlite3.connect(config.feature_db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM feature_capture_attempts").fetchone()[0] == 3
        assert conn.execute("SELECT status FROM feature_capture_jobs").fetchone()[0] == "complete"


@pytest.mark.parametrize(
    ("content", "reason"),
    [
        (b"\xff", "invalid_utf8"),
        (b"{", "invalid_json"),
        (b"[]", "invalid_payload"),
        (b'{"cik":9999,"facts":{}}', "issuer_identity_mismatch"),
    ],
)
def test_invalid_success_response_is_preserved_with_typed_missingness(
    tmp_path: Path, content: bytes, reason: str
) -> None:
    config = _config(tmp_path)
    _source(config.source_db)
    resource = _resource({})
    resource = SecResource(
        content=content,
        status_code=200,
        final_url=resource.final_url,
        etag=None,
        last_modified=None,
        content_type="application/json",
        upstream_digest=None,
    )
    times = _clock(
        DECISION + timedelta(seconds=1),
        DECISION + timedelta(seconds=2),
        DECISION + timedelta(seconds=3),
    )

    result = run_feature_capture_once(
        config, client=_Client(resource), now_fn=lambda: next(times)
    )

    assert result.status == "completed"
    assert result.missing_reason == reason
    raw_files = list((config.artifact_root / "raw").glob("*.bin"))
    assert len(raw_files) == 1 and raw_files[0].read_bytes() == content


def test_nonfinite_companyfact_becomes_missing_without_breaking_receipt(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _source(config.source_db)
    payload: Any = _payload()
    payload["facts"]["dei"]["EntityCommonStockSharesOutstanding"]["units"]["shares"][0][
        "val"
    ] = float("nan")
    times = _clock(
        DECISION + timedelta(seconds=1),
        DECISION + timedelta(seconds=2),
        DECISION + timedelta(seconds=3),
    )

    result = run_feature_capture_once(
        config, client=_Client(_resource(payload)), now_fn=lambda: next(times)
    )

    assert result.status == "completed"
    with sqlite3.connect(config.feature_db) as conn:
        receipt = json.loads(
            bytes(conn.execute("SELECT record_json FROM companyfacts_receipts").fetchone()[0])
        )
    first = receipt["selection"]["candidates"][0]
    assert first["rejection_reason"] == "value_not_finite_positive"
    assert first["value_text"] == "nan"


def test_configuration_boundary_and_policy_hash_cannot_change(tmp_path: Path) -> None:
    policy_copy = tmp_path / "policy.json"
    policy_copy.write_bytes(POLICY.read_bytes())
    config = _config(tmp_path, policy=policy_copy)
    _source(config.source_db, decision=config.activation_at_utc - timedelta(seconds=1))
    times = _clock(DECISION, DECISION)
    assert run_feature_capture_once(
        config, client=_Client(_resource(_payload())), now_fn=lambda: next(times)
    ).status == "idle"

    changed_activation = FeatureCaptureConfig(
        source_db=config.source_db,
        feature_db=config.feature_db,
        artifact_root=config.artifact_root,
        research_root=config.research_root,
        policy_path=config.policy_path,
        activation_at_utc=config.activation_at_utc + timedelta(seconds=1),
        git_commit=config.git_commit,
    )
    with pytest.raises(FeatureCaptureConfigurationError, match="immutable"):
        run_feature_capture_once(
            changed_activation,
            client=_Client(_resource(_payload())),
            now_fn=lambda: DECISION,
        )

    policy = json.loads(policy_copy.read_text(encoding="utf-8"))
    policy["maximum_request_lag_seconds"] = 899
    policy_copy.write_text(json.dumps(policy), encoding="utf-8")
    with pytest.raises(FeatureCaptureConfigurationError, match="policy changed"):
        run_feature_capture_once(
            config, client=_Client(_resource(_payload())), now_fn=lambda: DECISION
        )


def test_feature_paths_must_remain_beneath_research_root(tmp_path: Path) -> None:
    config = _config(tmp_path)
    escaped = FeatureCaptureConfig(
        source_db=config.source_db,
        feature_db=tmp_path / "escaped.db",
        artifact_root=config.artifact_root,
        research_root=config.research_root,
        policy_path=config.policy_path,
        activation_at_utc=config.activation_at_utc,
        git_commit=config.git_commit,
    )

    with pytest.raises(FeatureCaptureConfigurationError, match="escaped"):
        run_feature_capture_once(
            escaped, client=_Client(_resource(_payload())), now_fn=lambda: DECISION
        )


def test_worker_and_installer_are_order_incapable_and_hidden() -> None:
    worker = (ROOT / "src/insider_alerts/research/feature_worker.py").read_text(encoding="utf-8")
    module = (ROOT / "src/insider_alerts/research/feature_capture.py").read_text(encoding="utf-8")
    installer = (ROOT / "ops/windows/install-feature-capture-task.ps1").read_text(
        encoding="utf-8"
    )

    assert "ib_async" not in worker + module
    assert "insider_alerts.research.trial" not in worker + module
    assert 'model_copy(update={"sec_retry_attempts": 1})' in worker
    assert "pythonw.exe" in installer
    assert "-Hidden" in installer
    assert "-MultipleInstances IgnoreNew" in installer
    assert "-WindowStyle" not in installer
