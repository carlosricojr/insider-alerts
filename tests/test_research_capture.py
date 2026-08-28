from __future__ import annotations

import hashlib
import json
import sqlite3
import subprocess
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import rfc8785
from jsonschema import Draft202012Validator, FormatChecker

import insider_alerts.research.activation as activation_module
import insider_alerts.research.capture as capture_module
import insider_alerts.research.inference as inference_module
import insider_alerts.research.worker as worker_module
from insider_alerts.research.capture import (
    CaptureConfig,
    CaptureResult,
    CaptureWindow,
    ProcessResult,
    capture_status,
    run_capture_once,
    sha256_bytes,
    verify_history_runtime,
)
from insider_alerts.research.sec_history import HistoryStore
from insider_alerts.review.queue import (
    apply_decision,
    enqueue_review_packet,
    ensure_review_tables,
)
from insider_alerts.sec.models import FilingRef
from insider_alerts.sec.store import upsert_filing_refs
from tests.research_registry_support import draft_registry, write_draft_registry

ROOT = Path(__file__).resolve().parents[1]
VALIDATE_CAPTURE_WINDOW = capture_module._validated_capture_window


def _policy_sha(config: CaptureConfig) -> str:
    return sha256_bytes(config.policy_path.read_bytes())


@pytest.fixture(autouse=True)
def _active_capture_window(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        capture_module,
        "_validated_capture_window",
        lambda config, **_kwargs: CaptureWindow(
            status="active",
            policy_sha256=_policy_sha(config),
            activated_at=datetime(2000, 1, 1, tzinfo=UTC),
            deadline=datetime(2100, 1, 1, tzinfo=UTC),
        ),
    )


def _approved_job(
    tmp_path: Path,
    *,
    owner_ciks: tuple[str, ...] = ("0000000002",),
    owner_count: int | None = None,
) -> tuple[Path, str, datetime]:
    source_db = tmp_path / "source.db"
    accession = "0000000001-26-000001"
    cik = "0000000001"
    packet_id = f"{accession}|{cik}|4"
    observed = datetime.now(UTC) - timedelta(seconds=5)
    ref = FilingRef(
        source="sec_rss",
        cik=cik,
        accession_number=accession,
        form_type="4",
        filed_at=observed - timedelta(minutes=2),
        filing_detail_url="https://www.sec.gov/Archives/test-index.html",
        primary_doc_url=None,
        raw_rss_entry={},
    )
    upsert_filing_refs(str(source_db), [ref])
    with sqlite3.connect(source_db) as conn:
        conn.execute(
            "UPDATE filings SET form4_xml_url=? WHERE accession_number=?",
            ("https://www.sec.gov/Archives/test.xml", accession),
        )
    assert enqueue_review_packet(
        str(source_db),
        ref,
        {
            "issuer_symbol": "TEST",
            "issuer_cik": cik,
            "reporting_owner_cik": owner_ciks[0] if len(owner_ciks) == 1 else None,
            "reporting_owner_ciks": list(owner_ciks),
            "reporting_owner_count": owner_count if owner_count is not None else len(owner_ciks),
            "score": 9.0,
            "rationale": {},
        },
    )
    assert (
        apply_decision(
            str(source_db),
            {
                "packet_id": packet_id,
                "decision": "approve",
                "analyst": "fixture",
                "reason": "prospective fixture",
            },
        )
        == 1
    )
    with sqlite3.connect(source_db) as conn:
        decision_at = datetime.fromisoformat(
            str(
                conn.execute(
                    "SELECT decision_at_utc FROM research_capture_jobs WHERE packet_id=?",
                    (packet_id,),
                ).fetchone()[0]
            )
        ).astimezone(UTC)
    return source_db, packet_id, decision_at


def _config(tmp_path: Path, source_db: Path) -> CaptureConfig:
    runtime = tmp_path / "alpha"
    scripts = runtime / "scripts"
    python = runtime / ".venv" / "Scripts" / "python.exe"
    research = source_db.parent / "research"
    scripts.mkdir(parents=True, exist_ok=True)
    python.parent.mkdir(parents=True, exist_ok=True)
    research.mkdir(parents=True, exist_ok=True)
    python.write_bytes(b"python")
    (scripts / "capture.py").write_text("# live\n", encoding="utf-8")
    (scripts / "historical.py").write_text("# historical\n", encoding="utf-8")
    return CaptureConfig(
        source_db=source_db,
        evidence_db=tmp_path / "evidence.db",
        artifact_root=tmp_path / "artifacts",
        research_root=research,
        alpha_python=python,
        alpha_script=scripts / "capture.py",
        alpha_historical_script=scripts / "historical.py",
        option_chain_store_db=research / "option-chain.db",
        historical_pacing_db=research / "historical-pacing.db",
        canary_ledger=tmp_path / "canary.db",
        history_db=tmp_path / "history.db",
        history_snapshot_sha256="b" * 64,
        insider_git_commit="a" * 40,
        policy_path=write_draft_registry(ROOT, tmp_path),
        evidence_schema_path=ROOT / "docs/research/contracts/evidence-snapshot.schema.json",
        activation_db=tmp_path / "activation.db",
        capture_delay_seconds=1,
    )


def _not_applicable_result(*, request_id: str, observed_at: datetime) -> dict[str, Any]:
    return {
        "schema_version": "insider-evidence-option-surface-result-v1",
        "status": "not_applicable",
        "reason_code": "OPTION_CHAIN_NOT_LISTED",
        "source_id": "ib_gateway:US_OPTIONS:SMART:type1",
        "request_id": request_id,
        "symbol": "TEST",
        "client_id": 48,
        "observed_at_utc": observed_at.isoformat(),
    }


def _historical_artifact(
    *, request_id: str, decision_at: datetime, chain_observed_at: datetime | None = None
) -> dict[str, Any]:
    observed = chain_observed_at or decision_at - timedelta(seconds=1)
    expiry = "2026-09-18"
    targets = [
        {
            "target_id": f"{expiry}|{option_type}|{moneyness:.4f}",
            "expiry": expiry,
            "option_type": option_type,
            "target_moneyness": moneyness,
        }
        for moneyness in (0.9, 1.0)
        for option_type in ("call", "put")
    ]
    return {
        "schema_version": "insider-evidence-option-history-v2",
        "artifact_status": "RESEARCH_ONLY",
        "source_id": "ib_gateway:US_OPTIONS:SMART:type1:historical_bid_ask_15m",
        "capture_mode": "FORWARD_CLOSED_VENUE_FALLBACK",
        "trade_selection_authority": False,
        "backfill_authority": False,
        "request_id": request_id,
        "symbol": "TEST",
        "client_id": 49,
        "market_data_type": 1,
        "information_cutoff_utc": decision_at.isoformat(),
        "requested_at_utc": (decision_at + timedelta(milliseconds=1)).isoformat(),
        "captured_at_utc": (decision_at + timedelta(milliseconds=2)).isoformat(),
        "option_chain_feed_sequence": 1,
        "option_chain_feed_record_sha256": "c" * 64,
        "option_chain_snapshot": {
            "observed_at_utc": observed.isoformat(),
            "expiry_candidates": [expiry],
            "listed_strikes": [9.0, 10.0],
        },
        "option_chain_snapshot_staleness_seconds": (decision_at - observed).total_seconds(),
        "underlying_reference": {
            "symbol": "TEST",
            "what_to_show": "MIDPOINT",
            "bar_size_seconds": 900,
            "bar_start_utc": (decision_at - timedelta(minutes=16)).isoformat(),
            "bar_end_utc": (decision_at - timedelta(minutes=1)).isoformat(),
            "open": 10.0,
            "high": 10.1,
            "low": 9.9,
            "close": 10.0,
            "staleness_seconds": 60.0,
        },
        "targets": targets,
        "bars": [],
        "capture_errors": [
            {
                "target": target,
                "contract": None,
                "selected_strike": 10.0,
                "error_code": "OPTION_QUALIFICATION_FAILED",
                "provider_stage": "option_qualification",
                "historical_request_issued": False,
            }
            for target in targets
        ],
        "historical_request_count": 1,
        "historical_pacing_units": 1,
    }


def _install_chain_custody(
    path: Path,
    artifact: dict[str, Any],
    *,
    record_sha256: str | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    chain = artifact["option_chain_snapshot"]
    payload = {
        "record_type": "snapshot",
        "symbol": artifact["symbol"],
        "observed_at_utc": chain["observed_at_utc"],
        "snapshot": chain,
    }
    with closing(sqlite3.connect(path)) as conn, conn:
        conn.execute(
            """
            CREATE TABLE option_chain_feed_records(
              sequence INTEGER PRIMARY KEY,
              record_sha256 TEXT NOT NULL,
              symbol TEXT NOT NULL,
              observed_at_utc TEXT NOT NULL,
              payload_json TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "INSERT INTO option_chain_feed_records VALUES(?,?,?,?,?)",
            (
                artifact["option_chain_feed_sequence"],
                record_sha256 or artifact["option_chain_feed_record_sha256"],
                artifact["symbol"],
                chain["observed_at_utc"],
                json.dumps(payload),
            ),
        )


def test_draft_registry_heartbeats_without_claiming_or_writing_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_db, _, decision_at = _approved_job(tmp_path)
    config = _config(tmp_path, source_db)
    monkeypatch.setattr(
        capture_module,
        "_validated_capture_window",
        lambda config, **_kwargs: CaptureWindow(status="draft", policy_sha256=_policy_sha(config)),
    )

    result = run_capture_once(config, now=decision_at + timedelta(seconds=2))

    assert result.status == "idle"
    status = capture_status(source_db, config.evidence_db)
    assert status["evidence_count"] == 0
    assert status["jobs"] == {"pending": 1}
    assert status["health"]["last_result"] == "idle_registry_draft"


def test_active_window_excludes_pre_activation_job_without_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_db, _, decision_at = _approved_job(tmp_path)
    config = _config(tmp_path, source_db)
    activation = decision_at + timedelta(seconds=1)
    monkeypatch.setattr(
        capture_module,
        "_validated_capture_window",
        lambda config, **_kwargs: CaptureWindow(
            status="active",
            policy_sha256=_policy_sha(config),
            activated_at=activation,
            deadline=activation + timedelta(days=30),
        ),
    )

    result = run_capture_once(config, now=decision_at + timedelta(seconds=2))

    assert result.status == "idle"
    status = capture_status(source_db, config.evidence_db)
    assert status["evidence_count"] == 0
    assert status["jobs"] == {"failed": 1}
    with sqlite3.connect(source_db) as conn:
        assert (
            conn.execute("SELECT last_error_kind FROM research_capture_jobs").fetchone()[0]
            == "OUTSIDE_CONFIRMATORY_CAPTURE_WINDOW"
        )


def _source_observed_at(source_db: Path) -> datetime:
    with sqlite3.connect(source_db) as conn:
        value = conn.execute(
            "SELECT source_first_observed_at_utc FROM research_capture_jobs"
        ).fetchone()[0]
    return datetime.fromisoformat(str(value)).astimezone(UTC)


def test_active_window_excludes_exact_deadline_with_mixed_timestamp_encoding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_db, _, decision_at = _approved_job(tmp_path)
    config = _config(tmp_path, source_db)
    observed_at = _source_observed_at(source_db)
    monkeypatch.setattr(
        capture_module,
        "_validated_capture_window",
        lambda config, **_kwargs: CaptureWindow(
            status="active",
            policy_sha256=_policy_sha(config),
            activated_at=observed_at - timedelta(days=1),
            deadline=observed_at,
        ),
    )

    assert run_capture_once(config, now=decision_at + timedelta(seconds=2)).status == "idle"
    assert capture_status(source_db, config.evidence_db)["jobs"] == {"failed": 1}


def test_active_window_includes_exact_activation_with_mixed_timestamp_encoding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_db, _, decision_at = _approved_job(tmp_path)
    config = _config(tmp_path, source_db)
    observed_at = _source_observed_at(source_db)
    monkeypatch.setattr(
        capture_module,
        "_validated_capture_window",
        lambda config, **_kwargs: CaptureWindow(
            status="active",
            policy_sha256=_policy_sha(config),
            activated_at=observed_at,
            deadline=observed_at + timedelta(days=1),
        ),
    )

    assert run_capture_once(config, now=decision_at).status == "idle"
    assert capture_status(source_db, config.evidence_db)["jobs"] == {"pending": 1}


def test_active_window_leaves_terminal_out_of_window_job_immutable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_db, _, decision_at = _approved_job(tmp_path)
    config = _config(tmp_path, source_db)
    with sqlite3.connect(source_db) as conn:
        conn.execute(
            "UPDATE research_capture_jobs SET status='failed',last_error_kind=?",
            ("CAPTURE_ATTEMPTS_EXHAUSTED",),
        )
    monkeypatch.setattr(
        capture_module,
        "_validated_capture_window",
        lambda config, **_kwargs: CaptureWindow(
            status="active",
            policy_sha256=_policy_sha(config),
            activated_at=decision_at + timedelta(seconds=1),
            deadline=decision_at + timedelta(days=1),
        ),
    )

    assert run_capture_once(config, now=decision_at + timedelta(seconds=2)).status == "idle"
    with sqlite3.connect(source_db) as conn:
        assert conn.execute(
            "SELECT status,last_error_kind FROM research_capture_jobs"
        ).fetchone() == ("failed", "CAPTURE_ATTEMPTS_EXHAUSTED")


def test_active_window_quarantines_expired_out_of_window_lease(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_db, _, decision_at = _approved_job(tmp_path)
    config = _config(tmp_path, source_db)
    with sqlite3.connect(source_db) as conn:
        conn.execute(
            """
            UPDATE research_capture_jobs
            SET status='leased',attempt_count=1,lease_owner='old-worker',lease_expires_at_utc=?
            """,
            (capture_module.utc_text(decision_at),),
        )
    monkeypatch.setattr(
        capture_module,
        "_validated_capture_window",
        lambda config, **_kwargs: CaptureWindow(
            status="active",
            policy_sha256=_policy_sha(config),
            activated_at=decision_at + timedelta(seconds=1),
            deadline=decision_at + timedelta(days=1),
        ),
    )

    assert run_capture_once(config, now=decision_at + timedelta(seconds=2)).status == "idle"
    with sqlite3.connect(source_db) as conn:
        assert conn.execute(
            "SELECT status,lease_owner,last_error_kind FROM research_capture_jobs"
        ).fetchone() == ("failed", None, "OUTSIDE_CONFIRMATORY_CAPTURE_WINDOW")


def test_draft_fixture_validates_as_draft_capture_window(tmp_path: Path) -> None:
    config = _config(tmp_path, tmp_path / "source.db")
    assert VALIDATE_CAPTURE_WINDOW(config) == CaptureWindow(
        status="draft",
        policy_sha256=_policy_sha(config),
    )


def test_active_registry_derives_exact_capture_window(tmp_path: Path) -> None:
    registry = draft_registry(ROOT)
    registry["status"] = "active"
    activated_at = datetime(2026, 8, 27, 15, 0, tzinfo=UTC)
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    file_sha = inference_module._file_sha256
    registry["activation"] = {
        "activation_prepared_at_utc": capture_module.utc_text(activated_at - timedelta(hours=2)),
        "activated_at_utc": capture_module.utc_text(activated_at),
        "activation_git_commit": commit,
        "registry_definition_sha256": inference_module.registry_definition_sha256(registry),
        "preregistration_sha256": file_sha(ROOT / registry["preregistration"]),
        "hypothesis_schema_sha256": file_sha(
            ROOT / "docs/research/contracts/hypothesis-registry.schema.json"
        ),
        "evidence_schema_sha256": file_sha(
            ROOT / "docs/research/contracts/evidence-snapshot.schema.json"
        ),
        "inference_artifact_sha256": inference_module.inference_artifact_sha256(),
        "terminal_builder_artifact_sha256": file_sha(
            ROOT / "src/insider_alerts/research/terminal_builder.py"
        ),
        "activation_artifact_sha256": file_sha(ROOT / "src/insider_alerts/research/activation.py"),
        "dependency_lock_sha256": file_sha(ROOT / "uv.lock"),
        "policy_sha256": file_sha(ROOT / registry["strategy"]["policy_artifact"]),
        "classifier_version": inference_module.CLASSIFIER_VERSION,
        "enrollment_start_sequence": 1,
        "activation_receipt_sha256": "",
    }
    registry["activation"]["activation_receipt_sha256"] = activation_module.activation_receipt(
        registry
    )["receipt_sha256"]
    activation_module.ActivationStore(tmp_path / "activation.db").put(registry)
    policy_path = tmp_path / "active-registry.json"
    policy_path.write_bytes(rfc8785.dumps(registry))
    config = replace(_config(tmp_path, tmp_path / "source.db"), policy_path=policy_path)

    assert (
        VALIDATE_CAPTURE_WINDOW(config, now=activated_at - timedelta(microseconds=1)).status
        == "armed"
    )
    assert VALIDATE_CAPTURE_WINDOW(config, now=activated_at) == CaptureWindow(
        status="active",
        policy_sha256=_policy_sha(config),
        activated_at=activated_at,
        deadline=inference_module.enrollment_deadline(activated_at),
    )


def test_snapshot_rejects_registry_replacement_after_claim(tmp_path: Path) -> None:
    policy_path = write_draft_registry(ROOT, tmp_path)
    config = replace(
        _config(tmp_path, tmp_path / "source.db"),
        policy_path=policy_path,
    )
    expected_sha = _policy_sha(config)
    policy_path.write_bytes(policy_path.read_bytes() + b"\n")

    with pytest.raises(ValueError, match="registry changed after"):
        capture_module._validate_snapshot(
            config,
            {},
            expected_policy_sha256=expected_sha,
        )


def _install_history_snapshot(
    config: CaptureConfig,
    *,
    classification_year: int,
    created_at: datetime,
    missing_quarter: tuple[int, int] | None = None,
) -> str:
    store = HistoryStore(config.history_db)
    manifest_content = b"manifest"
    manifest_sha = hashlib.sha256(manifest_content).hexdigest()
    manifest_path = config.history_db.parent / "manifest"
    manifest_path.write_bytes(manifest_content)
    members: list[tuple[int, int, str]] = []
    archive_by_year: dict[int, str] = {}
    with store.connect() as conn:
        conn.execute(
            "INSERT INTO raw_objects VALUES(?,?,?)",
            (manifest_sha, len(manifest_content), str(manifest_path)),
        )
        for year in range(2006, classification_year):
            for quarter in range(1, 5):
                if missing_quarter == (year, quarter):
                    continue
                content = f"{year}Q{quarter}".encode()
                digest = hashlib.sha256(content).hexdigest()
                archive_path = config.history_db.parent / digest
                archive_path.write_bytes(content)
                conn.execute(
                    "INSERT INTO raw_objects VALUES(?,?,?)",
                    (digest, len(content), str(archive_path)),
                )
                conn.execute(
                    "INSERT INTO archive_releases VALUES(?,?,?,?,?)",
                    (
                        digest,
                        year,
                        quarter,
                        f"https://www.sec.gov/{year}q{quarter}.zip",
                        created_at.isoformat(),
                    ),
                )
                members.append((year, quarter, digest))
                if quarter == 1:
                    archive_by_year[year] = digest
        for month, year in enumerate(range(classification_year - 3, classification_year), start=1):
            archive_sha = archive_by_year[year]
            accession = f"0000000002-{year % 100:02d}-{month:06d}"
            filing_date = f"{year}-{month:02d}-02"
            transaction_date = f"{year}-{month:02d}-01"
            conn.execute(
                "INSERT INTO sec_submissions VALUES(?,?,?,?,?,?,?,?)",
                (
                    archive_sha,
                    accession,
                    filing_date,
                    transaction_date,
                    None,
                    "4",
                    "1",
                    "TEST",
                ),
            )
            conn.execute(
                "INSERT INTO sec_reporting_owners VALUES(?,?,?)",
                (archive_sha, accession, "2"),
            )
            conn.execute(
                "INSERT INTO sec_nonderiv_transactions VALUES(?,?,?,?,?,?,?,?)",
                (archive_sha, accession, "1", transaction_date, "4", "P", "A", 1),
            )
    return store.create_snapshot(
        manifest_sha256=manifest_sha,
        members=members,
        created_at=created_at,
    )


def test_approval_atomically_enqueues_only_future_approved_signals(tmp_path: Path) -> None:
    source_db, packet_id, _ = _approved_job(tmp_path)

    with sqlite3.connect(source_db) as conn:
        row = conn.execute("SELECT * FROM research_capture_jobs").fetchone()

    assert row is not None
    assert row[0] == f"{packet_id}|insider-evidence-capture-v1"
    assert row[1] == packet_id
    assert row[10] == "pending"
    with sqlite3.connect(source_db) as conn:
        with pytest.raises(sqlite3.IntegrityError, match="identity is immutable"):
            conn.execute("UPDATE research_capture_jobs SET payload_json='{}'")
        with pytest.raises(sqlite3.IntegrityError, match="cannot be deleted"):
            conn.execute("DELETE FROM research_capture_jobs")

    reject_db = tmp_path / "reject.db"
    ensure_review_tables(str(reject_db))
    with sqlite3.connect(reject_db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM research_capture_jobs").fetchone()[0] == 0


def test_terminal_option_failure_is_persisted_as_valid_immutable_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_db, packet_id, decision_at = _approved_job(tmp_path)
    config = _config(tmp_path, source_db)

    def fail_options(*_args: Any, **_kwargs: Any) -> tuple[Any, ...]:
        return (
            None,
            None,
            None,
            "OPTION_ARTIFACT_INVALID",
            "hostile invalid surface",
            False,
        )

    monkeypatch.setattr(capture_module, "_capture_options", fail_options)
    result = run_capture_once(config, now=decision_at + timedelta(seconds=2), worker_id="test")

    assert result.status == "completed"
    assert result.option_status == "OPTION_ARTIFACT_INVALID"
    with sqlite3.connect(config.evidence_db) as conn:
        row = conn.execute(
            "SELECT record_json,record_sha256,stored_bytes_sha256 FROM evidence_snapshots"
        ).fetchone()
        assert row is not None
        record_bytes = bytes(row[0])
        record = json.loads(record_bytes)
        schema = json.loads(
            (ROOT / "docs/research/contracts/evidence-snapshot.schema.json").read_text(
                encoding="utf-8"
            )
        )
        Draft202012Validator(schema, format_checker=FormatChecker()).validate(record)
        unsigned = dict(record)
        unsigned.pop("record_sha256")
        assert row[1] == sha256_bytes(rfc8785.dumps(unsigned))
        assert row[2] == sha256_bytes(record_bytes)
        assert record["payload"]["observations"]["options_surface"]["status"] == "error"
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            conn.execute("UPDATE evidence_snapshots SET job_id='mutated'")

    with sqlite3.connect(source_db) as conn:
        state = conn.execute(
            "SELECT status,attempt_count,record_sha256 FROM research_capture_jobs"
        ).fetchone()
        assert state == ("complete", 1, result.snapshot_sha256)
        assert conn.execute("SELECT COUNT(*) FROM research_capture_attempts").fetchone()[0] == 1
    assert run_capture_once(config, now=decision_at + timedelta(seconds=3)).status == "idle"
    assert packet_id in result.job_id
    assert capture_status(config.source_db, config.evidence_db)["owner_history"] == {"error": 1}


def test_evidence_store_migrates_legacy_status_rows_without_rewriting_them(
    tmp_path: Path,
) -> None:
    evidence_db = tmp_path / "legacy-evidence.db"
    legacy_record = json.dumps(
        {"payload": {"observations": {"owner_history": {"status": "missing"}}}}
    ).encode()
    with sqlite3.connect(evidence_db) as conn:
        conn.executescript(
            """
            CREATE TABLE evidence_snapshots (
                sequence INTEGER PRIMARY KEY,
                snapshot_id TEXT NOT NULL UNIQUE,
                job_id TEXT NOT NULL UNIQUE,
                record_sha256 TEXT NOT NULL UNIQUE,
                stored_bytes_sha256 TEXT NOT NULL,
                record_json BLOB NOT NULL,
                recorded_at_utc TEXT NOT NULL
            );
            CREATE TABLE capture_health (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                last_worker_heartbeat_utc TEXT NOT NULL,
                last_result TEXT NOT NULL,
                last_job_id TEXT
            );
            """
        )
        conn.execute(
            "INSERT INTO evidence_snapshots VALUES(1,'snapshot','job','record','stored',?,?)",
            (legacy_record, datetime.now(UTC).isoformat()),
        )

    capture_module.ensure_evidence_store(evidence_db)

    with sqlite3.connect(evidence_db) as conn:
        evidence_columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(evidence_snapshots)")
        }
        health_columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(capture_health)")}
        assert "owner_history_status" in evidence_columns
        assert {"last_error_kind", "last_error_message"} <= health_columns
        assert (
            conn.execute("SELECT owner_history_status FROM evidence_snapshots").fetchone()[0]
            is None
        )
    assert capture_status(tmp_path / "missing-source.db", evidence_db)["owner_history"] == {
        "missing": 1
    }


def test_capture_preserves_multi_owner_ambiguity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_db, _, decision_at = _approved_job(tmp_path, owner_ciks=("0000000002", "0000000003"))
    config = _config(tmp_path, source_db)

    monkeypatch.setattr(
        capture_module,
        "_capture_options",
        lambda *_args, **_kwargs: (
            None,
            None,
            None,
            "OPTION_ARTIFACT_INVALID",
            "fixture",
            False,
        ),
    )
    result = run_capture_once(config, now=decision_at + timedelta(seconds=2))
    assert result.status == "completed"
    with sqlite3.connect(config.evidence_db) as conn:
        row = conn.execute("SELECT record_json FROM evidence_snapshots").fetchone()
        assert row is not None
        record = json.loads(bytes(row[0]))
    assert record["payload"]["signal"]["reporting_owner_ciks"] == [
        "0000000002",
        "0000000003",
    ]
    assert record["payload"]["classification"]["owner_cik"] is None
    assert record["payload"]["classification"]["state"] == "ambiguous_multi_owner"
    assert record["payload"]["classification"]["transaction_owner_mapping"] == "ambiguous"


def test_capture_treats_missing_joint_owner_cik_as_ambiguous(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_db, _, decision_at = _approved_job(tmp_path, owner_ciks=("0000000002",), owner_count=2)
    config = _config(tmp_path, source_db)
    monkeypatch.setattr(
        capture_module,
        "_capture_options",
        lambda *_args, **_kwargs: (None, None, None, "OPTION_ARTIFACT_INVALID", "fixture", False),
    )

    result = run_capture_once(config, now=decision_at + timedelta(seconds=2))
    assert result.status == "completed"
    with sqlite3.connect(config.evidence_db) as conn:
        row = conn.execute("SELECT record_json FROM evidence_snapshots").fetchone()
        assert row is not None
        record = json.loads(bytes(row[0]))
    classification = record["payload"]["classification"]
    assert classification["state"] == "ambiguous_multi_owner"
    assert classification["owner_cik"] is None
    assert classification["transaction_owner_mapping"] == "ambiguous"


def test_exact_owner_history_is_classified_from_the_pinned_predecision_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_db, _, decision_at = _approved_job(tmp_path)
    config = _config(tmp_path, source_db)
    classification_year, expected_cutoff = capture_module._classification_boundary(decision_at)
    snapshot_sha = _install_history_snapshot(
        config,
        classification_year=classification_year,
        created_at=decision_at - timedelta(days=1),
    )
    config = replace(config, history_snapshot_sha256=snapshot_sha)
    monkeypatch.setattr(
        capture_module,
        "_capture_options",
        lambda *_args, **_kwargs: (None, None, None, "OPTION_INVALID", "fixture", False),
    )
    original_history_capture = capture_module._capture_owner_history
    history_complete = False

    def tracked_history_capture(*args: Any, **kwargs: Any) -> Any:
        nonlocal history_complete
        result = original_history_capture(*args, **kwargs)
        history_complete = True
        return result

    monotonic_calls = 0

    def tracked_monotonic() -> float:
        nonlocal monotonic_calls
        monotonic_calls += 1
        if monotonic_calls == 1:
            return 10.0
        assert history_complete
        return 15.0

    monkeypatch.setattr(capture_module, "_capture_owner_history", tracked_history_capture)
    monkeypatch.setattr(capture_module, "monotonic", tracked_monotonic)

    assert run_capture_once(config, now=decision_at + timedelta(seconds=2)).status == "completed"

    with sqlite3.connect(config.evidence_db) as conn:
        row = conn.execute("SELECT record_json FROM evidence_snapshots").fetchone()
        assert row is not None
        record = json.loads(bytes(row[0]))
    classification = record["payload"]["classification"]
    observation = record["payload"]["observations"]["owner_history"]
    assert record["enrollment_state"] == "pending_entry_selection"
    assert record["payload"]["versions"]["classifier_version"] is not None
    assert classification["state"] == "opportunistic"
    assert classification["owner_cik"] == "2"
    assert classification["classification_year"] == classification_year
    assert classification["cutoff_at_utc"] == capture_module.utc_text(expected_cutoff)
    assert classification["history_coverage_complete"] is True
    assert classification["history_source_snapshot_sha256"] == snapshot_sha
    assert observation["status"] == "captured"
    assert observation["artifact_sha256"] == snapshot_sha
    assert observation["values"]["classification_reason"] == "opportunistic_until_routine"
    assert observation["values"]["filing_count"] == 3
    assert observation["observed_at_utc"] == record["recorded_at_utc"]
    assert record["payload"]["timing"]["monotonic_capture_duration_ms"] == 5_000

    with HistoryStore(config.history_db).connect() as conn:
        conn.execute("DROP TRIGGER sec_submissions_immutable_update")
        conn.execute("UPDATE sec_submissions SET issuer_cik='tampered'")
    with pytest.raises(ValueError, match="normalized input digest mismatch"):
        verify_history_runtime(config)


def test_history_coverage_gap_is_captured_but_unpartitionable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_db, _, decision_at = _approved_job(tmp_path)
    config = _config(tmp_path, source_db)
    classification_year, _ = capture_module._classification_boundary(decision_at)
    snapshot_sha = _install_history_snapshot(
        config,
        classification_year=classification_year,
        created_at=decision_at - timedelta(days=1),
        missing_quarter=(2007, 2),
    )
    config = replace(config, history_snapshot_sha256=snapshot_sha)
    monkeypatch.setattr(
        capture_module,
        "_capture_options",
        lambda *_args, **_kwargs: (None, None, None, "OPTION_INVALID", "fixture", False),
    )

    assert run_capture_once(config, now=decision_at + timedelta(seconds=2)).status == "completed"

    with sqlite3.connect(config.evidence_db) as conn:
        row = conn.execute("SELECT record_json FROM evidence_snapshots").fetchone()
        assert row is not None
        record = json.loads(bytes(row[0]))
    classification = record["payload"]["classification"]
    observation = record["payload"]["observations"]["owner_history"]
    assert classification["state"] == "unpartitionable"
    assert classification["history_coverage_complete"] is False
    assert observation["status"] == "captured"
    assert observation["values"]["missing_quarters"] == ["2007Q2"]
    assert observation["values"]["classification_reason"] == "coverage_gap"


def test_exact_owner_capture_reauthenticates_history_material_at_point_of_use(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_db, _, decision_at = _approved_job(tmp_path)
    config = _config(tmp_path, source_db)
    classification_year, _ = capture_module._classification_boundary(decision_at)
    snapshot_sha = _install_history_snapshot(
        config,
        classification_year=classification_year,
        created_at=decision_at - timedelta(days=1),
    )
    config = replace(config, history_snapshot_sha256=snapshot_sha)
    with HistoryStore(config.history_db).connect() as conn:
        conn.execute("DROP TRIGGER sec_submissions_immutable_update")
        conn.execute("UPDATE sec_submissions SET issuer_cik='tampered'")
    monkeypatch.setattr(
        capture_module,
        "_capture_options",
        lambda *_args, **_kwargs: (None, None, None, "OPTION_INVALID", "fixture", False),
    )

    result = run_capture_once(config, now=decision_at + timedelta(seconds=2))

    assert result.status == "completed"
    with sqlite3.connect(config.evidence_db) as conn:
        row = conn.execute(
            "SELECT record_json,owner_history_status FROM evidence_snapshots"
        ).fetchone()
    assert row is not None
    record = json.loads(bytes(row[0]))
    assert row[1] == "error"
    assert record["payload"]["classification"]["state"] == "unpartitionable"
    assert any(
        "normalized input digest mismatch" in error["message"]
        for error in record["payload"]["errors"]
    )


def test_postdecision_history_snapshot_is_an_explicit_isolated_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_db, _, decision_at = _approved_job(tmp_path)
    config = _config(tmp_path, source_db)
    classification_year, _ = capture_module._classification_boundary(decision_at)
    snapshot_sha = _install_history_snapshot(
        config,
        classification_year=classification_year,
        created_at=decision_at + timedelta(microseconds=1),
    )
    config = replace(config, history_snapshot_sha256=snapshot_sha)
    monkeypatch.setattr(
        capture_module,
        "_capture_options",
        lambda *_args, **_kwargs: (None, None, None, "OPTION_INVALID", "fixture", False),
    )

    assert run_capture_once(config, now=decision_at + timedelta(seconds=2)).status == "completed"

    with sqlite3.connect(config.evidence_db) as conn:
        row = conn.execute("SELECT record_json FROM evidence_snapshots").fetchone()
        assert row is not None
        record = json.loads(bytes(row[0]))
    classification = record["payload"]["classification"]
    observation = record["payload"]["observations"]["owner_history"]
    assert classification["state"] == "unpartitionable"
    assert classification["history_source_snapshot_sha256"] is None
    assert observation["status"] == "error"
    assert any(
        error["kind"] == "OWNER_HISTORY_UNAVAILABLE" for error in record["payload"]["errors"]
    )


def test_transient_history_database_failure_retries_without_writing_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_db, _, decision_at = _approved_job(tmp_path)
    config = _config(tmp_path, source_db)
    config.history_db.touch()
    monkeypatch.setattr(
        capture_module,
        "_capture_options",
        lambda *_args, **_kwargs: (None, None, None, "OPTION_INVALID", "fixture", False),
    )
    monkeypatch.setattr(
        HistoryStore,
        "verify_snapshot_material",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            sqlite3.OperationalError("database is locked")
        ),
    )

    result = run_capture_once(config, now=decision_at + timedelta(seconds=2))

    assert result.status == "retry_scheduled"
    assert (
        not config.evidence_db.is_file()
        or capture_status(config.source_db, config.evidence_db)["evidence_count"] == 0
    )


def test_permanent_history_operational_error_is_isolated_in_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_db, _, decision_at = _approved_job(tmp_path)
    config = _config(tmp_path, source_db)
    config.history_db.touch()
    monkeypatch.setattr(
        capture_module,
        "_capture_options",
        lambda *_args, **_kwargs: (None, None, None, "OPTION_INVALID", "fixture", False),
    )
    monkeypatch.setattr(
        HistoryStore,
        "verify_snapshot_material",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            sqlite3.OperationalError("no such table: corrupted")
        ),
    )

    result = run_capture_once(config, now=decision_at + timedelta(seconds=2))

    assert result.status == "completed"
    assert capture_status(config.source_db, config.evidence_db)["owner_history"] == {"error": 1}


def test_classification_year_uses_new_york_calendar_boundary() -> None:
    decision_at = datetime(2027, 1, 1, 0, 30, tzinfo=UTC)

    classification_year, cutoff = capture_module._classification_boundary(decision_at)

    assert classification_year == 2026
    assert cutoff == datetime(2026, 1, 1, 5, 0, tzinfo=UTC)


def test_successful_option_capture_is_content_addressed_and_referenced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_db, packet_id, decision_at = _approved_job(tmp_path)
    config = _config(tmp_path, source_db)

    def successful_process(command: list[str], **_kwargs: Any) -> ProcessResult:
        output = Path(command[command.index("--output") + 1])
        request_id = command[command.index("--request-id") + 1]
        captured = datetime.now(UTC)
        artifact = {
            "schema_version": "insider-evidence-option-surface-v1",
            "artifact_status": "RESEARCH_ONLY",
            "source_id": "ib_gateway:US_OPTIONS:SMART:type1",
            "request_id": request_id,
            "symbol": "TEST",
            "client_id": 48,
            "market_data_type": 1,
            "requested_at_utc": (captured - timedelta(seconds=1)).isoformat(),
            "source_max_ts_utc": captured.isoformat(),
            "captured_at_utc": captured.isoformat(),
            "min_dte_days": 3,
            "max_dte_days": 30,
            "max_expiries": 3,
            "max_contracts_per_expiry": 120,
            "surfaces": [
                {
                    "expiry": "2026-09-18",
                    "underlying_price": 10.0,
                    "underlying_bid": 9.99,
                    "underlying_ask": 10.01,
                    "underlying_source_timestamp_utc": captured.isoformat(),
                    "quotes": [{}],
                }
            ],
        }
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(artifact), encoding="utf-8")
        return ProcessResult(returncode=0, stdout="", stderr="", timed_out=False)

    monkeypatch.setattr(capture_module, "run_hidden_process", successful_process)
    result = run_capture_once(config, now=decision_at + timedelta(seconds=2))

    assert result.status == "completed"
    assert result.option_status == "captured"
    option_files = list((config.artifact_root / "options").glob("*.json"))
    assert len(option_files) == 1
    assert option_files[0].stem == sha256_bytes(option_files[0].read_bytes())
    with sqlite3.connect(config.evidence_db) as conn:
        row = conn.execute("SELECT record_json FROM evidence_snapshots").fetchone()
        record = json.loads(bytes(row[0]))
    observation = record["payload"]["observations"]["options_surface"]
    assert observation["status"] == "captured"
    assert observation["artifact_sha256"] == option_files[0].stem
    assert packet_id in record["payload"]["signal"]["packet_id"]


def test_exact_no_chain_result_is_persisted_as_not_applicable_without_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_db, _, decision_at = _approved_job(tmp_path)
    config = _config(tmp_path, source_db)
    with sqlite3.connect(source_db) as conn:
        job_id = str(conn.execute("SELECT job_id FROM research_capture_jobs").fetchone()[0])
    emitted_at: list[datetime] = []

    def no_chain_process(command: list[str], **_kwargs: Any) -> ProcessResult:
        output = Path(command[command.index("--output") + 1])
        assert not output.exists()
        emitted_at.append(datetime.now(UTC))
        return ProcessResult(
            returncode=4,
            stdout=json.dumps(
                _not_applicable_result(
                    request_id=job_id,
                    observed_at=emitted_at[-1],
                )
            ),
            stderr="Error 200 from guessed contracts must not be interpreted",
            timed_out=False,
        )

    monkeypatch.setattr(capture_module, "run_hidden_process", no_chain_process)

    result = run_capture_once(config, now=decision_at + timedelta(seconds=2))

    assert result.status == "completed"
    assert result.option_status == "not_applicable"
    assert not list((config.artifact_root / "options").glob("*.json"))
    with sqlite3.connect(source_db) as conn:
        assert conn.execute(
            "SELECT status,last_error_kind,last_error_message FROM research_capture_jobs"
        ).fetchone() == ("complete", None, None)
        assert conn.execute(
            "SELECT status,error_kind,error_message FROM research_capture_attempts"
        ).fetchone() == ("completed", None, None)
    with sqlite3.connect(config.evidence_db) as conn:
        row = conn.execute("SELECT record_json FROM evidence_snapshots").fetchone()
    record = json.loads(bytes(row[0]))
    observation = record["payload"]["observations"]["options_surface"]
    assert observation == {
        "status": "not_applicable",
        "as_of_utc": None,
        "observed_at_utc": capture_module.utc_text(emitted_at[0]),
        "source": "ib_gateway:US_OPTIONS:SMART:type1",
        "artifact_ref": None,
        "artifact_sha256": None,
        "values": {
            "schema_version": "insider-evidence-option-surface-result-v1",
            "reason_code": "OPTION_CHAIN_NOT_LISTED",
            "request_id": job_id,
            "symbol": "TEST",
            "client_id": 48,
        },
        "error": None,
    }
    assert all(error["stage"] != "options_surface" for error in record["payload"]["errors"])


@pytest.mark.parametrize(
    ("field", "invalid"),
    [
        ("schema_version", "v0"),
        ("status", "error"),
        ("reason_code", "PROVIDER_FAILED"),
        ("source_id", "other"),
        ("request_id", "other-job"),
        ("symbol", "OTHER"),
        ("client_id", 47),
        ("observed_at_utc", "2026-08-28T12:00:00-04:00"),
    ],
)
def test_no_chain_result_rejects_identity_drift(
    tmp_path: Path, field: str, invalid: object
) -> None:
    output = tmp_path / "must-not-exist.json"
    payload = _not_applicable_result(
        request_id="job-1",
        observed_at=datetime(2026, 8, 28, 16, 0, tzinfo=UTC),
    )
    payload[field] = invalid

    with pytest.raises(ValueError):
        capture_module._validated_option_not_applicable_result(
            ProcessResult(4, json.dumps(payload), "", False),
            output=output,
            expected_request_id="job-1",
            expected_symbol="TEST",
        )


def test_no_chain_result_rejects_extra_fields_wrong_exit_and_artifact(
    tmp_path: Path,
) -> None:
    output = tmp_path / "surface.json"
    payload = _not_applicable_result(
        request_id="job-1",
        observed_at=datetime(2026, 8, 28, 16, 0, tzinfo=UTC),
    )
    cases = [
        ProcessResult(4, json.dumps({**payload, "extra": True}), "", False),
        ProcessResult(1, json.dumps(payload), "", False),
        ProcessResult(4, "not json", "", False),
    ]
    for result in cases:
        with pytest.raises(ValueError):
            capture_module._validated_option_not_applicable_result(
                result,
                output=output,
                expected_request_id="job-1",
                expected_symbol="TEST",
            )

    output.write_text("forbidden", encoding="utf-8")
    with pytest.raises(ValueError):
        capture_module._validated_option_not_applicable_result(
            ProcessResult(4, json.dumps(payload), "", False),
            output=output,
            expected_request_id="job-1",
            expected_symbol="TEST",
        )


@pytest.mark.parametrize("offset", [timedelta(microseconds=-1), timedelta(seconds=2)])
def test_no_chain_result_rejects_timestamp_outside_capture_window(
    tmp_path: Path, offset: timedelta
) -> None:
    decision_at = datetime(2026, 8, 28, 16, 0, tzinfo=UTC)
    payload = _not_applicable_result(
        request_id="job-1",
        observed_at=decision_at + offset,
    )

    with pytest.raises(ValueError):
        capture_module._validated_option_not_applicable_result(
            ProcessResult(4, json.dumps(payload), "", False),
            output=tmp_path / "must-not-exist.json",
            expected_request_id="job-1",
            expected_symbol="TEST",
            observed_not_before=decision_at,
            observed_not_after=decision_at + timedelta(seconds=1),
        )


def test_invalid_no_chain_result_is_a_typed_terminal_evidence_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_db, _, decision_at = _approved_job(tmp_path)
    config = _config(tmp_path, source_db)

    def invalid_no_chain_process(_command: list[str], **_kwargs: Any) -> ProcessResult:
        return ProcessResult(
            returncode=4,
            stdout=json.dumps({"status": "not_applicable"}),
            stderr="",
            timed_out=False,
        )

    monkeypatch.setattr(capture_module, "run_hidden_process", invalid_no_chain_process)

    result = run_capture_once(config, now=decision_at + timedelta(seconds=2))

    assert result.status == "completed"
    assert result.option_status == "OPTION_RESULT_INVALID"
    with sqlite3.connect(source_db) as conn:
        assert conn.execute(
            "SELECT status,last_error_kind FROM research_capture_jobs"
        ).fetchone() == ("complete", "OPTION_RESULT_INVALID")
        assert conn.execute(
            "SELECT status,error_kind,retryable FROM research_capture_attempts"
        ).fetchone() == ("completed", "OPTION_RESULT_INVALID", 0)
    with sqlite3.connect(config.evidence_db) as conn:
        row = conn.execute("SELECT record_json FROM evidence_snapshots").fetchone()
    record = json.loads(bytes(row[0]))
    observation = record["payload"]["observations"]["options_surface"]
    assert observation["status"] == "error"
    assert observation["error"]["kind"] == "OPTION_RESULT_INVALID"
    assert any(
        error["stage"] == "options_surface"
        and error["kind"] == "OPTION_RESULT_INVALID"
        for error in record["payload"]["errors"]
    )


def test_retryable_option_failure_keeps_job_durable_without_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_db, _, decision_at = _approved_job(tmp_path)
    config = _config(tmp_path, source_db)
    monkeypatch.setattr(
        capture_module,
        "_capture_options",
        lambda *_args, **_kwargs: (
            None,
            None,
            None,
            "OPTION_CAPTURE_TIMEOUT",
            "timed out",
            True,
        ),
    )

    result = run_capture_once(config, now=decision_at + timedelta(seconds=2))

    assert result.status == "retry_scheduled"
    with sqlite3.connect(source_db) as conn:
        assert conn.execute(
            "SELECT status,attempt_count FROM research_capture_jobs"
        ).fetchone() == ("retry", 1)
    with sqlite3.connect(config.evidence_db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM evidence_snapshots").fetchone()[0] == 0


def test_closed_venue_uses_one_cutoff_bound_historical_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_db, _, decision_at = _approved_job(tmp_path)
    config = _config(tmp_path, source_db)
    with sqlite3.connect(source_db) as conn:
        job_id = str(conn.execute("SELECT job_id FROM research_capture_jobs").fetchone()[0])
    calls: list[tuple[list[str], int]] = []

    def process(command: list[str], **kwargs: Any) -> ProcessResult:
        calls.append((command, int(kwargs["timeout_seconds"])))
        if command[1] == str(config.alpha_script):
            return ProcessResult(
                returncode=1,
                stdout="",
                stderr=(
                    "IB Gateway request failed: bid/ask packet is not actionable because "
                    "the venue session is not open"
                ),
                timed_out=False,
            )
        output = Path(command[command.index("--output") + 1])
        artifact = _historical_artifact(request_id=job_id, decision_at=decision_at)
        _install_chain_custody(config.option_chain_store_db, artifact)
        output.write_text(
            json.dumps(artifact),
            encoding="utf-8",
        )
        return ProcessResult(returncode=0, stdout="", stderr="", timed_out=False)

    monkeypatch.setattr(capture_module, "run_hidden_process", process)

    result = run_capture_once(config, now=decision_at + timedelta(seconds=2))

    assert result.status == "completed"
    assert result.option_status == "captured_historical"
    assert len(calls) == 2
    historical_command, historical_timeout = calls[1]
    assert historical_command == [
        str(config.alpha_python),
        str(config.alpha_historical_script),
        "--chain-store-db",
        str(config.option_chain_store_db),
        "--pacing-db",
        str(config.historical_pacing_db),
        "--symbol",
        "TEST",
        "--request-id",
        job_id,
        "--information-cutoff",
        capture_module.utc_text(decision_at),
        "--output",
        historical_command[-1],
    ]
    assert historical_timeout == 120
    with sqlite3.connect(source_db) as conn:
        assert conn.execute(
            "SELECT status,attempt_count,last_error_kind FROM research_capture_jobs"
        ).fetchone() == ("complete", 1, None)
    with sqlite3.connect(config.evidence_db) as conn:
        row = conn.execute("SELECT record_json FROM evidence_snapshots").fetchone()
    assert row is not None
    record = json.loads(bytes(row[0]))
    observation = record["payload"]["observations"]["options_surface"]
    assert observation["status"] == "captured"
    assert observation["as_of_utc"] == capture_module.utc_text(decision_at)
    assert observation["values"]["capture_mode"] == "FORWARD_CLOSED_VENUE_FALLBACK"
    assert observation["values"]["target_count"] == 4


def test_historical_fallback_timeout_is_terminal_and_never_retried(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_db, _, decision_at = _approved_job(tmp_path)
    config = _config(tmp_path, source_db)
    calls = 0

    def process(command: list[str], **_kwargs: Any) -> ProcessResult:
        nonlocal calls
        calls += 1
        if command[1] == str(config.alpha_script):
            return ProcessResult(
                returncode=1,
                stdout="",
                stderr="the venue session is not open",
                timed_out=False,
            )
        return ProcessResult(returncode=1, stdout="", stderr="", timed_out=True)

    monkeypatch.setattr(capture_module, "run_hidden_process", process)

    result = run_capture_once(config, now=decision_at + timedelta(seconds=2))

    assert result.status == "completed"
    assert result.option_status == "OPTION_HISTORY_AMBIGUOUS_TIMEOUT"
    assert calls == 2
    assert run_capture_once(config, now=decision_at + timedelta(seconds=3)).status == "idle"
    assert calls == 2
    with sqlite3.connect(source_db) as conn:
        assert conn.execute(
            "SELECT status,attempt_count,last_error_kind FROM research_capture_jobs"
        ).fetchone() == ("complete", 1, "OPTION_HISTORY_AMBIGUOUS_TIMEOUT")
        assert conn.execute(
            "SELECT status,retryable,error_kind FROM research_capture_attempts"
        ).fetchone() == ("completed", 0, "OPTION_HISTORY_AMBIGUOUS_TIMEOUT")
    with sqlite3.connect(config.evidence_db) as conn:
        row = conn.execute("SELECT record_json FROM evidence_snapshots").fetchone()
    record = json.loads(bytes(row[0]))
    assert record["payload"]["observations"]["options_surface"]["status"] == "error"
    assert any(
        error["kind"] == "OPTION_HISTORY_AMBIGUOUS_TIMEOUT" for error in record["payload"]["errors"]
    )


def test_post_cutoff_historical_chain_artifact_is_terminal_invalid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_db, _, decision_at = _approved_job(tmp_path)
    config = _config(tmp_path, source_db)
    with sqlite3.connect(source_db) as conn:
        job_id = str(conn.execute("SELECT job_id FROM research_capture_jobs").fetchone()[0])

    def process(command: list[str], **_kwargs: Any) -> ProcessResult:
        if command[1] == str(config.alpha_script):
            return ProcessResult(
                returncode=1,
                stdout="",
                stderr="the venue session is not open",
                timed_out=False,
            )
        output = Path(command[command.index("--output") + 1])
        artifact = _historical_artifact(
            request_id=job_id,
            decision_at=decision_at,
            chain_observed_at=decision_at + timedelta(microseconds=1),
        )
        _install_chain_custody(config.option_chain_store_db, artifact)
        output.write_text(
            json.dumps(artifact),
            encoding="utf-8",
        )
        return ProcessResult(returncode=0, stdout="", stderr="", timed_out=False)

    monkeypatch.setattr(capture_module, "run_hidden_process", process)

    result = run_capture_once(config, now=decision_at + timedelta(seconds=2))

    assert result.status == "completed"
    assert result.option_status == "OPTION_HISTORY_ARTIFACT_INVALID"


def test_option_database_escape_is_rejected_before_any_child_launch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_db, _, decision_at = _approved_job(tmp_path)
    config = replace(
        _config(tmp_path, source_db),
        option_chain_store_db=tmp_path / "outside" / "chain.db",
    )
    launches = 0

    def process(*_args: Any, **_kwargs: Any) -> ProcessResult:
        nonlocal launches
        launches += 1
        return ProcessResult(returncode=0, stdout="", stderr="", timed_out=False)

    monkeypatch.setattr(capture_module, "run_hidden_process", process)

    result = run_capture_once(config, now=decision_at + timedelta(seconds=2))

    assert result.status == "completed"
    assert result.option_status == "OPTION_RUNTIME_INVALID"
    assert launches == 0


def test_research_root_cannot_be_rebound_away_from_source_data_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_db, _, decision_at = _approved_job(tmp_path)
    outside_research = tmp_path / "outside" / "research"
    outside_research.mkdir(parents=True)
    config = replace(
        _config(tmp_path, source_db),
        research_root=outside_research,
        option_chain_store_db=outside_research / "chain.db",
        historical_pacing_db=outside_research / "pacing.db",
    )
    monkeypatch.setattr(
        capture_module,
        "run_hidden_process",
        lambda *_args, **_kwargs: pytest.fail("must not launch"),
    )

    result = run_capture_once(config, now=decision_at + timedelta(seconds=2))

    assert result.status == "completed"
    assert result.option_status == "OPTION_RUNTIME_INVALID"


def test_historical_chain_digest_must_match_durable_store_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_db, _, decision_at = _approved_job(tmp_path)
    config = _config(tmp_path, source_db)
    with sqlite3.connect(source_db) as conn:
        job_id = str(conn.execute("SELECT job_id FROM research_capture_jobs").fetchone()[0])

    def process(command: list[str], **_kwargs: Any) -> ProcessResult:
        if command[1] == str(config.alpha_script.resolve()):
            return ProcessResult(
                returncode=1,
                stdout="",
                stderr="the venue session is not open",
                timed_out=False,
            )
        artifact = _historical_artifact(request_id=job_id, decision_at=decision_at)
        _install_chain_custody(
            config.option_chain_store_db,
            artifact,
            record_sha256="d" * 64,
        )
        Path(command[command.index("--output") + 1]).write_text(
            json.dumps(artifact), encoding="utf-8"
        )
        return ProcessResult(returncode=0, stdout="", stderr="", timed_out=False)

    monkeypatch.setattr(capture_module, "run_hidden_process", process)

    result = run_capture_once(config, now=decision_at + timedelta(seconds=2))

    assert result.status == "completed"
    assert result.option_status == "OPTION_HISTORY_ARTIFACT_INVALID"


def test_restart_after_snapshot_append_recovers_without_recapture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_db, _, decision_at = _approved_job(tmp_path)
    config = _config(tmp_path, source_db)
    calls = 0

    def fail_options(*_args: Any, **_kwargs: Any) -> tuple[Any, ...]:
        nonlocal calls
        calls += 1
        return None, None, None, "OPTION_ARTIFACT_INVALID", "invalid", False

    monkeypatch.setattr(capture_module, "_capture_options", fail_options)
    first = run_capture_once(config, now=decision_at + timedelta(seconds=2))
    assert first.status == "completed"
    with sqlite3.connect(source_db) as conn:
        conn.execute("DROP TRIGGER research_capture_complete_immutable")
        conn.execute(
            """
            UPDATE research_capture_jobs
            SET status='leased', lease_owner='crashed', lease_expires_at_utc=?, record_sha256=NULL
            """,
            ((decision_at - timedelta(seconds=1)).isoformat().replace("+00:00", "Z"),),
        )
    ensure_review_tables(str(source_db))

    recovered = run_capture_once(config, now=decision_at + timedelta(seconds=3))

    assert recovered.status == "completed"
    assert recovered.option_status == "recovered_existing"
    assert recovered.snapshot_sha256 == first.snapshot_sha256
    assert calls == 1
    with sqlite3.connect(config.evidence_db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM evidence_snapshots").fetchone()[0] == 1


def test_permanent_internal_failure_is_terminal_and_releases_lease(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_db, _, decision_at = _approved_job(tmp_path)
    config = _config(tmp_path, source_db)
    monkeypatch.setattr(
        capture_module,
        "_capture_options",
        lambda *_args, **_kwargs: (None, None, None, "OPTION_INVALID", "invalid", False),
    )
    monkeypatch.setattr(
        capture_module,
        "_append_snapshot",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("invalid envelope")),
    )

    result = run_capture_once(config, now=decision_at + timedelta(seconds=2))

    assert result.status == "failed"
    with sqlite3.connect(source_db) as conn:
        assert conn.execute(
            "SELECT status,lease_owner,lease_expires_at_utc,last_error_kind "
            "FROM research_capture_jobs"
        ).fetchone() == ("failed", None, None, "CAPTURE_INTERNAL_TERMINAL")
        assert conn.execute(
            "SELECT status,retryable FROM research_capture_attempts"
        ).fetchone() == ("failed", 0)


def test_exhausted_unleased_job_is_failed_instead_of_reclaimed(tmp_path: Path) -> None:
    source_db, _, decision_at = _approved_job(tmp_path)
    config = _config(tmp_path, source_db)
    with sqlite3.connect(source_db) as conn:
        conn.execute(
            "UPDATE research_capture_jobs SET status='retry', attempt_count=?",
            (config.max_attempts,),
        )

    assert run_capture_once(config, now=decision_at + timedelta(seconds=2)).status == "idle"
    with sqlite3.connect(source_db) as conn:
        assert conn.execute(
            "SELECT status,last_error_kind FROM research_capture_jobs"
        ).fetchone() == ("failed", "CAPTURE_ATTEMPTS_EXHAUSTED")


def test_worker_entrypoint_is_structurally_order_incapable() -> None:
    worker = (ROOT / "src/insider_alerts/research/worker.py").read_text(encoding="utf-8")
    capture = (ROOT / "src/insider_alerts/research/capture.py").read_text(encoding="utf-8")
    combined = worker + capture

    assert "execution.ibkr" not in combined
    assert "placeOrder" not in combined
    assert "submit_market" not in combined
    assert "cancel_order" not in combined
    assert "verify_history_runtime(config, as_of=job.decision_at)" in capture


def _worker_args(tmp_path: Path) -> list[str]:
    return [
        "--database-path",
        str(tmp_path / "source.db"),
        "--evidence-db",
        str(tmp_path / "evidence.db"),
        "--artifact-root",
        str(tmp_path / "artifacts"),
        "--canary-ledger",
        str(tmp_path / "canary.db"),
        "--history-db",
        str(tmp_path / "history.db"),
        "--history-snapshot-sha256",
        "b" * 64,
        "--alpha-python",
        str(tmp_path / "alpha-python.exe"),
        "--alpha-script",
        str(tmp_path / "alpha-capture.py"),
        "--alpha-historical-script",
        str(tmp_path / "alpha-history.py"),
        "--option-chain-store-db",
        str(tmp_path / "option-chain.db"),
        "--historical-pacing-db",
        str(tmp_path / "historical-pacing.db"),
        "--error-log",
        str(tmp_path / "research-capture.err.log"),
    ]


def test_worker_main_runs_exactly_one_bounded_capture_cycle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    calls: list[CaptureConfig] = []
    job_calls: list[bool] = []
    monkeypatch.setattr(
        worker_module,
        "ensure_kill_on_close_process_tree",
        lambda: job_calls.append(True),
    )
    monkeypatch.setattr(worker_module, "ensure_review_tables", lambda _path: None)
    monkeypatch.setattr(worker_module, "resolve_git_commit", lambda _root: "a" * 40)

    def capture_once(config: CaptureConfig) -> CaptureResult:
        calls.append(config)
        return CaptureResult(status="idle")

    monkeypatch.setattr(worker_module, "run_capture_once", capture_once)

    assert worker_module.main(_worker_args(tmp_path)) == 0
    assert job_calls == [True]
    assert len(calls) == 1
    assert calls[0].alpha_historical_script == tmp_path / "alpha-history.py"
    assert calls[0].option_chain_store_db == tmp_path / "option-chain.db"
    assert calls[0].historical_pacing_db == tmp_path / "historical-pacing.db"
    assert calls[0].research_root == ROOT / "data" / "research"
    assert json.loads(capsys.readouterr().out) == {
        "job_id": None,
        "option_status": None,
        "snapshot_sha256": None,
        "status": "idle",
    }


def test_worker_main_persists_and_logs_fatal_setup_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(worker_module, "ensure_kill_on_close_process_tree", lambda: None)
    def fail_setup(_path: str) -> None:
        raise RuntimeError("setup failed")

    monkeypatch.setattr(worker_module, "ensure_review_tables", fail_setup)

    assert worker_module.main(_worker_args(tmp_path)) == 2
    status = capture_status(tmp_path / "source.db", tmp_path / "evidence.db")
    assert status["health"]["last_result"] == "worker_error"
    assert status["health"]["last_error_kind"] == "CAPTURE_WORKER_FATAL"
    assert status["health"]["last_error_message"] == "RuntimeError: setup failed"
    assert "RuntimeError: setup failed" in (tmp_path / "research-capture.err.log").read_text(
        encoding="utf-8"
    )


def test_research_task_is_hidden_bounded_and_overlap_safe() -> None:
    installer = (ROOT / "ops/windows/install-research-capture-task.ps1").read_text(encoding="utf-8")

    assert ".venv\\Scripts\\pythonw.exe" in installer
    assert '"--loop"' not in installer
    assert "capture_insider_historical_option_evidence.py" in installer
    assert '"--option-chain-store-db' in installer
    assert '"--historical-pacing-db' in installer
    assert "-ExecutionTimeLimit (New-TimeSpan -Minutes 15)" in installer
    assert "-MultipleInstances IgnoreNew" in installer
