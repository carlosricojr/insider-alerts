"""Isolated point-in-time Companyfacts custody for future research only."""

from __future__ import annotations

import hashlib
import inspect
import json
import math
import os
import sqlite3
import stat
import uuid
from collections.abc import Callable, Mapping
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, Protocol

import rfc8785

from insider_alerts.sec.client import SecHttpError, SecResource

FEATURE_CAPTURE_VERSION = "prospective-companyfacts-capture-v1"
_FACTS: tuple[tuple[str, str], ...] = (
    ("dei", "EntityCommonStockSharesOutstanding"),
    ("us-gaap", "CommonStockSharesOutstanding"),
)


@dataclass(frozen=True, slots=True)
class FeatureCaptureConfig:
    source_db: Path
    feature_db: Path
    artifact_root: Path
    research_root: Path
    policy_path: Path
    activation_at_utc: datetime
    git_commit: str
    lease_seconds: int = 120

    def __post_init__(self) -> None:
        if self.activation_at_utc.tzinfo is None:
            raise ValueError("feature capture activation cannot be naive")
        if len(self.git_commit) != 40 or any(
            character not in "0123456789abcdef" for character in self.git_commit
        ):
            raise ValueError("git_commit must be a lowercase 40-character SHA-1")
        if self.lease_seconds < 30:
            raise ValueError("lease_seconds must be at least 30")


@dataclass(frozen=True, slots=True)
class FeatureCaptureResult:
    status: Literal["idle", "retry_scheduled", "completed", "failed"]
    job_id: str | None = None
    receipt_sha256: str | None = None
    missing_reason: str | None = None


@dataclass(frozen=True, slots=True)
class _Job:
    job_id: str
    packet_id: str
    accession_number: str
    issuer_cik: str
    decision_at_utc: datetime
    source_first_observed_at_utc: datetime
    source_job_sha256: str
    source_validation_error: str | None
    attempt_number: int


class FeatureCaptureConfigurationError(RuntimeError):
    """The capture-only stream cannot prove its immutable configuration."""


class ResourceClient(Protocol):
    def get_resource(self, url: str) -> SecResource: ...


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("UTC timestamp cannot be naive")
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"persisted timestamp is naive: {value}")
    return parsed.astimezone(UTC)


def _now(now_fn: Callable[[], datetime]) -> datetime:
    value = now_fn()
    if value.tzinfo is None:
        raise ValueError("feature capture clock returned a naive timestamp")
    return value.astimezone(UTC)


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical(value: Mapping[str, Any]) -> bytes:
    return rfc8785.dumps(dict(value))


def _connect(path: Path, *, write: bool) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    if write:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _is_reparse_point(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except OSError:
        return False
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    attributes = getattr(metadata, "st_file_attributes", 0)
    return path.is_symlink() or bool(reparse_flag and attributes & reparse_flag)


def _confined_path(path: Path, *, research_root: Path, kind: str) -> Path:
    lexical_root = Path(os.path.abspath(research_root))
    lexical_path = Path(os.path.abspath(path))
    if lexical_path == lexical_root or not lexical_path.is_relative_to(lexical_root):
        raise FeatureCaptureConfigurationError(f"{kind} escaped data/research")
    if not lexical_root.is_dir() or _is_reparse_point(lexical_root):
        raise FeatureCaptureConfigurationError("data/research is unavailable or a reparse point")
    cursor = lexical_root
    relative_parts = lexical_path.relative_to(lexical_root).parts
    for part in relative_parts[:-1]:
        cursor /= part
        if cursor.exists() and _is_reparse_point(cursor):
            raise FeatureCaptureConfigurationError(f"{kind} parent is a reparse point")
    if lexical_path.exists() and _is_reparse_point(lexical_path):
        raise FeatureCaptureConfigurationError(f"{kind} is a reparse point")
    return lexical_path


def _load_policy(path: Path) -> tuple[dict[str, Any], bytes, str]:
    raw = path.read_bytes()
    try:
        policy = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FeatureCaptureConfigurationError(
            "Companyfacts policy is not valid UTF-8 JSON"
        ) from exc
    if not isinstance(policy, dict):
        raise FeatureCaptureConfigurationError("Companyfacts policy must be an object")
    required = {
        "schema_version": 1,
        "contract_id": FEATURE_CAPTURE_VERSION,
        "purpose": "capture_only_future_research",
        "current_trial_decision_use": "prohibited",
        "activation_boundary": "decision_at_utc_gte_feature_capture_activation",
    }
    for key, expected in required.items():
        if policy.get(key) != expected:
            raise FeatureCaptureConfigurationError(f"unexpected Companyfacts policy field: {key}")
    isolation = policy.get("isolation")
    if isolation != {
        "modifies_active_evidence_snapshot": False,
        "reads_trial_outcomes": False,
        "affects_enrollment": False,
        "affects_live_orders": False,
    }:
        raise FeatureCaptureConfigurationError("Companyfacts isolation policy is not fail-closed")
    if policy.get("maximum_request_lag_seconds") != 900 or policy.get("retry") != {
        "maximum_attempts": 3,
        "terminal_after_request_window": True,
    }:
        raise FeatureCaptureConfigurationError("Companyfacts timing or retry policy changed")
    if policy.get("selection") != {
        "as_of_field": "decision_at_utc",
        "filing_date_rule": "filed_date_strictly_before_as_of_utc_calendar_date",
        "period_end_rule": "period_end_lte_as_of_utc_calendar_date",
        "namespace_priority": [
            "dei:EntityCommonStockSharesOutstanding",
            "us-gaap:CommonStockSharesOutstanding",
        ],
        "unit": "shares",
        "value_rule": "finite_positive",
        "within_namespace_order": [
            "filed_desc",
            "period_end_desc",
            "accession_number_desc",
            "source_index_desc",
        ],
    }:
        raise FeatureCaptureConfigurationError("Companyfacts selection policy changed")
    if policy.get("missingness") != [
        "clock_before_decision",
        "request_window_expired",
        "request_failed",
        "artifact_publish_failed",
        "invalid_utf8",
        "invalid_json",
        "invalid_payload",
        "issuer_identity_mismatch",
        "no_eligible_fact",
    ]:
        raise FeatureCaptureConfigurationError("Companyfacts missingness policy changed")
    return policy, raw, _sha256(_canonical(policy))


def _ensure_store(
    config: FeatureCaptureConfig, *, policy_sha256: str, policy_bytes_sha256: str
) -> None:
    activation = _utc_text(config.activation_at_utc)
    selection_code_sha256 = _sha256(inspect.getsource(_select_shares).encode("utf-8"))
    configuration_record = {
        "contract_version": FEATURE_CAPTURE_VERSION,
        "activation_at_utc": activation,
        "policy_sha256": policy_sha256,
        "policy_bytes_sha256": policy_bytes_sha256,
        "selection_code_sha256": selection_code_sha256,
        "initial_runtime_git_commit": config.git_commit,
    }
    encoded = _canonical(configuration_record)
    record_sha = _sha256(encoded)
    with closing(_connect(config.feature_db, write=True)) as conn, conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS feature_capture_configuration (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                activation_at_utc TEXT NOT NULL,
                policy_sha256 TEXT NOT NULL,
                policy_bytes_sha256 TEXT NOT NULL,
                selection_code_sha256 TEXT NOT NULL,
                initial_runtime_git_commit TEXT NOT NULL,
                record_sha256 TEXT NOT NULL UNIQUE,
                record_json BLOB NOT NULL
            );
            CREATE TRIGGER IF NOT EXISTS feature_capture_configuration_no_update
            BEFORE UPDATE ON feature_capture_configuration
            BEGIN SELECT RAISE(ABORT, 'feature capture configuration is immutable'); END;
            CREATE TRIGGER IF NOT EXISTS feature_capture_configuration_no_delete
            BEFORE DELETE ON feature_capture_configuration
            BEGIN SELECT RAISE(ABORT, 'feature capture configuration is immutable'); END;

            CREATE TABLE IF NOT EXISTS feature_capture_jobs (
                job_id TEXT PRIMARY KEY,
                packet_id TEXT NOT NULL,
                accession_number TEXT NOT NULL,
                issuer_cik TEXT NOT NULL,
                decision_at_utc TEXT NOT NULL,
                source_first_observed_at_utc TEXT NOT NULL,
                source_job_sha256 TEXT NOT NULL UNIQUE,
                source_validation_error TEXT,
                status TEXT NOT NULL
                  CHECK(status IN ('pending','leased','retry','complete','failed')),
                attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count>=0),
                lease_owner TEXT,
                lease_expires_at_utc TEXT,
                last_error_kind TEXT,
                last_error_message TEXT
            );
            CREATE INDEX IF NOT EXISTS feature_capture_jobs_claim
            ON feature_capture_jobs(status,decision_at_utc,job_id);
            CREATE TRIGGER IF NOT EXISTS feature_capture_job_identity_immutable
            BEFORE UPDATE OF job_id,packet_id,accession_number,issuer_cik,decision_at_utc,
              source_first_observed_at_utc,source_job_sha256,source_validation_error
            ON feature_capture_jobs
            BEGIN SELECT RAISE(ABORT, 'feature capture job identity is immutable'); END;
            CREATE TRIGGER IF NOT EXISTS feature_capture_final_job_immutable
            BEFORE UPDATE ON feature_capture_jobs WHEN OLD.status IN ('complete','failed')
            BEGIN SELECT RAISE(ABORT, 'terminal feature capture job is immutable'); END;
            CREATE TRIGGER IF NOT EXISTS feature_capture_jobs_no_delete
            BEFORE DELETE ON feature_capture_jobs
            BEGIN SELECT RAISE(ABORT, 'feature capture jobs cannot be deleted'); END;

            CREATE TABLE IF NOT EXISTS feature_capture_attempts (
                sequence INTEGER PRIMARY KEY,
                attempt_id TEXT NOT NULL UNIQUE,
                job_id TEXT NOT NULL,
                attempt_number INTEGER NOT NULL CHECK(attempt_number>0),
                started_at_utc TEXT NOT NULL,
                finished_at_utc TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('retry','completed','failed')),
                error_kind TEXT,
                error_message TEXT,
                record_sha256 TEXT NOT NULL UNIQUE,
                record_json BLOB NOT NULL,
                UNIQUE(job_id,attempt_number)
            );
            CREATE TRIGGER IF NOT EXISTS feature_capture_attempts_sequence
            BEFORE INSERT ON feature_capture_attempts
            WHEN NEW.sequence<>(SELECT COALESCE(MAX(sequence),0)+1 FROM feature_capture_attempts)
            BEGIN SELECT RAISE(ABORT, 'feature capture attempt sequence must be gap-free'); END;
            CREATE TRIGGER IF NOT EXISTS feature_capture_attempts_no_update
            BEFORE UPDATE ON feature_capture_attempts
            BEGIN SELECT RAISE(ABORT, 'feature capture attempts are immutable'); END;
            CREATE TRIGGER IF NOT EXISTS feature_capture_attempts_no_delete
            BEFORE DELETE ON feature_capture_attempts
            BEGIN SELECT RAISE(ABORT, 'feature capture attempts are immutable'); END;

            CREATE TABLE IF NOT EXISTS companyfacts_receipts (
                sequence INTEGER PRIMARY KEY,
                receipt_id TEXT NOT NULL UNIQUE,
                job_id TEXT NOT NULL UNIQUE,
                packet_id TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL CHECK(status IN ('captured','missing')),
                missing_reason TEXT,
                raw_artifact_sha256 TEXT,
                receipt_sha256 TEXT NOT NULL UNIQUE,
                record_json BLOB NOT NULL,
                recorded_at_utc TEXT NOT NULL
            );
            CREATE TRIGGER IF NOT EXISTS companyfacts_receipts_sequence
            BEFORE INSERT ON companyfacts_receipts
            WHEN NEW.sequence<>(SELECT COALESCE(MAX(sequence),0)+1 FROM companyfacts_receipts)
            BEGIN SELECT RAISE(ABORT, 'Companyfacts receipt sequence must be gap-free'); END;
            CREATE TRIGGER IF NOT EXISTS companyfacts_receipts_no_update
            BEFORE UPDATE ON companyfacts_receipts
            BEGIN SELECT RAISE(ABORT, 'Companyfacts receipts are immutable'); END;
            CREATE TRIGGER IF NOT EXISTS companyfacts_receipts_no_delete
            BEFORE DELETE ON companyfacts_receipts
            BEGIN SELECT RAISE(ABORT, 'Companyfacts receipts are immutable'); END;

            CREATE TABLE IF NOT EXISTS feature_capture_health (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                last_worker_heartbeat_utc TEXT NOT NULL,
                last_result TEXT NOT NULL,
                last_job_id TEXT,
                last_error_kind TEXT,
                last_error_message TEXT
            );
            """
        )
        existing = conn.execute(
            "SELECT * FROM feature_capture_configuration WHERE singleton=1"
        ).fetchone()
        if existing is None:
            conn.execute(
                "INSERT INTO feature_capture_configuration VALUES(1,?,?,?,?,?,?,?)",
                (
                    activation,
                    policy_sha256,
                    policy_bytes_sha256,
                    selection_code_sha256,
                    config.git_commit,
                    record_sha,
                    encoded,
                ),
            )
        elif (
            str(existing["activation_at_utc"]) != activation
            or str(existing["policy_sha256"]) != policy_sha256
            or str(existing["policy_bytes_sha256"]) != policy_bytes_sha256
            or str(existing["selection_code_sha256"]) != selection_code_sha256
        ):
            raise FeatureCaptureConfigurationError(
                "runtime arguments do not match the immutable feature capture configuration"
            )


def _publish(root: Path, data: bytes, *, suffix: str) -> tuple[Path, str]:
    digest = _sha256(data)
    root.mkdir(parents=True, exist_ok=True)
    destination = root / f"{digest}{suffix}"
    if destination.exists():
        if destination.read_bytes() != data:
            raise RuntimeError(f"content-address collision at {destination}")
        return destination, digest
    staging = root / f".{digest}.{os.getpid()}.tmp"
    with staging.open("xb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        os.link(staging, destination)
    except FileExistsError as exc:
        if destination.read_bytes() != data:
            raise RuntimeError(f"content-address collision at {destination}") from exc
    finally:
        staging.unlink(missing_ok=True)
    return destination, digest


def _source_job_record(row: sqlite3.Row) -> tuple[dict[str, Any], str]:
    record = {
        "job_id": str(row["job_id"]),
        "packet_id": str(row["packet_id"]),
        "accession_number": str(row["accession_number"]),
        "issuer_cik": str(row["issuer_cik"]),
        "form_type": str(row["form_type"]),
        "contract_version": str(row["contract_version"]),
        "payload_json": str(row["payload_json"]),
        "decision_json": str(row["decision_json"]),
        "source_first_observed_at_utc": _utc_text(
            _parse_utc(str(row["source_first_observed_at_utc"]))
        ),
        "decision_at_utc": _utc_text(_parse_utc(str(row["decision_at_utc"]))),
        "created_at_utc": _utc_text(_parse_utc(str(row["created_at_utc"]))),
    }
    return record, _sha256(_canonical(record))


def _source_validation_error(record: Mapping[str, Any]) -> str | None:
    try:
        payload = json.loads(str(record["payload_json"]))
    except json.JSONDecodeError:
        return "source_payload_invalid_json"
    if not isinstance(payload, dict):
        return "source_payload_not_object"
    row_cik = _normalized_cik(record["issuer_cik"])
    payload_cik = _normalized_cik(payload.get("issuer_cik"))
    if row_cik is None or payload_cik is None or row_cik != payload_cik:
        return "source_issuer_identity_mismatch"
    observed = _parse_utc(str(record["source_first_observed_at_utc"]))
    decision = _parse_utc(str(record["decision_at_utc"]))
    if observed > decision:
        return "source_timestamps_out_of_order"
    return None


def _ingest_next_job(config: FeatureCaptureConfig) -> None:
    if not config.source_db.is_file():
        return
    with closing(_connect(config.source_db, write=False)) as source:
        exists = source.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='research_capture_jobs'"
        ).fetchone()
        if exists is None:
            return
        rows = source.execute(
            """
            SELECT job_id,packet_id,contract_version,accession_number,issuer_cik,form_type,
                   payload_json,
                   decision_json,source_first_observed_at_utc,decision_at_utc,created_at_utc
            FROM research_capture_jobs
            WHERE julianday(decision_at_utc)>=julianday(?)
            ORDER BY decision_at_utc,job_id
            """,
            (_utc_text(config.activation_at_utc),),
        ).fetchall()
    with closing(_connect(config.feature_db, write=True)) as conn, conn:
        for row in rows:
            record, source_sha = _source_job_record(row)
            if _parse_utc(str(record["decision_at_utc"])) < config.activation_at_utc:
                continue
            existing = conn.execute(
                "SELECT source_job_sha256 FROM feature_capture_jobs WHERE job_id=?",
                (record["job_id"],),
            ).fetchone()
            if existing is not None:
                if str(existing["source_job_sha256"]) != source_sha:
                    raise RuntimeError("source research job changed after feature admission")
                continue
            conn.execute(
                """
                INSERT INTO feature_capture_jobs(
                  job_id,packet_id,accession_number,issuer_cik,decision_at_utc,
                  source_first_observed_at_utc,source_job_sha256,source_validation_error,status
                ) VALUES(?,?,?,?,?,?,?,?,'pending')
                """,
                (
                    record["job_id"],
                    record["packet_id"],
                    record["accession_number"],
                    record["issuer_cik"],
                    record["decision_at_utc"],
                    record["source_first_observed_at_utc"],
                    source_sha,
                    _source_validation_error(record),
                ),
            )
            break


def _claim(config: FeatureCaptureConfig, *, now: datetime) -> _Job | None:
    worker_id = str(uuid.uuid4())
    with closing(_connect(config.feature_db, write=True)) as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT * FROM feature_capture_jobs
            WHERE status IN ('pending','retry')
               OR (status='leased' AND lease_expires_at_utc<=?)
            ORDER BY decision_at_utc,job_id LIMIT 1
            """,
            (_utc_text(now),),
        ).fetchone()
        if row is None:
            conn.commit()
            return None
        attempt = int(row["attempt_count"]) + 1
        conn.execute(
            """
            UPDATE feature_capture_jobs SET status='leased',attempt_count=?,lease_owner=?,
              lease_expires_at_utc=?,last_error_kind=NULL,last_error_message=NULL
            WHERE job_id=?
            """,
            (
                attempt,
                worker_id,
                _utc_text(now + timedelta(seconds=config.lease_seconds)),
                str(row["job_id"]),
            ),
        )
        conn.commit()
    return _Job(
        job_id=str(row["job_id"]),
        packet_id=str(row["packet_id"]),
        accession_number=str(row["accession_number"]),
        issuer_cik=str(row["issuer_cik"]),
        decision_at_utc=_parse_utc(str(row["decision_at_utc"])),
        source_first_observed_at_utc=_parse_utc(str(row["source_first_observed_at_utc"])),
        source_job_sha256=str(row["source_job_sha256"]),
        source_validation_error=(
            str(row["source_validation_error"])
            if row["source_validation_error"] is not None
            else None
        ),
        attempt_number=attempt,
    )


def _normalized_cik(value: object) -> str | None:
    candidate = str(value).strip()
    if candidate.upper().startswith("CIK"):
        candidate = candidate[3:]
    if not candidate.isdigit() or len(candidate) > 10:
        return None
    return candidate.zfill(10)


def _select_shares(payload: Mapping[str, Any], *, as_of: datetime) -> dict[str, Any]:
    facts = payload.get("facts")
    candidate_receipts: list[dict[str, Any]] = []
    eligible_by_namespace: dict[int, list[tuple[tuple[Any, ...], dict[str, Any]]]] = {}
    if isinstance(facts, dict):
        for priority, (namespace, fact_name) in enumerate(_FACTS):
            namespace_value = facts.get(namespace)
            fact = namespace_value.get(fact_name) if isinstance(namespace_value, dict) else None
            units = fact.get("units") if isinstance(fact, dict) else None
            observations = units.get("shares") if isinstance(units, dict) else None
            if not isinstance(observations, list):
                continue
            for index, raw in enumerate(observations):
                candidate: dict[str, Any] = {
                    "namespace": namespace,
                    "fact": fact_name,
                    "unit": "shares",
                    "source_index": index,
                }
                reason: str | None = None
                filed: date | None = None
                period_end: date | None = None
                value: float | None = None
                if not isinstance(raw, dict):
                    reason = "observation_not_object"
                else:
                    for field in ("filed", "end", "accn", "form", "fp", "frame"):
                        if field in raw and raw[field] is not None:
                            candidate[field] = str(raw[field])
                    if "fy" in raw:
                        raw_fy = raw["fy"]
                        if (
                            isinstance(raw_fy, (int, float))
                            and not isinstance(raw_fy, bool)
                            and math.isfinite(raw_fy)
                        ):
                            candidate["fy"] = raw_fy
                        else:
                            candidate["fy_text"] = str(raw_fy)
                    try:
                        filed = date.fromisoformat(str(raw["filed"]))
                        period_end = date.fromisoformat(str(raw["end"]))
                        if isinstance(raw["val"], bool):
                            raise ValueError("boolean value")
                        value = float(raw["val"])
                        if math.isfinite(value):
                            candidate["value"] = value
                        else:
                            candidate["value_text"] = str(raw["val"])
                    except (KeyError, TypeError, ValueError):
                        reason = "invalid_required_field"
                if reason is None and filed is not None and filed >= as_of.date():
                    reason = "filed_not_strictly_before_cutoff_date"
                if reason is None and period_end is not None and period_end > as_of.date():
                    reason = "period_end_after_cutoff_date"
                if reason is None and (value is None or not math.isfinite(value) or value <= 0):
                    reason = "value_not_finite_positive"
                candidate["eligible"] = reason is None
                candidate["rejection_reason"] = reason
                candidate_receipts.append(candidate)
                if reason is None:
                    assert filed is not None and period_end is not None and value is not None
                    order = (
                        filed,
                        period_end,
                        str(raw.get("accn", "")),
                        index,
                    )
                    eligible_by_namespace.setdefault(priority, []).append((order, candidate))
    selected: dict[str, Any] | None = None
    for priority in range(len(_FACTS)):
        eligible = eligible_by_namespace.get(priority, [])
        if eligible:
            selected = dict(max(eligible, key=lambda item: item[0])[1])
            break
    return {
        "status": "selected" if selected is not None else "no_eligible_fact",
        "as_of_utc": _utc_text(as_of),
        "selected": selected,
        "candidate_count": len(candidate_receipts),
        "candidates": candidate_receipts,
    }


def _attempt_record(
    job: _Job,
    *,
    started: datetime,
    finished: datetime,
    status: str,
    error_kind: str | None,
    error_message: str | None,
) -> tuple[bytes, str]:
    record = {
        "contract_version": FEATURE_CAPTURE_VERSION,
        "attempt_id": str(
            uuid.uuid5(uuid.NAMESPACE_URL, f"{job.job_id}|companyfacts|{job.attempt_number}")
        ),
        "job_id": job.job_id,
        "attempt_number": job.attempt_number,
        "started_at_utc": _utc_text(started),
        "finished_at_utc": _utc_text(finished),
        "status": status,
        "error_kind": error_kind,
        "error_message": error_message,
    }
    encoded = _canonical(record)
    return encoded, _sha256(encoded)


def _finalize(
    config: FeatureCaptureConfig,
    job: _Job,
    *,
    policy_sha256: str,
    started: datetime,
    finished: datetime,
    status: Literal["captured", "missing"],
    missing_reason: str | None,
    request: dict[str, Any] | None,
    response: dict[str, Any] | None,
    raw_artifact_sha256: str | None,
    raw_artifact_ref: str | None,
    selection: dict[str, Any] | None,
    attempt_error: str | None = None,
) -> str:
    receipt_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{job.job_id}|companyfacts"))
    record = {
        "schema_version": 1,
        "contract_version": FEATURE_CAPTURE_VERSION,
        "receipt_id": receipt_id,
        "job_id": job.job_id,
        "packet_id": job.packet_id,
        "accession_number": job.accession_number,
        "issuer_cik_10": _normalized_cik(job.issuer_cik),
        "source_first_observed_at_utc": _utc_text(job.source_first_observed_at_utc),
        "decision_at_utc": _utc_text(job.decision_at_utc),
        "recorded_at_utc": _utc_text(finished),
        "status": status,
        "missing_reason": missing_reason,
        "request": request,
        "response": response,
        "raw_artifact_sha256": raw_artifact_sha256,
        "raw_artifact_ref": raw_artifact_ref,
        "selection": selection,
        "provenance": {
            "source_job_sha256": job.source_job_sha256,
            "policy_sha256": policy_sha256,
            "runtime_git_commit": config.git_commit,
            "attempt_number": job.attempt_number,
        },
    }
    encoded = _canonical(record)
    receipt_sha = _sha256(encoded)
    receipt_path, published_sha = _publish(
        config.artifact_root / "receipts", encoded, suffix=".json"
    )
    if published_sha != receipt_sha or not receipt_path.is_file():
        raise RuntimeError("Companyfacts receipt publish failed integrity validation")
    attempt_status = "completed" if status == "captured" else "failed"
    attempt_bytes, attempt_sha = _attempt_record(
        job,
        started=started,
        finished=finished,
        status=attempt_status,
        error_kind=missing_reason,
        error_message=attempt_error,
    )
    attempt = json.loads(attempt_bytes)
    with closing(_connect(config.feature_db, write=True)) as conn:
        conn.execute("BEGIN IMMEDIATE")
        attempt_sequence = int(
            conn.execute(
                "SELECT COALESCE(MAX(sequence),0)+1 FROM feature_capture_attempts"
            ).fetchone()[0]
        )
        conn.execute(
            "INSERT INTO feature_capture_attempts VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                attempt_sequence,
                attempt["attempt_id"],
                job.job_id,
                job.attempt_number,
                attempt["started_at_utc"],
                attempt["finished_at_utc"],
                attempt_status,
                missing_reason,
                attempt_error,
                attempt_sha,
                attempt_bytes,
            ),
        )
        receipt_sequence = int(
            conn.execute(
                "SELECT COALESCE(MAX(sequence),0)+1 FROM companyfacts_receipts"
            ).fetchone()[0]
        )
        conn.execute(
            "INSERT INTO companyfacts_receipts VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                receipt_sequence,
                receipt_id,
                job.job_id,
                job.packet_id,
                status,
                missing_reason,
                raw_artifact_sha256,
                receipt_sha,
                encoded,
                _utc_text(finished),
            ),
        )
        conn.execute(
            """
            UPDATE feature_capture_jobs SET status=?,lease_owner=NULL,lease_expires_at_utc=NULL,
              last_error_kind=?,last_error_message=? WHERE job_id=?
            """,
            (
                "complete",
                missing_reason,
                attempt_error,
                job.job_id,
            ),
        )
        conn.commit()
    return receipt_sha


def _retry(
    config: FeatureCaptureConfig,
    job: _Job,
    *,
    started: datetime,
    finished: datetime,
    error_kind: str,
    error_message: str,
) -> None:
    attempt_bytes, attempt_sha = _attempt_record(
        job,
        started=started,
        finished=finished,
        status="retry",
        error_kind=error_kind,
        error_message=error_message,
    )
    attempt = json.loads(attempt_bytes)
    with closing(_connect(config.feature_db, write=True)) as conn:
        conn.execute("BEGIN IMMEDIATE")
        sequence = int(
            conn.execute(
                "SELECT COALESCE(MAX(sequence),0)+1 FROM feature_capture_attempts"
            ).fetchone()[0]
        )
        conn.execute(
            "INSERT INTO feature_capture_attempts VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                sequence,
                attempt["attempt_id"],
                job.job_id,
                job.attempt_number,
                attempt["started_at_utc"],
                attempt["finished_at_utc"],
                "retry",
                error_kind,
                error_message,
                attempt_sha,
                attempt_bytes,
            ),
        )
        conn.execute(
            """
            UPDATE feature_capture_jobs SET status='retry',lease_owner=NULL,
              lease_expires_at_utc=NULL,last_error_kind=?,last_error_message=? WHERE job_id=?
            """,
            (error_kind, error_message[:2000], job.job_id),
        )
        conn.commit()


def _write_health(
    config: FeatureCaptureConfig,
    *,
    now: datetime,
    result: str,
    job_id: str | None,
    error_kind: str | None = None,
    error_message: str | None = None,
) -> None:
    with closing(_connect(config.feature_db, write=True)) as conn, conn:
        conn.execute(
            """
            INSERT INTO feature_capture_health VALUES(1,?,?,?,?,?)
            ON CONFLICT(singleton) DO UPDATE SET
              last_worker_heartbeat_utc=excluded.last_worker_heartbeat_utc,
              last_result=excluded.last_result,last_job_id=excluded.last_job_id,
              last_error_kind=excluded.last_error_kind,
              last_error_message=excluded.last_error_message
            """,
            (
                _utc_text(now),
                result,
                job_id,
                error_kind,
                error_message[:2000] if error_message else None,
            ),
        )


def run_feature_capture_once(
    config: FeatureCaptureConfig,
    *,
    client: ResourceClient,
    now_fn: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> FeatureCaptureResult:
    """Capture at most one source job without consulting trial or broker state."""

    initialize_feature_capture(config)
    policy, _, policy_sha = _load_policy(config.policy_path)
    now = _now(now_fn)
    _ingest_next_job(config)
    job = _claim(config, now=now)
    if job is None:
        _write_health(config, now=now, result="idle", job_id=None)
        return FeatureCaptureResult(status="idle")
    max_lag = int(policy["maximum_request_lag_seconds"])
    max_attempts = int(policy["retry"]["maximum_attempts"])
    deadline = job.decision_at_utc + timedelta(seconds=max_lag)
    started = _now(now_fn)
    if started < job.decision_at_utc or started > deadline:
        reason = (
            "clock_before_decision"
            if started < job.decision_at_utc
            else "request_window_expired"
        )
        receipt_sha = _finalize(
            config,
            job,
            policy_sha256=policy_sha,
            started=started,
            finished=started,
            status="missing",
            missing_reason=reason,
            request=None,
            response=None,
            raw_artifact_sha256=None,
            raw_artifact_ref=None,
            selection=None,
        )
        _write_health(
            config, now=started, result="completed", job_id=job.job_id, error_kind=reason
        )
        return FeatureCaptureResult(
            status="completed",
            job_id=job.job_id,
            receipt_sha256=receipt_sha,
            missing_reason=reason,
        )
    cik = _normalized_cik(job.issuer_cik)
    if cik is None or job.source_validation_error is not None:
        receipt_sha = _finalize(
            config,
            job,
            policy_sha256=policy_sha,
            started=started,
            finished=started,
            status="missing",
            missing_reason="invalid_payload",
            request=None,
            response=None,
            raw_artifact_sha256=None,
            raw_artifact_ref=None,
            selection=None,
            attempt_error=job.source_validation_error,
        )
        _write_health(
            config,
            now=started,
            result="completed",
            job_id=job.job_id,
            error_kind="invalid_payload",
        )
        return FeatureCaptureResult(
            status="completed",
            job_id=job.job_id,
            receipt_sha256=receipt_sha,
            missing_reason="invalid_payload",
        )
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    request = {"url": url, "started_at_utc": _utc_text(started), "method": "GET"}
    try:
        resource = client.get_resource(url)
    except SecHttpError as exc:
        finished = _now(now_fn)
        if finished < started:
            raise ValueError(
                "feature capture clock moved backwards during SEC request"
            ) from exc
        message = str(exc)[:2000]
        if job.attempt_number < max_attempts and finished <= deadline:
            _retry(
                config,
                job,
                started=started,
                finished=finished,
                error_kind="request_failed",
                error_message=message,
            )
            _write_health(
                config,
                now=finished,
                result="retry_scheduled",
                job_id=job.job_id,
                error_kind="request_failed",
                error_message=message,
            )
            return FeatureCaptureResult(status="retry_scheduled", job_id=job.job_id)
        receipt_sha = _finalize(
            config,
            job,
            policy_sha256=policy_sha,
            started=started,
            finished=finished,
            status="missing",
            missing_reason="request_failed",
            request=request,
            response=None,
            raw_artifact_sha256=None,
            raw_artifact_ref=None,
            selection=None,
            attempt_error=message,
        )
        _write_health(
            config,
            now=finished,
            result="completed",
            job_id=job.job_id,
            error_kind="request_failed",
            error_message=message,
        )
        return FeatureCaptureResult(
            status="completed",
            job_id=job.job_id,
            receipt_sha256=receipt_sha,
            missing_reason="request_failed",
        )
    finished = _now(now_fn)
    if finished < started:
        raise ValueError("feature capture clock moved backwards during SEC request")
    try:
        raw_path, raw_sha = _publish(
            config.artifact_root / "raw", resource.content, suffix=".bin"
        )
    except OSError as exc:
        message = str(exc)[:2000]
        if job.attempt_number < max_attempts and finished <= deadline:
            _retry(
                config,
                job,
                started=started,
                finished=finished,
                error_kind="artifact_publish_failed",
                error_message=message,
            )
            _write_health(
                config,
                now=finished,
                result="retry_scheduled",
                job_id=job.job_id,
                error_kind="artifact_publish_failed",
                error_message=message,
            )
            return FeatureCaptureResult(status="retry_scheduled", job_id=job.job_id)
        receipt_sha = _finalize(
            config,
            job,
            policy_sha256=policy_sha,
            started=started,
            finished=finished,
            status="missing",
            missing_reason="artifact_publish_failed",
            request=request,
            response=_response_metadata(resource, finished=finished),
            raw_artifact_sha256=None,
            raw_artifact_ref=None,
            selection=None,
            attempt_error=message,
        )
        _write_health(
            config,
            now=finished,
            result="completed",
            job_id=job.job_id,
            error_kind="artifact_publish_failed",
            error_message=message,
        )
        return FeatureCaptureResult(
            status="completed",
            job_id=job.job_id,
            receipt_sha256=receipt_sha,
            missing_reason="artifact_publish_failed",
        )
    raw_ref = raw_path.relative_to(config.artifact_root).as_posix()
    response = _response_metadata(resource, finished=finished)
    missing_reason: str | None = None
    selection: dict[str, Any] | None = None
    try:
        decoded = resource.content.decode("utf-8")
    except UnicodeDecodeError:
        missing_reason = "invalid_utf8"
    else:
        try:
            parsed = json.loads(decoded)
        except json.JSONDecodeError:
            missing_reason = "invalid_json"
        else:
            if not isinstance(parsed, dict):
                missing_reason = "invalid_payload"
            elif _normalized_cik(parsed.get("cik")) != cik:
                missing_reason = "issuer_identity_mismatch"
            else:
                selection = _select_shares(parsed, as_of=job.decision_at_utc)
                if selection["status"] == "no_eligible_fact":
                    missing_reason = "no_eligible_fact"
    receipt_sha = _finalize(
        config,
        job,
        policy_sha256=policy_sha,
        started=started,
        finished=finished,
        status="captured" if missing_reason is None else "missing",
        missing_reason=missing_reason,
        request=request,
        response=response,
        raw_artifact_sha256=raw_sha,
        raw_artifact_ref=raw_ref,
        selection=selection,
    )
    _write_health(
        config,
        now=finished,
        result="completed",
        job_id=job.job_id,
        error_kind=missing_reason,
    )
    return FeatureCaptureResult(
        status="completed",
        job_id=job.job_id,
        receipt_sha256=receipt_sha,
        missing_reason=missing_reason,
    )


def initialize_feature_capture(config: FeatureCaptureConfig) -> dict[str, str]:
    """Validate confinement and seal or verify the immutable stream configuration."""

    feature_db = _confined_path(
        config.feature_db, research_root=config.research_root, kind="feature database"
    )
    artifact_root = _confined_path(
        config.artifact_root, research_root=config.research_root, kind="feature artifact root"
    )
    if feature_db != Path(os.path.abspath(config.feature_db)):
        raise FeatureCaptureConfigurationError("feature database normalization failed")
    if artifact_root != Path(os.path.abspath(config.artifact_root)):
        raise FeatureCaptureConfigurationError("feature artifact normalization failed")
    policy, policy_bytes, policy_sha = _load_policy(config.policy_path)
    _ensure_store(config, policy_sha256=policy_sha, policy_bytes_sha256=_sha256(policy_bytes))
    return {
        "activation_at_utc": _utc_text(config.activation_at_utc),
        "policy_sha256": policy_sha,
        "contract_version": str(policy["contract_id"]),
    }


def _response_metadata(resource: SecResource, *, finished: datetime) -> dict[str, Any]:
    return {
        "finished_at_utc": _utc_text(finished),
        "status_code": resource.status_code,
        "final_url": resource.final_url,
        "content_type": resource.content_type,
        "etag": resource.etag,
        "last_modified": resource.last_modified,
        "upstream_digest": resource.upstream_digest,
        "content_length_bytes": len(resource.content),
    }


def feature_capture_status(
    feature_db: Path, *, artifact_root: Path | None = None
) -> dict[str, Any]:
    if not feature_db.is_file():
        return {"valid": False, "reason": "feature_store_missing"}
    with closing(_connect(feature_db, write=False)) as conn:
        configuration = conn.execute(
            "SELECT * FROM feature_capture_configuration WHERE singleton=1"
        ).fetchone()
        health = conn.execute(
            "SELECT * FROM feature_capture_health WHERE singleton=1"
        ).fetchone()
        jobs = {
            str(row["status"]): int(row["count"])
            for row in conn.execute(
                "SELECT status,COUNT(*) count FROM feature_capture_jobs GROUP BY status"
            ).fetchall()
        }
        receipts = int(conn.execute("SELECT COUNT(*) FROM companyfacts_receipts").fetchone()[0])
        receipt_rows = conn.execute(
            "SELECT receipt_sha256,record_json FROM companyfacts_receipts ORDER BY sequence"
        ).fetchall()
    integrity_errors: list[str] = []
    if configuration is not None:
        configuration_bytes = bytes(configuration["record_json"])
        if _sha256(configuration_bytes) != str(configuration["record_sha256"]):
            integrity_errors.append("configuration_digest_mismatch")
    for row in receipt_rows:
        receipt_bytes = bytes(row["record_json"])
        receipt_sha = str(row["receipt_sha256"])
        if _sha256(receipt_bytes) != receipt_sha:
            integrity_errors.append(f"receipt_digest_mismatch:{receipt_sha}")
            continue
        try:
            receipt = json.loads(receipt_bytes)
        except json.JSONDecodeError:
            integrity_errors.append(f"receipt_invalid_json:{receipt_sha}")
            continue
        if not isinstance(receipt, dict):
            integrity_errors.append(f"receipt_not_object:{receipt_sha}")
            continue
        if artifact_root is not None and receipt.get("raw_artifact_sha256") is not None:
            raw_ref = receipt.get("raw_artifact_ref")
            if not isinstance(raw_ref, str):
                integrity_errors.append(f"raw_artifact_ref_missing:{receipt_sha}")
                continue
            relative = Path(raw_ref)
            raw_path = artifact_root / relative
            if relative.is_absolute() or ".." in relative.parts:
                integrity_errors.append(f"raw_artifact_ref_escaped:{receipt_sha}")
                continue
            if (
                not raw_path.is_file()
                or _sha256(raw_path.read_bytes()) != receipt["raw_artifact_sha256"]
            ):
                integrity_errors.append(f"raw_artifact_invalid:{receipt_sha}")
    return {
        "valid": configuration is not None and not integrity_errors,
        "activation_at_utc": str(configuration["activation_at_utc"]) if configuration else None,
        "policy_sha256": str(configuration["policy_sha256"]) if configuration else None,
        "health": dict(health) if health else None,
        "jobs": jobs,
        "receipts": receipts,
        "integrity_errors": integrity_errors,
    }
