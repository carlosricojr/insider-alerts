from __future__ import annotations

import hashlib
import json
import os
import platform
import sqlite3
import subprocess
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import monotonic
from typing import Any, Literal

import rfc8785
from jsonschema import Draft202012Validator, FormatChecker, ValidationError

CAPTURE_CONTRACT_VERSION = "insider-evidence-capture-v2"
HYPOTHESIS_ID = "OPP-E07-V1"
MAX_ERROR_LENGTH = 2_000


@dataclass(slots=True, frozen=True)
class CaptureConfig:
    source_db: Path
    evidence_db: Path
    artifact_root: Path
    alpha_python: Path
    alpha_script: Path
    canary_ledger: Path
    insider_git_commit: str
    policy_path: Path
    evidence_schema_path: Path
    capture_delay_seconds: int = 20
    capture_deadline_seconds: int = 600
    option_timeout_seconds: int = 90
    max_attempts: int = 3
    lease_seconds: int = 180

    def __post_init__(self) -> None:
        if len(self.insider_git_commit) != 40 or any(
            char not in "0123456789abcdef" for char in self.insider_git_commit
        ):
            raise ValueError("insider_git_commit must be a lowercase 40-character SHA-1")
        for name in (
            "capture_delay_seconds",
            "capture_deadline_seconds",
            "option_timeout_seconds",
            "max_attempts",
            "lease_seconds",
        ):
            if int(getattr(self, name)) < 1:
                raise ValueError(f"{name} must be positive")
        if self.capture_delay_seconds >= self.capture_deadline_seconds:
            raise ValueError("capture delay must be shorter than capture deadline")
        if self.lease_seconds <= self.option_timeout_seconds + 30:
            raise ValueError("lease must outlive the option timeout by more than 30 seconds")
        if self.capture_deadline_seconds <= (
            self.capture_delay_seconds + self.max_attempts * self.option_timeout_seconds
        ):
            raise ValueError("capture deadline cannot accommodate every bounded option attempt")


@dataclass(slots=True, frozen=True)
class CaptureJob:
    job_id: str
    packet_id: str
    accession_number: str
    issuer_cik: str
    form_type: str
    payload_json: str
    decision_json: str
    source_first_observed_at: datetime
    decision_at: datetime
    attempt_count: int


@dataclass(slots=True, frozen=True)
class CaptureResult:
    status: Literal["idle", "retry_scheduled", "completed", "failed"]
    job_id: str | None = None
    snapshot_sha256: str | None = None
    option_status: str | None = None


@dataclass(slots=True, frozen=True)
class ProcessResult:
    returncode: int
    stdout: str
    stderr: str
    timed_out: bool


def utc_text(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("UTC timestamp cannot be naive")
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"naive persisted timestamp: {value}")
    return parsed.astimezone(UTC)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def research_source_fingerprint(package_root: Path | None = None) -> str:
    root = package_root or Path(__file__).resolve().parents[1]
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*.py"), key=lambda item: item.relative_to(root).as_posix()):
        digest.update(path.relative_to(root).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _connect(path: Path, *, write: bool) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    if write:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def ensure_evidence_store(path: Path) -> None:
    with _connect(path, write=True) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS evidence_snapshots (
                sequence INTEGER PRIMARY KEY,
                snapshot_id TEXT NOT NULL UNIQUE,
                job_id TEXT NOT NULL UNIQUE,
                record_sha256 TEXT NOT NULL UNIQUE,
                stored_bytes_sha256 TEXT NOT NULL,
                record_json BLOB NOT NULL,
                recorded_at_utc TEXT NOT NULL
            );
            CREATE TRIGGER IF NOT EXISTS evidence_snapshots_no_update
            BEFORE UPDATE ON evidence_snapshots
            BEGIN SELECT RAISE(ABORT, 'evidence snapshots are append-only'); END;
            CREATE TRIGGER IF NOT EXISTS evidence_snapshots_no_delete
            BEFORE DELETE ON evidence_snapshots
            BEGIN SELECT RAISE(ABORT, 'evidence snapshots are append-only'); END;
            CREATE TABLE IF NOT EXISTS capture_health (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                last_worker_heartbeat_utc TEXT NOT NULL,
                last_result TEXT NOT NULL,
                last_job_id TEXT
            );
            """
        )


def _claim_job(config: CaptureConfig, *, worker_id: str, now: datetime) -> CaptureJob | None:
    with _connect(config.source_db, write=True) as conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """
            UPDATE research_capture_jobs
            SET status='failed', lease_owner=NULL, lease_expires_at_utc=NULL,
                last_error_kind='CAPTURE_ATTEMPTS_EXHAUSTED',
                last_error_message='capture attempts exhausted before finalization',
                updated_at_utc=?
            WHERE attempt_count >= ? AND (
                status IN ('pending','retry')
            )
            """,
            (utc_text(now), config.max_attempts),
        )
        rows = conn.execute(
            """
            SELECT * FROM research_capture_jobs
            WHERE (status IN ('pending', 'retry') AND attempt_count < ?)
               OR (status = 'leased' AND attempt_count <= ? AND lease_expires_at_utc <= ?)
            ORDER BY decision_at_utc, job_id
            LIMIT 100
            """,
            (config.max_attempts, config.max_attempts, utc_text(now)),
        ).fetchall()
        selected: sqlite3.Row | None = None
        for row in rows:
            decision_at = parse_utc(str(row["decision_at_utc"]))
            if now >= decision_at + timedelta(seconds=config.capture_delay_seconds):
                selected = row
                break
        if selected is None:
            conn.commit()
            return None
        previous_attempt_count = int(selected["attempt_count"])
        attempt_count = (
            previous_attempt_count
            if str(selected["status"]) == "leased"
            else previous_attempt_count + 1
        )
        cursor = conn.execute(
            """
            UPDATE research_capture_jobs
            SET status='leased', attempt_count=?, lease_owner=?, lease_expires_at_utc=?,
                updated_at_utc=?
            WHERE job_id=? AND (
                status IN ('pending', 'retry')
                OR (status='leased' AND lease_expires_at_utc <= ?)
            )
            """,
            (
                attempt_count,
                worker_id,
                utc_text(now + timedelta(seconds=config.lease_seconds)),
                utc_text(now),
                str(selected["job_id"]),
                utc_text(now),
            ),
        )
        if cursor.rowcount != 1:
            conn.rollback()
            return None
        conn.commit()
        return CaptureJob(
            job_id=str(selected["job_id"]),
            packet_id=str(selected["packet_id"]),
            accession_number=str(selected["accession_number"]),
            issuer_cik=str(selected["issuer_cik"]),
            form_type=str(selected["form_type"]),
            payload_json=str(selected["payload_json"]),
            decision_json=str(selected["decision_json"]),
            source_first_observed_at=parse_utc(str(selected["source_first_observed_at_utc"])),
            decision_at=parse_utc(str(selected["decision_at_utc"])),
            attempt_count=attempt_count,
        )


def _record_attempt(
    config: CaptureConfig,
    job: CaptureJob,
    *,
    started_at: datetime,
    finished_at: datetime,
    status: str,
    error_kind: str | None,
    error_message: str | None,
    retryable: bool,
) -> None:
    with _connect(config.source_db, write=True) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO research_capture_attempts(
                job_id, attempt_number, started_at_utc, finished_at_utc, status,
                error_kind, error_message, retryable
            ) VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                job.job_id,
                job.attempt_count,
                utc_text(started_at),
                utc_text(finished_at),
                status,
                error_kind,
                error_message[:MAX_ERROR_LENGTH] if error_message else None,
                int(retryable),
            ),
        )


def _set_job_state(
    config: CaptureConfig,
    job: CaptureJob,
    *,
    state: str,
    now: datetime,
    error_kind: str | None = None,
    error_message: str | None = None,
    record_sha256: str | None = None,
) -> None:
    with _connect(config.source_db, write=True) as conn:
        cursor = conn.execute(
            """
            UPDATE research_capture_jobs
            SET status=?, lease_owner=NULL, lease_expires_at_utc=NULL, updated_at_utc=?,
                last_error_kind=?, last_error_message=?, record_sha256=?
            WHERE job_id=? AND status='leased'
            """,
            (
                state,
                utc_text(now),
                error_kind,
                error_message[:MAX_ERROR_LENGTH] if error_message else None,
                record_sha256,
                job.job_id,
            ),
        )
        if cursor.rowcount != 1:
            raise RuntimeError(f"lost lease for capture job {job.job_id}")


def _kill_process_tree(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    if os.name == "nt":
        subprocess.run(
            ["taskkill.exe", "/PID", str(process.pid), "/T", "/F"],
            capture_output=True,
            text=True,
            check=False,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    if process.poll() is None:
        process.kill()
    process.wait(timeout=10)


def run_hidden_process(command: list[str], *, cwd: Path, timeout_seconds: int) -> ProcessResult:
    flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    if os.name == "nt":
        flags |= getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    process = subprocess.Popen(
        command,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        creationflags=flags,
    )
    try:
        stdout, stderr = process.communicate(timeout=timeout_seconds)
        return ProcessResult(process.returncode, stdout, stderr, False)
    except subprocess.TimeoutExpired:
        _kill_process_tree(process)
        stdout, stderr = process.communicate()
        return ProcessResult(process.returncode or -1, stdout, stderr, True)


def _publish_content_addressed(root: Path, data: bytes, *, suffix: str) -> tuple[Path, str]:
    digest = sha256_bytes(data)
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


def _optional_string(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _optional_sha256(value: Any) -> str | None:
    candidate = _optional_string(value)
    if candidate is None or len(candidate) != 64:
        return None
    return candidate if all(char in "0123456789abcdef" for char in candidate) else None


def _validate_snapshot(config: CaptureConfig, snapshot: dict[str, Any]) -> None:
    schema = json.loads(config.evidence_schema_path.read_text(encoding="utf-8"))
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(snapshot)
    registry = json.loads(config.policy_path.read_text(encoding="utf-8"))
    if registry.get("hypothesis_id") != snapshot["hypothesis_id"]:
        raise ValueError("snapshot hypothesis is not a member of the deployed registry")
    if (
        registry.get("status") == "draft"
        and snapshot["enrollment_state"] != "pending_entry_selection"
    ):
        raise ValueError("draft registry cannot emit a resolved enrollment state")
    timing = snapshot["payload"]["timing"]
    observed = parse_utc(timing["source_first_observed_at_utc"])
    decision = parse_utc(timing["decision_at_utc"])
    recorded = parse_utc(snapshot["recorded_at_utc"])
    if not observed <= decision <= recorded:
        raise ValueError("snapshot source, decision, and record timestamps are out of order")
    unsigned = dict(snapshot)
    record_sha = unsigned.pop("record_sha256")
    if record_sha != sha256_bytes(rfc8785.dumps(unsigned)):
        raise ValueError("snapshot record digest is invalid")


def _existing_snapshot_sha(path: Path, job_id: str) -> str | None:
    if not path.is_file():
        return None
    with _connect(path, write=False) as conn:
        row = conn.execute(
            "SELECT record_sha256,record_json,stored_bytes_sha256 FROM evidence_snapshots "
            "WHERE job_id=?",
            (job_id,),
        ).fetchone()
    if row is None:
        return None
    record_bytes = bytes(row["record_json"])
    if sha256_bytes(record_bytes) != str(row["stored_bytes_sha256"]):
        raise RuntimeError(f"stored evidence bytes failed integrity check for {job_id}")
    record = json.loads(record_bytes)
    unsigned = dict(record)
    persisted_sha = str(unsigned.pop("record_sha256"))
    if persisted_sha != str(row["record_sha256"]):
        raise RuntimeError(f"stored evidence envelope digest mismatch for {job_id}")
    if persisted_sha != sha256_bytes(rfc8785.dumps(unsigned)):
        raise RuntimeError(f"stored evidence canonical digest mismatch for {job_id}")
    return persisted_sha


def _capture_options(
    config: CaptureConfig,
    job: CaptureJob,
) -> tuple[dict[str, Any] | None, Path | None, str | None, str | None, str | None, bool]:
    staging_dir = config.artifact_root / ".staging"
    staging_dir.mkdir(parents=True, exist_ok=True)
    output = staging_dir / f"{sha256_bytes(job.job_id.encode())}.{job.attempt_count}.json"
    output.unlink(missing_ok=True)
    symbol = str(json.loads(job.payload_json).get("issuer_symbol", "")).upper()
    try:
        result = run_hidden_process(
            [
                str(config.alpha_python),
                str(config.alpha_script),
                "--symbol",
                symbol,
                "--request-id",
                job.job_id,
                "--output",
                str(output),
            ],
            cwd=config.alpha_script.parent.parent,
            timeout_seconds=config.option_timeout_seconds,
        )
    except OSError as exc:
        output.unlink(missing_ok=True)
        return None, None, None, "OPTION_CAPTURE_LAUNCH_FAILED", str(exc), False
    if result.timed_out:
        output.unlink(missing_ok=True)
        return None, None, None, "OPTION_CAPTURE_TIMEOUT", "alpha-core timed out", True
    if result.returncode != 0:
        output.unlink(missing_ok=True)
        message = (result.stderr or result.stdout or f"exit {result.returncode}").strip()
        retryable = any(
            token in message.lower()
            for token in ("connect", "timeout", "gateway", "temporar", "market data")
        )
        return None, None, None, "OPTION_CAPTURE_PROCESS_FAILED", message, retryable
    try:
        raw = output.read_bytes()
        artifact = json.loads(raw)
        if not isinstance(artifact, dict):
            raise ValueError("option artifact root is not an object")
        if artifact.get("schema_version") != "insider-evidence-option-surface-v1":
            raise ValueError("unexpected option artifact schema")
        if artifact.get("request_id") != job.job_id:
            raise ValueError("option artifact request identity mismatch")
        if artifact.get("symbol") != symbol:
            raise ValueError("option artifact symbol mismatch")
        expected_identity = {
            "artifact_status": "RESEARCH_ONLY",
            "source_id": "ib_gateway:US_OPTIONS:SMART:type1",
            "client_id": 48,
            "market_data_type": 1,
        }
        for field, expected in expected_identity.items():
            if artifact.get(field) != expected:
                raise ValueError(f"option artifact {field} mismatch")
        requested_at = parse_utc(str(artifact["requested_at_utc"]))
        captured_at = parse_utc(str(artifact["captured_at_utc"]))
        source_max = parse_utc(str(artifact["source_max_ts_utc"]))
        if not requested_at <= source_max <= captured_at:
            raise ValueError("option artifact timestamps are out of order")
        surfaces = artifact.get("surfaces")
        if not isinstance(surfaces, list) or not surfaces:
            raise ValueError("option artifact has no captured surfaces")
        rfc8785.dumps(artifact)
        destination, digest = _publish_content_addressed(
            config.artifact_root / "options", raw, suffix=".json"
        )
        return artifact, destination, digest, None, None, False
    except (KeyError, OSError, TypeError, json.JSONDecodeError, ValueError) as exc:
        return None, None, None, "OPTION_ARTIFACT_INVALID", str(exc), False
    finally:
        output.unlink(missing_ok=True)


def _candidate_context(path: Path, packet_id: str) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        with _connect(path, write=False) as conn:
            row = conn.execute(
                """
                SELECT prior_close, median_dollar_volume_20d, eligibility_reason,
                       entry_session, planned_quantity, created_at
                FROM candidates WHERE packet_id=?
                """,
                (packet_id,),
            ).fetchone()
    except sqlite3.Error:
        return None
    return dict(row) if row is not None else None


def _filing_context(path: Path, job: CaptureJob) -> dict[str, Any]:
    with _connect(path, write=False) as conn:
        row = conn.execute(
            """
            SELECT source, filed_at, filing_detail_url, form4_xml_url
            FROM filings
            WHERE accession_number=? AND cik=? AND form_type=?
            ORDER BY filed_at LIMIT 1
            """,
            (job.accession_number, job.issuer_cik, job.form_type),
        ).fetchone()
        notification = conn.execute(
            "SELECT notification_sent_at FROM review_packets WHERE packet_id=?",
            (job.packet_id,),
        ).fetchone()
    if row is None:
        raise ValueError(f"filing missing for capture job {job.job_id}")
    result = dict(row)
    result["notification_sent_at"] = notification[0] if notification else None
    return result


def _error(stage: str, kind: str, message: str, *, retryable: bool) -> dict[str, Any]:
    return {
        "stage": stage,
        "kind": kind,
        "message": message[:MAX_ERROR_LENGTH] or kind,
        "retryable": retryable,
    }


def _missing_observation(
    *, source: str, observed_at: datetime, error: dict[str, Any] | None = None
) -> dict[str, Any]:
    return {
        "status": "error" if error else "missing",
        "as_of_utc": None,
        "observed_at_utc": utc_text(observed_at),
        "source": source,
        "artifact_ref": None,
        "artifact_sha256": None,
        "values": None,
        "error": error,
    }


def _captured_observation(
    *,
    source: str,
    as_of: datetime,
    observed_at: datetime,
    values: dict[str, Any],
    artifact_ref: str | None = None,
    artifact_sha256: str | None = None,
) -> dict[str, Any]:
    return {
        "status": "captured",
        "as_of_utc": utc_text(as_of),
        "observed_at_utc": utc_text(observed_at),
        "source": source,
        "artifact_ref": artifact_ref,
        "artifact_sha256": artifact_sha256,
        "values": values,
        "error": None,
    }


def _configuration_sha(config: CaptureConfig) -> str:
    safe = asdict(config)
    for name in ("source_db", "evidence_db", "artifact_root"):
        safe.pop(name)
    for name in (
        "alpha_python",
        "alpha_script",
        "canary_ledger",
        "policy_path",
        "evidence_schema_path",
    ):
        safe[name] = str(getattr(config, name))
    return sha256_bytes(rfc8785.dumps(safe))


def _append_snapshot(
    config: CaptureConfig,
    job: CaptureJob,
    *,
    capture_started: datetime,
    capture_finished: datetime,
    duration_ms: int,
    option_artifact: dict[str, Any] | None,
    option_path: Path | None,
    option_sha: str | None,
    option_error: dict[str, Any] | None,
) -> str:
    payload = json.loads(job.payload_json)
    decision = json.loads(job.decision_json)
    if not isinstance(payload, dict) or not isinstance(decision, dict):
        raise ValueError("persisted signal payloads must be objects")
    filing = _filing_context(config.source_db, job)
    candidate = _candidate_context(config.canary_ledger, job.packet_id)
    payload_owner_ciks = payload.get("reporting_owner_ciks")
    if isinstance(payload_owner_ciks, list):
        owner_ciks = sorted(
            {
                str(value)
                for value in payload_owner_ciks
                if isinstance(value, str) and value
            }
        )
    else:
        owner_cik = payload.get("reporting_owner_cik")
        owner_ciks = [str(owner_cik)] if isinstance(owner_cik, str) and owner_cik else []
    payload_owner_count = payload.get("reporting_owner_count")
    owner_count = (
        payload_owner_count
        if isinstance(payload_owner_count, int) and payload_owner_count >= 0
        else len(owner_ciks)
    )
    owner_cik = owner_ciks[0] if owner_count == 1 and len(owner_ciks) == 1 else None
    owner_mapping = (
        "exact"
        if owner_cik
        else "ambiguous"
        if owner_count > 1 or len(owner_ciks) > 1
        else "missing"
    )
    classification_state = (
        "ambiguous_multi_owner" if owner_mapping == "ambiguous" else "unpartitionable"
    )
    issuer_cik = str(payload.get("issuer_cik") or job.issuer_cik).lstrip("0") or "0"
    notification_at = (
        parse_utc(str(filing["notification_sent_at"]))
        if filing.get("notification_sent_at")
        else None
    )
    errors = [option_error] if option_error else []
    if option_artifact is not None and option_path is not None and option_sha is not None:
        completed_at = parse_utc(str(option_artifact["captured_at_utc"]))
        surfaces = option_artifact.get("surfaces", [])
        underlying_quotes = [
            {
                "expiry": surface.get("expiry"),
                "price": surface.get("underlying_price"),
                "bid": surface.get("underlying_bid"),
                "ask": surface.get("underlying_ask"),
                "source_timestamp_utc": surface.get("underlying_source_timestamp_utc"),
            }
            for surface in surfaces
            if isinstance(surface, dict)
        ]
        options_observation = _captured_observation(
            source=str(option_artifact["source_id"]),
            as_of=completed_at,
            observed_at=capture_finished,
            values={
                "schema_version": option_artifact["schema_version"],
                "status": option_artifact["artifact_status"],
                "quote_count": sum(
                    len(surface.get("quotes", []))
                    for surface in surfaces
                    if isinstance(surface, dict)
                ),
                "underlying_quotes": underlying_quotes,
                "bounds": {
                    "min_dte_days": option_artifact.get("min_dte_days"),
                    "max_dte_days": option_artifact.get("max_dte_days"),
                    "max_expiries": option_artifact.get("max_expiries"),
                    "max_contracts_per_expiry": option_artifact.get(
                        "max_contracts_per_expiry"
                    ),
                },
            },
            artifact_ref=str(option_path),
            artifact_sha256=option_sha,
        )
        market_observation = _captured_observation(
            source=str(option_artifact["source_id"]),
            as_of=completed_at,
            observed_at=capture_finished,
            values={
                "underlying_quotes": underlying_quotes,
                "canary_daily_context": candidate,
            },
            artifact_ref=str(option_path),
            artifact_sha256=option_sha,
        )
    else:
        options_observation = _missing_observation(
            source="alpha-core", observed_at=capture_finished, error=option_error
        )
        market_observation = (
            _missing_observation(source="live-canary", observed_at=capture_finished)
            if candidate is None
            else _captured_observation(
                source="live-canary:IBKR:daily-bars",
                as_of=parse_utc(str(candidate["created_at"])),
                observed_at=capture_finished,
                values=candidate,
            )
        )
    notification_observation = (
        _captured_observation(
            source="ntfy",
            as_of=notification_at,
            observed_at=capture_finished,
            values={"provider_responded_at_utc": utc_text(notification_at)},
        )
        if notification_at is not None
        else _missing_observation(source="ntfy", observed_at=capture_finished)
    )
    with _connect(config.evidence_db, write=True) as conn:
        conn.execute("BEGIN IMMEDIATE")
        sequence_row = conn.execute(
            "SELECT COALESCE(MAX(sequence), 0) + 1 FROM evidence_snapshots"
        ).fetchone()
        sequence = int(sequence_row[0])
        snapshot_id = str(uuid.uuid5(uuid.NAMESPACE_URL, job.job_id))
        snapshot: dict[str, Any] = {
            "schema_version": 2,
            "snapshot_id": snapshot_id,
            "hypothesis_id": HYPOTHESIS_ID,
            "recorded_at_utc": utc_text(capture_finished),
            "enrollment_state": "pending_entry_selection",
            "confirmatory_enrollment_sequence": None,
            "supersedes_snapshot_id": None,
            "record_sha256": "",
            "payload": {
                "signal": {
                    "packet_id": job.packet_id,
                    "accession_number": job.accession_number,
                    "issuer_cik": issuer_cik,
                    "issuer_symbol": str(payload["issuer_symbol"]).upper(),
                    "form_type": job.form_type,
                    "decision": "approve",
                    "reporting_owner_ciks": owner_ciks,
                },
                "timing": {
                    "sec_filed_at_utc": utc_text(parse_utc(str(filing["filed_at"]))),
                    "source_first_observed_at_utc": utc_text(job.source_first_observed_at),
                    "decision_at_utc": utc_text(job.decision_at),
                    "notification_requested_at_utc": None,
                    "notification_responded_at_utc": (
                        utc_text(notification_at) if notification_at else None
                    ),
                    "client_received_at_utc": None,
                    "monotonic_capture_duration_ms": duration_ms,
                    "clock_skew_status": "valid",
                },
                "versions": {
                    "git_commit": config.insider_git_commit,
                    "source_fingerprint_sha256": research_source_fingerprint(),
                    "policy_sha256": hashlib.sha256(config.policy_path.read_bytes()).hexdigest(),
                    "classifier_version": None,
                    "model_id": _optional_string(decision.get("model_id")),
                    "prompt_sha256": _optional_sha256(decision.get("prompt_sha256")),
                    "configuration_sha256": _configuration_sha(config),
                },
                "classification": {
                    "state": classification_state,
                    "owner_cik": owner_cik,
                    "classification_year": job.decision_at.year,
                    "cutoff_at_utc": utc_text(job.decision_at),
                    "transaction_owner_mapping": owner_mapping,
                    "history_coverage_complete": False,
                    "left_censored": True,
                    "history_observation_start_date": None,
                    "history_source_snapshot_sha256": None,
                    "history_input_sha256": sha256_bytes(b"owner-history-not-yet-captured"),
                },
                "observations": {
                    "sec_source": _captured_observation(
                        source=str(filing["source"]),
                        as_of=parse_utc(str(filing["filed_at"])),
                        observed_at=capture_started,
                        values={
                            "filing_detail_url": filing["filing_detail_url"],
                            "form4_xml_url": filing["form4_xml_url"],
                            "packet_payload": payload,
                            "decision_payload": decision,
                        },
                    ),
                    "market_context": market_observation,
                    "options_surface": options_observation,
                    "owner_history": _missing_observation(
                        source="SEC-owner-history", observed_at=capture_finished
                    ),
                    "notification_transport": notification_observation,
                },
                "errors": errors,
                "provenance": {
                    "host_id_sha256": sha256_bytes(platform.node().encode("utf-8")),
                    "process_id": os.getpid(),
                    "writer": CAPTURE_CONTRACT_VERSION,
                    "append_only_sequence": sequence,
                },
            },
        }
        unsigned = dict(snapshot)
        unsigned.pop("record_sha256")
        record_sha = sha256_bytes(rfc8785.dumps(unsigned))
        snapshot["record_sha256"] = record_sha
        _validate_snapshot(config, snapshot)
        record_bytes = rfc8785.dumps(snapshot)
        _publish_content_addressed(config.artifact_root / "snapshots", record_bytes, suffix=".json")
        conn.execute(
            """
            INSERT INTO evidence_snapshots(
                sequence, snapshot_id, job_id, record_sha256, stored_bytes_sha256,
                record_json, recorded_at_utc
            ) VALUES(?,?,?,?,?,?,?)
            """,
            (
                sequence,
                snapshot_id,
                job.job_id,
                record_sha,
                sha256_bytes(record_bytes),
                record_bytes,
                utc_text(capture_finished),
            ),
        )
        conn.commit()
    return record_sha


def _heartbeat(config: CaptureConfig, *, now: datetime, result: str, job_id: str | None) -> None:
    ensure_evidence_store(config.evidence_db)
    with _connect(config.evidence_db, write=True) as conn:
        conn.execute(
            """
            INSERT INTO capture_health(singleton,last_worker_heartbeat_utc,last_result,last_job_id)
            VALUES(1,?,?,?)
            ON CONFLICT(singleton) DO UPDATE SET
              last_worker_heartbeat_utc=excluded.last_worker_heartbeat_utc,
              last_result=excluded.last_result,
              last_job_id=excluded.last_job_id
            """,
            (utc_text(now), result, job_id),
        )


def _process_claimed_job(config: CaptureConfig, job: CaptureJob) -> CaptureResult:
    started = datetime.now(UTC)
    timer = monotonic()
    existing_sha = _existing_snapshot_sha(config.evidence_db, job.job_id)
    if existing_sha is not None:
        _record_attempt(
            config,
            job,
            started_at=started,
            finished_at=started,
            status="completed",
            error_kind=None,
            error_message=None,
            retryable=False,
        )
        _set_job_state(
            config,
            job,
            state="complete",
            now=started,
            record_sha256=existing_sha,
        )
        _heartbeat(config, now=started, result="recovered_existing", job_id=job.job_id)
        return CaptureResult(
            status="completed",
            job_id=job.job_id,
            snapshot_sha256=existing_sha,
            option_status="recovered_existing",
        )
    deadline = job.decision_at + timedelta(seconds=config.capture_deadline_seconds)
    option_artifact: dict[str, Any] | None
    option_path: Path | None
    option_sha: str | None
    error_kind: str | None
    error_message: str | None
    retryable: bool
    if started >= deadline:
        option_artifact = option_path = option_sha = None
        error_kind = "OPTION_CAPTURE_DEADLINE_MISSED"
        error_message = "job was not claimed before its point-in-time capture deadline"
        retryable = False
    else:
        (
            option_artifact,
            option_path,
            option_sha,
            error_kind,
            error_message,
            retryable,
        ) = _capture_options(config, job)
    finished = datetime.now(UTC)
    if error_kind and retryable and job.attempt_count < config.max_attempts and finished < deadline:
        _record_attempt(
            config,
            job,
            started_at=started,
            finished_at=finished,
            status="retry",
            error_kind=error_kind,
            error_message=error_message,
            retryable=True,
        )
        _set_job_state(
            config,
            job,
            state="retry",
            now=finished,
            error_kind=error_kind,
            error_message=error_message,
        )
        _heartbeat(config, now=finished, result="retry_scheduled", job_id=job.job_id)
        return CaptureResult(
            status="retry_scheduled", job_id=job.job_id, option_status=error_kind
        )
    option_error = (
        _error("options_surface", error_kind, error_message or error_kind, retryable=False)
        if error_kind
        else None
    )
    record_sha = _append_snapshot(
        config,
        job,
        capture_started=started,
        capture_finished=finished,
        duration_ms=max(0, round((monotonic() - timer) * 1000)),
        option_artifact=option_artifact,
        option_path=option_path,
        option_sha=option_sha,
        option_error=option_error,
    )
    _record_attempt(
        config,
        job,
        started_at=started,
        finished_at=finished,
        status="completed",
        error_kind=error_kind,
        error_message=error_message,
        retryable=False,
    )
    _set_job_state(
        config,
        job,
        state="complete",
        now=finished,
        error_kind=error_kind,
        error_message=error_message,
        record_sha256=record_sha,
    )
    _heartbeat(config, now=finished, result="completed", job_id=job.job_id)
    return CaptureResult(
        status="completed",
        job_id=job.job_id,
        snapshot_sha256=record_sha,
        option_status="captured" if option_artifact else error_kind,
    )


def run_capture_once(
    config: CaptureConfig,
    *,
    now: datetime | None = None,
    worker_id: str | None = None,
) -> CaptureResult:
    now = (now or datetime.now(UTC)).astimezone(UTC)
    worker_id = worker_id or f"{platform.node()}:{os.getpid()}"
    ensure_evidence_store(config.evidence_db)
    job = _claim_job(config, worker_id=worker_id, now=now)
    if job is None:
        _heartbeat(config, now=now, result="idle", job_id=None)
        return CaptureResult(status="idle")
    try:
        return _process_claimed_job(config, job)
    except Exception as exc:
        finished = datetime.now(UTC)
        message = f"{type(exc).__name__}: {exc}"
        permanent = isinstance(exc, (KeyError, TypeError, ValueError, ValidationError)) or (
            "content-address collision" in str(exc)
        )
        deadline = job.decision_at + timedelta(seconds=config.capture_deadline_seconds)
        retryable = (
            not permanent and job.attempt_count < config.max_attempts and finished < deadline
        )
        state = "retry" if retryable else "failed"
        error_kind = "CAPTURE_INTERNAL_RETRYABLE" if retryable else "CAPTURE_INTERNAL_TERMINAL"
        _record_attempt(
            config,
            job,
            started_at=finished,
            finished_at=finished,
            status=state,
            error_kind=error_kind,
            error_message=message,
            retryable=retryable,
        )
        _set_job_state(
            config,
            job,
            state=state,
            now=finished,
            error_kind=error_kind,
            error_message=message,
        )
        _heartbeat(config, now=finished, result=state, job_id=job.job_id)
        return CaptureResult(
            status="retry_scheduled" if retryable else "failed",
            job_id=job.job_id,
            option_status=error_kind,
        )


def capture_status(source_db: Path, evidence_db: Path) -> dict[str, Any]:
    result: dict[str, Any] = {"jobs": {}, "evidence_count": 0, "health": None}
    if source_db.is_file():
        with _connect(source_db, write=False) as conn:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='research_capture_jobs'"
            ).fetchone()
            if exists:
                result["jobs"] = {
                    str(row["status"]): int(row["count"])
                    for row in conn.execute(
                        "SELECT status, COUNT(*) count FROM research_capture_jobs GROUP BY status"
                    )
                }
    if evidence_db.is_file():
        with _connect(evidence_db, write=False) as conn:
            result["evidence_count"] = int(
                conn.execute("SELECT COUNT(*) FROM evidence_snapshots").fetchone()[0]
            )
            row = conn.execute("SELECT * FROM capture_health WHERE singleton=1").fetchone()
            result["health"] = dict(row) if row else None
    return result


def resolve_git_commit(repo_root: Path) -> str:
    result = run_hidden_process(
        ["git", "rev-parse", "HEAD"], cwd=repo_root, timeout_seconds=10
    )
    commit = result.stdout.strip().lower()
    if result.timed_out or result.returncode != 0 or len(commit) != 40:
        raise RuntimeError("unable to resolve insider-alerts deployment commit")
    if any(char not in "0123456789abcdef" for char in commit):
        raise RuntimeError("git returned an invalid deployment commit")
    return commit
