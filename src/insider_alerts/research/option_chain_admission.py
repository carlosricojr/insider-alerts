"""Durable no-retry admission for causal pre-decision option-chain capture."""

from __future__ import annotations

import hashlib
import re
import sqlite3
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Literal, Protocol

from insider_alerts.research.capture import ProcessResult, run_hidden_process

OPTION_CHAIN_ADMISSION_SCHEMA_VERSION = "insider-option-chain-admission-v1"
OPTION_CHAIN_PROVIDER_CADENCE_SECONDS = 900
OPTION_CHAIN_CAPTURE_TIMEOUT_SECONDS = 15
_SYMBOL_RE = re.compile(r"^[A-Z0-9][A-Z0-9.-]{0,31}$")
_STORE_INIT_LOCK = Lock()

OptionChainAdmissionStatus = Literal[
    "admitted",
    "succeeded",
    "failed",
    "timed_out",
    "skipped_cadence",
]


class OptionChainAdmissionError(RuntimeError):
    """The research admission could not produce an authoritative launch verdict."""


class ProcessRunner(Protocol):
    def __call__(
        self,
        command: list[str],
        *,
        cwd: Path,
        timeout_seconds: int,
    ) -> ProcessResult: ...


@dataclass(frozen=True, slots=True)
class OptionChainAdmissionConfig:
    source_db: Path
    repo_root: Path
    chain_store_db: Path
    alpha_python: Path
    alpha_script: Path
    timeout_seconds: int = OPTION_CHAIN_CAPTURE_TIMEOUT_SECONDS


@dataclass(frozen=True, slots=True)
class OptionChainAdmissionResult:
    packet_id: str
    symbol: str
    batch_id: str
    status: OptionChainAdmissionStatus
    admitted_at_utc: datetime
    finished_at_utc: datetime | None
    launch_required: bool
    prior_packet_id: str | None
    exit_code: int | None
    error_kind: str | None


@dataclass(frozen=True, slots=True)
class _ValidatedConfig:
    source_db: Path
    chain_store_db: Path
    alpha_python: Path
    alpha_script: Path
    alpha_runtime_root: Path
    alpha_script_sha256: str
    timeout_seconds: int


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None or value.utcoffset() != timedelta(0):
        raise OptionChainAdmissionError("admission clock must return timezone-aware UTC")
    return value.astimezone(UTC)


def _stamp(value: datetime) -> str:
    return _utc(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _parse_stamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _text_sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _canonical_symbol(raw: str) -> str:
    symbol = raw.strip().upper()
    if _SYMBOL_RE.fullmatch(symbol) is None:
        raise OptionChainAdmissionError("issuer symbol is not canonicalizable")
    return symbol


def _batch_id(packet_id: str) -> str:
    if not isinstance(packet_id, str) or not packet_id or len(packet_id) > 256:
        raise OptionChainAdmissionError("packet_id must be a bounded non-empty string")
    return f"insider-{hashlib.sha256(packet_id.encode('utf-8')).hexdigest()}"


def _validate_config(config: OptionChainAdmissionConfig) -> _ValidatedConfig:
    if (
        isinstance(config.timeout_seconds, bool)
        or not isinstance(config.timeout_seconds, int)
        or config.timeout_seconds != OPTION_CHAIN_CAPTURE_TIMEOUT_SECONDS
    ):
        raise OptionChainAdmissionError(
            "option-chain process timeout must remain "
            f"{OPTION_CHAIN_CAPTURE_TIMEOUT_SECONDS} seconds"
        )
    try:
        if config.alpha_script.is_symlink() or config.alpha_python.is_symlink():
            raise OptionChainAdmissionError("alpha runtime files cannot be symbolic links")
        source_db = config.source_db.resolve(strict=True)
        repo_root = config.repo_root.resolve(strict=True)
        research_root = (repo_root / "data" / "research").resolve(strict=True)
        alpha_script = config.alpha_script.resolve(strict=True)
        alpha_python = config.alpha_python.resolve(strict=True)
        chain_parent = config.chain_store_db.parent.resolve(strict=True)
    except OSError as exc:
        raise OptionChainAdmissionError("configured option-chain path is unavailable") from exc
    if not source_db.is_file():
        raise OptionChainAdmissionError("source database must be an existing regular file")
    if not alpha_script.is_file() or not alpha_python.is_file():
        raise OptionChainAdmissionError("alpha runtime files must be regular files")
    if not chain_parent.is_relative_to(research_root):
        raise OptionChainAdmissionError("chain store must remain beneath data/research")
    chain_store_db = chain_parent / config.chain_store_db.name
    if chain_store_db.exists():
        if chain_store_db.is_symlink():
            raise OptionChainAdmissionError("chain store cannot be a symbolic link")
        chain_store_db = chain_store_db.resolve(strict=True)
        if not chain_store_db.is_file() or not chain_store_db.is_relative_to(research_root):
            raise OptionChainAdmissionError("existing chain store escaped data/research")
    runtime_root = alpha_script.parent.parent
    if alpha_script.parent != runtime_root / "scripts":
        raise OptionChainAdmissionError("alpha chain script must be directly under runtime scripts")
    try:
        expected_python = (runtime_root / ".venv" / "Scripts" / "python.exe").resolve(strict=True)
    except OSError as exc:
        raise OptionChainAdmissionError("alpha runtime interpreter is unavailable") from exc
    if alpha_python != expected_python:
        raise OptionChainAdmissionError("alpha interpreter does not belong to the script runtime")
    return _ValidatedConfig(
        source_db=source_db,
        chain_store_db=chain_store_db,
        alpha_python=alpha_python,
        alpha_script=alpha_script,
        alpha_runtime_root=runtime_root,
        alpha_script_sha256=_file_sha256(alpha_script),
        timeout_seconds=config.timeout_seconds,
    )


def _connect(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, timeout=30, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=30000")
    connection.execute("PRAGMA foreign_keys=ON")
    return connection


def _ensure_store(path: Path) -> None:
    # SQLite cannot transition journal mode while another connection is making the
    # same transition. Production is already WAL, but serialize first-use setup so
    # concurrent in-process approvals cannot race before the admission transaction.
    with _STORE_INIT_LOCK, closing(_connect(path)) as connection, connection:
        journal_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0])
        if journal_mode.casefold() != "wal":
            connection.execute("PRAGMA journal_mode=WAL")
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS research_option_chain_admission_metadata (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                schema_version TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS research_option_chain_admissions (
                packet_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                batch_id TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL CHECK(status IN (
                    'admitted','succeeded','failed','timed_out','skipped_cadence'
                )),
                prior_packet_id TEXT REFERENCES research_option_chain_admissions(packet_id),
                admitted_at_utc TEXT NOT NULL,
                finished_at_utc TEXT,
                alpha_python_path TEXT NOT NULL,
                alpha_script_path TEXT NOT NULL,
                alpha_script_sha256 TEXT NOT NULL CHECK(length(alpha_script_sha256)=64),
                chain_store_path TEXT NOT NULL,
                timeout_seconds INTEGER NOT NULL CHECK(timeout_seconds=15),
                exit_code INTEGER,
                error_kind TEXT,
                stdout_sha256 TEXT CHECK(stdout_sha256 IS NULL OR length(stdout_sha256)=64),
                stderr_sha256 TEXT CHECK(stderr_sha256 IS NULL OR length(stderr_sha256)=64),
                CHECK (
                    (status='admitted' AND prior_packet_id IS NULL
                     AND finished_at_utc IS NULL AND exit_code IS NULL
                     AND error_kind IS NULL AND stdout_sha256 IS NULL
                     AND stderr_sha256 IS NULL)
                    OR
                    (status='skipped_cadence' AND prior_packet_id IS NOT NULL
                     AND finished_at_utc IS NOT NULL AND exit_code IS NULL
                     AND error_kind='CADENCE_SUPPRESSED'
                     AND stdout_sha256 IS NULL AND stderr_sha256 IS NULL)
                    OR
                    (status='succeeded' AND prior_packet_id IS NULL
                     AND finished_at_utc IS NOT NULL AND exit_code=0
                     AND error_kind IS NULL AND stdout_sha256 IS NOT NULL
                     AND stderr_sha256 IS NOT NULL)
                    OR
                    (status='failed' AND prior_packet_id IS NULL
                     AND finished_at_utc IS NOT NULL AND exit_code<>0
                     AND error_kind IS NOT NULL AND stdout_sha256 IS NOT NULL
                     AND stderr_sha256 IS NOT NULL)
                    OR
                    (status='timed_out' AND prior_packet_id IS NULL
                     AND finished_at_utc IS NOT NULL AND exit_code IS NOT NULL
                     AND error_kind='CHILD_TIMEOUT' AND stdout_sha256 IS NOT NULL
                     AND stderr_sha256 IS NOT NULL)
                )
            );
            CREATE INDEX IF NOT EXISTS idx_research_option_chain_admission_symbol_time
                ON research_option_chain_admissions(symbol, admitted_at_utc DESC);
            CREATE TRIGGER IF NOT EXISTS research_option_chain_admission_identity_immutable
            BEFORE UPDATE OF packet_id,symbol,batch_id,prior_packet_id,admitted_at_utc,
                alpha_python_path,alpha_script_path,alpha_script_sha256,
                chain_store_path,timeout_seconds
            ON research_option_chain_admissions
            BEGIN SELECT RAISE(ABORT, 'option-chain admission identity is immutable'); END;
            CREATE TRIGGER IF NOT EXISTS research_option_chain_admission_terminal_immutable
            BEFORE UPDATE ON research_option_chain_admissions
            WHEN OLD.status <> 'admitted'
            BEGIN SELECT RAISE(ABORT, 'terminal option-chain admission is immutable'); END;
            CREATE TRIGGER IF NOT EXISTS research_option_chain_admission_no_delete
            BEFORE DELETE ON research_option_chain_admissions
            BEGIN SELECT RAISE(ABORT, 'option-chain admissions cannot be deleted'); END;
            """
        )
        connection.execute(
            """
            INSERT OR IGNORE INTO research_option_chain_admission_metadata(singleton,schema_version)
            VALUES(1,?)
            """,
            (OPTION_CHAIN_ADMISSION_SCHEMA_VERSION,),
        )
        version = connection.execute(
            "SELECT schema_version FROM research_option_chain_admission_metadata WHERE singleton=1"
        ).fetchone()[0]
        if version != OPTION_CHAIN_ADMISSION_SCHEMA_VERSION:
            raise OptionChainAdmissionError("option-chain admission schema version mismatch")


def _result(row: sqlite3.Row, *, launch_required: bool = False) -> OptionChainAdmissionResult:
    return OptionChainAdmissionResult(
        packet_id=str(row["packet_id"]),
        symbol=str(row["symbol"]),
        batch_id=str(row["batch_id"]),
        status=row["status"],
        admitted_at_utc=_parse_stamp(str(row["admitted_at_utc"])),
        finished_at_utc=(
            _parse_stamp(str(row["finished_at_utc"]))
            if row["finished_at_utc"] is not None
            else None
        ),
        launch_required=launch_required,
        prior_packet_id=(
            str(row["prior_packet_id"]) if row["prior_packet_id"] is not None else None
        ),
        exit_code=int(row["exit_code"]) if row["exit_code"] is not None else None,
        error_kind=str(row["error_kind"]) if row["error_kind"] is not None else None,
    )


def _identity(config: _ValidatedConfig, *, packet_id: str, symbol: str) -> tuple[object, ...]:
    return (
        packet_id,
        symbol,
        _batch_id(packet_id),
        str(config.alpha_python),
        str(config.alpha_script),
        config.alpha_script_sha256,
        str(config.chain_store_db),
        config.timeout_seconds,
    )


def _admit(
    config: _ValidatedConfig,
    *,
    packet_id: str,
    symbol: str,
    now: datetime,
) -> OptionChainAdmissionResult:
    _ensure_store(config.source_db)
    identity = _identity(config, packet_id=packet_id, symbol=symbol)
    with closing(_connect(config.source_db)) as connection, connection:
        connection.execute("BEGIN IMMEDIATE")
        existing = connection.execute(
            "SELECT * FROM research_option_chain_admissions WHERE packet_id=?",
            (packet_id,),
        ).fetchone()
        if existing is not None:
            actual = (
                existing["packet_id"],
                existing["symbol"],
                existing["batch_id"],
                existing["alpha_python_path"],
                existing["alpha_script_path"],
                existing["alpha_script_sha256"],
                existing["chain_store_path"],
                existing["timeout_seconds"],
            )
            if actual != identity:
                connection.rollback()
                raise OptionChainAdmissionError("existing packet admission identity mismatch")
            connection.commit()
            return _result(existing)
        tail = connection.execute(
            "SELECT admitted_at_utc FROM research_option_chain_admissions "
            "ORDER BY admitted_at_utc DESC,packet_id DESC LIMIT 1"
        ).fetchone()
        if tail is not None and now < _parse_stamp(str(tail["admitted_at_utc"])):
            connection.rollback()
            raise OptionChainAdmissionError("admission clock regressed behind durable state")
        cadence_cutoff = _stamp(now - timedelta(seconds=OPTION_CHAIN_PROVIDER_CADENCE_SECONDS))
        prior = connection.execute(
            """
            SELECT packet_id FROM research_option_chain_admissions
            WHERE symbol=? AND status IN ('admitted','succeeded','failed','timed_out')
              AND admitted_at_utc>?
            ORDER BY admitted_at_utc DESC,packet_id DESC LIMIT 1
            """,
            (symbol, cadence_cutoff),
        ).fetchone()
        admitted_at = _stamp(now)
        if prior is not None:
            connection.execute(
                """
                INSERT INTO research_option_chain_admissions(
                    packet_id,symbol,batch_id,status,prior_packet_id,
                    admitted_at_utc,finished_at_utc,alpha_python_path,
                    alpha_script_path,alpha_script_sha256,chain_store_path,
                    timeout_seconds,error_kind
                ) VALUES(?,?,?,'skipped_cadence',?,?,?,?,?,?,?,?, 'CADENCE_SUPPRESSED')
                """,
                (
                    *identity[:3],
                    str(prior["packet_id"]),
                    admitted_at,
                    admitted_at,
                    *identity[3:],
                ),
            )
            row = connection.execute(
                "SELECT * FROM research_option_chain_admissions WHERE packet_id=?",
                (packet_id,),
            ).fetchone()
            connection.commit()
            return _result(row)
        connection.execute(
            """
            INSERT INTO research_option_chain_admissions(
                packet_id,symbol,batch_id,status,admitted_at_utc,
                alpha_python_path,alpha_script_path,alpha_script_sha256,
                chain_store_path,timeout_seconds
            ) VALUES(?,?,?,'admitted',?,?,?,?,?,?)
            """,
            (*identity[:3], admitted_at, *identity[3:]),
        )
        row = connection.execute(
            "SELECT * FROM research_option_chain_admissions WHERE packet_id=?",
            (packet_id,),
        ).fetchone()
        connection.commit()
        return _result(row, launch_required=True)


def _finalize(
    config: _ValidatedConfig,
    *,
    packet_id: str,
    status: Literal["succeeded", "failed", "timed_out"],
    finished_at: datetime,
    exit_code: int,
    error_kind: str | None,
    stdout: str,
    stderr: str,
) -> OptionChainAdmissionResult:
    with closing(_connect(config.source_db)) as connection, connection:
        connection.execute("BEGIN IMMEDIATE")
        admitted = connection.execute(
            "SELECT admitted_at_utc FROM research_option_chain_admissions "
            "WHERE packet_id=? AND status='admitted'",
            (packet_id,),
        ).fetchone()
        if admitted is None:
            connection.rollback()
            raise OptionChainAdmissionError("admission is unavailable for finalization")
        if finished_at < _parse_stamp(str(admitted["admitted_at_utc"])):
            connection.rollback()
            raise OptionChainAdmissionError("finalization clock regressed behind admission")
        cursor = connection.execute(
            """
            UPDATE research_option_chain_admissions
            SET status=?,finished_at_utc=?,exit_code=?,error_kind=?,
                stdout_sha256=?,stderr_sha256=?
            WHERE packet_id=? AND status='admitted'
            """,
            (
                status,
                _stamp(finished_at),
                exit_code,
                error_kind,
                _text_sha256(stdout),
                _text_sha256(stderr),
                packet_id,
            ),
        )
        if cursor.rowcount != 1:
            connection.rollback()
            raise OptionChainAdmissionError("admission did not make one final transition")
        row = connection.execute(
            "SELECT * FROM research_option_chain_admissions WHERE packet_id=?",
            (packet_id,),
        ).fetchone()
        connection.commit()
        return _result(row)


def capture_predecision_option_chain(
    config: OptionChainAdmissionConfig,
    *,
    packet_id: str,
    symbol: str,
    clock: Callable[[], datetime] = _utc_now,
    process_runner: ProcessRunner = run_hidden_process,
) -> OptionChainAdmissionResult:
    """Admit once, then run at most one hidden alpha-core chain capture."""

    validated = _validate_config(config)
    canonical_symbol = _canonical_symbol(symbol)
    now = _utc(clock())
    admission = _admit(
        validated,
        packet_id=packet_id,
        symbol=canonical_symbol,
        now=now,
    )
    if not admission.launch_required:
        return admission
    command = [
        str(validated.alpha_python),
        str(validated.alpha_script),
        "--store-db",
        str(validated.chain_store_db),
        "--symbol",
        canonical_symbol,
        "--batch-id",
        admission.batch_id,
    ]
    try:
        completed = process_runner(
            command,
            cwd=validated.alpha_runtime_root,
            timeout_seconds=validated.timeout_seconds,
        )
    except OSError as exc:
        return _finalize(
            validated,
            packet_id=packet_id,
            status="failed",
            finished_at=_utc(clock()),
            exit_code=-1,
            error_kind="CHILD_LAUNCH_FAILED",
            stdout="",
            stderr=f"{type(exc).__name__}:{exc}",
        )
    status: Literal["succeeded", "failed", "timed_out"]
    try:
        script_changed = _file_sha256(validated.alpha_script) != validated.alpha_script_sha256
    except OSError:
        script_changed = True
    if script_changed:
        status = "failed"
        error_kind = "SCRIPT_UNAVAILABLE_OR_CHANGED_DURING_CAPTURE"
        exit_code = completed.returncode or -1
    elif completed.timed_out:
        status = "timed_out"
        error_kind = "CHILD_TIMEOUT"
        exit_code = completed.returncode
    elif completed.returncode == 0:
        status = "succeeded"
        error_kind = None
        exit_code = 0
    else:
        status = "failed"
        error_kind = "CHILD_EXIT_NONZERO"
        exit_code = completed.returncode
    return _finalize(
        validated,
        packet_id=packet_id,
        status=status,
        finished_at=_utc(clock()),
        exit_code=exit_code,
        error_kind=error_kind,
        stdout=completed.stdout,
        stderr=completed.stderr,
    )


def option_chain_admission_rows(path: Path) -> list[dict[str, object]]:
    """Return ordered operational rows without creating or changing the store."""

    with closing(_connect(path.resolve(strict=True))) as connection, connection:
        rows = connection.execute(
            "SELECT * FROM research_option_chain_admissions ORDER BY admitted_at_utc,packet_id"
        ).fetchall()
    return [dict(row) for row in rows]


__all__ = [
    "OPTION_CHAIN_ADMISSION_SCHEMA_VERSION",
    "OPTION_CHAIN_CAPTURE_TIMEOUT_SECONDS",
    "OPTION_CHAIN_PROVIDER_CADENCE_SECONDS",
    "OptionChainAdmissionConfig",
    "OptionChainAdmissionError",
    "OptionChainAdmissionResult",
    "capture_predecision_option_chain",
    "option_chain_admission_rows",
]
