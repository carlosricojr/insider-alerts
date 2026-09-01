from __future__ import annotations

import hashlib
import json
import os
import platform
import signal
import sqlite3
import stat
import subprocess
import tempfile
import threading
import uuid
from collections.abc import Iterator
from contextlib import closing, contextmanager, suppress
from dataclasses import asdict, dataclass, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from time import monotonic, perf_counter, sleep
from typing import Any, Literal
from zoneinfo import ZoneInfo

import rfc8785
from jsonschema import Draft202012Validator, FormatChecker, ValidationError

from insider_alerts.research.activation import validate_deployed_registry_state
from insider_alerts.research.inference import (
    _validate_registry,
    enrollment_deadline,
)
from insider_alerts.research.sec_history import (
    CLASSIFIER_VERSION,
    HistoryStore,
    SnapshotMetadata,
    classify_owner,
)

CAPTURE_CONTRACT_VERSION = "insider-evidence-capture-v2"
HYPOTHESIS_ID = "OPP-E07-V1"
MAX_ERROR_LENGTH = 2_000
HISTORICAL_OPTION_SCHEMA_VERSION = "insider-evidence-option-history-v2"
HISTORICAL_OPTION_SOURCE_ID = "ib_gateway:US_OPTIONS:SMART:type1:historical_bid_ask_15m"
OPTION_SURFACE_RESULT_SCHEMA_VERSION = "insider-evidence-option-surface-result-v1"
OPTION_SURFACE_SOURCE_ID = "ib_gateway:US_OPTIONS:SMART:type1"
OPTION_SURFACE_NOT_APPLICABLE_EXIT_CODE = 4
_OPTION_SURFACE_RESULT_FIELDS = frozenset(
    {
        "schema_version",
        "status",
        "reason_code",
        "source_id",
        "request_id",
        "symbol",
        "client_id",
        "observed_at_utc",
    }
)
_WINDOWS_ARTIFACT_HANDLES: dict[str, tuple[int, int]] = {}
_WINDOWS_ARTIFACT_HANDLE_LOCK = threading.RLock()
_POSIX_ARTIFACT_LOCKS: dict[str, tuple[int, int]] = {}
_POSIX_ARTIFACT_LOCK_GUARD = threading.RLock()


@dataclass(slots=True, frozen=True)
class CaptureConfig:
    source_db: Path
    evidence_db: Path
    artifact_root: Path
    research_root: Path
    alpha_python: Path
    alpha_script: Path
    alpha_historical_script: Path
    option_chain_store_db: Path
    historical_pacing_db: Path
    canary_ledger: Path
    history_db: Path
    history_snapshot_sha256: str
    insider_git_commit: str
    policy_path: Path
    evidence_schema_path: Path
    activation_db: Path
    capture_delay_seconds: int = 20
    capture_deadline_seconds: int = 600
    option_timeout_seconds: int = 90
    historical_option_timeout_seconds: int = 120
    max_attempts: int = 3
    lease_seconds: int = 180

    def __post_init__(self) -> None:
        if len(self.insider_git_commit) != 40 or any(
            char not in "0123456789abcdef" for char in self.insider_git_commit
        ):
            raise ValueError("insider_git_commit must be a lowercase 40-character SHA-1")
        if len(self.history_snapshot_sha256) != 64 or any(
            char not in "0123456789abcdef" for char in self.history_snapshot_sha256
        ):
            raise ValueError("history_snapshot_sha256 must be a lowercase 64-character SHA-256")
        for name in (
            "capture_delay_seconds",
            "capture_deadline_seconds",
            "option_timeout_seconds",
            "historical_option_timeout_seconds",
            "max_attempts",
            "lease_seconds",
        ):
            if int(getattr(self, name)) < 1:
                raise ValueError(f"{name} must be positive")
        if self.capture_delay_seconds >= self.capture_deadline_seconds:
            raise ValueError("capture delay must be shorter than capture deadline")
        if (
            self.lease_seconds
            <= max(self.option_timeout_seconds, self.historical_option_timeout_seconds) + 30
        ):
            raise ValueError("lease must outlive every option timeout by more than 30 seconds")
        if self.capture_deadline_seconds <= (
            self.capture_delay_seconds + self.max_attempts * self.option_timeout_seconds
        ):
            raise ValueError("capture deadline cannot accommodate every bounded option attempt")


class OptionRuntimeValidationError(RuntimeError):
    """The configured alpha runtime or research database path is not confined."""


class ArtifactPublicationError(RuntimeError):
    """A validated artifact could not be published to its content-addressed store."""


@dataclass(slots=True, frozen=True)
class _ValidatedOptionRuntime:
    alpha_python: Path
    alpha_script: Path
    alpha_historical_script: Path
    alpha_runtime_root: Path
    research_root: Path
    artifact_root: Path
    staging_root: Path
    options_root: Path
    option_chain_store_db: Path
    historical_pacing_db: Path


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
    lease_owner: str


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


class ProcessTreeCleanupError(RuntimeError):
    """A timed-out hidden process may still have live descendants or open pipes."""


@dataclass(slots=True, frozen=True)
class CaptureWindow:
    status: Literal["draft", "armed", "active"]
    policy_sha256: str
    activated_at: datetime | None = None
    deadline: datetime | None = None


def utc_text(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("UTC timestamp cannot be naive")
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"naive persisted timestamp: {value}")
    return parsed.astimezone(UTC)


def _is_reparse_point(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except OSError:
        return False
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    attributes = getattr(metadata, "st_file_attributes", 0)
    return path.is_symlink() or bool(reparse_flag and attributes & reparse_flag)


def _reject_reparse_path(path: Path, *, root: Path, label: str) -> None:
    lexical_root = Path(os.path.abspath(root))
    lexical_path = Path(os.path.abspath(path))
    if not lexical_path.is_relative_to(lexical_root):
        raise OptionRuntimeValidationError(f"{label} escaped its configured checkout")
    cursor = lexical_root
    for part in (".", *lexical_path.relative_to(lexical_root).parts):
        if part != ".":
            cursor /= part
        if _is_reparse_point(cursor):
            raise OptionRuntimeValidationError(f"{label} contains a reparse point")


def _confined_research_db(path: Path, *, research_root: Path) -> Path:
    lexical_root = Path(os.path.abspath(research_root))
    lexical_path = Path(os.path.abspath(path))
    if not lexical_path.is_relative_to(lexical_root) or lexical_path == lexical_root:
        raise OptionRuntimeValidationError("option database escaped data/research")
    cursor = lexical_root
    if not cursor.is_dir() or _is_reparse_point(cursor):
        raise OptionRuntimeValidationError("data/research is unavailable or a reparse point")
    for part in lexical_path.parent.relative_to(lexical_root).parts:
        cursor /= part
        if not cursor.is_dir() or _is_reparse_point(cursor):
            raise OptionRuntimeValidationError(
                "option database parent is unavailable or a reparse point"
            )
    resolved_root = lexical_root.resolve(strict=True)
    resolved_parent = lexical_path.parent.resolve(strict=True)
    if not resolved_parent.is_relative_to(resolved_root):
        raise OptionRuntimeValidationError("option database parent escaped data/research")
    candidate = resolved_parent / lexical_path.name
    if candidate.exists():
        if _is_reparse_point(candidate):
            raise OptionRuntimeValidationError("option database cannot be a reparse point")
        candidate = candidate.resolve(strict=True)
        if not candidate.is_file() or not candidate.is_relative_to(resolved_root):
            raise OptionRuntimeValidationError("existing option database escaped data/research")
    return candidate


def _confined_artifact_root(path: Path, *, research_root: Path) -> Path:
    lexical_root = Path(os.path.abspath(research_root))
    lexical_path = Path(os.path.abspath(path))
    if not lexical_path.is_relative_to(lexical_root) or lexical_path == lexical_root:
        raise OptionRuntimeValidationError("artifact root escaped data/research")
    if not lexical_root.is_dir() or _is_reparse_point(lexical_root):
        raise OptionRuntimeValidationError("data/research is unavailable or a reparse point")
    cursor = lexical_root
    for part in lexical_path.relative_to(lexical_root).parts:
        cursor /= part
        try:
            cursor.mkdir(exist_ok=True)
        except OSError as exc:
            raise OptionRuntimeValidationError("artifact root is unavailable") from exc
        if not cursor.is_dir() or _is_reparse_point(cursor):
            raise OptionRuntimeValidationError(
                "artifact root is unavailable or contains a reparse point"
            )
    try:
        resolved_root = lexical_root.resolve(strict=True)
        resolved_path = lexical_path.resolve(strict=True)
    except OSError as exc:
        raise OptionRuntimeValidationError("artifact root is unavailable") from exc
    if not resolved_path.is_relative_to(resolved_root):
        raise OptionRuntimeValidationError("artifact root escaped data/research")
    return resolved_path


def _confined_artifact_subdirectory(
    artifact_root: Path, name: str, *, research_root: Path
) -> Path:
    if Path(name).name != name or name in {"", ".", ".."}:
        raise OptionRuntimeValidationError("artifact subdirectory name is invalid")
    candidate = artifact_root / name
    try:
        with _locked_artifact_directory(
            artifact_root, research_root=research_root
        ) as locked_root:
            locked_candidate = locked_root / name
            locked_candidate.mkdir(exist_ok=True)
            if not locked_candidate.is_dir() or _is_reparse_point(locked_candidate):
                raise OptionRuntimeValidationError(
                    "artifact subdirectory is unavailable or a reparse point"
                )
            locked_stat = os.stat(locked_candidate)
            lexical_stat = os.stat(candidate, follow_symlinks=False)
            if (locked_stat.st_dev, locked_stat.st_ino) != (
                lexical_stat.st_dev,
                lexical_stat.st_ino,
            ):
                raise OptionRuntimeValidationError(
                    "artifact subdirectory changed during creation"
                )
            resolved_root = artifact_root.resolve(strict=True)
            resolved_candidate = candidate.resolve(strict=True)
    except OSError as exc:
        raise OptionRuntimeValidationError("artifact subdirectory is unavailable") from exc
    if resolved_candidate.parent != resolved_root:
        raise OptionRuntimeValidationError("artifact subdirectory escaped artifact root")
    return resolved_candidate


@contextmanager
def _artifact_process_mutex(
    research_root: Path, *, timeout_seconds: int = 180
) -> Iterator[None]:
    if os.name != "nt":
        import fcntl

        fcntl_portable: Any = fcntl
        root_identity = os.path.normcase(str(Path(os.path.abspath(research_root))))
        lock_name = f"insider-alerts-research-{sha256_bytes(root_identity.encode())}.lock"
        lock_path = Path(tempfile.gettempdir()) / lock_name
        flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
        with _POSIX_ARTIFACT_LOCK_GUARD:
            existing = _POSIX_ARTIFACT_LOCKS.get(root_identity)
            if existing is not None:
                _POSIX_ARTIFACT_LOCKS[root_identity] = (existing[0], existing[1] + 1)
                try:
                    yield
                finally:
                    descriptor, count = _POSIX_ARTIFACT_LOCKS[root_identity]
                    _POSIX_ARTIFACT_LOCKS[root_identity] = (descriptor, count - 1)
                return
            while True:
                try:
                    descriptor = os.open(lock_path, flags, 0o600)
                    if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                        os.close(descriptor)
                        raise OptionRuntimeValidationError(
                            "artifact process lock is not a regular file"
                        )
                    break
                except OSError as exc:
                    raise OptionRuntimeValidationError(
                        "artifact process lock could not be opened"
                    ) from exc
            deadline = perf_counter() + timeout_seconds
            try:
                while True:
                    try:
                        fcntl_portable.flock(
                            descriptor, fcntl_portable.LOCK_EX | fcntl_portable.LOCK_NB
                        )
                        break
                    except BlockingIOError as exc:
                        if perf_counter() >= deadline:
                            raise OptionRuntimeValidationError(
                                "artifact process lock timed out"
                            ) from exc
                        sleep(0.05)
                _POSIX_ARTIFACT_LOCKS[root_identity] = (descriptor, 1)
                yield
            finally:
                fcntl_portable.flock(descriptor, fcntl_portable.LOCK_UN)
                os.close(descriptor)
                _POSIX_ARTIFACT_LOCKS.pop(root_identity, None)
        return

    import ctypes
    from ctypes import wintypes

    ctypes_windows: Any = ctypes
    kernel32 = ctypes_windows.WinDLL("kernel32", use_last_error=True)
    create_mutex = kernel32.CreateMutexW
    create_mutex.argtypes = (wintypes.LPVOID, wintypes.BOOL, wintypes.LPCWSTR)
    create_mutex.restype = wintypes.HANDLE
    wait_for_single_object = kernel32.WaitForSingleObject
    wait_for_single_object.argtypes = (wintypes.HANDLE, wintypes.DWORD)
    wait_for_single_object.restype = wintypes.DWORD
    release_mutex = kernel32.ReleaseMutex
    release_mutex.argtypes = (wintypes.HANDLE,)
    release_mutex.restype = wintypes.BOOL
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = (wintypes.HANDLE,)
    close_handle.restype = wintypes.BOOL
    root_identity = os.path.normcase(str(Path(os.path.abspath(research_root))))
    mutex_name = f"Global\\InsiderAlertsResearchArtifacts-{sha256_bytes(root_identity.encode())}"
    handle = create_mutex(None, False, mutex_name)
    if not handle:
        raise OptionRuntimeValidationError("artifact process mutex could not be created") from (
            ctypes_windows.WinError(ctypes_windows.get_last_error())
        )
    acquired = False
    try:
        wait_result = int(wait_for_single_object(handle, timeout_seconds * 1_000))
        if wait_result not in {0x00000000, 0x00000080}:
            if wait_result == 0x00000102:
                raise OptionRuntimeValidationError("artifact process mutex timed out")
            raise OptionRuntimeValidationError("artifact process mutex wait failed") from (
                ctypes_windows.WinError(ctypes_windows.get_last_error())
            )
        acquired = True
        yield
    finally:
        if acquired:
            release_mutex(handle)
        close_handle(handle)


@contextmanager
def _locked_artifact_directory(path: Path, *, research_root: Path) -> Iterator[Path]:
    """Serialize writers and pin ancestors so a validated path cannot be replaced."""

    with _artifact_process_mutex(research_root), _locked_artifact_directory_under_mutex(
        path, research_root=research_root
    ) as locked:
        yield locked


@contextmanager
def _locked_artifact_directory_under_mutex(
    path: Path, *, research_root: Path
) -> Iterator[Path]:

    verified = _confined_artifact_root(path, research_root=research_root)
    if os.name != "nt":
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        lexical_path = Path(os.path.abspath(verified))
        opened_descriptors: list[int] = []
        try:
            cursor_fd = os.open(lexical_path.anchor, flags)
            opened_descriptors.append(cursor_fd)
            for component in lexical_path.parts[1:]:
                cursor_fd = os.open(component, flags, dir_fd=cursor_fd)
                opened_descriptors.append(cursor_fd)
        except OSError as exc:
            for descriptor in reversed(opened_descriptors):
                os.close(descriptor)
            raise OptionRuntimeValidationError(
                f"artifact directory could not be pinned: {verified}"
            ) from exc
        try:
            directory_fd = opened_descriptors[-1]
            opened = os.fstat(directory_fd)
            current = os.stat(verified, follow_symlinks=False)
            if (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino):
                raise OptionRuntimeValidationError(
                    f"artifact directory changed during validation: {verified}"
                )
            descriptor_path = Path(f"/proc/self/fd/{directory_fd}")
            if not descriptor_path.is_dir():
                raise OptionRuntimeValidationError(
                    "handle-relative artifact access is unavailable on this platform"
                )
            yield descriptor_path
        finally:
            for descriptor in reversed(opened_descriptors):
                os.close(descriptor)
        return

    import ctypes
    from ctypes import wintypes

    ctypes_windows: Any = ctypes
    kernel32 = ctypes_windows.WinDLL("kernel32", use_last_error=True)
    create_file = kernel32.CreateFileW
    create_file.argtypes = (
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    )
    create_file.restype = wintypes.HANDLE
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = (wintypes.HANDLE,)
    close_handle.restype = wintypes.BOOL
    invalid_handle = ctypes.c_void_p(-1).value
    read_attributes_and_delete = 0x00000080 | 0x00010000
    share_read_write = 0x00000001 | 0x00000002
    open_existing = 3
    open_directory_without_following = 0x02000000 | 0x00200000
    lexical_root = Path(os.path.abspath(research_root))
    lexical_path = Path(os.path.abspath(verified))
    acquired_keys: list[str] = []
    try:
        with _WINDOWS_ARTIFACT_HANDLE_LOCK:
            cursor = lexical_root
            for part in (None, *lexical_path.relative_to(lexical_root).parts):
                if part is not None:
                    cursor /= part
                key = os.path.normcase(str(cursor))
                existing = _WINDOWS_ARTIFACT_HANDLES.get(key)
                if existing is not None:
                    _WINDOWS_ARTIFACT_HANDLES[key] = (existing[0], existing[1] + 1)
                    acquired_keys.append(key)
                    continue
                handle = create_file(
                    str(cursor),
                    read_attributes_and_delete,
                    share_read_write,
                    None,
                    open_existing,
                    open_directory_without_following,
                    None,
                )
                if handle == invalid_handle:
                    raise OptionRuntimeValidationError(
                        f"artifact directory could not be pinned: {cursor}"
                    ) from ctypes_windows.WinError(ctypes_windows.get_last_error())
                handle_value = int(handle)
                _WINDOWS_ARTIFACT_HANDLES[key] = (handle_value, 1)
                acquired_keys.append(key)
                if not cursor.is_dir() or _is_reparse_point(cursor):
                    raise OptionRuntimeValidationError(
                        f"artifact directory changed during validation: {cursor}"
                    )
        yield _confined_artifact_root(lexical_path, research_root=lexical_root)
    finally:
        with _WINDOWS_ARTIFACT_HANDLE_LOCK:
            for key in reversed(acquired_keys):
                handle, count = _WINDOWS_ARTIFACT_HANDLES[key]
                if count == 1:
                    close_handle(handle)
                    del _WINDOWS_ARTIFACT_HANDLES[key]
                else:
                    _WINDOWS_ARTIFACT_HANDLES[key] = (handle, count - 1)


def _validated_option_runtime(config: CaptureConfig) -> _ValidatedOptionRuntime:
    lexical_python = Path(os.path.abspath(config.alpha_python))
    lexical_script = Path(os.path.abspath(config.alpha_script))
    lexical_historical_script = Path(os.path.abspath(config.alpha_historical_script))
    lexical_runtime_root = lexical_script.parent.parent
    lexical_scripts_root = lexical_runtime_root / "scripts"
    if (
        lexical_script.parent != lexical_scripts_root
        or lexical_historical_script.parent != lexical_scripts_root
        or lexical_python != lexical_runtime_root / ".venv" / "Scripts" / "python.exe"
    ):
        raise OptionRuntimeValidationError("alpha runtime paths do not belong to one checkout")
    for path in (lexical_python, lexical_script, lexical_historical_script):
        _reject_reparse_path(path, root=lexical_runtime_root, label="alpha runtime path")
    try:
        if _is_reparse_point(config.source_db):
            raise OptionRuntimeValidationError("source database cannot be a reparse point")
        source_db = config.source_db.resolve(strict=True)
        for path in (
            config.alpha_python,
            config.alpha_script,
            config.alpha_historical_script,
        ):
            if _is_reparse_point(path):
                raise OptionRuntimeValidationError("alpha runtime files cannot be reparse points")
        alpha_python = config.alpha_python.resolve(strict=True)
        alpha_script = config.alpha_script.resolve(strict=True)
        alpha_historical_script = config.alpha_historical_script.resolve(strict=True)
    except OSError as exc:
        raise OptionRuntimeValidationError("configured option runtime is unavailable") from exc
    if not source_db.is_file() or not all(
        path.is_file() for path in (alpha_python, alpha_script, alpha_historical_script)
    ):
        raise OptionRuntimeValidationError("option runtime paths must be regular files")
    runtime_root = alpha_script.parent.parent
    scripts_root = runtime_root / "scripts"
    if alpha_script.parent != scripts_root or alpha_historical_script.parent != scripts_root:
        raise OptionRuntimeValidationError("alpha entrypoints must share the runtime scripts root")
    try:
        expected_python = (runtime_root / ".venv" / "Scripts" / "python.exe").resolve(strict=True)
    except OSError as exc:
        raise OptionRuntimeValidationError("alpha runtime interpreter is unavailable") from exc
    if alpha_python != expected_python:
        raise OptionRuntimeValidationError("alpha interpreter does not belong to the scripts")
    research_root = Path(os.path.abspath(config.research_root))
    try:
        research_parent = research_root.parent.resolve(strict=True)
    except OSError as exc:
        raise OptionRuntimeValidationError("research root parent is unavailable") from exc
    if research_parent != source_db.parent:
        raise OptionRuntimeValidationError("research root is not bound to the source data root")
    artifact_root = _confined_artifact_root(config.artifact_root, research_root=research_root)
    return _ValidatedOptionRuntime(
        alpha_python=alpha_python,
        alpha_script=alpha_script,
        alpha_historical_script=alpha_historical_script,
        alpha_runtime_root=runtime_root,
        research_root=research_root.resolve(strict=True),
        artifact_root=artifact_root,
        staging_root=_confined_artifact_subdirectory(
            artifact_root, ".staging", research_root=research_root
        ),
        options_root=_confined_artifact_subdirectory(
            artifact_root, "options", research_root=research_root
        ),
        option_chain_store_db=_confined_research_db(
            config.option_chain_store_db, research_root=research_root
        ),
        historical_pacing_db=_confined_research_db(
            config.historical_pacing_db, research_root=research_root
        ),
    )


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
                recorded_at_utc TEXT NOT NULL,
                owner_history_status TEXT
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
                last_job_id TEXT,
                last_error_kind TEXT,
                last_error_message TEXT
            );
            """
        )
        evidence_columns = {
            str(row["name"]) for row in conn.execute("PRAGMA table_info(evidence_snapshots)")
        }
        if "owner_history_status" not in evidence_columns:
            conn.execute("ALTER TABLE evidence_snapshots ADD COLUMN owner_history_status TEXT")
        health_columns = {
            str(row["name"]) for row in conn.execute("PRAGMA table_info(capture_health)")
        }
        if "last_error_kind" not in health_columns:
            conn.execute("ALTER TABLE capture_health ADD COLUMN last_error_kind TEXT")
        if "last_error_message" not in health_columns:
            conn.execute("ALTER TABLE capture_health ADD COLUMN last_error_message TEXT")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS evidence_snapshots_owner_history_status "
            "ON evidence_snapshots(owner_history_status)"
        )


def _claim_job(
    config: CaptureConfig,
    *,
    worker_id: str,
    now: datetime,
    window: CaptureWindow,
) -> CaptureJob | None:
    if window.status != "active" or window.activated_at is None or window.deadline is None:
        raise ValueError("capture jobs can be claimed only inside an active registry window")
    with _connect(config.source_db, write=True) as conn:
        conn.execute("BEGIN IMMEDIATE")
        available_rows = conn.execute(
            """
            SELECT * FROM research_capture_jobs
            WHERE status IN ('pending','retry')
               OR (status='leased' AND lease_expires_at_utc <= ?)
            ORDER BY decision_at_utc, job_id
            """,
            (utc_text(now),),
        ).fetchall()
        for row in available_rows:
            observed_at = parse_utc(str(row["source_first_observed_at_utc"]))
            if window.activated_at <= observed_at < window.deadline:
                continue
            conn.execute(
                """
                UPDATE research_capture_jobs
                SET status='failed',
                    lease_owner=NULL, lease_expires_at_utc=NULL,
                    last_error_kind='OUTSIDE_CONFIRMATORY_CAPTURE_WINDOW',
                    last_error_message='signal is outside the sealed confirmatory capture window',
                    updated_at_utc=?
                WHERE job_id=?
                """,
                (utc_text(now), str(row["job_id"])),
            )
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
            WHERE ((status IN ('pending', 'retry') AND attempt_count < ?)
               OR (status = 'leased' AND attempt_count <= ? AND lease_expires_at_utc <= ?))
            ORDER BY decision_at_utc, job_id
            LIMIT 100
            """,
            (
                config.max_attempts,
                config.max_attempts,
                utc_text(now),
            ),
        ).fetchall()
        selected: sqlite3.Row | None = None
        for row in rows:
            observed_at = parse_utc(str(row["source_first_observed_at_utc"]))
            if not window.activated_at <= observed_at < window.deadline:
                raise RuntimeError("unquarantined capture job is outside the active window")
            decision_at = parse_utc(str(row["decision_at_utc"]))
            if now >= decision_at + timedelta(seconds=config.capture_delay_seconds):
                selected = row
                break
        if selected is None:
            conn.commit()
            return None
        previous_attempt_count = int(selected["attempt_count"])
        existing_attempt = conn.execute(
            "SELECT 1 FROM research_capture_attempts WHERE job_id=? AND attempt_number=?",
            (str(selected["job_id"]), previous_attempt_count),
        ).fetchone()
        attempt_count = (
            previous_attempt_count + 1
            if str(selected["status"]) != "leased"
            or (existing_attempt is not None and previous_attempt_count < config.max_attempts)
            else previous_attempt_count
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
            lease_owner=worker_id,
        )


def _finish_job_attempt(
    config: CaptureConfig,
    job: CaptureJob,
    *,
    started_at: datetime,
    finished_at: datetime,
    attempt_status: str,
    job_state: str,
    error_kind: str | None,
    error_message: str | None,
    retryable: bool,
    record_sha256: str | None = None,
) -> None:
    with _connect(config.source_db, write=True) as conn:
        cursor = conn.execute(
            """
            UPDATE research_capture_jobs
            SET status=?, lease_owner=NULL, lease_expires_at_utc=NULL, updated_at_utc=?,
                last_error_kind=?, last_error_message=?, record_sha256=?
            WHERE job_id=? AND status='leased' AND lease_owner=?
            """,
            (
                job_state,
                utc_text(finished_at),
                error_kind,
                error_message[:MAX_ERROR_LENGTH] if error_message else None,
                record_sha256,
                job.job_id,
                job.lease_owner,
            ),
        )
        if cursor.rowcount != 1:
            raise RuntimeError(f"lost lease for capture job {job.job_id}")
        conn.execute(
            """
            INSERT INTO research_capture_attempts(
                job_id, attempt_number, started_at_utc, finished_at_utc, status,
                error_kind, error_message, retryable
            ) VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                job.job_id,
                job.attempt_count,
                utc_text(started_at),
                utc_text(finished_at),
                attempt_status,
                error_kind,
                error_message[:MAX_ERROR_LENGTH] if error_message else None,
                int(retryable),
            ),
        )


def _renew_job_lease(config: CaptureConfig, job: CaptureJob, *, now: datetime) -> None:
    with _connect(config.source_db, write=True) as conn:
        cursor = conn.execute(
            """
            UPDATE research_capture_jobs
            SET lease_expires_at_utc=?, updated_at_utc=?
            WHERE job_id=? AND status='leased' AND lease_owner=?
            """,
            (
                utc_text(now + timedelta(seconds=config.lease_seconds)),
                utc_text(now),
                job.job_id,
                job.lease_owner,
            ),
        )
        if cursor.rowcount != 1:
            raise RuntimeError(f"lost lease for capture job {job.job_id}")


def _attempt_exists(config: CaptureConfig, job: CaptureJob) -> bool:
    with _connect(config.source_db, write=False) as conn:
        return (
            conn.execute(
                "SELECT 1 FROM research_capture_attempts WHERE job_id=? AND attempt_number=?",
                (job.job_id, job.attempt_count),
            ).fetchone()
            is not None
        )


def _finish_job_without_new_attempt(
    config: CaptureConfig,
    job: CaptureJob,
    *,
    finished_at: datetime,
    job_state: str,
    error_kind: str | None,
    error_message: str | None,
    record_sha256: str | None = None,
) -> None:
    with _connect(config.source_db, write=True) as conn:
        cursor = conn.execute(
            """
            UPDATE research_capture_jobs
            SET status=?, lease_owner=NULL, lease_expires_at_utc=NULL, updated_at_utc=?,
                last_error_kind=?, last_error_message=?, record_sha256=?
            WHERE job_id=? AND status='leased' AND lease_owner=?
              AND EXISTS (
                  SELECT 1 FROM research_capture_attempts attempts
                  WHERE attempts.job_id = research_capture_jobs.job_id
                    AND attempts.attempt_number = research_capture_jobs.attempt_count
              )
            """,
            (
                job_state,
                utc_text(finished_at),
                error_kind,
                error_message[:MAX_ERROR_LENGTH] if error_message else None,
                record_sha256,
                job.job_id,
                job.lease_owner,
            ),
        )
        if cursor.rowcount != 1:
            raise RuntimeError(f"lost lease for capture job {job.job_id}")


def _kill_process_tree(process: subprocess.Popen[str], *, platform: str = os.name) -> None:
    if platform == "nt" and process.poll() is not None:
        raise ProcessTreeCleanupError(
            "hidden child exited before its descendant tree could be targeted"
        )
    try:
        if platform == "nt":
            killed = subprocess.run(
                ["taskkill.exe", "/PID", str(process.pid), "/T", "/F"],
                capture_output=True,
                text=True,
                check=False,
                timeout=5,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            if killed.returncode != 0:
                detail = (killed.stderr or killed.stdout or "unknown taskkill failure").strip()
                raise ProcessTreeCleanupError(
                    f"failed to terminate hidden child process tree: {detail}"
                )
        else:
            os_portable: Any = os
            signal_portable: Any = signal
            with suppress(ProcessLookupError):
                os_portable.killpg(process.pid, signal_portable.SIGKILL)
        process.wait(timeout=5)
    except ProcessTreeCleanupError:
        raise
    except subprocess.TimeoutExpired as exc:
        raise ProcessTreeCleanupError("hidden child did not terminate after tree kill") from exc
    except Exception as exc:
        raise ProcessTreeCleanupError(
            f"hidden child process-tree cleanup failed: {type(exc).__name__}: {exc}"
        ) from exc


def run_hidden_process(
    command: list[str],
    *,
    cwd: Path,
    timeout_seconds: int,
    pass_fds: tuple[int, ...] = (),
) -> ProcessResult:
    flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    if os.name == "nt":
        flags |= getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    popen_kwargs: dict[str, Any] = {}
    if os.name != "nt":
        popen_kwargs["pass_fds"] = pass_fds
        popen_kwargs["start_new_session"] = True
    process = subprocess.Popen(
        command,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        creationflags=flags,
        **popen_kwargs,
    )
    try:
        stdout, stderr = process.communicate(timeout=timeout_seconds)
        return ProcessResult(process.returncode, stdout, stderr, False)
    except subprocess.TimeoutExpired:
        _kill_process_tree(process)
        try:
            stdout, stderr = process.communicate(timeout=5)
        except Exception as exc:
            raise ProcessTreeCleanupError(
                f"hidden child pipes remained unusable after tree kill: {type(exc).__name__}: {exc}"
            ) from exc
        return ProcessResult(process.returncode or -1, stdout, stderr, True)


def _publish_content_addressed(
    root: Path,
    data: bytes,
    *,
    suffix: str,
    research_root: Path,
) -> tuple[Path, str]:
    try:
        with _locked_artifact_directory(root, research_root=research_root) as locked_root:
            destination, digest = _publish_content_addressed_locked(
                locked_root, data, suffix=suffix
            )
            lexical_destination = root / destination.name
            locked_directory_stat = os.stat(locked_root)
            lexical_directory_stat = os.stat(root, follow_symlinks=False)
            locked_destination_stat = os.stat(destination)
            lexical_destination_stat = os.stat(lexical_destination, follow_symlinks=False)
            if (
                (locked_directory_stat.st_dev, locked_directory_stat.st_ino)
                != (lexical_directory_stat.st_dev, lexical_directory_stat.st_ino)
                or (locked_destination_stat.st_dev, locked_destination_stat.st_ino)
                != (lexical_destination_stat.st_dev, lexical_destination_stat.st_ino)
                or lexical_destination.read_bytes() != data
            ):
                raise ArtifactPublicationError(
                    f"content-address publication path changed: {root}"
                )
            return lexical_destination, digest
    except OptionRuntimeValidationError as exc:
        raise ArtifactPublicationError(f"content-address root is invalid: {root}") from exc
    except OSError as exc:
        raise ArtifactPublicationError(f"content-address publication failed: {root}") from exc


def _inherited_descriptor_for(path: Path) -> tuple[int, ...]:
    if os.name == "nt" or path.parent.parent != Path("/proc/self/fd"):
        return ()
    try:
        descriptor = int(path.parent.name)
        os.fstat(descriptor)
    except (OSError, ValueError) as exc:
        raise OptionRuntimeValidationError("staging directory handle is unavailable") from exc
    return (descriptor,)


def _publish_content_addressed_locked(
    root: Path, data: bytes, *, suffix: str
) -> tuple[Path, str]:
    digest = sha256_bytes(data)
    destination = root / f"{digest}{suffix}"
    if destination.exists():
        if _is_reparse_point(destination) or not destination.is_file():
            raise ArtifactPublicationError(
                f"content-address destination is not a regular file: {destination}"
            )
        if destination.read_bytes() != data:
            raise ArtifactPublicationError(f"content-address collision at {destination}")
        return destination, digest
    staging = root / f".{digest}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    staging_created = False
    primary_error: BaseException | None = None
    try:
        descriptor = os.open(
            staging,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_BINARY", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        staging_created = True
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
        except BaseException:
            with suppress(OSError):
                os.close(descriptor)
            raise
        os.link(staging, destination)
    except FileExistsError as exc:
        try:
            if _is_reparse_point(destination) or not destination.is_file():
                raise ArtifactPublicationError(
                    f"content-address destination is not a regular file: {destination}"
                ) from exc
            if destination.read_bytes() != data:
                raise ArtifactPublicationError(
                    f"content-address collision at {destination}"
                ) from exc
        except BaseException as collision_error:
            primary_error = collision_error
            raise
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        if staging_created:
            try:
                staging.unlink(missing_ok=True)
            except OSError as cleanup_exc:
                if primary_error is not None:
                    primary_error.add_note(
                        f"publication staging cleanup also failed: {cleanup_exc}"
                    )
                else:
                    raise
    return destination, digest


def _optional_string(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _optional_sha256(value: Any) -> str | None:
    candidate = _optional_string(value)
    if candidate is None or len(candidate) != 64:
        return None
    return candidate if all(char in "0123456789abcdef" for char in candidate) else None


def _validated_option_not_applicable_result(
    result: ProcessResult,
    *,
    output: Path,
    expected_request_id: str,
    expected_symbol: str,
    observed_not_before: datetime | None = None,
    observed_not_after: datetime | None = None,
) -> dict[str, Any]:
    if result.timed_out or result.returncode != OPTION_SURFACE_NOT_APPLICABLE_EXIT_CODE:
        raise ValueError("option not-applicable result used an unexpected process outcome")
    if output.exists():
        raise ValueError("option not-applicable result published a forbidden artifact")
    try:
        payload = json.loads(result.stdout)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("option not-applicable stdout is not one JSON object") from exc
    if not isinstance(payload, dict) or set(payload) != _OPTION_SURFACE_RESULT_FIELDS:
        raise ValueError("option not-applicable result field set is invalid")
    expected_identity = {
        "schema_version": OPTION_SURFACE_RESULT_SCHEMA_VERSION,
        "status": "not_applicable",
        "reason_code": "OPTION_CHAIN_NOT_LISTED",
        "source_id": OPTION_SURFACE_SOURCE_ID,
        "request_id": expected_request_id,
        "symbol": expected_symbol,
        "client_id": 48,
    }
    for field, expected in expected_identity.items():
        if payload.get(field) != expected:
            raise ValueError(f"option not-applicable {field} mismatch")
    observed_text = payload.get("observed_at_utc")
    if not isinstance(observed_text, str):
        raise ValueError("option not-applicable observation timestamp is missing")
    try:
        raw_observed = datetime.fromisoformat(observed_text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("option not-applicable observation timestamp is invalid") from exc
    if raw_observed.tzinfo is None or raw_observed.utcoffset() != timedelta(0):
        raise ValueError("option not-applicable observation timestamp must be UTC")
    observed = raw_observed.astimezone(UTC)
    if observed_not_before is not None and observed < observed_not_before.astimezone(UTC):
        raise ValueError("option not-applicable observation predates the capture request")
    if observed_not_after is not None and observed > observed_not_after.astimezone(UTC):
        raise ValueError("option not-applicable observation is in the future")
    payload["observed_at_utc"] = utc_text(observed)
    rfc8785.dumps(payload)
    return payload


def _validate_snapshot(
    config: CaptureConfig,
    snapshot: dict[str, Any],
    *,
    expected_policy_sha256: str,
) -> None:
    policy_bytes = config.policy_path.read_bytes()
    if sha256_bytes(policy_bytes) != expected_policy_sha256:
        raise ValueError("prospective registry changed after the capture job was claimed")
    registry = json.loads(policy_bytes)
    schema = json.loads(config.evidence_schema_path.read_text(encoding="utf-8"))
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(snapshot)
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
    try:
        runtime = _validated_option_runtime(config)
    except OptionRuntimeValidationError as exc:
        return None, None, None, "OPTION_RUNTIME_INVALID", str(exc), False
    try:
        with _locked_artifact_directory(
            runtime.staging_root, research_root=runtime.research_root
        ) as staging_root:
            return _capture_options_with_runtime(
                config,
                job,
                replace(runtime, staging_root=staging_root),
            )
    except OptionRuntimeValidationError as exc:
        return None, None, None, "OPTION_RUNTIME_INVALID", str(exc), False


def _capture_options_with_runtime(
    config: CaptureConfig,
    job: CaptureJob,
    runtime: _ValidatedOptionRuntime,
) -> tuple[dict[str, Any] | None, Path | None, str | None, str | None, str | None, bool]:
    staging_dir = runtime.staging_root
    output = staging_dir / f"{sha256_bytes(job.job_id.encode())}.{job.attempt_count}.json"
    try:
        with _managed_staging_output(output) as cleanup:
            result = _capture_options_with_managed_output(config, job, runtime, output=output)
    except ArtifactPublicationError as exc:
        return None, None, None, "OPTION_STAGING_CLEANUP_FAILED", str(exc), False
    return _apply_staging_cleanup_result(
        result,
        cleanup_error=cleanup.error_message,
        cleanup_kind="OPTION_STAGING_CLEANUP_FAILED",
    )


@dataclass(slots=True)
class _StagingCleanupState:
    error_message: str | None = None


@contextmanager
def _managed_staging_output(path: Path) -> Iterator[_StagingCleanupState]:
    try:
        path.unlink(missing_ok=True)
    except OSError as exc:
        raise ArtifactPublicationError(f"staging cleanup failed: {path}") from exc
    state = _StagingCleanupState()
    try:
        yield state
    except BaseException as exc:
        try:
            path.unlink(missing_ok=True)
        except OSError as cleanup_exc:
            exc.add_note(f"staging cleanup also failed: {cleanup_exc}")
        raise
    else:
        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            state.error_message = f"staging cleanup failed: {path}: {exc}"


def _apply_staging_cleanup_result(
    result: tuple[
        dict[str, Any] | None,
        Path | None,
        str | None,
        str | None,
        str | None,
        bool,
    ],
    *,
    cleanup_error: str | None,
    cleanup_kind: str,
) -> tuple[dict[str, Any] | None, Path | None, str | None, str | None, str | None, bool]:
    if cleanup_error is None:
        return result
    artifact, artifact_path, digest, error_kind, error_message, retryable = result
    if error_kind is not None:
        joined_message = f"{error_message or error_kind}; {cleanup_error}"
        return artifact, artifact_path, digest, error_kind, joined_message, retryable
    return None, None, None, cleanup_kind, cleanup_error, False


def _capture_options_with_managed_output(
    config: CaptureConfig,
    job: CaptureJob,
    runtime: _ValidatedOptionRuntime,
    *,
    output: Path,
) -> tuple[dict[str, Any] | None, Path | None, str | None, str | None, str | None, bool]:
    symbol = str(json.loads(job.payload_json).get("issuer_symbol", "")).upper()
    provider_requested_at = datetime.now(UTC)
    try:
        result = run_hidden_process(
            [
                str(runtime.alpha_python),
                str(runtime.alpha_script),
                "--symbol",
                symbol,
                "--request-id",
                job.job_id,
                "--output",
                str(output),
            ],
            cwd=runtime.alpha_runtime_root,
            timeout_seconds=config.option_timeout_seconds,
            pass_fds=_inherited_descriptor_for(output),
        )
    except OSError as exc:
        return None, None, None, "OPTION_CAPTURE_LAUNCH_FAILED", str(exc), False
    if result.timed_out:
        return None, None, None, "OPTION_CAPTURE_TIMEOUT", "alpha-core timed out", True
    if result.returncode == OPTION_SURFACE_NOT_APPLICABLE_EXIT_CODE:
        try:
            unavailable = _validated_option_not_applicable_result(
                result,
                output=output,
                expected_request_id=job.job_id,
                expected_symbol=symbol,
                observed_not_before=provider_requested_at,
                observed_not_after=datetime.now(UTC),
            )
        except (OSError, TypeError, ValueError) as exc:
            return None, None, None, "OPTION_RESULT_INVALID", str(exc), False
        return unavailable, None, None, None, None, False
    if result.returncode != 0:
        message = (result.stderr or result.stdout or f"exit {result.returncode}").strip()
        normalized_message = message.casefold()
        if "venue session is not open" in normalized_message:
            _renew_job_lease(config, job, now=datetime.now(UTC))
            return _capture_historical_options(config, job, symbol=symbol)
        retryable = any(
            token in normalized_message
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
            runtime.options_root,
            raw,
            suffix=".json",
            research_root=runtime.research_root,
        )
        return artifact, destination, digest, None, None, False
    except ArtifactPublicationError as exc:
        return None, None, None, "OPTION_ARTIFACT_PUBLICATION_FAILED", str(exc), False
    except (KeyError, OSError, TypeError, json.JSONDecodeError, ValueError) as exc:
        return None, None, None, "OPTION_ARTIFACT_INVALID", str(exc), False


def _history_target_id(value: Any) -> str:
    if not isinstance(value, dict):
        raise ValueError("historical option target is not an object")
    target_id = value.get("target_id")
    if not isinstance(target_id, str) or not target_id:
        raise ValueError("historical option target identity is missing")
    return target_id


def _validate_historical_option_artifact(
    artifact: dict[str, Any],
    *,
    job: CaptureJob,
    symbol: str,
    chain_store_db: Path,
) -> None:
    expected_identity = {
        "schema_version": HISTORICAL_OPTION_SCHEMA_VERSION,
        "artifact_status": "RESEARCH_ONLY",
        "source_id": HISTORICAL_OPTION_SOURCE_ID,
        "capture_mode": "FORWARD_CLOSED_VENUE_FALLBACK",
        "trade_selection_authority": False,
        "backfill_authority": False,
        "request_id": job.job_id,
        "symbol": symbol,
        "client_id": 49,
        "market_data_type": 1,
    }
    for field, expected in expected_identity.items():
        if artifact.get(field) != expected:
            raise ValueError(f"historical option artifact {field} mismatch")
    cutoff = parse_utc(str(artifact["information_cutoff_utc"]))
    requested_at = parse_utc(str(artifact["requested_at_utc"]))
    captured_at = parse_utc(str(artifact["captured_at_utc"]))
    if cutoff != job.decision_at:
        raise ValueError("historical option artifact cutoff does not equal the decision time")
    if not cutoff <= requested_at <= captured_at:
        raise ValueError("historical option artifact timestamps are out of order")
    chain = artifact.get("option_chain_snapshot")
    if not isinstance(chain, dict):
        raise ValueError("historical option chain snapshot is missing")
    if parse_utc(str(chain["observed_at_utc"])) > cutoff:
        raise ValueError("historical option chain snapshot is post-cutoff")
    digest = artifact.get("option_chain_feed_record_sha256")
    if (
        not isinstance(digest, str)
        or len(digest) != 64
        or any(char not in "0123456789abcdef" for char in digest)
    ):
        raise ValueError("historical option chain digest is invalid")
    underlying = artifact.get("underlying_reference")
    if not isinstance(underlying, dict) or underlying.get("symbol") != symbol:
        raise ValueError("historical underlying reference identity mismatch")
    if parse_utc(str(underlying["bar_end_utc"])) > cutoff:
        raise ValueError("historical underlying reference is post-cutoff")
    targets = artifact.get("targets")
    bars = artifact.get("bars")
    errors = artifact.get("capture_errors")
    if not isinstance(targets, list) or len(targets) != 4:
        raise ValueError("historical artifact must contain the fixed four targets")
    if not isinstance(bars, list) or not isinstance(errors, list):
        raise ValueError("historical artifact results must be arrays")
    target_ids = [_history_target_id(target) for target in targets]
    result_ids = [
        _history_target_id(result.get("target") if isinstance(result, dict) else None)
        for result in (*bars, *errors)
    ]
    if len(set(target_ids)) != 4 or sorted(result_ids) != sorted(target_ids):
        raise ValueError("historical artifact must have exactly one result per target")
    for bar in bars:
        if not isinstance(bar, dict) or parse_utc(str(bar["bar_end_utc"])) > cutoff:
            raise ValueError("historical option bar is post-cutoff")
    request_count = artifact.get("historical_request_count")
    pacing_units = artifact.get("historical_pacing_units")
    if (
        isinstance(request_count, bool)
        or not isinstance(request_count, int)
        or request_count < 1
        or isinstance(pacing_units, bool)
        or not isinstance(pacing_units, int)
        or not 1 <= pacing_units <= 9
    ):
        raise ValueError("historical request accounting is invalid")
    _validate_historical_chain_custody(
        artifact,
        chain_store_db=chain_store_db,
        symbol=symbol,
        chain=chain,
        digest=digest,
    )


def _validate_historical_chain_custody(
    artifact: dict[str, Any],
    *,
    chain_store_db: Path,
    symbol: str,
    chain: dict[str, Any],
    digest: str,
) -> None:
    sequence = artifact.get("option_chain_feed_sequence")
    if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 1:
        raise ValueError("historical option chain sequence is invalid")
    try:
        with closing(sqlite3.connect(f"{chain_store_db.as_uri()}?mode=ro", uri=True)) as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute(
                "SELECT record_sha256,symbol,observed_at_utc,payload_json "
                "FROM option_chain_feed_records WHERE sequence=?",
                (sequence,),
            ).fetchone()
    except sqlite3.Error as exc:
        raise ValueError("historical option chain custody lookup failed") from exc
    if row is None or str(row["record_sha256"]) != digest or str(row["symbol"]) != symbol:
        raise ValueError("historical option chain custody identity mismatch")
    raw_payload = row["payload_json"]
    try:
        if isinstance(raw_payload, bytes):
            payload = json.loads(raw_payload.decode("utf-8", errors="strict"))
        elif isinstance(raw_payload, str):
            payload = json.loads(raw_payload)
        else:
            raise ValueError("historical option chain custody payload has invalid storage type")
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("historical option chain custody payload is invalid") from exc
    if not isinstance(payload, dict):
        raise ValueError("historical option chain custody payload is not an object")
    if (
        payload.get("record_type") != "snapshot"
        or payload.get("symbol") != symbol
        or payload.get("snapshot") != chain
        or parse_utc(str(payload.get("observed_at_utc"))) != parse_utc(str(row["observed_at_utc"]))
    ):
        raise ValueError("historical option chain custody payload mismatch")


def _capture_historical_options(
    config: CaptureConfig,
    job: CaptureJob,
    *,
    symbol: str,
) -> tuple[dict[str, Any] | None, Path | None, str | None, str | None, str | None, bool]:
    try:
        runtime = _validated_option_runtime(config)
    except OptionRuntimeValidationError as exc:
        return None, None, None, "OPTION_RUNTIME_INVALID", str(exc), False
    try:
        with _locked_artifact_directory(
            runtime.staging_root, research_root=runtime.research_root
        ) as staging_root:
            return _capture_historical_options_with_runtime(
                config,
                job,
                symbol=symbol,
                runtime=replace(runtime, staging_root=staging_root),
            )
    except OptionRuntimeValidationError as exc:
        return None, None, None, "OPTION_RUNTIME_INVALID", str(exc), False


def _capture_historical_options_with_runtime(
    config: CaptureConfig,
    job: CaptureJob,
    *,
    symbol: str,
    runtime: _ValidatedOptionRuntime,
) -> tuple[dict[str, Any] | None, Path | None, str | None, str | None, str | None, bool]:
    staging_dir = runtime.staging_root
    output = staging_dir / f"{sha256_bytes(job.job_id.encode())}.historical.json"
    try:
        with _managed_staging_output(output) as cleanup:
            result = _capture_historical_options_with_managed_output(
                config,
                job,
                symbol=symbol,
                runtime=runtime,
                output=output,
            )
    except ArtifactPublicationError as exc:
        return None, None, None, "OPTION_HISTORY_STAGING_CLEANUP_FAILED", str(exc), False
    return _apply_staging_cleanup_result(
        result,
        cleanup_error=cleanup.error_message,
        cleanup_kind="OPTION_HISTORY_STAGING_CLEANUP_FAILED",
    )


def _capture_historical_options_with_managed_output(
    config: CaptureConfig,
    job: CaptureJob,
    *,
    symbol: str,
    runtime: _ValidatedOptionRuntime,
    output: Path,
) -> tuple[dict[str, Any] | None, Path | None, str | None, str | None, str | None, bool]:
    try:
        result = run_hidden_process(
            [
                str(runtime.alpha_python),
                str(runtime.alpha_historical_script),
                "--chain-store-db",
                str(runtime.option_chain_store_db),
                "--pacing-db",
                str(runtime.historical_pacing_db),
                "--symbol",
                symbol,
                "--request-id",
                job.job_id,
                "--information-cutoff",
                utc_text(job.decision_at),
                "--output",
                str(output),
            ],
            cwd=runtime.alpha_runtime_root,
            timeout_seconds=config.historical_option_timeout_seconds,
            pass_fds=_inherited_descriptor_for(output),
        )
    except OSError as exc:
        return None, None, None, "OPTION_HISTORY_LAUNCH_FAILED", str(exc), False
    if result.timed_out:
        return (
            None,
            None,
            None,
            "OPTION_HISTORY_AMBIGUOUS_TIMEOUT",
            "alpha-core historical request timed out after possible pacing admission",
            False,
        )
    if result.returncode != 0:
        message = (result.stderr or result.stdout or f"exit {result.returncode}").strip()
        return None, None, None, "OPTION_HISTORY_PROCESS_FAILED", message, False
    try:
        raw = output.read_bytes()
        artifact = json.loads(raw)
        if not isinstance(artifact, dict):
            raise ValueError("historical option artifact root is not an object")
        _validate_historical_option_artifact(
            artifact,
            job=job,
            symbol=symbol,
            chain_store_db=runtime.option_chain_store_db,
        )
        rfc8785.dumps(artifact)
        destination, digest = _publish_content_addressed(
            runtime.options_root,
            raw,
            suffix=".json",
            research_root=runtime.research_root,
        )
        return artifact, destination, digest, None, None, False
    except ArtifactPublicationError as exc:
        return None, None, None, "OPTION_HISTORY_PUBLICATION_FAILED", str(exc), False
    except (KeyError, OSError, TypeError, json.JSONDecodeError, ValueError) as exc:
        return None, None, None, "OPTION_HISTORY_ARTIFACT_INVALID", str(exc), False


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


def _not_applicable_observation(
    *, source: str, observed_at: datetime, values: dict[str, Any] | None = None
) -> dict[str, Any]:
    return {
        "status": "not_applicable",
        "as_of_utc": None,
        "observed_at_utc": utc_text(observed_at),
        "source": source,
        "artifact_ref": None,
        "artifact_sha256": None,
        "values": values,
        "error": None,
    }


def _classification_boundary(decision_at: datetime) -> tuple[int, datetime]:
    local = decision_at.astimezone(ZoneInfo("America/New_York"))
    cutoff_local = datetime(local.year, 1, 1, tzinfo=local.tzinfo)
    return local.year, cutoff_local.astimezone(UTC)


@dataclass(slots=True, frozen=True)
class OwnerHistoryResult:
    classifier_version: str | None
    classification: dict[str, Any]
    observation: dict[str, Any]
    error: dict[str, Any] | None
    recorded_at: datetime


def _is_transient_sqlite_error(exc: sqlite3.OperationalError) -> bool:
    code = getattr(exc, "sqlite_errorcode", None)
    if isinstance(code, int) and (code & 0xFF) in {
        sqlite3.SQLITE_BUSY,
        sqlite3.SQLITE_LOCKED,
    }:
        return True
    return str(exc).strip().lower() in {
        "database is locked",
        "database table is locked",
    }


def _owner_history_failure(baseline: dict[str, Any], *, exc: BaseException) -> OwnerHistoryResult:
    recorded_at = datetime.now(UTC)
    error = _error(
        "owner_history",
        "OWNER_HISTORY_UNAVAILABLE",
        f"{type(exc).__name__}: {exc}",
        retryable=False,
    )
    return OwnerHistoryResult(
        classifier_version=None,
        classification=baseline,
        observation=_missing_observation(
            source="SEC-owner-history", observed_at=recorded_at, error=error
        ),
        error=error,
        recorded_at=recorded_at,
    )


def _capture_owner_history(
    config: CaptureConfig,
    job: CaptureJob,
    *,
    owner_cik: str | None,
    owner_mapping: str,
) -> OwnerHistoryResult:
    classification_year, cutoff_at = _classification_boundary(job.decision_at)
    baseline: dict[str, Any] = {
        "state": ("ambiguous_multi_owner" if owner_mapping == "ambiguous" else "unpartitionable"),
        "owner_cik": owner_cik,
        "classification_year": classification_year,
        "cutoff_at_utc": utc_text(cutoff_at),
        "transaction_owner_mapping": owner_mapping,
        "history_coverage_complete": False,
        "left_censored": True,
        "history_observation_start_date": None,
        "history_source_snapshot_sha256": None,
        "history_input_sha256": sha256_bytes(b"owner-history-not-captured"),
    }
    if owner_mapping == "ambiguous":
        recorded_at = datetime.now(UTC)
        return OwnerHistoryResult(
            classifier_version=None,
            classification=baseline,
            observation=_not_applicable_observation(
                source="SEC-owner-history", observed_at=recorded_at
            ),
            error=None,
            recorded_at=recorded_at,
        )
    if owner_mapping == "missing" or owner_cik is None:
        recorded_at = datetime.now(UTC)
        return OwnerHistoryResult(
            classifier_version=None,
            classification=baseline,
            observation=_missing_observation(source="SEC-owner-history", observed_at=recorded_at),
            error=None,
            recorded_at=recorded_at,
        )
    try:
        metadata = verify_history_runtime(config, as_of=job.decision_at)
        store = HistoryStore(config.history_db)
        _, coverage = store.coverage_for_classification(
            config.history_snapshot_sha256,
            classification_year=classification_year,
        )
        filings = store.owner_filings(
            config.history_snapshot_sha256,
            owner_cik,
            cutoff_date=date(classification_year, 1, 1),
        )
        result = classify_owner(
            owner_cik=owner_cik,
            classification_year=classification_year,
            filings=filings,
            coverage=coverage,
        )
        classification = {
            "state": result.state,
            "owner_cik": result.owner_cik,
            "classification_year": result.classification_year,
            "cutoff_at_utc": utc_text(cutoff_at),
            "transaction_owner_mapping": "exact",
            "history_coverage_complete": result.history_coverage_complete,
            "left_censored": result.left_censored,
            "history_observation_start_date": (result.history_observation_start_date.isoformat()),
            "history_source_snapshot_sha256": result.history_source_snapshot_sha256,
            "history_input_sha256": result.history_input_sha256,
        }
        recorded_at = datetime.now(UTC)
        observation = _captured_observation(
            source="SEC-owner-history:bulk-archives",
            as_of=metadata.created_at,
            observed_at=recorded_at,
            artifact_ref=f"sec-history-snapshot:{metadata.snapshot_sha256}",
            artifact_sha256=metadata.snapshot_sha256,
            values={
                "snapshot_sha256": metadata.snapshot_sha256,
                "manifest_sha256": metadata.manifest_sha256,
                "normalized_sha256": metadata.normalized_sha256,
                "snapshot_created_at_utc": utc_text(metadata.created_at),
                "first_quarter": metadata.first_quarter,
                "last_quarter": metadata.last_quarter,
                "coverage_complete_from": coverage.complete_from.isoformat(),
                "coverage_complete_through": coverage.complete_through.isoformat(),
                "missing_quarters": list(coverage.missing_quarters),
                "filing_count": len(filings),
                "classification_reason": result.reason,
                "routine_since_year": result.routine_since_year,
                "history_input_sha256": result.history_input_sha256,
            },
        )
        return OwnerHistoryResult(
            classifier_version=CLASSIFIER_VERSION,
            classification=classification,
            observation=observation,
            error=None,
            recorded_at=recorded_at,
        )
    except sqlite3.OperationalError as exc:
        if _is_transient_sqlite_error(exc):
            raise
        return _owner_history_failure(baseline, exc=exc)
    except (OSError, RuntimeError, ValueError, sqlite3.DatabaseError) as exc:
        return _owner_history_failure(baseline, exc=exc)


def verify_history_runtime(
    config: CaptureConfig, *, as_of: datetime | None = None
) -> SnapshotMetadata:
    """Authenticate sealed history immediately before an exact-owner classification."""

    if not config.history_db.is_file():
        raise ValueError("pinned owner-history database is missing")
    metadata = HistoryStore(config.history_db).verify_snapshot_material(
        config.history_snapshot_sha256
    )
    if metadata.created_at > (as_of or datetime.now(UTC)):
        raise ValueError("pinned owner-history snapshot was created after the decision")
    return metadata


def _configuration_sha(config: CaptureConfig) -> str:
    safe = asdict(config)
    for name in ("source_db", "evidence_db", "artifact_root"):
        safe.pop(name)
    for name in (
        "alpha_python",
        "alpha_script",
        "alpha_historical_script",
        "research_root",
        "option_chain_store_db",
        "historical_pacing_db",
        "canary_ledger",
        "history_db",
        "policy_path",
        "evidence_schema_path",
        "activation_db",
    ):
        safe[name] = str(getattr(config, name))
    return sha256_bytes(rfc8785.dumps(safe))


def _captured_option_observations(
    artifact: dict[str, Any],
    *,
    artifact_path: Path,
    artifact_sha256: str,
    observed_at: datetime,
    candidate: dict[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    source = str(artifact["source_id"])
    if artifact["schema_version"] == HISTORICAL_OPTION_SCHEMA_VERSION:
        cutoff = parse_utc(str(artifact["information_cutoff_utc"]))
        options = _captured_observation(
            source=source,
            as_of=cutoff,
            observed_at=observed_at,
            values={
                "schema_version": artifact["schema_version"],
                "status": artifact["artifact_status"],
                "capture_mode": artifact["capture_mode"],
                "information_cutoff_utc": artifact["information_cutoff_utc"],
                "target_count": len(artifact["targets"]),
                "bar_count": len(artifact["bars"]),
                "capture_error_count": len(artifact["capture_errors"]),
                "historical_request_count": artifact["historical_request_count"],
                "historical_pacing_units": artifact["historical_pacing_units"],
            },
            artifact_ref=str(artifact_path),
            artifact_sha256=artifact_sha256,
        )
        market = _captured_observation(
            source=source,
            as_of=cutoff,
            observed_at=observed_at,
            values={
                "underlying_reference": artifact["underlying_reference"],
                "option_chain_snapshot_observed_at_utc": artifact["option_chain_snapshot"][
                    "observed_at_utc"
                ],
                "option_chain_snapshot_staleness_seconds": artifact[
                    "option_chain_snapshot_staleness_seconds"
                ],
                "canary_daily_context": candidate,
            },
            artifact_ref=str(artifact_path),
            artifact_sha256=artifact_sha256,
        )
        return options, market
    completed_at = parse_utc(str(artifact["captured_at_utc"]))
    surfaces = artifact.get("surfaces", [])
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
    options = _captured_observation(
        source=source,
        as_of=completed_at,
        observed_at=observed_at,
        values={
            "schema_version": artifact["schema_version"],
            "status": artifact["artifact_status"],
            "quote_count": sum(
                len(surface.get("quotes", [])) for surface in surfaces if isinstance(surface, dict)
            ),
            "underlying_quotes": underlying_quotes,
            "bounds": {
                "min_dte_days": artifact.get("min_dte_days"),
                "max_dte_days": artifact.get("max_dte_days"),
                "max_expiries": artifact.get("max_expiries"),
                "max_contracts_per_expiry": artifact.get("max_contracts_per_expiry"),
            },
        },
        artifact_ref=str(artifact_path),
        artifact_sha256=artifact_sha256,
    )
    market = _captured_observation(
        source=source,
        as_of=completed_at,
        observed_at=observed_at,
        values={
            "underlying_quotes": underlying_quotes,
            "canary_daily_context": candidate,
        },
        artifact_ref=str(artifact_path),
        artifact_sha256=artifact_sha256,
    )
    return options, market


def _append_snapshot(
    config: CaptureConfig,
    job: CaptureJob,
    *,
    capture_window: CaptureWindow,
    capture_started: datetime,
    capture_finished: datetime,
    timer_started: float,
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
            {str(value) for value in payload_owner_ciks if isinstance(value, str) and value}
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
    owner_history = _capture_owner_history(
        config,
        job,
        owner_cik=owner_cik,
        owner_mapping=owner_mapping,
    )
    recorded_at = owner_history.recorded_at
    duration_ms = max(0, round((monotonic() - timer_started) * 1000))
    owner_history_observation = owner_history.observation
    issuer_cik = str(payload.get("issuer_cik") or job.issuer_cik).lstrip("0") or "0"
    notification_at = (
        parse_utc(str(filing["notification_sent_at"]))
        if filing.get("notification_sent_at")
        else None
    )
    errors = [error for error in (option_error, owner_history.error) if error is not None]
    option_not_applicable = (
        option_artifact is not None
        and option_artifact.get("schema_version") == OPTION_SURFACE_RESULT_SCHEMA_VERSION
        and option_artifact.get("status") == "not_applicable"
    )
    if option_artifact is not None and option_path is not None and option_sha is not None:
        options_observation, market_observation = _captured_option_observations(
            option_artifact,
            artifact_path=option_path,
            artifact_sha256=option_sha,
            observed_at=capture_finished,
            candidate=candidate,
        )
    else:
        options_observation = (
            _not_applicable_observation(
                source=str(option_artifact["source_id"]),
                observed_at=parse_utc(str(option_artifact["observed_at_utc"])),
                values={
                    "schema_version": option_artifact["schema_version"],
                    "reason_code": option_artifact["reason_code"],
                    "request_id": option_artifact["request_id"],
                    "symbol": option_artifact["symbol"],
                    "client_id": option_artifact["client_id"],
                },
            )
            if option_not_applicable and option_artifact is not None
            else _missing_observation(
                source="alpha-core", observed_at=capture_finished, error=option_error
            )
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
            "recorded_at_utc": utc_text(recorded_at),
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
                    "policy_sha256": capture_window.policy_sha256,
                    "classifier_version": owner_history.classifier_version,
                    "model_id": _optional_string(decision.get("model_id")),
                    "prompt_sha256": _optional_sha256(decision.get("prompt_sha256")),
                    "configuration_sha256": _configuration_sha(config),
                },
                "classification": owner_history.classification,
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
                    "owner_history": owner_history_observation,
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
        _validate_snapshot(
            config,
            snapshot,
            expected_policy_sha256=capture_window.policy_sha256,
        )
        record_bytes = rfc8785.dumps(snapshot)
        snapshot_root = _confined_artifact_subdirectory(
            config.artifact_root,
            "snapshots",
            research_root=config.research_root,
        )
        _publish_content_addressed(
            snapshot_root,
            record_bytes,
            suffix=".json",
            research_root=config.research_root,
        )
        conn.execute(
            """
            INSERT INTO evidence_snapshots(
                sequence, snapshot_id, job_id, record_sha256, stored_bytes_sha256,
                record_json, recorded_at_utc, owner_history_status
            ) VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                sequence,
                snapshot_id,
                job.job_id,
                record_sha,
                sha256_bytes(record_bytes),
                record_bytes,
                utc_text(recorded_at),
                str(owner_history_observation["status"]),
            ),
        )
        conn.commit()
    return record_sha


def _write_health(
    evidence_db: Path,
    *,
    now: datetime,
    result: str,
    job_id: str | None,
    error_kind: str | None = None,
    error_message: str | None = None,
) -> None:
    ensure_evidence_store(evidence_db)
    with _connect(evidence_db, write=True) as conn:
        conn.execute(
            """
            INSERT INTO capture_health(
              singleton,last_worker_heartbeat_utc,last_result,last_job_id,
              last_error_kind,last_error_message
            ) VALUES(1,?,?,?,?,?)
            ON CONFLICT(singleton) DO UPDATE SET
              last_worker_heartbeat_utc=excluded.last_worker_heartbeat_utc,
              last_result=excluded.last_result,
              last_job_id=excluded.last_job_id,
              last_error_kind=excluded.last_error_kind,
              last_error_message=excluded.last_error_message
            """,
            (
                utc_text(now),
                result,
                job_id,
                error_kind,
                error_message[:MAX_ERROR_LENGTH] if error_message else None,
            ),
        )


def record_worker_failure(evidence_db: Path, exc: BaseException) -> None:
    """Persist a fatal one-shot worker failure when no capture job can own it."""

    _write_health(
        evidence_db,
        now=datetime.now(UTC),
        result="worker_error",
        job_id=None,
        error_kind="CAPTURE_WORKER_FATAL",
        error_message=f"{type(exc).__name__}: {exc}",
    )


def _heartbeat(config: CaptureConfig, *, now: datetime, result: str, job_id: str | None) -> None:
    _write_health(config.evidence_db, now=now, result=result, job_id=job_id)


def _validated_capture_window(
    config: CaptureConfig, *, now: datetime | None = None
) -> CaptureWindow:
    try:
        policy_bytes = config.policy_path.read_bytes()
        registry = json.loads(policy_bytes)
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("unable to load the prospective registry") from exc
    if not isinstance(registry, dict):
        raise ValueError("prospective registry must be a JSON object")
    status = registry.get("status")
    if status == "draft":
        _validate_registry(registry, allow_draft=True)
        validate_deployed_registry_state(
            registry,
            config.activation_db,
            registry_bytes=policy_bytes,
            now=now,
        )
        return CaptureWindow(status="draft", policy_sha256=sha256_bytes(policy_bytes))
    if status != "active":
        raise ValueError("prospective registry is neither draft nor active")
    _validate_registry(registry, allow_draft=False)
    phase = validate_deployed_registry_state(
        registry,
        config.activation_db,
        registry_bytes=policy_bytes,
        now=now,
    )
    activation = registry.get("activation")
    if not isinstance(activation, dict):
        raise ValueError("active registry has no activation record")
    activated_at = parse_utc(str(activation.get("activated_at_utc", "")))
    return CaptureWindow(
        status=phase,
        policy_sha256=sha256_bytes(policy_bytes),
        activated_at=activated_at,
        deadline=enrollment_deadline(activated_at),
    )


def _process_claimed_job(
    config: CaptureConfig,
    job: CaptureJob,
    *,
    capture_window: CaptureWindow,
) -> CaptureResult:
    started = datetime.now(UTC)
    timer = monotonic()
    existing_sha = _existing_snapshot_sha(config.evidence_db, job.job_id)
    existing_attempt = _attempt_exists(config, job)
    if existing_sha is not None:
        if existing_attempt:
            _finish_job_without_new_attempt(
                config,
                job,
                finished_at=started,
                job_state="complete",
                error_kind=None,
                error_message=None,
                record_sha256=existing_sha,
            )
        else:
            _finish_job_attempt(
                config,
                job,
                started_at=started,
                finished_at=started,
                attempt_status="completed",
                job_state="complete",
                error_kind=None,
                error_message=None,
                retryable=False,
                record_sha256=existing_sha,
            )
        _heartbeat(config, now=started, result="recovered_existing", job_id=job.job_id)
        return CaptureResult(
            status="completed",
            job_id=job.job_id,
            snapshot_sha256=existing_sha,
            option_status="recovered_existing",
        )
    if existing_attempt and job.attempt_count >= config.max_attempts:
        exhaustion_kind = "CAPTURE_ATTEMPTS_EXHAUSTED"
        exhaustion_message = "capture attempts exhausted before finalization"
        _finish_job_without_new_attempt(
            config,
            job,
            finished_at=started,
            job_state="failed",
            error_kind=exhaustion_kind,
            error_message=exhaustion_message,
        )
        _heartbeat(config, now=started, result="failed", job_id=job.job_id)
        return CaptureResult(status="failed", job_id=job.job_id, option_status=exhaustion_kind)
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
        _finish_job_attempt(
            config,
            job,
            started_at=started,
            finished_at=finished,
            attempt_status="retry",
            job_state="retry",
            error_kind=error_kind,
            error_message=error_message,
            retryable=True,
        )
        _heartbeat(config, now=finished, result="retry_scheduled", job_id=job.job_id)
        return CaptureResult(status="retry_scheduled", job_id=job.job_id, option_status=error_kind)
    option_error = (
        _error("options_surface", error_kind, error_message or error_kind, retryable=False)
        if error_kind
        else None
    )
    _renew_job_lease(config, job, now=datetime.now(UTC))
    record_sha = _append_snapshot(
        config,
        job,
        capture_window=capture_window,
        capture_started=started,
        capture_finished=finished,
        timer_started=timer,
        option_artifact=option_artifact,
        option_path=option_path,
        option_sha=option_sha,
        option_error=option_error,
    )
    completed = datetime.now(UTC)
    _finish_job_attempt(
        config,
        job,
        started_at=started,
        finished_at=completed,
        attempt_status="completed",
        job_state="complete",
        error_kind=error_kind,
        error_message=error_message,
        retryable=False,
        record_sha256=record_sha,
    )
    _heartbeat(config, now=completed, result="completed", job_id=job.job_id)
    return CaptureResult(
        status="completed",
        job_id=job.job_id,
        snapshot_sha256=record_sha,
        option_status=(
            "captured_historical"
            if option_artifact
            and option_artifact.get("schema_version") == HISTORICAL_OPTION_SCHEMA_VERSION
            else "not_applicable"
            if option_artifact
            and option_artifact.get("schema_version") == OPTION_SURFACE_RESULT_SCHEMA_VERSION
            and option_artifact.get("status") == "not_applicable"
            else "captured"
            if option_artifact
            else error_kind
        ),
    )


def run_capture_once(
    config: CaptureConfig,
    *,
    now: datetime | None = None,
    worker_id: str | None = None,
) -> CaptureResult:
    resolved_research_root = Path(os.path.abspath(config.research_root)).resolve(strict=True)
    config = replace(
        config,
        artifact_root=_confined_artifact_root(
            config.artifact_root,
            research_root=config.research_root,
        ),
        research_root=resolved_research_root,
    )
    now = (now or datetime.now(UTC)).astimezone(UTC)
    worker_id = worker_id or f"{platform.node()}:{os.getpid()}"
    ensure_evidence_store(config.evidence_db)
    window = _validated_capture_window(config, now=now)
    if window.status != "active":
        _heartbeat(config, now=now, result=f"idle_registry_{window.status}", job_id=None)
        return CaptureResult(status="idle")
    # Acquire cross-process ownership before leasing a job. A process waiting for
    # another option capture therefore cannot let its database lease expire before
    # it has begun work.
    lock_wait_started = perf_counter()
    with _artifact_process_mutex(config.research_root):
        claim_now = now + timedelta(seconds=perf_counter() - lock_wait_started)
        return _run_active_capture_once(
            config,
            now=claim_now,
            worker_id=worker_id,
            window=window,
        )


def _run_active_capture_once(
    config: CaptureConfig,
    *,
    now: datetime,
    worker_id: str,
    window: CaptureWindow,
) -> CaptureResult:
    job = _claim_job(config, worker_id=worker_id, now=now, window=window)
    if job is None:
        _heartbeat(config, now=now, result="idle", job_id=None)
        return CaptureResult(status="idle")
    try:
        return _process_claimed_job(config, job, capture_window=window)
    except ProcessTreeCleanupError:
        raise
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
        _finish_job_attempt(
            config,
            job,
            started_at=finished,
            finished_at=finished,
            attempt_status=state,
            job_state=state,
            error_kind=error_kind,
            error_message=message,
            retryable=retryable,
        )
        _heartbeat(config, now=finished, result=state, job_id=job.job_id)
        return CaptureResult(
            status="retry_scheduled" if retryable else "failed",
            job_id=job.job_id,
            option_status=error_kind,
        )


def capture_status(source_db: Path, evidence_db: Path) -> dict[str, Any]:
    result: dict[str, Any] = {
        "jobs": {},
        "evidence_count": 0,
        "owner_history": {},
        "health": None,
    }
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
            owner_history_counts: dict[str, int] = {
                str(row["owner_history_status"]): int(row["count"])
                for row in conn.execute(
                    "SELECT owner_history_status,COUNT(*) count "
                    "FROM evidence_snapshots WHERE owner_history_status IS NOT NULL "
                    "GROUP BY owner_history_status"
                )
            }
            for snapshot_row in conn.execute(
                "SELECT record_json FROM evidence_snapshots "
                "WHERE owner_history_status IS NULL ORDER BY sequence"
            ):
                try:
                    record = json.loads(bytes(snapshot_row["record_json"]))
                    status = str(record["payload"]["observations"]["owner_history"]["status"])
                except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                    status = "invalid_record"
                owner_history_counts[status] = owner_history_counts.get(status, 0) + 1
            result["owner_history"] = owner_history_counts
            row = conn.execute("SELECT * FROM capture_health WHERE singleton=1").fetchone()
            result["health"] = dict(row) if row else None
    return result


def resolve_git_commit(repo_root: Path, *, timeout_seconds: int = 10) -> str:
    if timeout_seconds < 1:
        raise ValueError("git resolution timeout must be at least one second")
    result = run_hidden_process(
        ["git", "rev-parse", "HEAD"],
        cwd=repo_root,
        timeout_seconds=timeout_seconds,
    )
    commit = result.stdout.strip().lower()
    if result.timed_out or result.returncode != 0 or len(commit) != 40:
        raise RuntimeError("unable to resolve insider-alerts deployment commit")
    if any(char not in "0123456789abcdef" for char in commit):
        raise RuntimeError("git returned an invalid deployment commit")
    return commit
