"""Append-only, future-dated activation preparation for OPP-E07-V1."""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import sqlite3
import subprocess
import tempfile
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

import rfc8785

from insider_alerts.research.inference import (
    HYPOTHESIS_ID,
    TrialInvalid,
    TrialSealStore,
    _artifact_sha256,
    _file_sha256,
    _git_blob,
    _git_commit_sha,
    _reviewed_repo_root,
    _validate_registry,
    registry_definition_sha256,
)
from insider_alerts.research.sec_history import CLASSIFIER_VERSION

ACTIVATION_SCHEMA_VERSION = 1
MINIMUM_ACTIVATION_LEAD = timedelta(hours=2)
ACTIVATION_ARTIFACT = Path("src/insider_alerts/research/activation.py")
REGISTRY_ARTIFACT = Path("docs/research/registry/OPP-E07-V1.json")

EVIDENCE_TABLES = ("evidence_snapshots",)
TRIAL_TABLES = (
    "trial_candidates",
    "trial_evidence_dispositions",
    "trial_resolutions",
    "trial_entry_date_completions",
    "trial_entry_date_lapses",
    "trial_outcomes",
    "trial_faults",
)
DIAGNOSTIC_TABLES = (
    "diagnostic_candidates",
    "diagnostic_evidence_bindings",
    "diagnostic_state_bindings",
    "diagnostic_reconciliations",
    "diagnostic_outcomes",
    "diagnostic_outcome_receipts",
)
SEAL_TABLES = ("trial_receipts", "terminal_pending", "decision_report")


class ActivationInvalid(RuntimeError):
    """Activation preparation or verification failed closed."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True, slots=True)
class ActivationConfig:
    evidence_db: Path
    trial_db: Path
    diagnostics_db: Path
    seal_db: Path
    activation_db: Path
    registry_path: Path
    artifact_root: Path


@dataclass(frozen=True, slots=True)
class ActivationResult:
    status: Literal["unprepared", "prepared", "active", "invalid"]
    activated_at_utc: str | None = None
    prepared_at_utc: str | None = None
    activation_receipt_sha256: str | None = None
    active_registry_sha256: str | None = None
    registry_artifact_path: str | None = None
    reason: str | None = None


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None:
        raise ActivationInvalid("activation_timestamp_naive")
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _parse_utc(value: object, name: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ActivationInvalid(f"{name}_invalid")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ActivationInvalid(f"{name}_invalid") from exc
    return parsed.astimezone(UTC)


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def activation_receipt(registry: Mapping[str, Any]) -> dict[str, Any]:
    """Derive the receipt whose digest is embedded in an active registry."""

    activation = registry.get("activation")
    if not isinstance(activation, Mapping):
        raise ActivationInvalid("activation_record_missing")
    record = {
        "schema_version": ACTIVATION_SCHEMA_VERSION,
        "hypothesis_id": HYPOTHESIS_ID,
        "kind": "prospective_activation",
        **{
            str(key): value
            for key, value in activation.items()
            if key != "activation_receipt_sha256"
        },
    }
    digest = _sha256(rfc8785.dumps(record))
    return {**record, "receipt_sha256": digest}


def validate_activation_receipt_digest(registry: Mapping[str, Any]) -> dict[str, Any]:
    """Validate the self-contained activation receipt commitment."""

    activation = registry.get("activation")
    if not isinstance(activation, Mapping):
        raise ActivationInvalid("activation_record_missing")
    receipt = activation_receipt(registry)
    if activation.get("activation_receipt_sha256") != receipt["receipt_sha256"]:
        raise ActivationInvalid("activation_receipt_digest_mismatch")
    prepared_at = _parse_utc(activation.get("activation_prepared_at_utc"), "prepared_at")
    activated_at = _parse_utc(activation.get("activated_at_utc"), "activated_at")
    if prepared_at >= activated_at:
        raise ActivationInvalid("activation_not_prospective")
    return receipt


class ActivationStore:
    """Immutable singleton receipt plus the exact active registry bytes."""

    def __init__(self, path: Path, *, initialize: bool = True) -> None:
        self.path = path
        if not initialize:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with contextlib.closing(self._connect()) as conn, conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS activation_receipt(
                  singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                  receipt_json BLOB NOT NULL,
                  receipt_sha256 TEXT NOT NULL UNIQUE,
                  active_registry_json BLOB NOT NULL,
                  active_registry_sha256 TEXT NOT NULL UNIQUE
                );
                CREATE TRIGGER IF NOT EXISTS activation_receipt_no_update
                BEFORE UPDATE ON activation_receipt
                BEGIN SELECT RAISE(ABORT,'activation receipt is append-only'); END;
                CREATE TRIGGER IF NOT EXISTS activation_receipt_no_delete
                BEFORE DELETE ON activation_receipt
                BEGIN SELECT RAISE(ABORT,'activation receipt is append-only'); END;
                CREATE TABLE IF NOT EXISTS activation_armed_attestation(
                  singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                  active_registry_sha256 TEXT NOT NULL UNIQUE,
                  armed_at_utc TEXT NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS activation_armed_attestation_no_update
                BEFORE UPDATE ON activation_armed_attestation
                BEGIN SELECT RAISE(ABORT,'armed attestation is append-only'); END;
                CREATE TRIGGER IF NOT EXISTS activation_armed_attestation_no_delete
                BEFORE DELETE ON activation_armed_attestation
                BEGIN SELECT RAISE(ABORT,'armed attestation is append-only'); END;
                """
            )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        return conn

    def _read_connect(self) -> sqlite3.Connection:
        uri = f"file:{self.path.resolve().as_posix()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _row(self) -> sqlite3.Row | None:
        if not self.path.is_file():
            return None
        try:
            with contextlib.closing(self._read_connect()) as conn:
                row: sqlite3.Row | None = conn.execute(
                    "SELECT * FROM activation_receipt WHERE singleton=1"
                ).fetchone()
                return row
        except sqlite3.Error as exc:
            raise ActivationInvalid("activation_store_invalid") from exc

    def receipt(self) -> dict[str, Any] | None:
        row = self._row()
        if row is None:
            return None
        raw = bytes(row["receipt_json"])
        parsed = json.loads(raw)
        if not isinstance(parsed, dict) or rfc8785.dumps(parsed) != raw:
            raise ActivationInvalid("stored_activation_receipt_not_canonical")
        unsigned = dict(parsed)
        digest = unsigned.pop("receipt_sha256", None)
        if digest != _sha256(rfc8785.dumps(unsigned)) or row["receipt_sha256"] != digest:
            raise ActivationInvalid("stored_activation_receipt_digest_mismatch")
        registry_raw = bytes(row["active_registry_json"])
        registry = json.loads(registry_raw)
        if (
            not isinstance(registry, dict)
            or rfc8785.dumps(registry) != registry_raw
            or row["active_registry_sha256"] != _sha256(registry_raw)
        ):
            raise ActivationInvalid("stored_active_registry_invalid")
        expected = activation_receipt(registry)
        if parsed != expected:
            raise ActivationInvalid("stored_activation_receipt_registry_mismatch")
        return parsed

    def active_registry(self) -> dict[str, Any] | None:
        row = self._row()
        if row is None:
            return None
        self.receipt()
        parsed = json.loads(bytes(row["active_registry_json"]))
        if not isinstance(parsed, dict):
            raise ActivationInvalid("stored_active_registry_not_object")
        return parsed

    def active_registry_bytes(self) -> bytes | None:
        row = self._row()
        if row is None:
            return None
        self.receipt()
        return bytes(row["active_registry_json"])

    def put(self, registry: Mapping[str, Any]) -> dict[str, Any]:
        receipt = validate_activation_receipt_digest(registry)
        receipt_bytes = rfc8785.dumps(receipt)
        registry_bytes = rfc8785.dumps(dict(registry))
        registry_digest = _sha256(registry_bytes)
        with contextlib.closing(self._connect()) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM activation_receipt WHERE singleton=1"
            ).fetchone()
            if row is not None:
                if (
                    bytes(row["receipt_json"]) != receipt_bytes
                    or row["receipt_sha256"] != receipt["receipt_sha256"]
                    or bytes(row["active_registry_json"]) != registry_bytes
                    or row["active_registry_sha256"] != registry_digest
                ):
                    raise ActivationInvalid("alternate_activation_prohibited")
                return receipt
            conn.execute(
                "INSERT INTO activation_receipt VALUES(1,?,?,?,?)",
                (
                    receipt_bytes,
                    receipt["receipt_sha256"],
                    registry_bytes,
                    registry_digest,
                ),
            )
        return receipt

    def verify_active(
        self, registry: Mapping[str, Any], deployed_registry_bytes: bytes
    ) -> dict[str, Any]:
        receipt = self.receipt()
        if receipt is None:
            raise ActivationInvalid("activation_receipt_missing")
        stored_bytes = self.active_registry_bytes()
        if stored_bytes is None:
            raise ActivationInvalid("prepared_active_registry_missing")
        if deployed_registry_bytes != stored_bytes:
            raise ActivationInvalid("active_registry_bytes_do_not_match_receipt")
        deployed = json.loads(deployed_registry_bytes)
        if not isinstance(deployed, dict) or deployed != dict(registry):
            raise ActivationInvalid("deployed_registry_parse_mismatch")
        if self.active_registry() != deployed:
            raise ActivationInvalid("active_registry_does_not_match_receipt")
        expected = validate_activation_receipt_digest(deployed)
        if receipt != expected:
            raise ActivationInvalid("active_registry_receipt_mismatch")
        return receipt

    def attest_armed(
        self,
        deployed_registry_bytes: bytes,
        *,
        armed_at: datetime,
        activated_at: datetime,
    ) -> None:
        if armed_at >= activated_at:
            raise ActivationInvalid("activation_armed_attestation_not_pre_boundary")
        registry_digest = _sha256(deployed_registry_bytes)
        armed_text = _utc_text(armed_at)
        with contextlib.closing(self._connect()) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM activation_armed_attestation WHERE singleton=1"
            ).fetchone()
            if row is not None:
                if row["active_registry_sha256"] != registry_digest:
                    raise ActivationInvalid("alternate_armed_attestation_prohibited")
                existing_armed_at = _parse_utc(row["armed_at_utc"], "armed_at")
                if existing_armed_at >= activated_at:
                    raise ActivationInvalid("activation_armed_attestation_not_pre_boundary")
                return
            conn.execute(
                "INSERT INTO activation_armed_attestation VALUES(1,?,?)",
                (registry_digest, armed_text),
            )

    def verify_armed(
        self, deployed_registry_bytes: bytes, *, activated_at: datetime
    ) -> None:
        try:
            with contextlib.closing(self._read_connect()) as conn:
                row = conn.execute(
                    "SELECT * FROM activation_armed_attestation WHERE singleton=1"
                ).fetchone()
        except sqlite3.Error as exc:
            raise ActivationInvalid("activation_store_invalid") from exc
        if row is None:
            raise ActivationInvalid("activation_armed_attestation_missing")
        if row["active_registry_sha256"] != _sha256(deployed_registry_bytes):
            raise ActivationInvalid("activation_armed_attestation_registry_mismatch")
        armed_at = _parse_utc(row["armed_at_utc"], "armed_at")
        if armed_at >= activated_at:
            raise ActivationInvalid("activation_armed_attestation_not_pre_boundary")


def validate_deployed_registry_state(
    registry: Mapping[str, Any],
    activation_db: Path,
    *,
    registry_bytes: bytes,
    now: datetime | None = None,
) -> Literal["draft", "armed", "active"]:
    """Enforce the prepared-to-active state transition for every runtime consumer."""

    checked_at = (now or datetime.now(UTC)).astimezone(UTC)
    store = ActivationStore(activation_db, initialize=False)
    receipt = store.receipt()
    status = registry.get("status")
    if status == "draft":
        if receipt is None:
            return "draft"
        active = store.active_registry()
        if active is None or not isinstance(active.get("activation"), Mapping):
            raise ActivationInvalid("prepared_active_registry_missing")
        activated_at = _parse_utc(active["activation"].get("activated_at_utc"), "activated_at")
        if registry_definition_sha256(active) != registry_definition_sha256(registry):
            raise ActivationInvalid("prepared_registry_definition_changed")
        if checked_at >= activated_at:
            raise ActivationInvalid("activation_boundary_passed_while_registry_draft")
        return "draft"
    if status != "active":
        raise ActivationInvalid("deployed_registry_status_invalid")
    store.verify_active(registry, registry_bytes)
    activation = registry.get("activation")
    if not isinstance(activation, Mapping):
        raise ActivationInvalid("activation_record_missing")
    activated_at = _parse_utc(activation.get("activated_at_utc"), "activated_at")
    if checked_at < activated_at:
        store.attest_armed(
            registry_bytes,
            armed_at=checked_at,
            activated_at=activated_at,
        )
        return "armed"
    store.verify_armed(registry_bytes, activated_at=activated_at)
    return "active"


def _artifact_expectations(registry: Mapping[str, Any]) -> dict[str, Path]:
    return {
        "preregistration_sha256": Path(str(registry["preregistration"])),
        "hypothesis_schema_sha256": Path(
            "docs/research/contracts/hypothesis-registry.schema.json"
        ),
        "evidence_schema_sha256": Path("docs/research/contracts/evidence-snapshot.schema.json"),
        "inference_artifact_sha256": Path("src/insider_alerts/research/inference.py"),
        "terminal_builder_artifact_sha256": Path(
            "src/insider_alerts/research/terminal_builder.py"
        ),
        "activation_artifact_sha256": ACTIVATION_ARTIFACT,
        "dependency_lock_sha256": Path("uv.lock"),
        "policy_sha256": Path(str(registry["strategy"]["policy_artifact"])),
    }


def _git_state(repo_root: Path) -> tuple[str, str, str, bool]:
    flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)

    def run(*args: str) -> str:
        try:
            completed = subprocess.run(
                ["git", *args],
                cwd=repo_root,
                check=True,
                capture_output=True,
                text=True,
                timeout=10,
                creationflags=flags,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise ActivationInvalid("activation_git_state_unverifiable") from exc
        return completed.stdout.strip()

    run("fetch", "--quiet", "origin", "main")
    branch = run("branch", "--show-current")
    head = run("rev-parse", "HEAD")
    origin = run("rev-parse", "origin/main")
    clean = run("status", "--porcelain") == ""
    return branch, head, origin, clean


def _initialize_scientific_stores(config: ActivationConfig) -> None:
    from insider_alerts.research.capture import ensure_evidence_store
    from insider_alerts.research.diagnostics import DiagnosticStore
    from insider_alerts.research.trial_runtime import TrialStore

    ensure_evidence_store(config.evidence_db)
    TrialStore(config.trial_db).validate_integrity()
    DiagnosticStore(config.diagnostics_db).validate_integrity()
    TrialSealStore(config.seal_db)


@contextlib.contextmanager
def _locked_empty_stores(config: ActivationConfig) -> Iterator[None]:
    paths_and_tables: tuple[tuple[Path, Sequence[str]], ...] = (
        (config.evidence_db, EVIDENCE_TABLES),
        (config.trial_db, TRIAL_TABLES),
        (config.diagnostics_db, DIAGNOSTIC_TABLES),
        (config.seal_db, SEAL_TABLES),
    )
    connections: list[sqlite3.Connection] = []
    try:
        for path, _tables in paths_and_tables:
            conn = sqlite3.connect(path, timeout=30)
            connections.append(conn)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute("BEGIN IMMEDIATE")
        for conn, (_path, tables) in zip(connections, paths_and_tables, strict=True):
            for table in tables:
                count = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                if count:
                    raise ActivationInvalid(f"activation_store_not_empty:{table}")
        yield
    finally:
        for conn in reversed(connections):
            with contextlib.suppress(sqlite3.Error):
                conn.rollback()
            with contextlib.suppress(sqlite3.Error):
                conn.close()


def _publish_registry_artifact(root: Path, registry: Mapping[str, Any]) -> Path:
    encoded = rfc8785.dumps(dict(registry))
    digest = _sha256(encoded)
    directory = root / "activation-artifacts"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{digest}.registry.json"
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
                raise ActivationInvalid("activation_artifact_path_collision") from None
        if os.name != "nt":
            directory_descriptor = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
    finally:
        temporary_path.unlink(missing_ok=True)
    return path


def _active_registry(
    draft: Mapping[str, Any],
    *,
    implementation_commit: str,
    prepared_at: datetime,
    activated_at: datetime,
    repo_root: Path,
) -> dict[str, Any]:
    loaded = json.loads(json.dumps(dict(draft)))
    if not isinstance(loaded, dict):  # pragma: no cover - a mapping always round-trips to an object
        raise ActivationInvalid("draft_registry_round_trip_invalid")
    registry: dict[str, Any] = loaded
    registry["status"] = "active"
    activation: dict[str, Any] = {
        "activation_prepared_at_utc": _utc_text(prepared_at),
        "activated_at_utc": _utc_text(activated_at),
        "activation_git_commit": implementation_commit,
        "registry_definition_sha256": registry_definition_sha256(registry),
        "classifier_version": CLASSIFIER_VERSION,
        "enrollment_start_sequence": 1,
    }
    for field, relative_path in _artifact_expectations(registry).items():
        working_digest = _file_sha256(repo_root / relative_path)
        git_digest = _artifact_sha256(_git_blob(repo_root, implementation_commit, relative_path))
        if working_digest != git_digest:
            raise ActivationInvalid(f"activation_artifact_not_at_implementation_commit:{field}")
        activation[field] = working_digest
    registry["activation"] = activation
    activation["activation_receipt_sha256"] = activation_receipt(registry)["receipt_sha256"]
    return registry


def prepare_activation(
    config: ActivationConfig,
    *,
    activated_at: datetime,
    now: datetime | None = None,
    minimum_lead: timedelta = MINIMUM_ACTIVATION_LEAD,
) -> ActivationResult:
    """Seal one future boundary and publish the exact registry bytes for review."""

    prepared_at = (now or datetime.now(UTC)).astimezone(UTC)
    if activated_at.tzinfo is None:
        raise ActivationInvalid("activation_timestamp_naive")
    activated_at = activated_at.astimezone(UTC)
    repo_root = _reviewed_repo_root()
    branch, head, origin, clean = _git_state(repo_root)
    if branch != "main" or head != origin or not clean:
        raise ActivationInvalid("activation_checkout_not_clean_synced_main")
    implementation_commit = _git_commit_sha(head)
    draft = json.loads(config.registry_path.read_text(encoding="utf-8"))
    if not isinstance(draft, dict):
        raise ActivationInvalid("draft_registry_not_object")
    if draft.get("status") != "draft":
        raise ActivationInvalid("activation_prepare_requires_draft_registry")
    try:
        _validate_registry(draft, allow_draft=True)
    except TrialInvalid as exc:
        raise ActivationInvalid(f"draft_registry_invalid:{exc}") from exc
    _initialize_scientific_stores(config)
    store = ActivationStore(config.activation_db)
    existing = store.active_registry()
    if existing is not None:
        existing_activation = existing.get("activation")
        if not isinstance(existing_activation, Mapping):
            raise ActivationInvalid("stored_active_registry_activation_missing")
        if _parse_utc(existing_activation.get("activated_at_utc"), "activated_at") != activated_at:
            raise ActivationInvalid("alternate_activation_prohibited")
        if prepared_at >= activated_at:
            raise ActivationInvalid("activation_boundary_passed_before_registry_publication")
        if registry_definition_sha256(existing) != registry_definition_sha256(draft):
            raise ActivationInvalid("stored_active_registry_definition_mismatch")
        with _locked_empty_stores(config):
            stored_bytes = store.active_registry_bytes()
            if stored_bytes is None:
                raise ActivationInvalid("prepared_active_registry_missing")
            receipt = store.verify_active(existing, stored_bytes)
        artifact = _publish_registry_artifact(config.artifact_root, existing)
        return ActivationResult(
            "prepared",
            activated_at_utc=str(existing_activation["activated_at_utc"]),
            prepared_at_utc=str(existing_activation["activation_prepared_at_utc"]),
            activation_receipt_sha256=str(receipt["receipt_sha256"]),
            active_registry_sha256=_sha256(rfc8785.dumps(existing)),
            registry_artifact_path=str(artifact.resolve()),
        )
    if activated_at - prepared_at < minimum_lead:
        raise ActivationInvalid("activation_lead_time_insufficient")
    active = _active_registry(
        draft,
        implementation_commit=implementation_commit,
        prepared_at=prepared_at,
        activated_at=activated_at,
        repo_root=repo_root,
    )
    try:
        _validate_registry(active, allow_draft=False)
    except TrialInvalid as exc:
        raise ActivationInvalid(f"active_registry_invalid:{exc}") from exc
    validate_activation_receipt_digest(active)
    with _locked_empty_stores(config):
        receipt = store.put(active)
    artifact = _publish_registry_artifact(config.artifact_root, active)
    return ActivationResult(
        "prepared",
        activated_at_utc=str(active["activation"]["activated_at_utc"]),
        prepared_at_utc=str(active["activation"]["activation_prepared_at_utc"]),
        activation_receipt_sha256=str(receipt["receipt_sha256"]),
        active_registry_sha256=_sha256(rfc8785.dumps(active)),
        registry_artifact_path=str(artifact.resolve()),
    )


def activation_status(config: ActivationConfig, *, now: datetime | None = None) -> ActivationResult:
    try:
        registry_bytes = config.registry_path.read_bytes()
        registry = json.loads(registry_bytes)
        if not isinstance(registry, dict):
            raise ActivationInvalid("deployed_registry_not_object")
        _validate_registry(registry, allow_draft=registry.get("status") == "draft")
        validate_deployed_registry_state(
            registry,
            config.activation_db,
            registry_bytes=registry_bytes,
            now=now,
        )
        store = ActivationStore(config.activation_db, initialize=False)
        receipt = store.receipt()
        if receipt is None:
            return ActivationResult("unprepared")
        active = store.active_registry()
        if active is None:
            raise ActivationInvalid("prepared_active_registry_missing")
        activation = active["activation"]
        artifact_digest = _sha256(rfc8785.dumps(active))
        artifact = config.artifact_root / "activation-artifacts" / (
            f"{artifact_digest}.registry.json"
        )
        return ActivationResult(
            "active" if registry.get("status") == "active" else "prepared",
            activated_at_utc=str(activation["activated_at_utc"]),
            prepared_at_utc=str(activation["activation_prepared_at_utc"]),
            activation_receipt_sha256=str(receipt["receipt_sha256"]),
            active_registry_sha256=artifact_digest,
            registry_artifact_path=str(artifact.resolve()),
            reason=None if artifact.is_file() else "activation_registry_artifact_missing",
        )
    except (ActivationInvalid, OSError, ValueError, json.JSONDecodeError, sqlite3.Error) as exc:
        code = exc.code if isinstance(exc, ActivationInvalid) else f"{type(exc).__name__}:{exc}"
        return ActivationResult("invalid", reason=str(code))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare or verify OPP-E07-V1 activation")
    parser.add_argument("action", choices=("status", "prepare"))
    parser.add_argument("--activated-at-utc")
    parser.add_argument("--evidence-db", type=Path, default=Path("data/research/evidence.db"))
    parser.add_argument("--trial-db", type=Path, default=Path("data/research/trial.db"))
    parser.add_argument(
        "--diagnostics-db", type=Path, default=Path("data/research/diagnostics.db")
    )
    parser.add_argument("--seal-db", type=Path, default=Path("data/research/trial_seals.db"))
    parser.add_argument(
        "--activation-db", type=Path, default=Path("data/research/activation.db")
    )
    parser.add_argument(
        "--registry-path", type=Path, default=REGISTRY_ARTIFACT
    )
    parser.add_argument("--artifact-root", type=Path, default=Path("data/research/artifacts"))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    config = ActivationConfig(
        evidence_db=args.evidence_db,
        trial_db=args.trial_db,
        diagnostics_db=args.diagnostics_db,
        seal_db=args.seal_db,
        activation_db=args.activation_db,
        registry_path=args.registry_path,
        artifact_root=args.artifact_root,
    )
    try:
        if args.action == "prepare":
            if not args.activated_at_utc:
                raise ActivationInvalid("activated_at_utc_required")
            result = prepare_activation(
                config,
                activated_at=_parse_utc(args.activated_at_utc, "activated_at"),
            )
        else:
            result = activation_status(config)
    except (ActivationInvalid, OSError, ValueError, json.JSONDecodeError, sqlite3.Error) as exc:
        reason = exc.code if isinstance(exc, ActivationInvalid) else f"{type(exc).__name__}:{exc}"
        result = ActivationResult("invalid", reason=str(reason))
    print(json.dumps(asdict(result), indent=2, sort_keys=True))
    return 0 if result.status in {"unprepared", "prepared", "active"} else 3


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
