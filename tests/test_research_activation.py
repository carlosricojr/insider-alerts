from __future__ import annotations

import ast
import json
import sqlite3
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import rfc8785

import insider_alerts.research.activation as activation
import insider_alerts.research.inference as inference

ROOT = Path(__file__).resolve().parents[1]
PREPARED_AT = datetime(2026, 8, 27, 20, 0, tzinfo=UTC)
ACTIVATED_AT = datetime(2026, 8, 28, 4, 0, tzinfo=UTC)


def _head() -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _config(tmp_path: Path) -> activation.ActivationConfig:
    registry_path = tmp_path / "OPP-E07-V1.json"
    registry_path.write_bytes(
        (ROOT / "docs/research/registry/OPP-E07-V1.json").read_bytes()
    )
    return activation.ActivationConfig(
        evidence_db=tmp_path / "evidence.db",
        trial_db=tmp_path / "trial.db",
        diagnostics_db=tmp_path / "diagnostics.db",
        seal_db=tmp_path / "trial_seals.db",
        activation_db=tmp_path / "activation.db",
        registry_path=registry_path,
        artifact_root=tmp_path / "artifacts",
    )


def _allow_synced_main(monkeypatch: pytest.MonkeyPatch) -> None:
    head = _head()
    monkeypatch.setattr(activation, "_git_state", lambda _root: ("main", head, head, True))


def test_prepare_is_atomic_idempotent_and_replays_exact_registry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _allow_synced_main(monkeypatch)
    config = _config(tmp_path)
    activation._initialize_scientific_stores(config)
    activation.ActivationStore(config.activation_db)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(
            executor.map(
                lambda _index: activation.prepare_activation(
                    config,
                    activated_at=ACTIVATED_AT,
                    now=PREPARED_AT,
                ),
                range(2),
            )
        )

    assert results[0] == results[1]
    assert results[0].status == "prepared"
    assert activation.activation_status(config, now=PREPARED_AT).status == "prepared"
    store = activation.ActivationStore(config.activation_db, initialize=False)
    active = store.active_registry()
    assert active is not None
    artifact = Path(str(results[0].registry_artifact_path))
    assert artifact.read_bytes() == rfc8785.dumps(active)
    with sqlite3.connect(config.activation_db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM activation_receipt").fetchone()[0] == 1


def test_prepare_recovers_published_artifact_from_stored_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _allow_synced_main(monkeypatch)
    config = _config(tmp_path)
    publish = activation._publish_registry_artifact
    monkeypatch.setattr(
        activation,
        "_publish_registry_artifact",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("publication crash")),
    )
    with pytest.raises(RuntimeError, match="publication crash"):
        activation.prepare_activation(
            config,
            activated_at=ACTIVATED_AT,
            now=PREPARED_AT,
        )
    stored = activation.ActivationStore(config.activation_db, initialize=False).active_registry()
    assert stored is not None
    monkeypatch.setattr(activation, "_publish_registry_artifact", publish)

    replay = activation.prepare_activation(
        config,
        activated_at=ACTIVATED_AT,
        now=PREPARED_AT + timedelta(minutes=30),
    )

    assert Path(str(replay.registry_artifact_path)).read_bytes() == rfc8785.dumps(stored)
    assert replay.prepared_at_utc == activation._utc_text(PREPARED_AT)


def test_status_cannot_miss_receipt_committed_during_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _allow_synced_main(monkeypatch)
    config = _config(tmp_path)
    real_validate = activation.validate_deployed_registry_state

    def prepare_during_validation(
        registry: dict[str, object],
        activation_db: Path,
        *,
        registry_bytes: bytes,
        now: datetime | None = None,
    ) -> None:
        activation.prepare_activation(
            config,
            activated_at=ACTIVATED_AT,
            now=PREPARED_AT,
        )
        real_validate(
            registry,
            activation_db,
            registry_bytes=registry_bytes,
            now=now,
        )

    monkeypatch.setattr(
        activation,
        "validate_deployed_registry_state",
        prepare_during_validation,
    )

    assert activation.activation_status(config, now=PREPARED_AT).status == "prepared"


def test_prepare_refuses_dirty_checkout_short_lead_and_nonempty_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    head = _head()
    monkeypatch.setattr(
        activation,
        "_git_state",
        lambda _root: ("codex/activation-cutover", head, head, False),
    )
    with pytest.raises(
        activation.ActivationInvalid, match="activation_checkout_not_clean_synced_main"
    ):
        activation.prepare_activation(
            config,
            activated_at=ACTIVATED_AT,
            now=PREPARED_AT,
        )

    _allow_synced_main(monkeypatch)
    with pytest.raises(activation.ActivationInvalid, match="activation_lead_time_insufficient"):
        activation.prepare_activation(
            config,
            activated_at=PREPARED_AT + timedelta(minutes=30),
            now=PREPARED_AT,
        )

    activation._initialize_scientific_stores(config)
    with sqlite3.connect(config.evidence_db) as conn:
        conn.execute(
            "INSERT INTO evidence_snapshots VALUES(1,?,?,?,?,?,?,?)",
            (
                "snapshot",
                "job",
                "a" * 64,
                "b" * 64,
                b"{}",
                activation._utc_text(PREPARED_AT),
                "captured",
            ),
        )
    with pytest.raises(
        activation.ActivationInvalid,
        match="activation_store_not_empty:evidence_snapshots",
    ):
        activation.prepare_activation(
            config,
            activated_at=ACTIVATED_AT,
            now=PREPARED_AT,
        )


def test_prepared_draft_has_one_irrevocable_boundary_and_active_copy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _allow_synced_main(monkeypatch)
    config = _config(tmp_path)
    first = activation.prepare_activation(
        config,
        activated_at=ACTIVATED_AT,
        now=PREPARED_AT,
    )
    registry = json.loads(config.registry_path.read_text(encoding="utf-8"))
    assert (
        activation.validate_deployed_registry_state(
            registry,
            config.activation_db,
            registry_bytes=rfc8785.dumps(registry),
            now=ACTIVATED_AT - timedelta(microseconds=1),
        )
        == "draft"
    )
    changed_draft = json.loads(json.dumps(registry))
    changed_draft["title"] = "changed after preparation"
    with pytest.raises(
        activation.ActivationInvalid,
        match="prepared_registry_definition_changed",
    ):
        activation.validate_deployed_registry_state(
            changed_draft,
            config.activation_db,
            registry_bytes=rfc8785.dumps(changed_draft),
            now=ACTIVATED_AT - timedelta(microseconds=1),
        )
    with pytest.raises(
        activation.ActivationInvalid,
        match="activation_boundary_passed_while_registry_draft",
    ):
        activation.validate_deployed_registry_state(
            registry,
            config.activation_db,
            registry_bytes=rfc8785.dumps(registry),
            now=ACTIVATED_AT,
        )
    with pytest.raises(activation.ActivationInvalid, match="alternate_activation_prohibited"):
        activation.prepare_activation(
            config,
            activated_at=ACTIVATED_AT + timedelta(hours=1),
            now=PREPARED_AT,
        )

    artifact = Path(str(first.registry_artifact_path))
    active = json.loads(artifact.read_text(encoding="utf-8"))
    assert (
        activation.validate_deployed_registry_state(
            active,
            config.activation_db,
            registry_bytes=rfc8785.dumps(active),
            now=PREPARED_AT,
        )
        == "armed"
    )
    assert (
        activation.validate_deployed_registry_state(
            active,
            config.activation_db,
            registry_bytes=rfc8785.dumps(active),
            now=PREPARED_AT + timedelta(minutes=1),
        )
        == "armed"
    )
    assert (
        activation.validate_deployed_registry_state(
            active,
            config.activation_db,
            registry_bytes=rfc8785.dumps(active),
            now=ACTIVATED_AT,
        )
        == "active"
    )
    tampered = json.loads(json.dumps(active))
    tampered["activation"]["activated_at_utc"] = activation._utc_text(
        ACTIVATED_AT + timedelta(seconds=1)
    )
    with pytest.raises(
        activation.ActivationInvalid, match="active_registry_bytes_do_not_match_receipt"
    ):
        activation.validate_deployed_registry_state(
            tampered,
            config.activation_db,
            registry_bytes=rfc8785.dumps(tampered),
        )
    with pytest.raises(inference.TrialInvalid, match="activation_receipt_digest_mismatch"):
        inference._validate_registry(tampered, allow_draft=False)

    config.registry_path.write_bytes(rfc8785.dumps(active))
    assert activation.activation_status(config, now=PREPARED_AT).status == "active"
    with pytest.raises(
        activation.ActivationInvalid,
        match="activation_prepare_requires_draft_registry",
    ):
        activation.prepare_activation(
            config,
            activated_at=ACTIVATED_AT,
            now=PREPARED_AT,
        )


def test_active_registry_requires_exact_bytes_and_pre_boundary_armed_attestation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _allow_synced_main(monkeypatch)
    config = _config(tmp_path)
    result = activation.prepare_activation(
        config,
        activated_at=ACTIVATED_AT,
        now=PREPARED_AT,
    )
    canonical_bytes = Path(str(result.registry_artifact_path)).read_bytes()
    active = json.loads(canonical_bytes)
    noncanonical_bytes = json.dumps(active, indent=2).encode("utf-8")

    with pytest.raises(
        activation.ActivationInvalid,
        match="active_registry_bytes_do_not_match_receipt",
    ):
        activation.validate_deployed_registry_state(
            active,
            config.activation_db,
            registry_bytes=noncanonical_bytes,
            now=PREPARED_AT,
        )

    with pytest.raises(
        activation.ActivationInvalid,
        match="activation_armed_attestation_missing",
    ):
        activation.validate_deployed_registry_state(
            active,
            config.activation_db,
            registry_bytes=canonical_bytes,
            now=ACTIVATED_AT,
        )


def test_active_registry_requires_append_only_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _allow_synced_main(monkeypatch)
    config = _config(tmp_path)
    result = activation.prepare_activation(
        config,
        activated_at=ACTIVATED_AT,
        now=PREPARED_AT,
    )
    active = json.loads(Path(str(result.registry_artifact_path)).read_text(encoding="utf-8"))

    with pytest.raises(activation.ActivationInvalid, match="activation_receipt_missing"):
        activation.validate_deployed_registry_state(
            active,
            tmp_path / "missing-activation.db",
            registry_bytes=rfc8785.dumps(active),
            now=PREPARED_AT,
        )
    canonical_bytes = Path(str(result.registry_artifact_path)).read_bytes()
    assert (
        activation.validate_deployed_registry_state(
            active,
            config.activation_db,
            registry_bytes=canonical_bytes,
            now=PREPARED_AT,
        )
        == "armed"
    )
    with (
        sqlite3.connect(config.activation_db) as conn,
        pytest.raises(sqlite3.IntegrityError, match="activation receipt is append-only"),
    ):
        conn.execute("DELETE FROM activation_receipt")
    with (
        sqlite3.connect(config.activation_db) as conn,
        pytest.raises(sqlite3.IntegrityError, match="armed attestation is append-only"),
    ):
        conn.execute("DELETE FROM activation_armed_attestation")


def test_armed_attestation_translates_sqlite_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class FailingConnection:
        def __enter__(self) -> FailingConnection:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def close(self) -> None:
            return None

        def execute(self, _statement: str) -> None:
            raise sqlite3.OperationalError("unavailable")

    store = activation.ActivationStore(tmp_path / "activation.db")
    monkeypatch.setattr(store, "_connect", FailingConnection)

    with pytest.raises(activation.ActivationInvalid, match="activation_store_invalid"):
        store.attest_armed(
            b"registry",
            armed_at=PREPARED_AT,
            activated_at=ACTIVATED_AT,
        )


def test_armed_attestation_preserves_domain_failures(tmp_path: Path) -> None:
    store = activation.ActivationStore(tmp_path / "activation.db")
    store.attest_armed(
        b"first-registry",
        armed_at=PREPARED_AT,
        activated_at=ACTIVATED_AT,
    )

    with pytest.raises(
        activation.ActivationInvalid,
        match="alternate_armed_attestation_prohibited",
    ):
        store.attest_armed(
            b"alternate-registry",
            armed_at=PREPARED_AT + timedelta(minutes=1),
            activated_at=ACTIVATED_AT,
        )


def test_activation_module_has_no_broker_or_order_capability() -> None:
    source = (ROOT / "src/insider_alerts/research/activation.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }
    assert not any(name.startswith(("ib_async", "insider_alerts.execution")) for name in imports)
    assert "placeOrder" not in source
