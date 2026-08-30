from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from insider_alerts.notify.ntfy import NtfyTransportEvent
from insider_alerts.research.notification_coverage import (
    NotificationCoverageConfig,
    NotificationCoverageError,
    _ensure_store,
    _write_health,
    activate_notification_coverage,
    confined_notification_coverage_source,
    notification_coverage_status,
    run_notification_coverage_once,
)
from insider_alerts.research.notification_transport import (
    NotificationJournalConfig,
    NotificationTransportJournal,
    activate_notification_journal,
    notification_transport_id,
)
from insider_alerts.review.queue import (
    NotificationDeliveryProof,
    apply_decision,
    ensure_review_tables,
    initialize_notification_delivery_schema,
    mark_notification_delivered,
    replay_deadletter,
)

ROOT = Path(__file__).resolve().parents[1]
JOURNAL_POLICY = ROOT / "docs" / "research" / "contracts" / "notification-transport-v1.json"
COVERAGE_POLICY = ROOT / "docs" / "research" / "contracts" / "notification-coverage-v1.json"
NORMAL_PACKET = "0000320193-26-000001|0000320193|4"
AMENDED_PACKET = "0001866174-26-000011|0001866174|4/A"


def _config(tmp_path: Path, *, now: datetime) -> NotificationCoverageConfig:
    data_root = tmp_path / "data"
    research_root = data_root / "research"
    research_root.mkdir(parents=True)
    source_db = data_root / "insider_alerts.db"
    ensure_review_tables(str(source_db))
    initialize_notification_delivery_schema(str(source_db))
    journal = NotificationJournalConfig(
        database=research_root / "notification_transport.db",
        research_root=research_root,
        policy_path=JOURNAL_POLICY,
        policy_root=JOURNAL_POLICY.parent,
        runtime_git_commit="a" * 40,
    )
    activate_notification_journal(journal, activated_at_utc=now - timedelta(days=1))
    return NotificationCoverageConfig(
        source_db=source_db,
        source_root=data_root,
        coverage_db=research_root / "notification_coverage.db",
        research_root=research_root,
        policy_path=COVERAGE_POLICY,
        policy_root=COVERAGE_POLICY.parent,
        journal=journal,
        runtime_git_commit="a" * 40,
    )


def _transport_event(
    phase: str,
    *,
    occurred_at: datetime,
    attempt: int = 1,
    status: int = 200,
) -> NtfyTransportEvent:
    common = {
        "attempt_number": attempt,
        "phase": phase,
        "occurred_at_utc": occurred_at,
        "request_body_sha256": "b" * 64,
        "route_sha256": "c" * 64,
    }
    if phase == "response_received":
        return NtfyTransportEvent(
            **common,  # type: ignore[arg-type]
            http_status=status,
            response_body_sha256="d" * 64,
            provider_message_id="provider_1",
            provider_message_time=1787904000,
        )
    return NtfyTransportEvent(**common)  # type: ignore[arg-type]


def _journal_attempt(
    config: NotificationCoverageConfig,
    *,
    packet_id: str,
    transport_id: str,
    started_at: datetime,
    responded_at: datetime,
    status: int = 200,
) -> None:
    journal = NotificationTransportJournal(config.journal)
    assert journal.append(
        packet_id=packet_id,
        transport_id=transport_id,
        event=_transport_event("request_started", occurred_at=started_at),
    )
    assert journal.append(
        packet_id=packet_id,
        transport_id=transport_id,
        event=_transport_event("response_received", occurred_at=responded_at, status=status),
    )


def _decision(packet_id: str, *, decision: str = "approve") -> dict[str, object]:
    return {
        "packet_id": packet_id,
        "decision": decision,
        "analyst": "quant",
        "reason": "test decision",
    }


def _insert_pending(config: NotificationCoverageConfig, packet_id: str, *, now: datetime) -> None:
    accession, cik, form_type = packet_id.split("|")
    timestamp = now.isoformat()
    with sqlite3.connect(config.source_db) as conn:
        conn.execute(
            """
            INSERT INTO review_packets(
              packet_id,accession_number,cik,form_type,payload_json,status,
              decision_json,created_at,updated_at,notification_sent_at,
              notification_required,notification_suppressed_at
            ) VALUES(?,?,?,?,?,'pending',NULL,?,?,NULL,0,NULL)
            """,
            (packet_id, accession, cik, form_type, "{}", timestamp, timestamp),
        )


def _insert_baseline_sent(
    config: NotificationCoverageConfig,
    packet_id: str,
    *,
    sent_at: datetime,
) -> None:
    _insert_pending(config, packet_id, now=sent_at - timedelta(minutes=1))
    decision = _decision(packet_id)
    encoded = json.dumps(decision, separators=(",", ":"), sort_keys=True)
    with sqlite3.connect(config.source_db) as conn:
        conn.execute(
            """
            UPDATE review_packets
            SET status='approve',decision_json=?,notification_required=1,
                notification_sent_at=?,updated_at=?
            WHERE packet_id=?
            """,
            (encoded, sent_at.isoformat(), sent_at.isoformat(), packet_id),
        )


def _proof(
    *,
    transport_id: str | None,
    responded_at: datetime,
    status: int = 200,
) -> NotificationDeliveryProof:
    return NotificationDeliveryProof(
        transport_id=transport_id,
        attempt_number=1,
        responded_at_utc=responded_at,
        request_body_sha256="b" * 64,
        route_sha256="c" * 64,
        http_status=status,
    )


def test_activation_freezes_known_missing_baseline_without_backfill(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    config = _config(tmp_path, now=now)
    normal_response = now - timedelta(hours=2)
    normal_transport = notification_transport_id(NORMAL_PACKET, "baseline")
    _journal_attempt(
        config,
        packet_id=NORMAL_PACKET,
        transport_id=normal_transport,
        started_at=normal_response - timedelta(milliseconds=2),
        responded_at=normal_response,
    )
    _insert_pending(config, NORMAL_PACKET, now=normal_response - timedelta(minutes=1))
    normal_decision = _decision(NORMAL_PACKET)
    assert apply_decision(str(config.source_db), normal_decision, notification_required=True) == 1
    assert (
        mark_notification_delivered(
            str(config.source_db),
            NORMAL_PACKET,
            normal_decision,
            proof=_proof(transport_id=normal_transport, responded_at=normal_response),
        )
        == 1
    )
    _insert_baseline_sent(config, AMENDED_PACKET, sent_at=now - timedelta(hours=1))

    activation = activate_notification_coverage(config, now_fn=lambda: now)

    assert activation["baseline_sent_count"] == 2
    assert activation["baseline_covered_count"] == 1
    assert activation["baseline_missing_count"] == 1
    with (
        sqlite3.connect(config.coverage_db) as conn,
        pytest.raises(sqlite3.IntegrityError, match="immutable"),
    ):
        conn.execute("UPDATE notification_coverage_baseline SET classification='covered'")
    first_status = notification_coverage_status(config, now=now + timedelta(seconds=1))
    assert first_status["valid"] is True
    assert first_status["baseline"]["missing"] == 1

    amended_response = now + timedelta(seconds=2)
    _journal_attempt(
        config,
        packet_id=AMENDED_PACKET,
        transport_id=notification_transport_id(AMENDED_PACKET, "too-late"),
        started_at=amended_response - timedelta(milliseconds=1),
        responded_at=amended_response,
    )
    second_status = notification_coverage_status(config, now=now + timedelta(seconds=3))
    assert second_status["valid"] is True
    assert second_status["baseline"]["missing"] == 1
    assert second_status["baseline"]["missing_sha256"] == first_status["baseline"]["missing_sha256"]


def test_packet_level_legacy_attempt_cannot_create_covered_baseline(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    config = _config(tmp_path, now=now)
    response = now - timedelta(minutes=2)
    _journal_attempt(
        config,
        packet_id=NORMAL_PACKET,
        transport_id=notification_transport_id(NORMAL_PACKET, "legacy-only"),
        started_at=response - timedelta(milliseconds=1),
        responded_at=response,
    )
    _insert_baseline_sent(config, NORMAL_PACKET, sent_at=response + timedelta(seconds=1))

    activation = activate_notification_coverage(config, now_fn=lambda: now)

    assert activation["baseline_covered_count"] == 0
    assert activation["baseline_missing_count"] == 1


def test_exact_post_activation_ack_is_covered_and_survives_replay(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    config = _config(tmp_path, now=now)
    activate_notification_coverage(config, now_fn=lambda: now)
    packet = "0000320193-26-000002|0000320193|4"
    _insert_pending(config, packet, now=now)
    decision = _decision(packet, decision="deadletter")
    assert apply_decision(str(config.source_db), decision, notification_required=True) == 1
    responded = now
    transport_id = notification_transport_id(packet, "post-activation")
    _journal_attempt(
        config,
        packet_id=packet,
        transport_id=transport_id,
        started_at=responded - timedelta(milliseconds=2),
        responded_at=responded,
    )
    assert (
        mark_notification_delivered(
            str(config.source_db),
            packet,
            decision,
            proof=_proof(transport_id=transport_id, responded_at=responded),
        )
        == 1
    )

    report = run_notification_coverage_once(config, now_fn=lambda: now + timedelta(seconds=2))
    assert report["valid"] is True
    assert report["post_activation_ack_count"] == 1
    assert report["current_gaps"] == []

    assert replay_deadletter(str(config.source_db), packet) == 1
    with sqlite3.connect(config.source_db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM notification_delivery_acks").fetchone()[0] == 1


@pytest.mark.parametrize(
    ("transport_id", "journal_status", "expected_reason"),
    [
        (None, None, "transport_identity_missing"),
        ("exact", 500, "successful_status_mismatch"),
    ],
)
def test_future_delivery_gap_is_append_only_and_strictly_unhealthy(
    tmp_path: Path,
    transport_id: str | None,
    journal_status: int | None,
    expected_reason: str,
) -> None:
    now = datetime.now(UTC)
    config = _config(tmp_path, now=now)
    activate_notification_coverage(config, now_fn=lambda: now)
    packet = "0000320193-26-000003|0000320193|4"
    _insert_pending(config, packet, now=now)
    decision = _decision(packet)
    assert apply_decision(str(config.source_db), decision, notification_required=True) == 1
    responded = now
    exact_transport = notification_transport_id(packet, "bad-proof")
    if journal_status is not None:
        _journal_attempt(
            config,
            packet_id=packet,
            transport_id=exact_transport,
            started_at=responded - timedelta(milliseconds=1),
            responded_at=responded,
            status=journal_status,
        )
    proof_transport = exact_transport if transport_id == "exact" else None
    assert (
        mark_notification_delivered(
            str(config.source_db),
            packet,
            decision,
            proof=_proof(transport_id=proof_transport, responded_at=responded),
        )
        == 1
    )

    report = run_notification_coverage_once(config, now_fn=lambda: now + timedelta(seconds=2))
    assert report["valid"] is False
    assert report["current_gaps"][0]["reason"] == expected_reason
    status = notification_coverage_status(config, now=now + timedelta(seconds=3))
    assert status["valid"] is False
    assert status["stored_gap_count"] == 1
    with (
        sqlite3.connect(config.coverage_db) as conn,
        pytest.raises(sqlite3.IntegrityError, match="immutable"),
    ):
        conn.execute("DELETE FROM notification_coverage_gaps")


def test_gap_observation_cannot_predate_linked_acknowledgement(tmp_path: Path) -> None:
    boundary = datetime(2026, 8, 30, 16, 0, tzinfo=UTC)
    config = _config(tmp_path, now=boundary)
    activate_notification_coverage(config, now_fn=lambda: boundary)
    packet = "0000320193-26-000013|0000320193|4"
    _insert_pending(config, packet, now=boundary)
    decision = _decision(packet)
    assert apply_decision(str(config.source_db), decision, notification_required=True) == 1
    responded = boundary + timedelta(seconds=1)
    assert (
        mark_notification_delivered(
            str(config.source_db),
            packet,
            decision,
            proof=_proof(transport_id=None, responded_at=responded),
        )
        == 1
    )
    clock = iter(
        [
            boundary,
            boundary + timedelta(seconds=2),
            boundary + timedelta(seconds=3),
        ]
    )

    report = run_notification_coverage_once(config, now_fn=lambda: next(clock))

    assert report["valid"] is False
    with sqlite3.connect(config.coverage_db) as conn:
        evidence_at, observed_at = conn.execute(
            "SELECT evidence_not_before_at_utc,first_observed_at_utc "
            "FROM notification_coverage_gaps"
        ).fetchone()
    assert datetime.fromisoformat(observed_at.replace("Z", "+00:00")) >= datetime.fromisoformat(
        evidence_at.replace("Z", "+00:00")
    )
    assert datetime.fromisoformat(observed_at.replace("Z", "+00:00")) >= boundary + timedelta(
        seconds=3
    )


def test_unledgered_old_process_ack_is_detected(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    config = _config(tmp_path, now=now)
    activate_notification_coverage(config, now_fn=lambda: now)
    packet = "0000320193-26-000004|0000320193|4"
    _insert_baseline_sent(config, packet, sent_at=now + timedelta(seconds=1))

    report = run_notification_coverage_once(config, now_fn=lambda: now + timedelta(seconds=2))

    assert report["valid"] is False
    assert report["current_gaps"][0]["reason"] == "unledgered_current_delivery"


def test_source_schema_and_paths_fail_closed_as_degraded(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    config = _config(tmp_path, now=now)
    escaped = NotificationCoverageConfig(
        source_db=tmp_path / "escaped.db",
        source_root=config.source_root,
        coverage_db=config.coverage_db,
        research_root=config.research_root,
        policy_path=config.policy_path,
        policy_root=config.policy_root,
        journal=config.journal,
        runtime_git_commit=config.runtime_git_commit,
    )
    with pytest.raises(NotificationCoverageError, match="escaped"):
        activate_notification_coverage(escaped, now_fn=lambda: now)

    activate_notification_coverage(config, now_fn=lambda: now)
    with sqlite3.connect(config.source_db) as conn:
        conn.execute("DROP TRIGGER notification_delivery_acks_no_update")
    status = notification_coverage_status(config, now=now + timedelta(seconds=1))
    assert status["valid"] is False
    assert status["reason"] == "notification_coverage_degraded"


def test_status_rejects_same_named_noop_source_and_coverage_triggers(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    source_config = _config(tmp_path / "source", now=now)
    activate_notification_coverage(source_config, now_fn=lambda: now)
    with sqlite3.connect(source_config.source_db) as conn:
        conn.execute("DROP TRIGGER notification_delivery_acks_no_update")
        conn.execute(
            "CREATE TRIGGER notification_delivery_acks_no_update "
            "BEFORE UPDATE ON notification_delivery_acks BEGIN SELECT 1; END"
        )
    source_status = notification_coverage_status(source_config, now=now + timedelta(seconds=1))
    assert source_status["valid"] is False
    assert "schema definition mismatch" in source_status["detail"]

    coverage_config = _config(tmp_path / "coverage", now=now)
    activate_notification_coverage(coverage_config, now_fn=lambda: now)
    with sqlite3.connect(coverage_config.coverage_db) as conn:
        conn.execute("DROP TRIGGER notification_coverage_gaps_no_delete")
        conn.execute(
            "CREATE TRIGGER notification_coverage_gaps_no_delete "
            "BEFORE DELETE ON notification_coverage_gaps BEGIN SELECT 1; END"
        )
    coverage_status = notification_coverage_status(coverage_config, now=now + timedelta(seconds=1))
    assert coverage_status["valid"] is False
    assert "schema definition mismatch" in coverage_status["detail"]


def test_status_rejects_configured_source_path_drift(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    config = _config(tmp_path, now=now)
    activate_notification_coverage(config, now_fn=lambda: now)
    alternate_source = config.source_root / "alternate.db"
    ensure_review_tables(str(alternate_source))
    initialize_notification_delivery_schema(str(alternate_source))
    drifted = NotificationCoverageConfig(
        source_db=alternate_source,
        source_root=config.source_root,
        coverage_db=config.coverage_db,
        research_root=config.research_root,
        policy_path=config.policy_path,
        policy_root=config.policy_root,
        journal=config.journal,
        runtime_git_commit=config.runtime_git_commit,
    )

    status = notification_coverage_status(drifted, now=now + timedelta(seconds=1))

    assert status["valid"] is False
    assert "configured paths changed" in status["detail"]


def test_status_rejects_source_acknowledgement_rollback(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    config = _config(tmp_path, now=now)
    packet = "0000320193-26-000005|0000320193|4"
    response = now - timedelta(seconds=2)
    transport_id = notification_transport_id(packet, "baseline")
    _journal_attempt(
        config,
        packet_id=packet,
        transport_id=transport_id,
        started_at=response - timedelta(milliseconds=1),
        responded_at=response,
    )
    _insert_pending(config, packet, now=response - timedelta(minutes=1))
    decision = _decision(packet)
    assert apply_decision(str(config.source_db), decision, notification_required=True) == 1
    assert (
        mark_notification_delivered(
            str(config.source_db),
            packet,
            decision,
            proof=_proof(transport_id=transport_id, responded_at=response),
        )
        == 1
    )
    activate_notification_coverage(config, now_fn=lambda: now)
    with sqlite3.connect(config.source_db) as conn:
        trigger_sql = str(
            conn.execute(
                "SELECT sql FROM sqlite_master "
                "WHERE type='trigger' AND name='notification_delivery_acks_no_delete'"
            ).fetchone()[0]
        )
        conn.execute("DROP TRIGGER notification_delivery_acks_no_delete")
        conn.execute("DELETE FROM notification_delivery_acks")
        conn.execute(trigger_sql)

    status = notification_coverage_status(config, now=now + timedelta(seconds=1))

    assert status["valid"] is False
    assert "sequence rolled back" in status["detail"]


def test_status_rejects_transport_journal_rollback(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    config = _config(tmp_path, now=now)
    response = now - timedelta(seconds=2)
    _journal_attempt(
        config,
        packet_id=NORMAL_PACKET,
        transport_id=notification_transport_id(NORMAL_PACKET, "baseline"),
        started_at=response - timedelta(milliseconds=1),
        responded_at=response,
    )
    activate_notification_coverage(config, now_fn=lambda: now)
    with sqlite3.connect(config.journal.database) as conn:
        trigger_sql = str(
            conn.execute(
                "SELECT sql FROM sqlite_master "
                "WHERE type='trigger' AND name='notification_transport_events_no_delete'"
            ).fetchone()[0]
        )
        conn.execute("DROP TRIGGER notification_transport_events_no_delete")
        conn.execute("DELETE FROM notification_transport_events")
        conn.execute(trigger_sql)
        conn.execute("DELETE FROM notification_journal_health")

    status = notification_coverage_status(config, now=now + timedelta(seconds=1))

    assert status["valid"] is False
    assert "sequence rolled back" in status["detail"]


def test_status_rejects_post_activation_prefix_substitution(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    config = _config(tmp_path, now=now)
    activate_notification_coverage(config, now_fn=lambda: now)
    packet = "0000320193-26-000006|0000320193|4"
    response = now + timedelta(seconds=1)
    transport_id = notification_transport_id(packet, "post-activation")
    _insert_pending(config, packet, now=now)
    decision = _decision(packet)
    assert apply_decision(str(config.source_db), decision, notification_required=True) == 1
    _journal_attempt(
        config,
        packet_id=packet,
        transport_id=transport_id,
        started_at=response - timedelta(milliseconds=1),
        responded_at=response,
    )
    assert (
        mark_notification_delivered(
            str(config.source_db),
            packet,
            decision,
            proof=_proof(transport_id=transport_id, responded_at=response),
        )
        == 1
    )
    report = run_notification_coverage_once(config, now_fn=lambda: now + timedelta(seconds=2))
    assert report["valid"] is True

    with sqlite3.connect(config.source_db) as conn:
        trigger_sql = str(
            conn.execute(
                "SELECT sql FROM sqlite_master "
                "WHERE type='trigger' AND name='notification_delivery_acks_no_update'"
            ).fetchone()[0]
        )
        row = conn.execute(
            "SELECT record_json FROM notification_delivery_acks WHERE sequence=1"
        ).fetchone()
        record = json.loads(bytes(row[0]))
        record["route_sha256"] = "e" * 64
        encoded = json.dumps(record, separators=(",", ":"), sort_keys=True).encode()
        conn.execute("DROP TRIGGER notification_delivery_acks_no_update")
        conn.execute(
            "UPDATE notification_delivery_acks "
            "SET route_sha256=?,record_sha256=?,record_json=? WHERE sequence=1",
            ("e" * 64, hashlib.sha256(encoded).hexdigest(), encoded),
        )
        conn.execute(trigger_sql)

    status = notification_coverage_status(config, now=now + timedelta(seconds=3))

    assert status["valid"] is False
    assert "prefix changed after monitoring" in status["detail"]


def test_status_rejects_post_activation_journal_prefix_substitution(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    config = _config(tmp_path, now=now)
    activate_notification_coverage(config, now_fn=lambda: now)
    response = now + timedelta(seconds=1)
    _journal_attempt(
        config,
        packet_id=NORMAL_PACKET,
        transport_id=notification_transport_id(NORMAL_PACKET, "post-activation"),
        started_at=response - timedelta(milliseconds=1),
        responded_at=response,
    )
    assert (
        run_notification_coverage_once(config, now_fn=lambda: now + timedelta(seconds=2))["valid"]
        is True
    )

    with sqlite3.connect(config.journal.database) as conn:
        trigger_sql = str(
            conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='trigger' "
                "AND name='notification_transport_events_no_update'"
            ).fetchone()[0]
        )
        rows = conn.execute(
            "SELECT sequence,record_json FROM notification_transport_events ORDER BY sequence"
        ).fetchall()
        conn.execute("DROP TRIGGER notification_transport_events_no_update")
        for sequence, raw_record in rows:
            record = json.loads(bytes(raw_record))
            record["route_sha256"] = "e" * 64
            encoded = json.dumps(record, separators=(",", ":"), sort_keys=True).encode()
            conn.execute(
                "UPDATE notification_transport_events SET record_sha256=?,record_json=? "
                "WHERE sequence=?",
                (hashlib.sha256(encoded).hexdigest(), encoded, sequence),
            )
        conn.execute(trigger_sql)

    status = notification_coverage_status(config, now=now + timedelta(seconds=3))

    assert status["valid"] is False
    assert "prefix changed after monitoring" in status["detail"]


def test_status_rejects_stale_and_future_worker_health(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    config = _config(tmp_path, now=now)
    activate_notification_coverage(config, now_fn=lambda: now)

    stale_at = (now - timedelta(minutes=4)).isoformat()
    with sqlite3.connect(config.coverage_db) as conn:
        conn.execute(
            "UPDATE notification_coverage_health SET last_started_at_utc=?,last_success_at_utc=?",
            (stale_at, stale_at),
        )
    stale = notification_coverage_status(config, now=now)
    assert stale["valid"] is False
    assert "coverage_health_stale" in stale["integrity_errors"]

    future_at = (now + timedelta(minutes=1)).isoformat()
    with sqlite3.connect(config.coverage_db) as conn:
        conn.execute(
            "UPDATE notification_coverage_health SET last_started_at_utc=?,last_success_at_utc=?",
            (future_at, future_at),
        )
    future = notification_coverage_status(config, now=now)
    assert future["valid"] is False
    assert "coverage_health_from_future" in future["integrity_errors"]


def test_out_of_order_health_writer_preserves_newer_sequence_prefix_pairs(
    tmp_path: Path,
) -> None:
    coverage_db = tmp_path / "notification_coverage.db"
    _ensure_store(coverage_db)
    _write_health(
        coverage_db,
        started_at="2026-08-30T00:00:02Z",
        success_at="2026-08-30T00:00:02Z",
        error=None,
        post_ack_count=2,
        current_gap_count=0,
        source_sequence=2,
        journal_sequence=4,
        source_prefix_sha256="b" * 64,
        journal_prefix_sha256="d" * 64,
    )

    _write_health(
        coverage_db,
        started_at="2026-08-30T00:00:01Z",
        success_at="2026-08-30T00:00:01Z",
        error=None,
        post_ack_count=1,
        current_gap_count=0,
        source_sequence=1,
        journal_sequence=2,
        source_prefix_sha256="a" * 64,
        journal_prefix_sha256="c" * 64,
    )

    with sqlite3.connect(coverage_db) as conn:
        checkpoint = conn.execute(
            "SELECT last_source_ack_sequence,last_source_prefix_sha256,"
            "last_journal_sequence,last_journal_prefix_sha256 "
            "FROM notification_coverage_health WHERE singleton=1"
        ).fetchone()
    assert checkpoint == (2, "b" * 64, 4, "d" * 64)

    _write_health(
        coverage_db,
        started_at="2026-08-30T00:00:03Z",
        success_at="2026-08-30T00:00:03Z",
        error=None,
        post_ack_count=3,
        current_gap_count=0,
        source_sequence=3,
        journal_sequence=3,
        source_prefix_sha256="e" * 64,
        journal_prefix_sha256="f" * 64,
    )

    with sqlite3.connect(coverage_db) as conn:
        independent_checkpoint = conn.execute(
            "SELECT last_source_ack_sequence,last_source_prefix_sha256,"
            "last_journal_sequence,last_journal_prefix_sha256 "
            "FROM notification_coverage_health WHERE singleton=1"
        ).fetchone()
    assert independent_checkpoint == (3, "e" * 64, 4, "d" * 64)

    _write_health(
        coverage_db,
        started_at="2026-08-30T00:00:05Z",
        success_at=None,
        error=RuntimeError("newer failure"),
        post_ack_count=5,
        current_gap_count=1,
        source_sequence=3,
        journal_sequence=4,
        source_prefix_sha256="e" * 64,
        journal_prefix_sha256="d" * 64,
    )
    _write_health(
        coverage_db,
        started_at="2026-08-30T00:00:04Z",
        success_at="2026-08-30T00:00:04Z",
        error=None,
        post_ack_count=4,
        current_gap_count=0,
        source_sequence=3,
        journal_sequence=4,
        source_prefix_sha256="e" * 64,
        journal_prefix_sha256="d" * 64,
    )

    with sqlite3.connect(coverage_db) as conn:
        health = conn.execute(
            "SELECT last_started_at_utc,last_error_kind,post_activation_ack_count,"
            "current_gap_count FROM notification_coverage_health WHERE singleton=1"
        ).fetchone()
    assert health == ("2026-08-30T00:00:05.000000Z", "RuntimeError", 5, 1)


def test_worker_and_installer_are_order_incapable_hidden_and_strict() -> None:
    worker = (
        ROOT / "src" / "insider_alerts" / "research" / "notification_coverage_worker.py"
    ).read_text(encoding="utf-8")
    module = (ROOT / "src" / "insider_alerts" / "research" / "notification_coverage.py").read_text(
        encoding="utf-8"
    )
    installer = (ROOT / "ops" / "windows" / "install-notification-coverage-task.ps1").read_text(
        encoding="utf-8"
    )

    assert "ensure_kill_on_close_process_tree()" in worker
    assert "ib_async" not in worker + module
    assert "insider_alerts.research.trial" not in worker + module
    assert "mode=ro" in module
    assert "PRAGMA query_only=ON" in module
    assert "_EXPECTED_COVERAGE_SCHEMA = _expected_coverage_schema()" in module
    assert "_COVERAGE_SCHEMA_SQL_SHA256" not in module
    assert "pythonw.exe" in installer
    assert "Insider Alerts Live Canary Worker" in installer
    assert "branch --show-current" in installer
    assert "rev-parse origin/main" in installer
    assert "status --porcelain=v1 --untracked-files=all" in installer
    assert "$preflightArguments" in installer
    assert "$deploymentAction.Arguments -ne $expectedCanaryArguments" in installer
    assert "Insider Alerts Autopilot Worker" in installer
    assert "$expectedProducerArgumentsSha256" in installer
    assert "$producerRepoRoot -ne $repoRoot" in installer
    assert "AutopilotHealthStore" in installer
    assert "runtime_source_fingerprint" in installer
    assert "$producerProgressAge.TotalSeconds -gt 600" in installer
    assert "GetEnvironmentVariable($environmentName, \"User\")" in installer
    assert "base64.b64decode(sys.argv[2])" in installer
    assert "from insider_alerts.config import get_settings; s=get_settings()" in installer
    assert "$sourceDb -ne $effectiveSourceDb" in installer
    assert "$journalDb -ne $effectiveJournalDb" in installer
    assert "$effectiveSettingsOutput = @(" in installer
    assert "$IntervalMinutes -ne 1" in installer
    assert "Out-String" not in installer
    assert "confined_notification_coverage_source(config)" in worker
    assert "initialize_notification_delivery_schema" in worker
    assert "-Hidden" in installer
    assert "-MultipleInstances IgnoreNew" in installer
    assert "-WindowStyle" not in installer
    assert "HRESULT 0x80070005,Register-ScheduledTask" in installer


def test_source_initialization_uses_the_monitor_path_confinement(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    config = _config(tmp_path, now=now)
    escaped = NotificationCoverageConfig(
        source_db=tmp_path / "escaped.db",
        source_root=config.source_root,
        coverage_db=config.coverage_db,
        research_root=config.research_root,
        policy_path=config.policy_path,
        policy_root=config.policy_root,
        journal=config.journal,
        runtime_git_commit=config.runtime_git_commit,
    )

    with pytest.raises(NotificationCoverageError, match="escaped"):
        confined_notification_coverage_source(escaped)


def test_source_initialization_rejects_dangling_reparse_point(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    config = _config(tmp_path, now=now)
    dangling = config.source_root / "dangling.db"
    try:
        dangling.symlink_to(tmp_path / "missing-target.db")
    except OSError as exc:
        pytest.skip(f"filesystem cannot create a test symlink: {exc}")
    escaped = NotificationCoverageConfig(
        source_db=dangling,
        source_root=config.source_root,
        coverage_db=config.coverage_db,
        research_root=config.research_root,
        policy_path=config.policy_path,
        policy_root=config.policy_root,
        journal=config.journal,
        runtime_git_commit=config.runtime_git_commit,
    )

    with pytest.raises(NotificationCoverageError, match="reparse"):
        confined_notification_coverage_source(escaped)
