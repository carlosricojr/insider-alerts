import hashlib
import json
import sqlite3
from datetime import UTC, datetime

import pytest

from insider_alerts.review.queue import (
    DecisionValidationError,
    NotificationDeliveryProof,
    apply_decision,
    enqueue_review_packet,
    list_deadletters,
    list_notification_outbox,
    list_pending_review_packets,
    mark_notification_delivered,
    mark_notification_suppressed,
    replay_deadletter,
)
from insider_alerts.sec.models import FilingRef
from insider_alerts.sec.store import init_db


def _delivery_proof() -> NotificationDeliveryProof:
    return NotificationDeliveryProof(
        transport_id="a" * 64,
        attempt_number=1,
        responded_at_utc=datetime(2026, 2, 11, 1, 1, tzinfo=UTC),
        request_body_sha256="b" * 64,
        route_sha256="c" * 64,
        http_status=200,
    )


def _sample_ref(
    *,
    accession_number: str = "0000320193-24-000123",
    form_type: str = "4",
    cik: str = "0000320193",
) -> FilingRef:
    return FilingRef(
        source="sec_rss",
        cik=cik,
        accession_number=accession_number,
        form_type=form_type,
        filed_at=datetime(2026, 2, 11, 1, 0, tzinfo=UTC),
        filing_detail_url="https://www.sec.gov/Archives/edgar/data/320193/000032019324000123-index.htm",
        primary_doc_url="https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/wk-form4.xml",
        raw_rss_entry={"title": "4 - Apple Inc"},
    )


def test_enqueue_review_packet_idempotent(tmp_path) -> None:
    db = str(tmp_path / "insider_alerts.db")
    init_db(db)
    packet = {"score": 77.5, "rationale": {"a": 1}}
    first = enqueue_review_packet(db, _sample_ref(), packet)
    second = enqueue_review_packet(db, _sample_ref(), packet)
    assert first is True
    assert second is False


def test_enqueue_review_packet_dedupes_same_accession_form_across_cik(tmp_path) -> None:
    db = str(tmp_path / "insider_alerts.db")
    init_db(db)
    packet = {"score": 77.5, "rationale": {"a": 1}}
    first = enqueue_review_packet(
        db,
        _sample_ref(accession_number="0000320193-24-000123", form_type="4", cik="0000320193"),
        packet,
    )
    second = enqueue_review_packet(
        db,
        _sample_ref(accession_number="0000320193-24-000123", form_type="4", cik="0000000001"),
        packet,
    )
    assert first is True
    assert second is False


def test_apply_decision_validates_schema(tmp_path) -> None:
    db = str(tmp_path / "insider_alerts.db")
    init_db(db)
    enqueue_review_packet(db, _sample_ref(), {"score": 10})

    bad = {"decision": "approve"}
    try:
        apply_decision(db, bad)
    except DecisionValidationError:
        pass
    else:
        raise AssertionError("expected DecisionValidationError")

    good = {
        "packet_id": "0000320193-24-000123|0000320193|4",
        "decision": "approve",
        "analyst": "carlo",
        "reason": "high confidence",
    }
    updated = apply_decision(db, good, notification_required=True)
    assert updated == 1

    assert mark_notification_delivered(
        db, good["packet_id"], good, proof=_delivery_proof()
    ) == 1
    with sqlite3.connect(db) as conn:
        delivered_at = conn.execute(
            "SELECT notification_sent_at FROM review_packets WHERE packet_id = ?",
            (good["packet_id"],),
        ).fetchone()[0]
    assert delivered_at is not None

    try:
        apply_decision(
            db,
            {
                "packet_id": "0000320193-24-000123|0000320193|4",
                "decision": "invalid",
                "analyst": "carlo",
                "reason": "no",
            },
        )
    except DecisionValidationError:
        pass
    else:
        raise AssertionError("expected validation error for invalid decision")


def test_notification_intent_is_atomic_and_remains_until_delivery(tmp_path) -> None:
    db = str(tmp_path / "insider_alerts.db")
    init_db(db)
    enqueue_review_packet(db, _sample_ref(), {"score": 99})
    packet_id = "0000320193-24-000123|0000320193|4"

    assert apply_decision(
        db,
        {
            "packet_id": packet_id,
            "decision": "approve",
            "analyst": "quant",
            "reason": "send this",
        },
        notification_required=True,
    ) == 1

    outbox = list_notification_outbox(db, limit=10)
    assert [row["packet_id"] for row in outbox] == [packet_id]
    assert outbox[0]["decision"]["reason"] == "send this"  # type: ignore[index]
    decision = outbox[0]["decision"]
    assert isinstance(decision, dict)
    assert mark_notification_delivered(
        db, packet_id, decision, proof=_delivery_proof()
    ) == 1
    assert list_notification_outbox(db, limit=10) == []


def test_invalid_delivery_proof_leaves_outbox_and_ledger_unchanged(tmp_path) -> None:
    db = str(tmp_path / "insider_alerts.db")
    init_db(db)
    enqueue_review_packet(db, _sample_ref(), {"score": 99})
    packet_id = "0000320193-24-000123|0000320193|4"
    decision = {
        "packet_id": packet_id,
        "decision": "approve",
        "analyst": "quant",
        "reason": "send this",
    }
    assert apply_decision(db, decision, notification_required=True) == 1
    invalid_proof = NotificationDeliveryProof(
        transport_id="a" * 64,
        attempt_number=True,
        responded_at_utc=datetime.now(UTC),
        request_body_sha256="b" * 64,
        route_sha256="c" * 64,
        http_status=200,
    )

    with pytest.raises(ValueError, match="invalid"):
        mark_notification_delivered(db, packet_id, decision, proof=invalid_proof)

    assert [row["packet_id"] for row in list_notification_outbox(db, limit=10)] == [packet_id]
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM notification_delivery_acks").fetchone()[0] == 0


def test_replayed_decision_clears_prior_notification_acknowledgement(tmp_path) -> None:
    db = str(tmp_path / "insider_alerts.db")
    init_db(db)
    enqueue_review_packet(db, _sample_ref(), {"score": 99})
    packet_id = "0000320193-24-000123|0000320193|4"
    deadletter = {
        "packet_id": packet_id,
        "decision": "deadletter",
        "analyst": "quant",
        "reason": "retry later",
    }
    assert apply_decision(db, deadletter, notification_required=True) == 1
    assert mark_notification_delivered(
        db, packet_id, deadletter, proof=_delivery_proof()
    ) == 1

    assert replay_deadletter(db, packet_id) == 1
    approval = {**deadletter, "decision": "approve", "reason": "send replay"}
    assert apply_decision(db, approval, notification_required=True) == 1

    assert [row["packet_id"] for row in list_notification_outbox(db, limit=10)] == [packet_id]
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        ack = conn.execute("SELECT * FROM notification_delivery_acks").fetchone()
        assert ack is not None
        encoded = bytes(ack["record_json"])
        record = json.loads(encoded)
        assert int(ack["sequence"]) == 1
        assert record["contract_version"] == "notification-delivery-ack-v1"
        assert record["transport_id"] == "a" * 64
        assert record["http_status"] == 200
        assert hashlib.sha256(encoded).hexdigest() == ack["record_sha256"]
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            conn.execute("UPDATE notification_delivery_acks SET http_status=201")
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            conn.execute("DELETE FROM notification_delivery_acks")


def test_stale_notification_ack_cannot_acknowledge_replayed_decision(tmp_path) -> None:
    db = str(tmp_path / "insider_alerts.db")
    init_db(db)
    enqueue_review_packet(db, _sample_ref(), {"score": 99})
    packet_id = "0000320193-24-000123|0000320193|4"
    first = {
        "packet_id": packet_id,
        "decision": "deadletter",
        "analyst": "quant",
        "reason": "first decision",
    }
    second = {**first, "decision": "approve", "reason": "replacement decision"}
    assert apply_decision(db, first, notification_required=True) == 1
    assert replay_deadletter(db, packet_id) == 1
    assert apply_decision(db, second, notification_required=True) == 1

    assert mark_notification_delivered(
        db, packet_id, first, proof=_delivery_proof()
    ) == 0
    assert [row["decision"] for row in list_notification_outbox(db, limit=10)] == [second]
    assert mark_notification_delivered(
        db, packet_id, second, proof=_delivery_proof()
    ) == 1


def test_suppression_is_distinct_from_provider_delivery(tmp_path) -> None:
    db = str(tmp_path / "insider_alerts.db")
    init_db(db)
    enqueue_review_packet(db, _sample_ref(), {"score": 99})
    packet_id = "0000320193-24-000123|0000320193|4"
    decision = {
        "packet_id": packet_id,
        "decision": "approve",
        "analyst": "quant",
        "reason": "duplicate co-filing",
    }
    assert apply_decision(db, decision, notification_required=True) == 1
    assert mark_notification_suppressed(db, packet_id, decision) == 1
    assert list_notification_outbox(db, limit=10) == []
    with sqlite3.connect(db) as conn:
        sent_at, suppressed_at = conn.execute(
            "SELECT notification_sent_at, notification_suppressed_at "
            "FROM review_packets WHERE packet_id=?",
            (packet_id,),
        ).fetchone()
    assert sent_at is None
    assert suppressed_at is not None


def test_decision_can_atomically_record_duplicate_suppression(tmp_path) -> None:
    db = str(tmp_path / "insider_alerts.db")
    init_db(db)
    enqueue_review_packet(db, _sample_ref(), {"score": 99})
    packet_id = "0000320193-24-000123|0000320193|4"
    decision = {
        "packet_id": packet_id,
        "decision": "approve",
        "analyst": "quant",
        "reason": "same economic event",
    }

    assert (
        apply_decision(
            db,
            decision,
            notification_required=True,
            notification_suppressed=True,
        )
        == 1
    )
    assert list_notification_outbox(db, limit=10) == []
    with sqlite3.connect(db) as conn:
        required, sent_at, suppressed_at = conn.execute(
            "SELECT notification_required, notification_sent_at, notification_suppressed_at "
            "FROM review_packets WHERE packet_id=?",
            (packet_id,),
        ).fetchone()
    assert required == 1
    assert sent_at is None
    assert suppressed_at is not None


def test_list_deadletters_returns_records(tmp_path) -> None:
    db = str(tmp_path / "insider_alerts.db")
    init_db(db)
    enqueue_review_packet(db, _sample_ref(), {"score": 10})
    apply_decision(
        db,
        {
            "packet_id": "0000320193-24-000123|0000320193|4",
            "decision": "deadletter",
            "analyst": "carlo",
            "reason": "parser drift",
        },
    )
    rows = list_deadletters(db)
    assert len(rows) == 1
    payload = json.loads(rows[0]["decision_json"])
    assert payload["decision"] == "deadletter"

    replayed = replay_deadletter(db, "0000320193-24-000123|0000320193|4")
    assert replayed == 1


def test_apply_decision_rejects_invalid_packet_id(tmp_path) -> None:
    db = str(tmp_path / "insider_alerts.db")
    init_db(db)
    enqueue_review_packet(db, _sample_ref(), {"score": 10})

    try:
        apply_decision(
            db,
            {
                "packet_id": "bad-id",
                "decision": "approve",
                "analyst": "carlo",
                "reason": "bad",
            },
        )
    except DecisionValidationError:
        pass
    else:
        raise AssertionError("expected validation error for invalid packet_id")


def test_list_pending_review_packets_returns_pending_only(tmp_path) -> None:
    db = str(tmp_path / "insider_alerts.db")
    init_db(db)
    enqueue_review_packet(db, _sample_ref(accession_number="0000320193-24-000123"), {"score": 10})
    enqueue_review_packet(db, _sample_ref(accession_number="0000320193-24-000124"), {"score": 12})
    apply_decision(
        db,
        {
            "packet_id": "0000320193-24-000123|0000320193|4",
            "decision": "deadletter",
            "analyst": "carlo",
            "reason": "parser drift",
        },
    )

    rows = list_pending_review_packets(db, limit=10)
    assert len(rows) == 1
    assert rows[0]["packet_id"] == "0000320193-24-000124|0000320193|4"
    assert rows[0]["payload"]["score"] == 12


def test_list_pending_review_packets_prioritizes_oldest_retry(tmp_path) -> None:
    db = str(tmp_path / "insider_alerts.db")
    init_db(db)
    enqueue_review_packet(db, _sample_ref(accession_number="0000320193-24-000123"), {"score": 10})
    enqueue_review_packet(db, _sample_ref(accession_number="0000320193-24-000124"), {"score": 12})
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE review_packets SET created_at=? WHERE accession_number=?",
            ("2026-08-27T21:01:00+00:00", "0000320193-24-000123"),
        )
        conn.execute(
            "UPDATE review_packets SET created_at=? WHERE accession_number=?",
            ("2026-08-27T21:02:00+00:00", "0000320193-24-000124"),
        )

    rows = list_pending_review_packets(db, limit=1)

    assert [row["packet_id"] for row in rows] == [
        "0000320193-24-000123|0000320193|4"
    ]
