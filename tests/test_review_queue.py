import json
from datetime import UTC, datetime

from insider_alerts.review.queue import (
    DecisionValidationError,
    apply_decision,
    enqueue_review_packet,
    list_deadletters,
    list_pending_review_packets,
    replay_deadletter,
)
from insider_alerts.sec.models import FilingRef
from insider_alerts.sec.store import init_db


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
    updated = apply_decision(db, good)
    assert updated == 1

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
