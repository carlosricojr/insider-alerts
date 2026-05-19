import json
import sqlite3
from datetime import UTC, datetime

from insider_alerts.backtest.data import (
    _optional_float,
    _rationale_bool,
    _rationale_float,
    _string_keyed_dict,
    load_scored_signals,
)
from insider_alerts.review.queue import ensure_review_tables
from insider_alerts.sec.store import init_db


def test_data_type_narrowing_helpers_drop_invalid_shapes() -> None:
    assert _string_keyed_dict(["not", "a", "dict"]) == {}
    assert _string_keyed_dict({"score": "95", 1: "drop"}) == {"score": "95"}

    assert _optional_float("12.5") == 12.5
    assert _optional_float("not-a-number") is None
    assert _optional_float(object()) is None

    rationale: dict[str, object] = {
        "numeric": "7.5",
        "bad_numeric": "not-a-number",
        "truthy_int": 1,
        "truthy_str": "yes",
        "nested": {},
    }
    assert _rationale_float(rationale, "numeric") == 7.5
    assert _rationale_float(rationale, "bad_numeric") == 0.0
    assert _rationale_bool(rationale, "truthy_int") is True
    assert _rationale_bool(rationale, "truthy_str") is True
    assert _rationale_bool(rationale, "nested") is False


def test_load_scored_signals_reads_payload_and_filters_dates(tmp_path) -> None:
    db_path = str(tmp_path / "db.sqlite3")
    init_db(db_path)
    ensure_review_tables(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO filings (
                source, cik, accession_number, form_type, filed_at,
                filing_detail_url, primary_doc_url, raw_rss_entry
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "sec_rss",
                "0000063276",
                "0001708842-26-000005",
                "4",
                datetime(2026, 2, 12, 20, 39, 47, tzinfo=UTC).isoformat(),
                "https://www.sec.gov/example-index.htm",
                None,
                json.dumps({"title": "example"}),
            ),
        )
        conn.execute(
            """
            INSERT INTO review_packets (
                packet_id, accession_number, cik, form_type, payload_json, status,
                decision_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "0001708842-26-000005|0000063276|4",
                "0001708842-26-000005",
                "0000063276",
                "4",
                json.dumps(
                    {
                        "issuer_symbol": "MAT",
                        "score": 95.0,
                        "rationale": {
                            "open_market_buy_shares": 65000.0,
                            "open_market_net_shares": 65000.0,
                            "has_10b5_1_plan": False,
                            "has_equity_comp_event": False,
                            "has_tax_withholding_language": False,
                            "role_tier": "chief_exec",
                        },
                    }
                ),
                "pending",
                None,
                datetime(2026, 2, 12, 20, 42, 24, tzinfo=UTC).isoformat(),
                datetime(2026, 2, 12, 20, 42, 24, tzinfo=UTC).isoformat(),
            ),
        )
        conn.commit()

    signals = load_scored_signals(db_path)
    assert len(signals) == 1
    assert signals[0].symbol == "MAT"
    assert signals[0].score == 95.0
    assert signals[0].open_market_buy_shares == 65000.0
    assert signals[0].role_tier == "chief_exec"

    filtered = load_scored_signals(
        db_path,
        start_date=datetime(2026, 2, 13, tzinfo=UTC).date(),
    )
    assert filtered == []
