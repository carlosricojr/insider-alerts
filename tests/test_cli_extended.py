import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from shutil import rmtree
from types import SimpleNamespace
from uuid import uuid4

import pytest
from typer.testing import CliRunner

from insider_alerts import cli
from insider_alerts.backtest.engine import (
    BacktestMetrics,
    BacktestParams,
    GridSearchResult,
    WalkForwardResult,
)
from insider_alerts.backtest.event_data import CanonicalEvent
from insider_alerts.backtest.event_study import (
    MonotonicityResult,
    NegativeControlSummary,
    OosEventStudyResult,
    OosFoldResult,
    ScoreBucketMetrics,
)
from insider_alerts.backtest.models import DailyBar, SignalEvent
from insider_alerts.backtest.readiness import EventStudyReadinessReport
from insider_alerts.sec.client import SecHttpError
from insider_alerts.sec.pipeline import BackfillResult, EnrichResult, PollResult, QueueResult


def test_cli_sec_enrich(monkeypatch) -> None:
    runner = CliRunner()

    def fake(settings, *, limit: int):  # type: ignore[no-untyped-def]
        assert limit == 11
        return EnrichResult(scanned=11, updated=7)

    monkeypatch.setattr(cli, "enrich_filings_with_xml_url", fake)
    result = runner.invoke(cli.app, ["sec", "enrich", "--limit", "11"])
    assert result.exit_code == 0
    assert "updated=7" in result.stdout


def test_cli_review_enqueue(monkeypatch) -> None:
    runner = CliRunner()
    captured: dict[str, object] = {}

    def fake(
        settings,
        *,
        limit: int,
        oldest_first: bool = False,
        start_date=None,
        end_date=None,
    ):  # type: ignore[no-untyped-def]
        assert limit == 9
        captured["oldest_first"] = oldest_first
        captured["start_date"] = start_date
        captured["end_date"] = end_date
        return QueueResult(processed=9, enqueued=3)

    monkeypatch.setattr(cli, "enqueue_review_packets", fake)
    result = runner.invoke(
        cli.app,
        [
            "review",
            "enqueue",
            "--limit",
            "9",
            "--oldest-first",
            "--start-date",
            "2025-01-01",
            "--end-date",
            "2025-12-31",
        ],
    )
    assert result.exit_code == 0
    assert "enqueued=3" in result.stdout
    assert "http_failed=0" in result.stdout
    assert captured["oldest_first"] is True
    assert str(captured["start_date"]) == "2025-01-01"
    assert str(captured["end_date"]) == "2025-12-31"


def test_cli_review_enqueue_requires_both_dates(monkeypatch) -> None:
    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "review",
            "enqueue",
            "--start-date",
            "2025-01-01",
        ],
    )
    assert result.exit_code == 2
    assert "must provide both --start-date and --end-date" in result.stderr


def test_cli_ops_deadletter(monkeypatch) -> None:
    runner = CliRunner()
    monkeypatch.setattr(
        cli,
        "list_deadletters",
        lambda db_path: [
            {
                "packet_id": "p-1",
                "reason": "x",
                "decision_json": "{}",
                "created_at": "now",
            }
        ],
    )
    monkeypatch.setattr(cli, "replay_deadletter", lambda db_path, packet_id: 1)

    list_result = runner.invoke(cli.app, ["ops", "deadletter-list"])
    assert list_result.exit_code == 0
    assert json.loads(list_result.stdout)[0]["packet_id"] == "p-1"

    replay_result = runner.invoke(cli.app, ["ops", "deadletter-replay", "--packet-id", "p-1"])
    assert replay_result.exit_code == 0
    assert "updated=1" in replay_result.stdout


def test_cli_ops_autopilot_once(monkeypatch, tmp_path: Path) -> None:
    runner = CliRunner()
    monkeypatch.setattr(
        cli,
        "_load_conviction_history",
        lambda *args, **kwargs: cli._empty_conviction_history(),
    )

    monkeypatch.setattr(
        cli,
        "run_sec_poll_once",
        lambda settings, *, max_items, dry_run: PollResult(
            fetched=5,
            inserted=3,
            skipped_existing=2,
        ),
    )
    monkeypatch.setattr(
        cli,
        "enrich_filings_with_xml_url",
        lambda settings, *, limit: EnrichResult(scanned=3, updated=2),
    )
    monkeypatch.setattr(
        cli,
        "enqueue_review_packets",
        lambda settings, *, limit: QueueResult(processed=3, enqueued=3),
    )
    monkeypatch.setattr(
        cli,
        "list_pending_review_packets",
        lambda db_path, limit: [
            {
                "packet_id": "0000905148-26-000640|0001824653|4",
                "payload": {
                    "score": 100.0,
                    "rationale": {
                        "net_buy_shares": 4754.0,
                        "open_market_buy_shares": 4754.0,
                        "open_market_gross_value": 13268128.95,
                        "holding_change_ratio": 0.14,
                        "trade_pct_daily_turnover": 0.42,
                    },
                },
            },
            {
                "packet_id": "0001818383-26-000028|0001829946|4",
                "payload": {
                    "score": 16.0,
                    "rationale": {
                        "net_buy_shares": -12000.0,
                        "open_market_buy_shares": 0.0,
                    },
                },
            },
            {
                "packet_id": "0000950103-26-001988|0001326801|4",
                "payload": {
                    "score": 58.2,
                    "rationale": {
                        "net_buy_shares": -517.0,
                        "open_market_buy_shares": 0.0,
                    },
                },
            },
        ],
    )

    decisions: list[str] = []

    def fake_apply(db_path: str, payload):  # type: ignore[no-untyped-def]
        decisions.append(str(payload["decision"]))
        return 1

    monkeypatch.setattr(cli, "apply_decision", fake_apply)

    notifications: list[str] = []

    def fake_notify(settings, payload, *, packet=None, dry_message=None):  # type: ignore[no-untyped-def]
        notifications.append(str(payload["decision"]))

    monkeypatch.setattr(cli, "_send_review_notification", fake_notify)

    result = runner.invoke(
        cli.app,
        [
            "ops",
            "autopilot",
            "--once",
            "--decision-engine",
            "rules",
            "--poll-max-items",
            "40",
            "--enrich-limit",
            "100",
            "--enqueue-limit",
            "100",
            "--decision-limit",
            "100",
            "--output-log",
            str(tmp_path / "autopilot.out.log"),
            "--error-log",
            str(tmp_path / "autopilot.err.log"),
        ],
    )

    assert result.exit_code == 0
    assert decisions == ["approve", "reject", "reject"]
    assert notifications == ["approve"]
    assert "approved=1" in result.stdout
    assert "rejected=2" in result.stdout
    assert "approved=1" in (tmp_path / "autopilot.out.log").read_text(encoding="utf-8")
    assert not (tmp_path / "autopilot.err.log").exists()


def test_cli_ops_autopilot_quant_reason_flows_to_apply_and_notify(monkeypatch) -> None:
    runner = CliRunner()
    monkeypatch.setattr(
        cli,
        "_load_conviction_history",
        lambda *args, **kwargs: cli._empty_conviction_history(),
    )
    monkeypatch.setattr(
        cli,
        "run_sec_poll_once",
        lambda settings, *, max_items, dry_run: PollResult(
            fetched=1,
            inserted=1,
            skipped_existing=0,
        ),
    )
    monkeypatch.setattr(
        cli,
        "enrich_filings_with_xml_url",
        lambda settings, *, limit: EnrichResult(scanned=1, updated=1),
    )
    monkeypatch.setattr(
        cli,
        "enqueue_review_packets",
        lambda settings, *, limit: QueueResult(processed=1, enqueued=1),
    )
    packet = {
        "packet_id": "0000905148-26-000640|0001824653|4",
        "payload": {
            "issuer_symbol": "CEG",
            "owner": "Hanson Bryan Craig",
            "score": 100.0,
                "rationale": {
                    "net_buy_shares": 4754.0,
                    "gross_value": 13268128.95,
                    "open_market_buy_shares": 4754.0,
                    "open_market_gross_value": 13268128.95,
                    "holding_change_ratio": 0.14,
                    "trade_pct_daily_turnover": 0.42,
                    "has_10b5_1_plan": False,
                    "has_equity_comp_event": False,
                    "has_tax_withholding_language": False,
                "owner_is_ten_percent_owner": False,
                "owner_is_exec": True,
            },
        },
    }
    monkeypatch.setattr(cli, "list_pending_review_packets", lambda db_path, limit: [packet])
    def fake_quant_decide(  # type: ignore[no-untyped-def]
        packets, *, quant_agent_id, quant_timeout_seconds, quant_thinking, quant_batch_size
    ):
        return (
            {
                "0000905148-26-000640|0001824653|4": cli.AutoDecisionRuleResult(
                    decision="approve",
                    reason="Quant thesis: large insider open-market buy with unusual size.",
                    source="quant:main",
                    confidence=0.92,
                )
            },
            None,
        )

    monkeypatch.setattr(cli, "_decide_packets_with_quant", fake_quant_decide)

    applied: list[dict[str, object]] = []

    def fake_apply(db_path: str, payload):  # type: ignore[no-untyped-def]
        applied.append(payload)
        return 1

    monkeypatch.setattr(cli, "apply_decision", fake_apply)

    notified: list[dict[str, str]] = []

    def fake_notify(settings, payload, *, packet=None, dry_message=None):  # type: ignore[no-untyped-def]
        notified.append(payload)

    monkeypatch.setattr(cli, "_send_review_notification", fake_notify)

    result = runner.invoke(
        cli.app,
        [
                "ops",
                "autopilot",
                "--once",
                "--decision-engine",
                "quant",
                "--quant-agent-id",
                "quant-insider",
                "--decision-limit",
                "10",
            ],
        )

    assert result.exit_code == 0
    assert len(applied) == 1
    assert applied[0]["reason"] == "Quant thesis: large insider open-market buy with unusual size."
    assert applied[0]["decision_source"] == "quant:main"
    assert applied[0]["confidence"] == 0.92
    assert len(notified) == 1
    assert notified[0]["reason"] == "Quant thesis: large insider open-market buy with unusual size."


def test_cli_ops_autopilot_blocks_low_liquidity_director_approval(monkeypatch) -> None:
    runner = CliRunner()
    monkeypatch.setattr(
        cli,
        "run_sec_poll_once",
        lambda settings, *, max_items, dry_run: PollResult(
            fetched=1,
            inserted=1,
            skipped_existing=0,
        ),
    )
    monkeypatch.setattr(
        cli,
        "enrich_filings_with_xml_url",
        lambda settings, *, limit: EnrichResult(scanned=1, updated=1),
    )
    monkeypatch.setattr(
        cli,
        "enqueue_review_packets",
        lambda settings, *, limit: QueueResult(processed=1, enqueued=1),
    )
    packet = {
        "packet_id": "0001467638-26-000004|0000064040|4",
        "payload": {
            "issuer_symbol": "SPGI",
            "owner": "Joly Hubert",
            "score": 100.0,
            "rationale": {
                "net_buy_shares": 2500.0,
                "open_market_buy_shares": 2500.0,
                "trade_pct_daily_turnover": 0.0493,
                "role_tier": "director",
                "has_10b5_1_plan": False,
                "has_equity_comp_event": False,
                "has_tax_withholding_language": False,
                "owner_is_ten_percent_owner": False,
                "owner_is_exec": True,
            },
        },
    }
    monkeypatch.setattr(cli, "list_pending_review_packets", lambda db_path, limit: [packet])

    def fake_quant_decide(  # type: ignore[no-untyped-def]
        packets, *, quant_agent_id, quant_timeout_seconds, quant_thinking, quant_batch_size
    ):
        return (
            {
                "0001467638-26-000004|0000064040|4": cli.AutoDecisionRuleResult(
                    decision="approve",
                    reason="Quant thesis: director buy.",
                    source="quant:main",
                    confidence=0.99,
                    reason_code="quant_high_edge",
                )
            },
            None,
        )

    monkeypatch.setattr(cli, "_decide_packets_with_quant", fake_quant_decide)

    applied: list[dict[str, object]] = []

    def fake_apply(db_path: str, payload):  # type: ignore[no-untyped-def]
        applied.append(payload)
        return 1

    monkeypatch.setattr(cli, "apply_decision", fake_apply)
    monkeypatch.setattr(cli, "_send_review_notification", lambda *args, **kwargs: None)

    result = runner.invoke(
        cli.app,
        [
            "ops",
            "autopilot",
            "--once",
            "--decision-engine",
            "quant",
            "--quant-agent-id",
            "quant-insider",
            "--decision-limit",
            "10",
        ],
    )

    assert result.exit_code == 0
    assert len(applied) == 1
    assert applied[0]["decision"] == "escalate"
    assert applied[0]["decision_reason_code"] == "safety_low_edge_director"


def test_cli_ops_autopilot_quant_only_requests_baseline_pass_packets(monkeypatch) -> None:
    runner = CliRunner()
    monkeypatch.setattr(
        cli,
        "_load_conviction_history",
        lambda *args, **kwargs: cli._empty_conviction_history(),
    )
    monkeypatch.setattr(
        cli,
        "run_sec_poll_once",
        lambda settings, *, max_items, dry_run: PollResult(
            fetched=2,
            inserted=2,
            skipped_existing=0,
        ),
    )
    monkeypatch.setattr(
        cli,
        "enrich_filings_with_xml_url",
        lambda settings, *, limit: EnrichResult(scanned=2, updated=2),
    )
    monkeypatch.setattr(
        cli,
        "enqueue_review_packets",
        lambda settings, *, limit: QueueResult(processed=2, enqueued=2),
    )
    monkeypatch.setattr(
        cli,
        "list_pending_review_packets",
        lambda db_path, limit: [
            {
                "packet_id": "0000905148-26-000640|0001824653|4",
                "payload": {
                    "score": 100.0,
                    "rationale": {
                        "net_buy_shares": 4754.0,
                        "open_market_buy_shares": 4754.0,
                        "open_market_gross_value": 13268128.95,
                        "holding_change_ratio": 0.14,
                        "trade_pct_daily_turnover": 0.42,
                    },
                },
            },
            {
                "packet_id": "0001818383-26-000028|0001829946|4",
                "payload": {
                    "score": 10.0,
                    "rationale": {
                        "net_buy_shares": -12000.0,
                        "open_market_buy_shares": 0.0,
                    },
                },
            },
        ],
    )

    captured_quant_packets: list[str] = []

    def fake_quant_decide(  # type: ignore[no-untyped-def]
        packets, *, quant_agent_id, quant_timeout_seconds, quant_thinking, quant_batch_size
    ):
        captured_quant_packets.extend(str(packet.get("packet_id")) for packet in packets)
        return (
            {
                "0000905148-26-000640|0001824653|4": cli.AutoDecisionRuleResult(
                    decision="approve",
                    reason="Quant thesis: high-conviction signal.",
                    source="quant:main",
                    confidence=0.93,
                )
            },
            None,
        )

    monkeypatch.setattr(cli, "_decide_packets_with_quant", fake_quant_decide)
    monkeypatch.setattr(cli, "apply_decision", lambda db_path, payload: 1)
    monkeypatch.setattr(cli, "_send_review_notification", lambda *args, **kwargs: None)

    result = runner.invoke(
        cli.app,
        [
            "ops",
            "autopilot",
            "--once",
            "--decision-engine",
            "quant",
            "--quant-agent-id",
            "quant-insider",
            "--decision-limit",
            "10",
        ],
    )
    assert result.exit_code == 0
    assert captured_quant_packets == ["0000905148-26-000640|0001824653|4"]


@dataclass
class _Completed:
    returncode: int
    stdout: str
    stderr: str


def test_decide_packets_with_quant_batches_requests(monkeypatch) -> None:
    monkeypatch.setattr(cli, "_resolve_quant_cmds", lambda: [("claude.exe", "claude")])
    calls: list[int] = []

    def fake_run(args, **kwargs):  # type: ignore[no-untyped-def]
        message = str(args[args.index("-p") + 1])
        for char in ["|", "<", ">", "&", "%", "^"]:
            assert char not in message
        request_json = message.split("Input: ", 1)[1]
        request = json.loads(request_json)
        packets = request["packets"]
        assert "has_10b5_1_plan" in packets[0]
        assert "owner_is_ten_percent_owner" in packets[0]
        assert "holding_change_ratio" in packets[0]
        calls.append(len(packets))
        decisions = [
            {
                "packet_id": packet["packet_id"],
                "decision": "escalate",
                "why": "quant batched",
                "edge_hypothesis": "no edge",
                "risk_flags": ["insufficient novelty"],
                "evidence": {
                    "role_tier": "director",
                    "open_market_buy_shares": 0,
                    "trade_pct_daily_turnover": 0,
                    "novelty_penalty": 55,
                    "regime_earnings_shock_flag": False,
                },
                "confidence": 0.9,
            }
            for packet in packets
        ]
        inner = json.dumps({"decisions": decisions})
        outer = json.dumps({"type": "result", "is_error": False, "result": inner})
        return _Completed(returncode=0, stdout=outer, stderr="")

    monkeypatch.setattr(cli.subprocess, "run", fake_run)

    packets = [
        {
            "packet_id": f"0000000000-00-0000{i:02d}|00000000{i:02d}|4",
            "payload": {"score": 50.0, "rationale": {"net_buy_shares": 10.0}},
        }
        for i in range(25)
    ]
    mapped, error = cli._decide_packets_with_quant(
        packets,
        quant_agent_id="quant-insider",
        quant_timeout_seconds=30,
        quant_thinking="low",
        quant_batch_size=10,
    )

    assert error is None
    assert len(mapped) == 25
    assert set(mapped.keys()) == {packet["packet_id"] for packet in packets}
    assert calls == [10, 10, 5]


def test_decide_packets_with_quant_rejects_invalid_schema(monkeypatch) -> None:
    monkeypatch.setattr(cli, "_resolve_quant_cmds", lambda: [("claude.exe", "claude")])

    def fake_run(args, **kwargs):  # type: ignore[no-untyped-def]
        message = str(args[args.index("-p") + 1])
        request_json = message.split("Input: ", 1)[1]
        request = json.loads(request_json)
        packet_id = request["packets"][0]["packet_id"]
        decisions = [
            {
                "packet_id": packet_id,
                "decision": "approve",
                "why": "missing required fields",
                "confidence": 0.95,
            }
        ]
        inner = json.dumps({"decisions": decisions})
        outer = json.dumps({"type": "result", "is_error": False, "result": inner})
        return _Completed(returncode=0, stdout=outer, stderr="")

    monkeypatch.setattr(cli.subprocess, "run", fake_run)

    packets = [
        {
            "packet_id": "0000000000-00-000001|0000000001|4",
            "payload": {"score": 99.0, "rationale": {"net_buy_shares": 1000.0}},
        }
    ]
    mapped, error = cli._decide_packets_with_quant(
        packets,
        quant_agent_id="quant-insider",
        quant_timeout_seconds=30,
        quant_thinking="low",
        quant_batch_size=10,
    )

    assert mapped == {}
    assert error is not None
    assert "invalid decision schema" in error


def test_decide_packets_with_quant_fails_over_per_missing_packet_and_surfaces_stdout_error(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        cli,
        "_resolve_quant_cmds",
        lambda: [("claude.exe", "claude"), ("codex.exe", "codex")],
    )
    calls: list[str] = []

    def fake_run(args, **kwargs):  # type: ignore[no-untyped-def]
        calls.append(str(args[0]))
        assert "creationflags" in kwargs
        if args[0] == "claude.exe":
            outer = json.dumps(
                {
                    "type": "result",
                    "is_error": True,
                    "api_error_status": 429,
                    "result": "You've reached your model limit",
                }
            )
            return _Completed(returncode=1, stdout=outer, stderr="")
        message = str(args[-1])
        request = json.loads(message.split("Input: ", 1)[1])
        decisions = [
            {
                "packet_id": packet["packet_id"],
                "decision": "approve",
                "why": "fallback classified immutable payload",
                "edge_hypothesis": "discretionary conviction buy",
                "risk_flags": [],
                "evidence": {
                    "role_tier": "executive",
                    "open_market_buy_shares": 1000,
                    "trade_pct_daily_turnover": 0.4,
                    "novelty_penalty": 0,
                    "regime_earnings_shock_flag": False,
                },
                "confidence": 0.95,
            }
            for packet in request["packets"]
        ]
        return _Completed(
            returncode=0,
            stdout=json.dumps({"decisions": decisions}),
            stderr="",
        )

    monkeypatch.setattr(cli.subprocess, "run", fake_run)
    packet_id = "0000000000-00-000001|0000000001|4"
    mapped, error = cli._decide_packets_with_quant(
        [
            {
                "packet_id": packet_id,
                "payload": {"score": 99.0, "rationale": {"net_buy_shares": 1000.0}},
            }
        ],
        quant_agent_id="quant-insider",
        quant_timeout_seconds=30,
        quant_thinking="low",
        quant_batch_size=10,
    )

    assert calls == ["claude.exe", "codex.exe"]
    assert mapped[packet_id].decision == "approve"
    assert mapped[packet_id].source == "quant:quant-insider:codex:gpt-5.6-sol:low"
    assert error is not None
    assert "You've reached your model limit" in error


def test_decide_packets_with_quant_keeps_valid_primary_decision_and_falls_back_only_missing(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        cli,
        "_resolve_quant_cmds",
        lambda: [("primary.exe", "codex"), ("secondary.exe", "codex")],
    )
    requested_aliases: list[list[str]] = []

    def decision(alias: str) -> dict[str, object]:
        return {
            "packet_id": alias,
            "decision": "reject",
            "why": "classified immutable payload",
            "edge_hypothesis": "no durable edge",
            "risk_flags": [],
            "evidence": {
                "role_tier": "director",
                "open_market_buy_shares": 10,
                "trade_pct_daily_turnover": 0.001,
                "novelty_penalty": 50,
                "regime_earnings_shock_flag": False,
            },
            "confidence": 0.9,
        }

    def fake_run(args, **kwargs):  # type: ignore[no-untyped-def]
        request = json.loads(str(args[-1]).split("Input: ", 1)[1])
        aliases = [str(packet["packet_id"]) for packet in request["packets"]]
        requested_aliases.append(aliases)
        selected = aliases[:1] if args[0] == "primary.exe" else aliases
        return _Completed(
            returncode=0,
            stdout=json.dumps({"decisions": [decision(alias) for alias in selected]}),
            stderr="",
        )

    monkeypatch.setattr(cli.subprocess, "run", fake_run)
    packet_ids = [
        "0000000000-00-000001|0000000001|4",
        "0000000000-00-000002|0000000002|4",
    ]
    mapped, error = cli._decide_packets_with_quant(
        [
            {
                "packet_id": packet_id,
                "payload": {"score": 50.0, "rationale": {"net_buy_shares": 10.0}},
            }
            for packet_id in packet_ids
        ],
        quant_agent_id="quant-insider",
        quant_timeout_seconds=30,
        quant_thinking="low",
        quant_batch_size=10,
    )

    assert requested_aliases == [["P00000", "P00001"], ["P00001"]]
    assert set(mapped) == set(packet_ids)
    assert error is not None
    assert "missing decisions for 1 packet" in error


def test_decide_packets_with_quant_handles_nontext_error_streams(monkeypatch) -> None:
    monkeypatch.setattr(cli, "_resolve_quant_cmds", lambda: [("claude.exe", "claude")])
    monkeypatch.setattr(
        cli.subprocess,
        "run",
        lambda *args, **kwargs: _Completed(  # type: ignore[arg-type]
            returncode=1,
            stdout=None,
            stderr=None,
        ),
    )

    mapped, error = cli._decide_packets_with_quant(
        [
            {
                "packet_id": "0000000000-00-000001|0000000001|4",
                "payload": {"score": 50.0, "rationale": {"net_buy_shares": 10.0}},
            }
        ],
        quant_agent_id="quant-insider",
        quant_timeout_seconds=30,
        quant_thinking="low",
        quant_batch_size=10,
    )

    assert mapped == {}
    assert error is not None
    assert "invalid JSON envelope" in error


def test_cli_ops_autopilot_defers_quant_infrastructure_failure(monkeypatch) -> None:
    runner = CliRunner()
    monkeypatch.setattr(
        cli,
        "_load_conviction_history",
        lambda *args, **kwargs: cli._empty_conviction_history(),
    )
    monkeypatch.setattr(
        cli,
        "run_sec_poll_once",
        lambda settings, *, max_items, dry_run: PollResult(0, 0, 0),
    )
    monkeypatch.setattr(
        cli,
        "enrich_filings_with_xml_url",
        lambda settings, *, limit: EnrichResult(scanned=0, updated=0),
    )
    monkeypatch.setattr(
        cli,
        "enqueue_review_packets",
        lambda settings, *, limit: QueueResult(processed=0, enqueued=0),
    )
    packet_id = "0000000000-00-000001|0000000001|4"
    monkeypatch.setattr(
        cli,
        "list_pending_review_packets",
        lambda db_path, limit: [
            {
                "packet_id": packet_id,
                "payload": {
                    "score": 99.0,
                    "rationale": {
                        "net_buy_shares": 1000.0,
                        "open_market_buy_shares": 1000.0,
                    },
                },
            }
        ],
    )
    monkeypatch.setattr(
        cli,
        "_decide_packets_with_quant",
        lambda *args, **kwargs: ({}, "claude 429; codex unavailable"),
    )
    applied: list[dict[str, object]] = []
    monkeypatch.setattr(
        cli,
        "apply_decision",
        lambda db_path, payload: applied.append(dict(payload)) or 1,
    )

    result = runner.invoke(
        cli.app,
        ["ops", "autopilot", "--once", "--decision-engine", "quant"],
    )

    assert result.exit_code == 0
    assert applied == []
    assert "decided=0" in result.stdout
    assert "quant_deferred=1" in result.stdout
    assert "quant decision engine degraded (0/1 decided)" in result.stderr


def test_cli_ops_autopilot_blocks_main_quant_agent_in_isolated_mode() -> None:
    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "ops",
            "autopilot",
            "--once",
            "--decision-engine",
            "quant",
            "--quant-agent-id",
            "main",
        ],
    )
    assert result.exit_code == 2
    assert "unsafe quant agent" in result.stderr


def test_cli_ops_autopilot_deadletters_duplicate_packets(monkeypatch) -> None:
    runner = CliRunner()
    monkeypatch.setattr(
        cli,
        "_load_conviction_history",
        lambda *args, **kwargs: cli._empty_conviction_history(),
    )
    monkeypatch.setattr(
        cli,
        "run_sec_poll_once",
        lambda settings, *, max_items, dry_run: PollResult(
            fetched=2,
            inserted=2,
            skipped_existing=0,
        ),
    )
    monkeypatch.setattr(
        cli,
        "enrich_filings_with_xml_url",
        lambda settings, *, limit: EnrichResult(scanned=2, updated=2),
    )
    monkeypatch.setattr(
        cli,
        "enqueue_review_packets",
        lambda settings, *, limit: QueueResult(processed=2, enqueued=2),
    )
    monkeypatch.setattr(
        cli,
        "list_pending_review_packets",
        lambda db_path, limit: [
            {
                "packet_id": "0000905148-26-000640|0001824653|4",
                "payload": {
                    "score": 100.0,
                    "rationale": {
                        "net_buy_shares": 4754.0,
                        "open_market_buy_shares": 4754.0,
                        "open_market_gross_value": 13268128.95,
                        "holding_change_ratio": 0.14,
                        "trade_pct_daily_turnover": 0.42,
                    },
                },
            },
            {
                "packet_id": "0000905148-26-000640|0001868275|4",
                "payload": {
                    "score": 100.0,
                    "rationale": {
                        "net_buy_shares": 4754.0,
                        "open_market_buy_shares": 4754.0,
                        "open_market_gross_value": 13268128.95,
                        "holding_change_ratio": 0.14,
                        "trade_pct_daily_turnover": 0.42,
                    },
                },
            },
        ],
    )

    decisions: list[str] = []

    def fake_apply(db_path: str, payload):  # type: ignore[no-untyped-def]
        decisions.append(str(payload["decision"]))
        return 1

    monkeypatch.setattr(cli, "apply_decision", fake_apply)

    notifications: list[str] = []

    def fake_notify(settings, payload, *, packet=None, dry_message=None):  # type: ignore[no-untyped-def]
        notifications.append(str(payload["decision"]))

    monkeypatch.setattr(cli, "_send_review_notification", fake_notify)

    result = runner.invoke(
        cli.app,
        [
            "ops",
            "autopilot",
            "--once",
            "--decision-engine",
            "rules",
        ],
    )

    assert result.exit_code == 0
    assert decisions == ["approve", "deadletter"]
    assert notifications == ["approve"]
    assert "deadlettered=1" in result.stdout


@pytest.mark.parametrize(
    ("chain_status", "counter"),
    [
        ("succeeded", "option_chain_succeeded=1"),
        ("skipped_cadence", "option_chain_skipped_cadence=1"),
        ("failed", "option_chain_failed=1"),
        ("timed_out", "option_chain_timed_out=1"),
        ("admitted", "option_chain_ambiguous=1"),
        ("exception", "option_chain_failed=1"),
    ],
)
def test_cli_ops_autopilot_capture_is_causal_and_fail_isolated(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    chain_status: str,
    counter: str,
) -> None:
    runner = CliRunner()
    monkeypatch.setattr(
        cli,
        "_load_conviction_history",
        lambda *args, **kwargs: cli._empty_conviction_history(),
    )
    monkeypatch.setattr(
        cli,
        "run_sec_poll_once",
        lambda settings, *, max_items, dry_run: PollResult(1, 1, 0),
    )
    monkeypatch.setattr(
        cli,
        "enrich_filings_with_xml_url",
        lambda settings, *, limit: EnrichResult(scanned=1, updated=1),
    )
    monkeypatch.setattr(
        cli,
        "enqueue_review_packets",
        lambda settings, *, limit: QueueResult(processed=1, enqueued=1),
    )
    packet_id = "0000905148-26-000640|0001824653|4"
    monkeypatch.setattr(
        cli,
        "list_pending_review_packets",
        lambda db_path, limit: [
            {
                "packet_id": packet_id,
                "payload": {
                    "issuer_symbol": "ABC",
                    "score": 100.0,
                    "rationale": {
                        "net_buy_shares": 5000.0,
                        "open_market_buy_shares": 5000.0,
                        "open_market_gross_value": 1_000_000.0,
                        "holding_change_ratio": 0.5,
                        "trade_pct_daily_turnover": 0.5,
                    },
                },
            }
        ],
    )
    events: list[str] = []

    def fake_capture(config, *, packet_id: str, symbol: str):  # type: ignore[no-untyped-def]
        events.append("capture")
        assert symbol == "ABC"
        assert config.chain_store_db == tmp_path / "chain.db"
        if chain_status == "exception":
            raise RuntimeError("research-only failure")
        return SimpleNamespace(
            status=chain_status,
            batch_id="insider-batch",
            exit_code=0 if chain_status == "succeeded" else 2,
            error_kind=None if chain_status == "succeeded" else "TEST_FAILURE",
        )

    monkeypatch.setattr(cli, "capture_predecision_option_chain", fake_capture)
    applied: list[dict[str, object]] = []

    def fake_apply(db_path: str, payload: dict[str, object]) -> int:
        events.append("apply")
        applied.append(dict(payload))
        return 1

    monkeypatch.setattr(cli, "apply_decision", fake_apply)

    result = runner.invoke(
        cli.app,
        [
            "ops",
            "autopilot",
            "--once",
            "--decision-engine",
            "rules",
            "--no-notify",
            "--alpha-chain-python",
            str(tmp_path / "python.exe"),
            "--alpha-chain-script",
            str(tmp_path / "capture.py"),
            "--option-chain-store-db",
            str(tmp_path / "chain.db"),
            "--error-log",
            str(tmp_path / "errors.log"),
        ],
    )

    assert result.exit_code == 0, result.exception
    assert events == ["capture", "apply"]
    assert [payload["decision"] for payload in applied] == ["approve"]
    assert counter in result.stdout


def test_cli_ops_autopilot_requires_complete_chain_configuration(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        cli.app,
        [
            "ops",
            "autopilot",
            "--once",
            "--alpha-chain-python",
            str(tmp_path / "python.exe"),
        ],
    )

    assert result.exit_code == 2
    assert "must be provided together" in result.stderr


def test_cli_ops_autopilot_once_exits_on_sec_http_error(
    monkeypatch, tmp_path: Path
) -> None:
    runner = CliRunner()

    def fake_poll(settings, *, max_items: int, dry_run: bool):  # type: ignore[no-untyped-def]
        raise SecHttpError("dns resolution failed")

    monkeypatch.setattr(cli, "run_sec_poll_once", fake_poll)

    result = runner.invoke(
        cli.app,
        [
            "ops",
            "autopilot",
            "--once",
            "--decision-engine",
            "rules",
            "--error-log",
            str(tmp_path / "autopilot.err.log"),
        ],
    )

    assert result.exit_code == 1
    assert "ops autopilot cycle failed" in result.stderr
    assert "dns resolution failed" in result.stderr
    assert "dns resolution failed" in (tmp_path / "autopilot.err.log").read_text(
        encoding="utf-8"
    )


def test_cli_ops_autopilot_loop_recovers_from_sec_http_error(
    monkeypatch, tmp_path: Path
) -> None:
    runner = CliRunner()

    calls = {"poll": 0, "sleep": 0}

    def fake_poll(settings, *, max_items: int, dry_run: bool):  # type: ignore[no-untyped-def]
        calls["poll"] += 1
        if calls["poll"] == 1:
            raise SecHttpError("transient dns failure")
        if calls["poll"] >= 3:
            raise RuntimeError("stop-loop")
        return PollResult(fetched=0, inserted=0, skipped_existing=0)

    def fake_sleep(seconds: int) -> None:
        calls["sleep"] += 1

    monkeypatch.setattr(cli, "run_sec_poll_once", fake_poll)
    monkeypatch.setattr(
        cli,
        "enrich_filings_with_xml_url",
        lambda settings, *, limit: EnrichResult(scanned=0, updated=0),
    )
    monkeypatch.setattr(
        cli,
        "enqueue_review_packets",
        lambda settings, *, limit: QueueResult(processed=0, enqueued=0),
    )
    monkeypatch.setattr(cli, "list_pending_review_packets", lambda db_path, limit: [])
    monkeypatch.setattr(cli.time, "sleep", fake_sleep)
    monkeypatch.setattr(cli, "runtime_source_fingerprint", lambda: "a" * 64)

    result = runner.invoke(
        cli.app,
        [
            "ops",
            "autopilot",
            "--loop",
            "--interval",
            "10",
            "--decision-engine",
            "rules",
            "--error-log",
            str(tmp_path / "autopilot.err.log"),
        ],
    )

    assert result.exit_code == 1
    assert isinstance(result.exception, RuntimeError)
    assert "stop-loop" in str(result.exception)
    assert calls == {"poll": 3, "sleep": 2}
    assert "ops autopilot cycle failed" in result.stderr
    assert "transient dns failure" in result.stderr
    assert "ops autopilot cycle completed" in result.stdout
    error_log = (tmp_path / "autopilot.err.log").read_text(encoding="utf-8")
    assert "transient dns failure" in error_log
    assert "autopilot process failed (RuntimeError: stop-loop)" in error_log


def test_cli_ops_autopilot_loop_exits_cleanly_when_source_changes(
    monkeypatch, tmp_path: Path
) -> None:
    calls: dict[str, object] = {"poll": 0, "sleeps": []}
    fingerprints = iter(("a" * 64, "a" * 64, "a" * 64, "a" * 64, "b" * 64))

    def fake_poll(settings, *, max_items: int, dry_run: bool):  # type: ignore[no-untyped-def]
        calls["poll"] = int(calls["poll"]) + 1
        return PollResult(fetched=0, inserted=0, skipped_existing=0)

    monkeypatch.setattr(cli, "run_sec_poll_once", fake_poll)
    monkeypatch.setattr(
        cli,
        "enrich_filings_with_xml_url",
        lambda settings, *, limit: EnrichResult(scanned=0, updated=0),
    )
    monkeypatch.setattr(
        cli,
        "enqueue_review_packets",
        lambda settings, *, limit: QueueResult(processed=0, enqueued=0),
    )
    monkeypatch.setattr(cli, "list_pending_review_packets", lambda db_path, limit: [])
    monkeypatch.setattr(cli, "runtime_source_fingerprint", lambda: next(fingerprints))
    monkeypatch.setattr(
        cli.time,
        "sleep",
        lambda seconds: calls["sleeps"].append(seconds),  # type: ignore[union-attr]
    )
    output_log = tmp_path / "autopilot.out.log"

    result = CliRunner().invoke(
        cli.app,
        [
            "ops",
            "autopilot",
            "--loop",
            "--interval",
            "31",
            "--decision-engine",
            "rules",
            "--output-log",
            str(output_log),
        ],
    )

    assert result.exit_code == 0
    assert calls == {"poll": 1, "sleeps": [15.0, 15.0, 1.0]}
    message = "autopilot source changed; exiting so the hidden repeating task can start"
    assert message in result.stderr
    assert message in output_log.read_text(encoding="utf-8")


def test_cli_ops_autopilot_once_does_not_fingerprint_source(monkeypatch) -> None:
    def forbidden_fingerprint() -> str:
        raise AssertionError("once mode must not fingerprint source")

    monkeypatch.setattr(cli, "runtime_source_fingerprint", forbidden_fingerprint)
    monkeypatch.setattr(
        cli,
        "run_sec_poll_once",
        lambda settings, *, max_items, dry_run: PollResult(
            fetched=0, inserted=0, skipped_existing=0
        ),
    )
    monkeypatch.setattr(
        cli,
        "enrich_filings_with_xml_url",
        lambda settings, *, limit: EnrichResult(scanned=0, updated=0),
    )
    monkeypatch.setattr(
        cli,
        "enqueue_review_packets",
        lambda settings, *, limit: QueueResult(processed=0, enqueued=0),
    )
    monkeypatch.setattr(cli, "list_pending_review_packets", lambda db_path, limit: [])

    result = CliRunner().invoke(
        cli.app,
        ["ops", "autopilot", "--once", "--decision-engine", "rules"],
    )

    assert result.exit_code == 0


def test_cli_ops_autopilot_logs_startup_fingerprint_failure(
    monkeypatch, tmp_path: Path
) -> None:
    poll_called = False

    def fail_fingerprint() -> str:
        raise OSError("source temporarily unreadable")

    def forbidden_poll(settings, *, max_items: int, dry_run: bool):  # type: ignore[no-untyped-def]
        nonlocal poll_called
        poll_called = True
        raise AssertionError("cycle must not start without source provenance")

    monkeypatch.setattr(cli, "runtime_source_fingerprint", fail_fingerprint)
    monkeypatch.setattr(cli, "run_sec_poll_once", forbidden_poll)
    error_log = tmp_path / "autopilot.err.log"

    result = CliRunner().invoke(
        cli.app,
        [
            "ops",
            "autopilot",
            "--loop",
            "--interval",
            "10",
            "--decision-engine",
            "rules",
            "--error-log",
            str(error_log),
        ],
    )

    assert result.exit_code == 1
    assert poll_called is False
    assert "autopilot process failed (OSError: source temporarily unreadable)" in (
        error_log.read_text(encoding="utf-8")
    )


def test_trade_signal_notification_includes_ticker_and_why() -> None:
    packet = {
        "packet_id": "0000905148-26-000640|0001824653|4",
        "payload": {
            "issuer_symbol": "CEG",
            "owner": "Hanson Bryan Craig",
            "score": 100.0,
            "rationale": {"net_buy_shares": 4754.0, "gross_value": 13268128.95},
        },
    }
    decision_payload = {
        "packet_id": "0000905148-26-000640|0001824653|4",
        "decision": "approve",
        "analyst": "quant",
        "decision_source": "quant:main",
        "reason": "Quant thesis: high-conviction insider accumulation.",
    }
    title, message, tags, priority = cli._build_trade_signal_notification(packet, decision_payload)
    assert title == "TRADE SIGNAL: CEG"
    assert "ticker=CEG" in message
    assert "why=Quant thesis: high-conviction insider accumulation." in message
    assert "trade-signal" in tags
    assert priority == 4


def test_trade_signal_notification_includes_conviction_metrics() -> None:
    packet = {
        "packet_id": "0000905148-26-000640|0001824653|4",
        "payload": {
            "issuer_symbol": "CEG",
            "owner": "Hanson Bryan Craig",
            "score": 100.0,
            "rationale": {"net_buy_shares": 4754.0, "gross_value": 13268128.95},
        },
    }
    decision_payload = {
        "packet_id": "0000905148-26-000640|0001824653|4",
        "decision": "approve",
        "analyst": "quant",
        "decision_source": "quant:main",
        "reason": "Quant thesis.",
        "conviction_score": "78.2",
        "conviction_holding_pct": "81.0",
        "conviction_value_pct": "76.0",
        "conviction_liquidity_pct": "62.0",
    }
    _, message, _, _ = cli._build_trade_signal_notification(packet, decision_payload)
    assert "conviction_score=78.2" in message
    assert "conviction_holding_pct=81.0" in message
    assert "conviction_value_pct=76.0" in message
    assert "conviction_liquidity_pct=62.0" in message


def test_cli_ops_backtest_outputs_report(monkeypatch) -> None:
    runner = CliRunner()
    signal = SignalEvent(
        packet_id="0001708842-26-000005|0000063276|4",
        symbol="MAT",
        filed_at=datetime(2026, 2, 12, 20, 39, 47, tzinfo=UTC),
        score=95.0,
        open_market_buy_shares=65000.0,
        open_market_net_shares=65000.0,
        has_10b5_1_plan=False,
        has_equity_comp_event=False,
        has_tax_withholding_language=False,
        role_tier="chief_exec",
    )
    bar = DailyBar(
        symbol="MAT",
        trade_date=date(2026, 2, 13),
        open=16.0,
        high=16.5,
        low=15.8,
        close=16.2,
        volume=1000000.0,
    )
    metrics = BacktestMetrics(
        trade_count=1,
        skipped_count=0,
        mean_return=0.01,
        median_return=0.01,
        win_rate=1.0,
        profit_factor=float("inf"),
        max_drawdown=0.0,
        sharpe_like=None,
        mean_alpha=0.005,
        median_alpha=0.005,
        objective_score=0.005,
    )
    params = BacktestParams(min_score=90.0, hold_days=5, stop_loss_pct=0.05, take_profit_rr=2.0)

    monkeypatch.setattr(cli, "load_scored_signals", lambda *args, **kwargs: [signal])
    monkeypatch.setattr(
        cli,
        "get_filing_date_bounds",
        lambda *args, **kwargs: (date(2000, 1, 1), date(2099, 1, 1)),
    )
    monkeypatch.setattr(
        cli,
        "backfill_form4_filings",
        lambda *args, **kwargs: BackfillResult(
            requested_quarters=0,
            fetched_quarters=0,
            matched_filings=0,
            inserted=0,
            skipped_existing=0,
        ),
    )
    monkeypatch.setattr(cli, "get_price_bar_bounds", lambda *args, **kwargs: (None, None))
    monkeypatch.setattr(cli, "refresh_price_bars", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "get_price_bars", lambda *args, **kwargs: [bar])

    class _FakePriceClient:
        def fetch_history(self, symbol):  # type: ignore[no-untyped-def]
            return [bar]

    monkeypatch.setattr(cli, "StooqPriceClient", lambda **kwargs: _FakePriceClient())
    monkeypatch.setattr(
        cli,
        "evaluate_parameter_grid",
        lambda *args, **kwargs: [GridSearchResult(params=params, metrics=metrics)],
    )
    monkeypatch.setattr(cli, "run_backtest", lambda *args, **kwargs: (metrics, []))
    monkeypatch.setattr(
        cli,
        "run_walk_forward",
        lambda *args, **kwargs: WalkForwardResult(
            folds=[],
            aggregate_test_metrics=metrics,
            recommended_params=params,
        ),
    )

    tmp_dir = Path(".tmp_testdata") / f"cli_backtest_{uuid4().hex}"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    try:
        output_path = tmp_dir / "report.json"
        result = runner.invoke(
            cli.app,
            [
                "ops",
                "backtest",
                "--output-json",
                str(output_path),
                "--start-date",
                "2026-02-01",
                "--end-date",
                "2026-02-20",
            ],
        )
        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        assert payload["signals_total"] == 1
        assert payload["best_in_sample_params"]["min_score"] == 90.0
        assert output_path.exists()
    finally:
        rmtree(tmp_dir, ignore_errors=True)


def test_cli_ops_backtest_defaults_to_last_year_when_dates_omitted(monkeypatch) -> None:
    runner = CliRunner()
    signal = SignalEvent(
        packet_id="0001708842-26-000005|0000063276|4",
        symbol="MAT",
        filed_at=datetime(2026, 2, 12, 20, 39, 47, tzinfo=UTC),
        score=95.0,
        open_market_buy_shares=65000.0,
        open_market_net_shares=65000.0,
        has_10b5_1_plan=False,
        has_equity_comp_event=False,
        has_tax_withholding_language=False,
        role_tier="chief_exec",
    )
    bar = DailyBar(
        symbol="MAT",
        trade_date=date(2026, 2, 13),
        open=16.0,
        high=16.5,
        low=15.8,
        close=16.2,
        volume=1000000.0,
    )
    metrics = BacktestMetrics(
        trade_count=1,
        skipped_count=0,
        mean_return=0.01,
        median_return=0.01,
        win_rate=1.0,
        profit_factor=float("inf"),
        max_drawdown=0.0,
        sharpe_like=None,
        mean_alpha=0.005,
        median_alpha=0.005,
        objective_score=0.005,
    )
    params = BacktestParams(min_score=90.0, hold_days=5, stop_loss_pct=0.05, take_profit_rr=2.0)
    captured: dict[str, date] = {}

    def _fake_load_scored_signals(db_path, *, start_date, end_date):  # type: ignore[no-untyped-def]
        captured["start_date"] = start_date
        captured["end_date"] = end_date
        return [signal]

    monkeypatch.setattr(cli, "load_scored_signals", _fake_load_scored_signals)
    monkeypatch.setattr(
        cli,
        "get_filing_date_bounds",
        lambda *args, **kwargs: (date(2000, 1, 1), date(2099, 1, 1)),
    )
    monkeypatch.setattr(
        cli,
        "backfill_form4_filings",
        lambda *args, **kwargs: BackfillResult(
            requested_quarters=0,
            fetched_quarters=0,
            matched_filings=0,
            inserted=0,
            skipped_existing=0,
        ),
    )
    monkeypatch.setattr(
        cli,
        "get_price_bar_bounds",
        lambda *args, **kwargs: (date(2026, 1, 1), date(2026, 2, 13)),
    )
    monkeypatch.setattr(cli, "refresh_price_bars", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "get_price_bars", lambda *args, **kwargs: [bar])

    class _FakePriceClient:
        def fetch_history(self, symbol):  # type: ignore[no-untyped-def]
            return [bar]

    monkeypatch.setattr(cli, "StooqPriceClient", lambda **kwargs: _FakePriceClient())
    monkeypatch.setattr(
        cli,
        "evaluate_parameter_grid",
        lambda *args, **kwargs: [GridSearchResult(params=params, metrics=metrics)],
    )
    monkeypatch.setattr(cli, "run_backtest", lambda *args, **kwargs: (metrics, []))
    monkeypatch.setattr(
        cli,
        "run_walk_forward",
        lambda *args, **kwargs: WalkForwardResult(
            folds=[],
            aggregate_test_metrics=metrics,
            recommended_params=params,
        ),
    )

    result = runner.invoke(cli.app, ["ops", "backtest"])
    assert result.exit_code == 0
    today = date.today()
    assert captured["end_date"] == today
    assert captured["start_date"] == today - timedelta(days=365)
    payload = json.loads(result.stdout)
    assert payload["date_window_mode"] == "default_last_year"


def test_cli_ops_backtest_requires_both_dates(monkeypatch) -> None:
    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "ops",
            "backtest",
            "--start-date",
            "2026-01-01",
        ],
    )
    assert result.exit_code == 2
    assert "must provide both --start-date and --end-date" in result.stderr


def test_cli_ops_backtest_bootstraps_when_default_window_has_no_signals(monkeypatch) -> None:
    runner = CliRunner()
    signal = SignalEvent(
        packet_id="0001708842-26-000005|0000063276|4",
        symbol="MAT",
        filed_at=datetime(2026, 2, 12, 20, 39, 47, tzinfo=UTC),
        score=95.0,
        open_market_buy_shares=65000.0,
        open_market_net_shares=65000.0,
        has_10b5_1_plan=False,
        has_equity_comp_event=False,
        has_tax_withholding_language=False,
        role_tier="chief_exec",
    )
    bar = DailyBar(
        symbol="MAT",
        trade_date=date(2026, 2, 13),
        open=16.0,
        high=16.5,
        low=15.8,
        close=16.2,
        volume=1000000.0,
    )
    metrics = BacktestMetrics(
        trade_count=1,
        skipped_count=0,
        mean_return=0.01,
        median_return=0.01,
        win_rate=1.0,
        profit_factor=float("inf"),
        max_drawdown=0.0,
        sharpe_like=None,
        mean_alpha=0.005,
        median_alpha=0.005,
        objective_score=0.005,
    )
    params = BacktestParams(min_score=90.0, hold_days=5, stop_loss_pct=0.05, take_profit_rr=2.0)
    calls = {"load": 0, "backfill": 0, "enrich": 0, "enqueue": 0}

    def _fake_load_scored_signals(db_path, *, start_date, end_date):  # type: ignore[no-untyped-def]
        calls["load"] += 1
        if calls["load"] == 1:
            return []
        return [signal]

    monkeypatch.setattr(cli, "load_scored_signals", _fake_load_scored_signals)

    def _fake_backfill(settings, *, start_date, end_date):  # type: ignore[no-untyped-def]
        calls["backfill"] += 1
        return BackfillResult(
            requested_quarters=4,
            fetched_quarters=4,
            matched_filings=120,
            inserted=120,
            skipped_existing=0,
        )

    def _fake_enrich(settings, *, limit):  # type: ignore[no-untyped-def]
        calls["enrich"] += 1
        return EnrichResult(scanned=2, updated=2)

    def _fake_enqueue(settings, *, limit):  # type: ignore[no-untyped-def]
        calls["enqueue"] += 1
        return QueueResult(processed=2, enqueued=1)

    monkeypatch.setattr(cli, "backfill_form4_filings", _fake_backfill)
    monkeypatch.setattr(cli, "enrich_filings_with_xml_url", _fake_enrich)
    monkeypatch.setattr(cli, "enqueue_review_packets", _fake_enqueue)
    monkeypatch.setattr(cli, "get_filing_date_bounds", lambda *args, **kwargs: (None, None))
    monkeypatch.setattr(
        cli,
        "get_price_bar_bounds",
        lambda *args, **kwargs: (date(2026, 1, 1), date(2026, 2, 13)),
    )
    monkeypatch.setattr(cli, "refresh_price_bars", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "get_price_bars", lambda *args, **kwargs: [bar])

    class _FakePriceClient:
        def fetch_history(self, symbol):  # type: ignore[no-untyped-def]
            return [bar]

    monkeypatch.setattr(cli, "StooqPriceClient", lambda **kwargs: _FakePriceClient())
    monkeypatch.setattr(
        cli,
        "evaluate_parameter_grid",
        lambda *args, **kwargs: [GridSearchResult(params=params, metrics=metrics)],
    )
    monkeypatch.setattr(cli, "run_backtest", lambda *args, **kwargs: (metrics, []))
    monkeypatch.setattr(
        cli,
        "run_walk_forward",
        lambda *args, **kwargs: WalkForwardResult(
            folds=[],
            aggregate_test_metrics=metrics,
            recommended_params=params,
        ),
    )

    result = runner.invoke(cli.app, ["ops", "backtest"])
    assert result.exit_code == 0
    assert calls["load"] == 2
    assert calls["backfill"] == 1
    assert calls["enrich"] == 1
    assert calls["enqueue"] == 1
    payload = json.loads(result.stdout)
    assert payload["bootstrap_refresh"]["backfill_requested_quarters"] == 4
    assert payload["bootstrap_refresh"]["enqueue_enqueued"] == 1


def test_cli_ops_backtest_bootstrap_uses_oldest_first_enqueue_with_window(monkeypatch) -> None:
    runner = CliRunner()
    signal = SignalEvent(
        packet_id="0001708842-26-000005|0000063276|4",
        symbol="MAT",
        filed_at=datetime(2026, 2, 12, 20, 39, 47, tzinfo=UTC),
        score=95.0,
        open_market_buy_shares=65000.0,
        open_market_net_shares=65000.0,
        has_10b5_1_plan=False,
        has_equity_comp_event=False,
        has_tax_withholding_language=False,
        role_tier="chief_exec",
    )
    bar = DailyBar(
        symbol="MAT",
        trade_date=date(2026, 2, 13),
        open=16.0,
        high=16.5,
        low=15.8,
        close=16.2,
        volume=1000000.0,
    )
    metrics = BacktestMetrics(
        trade_count=1,
        skipped_count=0,
        mean_return=0.01,
        median_return=0.01,
        win_rate=1.0,
        profit_factor=float("inf"),
        max_drawdown=0.0,
        sharpe_like=None,
        mean_alpha=0.005,
        median_alpha=0.005,
        objective_score=0.005,
    )
    params = BacktestParams(min_score=90.0, hold_days=5, stop_loss_pct=0.05, take_profit_rr=2.0)
    calls = {"load": 0}
    captured: dict[str, object] = {}

    def _fake_load_scored_signals(db_path, *, start_date, end_date):  # type: ignore[no-untyped-def]
        calls["load"] += 1
        if calls["load"] == 1:
            return []
        return [signal]

    def _fake_enqueue(
        settings,
        *,
        limit,
        oldest_first=False,
        start_date=None,
        end_date=None,
    ):  # type: ignore[no-untyped-def]
        captured["limit"] = limit
        captured["oldest_first"] = oldest_first
        captured["start_date"] = start_date
        captured["end_date"] = end_date
        return QueueResult(processed=2, enqueued=1)

    monkeypatch.setattr(cli, "load_scored_signals", _fake_load_scored_signals)
    monkeypatch.setattr(
        cli,
        "backfill_form4_filings",
        lambda *args, **kwargs: BackfillResult(
            requested_quarters=4,
            fetched_quarters=4,
            matched_filings=120,
            inserted=120,
            skipped_existing=0,
        ),
    )
    monkeypatch.setattr(
        cli,
        "enrich_filings_with_xml_url",
        lambda settings, *, limit: EnrichResult(scanned=2, updated=2),
    )
    monkeypatch.setattr(cli, "enqueue_review_packets", _fake_enqueue)
    monkeypatch.setattr(cli, "get_filing_date_bounds", lambda *args, **kwargs: (None, None))
    monkeypatch.setattr(
        cli,
        "get_price_bar_bounds",
        lambda *args, **kwargs: (date(2026, 1, 1), date(2026, 2, 13)),
    )
    monkeypatch.setattr(cli, "refresh_price_bars", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "get_price_bars", lambda *args, **kwargs: [bar])

    class _FakePriceClient:
        def fetch_history(self, symbol):  # type: ignore[no-untyped-def]
            return [bar]

    monkeypatch.setattr(cli, "StooqPriceClient", lambda **kwargs: _FakePriceClient())
    monkeypatch.setattr(
        cli,
        "evaluate_parameter_grid",
        lambda *args, **kwargs: [GridSearchResult(params=params, metrics=metrics)],
    )
    monkeypatch.setattr(cli, "run_backtest", lambda *args, **kwargs: (metrics, []))
    monkeypatch.setattr(
        cli,
        "run_walk_forward",
        lambda *args, **kwargs: WalkForwardResult(
            folds=[],
            aggregate_test_metrics=metrics,
            recommended_params=params,
        ),
    )

    result = runner.invoke(
        cli.app,
        [
            "ops",
            "backtest",
            "--start-date",
            "2025-02-12",
            "--end-date",
            "2026-02-12",
        ],
    )
    assert result.exit_code == 0
    assert captured["limit"] == 1000
    assert captured["oldest_first"] is True
    assert str(captured["start_date"]) == "2025-02-12"
    assert str(captured["end_date"]) == "2026-02-12"


def test_cli_ops_backtest_backfills_when_date_coverage_is_missing(monkeypatch) -> None:
    runner = CliRunner()
    signal = SignalEvent(
        packet_id="0001708842-26-000005|0000063276|4",
        symbol="MAT",
        filed_at=datetime(2026, 2, 12, 20, 39, 47, tzinfo=UTC),
        score=95.0,
        open_market_buy_shares=65000.0,
        open_market_net_shares=65000.0,
        has_10b5_1_plan=False,
        has_equity_comp_event=False,
        has_tax_withholding_language=False,
        role_tier="chief_exec",
    )
    bar = DailyBar(
        symbol="MAT",
        trade_date=date(2026, 2, 13),
        open=16.0,
        high=16.5,
        low=15.8,
        close=16.2,
        volume=1000000.0,
    )
    metrics = BacktestMetrics(
        trade_count=1,
        skipped_count=0,
        mean_return=0.01,
        median_return=0.01,
        win_rate=1.0,
        profit_factor=float("inf"),
        max_drawdown=0.0,
        sharpe_like=None,
        mean_alpha=0.005,
        median_alpha=0.005,
        objective_score=0.005,
    )
    params = BacktestParams(min_score=90.0, hold_days=5, stop_loss_pct=0.05, take_profit_rr=2.0)
    calls = {"backfill": 0}

    monkeypatch.setattr(cli, "load_scored_signals", lambda *args, **kwargs: [signal])

    def _fake_backfill(settings, *, start_date, end_date):  # type: ignore[no-untyped-def]
        calls["backfill"] += 1
        return BackfillResult(
            requested_quarters=5,
            fetched_quarters=5,
            matched_filings=2000,
            inserted=1000,
            skipped_existing=1000,
        )

    monkeypatch.setattr(cli, "backfill_form4_filings", _fake_backfill)
    monkeypatch.setattr(
        cli,
        "enrich_filings_with_xml_url",
        lambda *args, **kwargs: EnrichResult(scanned=0, updated=0),
    )
    monkeypatch.setattr(
        cli,
        "enqueue_review_packets",
        lambda *args, **kwargs: QueueResult(processed=0, enqueued=0),
    )
    monkeypatch.setattr(
        cli,
        "get_filing_date_bounds",
        lambda *args, **kwargs: (date(2026, 2, 10), date(2026, 2, 12)),
    )
    monkeypatch.setattr(
        cli,
        "get_price_bar_bounds",
        lambda *args, **kwargs: (date(2026, 1, 1), date(2026, 2, 13)),
    )
    monkeypatch.setattr(cli, "refresh_price_bars", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "get_price_bars", lambda *args, **kwargs: [bar])

    class _FakePriceClient:
        def fetch_history(self, symbol):  # type: ignore[no-untyped-def]
            return [bar]

    monkeypatch.setattr(cli, "StooqPriceClient", lambda **kwargs: _FakePriceClient())
    monkeypatch.setattr(
        cli,
        "evaluate_parameter_grid",
        lambda *args, **kwargs: [GridSearchResult(params=params, metrics=metrics)],
    )
    monkeypatch.setattr(cli, "run_backtest", lambda *args, **kwargs: (metrics, []))
    monkeypatch.setattr(
        cli,
        "run_walk_forward",
        lambda *args, **kwargs: WalkForwardResult(
            folds=[],
            aggregate_test_metrics=metrics,
            recommended_params=params,
        ),
    )

    result = runner.invoke(
        cli.app,
        [
            "ops",
            "backtest",
            "--start-date",
            "2025-02-12",
            "--end-date",
            "2026-02-12",
        ],
    )

    assert result.exit_code == 0
    assert calls["backfill"] == 1
    payload = json.loads(result.stdout)
    assert payload["bootstrap_refresh"]["backfill_fetched_quarters"] == 5


def test_cli_ops_backtest_continues_when_bootstrap_db_is_locked(monkeypatch) -> None:
    runner = CliRunner()
    signal = SignalEvent(
        packet_id="0001708842-26-000005|0000063276|4",
        symbol="MAT",
        filed_at=datetime(2026, 2, 12, 20, 39, 47, tzinfo=UTC),
        score=95.0,
        open_market_buy_shares=65000.0,
        open_market_net_shares=65000.0,
        has_10b5_1_plan=False,
        has_equity_comp_event=False,
        has_tax_withholding_language=False,
        role_tier="chief_exec",
    )
    bar = DailyBar(
        symbol="MAT",
        trade_date=date(2026, 2, 13),
        open=16.0,
        high=16.5,
        low=15.8,
        close=16.2,
        volume=1000000.0,
    )
    metrics = BacktestMetrics(
        trade_count=1,
        skipped_count=0,
        mean_return=0.01,
        median_return=0.01,
        win_rate=1.0,
        profit_factor=float("inf"),
        max_drawdown=0.0,
        sharpe_like=None,
        mean_alpha=0.005,
        median_alpha=0.005,
        objective_score=0.005,
    )
    params = BacktestParams(min_score=90.0, hold_days=5, stop_loss_pct=0.05, take_profit_rr=2.0)

    monkeypatch.setattr(cli, "load_scored_signals", lambda *args, **kwargs: [signal])
    monkeypatch.setattr(
        cli,
        "get_filing_date_bounds",
        lambda *args, **kwargs: (date(2026, 2, 10), date(2026, 2, 12)),
    )
    monkeypatch.setattr(
        cli,
        "backfill_form4_filings",
        lambda *args, **kwargs: BackfillResult(
            requested_quarters=5,
            fetched_quarters=5,
            matched_filings=2000,
            inserted=1000,
            skipped_existing=1000,
        ),
    )

    def _locked_enrich(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(cli, "enrich_filings_with_xml_url", _locked_enrich)
    monkeypatch.setattr(
        cli,
        "get_price_bar_bounds",
        lambda *args, **kwargs: (date(2026, 1, 1), date(2026, 2, 13)),
    )
    monkeypatch.setattr(cli, "refresh_price_bars", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "get_price_bars", lambda *args, **kwargs: [bar])

    class _FakePriceClient:
        def fetch_history(self, symbol):  # type: ignore[no-untyped-def]
            return [bar]

    monkeypatch.setattr(cli, "StooqPriceClient", lambda **kwargs: _FakePriceClient())
    monkeypatch.setattr(
        cli,
        "evaluate_parameter_grid",
        lambda *args, **kwargs: [GridSearchResult(params=params, metrics=metrics)],
    )
    monkeypatch.setattr(cli, "run_backtest", lambda *args, **kwargs: (metrics, []))
    monkeypatch.setattr(
        cli,
        "run_walk_forward",
        lambda *args, **kwargs: WalkForwardResult(
            folds=[],
            aggregate_test_metrics=metrics,
            recommended_params=params,
        ),
    )

    result = runner.invoke(cli.app, ["ops", "backtest"])

    assert result.exit_code == 0
    assert "database is locked during bootstrap refresh" in result.stderr
    payload = json.loads(result.stdout)
    assert payload["signals_total"] == 1
    assert payload["bootstrap_refresh"] is None


def test_cli_ops_backtest_continues_on_unexpected_price_refresh_error(monkeypatch) -> None:
    runner = CliRunner()
    mat_signal = SignalEvent(
        packet_id="0001708842-26-000005|0000063276|4",
        symbol="MAT",
        filed_at=datetime(2026, 2, 12, 20, 39, 47, tzinfo=UTC),
        score=95.0,
        open_market_buy_shares=65000.0,
        open_market_net_shares=65000.0,
        has_10b5_1_plan=False,
        has_equity_comp_event=False,
        has_tax_withholding_language=False,
        role_tier="chief_exec",
    )
    bad_symbol_signal = SignalEvent(
        packet_id="0001234567-26-000001|0001234567|4",
        symbol="Z AND ZG",
        filed_at=datetime(2026, 2, 12, 21, 0, 0, tzinfo=UTC),
        score=92.0,
        open_market_buy_shares=1000.0,
        open_market_net_shares=1000.0,
        has_10b5_1_plan=False,
        has_equity_comp_event=False,
        has_tax_withholding_language=False,
        role_tier="officer",
    )
    bar = DailyBar(
        symbol="MAT",
        trade_date=date(2026, 2, 13),
        open=16.0,
        high=16.5,
        low=15.8,
        close=16.2,
        volume=1000000.0,
    )
    metrics = BacktestMetrics(
        trade_count=1,
        skipped_count=0,
        mean_return=0.01,
        median_return=0.01,
        win_rate=1.0,
        profit_factor=float("inf"),
        max_drawdown=0.0,
        sharpe_like=None,
        mean_alpha=0.005,
        median_alpha=0.005,
        objective_score=0.005,
    )
    params = BacktestParams(min_score=90.0, hold_days=5, stop_loss_pct=0.05, take_profit_rr=2.0)

    monkeypatch.setattr(
        cli,
        "load_scored_signals",
        lambda *args, **kwargs: [mat_signal, bad_symbol_signal],
    )
    monkeypatch.setattr(
        cli,
        "get_filing_date_bounds",
        lambda *args, **kwargs: (date(2000, 1, 1), date(2099, 1, 1)),
    )
    monkeypatch.setattr(
        cli,
        "backfill_form4_filings",
        lambda *args, **kwargs: BackfillResult(
            requested_quarters=0,
            fetched_quarters=0,
            matched_filings=0,
            inserted=0,
            skipped_existing=0,
        ),
    )
    monkeypatch.setattr(cli, "get_price_bar_bounds", lambda *args, **kwargs: (None, None))
    monkeypatch.setattr(cli, "refresh_price_bars", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        cli,
        "get_price_bars",
        lambda db_path, *, symbol, start_date, end_date: [bar] if symbol == "MAT" else [],
    )

    class _FakePriceClient:
        def fetch_history(self, symbol):  # type: ignore[no-untyped-def]
            if symbol == "Z AND ZG":
                raise ValueError("URL can't contain control characters")
            return [bar]

    monkeypatch.setattr(cli, "StooqPriceClient", lambda **kwargs: _FakePriceClient())
    monkeypatch.setattr(
        cli,
        "evaluate_parameter_grid",
        lambda *args, **kwargs: [GridSearchResult(params=params, metrics=metrics)],
    )
    monkeypatch.setattr(cli, "run_backtest", lambda *args, **kwargs: (metrics, []))
    monkeypatch.setattr(
        cli,
        "run_walk_forward",
        lambda *args, **kwargs: WalkForwardResult(
            folds=[],
            aggregate_test_metrics=metrics,
            recommended_params=params,
        ),
    )

    result = runner.invoke(
        cli.app,
        [
            "ops",
            "backtest",
            "--start-date",
            "2026-02-01",
            "--end-date",
            "2026-02-20",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["signals_total"] == 2
    assert any(
        error.startswith("Z AND ZG: unexpected price refresh error:")
        for error in payload["price_errors"]
    )


def test_cli_ops_backtest_skips_fetch_when_cache_is_fresh(monkeypatch) -> None:
    runner = CliRunner()
    signal = SignalEvent(
        packet_id="0001708842-26-000005|0000063276|4",
        symbol="MAT",
        filed_at=datetime(2026, 2, 12, 20, 39, 47, tzinfo=UTC),
        score=95.0,
        open_market_buy_shares=65000.0,
        open_market_net_shares=65000.0,
        has_10b5_1_plan=False,
        has_equity_comp_event=False,
        has_tax_withholding_language=False,
        role_tier="chief_exec",
    )
    bar = DailyBar(
        symbol="MAT",
        trade_date=date(2026, 2, 13),
        open=16.0,
        high=16.5,
        low=15.8,
        close=16.2,
        volume=1000000.0,
    )
    metrics = BacktestMetrics(
        trade_count=1,
        skipped_count=0,
        mean_return=0.01,
        median_return=0.01,
        win_rate=1.0,
        profit_factor=float("inf"),
        max_drawdown=0.0,
        sharpe_like=None,
        mean_alpha=0.005,
        median_alpha=0.005,
        objective_score=0.005,
    )
    params = BacktestParams(min_score=90.0, hold_days=5, stop_loss_pct=0.05, take_profit_rr=2.0)

    monkeypatch.setattr(cli, "load_scored_signals", lambda *args, **kwargs: [signal])
    monkeypatch.setattr(
        cli,
        "get_filing_date_bounds",
        lambda *args, **kwargs: (date(2000, 1, 1), date(2099, 1, 1)),
    )
    monkeypatch.setattr(
        cli,
        "backfill_form4_filings",
        lambda *args, **kwargs: BackfillResult(
            requested_quarters=0,
            fetched_quarters=0,
            matched_filings=0,
            inserted=0,
            skipped_existing=0,
        ),
    )
    monkeypatch.setattr(
        cli,
        "get_price_bar_bounds",
        lambda *args, **kwargs: (date(2020, 1, 1), date(2099, 1, 1)),
    )
    monkeypatch.setattr(cli, "refresh_price_bars", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "get_price_bars", lambda *args, **kwargs: [bar])

    fetched_symbols: list[str] = []

    class _FakePriceClient:
        def fetch_history(self, symbol):  # type: ignore[no-untyped-def]
            fetched_symbols.append(symbol)
            return [bar]

    monkeypatch.setattr(cli, "StooqPriceClient", lambda **kwargs: _FakePriceClient())
    monkeypatch.setattr(
        cli,
        "evaluate_parameter_grid",
        lambda *args, **kwargs: [GridSearchResult(params=params, metrics=metrics)],
    )
    monkeypatch.setattr(cli, "run_backtest", lambda *args, **kwargs: (metrics, []))
    monkeypatch.setattr(
        cli,
        "run_walk_forward",
        lambda *args, **kwargs: WalkForwardResult(
            folds=[],
            aggregate_test_metrics=metrics,
            recommended_params=params,
        ),
    )

    result = runner.invoke(
        cli.app,
        [
            "ops",
            "backtest",
            "--start-date",
            "2026-02-01",
            "--end-date",
            "2026-02-20",
        ],
    )
    assert result.exit_code == 0
    assert fetched_symbols == []


def test_cli_ops_backtest_fetches_only_trade_eligible_symbols(monkeypatch) -> None:
    runner = CliRunner()
    mat_signal = SignalEvent(
        packet_id="0001708842-26-000005|0000063276|4",
        symbol="MAT",
        filed_at=datetime(2026, 2, 12, 20, 39, 47, tzinfo=UTC),
        score=95.0,
        open_market_buy_shares=65000.0,
        open_market_net_shares=65000.0,
        has_10b5_1_plan=False,
        has_equity_comp_event=False,
        has_tax_withholding_language=False,
        role_tier="chief_exec",
    )
    filtered_signal = SignalEvent(
        packet_id="0001708842-26-000006|0000063276|4",
        symbol="AAPL",
        filed_at=datetime(2026, 2, 12, 20, 39, 47, tzinfo=UTC),
        score=99.0,
        open_market_buy_shares=0.0,
        open_market_net_shares=0.0,
        has_10b5_1_plan=False,
        has_equity_comp_event=False,
        has_tax_withholding_language=False,
        role_tier="chief_exec",
    )
    bar = DailyBar(
        symbol="MAT",
        trade_date=date(2026, 2, 13),
        open=16.0,
        high=16.5,
        low=15.8,
        close=16.2,
        volume=1000000.0,
    )
    metrics = BacktestMetrics(
        trade_count=1,
        skipped_count=1,
        mean_return=0.01,
        median_return=0.01,
        win_rate=1.0,
        profit_factor=float("inf"),
        max_drawdown=0.0,
        sharpe_like=None,
        mean_alpha=0.005,
        median_alpha=0.005,
        objective_score=0.005,
    )
    params = BacktestParams(min_score=90.0, hold_days=5, stop_loss_pct=0.05, take_profit_rr=2.0)
    fetched_symbols: list[str] = []

    monkeypatch.setattr(
        cli,
        "load_scored_signals",
        lambda *args, **kwargs: [mat_signal, filtered_signal],
    )
    monkeypatch.setattr(
        cli,
        "get_filing_date_bounds",
        lambda *args, **kwargs: (date(2000, 1, 1), date(2099, 1, 1)),
    )
    monkeypatch.setattr(
        cli,
        "backfill_form4_filings",
        lambda *args, **kwargs: BackfillResult(
            requested_quarters=0,
            fetched_quarters=0,
            matched_filings=0,
            inserted=0,
            skipped_existing=0,
        ),
    )
    monkeypatch.setattr(cli, "get_price_bar_bounds", lambda *args, **kwargs: (None, None))
    monkeypatch.setattr(cli, "refresh_price_bars", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "get_price_bars", lambda *args, **kwargs: [bar])

    class _FakePriceClient:
        def fetch_history(self, symbol):  # type: ignore[no-untyped-def]
            fetched_symbols.append(symbol)
            return [bar]

    monkeypatch.setattr(cli, "StooqPriceClient", lambda **kwargs: _FakePriceClient())
    monkeypatch.setattr(
        cli,
        "evaluate_parameter_grid",
        lambda *args, **kwargs: [GridSearchResult(params=params, metrics=metrics)],
    )
    monkeypatch.setattr(cli, "run_backtest", lambda *args, **kwargs: (metrics, []))
    monkeypatch.setattr(
        cli,
        "run_walk_forward",
        lambda *args, **kwargs: WalkForwardResult(
            folds=[],
            aggregate_test_metrics=metrics,
            recommended_params=params,
        ),
    )

    result = runner.invoke(
        cli.app,
        [
            "ops",
            "backtest",
            "--start-date",
            "2026-02-01",
            "--end-date",
            "2026-02-20",
        ],
    )
    assert result.exit_code == 0
    assert "AAPL" not in fetched_symbols
    assert "MAT" in fetched_symbols
    assert "SPY" in fetched_symbols


def test_cli_ops_backtest_refreshes_when_cache_misses_exit_window(monkeypatch) -> None:
    runner = CliRunner()
    signal = SignalEvent(
        packet_id="0001708842-26-000005|0000063276|4",
        symbol="MAT",
        filed_at=datetime(2026, 2, 12, 20, 39, 47, tzinfo=UTC),
        score=95.0,
        open_market_buy_shares=65000.0,
        open_market_net_shares=65000.0,
        has_10b5_1_plan=False,
        has_equity_comp_event=False,
        has_tax_withholding_language=False,
        role_tier="chief_exec",
    )
    bar = DailyBar(
        symbol="MAT",
        trade_date=date(2026, 2, 13),
        open=16.0,
        high=16.5,
        low=15.8,
        close=16.2,
        volume=1000000.0,
    )
    metrics = BacktestMetrics(
        trade_count=1,
        skipped_count=0,
        mean_return=0.01,
        median_return=0.01,
        win_rate=1.0,
        profit_factor=float("inf"),
        max_drawdown=0.0,
        sharpe_like=None,
        mean_alpha=0.005,
        median_alpha=0.005,
        objective_score=0.005,
    )
    params = BacktestParams(min_score=90.0, hold_days=5, stop_loss_pct=0.05, take_profit_rr=2.0)
    fetched_symbols: list[str] = []

    monkeypatch.setattr(cli, "load_scored_signals", lambda *args, **kwargs: [signal])
    monkeypatch.setattr(
        cli,
        "get_filing_date_bounds",
        lambda *args, **kwargs: (date(2000, 1, 1), date(2099, 1, 1)),
    )
    monkeypatch.setattr(
        cli,
        "backfill_form4_filings",
        lambda *args, **kwargs: BackfillResult(
            requested_quarters=0,
            fetched_quarters=0,
            matched_filings=0,
            inserted=0,
            skipped_existing=0,
        ),
    )
    monkeypatch.setattr(
        cli,
        "get_price_bar_bounds",
        lambda *args, **kwargs: (date(2020, 1, 1), date(2026, 2, 15)),
    )
    monkeypatch.setattr(cli, "refresh_price_bars", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "get_price_bars", lambda *args, **kwargs: [bar])

    class _FakePriceClient:
        def fetch_history(self, symbol):  # type: ignore[no-untyped-def]
            fetched_symbols.append(symbol)
            return [bar]

    monkeypatch.setattr(cli, "StooqPriceClient", lambda **kwargs: _FakePriceClient())
    monkeypatch.setattr(
        cli,
        "evaluate_parameter_grid",
        lambda *args, **kwargs: [GridSearchResult(params=params, metrics=metrics)],
    )
    monkeypatch.setattr(cli, "run_backtest", lambda *args, **kwargs: (metrics, []))
    monkeypatch.setattr(
        cli,
        "run_walk_forward",
        lambda *args, **kwargs: WalkForwardResult(
            folds=[],
            aggregate_test_metrics=metrics,
            recommended_params=params,
        ),
    )

    result = runner.invoke(
        cli.app,
        [
            "ops",
            "backtest",
            "--start-date",
            "2026-02-01",
            "--end-date",
            "2026-02-20",
        ],
    )
    assert result.exit_code == 0
    assert "MAT" in fetched_symbols


def test_cli_ops_event_study_outputs_schema_and_promising_label(monkeypatch, tmp_path) -> None:
    runner = CliRunner()
    canonical_events = [
        CanonicalEvent(
            packet_id="0000000001-25-000001|0000000001|4",
            accession_number="0000000001-25-000001",
            cik="0000000001",
            form_type="4",
            symbol="MAT",
            filed_at=datetime(2025, 1, 10, 20, 0, tzinfo=UTC),
            score=92.0,
            rationale={},
            cluster_packet_count=1,
            cluster_max_score=92.0,
        ),
        CanonicalEvent(
            packet_id="0000000002-25-000001|0000000002|4",
            accession_number="0000000002-25-000001",
            cik="0000000002",
            form_type="4",
            symbol="MAT",
            filed_at=datetime(2025, 1, 12, 20, 0, tzinfo=UTC),
            score=95.0,
            rationale={},
            cluster_packet_count=2,
            cluster_max_score=95.0,
        ),
    ]
    raw_signals = [
        SignalEvent(
            packet_id="0000000001-25-000001|0000000001|4",
            symbol="MAT",
            filed_at=datetime(2025, 1, 10, 20, 0, tzinfo=UTC),
            score=92.0,
            open_market_buy_shares=1000.0,
            open_market_net_shares=1000.0,
            has_10b5_1_plan=False,
            has_equity_comp_event=False,
            has_tax_withholding_language=False,
            role_tier="officer",
        ),
        SignalEvent(
            packet_id="0000000002-25-000001|0000000002|4",
            symbol="MAT",
            filed_at=datetime(2025, 1, 12, 20, 0, tzinfo=UTC),
            score=95.0,
            open_market_buy_shares=2000.0,
            open_market_net_shares=2000.0,
            has_10b5_1_plan=False,
            has_equity_comp_event=False,
            has_tax_withholding_language=False,
            role_tier="chief_exec",
        ),
        SignalEvent(
            packet_id="0000000002-25-000001|0000000003|4",
            symbol="MAT",
            filed_at=datetime(2025, 1, 12, 20, 0, tzinfo=UTC),
            score=95.0,
            open_market_buy_shares=2000.0,
            open_market_net_shares=2000.0,
            has_10b5_1_plan=False,
            has_equity_comp_event=False,
            has_tax_withholding_language=False,
            role_tier="chief_exec",
        ),
    ]
    readiness = EventStudyReadinessReport(
        requested_start_date=date(2025, 1, 1),
        requested_end_date=date(2025, 12, 31),
        filing_min_date=date(2025, 1, 1),
        filing_max_date=date(2025, 12, 31),
        canonical_event_count=2,
        symbol_count=1,
        full_months_evaluated=["2025-01"],
        monthly_filing_counts={"2025-01": 2},
        monthly_canonical_counts={"2025-01": 2},
        missing_internal_months=[],
        insufficient_monthly_event_months=[],
        rationale_feature_coverage={
            "holding_change_ratio": 1.0,
            "open_market_gross_value": 1.0,
            "trade_pct_daily_turnover": 1.0,
        },
        conviction_feature_coverage_ready=True,
        symbol_price_coverage_rate=1.0,
        covered_symbol_count=1,
        uncovered_symbols=[],
        hard_failure_codes=[],
    )
    metric_h5 = ScoreBucketMetrics(
        horizon_days=5,
        bucket_index=3,
        bucket_count=3,
        bucket_score_min=90.0,
        bucket_score_max=None,
        total_events=10,
        executed_events=10,
        benchmark_available_events=10,
        execution_coverage_rate=1.0,
        benchmark_coverage_rate=1.0,
        mean_alpha=0.01,
        median_alpha=0.01,
        win_rate=0.7,
        mean_alpha_ci_low=0.001,
        mean_alpha_ci_high=0.02,
        alpha_p_value=0.02,
        alpha_q_value=0.05,
    )
    metric_h10 = ScoreBucketMetrics(
        horizon_days=10,
        bucket_index=3,
        bucket_count=3,
        bucket_score_min=90.0,
        bucket_score_max=None,
        total_events=10,
        executed_events=10,
        benchmark_available_events=10,
        execution_coverage_rate=1.0,
        benchmark_coverage_rate=1.0,
        mean_alpha=0.012,
        median_alpha=0.011,
        win_rate=0.72,
        mean_alpha_ci_low=0.001,
        mean_alpha_ci_high=0.023,
        alpha_p_value=0.03,
        alpha_q_value=0.08,
    )
    def _fold(fold_index: int) -> OosFoldResult:
        return OosFoldResult(
            fold_index=fold_index,
            train_start=date(2025, 1, 1),
            train_end=date(2025, 6, 30),
            test_start=date(2025, 7, 1),
            test_end=date(2025, 9, 28),
            train_event_count=30,
            test_event_count=10,
            score_bucket_edges=[60.0, 80.0],
            bucket_metrics=[metric_h5, metric_h10],
            skip_diagnostics={},
            skip_diagnostics_by_horizon={},
        )

    event_study_result = OosEventStudyResult(
        folds=[_fold(1), _fold(2), _fold(3)],
        skipped_folds=[],
        aggregate_bucket_metrics=[metric_h5, metric_h10],
        aggregate_skip_diagnostics={},
        aggregate_skip_diagnostics_by_horizon={},
        monotonicity=[
            MonotonicityResult(
                horizon_days=5,
                spearman_rho=0.8,
                p_value_proxy=0.04,
                non_negative=True,
                bucket_points_used=3,
            )
        ],
        negative_control=[
            NegativeControlSummary(
                horizon_days=5,
                actual_top_bucket_mean_alpha=0.01,
                null_mean_alpha=0.001,
                null_ci_low=-0.001,
                null_ci_high=0.005,
                p_value_proxy=0.03,
                iterations=100,
            ),
            NegativeControlSummary(
                horizon_days=10,
                actual_top_bucket_mean_alpha=0.012,
                null_mean_alpha=0.001,
                null_ci_low=-0.001,
                null_ci_high=0.004,
                p_value_proxy=0.02,
                iterations=100,
            ),
        ],
    )

    bar = DailyBar(
        symbol="MAT",
        trade_date=date(2025, 1, 15),
        open=20.0,
        high=20.1,
        low=19.9,
        close=20.0,
        volume=1_000_000.0,
    )
    monkeypatch.setattr(cli, "load_canonical_events", lambda *args, **kwargs: canonical_events)
    monkeypatch.setattr(cli, "load_scored_signals", lambda *args, **kwargs: raw_signals)
    monkeypatch.setattr(cli, "audit_event_study_readiness", lambda *args, **kwargs: readiness)
    monkeypatch.setattr(cli, "run_oos_event_study", lambda *args, **kwargs: event_study_result)
    monkeypatch.setattr(
        cli,
        "get_price_bar_bounds",
        lambda *args, **kwargs: (date(2024, 1, 1), date(2026, 1, 1)),
    )
    monkeypatch.setattr(cli, "get_price_bars", lambda *args, **kwargs: [bar])
    monkeypatch.setattr(cli, "refresh_price_bars", lambda *args, **kwargs: None)

    class _NeverFetch:
        def fetch_history(self, symbol):  # type: ignore[no-untyped-def]
            raise AssertionError(f"fetch_history should not run for {symbol}")

    monkeypatch.setattr(cli, "StooqPriceClient", lambda **kwargs: _NeverFetch())
    study_db = tmp_path / "insider_alerts_research_2026-08-17.db"
    study_db.write_bytes(b"frozen research snapshot")
    confirmatory_report = tmp_path / "signal-study.json"
    confirmatory_report.write_text(
        json.dumps(
            {
                "schema_version": "signal-study-v1",
                "family_size": 168,
                "cohort": "live",
                "requested_start_date": "2026-02-11",
                "requested_end_date": "2026-08-17",
                "database_path": str(study_db.resolve()),
                "database_sha256": cli._file_sha256(study_db),
                "surviving_hypotheses": ["E07|F00"],
            }
        ),
        encoding="utf-8",
    )
    result = runner.invoke(
        cli.app,
        [
            "ops",
            "event-study",
            "--database-path",
            str(study_db),
            "--start-date",
            "2025-01-01",
            "--end-date",
            "2025-12-31",
            "--horizons",
            "5,10",
            "--bucket-count",
            "3",
            "--min-test-events",
            "5",
            "--no-refresh-prices",
            "--confirmatory-report",
            str(confirmatory_report),
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["go_no_go"]["label"] == "promising_edge"
    assert payload["go_no_go"]["hard_gates"]["locked_confirmatory_result_pass"] is True
    assert "readiness" in payload
    assert "dedupe_diagnostics" in payload
    assert "aggregate_bucket_metrics" in payload
    assert "negative_control" in payload
    assert payload["conviction_bucket_analysis"]["available"] is True
    assert payload["conviction_bucket_analysis"]["aggregate_bucket_metrics"]
    assert payload["dedupe_diagnostics"]["collapsed_duplicate_count"] == 1


def test_cli_ops_event_study_returns_non_zero_when_not_decision_grade(monkeypatch) -> None:
    runner = CliRunner()
    readiness = EventStudyReadinessReport(
        requested_start_date=date(2025, 1, 1),
        requested_end_date=date(2025, 12, 31),
        filing_min_date=None,
        filing_max_date=None,
        canonical_event_count=0,
        symbol_count=0,
        full_months_evaluated=["2025-01"],
        monthly_filing_counts={},
        monthly_canonical_counts={},
        missing_internal_months=["2025-01"],
        insufficient_monthly_event_months=["2025-01"],
        rationale_feature_coverage={
            "holding_change_ratio": 0.0,
            "open_market_gross_value": 0.0,
            "trade_pct_daily_turnover": 0.0,
        },
        conviction_feature_coverage_ready=False,
        symbol_price_coverage_rate=0.0,
        covered_symbol_count=0,
        uncovered_symbols=[],
        hard_failure_codes=["insufficient_canonical_events"],
    )
    monkeypatch.setattr(cli, "load_canonical_events", lambda *args, **kwargs: [])
    monkeypatch.setattr(cli, "load_scored_signals", lambda *args, **kwargs: [])
    monkeypatch.setattr(cli, "audit_event_study_readiness", lambda *args, **kwargs: readiness)
    monkeypatch.setattr(
        cli,
        "run_oos_event_study",
        lambda *args, **kwargs: OosEventStudyResult(
            folds=[],
            skipped_folds=[],
            aggregate_bucket_metrics=[],
            aggregate_skip_diagnostics={},
            aggregate_skip_diagnostics_by_horizon={},
            monotonicity=[],
            negative_control=[],
        ),
    )
    monkeypatch.setattr(cli, "get_price_bar_bounds", lambda *args, **kwargs: (None, None))
    monkeypatch.setattr(cli, "get_price_bars", lambda *args, **kwargs: [])
    monkeypatch.setattr(cli, "refresh_price_bars", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "StooqPriceClient", lambda **kwargs: object())

    result = runner.invoke(
        cli.app,
        [
            "ops",
            "event-study",
            "--start-date",
            "2025-01-01",
            "--end-date",
            "2025-12-31",
            "--horizons",
            "5,10",
            "--bucket-count",
            "3",
            "--no-refresh-prices",
        ],
    )
    assert result.exit_code == 3
    payload = json.loads(result.stdout)
    assert payload["go_no_go"]["label"] == "non_decision_grade"
    assert payload["go_no_go"]["hard_gates"]["readiness_pass"] is False


def test_confirmatory_gate_rejects_wrong_family_and_missing_candidate(tmp_path) -> None:
    study_db = tmp_path / "insider_alerts_research_2026-08-17.db"
    study_db.write_bytes(b"frozen research snapshot")
    report_path = tmp_path / "signal-study.json"
    report_path.write_text(
        json.dumps(
            {
                "schema_version": "signal-study-v1",
                "family_size": 12,
                "surviving_hypotheses": ["E06|F00"],
            }
        ),
        encoding="utf-8",
    )

    gate = cli._load_confirmatory_gate(
        report_path,
        candidate_hypothesis="E07|F00",
        expected_database_path=study_db,
    )

    assert gate["pass"] is False
    assert gate["family_size_valid"] is False
    assert gate["candidate_survived"] is False


def _mkzr_packet(packet_id: str, owner: str) -> dict[str, object]:
    """A single co-filer of the real 2026-08-10 MKZR joint filing."""
    return {
        "packet_id": packet_id,
        "payload": {
            "issuer_symbol": "MKZR",
            "owner": owner,
            "rationale": {
                "net_buy_shares": 33400.0,
                "pre_trade_shares_estimate": 66600.0,
                "post_trade_shares": 100000.0,
                "gross_value": 53340.0,
            },
        },
    }


def test_economic_event_key_collapses_joint_filing_co_filers() -> None:
    # Three reporting persons, three accessions, ONE economic trade -> one key.
    keys = {
        cli._economic_event_key(_mkzr_packet("0001582328-26-000010|0001550913|4", "PATTERSON")),
        cli._economic_event_key(_mkzr_packet("0001582328-26-000011|0001103014|4", "FULLER")),
        cli._economic_event_key(_mkzr_packet("0001582328-26-000012|0001103016|4", "DIXON")),
    }
    assert len(keys) == 1
    assert next(iter(keys)) is not None


def test_economic_event_key_separates_distinct_trades() -> None:
    base = _mkzr_packet("a|1|4", "PATTERSON")
    other_size = _mkzr_packet("b|2|4", "PATTERSON")
    other_size["payload"]["rationale"]["net_buy_shares"] = 33500.0
    other_issuer = _mkzr_packet("c|3|4", "PATTERSON")
    other_issuer["payload"]["issuer_symbol"] = "ELAN"
    other_holding = _mkzr_packet("d|4|4", "PATTERSON")
    other_holding["payload"]["rationale"]["pre_trade_shares_estimate"] = 10.0

    keys = [
        cli._economic_event_key(base),
        cli._economic_event_key(other_size),
        cli._economic_event_key(other_issuer),
        cli._economic_event_key(other_holding),
    ]
    assert len(set(keys)) == 4, "distinct trades must not collapse"


def test_economic_event_key_fails_open_on_incomplete_fingerprint() -> None:
    # Without the full fingerprint we cannot prove two filings are the same trade, so the
    # key is None and the caller alerts rather than risking suppression of a real signal.
    missing_field = _mkzr_packet("a|1|4", "PATTERSON")
    del missing_field["payload"]["rationale"]["post_trade_shares"]
    assert cli._economic_event_key(missing_field) is None

    no_symbol = _mkzr_packet("b|2|4", "PATTERSON")
    no_symbol["payload"]["issuer_symbol"] = "  "
    assert cli._economic_event_key(no_symbol) is None

    assert cli._economic_event_key({"packet_id": "x|1|4"}) is None


def test_recent_alerted_event_keys_only_returns_recent_approvals(tmp_path) -> None:
    db = tmp_path / "packets.db"
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            CREATE TABLE review_packets (
                packet_id TEXT, accession_number TEXT, cik TEXT, form_type TEXT,
                payload_json TEXT, status TEXT, decision_json TEXT,
                created_at TEXT, updated_at TEXT, notification_sent_at TEXT
            )
            """
        )
        recent = (datetime.now(UTC) - timedelta(days=1)).isoformat()
        stale = (datetime.now(UTC) - timedelta(days=30)).isoformat()
        rows = [
            ("recent-approve", json.dumps(_mkzr_packet("x", "P")["payload"]),
             json.dumps({"decision": "approve"}), recent),
            ("recent-reject", json.dumps(_mkzr_packet("y", "P")["payload"]),
             json.dumps({"decision": "reject"}), recent),
            ("stale-approve", json.dumps(_mkzr_packet("z", "P")["payload"]),
             json.dumps({"decision": "approve"}), stale),
        ]
        for packet_id, payload, decision, updated in rows:
            conn.execute(
                "INSERT INTO review_packets ("
                "packet_id, payload_json, decision_json, updated_at, notification_sent_at"
                ") VALUES (?, ?, ?, ?, ?)",
                (packet_id, payload, decision, updated, updated),
            )
        conn.commit()

    keys = cli._recent_alerted_event_keys(str(db), lookback_days=7)
    assert keys == {cli._economic_event_key(_mkzr_packet("x", "P"))}, (
        "only recently delivered APPROVED packets seed the suppression set"
    )


def test_recent_alerted_event_keys_survives_missing_table(tmp_path) -> None:
    # A brand-new DB must degrade to "nothing suppressed", never crash the cycle.
    assert cli._recent_alerted_event_keys(str(tmp_path / "absent.db")) == set()
