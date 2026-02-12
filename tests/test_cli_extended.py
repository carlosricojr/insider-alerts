import json
from dataclasses import dataclass

from typer.testing import CliRunner

from insider_alerts import cli
from insider_alerts.sec.client import SecHttpError
from insider_alerts.sec.pipeline import EnrichResult, PollResult, QueueResult


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

    def fake(settings, *, limit: int):  # type: ignore[no-untyped-def]
        assert limit == 9
        return QueueResult(processed=9, enqueued=3)

    monkeypatch.setattr(cli, "enqueue_review_packets", fake)
    result = runner.invoke(cli.app, ["review", "enqueue", "--limit", "9"])
    assert result.exit_code == 0
    assert "enqueued=3" in result.stdout


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


def test_cli_ops_autopilot_once(monkeypatch) -> None:
    runner = CliRunner()

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
        ],
    )

    assert result.exit_code == 0
    assert decisions == ["approve", "reject", "reject"]
    assert notifications == ["approve"]
    assert "approved=1" in result.stdout
    assert "rejected=2" in result.stdout


def test_cli_ops_autopilot_quant_reason_flows_to_apply_and_notify(monkeypatch) -> None:
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
        "packet_id": "0000905148-26-000640|0001824653|4",
        "payload": {
            "issuer_symbol": "CEG",
            "owner": "Hanson Bryan Craig",
            "score": 100.0,
            "rationale": {
                "net_buy_shares": 4754.0,
                "gross_value": 13268128.95,
                "open_market_buy_shares": 4754.0,
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


@dataclass
class _Completed:
    returncode: int
    stdout: str
    stderr: str


def test_decide_packets_with_quant_batches_requests(monkeypatch) -> None:
    monkeypatch.setattr(cli, "_resolve_openclaw_cmd", lambda: "openclaw.cmd")
    calls: list[int] = []

    def fake_run(args, **kwargs):  # type: ignore[no-untyped-def]
        message = str(args[args.index("--message") + 1])
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
                "confidence": 0.9,
            }
            for packet in packets
        ]
        inner = json.dumps({"decisions": decisions})
        outer = json.dumps({"result": {"payloads": [{"text": inner}]}})
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


def test_cli_ops_autopilot_once_exits_on_sec_http_error(monkeypatch) -> None:
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
        ],
    )

    assert result.exit_code == 1
    assert "ops autopilot cycle failed" in result.stderr
    assert "dns resolution failed" in result.stderr


def test_cli_ops_autopilot_loop_recovers_from_sec_http_error(monkeypatch) -> None:
    runner = CliRunner()

    calls = {"poll": 0, "sleep": 0}

    def fake_poll(settings, *, max_items: int, dry_run: bool):  # type: ignore[no-untyped-def]
        calls["poll"] += 1
        if calls["poll"] == 1:
            raise SecHttpError("transient dns failure")
        return PollResult(fetched=0, inserted=0, skipped_existing=0)

    def fake_sleep(seconds: int) -> None:
        calls["sleep"] += 1
        if calls["sleep"] >= 2:
            raise RuntimeError("stop-loop")

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
        ],
    )

    assert result.exit_code == 1
    assert isinstance(result.exception, RuntimeError)
    assert "stop-loop" in str(result.exception)
    assert calls["poll"] == 2
    assert "ops autopilot cycle failed" in result.stderr
    assert "transient dns failure" in result.stderr
    assert "ops autopilot cycle completed" in result.stdout


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
