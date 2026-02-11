import json

from typer.testing import CliRunner

from insider_alerts import cli
from insider_alerts.sec.pipeline import EnrichResult, QueueResult


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
