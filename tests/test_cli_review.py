import json

from typer.testing import CliRunner

from insider_alerts import cli
from insider_alerts.config import Settings
from insider_alerts.review.queue import DecisionValidationError


def test_cli_review_apply(monkeypatch, tmp_path) -> None:
    decision_path = tmp_path / "decision.json"
    decision_path.write_text(
        json.dumps(
            {
                "packet_id": "id-1",
                "decision": "approve",
                "analyst": "carlo",
                "reason": "ok",
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        cli,
        "get_settings",
        lambda: Settings(DATABASE_PATH=str(tmp_path / "db.sqlite3")),
    )

    def fake_apply(db_path: str, payload):  # type: ignore[no-untyped-def]
        assert db_path.endswith("db.sqlite3")
        assert payload["decision"] == "approve"
        return 1

    monkeypatch.setattr(cli, "apply_decision", fake_apply)

    runner = CliRunner()
    result = runner.invoke(cli.app, ["review", "apply", "--decision-file", str(decision_path)])
    assert result.exit_code == 0
    assert "updated=1" in result.stdout


def test_cli_review_apply_validation_error(monkeypatch, tmp_path) -> None:
    decision_path = tmp_path / "decision.json"
    decision_path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        cli,
        "get_settings",
        lambda: Settings(DATABASE_PATH=str(tmp_path / "db.sqlite3")),
    )

    def fake_apply(db_path: str, payload):  # type: ignore[no-untyped-def]
        raise DecisionValidationError("bad")

    monkeypatch.setattr(cli, "apply_decision", fake_apply)

    runner = CliRunner()
    result = runner.invoke(cli.app, ["review", "apply", "--decision-file", str(decision_path)])
    assert result.exit_code == 2
    assert "validation failed" in result.stderr


def test_cli_review_apply_not_pending_returns_nonzero(monkeypatch, tmp_path) -> None:
    decision_path = tmp_path / "decision.json"
    decision_path.write_text(
        json.dumps(
            {
                "packet_id": "0000320193-24-000123|0000320193|4",
                "decision": "approve",
                "analyst": "carlo",
                "reason": "ok",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        cli,
        "get_settings",
        lambda: Settings(DATABASE_PATH=str(tmp_path / "db.sqlite3")),
    )
    monkeypatch.setattr(cli, "apply_decision", lambda db_path, payload: 0)

    runner = CliRunner()
    result = runner.invoke(cli.app, ["review", "apply", "--decision-file", str(decision_path)])
    assert result.exit_code == 3
    assert "not found or not pending" in result.stderr


def test_cli_review_pending(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        cli,
        "get_settings",
        lambda: Settings(DATABASE_PATH=str(tmp_path / "db.sqlite3")),
    )
    monkeypatch.setattr(
        cli,
        "list_pending_review_packets",
        lambda db_path, limit: [
            {
                "packet_id": "0000320193-24-000123|0000320193|4",
                "status": "pending",
                "payload": {"score": 77.5},
            }
        ],
    )

    runner = CliRunner()
    result = runner.invoke(cli.app, ["review", "pending", "--limit", "5"])
    assert result.exit_code == 0
    rows = json.loads(result.stdout)
    assert rows[0]["packet_id"] == "0000320193-24-000123|0000320193|4"


def test_cli_review_decide(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        cli,
        "get_settings",
        lambda: Settings(DATABASE_PATH=str(tmp_path / "db.sqlite3")),
    )

    def fake_apply(db_path: str, payload):  # type: ignore[no-untyped-def]
        assert payload["decision"] == "approve"
        assert payload["analyst"] == "quant"
        return 1

    monkeypatch.setattr(cli, "apply_decision", fake_apply)

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "review",
            "decide",
            "--packet-id",
            "0000320193-24-000123|0000320193|4",
            "--decision",
            "approve",
            "--reason",
            "high confidence",
        ],
    )
    assert result.exit_code == 0
    assert "updated=1" in result.stdout
