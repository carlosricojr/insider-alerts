from typer.testing import CliRunner

from insider_alerts import cli
from insider_alerts.sec.pipeline import PollResult


def test_cli_sec_poll_once(monkeypatch) -> None:
    runner = CliRunner()

    def fake_run(settings, *, max_items: int, dry_run: bool):  # type: ignore[no-untyped-def]
        assert max_items == 10
        assert dry_run is True
        return PollResult(fetched=3, inserted=0, skipped_existing=0)

    monkeypatch.setattr(cli, "run_sec_poll_once", fake_run)

    result = runner.invoke(cli.app, ["sec", "poll", "--once", "--max-items", "10", "--dry-run"])
    assert result.exit_code == 0
    assert "fetched=3" in result.stdout
