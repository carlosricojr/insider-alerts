from typer.testing import CliRunner

from insider_alerts.cli import app


def test_cli_help() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "Insider alerts command-line interface." in result.stdout
    assert "notify" in result.stdout
