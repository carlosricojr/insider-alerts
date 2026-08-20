from typer.testing import CliRunner

from insider_alerts import cli
from insider_alerts.sec.pipeline import BackfillResult, PollResult


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


def test_cli_sec_backfill(monkeypatch) -> None:
    runner = CliRunner()

    def fake_backfill(settings, *, start_date, end_date):  # type: ignore[no-untyped-def]
        assert str(start_date) == "2025-01-01"
        assert str(end_date) == "2025-12-31"
        return BackfillResult(
            requested_quarters=4,
            fetched_quarters=4,
            matched_filings=1234,
            inserted=1200,
            skipped_existing=34,
        )

    monkeypatch.setattr(cli, "backfill_form4_filings", fake_backfill)
    result = runner.invoke(
        cli.app,
        [
            "sec",
            "backfill",
            "--start-date",
            "2025-01-01",
            "--end-date",
            "2025-12-31",
        ],
    )

    assert result.exit_code == 0
    assert "requested_quarters=4" in result.stdout
    assert "inserted=1200" in result.stdout
