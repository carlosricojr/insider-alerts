from __future__ import annotations

import time

import typer

from insider_alerts.config import get_settings
from insider_alerts.notify.ntfy import NtfyNotificationError, NtfyNotifier
from insider_alerts.sec.pipeline import run_sec_poll_once

app = typer.Typer(help="Insider alerts command-line interface.")
notify_app = typer.Typer(help="Notification commands.")
sec_app = typer.Typer(help="SEC ingestion commands.")
app.add_typer(notify_app, name="notify")
app.add_typer(sec_app, name="sec")


@notify_app.command("test")
def notify_test() -> None:
    """Send a test notification via NTFY."""
    settings = get_settings()
    notifier = NtfyNotifier(settings)

    try:
        notifier.send(
            title="Insider Alerts Test",
            message="Test notification from insider-alerts CLI.",
            tags=["test", "insider-alerts"],
            priority=3,
            markdown=True,
        )
    except NtfyNotificationError as exc:
        typer.secho(f"Notification failed: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from exc

    typer.secho("Notification sent.", fg=typer.colors.GREEN)


@sec_app.command("poll")
def sec_poll(
    once: bool = typer.Option(True, "--once", help="Run a single poll cycle."),
    interval: int = typer.Option(
        600,
        "--interval",
        min=1,
        help="Seconds between polls when looping.",
    ),
    max_items: int = typer.Option(40, "--max-items", min=1, max=200, help="Max parsed items."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Parse only, no DB writes."),
) -> None:
    """Poll SEC Form 4 RSS and persist new filing references."""
    settings = get_settings()

    def _run_once() -> None:
        result = run_sec_poll_once(settings, max_items=max_items, dry_run=dry_run)
        summary = (
            "sec poll completed "
            f"(fetched={result.fetched}, "
            f"inserted={result.inserted}, "
            f"skipped_existing={result.skipped_existing}, "
            f"dry_run={dry_run})"
        )
        typer.echo(summary)

    _run_once()
    if not once:
        while True:
            time.sleep(interval)
            _run_once()


if __name__ == "__main__":
    app()
