from __future__ import annotations

import typer

from insider_alerts.config import get_settings
from insider_alerts.notify.ntfy import NtfyNotificationError, NtfyNotifier

app = typer.Typer(help="Insider alerts command-line interface.")
notify_app = typer.Typer(help="Notification commands.")
app.add_typer(notify_app, name="notify")


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


# TODO(sprint-2): Add ingest/fetch commands for insider transactions.


if __name__ == "__main__":
    app()
