from __future__ import annotations

import json
import time
from pathlib import Path

import typer

from insider_alerts.config import get_settings
from insider_alerts.notify.ntfy import NtfyNotificationError, NtfyNotifier
from insider_alerts.review.queue import (
    DecisionValidationError,
    apply_decision,
    list_deadletters,
    replay_deadletter,
)
from insider_alerts.sec.pipeline import (
    enqueue_review_packets,
    enrich_filings_with_xml_url,
    run_sec_poll_once,
)

app = typer.Typer(help="Insider alerts command-line interface.")
notify_app = typer.Typer(help="Notification commands.")
sec_app = typer.Typer(help="SEC ingestion commands.")
review_app = typer.Typer(help="Review queue commands.")
ops_app = typer.Typer(help="Operations commands.")
app.add_typer(notify_app, name="notify")
app.add_typer(sec_app, name="sec")
app.add_typer(review_app, name="review")
app.add_typer(ops_app, name="ops")


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


@sec_app.command("enrich")
def sec_enrich(
    limit: int = typer.Option(40, "--limit", min=1, max=500, help="Max filings to enrich."),
) -> None:
    """Fetch filing index pages and store discovered Form 4 XML URLs."""
    settings = get_settings()
    result = enrich_filings_with_xml_url(settings, limit=limit)
    typer.echo(f"sec enrich completed (scanned={result.scanned}, updated={result.updated})")


@review_app.command("enqueue")
def review_enqueue(
    limit: int = typer.Option(50, "--limit", min=1, max=1000, help="Max filings to process."),
) -> None:
    """Build scored review packets from filings that have Form 4 XML URLs."""
    settings = get_settings()
    result = enqueue_review_packets(settings, limit=limit)
    typer.echo(
        "review enqueue completed "
        f"(processed={result.processed}, enqueued={result.enqueued})"
    )


@review_app.command("apply")
def review_apply(
    decision_file: Path = typer.Option(  # noqa: B008
        ..., "--decision-file", exists=True, readable=True
    ),
    notify: bool = typer.Option(False, "--notify", help="Send NTFY notification when applied."),
) -> None:
    """Apply review decision JSON payload to pending queue packet."""
    settings = get_settings()
    payload = json.loads(decision_file.read_text(encoding="utf-8"))

    try:
        updated = apply_decision(settings.database_path, payload)
    except DecisionValidationError as exc:
        typer.secho(f"decision validation failed: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc

    typer.echo(f"review apply completed (updated={updated})")
    if notify and updated == 1:
        notifier = NtfyNotifier(settings)
        notifier.send(
            title="Insider Review Applied",
            message=(
                f"packet={payload['packet_id']} decision={payload['decision']} "
                f"analyst={payload['analyst']}"
            ),
            tags=["insider-alerts", "review"],
            priority=3,
            markdown=False,
        )


@ops_app.command("deadletter-list")
def deadletter_list() -> None:
    """List deadletter records for failed packets."""
    settings = get_settings()
    rows = list_deadletters(settings.database_path)
    typer.echo(json.dumps(rows, indent=2, sort_keys=True))


@ops_app.command("deadletter-replay")
def deadletter_replay(packet_id: str = typer.Option(..., "--packet-id")) -> None:
    """Replay a deadletter packet by resetting its status to pending."""
    settings = get_settings()
    updated = replay_deadletter(settings.database_path, packet_id)
    typer.echo(f"deadletter replay completed (updated={updated})")


if __name__ == "__main__":
    app()
