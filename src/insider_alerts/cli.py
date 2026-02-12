from __future__ import annotations

import json
import re
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

import typer

from insider_alerts.config import Settings, get_settings
from insider_alerts.notify.ntfy import NtfyNotificationError, NtfyNotifier
from insider_alerts.review.queue import (
    DecisionValidationError,
    apply_decision,
    get_review_packet,
    list_deadletters,
    list_pending_review_packets,
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


@dataclass(slots=True)
class AutoDecisionRuleResult:
    decision: str
    reason: str
    source: str
    confidence: float | None


@dataclass(slots=True)
class AutoPilotCycleResult:
    fetched: int
    inserted: int
    skipped_existing: int
    enriched_scanned: int
    enriched_updated: int
    enqueue_processed: int
    enqueue_enqueued: int
    pending_seen: int
    decided: int
    approved: int
    rejected: int
    escalated: int
    deadlettered: int
    notified: int


_JSON_OBJECT_RE = re.compile(r"\{.*\}", re.DOTALL)


def _to_float(value: object) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _auto_decide_packet(
    packet: dict[str, object],
    *,
    approve_score_min: float,
    approve_net_buy_shares_min: float,
    reject_score_max: float,
) -> AutoDecisionRuleResult:
    packet_id = str(packet.get("packet_id", "unknown"))
    payload_obj = packet.get("payload")
    if not isinstance(payload_obj, dict):
        return AutoDecisionRuleResult(
            decision="escalate",
            reason=f"auto rule: packet={packet_id} missing payload",
            source="rules",
            confidence=None,
        )

    score = _to_float(payload_obj.get("score"))
    rationale_obj = payload_obj.get("rationale")
    net_buy_shares = None
    if isinstance(rationale_obj, dict):
        net_buy_shares = _to_float(rationale_obj.get("net_buy_shares"))

    if score is None or net_buy_shares is None:
        return AutoDecisionRuleResult(
            decision="escalate",
            reason=f"auto rule: packet={packet_id} missing score/net_buy_shares",
            source="rules",
            confidence=None,
        )

    if score >= approve_score_min and net_buy_shares > approve_net_buy_shares_min:
        return AutoDecisionRuleResult(
            decision="approve",
            reason=(
                "auto rule: "
                f"score={score:.2f} >= {approve_score_min:.2f} and "
                f"net_buy_shares={net_buy_shares:.2f} > {approve_net_buy_shares_min:.2f}"
            ),
            source="rules",
            confidence=None,
        )

    if score <= reject_score_max or net_buy_shares < 0:
        return AutoDecisionRuleResult(
            decision="reject",
            reason=(
                "auto rule: "
                f"score={score:.2f}, net_buy_shares={net_buy_shares:.2f} "
                f"(reject if score <= {reject_score_max:.2f} or net_buy_shares < 0)"
            ),
            source="rules",
            confidence=None,
        )

    return AutoDecisionRuleResult(
        decision="escalate",
        reason=(
            "auto rule: "
            f"score={score:.2f}, net_buy_shares={net_buy_shares:.2f} "
            "(between approve/reject thresholds)"
        ),
        source="rules",
        confidence=None,
    )


def _extract_json_object(text: str) -> dict[str, object] | None:
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    decoder = json.JSONDecoder()
    for idx, char in enumerate(text):
        if char != "{":
            continue
        try:
            candidate, _ = decoder.raw_decode(text[idx:])
        except json.JSONDecodeError:
            continue
        if isinstance(candidate, dict):
            return candidate

    match = _JSON_OBJECT_RE.search(text)
    if match is None:
        return None
    try:
        parsed = json.loads(match.group(0))
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _compact_packet_for_quant(packet: dict[str, object]) -> dict[str, object]:
    payload = packet.get("payload")
    payload_dict = payload if isinstance(payload, dict) else {}
    rationale = payload_dict.get("rationale")
    rationale_dict = rationale if isinstance(rationale, dict) else {}
    return {
        "score": payload_dict.get("score"),
        "net_buy_shares": rationale_dict.get("net_buy_shares"),
        "gross_value": rationale_dict.get("gross_value"),
    }


def _packet_decision_key(packet: dict[str, object]) -> str | None:
    packet_id_obj = packet.get("packet_id")
    if isinstance(packet_id_obj, str):
        parts = [part.strip() for part in packet_id_obj.split("|")]
        if len(parts) == 3 and parts[0] and parts[2]:
            return f"{parts[0]}|{parts[2]}"

    accession_obj = packet.get("accession_number")
    form_type_obj = packet.get("form_type")
    if isinstance(accession_obj, str) and isinstance(form_type_obj, str):
        accession = accession_obj.strip()
        form_type = form_type_obj.strip()
        if accession and form_type:
            return f"{accession}|{form_type}"
    return None


def _resolve_openclaw_cmd() -> str | None:
    cmd = shutil.which("openclaw.cmd")
    if cmd:
        return cmd
    cmd = shutil.which("openclaw")
    if cmd:
        return cmd
    appdata = Path.home() / "AppData" / "Roaming" / "npm" / "openclaw.cmd"
    if appdata.exists():
        return str(appdata)
    return None


def _decide_packets_with_quant(
    packets: list[dict[str, object]],
    *,
    quant_agent_id: str,
    quant_timeout_seconds: int,
    quant_thinking: str,
    quant_batch_size: int,
) -> tuple[dict[str, AutoDecisionRuleResult], str | None]:
    openclaw_cmd = _resolve_openclaw_cmd()
    if openclaw_cmd is None:
        return {}, "openclaw CLI not found"
    mapped: dict[str, AutoDecisionRuleResult] = {}
    errors: list[str] = []
    batch_size = max(1, quant_batch_size)

    for start in range(0, len(packets), batch_size):
        chunk = packets[start : start + batch_size]
        alias_to_packet_id: dict[str, str] = {}
        compact_packets: list[dict[str, object]] = []
        for offset, packet in enumerate(chunk):
            packet_id_obj = packet.get("packet_id")
            if not isinstance(packet_id_obj, str):
                continue
            alias = f"P{start + offset:05d}"
            alias_to_packet_id[alias] = packet_id_obj
            compact = _compact_packet_for_quant(packet)
            compact["packet_id"] = alias
            compact_packets.append(compact)
        if not compact_packets:
            continue

        request = {"packets": compact_packets}
        prompt = (
            "Decide insider packets. Return ONLY JSON: "
            "{\"decisions\":[{\"packet_id\":\"...\",\"decision\":\"approve, reject, or escalate\","
            "\"why\":\"max 240 chars\",\"confidence\":0.0}]}. "
            f"Input: {json.dumps(request, separators=(',', ':'))}"
        )

        args = [
            openclaw_cmd,
            "agent",
            "--agent",
            quant_agent_id,
            "--message",
            prompt,
            "--json",
            "--timeout",
            str(quant_timeout_seconds),
            "--thinking",
            quant_thinking,
        ]

        try:
            completed = subprocess.run(
                args,
                capture_output=True,
                text=True,
                timeout=quant_timeout_seconds + 10,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            errors.append(f"chunk[{start}:{start + len(chunk)}] failed: {exc}")
            continue

        if completed.returncode != 0:
            stderr = completed.stderr.strip() if completed.stderr else "unknown error"
            errors.append(f"chunk[{start}:{start + len(chunk)}] non-zero: {stderr}")
            continue

        outer = _extract_json_object(completed.stdout)
        if outer is None:
            errors.append(f"chunk[{start}:{start + len(chunk)}] invalid JSON envelope")
            continue

        result_obj = outer.get("result")
        if not isinstance(result_obj, dict):
            errors.append(f"chunk[{start}:{start + len(chunk)}] missing result")
            continue
        payloads_obj = result_obj.get("payloads")
        if not isinstance(payloads_obj, list) or not payloads_obj:
            errors.append(f"chunk[{start}:{start + len(chunk)}] missing payloads")
            continue
        first_payload = payloads_obj[0]
        if not isinstance(first_payload, dict):
            errors.append(f"chunk[{start}:{start + len(chunk)}] payload malformed")
            continue
        text_obj = first_payload.get("text")
        if not isinstance(text_obj, str):
            errors.append(f"chunk[{start}:{start + len(chunk)}] response text missing")
            continue

        inner = _extract_json_object(text_obj)
        if inner is None:
            errors.append(f"chunk[{start}:{start + len(chunk)}] invalid decision JSON")
            continue

        decisions_obj = inner.get("decisions")
        if not isinstance(decisions_obj, list):
            errors.append(f"chunk[{start}:{start + len(chunk)}] decisions missing")
            continue

        for entry in decisions_obj:
            if not isinstance(entry, dict):
                continue
            packet_id_obj = entry.get("packet_id")
            decision_obj = entry.get("decision")
            why_obj = entry.get("why")
            if not isinstance(packet_id_obj, str) or not packet_id_obj.strip():
                continue
            original_packet_id = alias_to_packet_id.get(packet_id_obj)
            if original_packet_id is None:
                continue
            if not isinstance(decision_obj, str) or decision_obj not in {
                "approve",
                "reject",
                "escalate",
            }:
                continue
            if not isinstance(why_obj, str) or not why_obj.strip():
                continue

            confidence = _to_float(entry.get("confidence"))
            if confidence is not None:
                confidence = max(0.0, min(1.0, confidence))

            mapped[original_packet_id] = AutoDecisionRuleResult(
                decision=decision_obj,
                reason=why_obj.strip()[:240],
                source=f"quant:{quant_agent_id}",
                confidence=confidence,
            )

    if not errors:
        return mapped, None
    if len(errors) == 1:
        return mapped, errors[0]
    return mapped, f"{errors[0]}; +{len(errors) - 1} more chunk errors"


def _apply_approve_guardrails(
    rule: AutoDecisionRuleResult,
    packet: dict[str, object],
    *,
    approve_score_min: float,
    approve_net_buy_shares_min: float,
    quant_min_confidence: float,
) -> AutoDecisionRuleResult:
    if rule.decision != "approve":
        return rule

    payload = packet.get("payload")
    payload_dict = payload if isinstance(payload, dict) else {}
    rationale = payload_dict.get("rationale")
    rationale_dict = rationale if isinstance(rationale, dict) else {}

    score = _to_float(payload_dict.get("score"))
    net_buy_shares = _to_float(rationale_dict.get("net_buy_shares"))
    packet_id = str(packet.get("packet_id", "unknown"))

    if score is None or net_buy_shares is None:
        return AutoDecisionRuleResult(
            decision="escalate",
            reason=f"safety block: packet={packet_id} missing score/net_buy_shares for approve",
            source="safety",
            confidence=None,
        )

    if score < approve_score_min or net_buy_shares <= approve_net_buy_shares_min:
        return AutoDecisionRuleResult(
            decision="escalate",
            reason=(
                "safety block: "
                f"score={score:.2f}, net_buy_shares={net_buy_shares:.2f} "
                f"(requires score >= {approve_score_min:.2f} and "
                f"net_buy_shares > {approve_net_buy_shares_min:.2f})"
            ),
            source="safety",
            confidence=None,
        )

    if (
        rule.source.startswith("quant:")
        and rule.confidence is not None
        and rule.confidence < quant_min_confidence
    ):
        return AutoDecisionRuleResult(
            decision="escalate",
            reason=(
                "safety block: "
                f"quant confidence={rule.confidence:.2f} below {quant_min_confidence:.2f}"
            ),
            source="safety",
            confidence=None,
        )

    return rule


def _build_trade_signal_notification(
    packet: dict[str, object],
    decision_payload: dict[str, str],
) -> tuple[str, str, list[str], int]:
    payload = packet.get("payload")
    payload_dict = payload if isinstance(payload, dict) else {}
    rationale = payload_dict.get("rationale")
    rationale_dict = rationale if isinstance(rationale, dict) else {}

    ticker = str(payload_dict.get("issuer_symbol") or "UNKNOWN")
    owner = str(payload_dict.get("owner") or "UNKNOWN")
    score = _to_float(payload_dict.get("score"))
    net_buy = _to_float(rationale_dict.get("net_buy_shares"))
    gross = _to_float(rationale_dict.get("gross_value"))
    packet_id = str(packet.get("packet_id") or decision_payload["packet_id"])
    why = decision_payload.get("reason", "").strip()
    source = decision_payload.get("decision_source", decision_payload.get("analyst", "quant"))

    title = f"TRADE SIGNAL: {ticker}"
    message = "\n".join(
        [
            f"ticker={ticker}",
            f"packet={packet_id}",
            f"owner={owner}",
            f"score={score:.2f}" if score is not None else "score=NA",
            f"net_buy_shares={net_buy:.2f}" if net_buy is not None else "net_buy_shares=NA",
            f"gross_value={gross:.2f}" if gross is not None else "gross_value=NA",
            f"source={source}",
            f"why={why or 'N/A'}",
        ]
    )
    tags = ["trade-signal", "insider-alerts", ticker.lower().replace(" ", "-")]
    return title, message, tags, 4


def _send_review_notification(
    settings: Settings,
    payload: dict[str, str],
    *,
    packet: dict[str, object] | None = None,
    dry_message: str | None = None,
) -> None:
    notifier = NtfyNotifier(settings)
    decision = payload.get("decision", "")
    if decision == "approve" and packet is not None:
        title, message, tags, priority = _build_trade_signal_notification(packet, payload)
        notifier.send(
            title=title,
            message=message,
            tags=tags,
            priority=priority,
            markdown=True,
        )
        return

    message = f"packet={payload['packet_id']} decision={decision} analyst={payload['analyst']}"
    if dry_message:
        message = f"{message} note={dry_message}"
    notifier.send(
        title="Insider Review Applied",
        message=message,
        tags=["insider-alerts", "review"],
        priority=3,
        markdown=False,
    )


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
    once: bool = typer.Option(
        True,
        "--once/--loop",
        help="Run a single poll cycle or keep polling.",
    ),
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


@review_app.command("pending")
def review_pending(
    limit: int = typer.Option(50, "--limit", min=1, max=1000, help="Max packets to list."),
) -> None:
    """List pending review packets in JSON for analyst/agent decisioning."""
    settings = get_settings()
    rows = list_pending_review_packets(settings.database_path, limit=limit)
    typer.echo(json.dumps(rows, indent=2, sort_keys=True))


@review_app.command("decide")
def review_decide(
    packet_id: str = typer.Option(..., "--packet-id"),
    decision: str = typer.Option(..., "--decision", help="approve|reject|escalate|deadletter"),
    reason: str = typer.Option(..., "--reason"),
    analyst: str = typer.Option("quant", "--analyst"),
    notify: bool = typer.Option(False, "--notify", help="Send NTFY notification when applied."),
) -> None:
    """Apply a single decision directly (automation-friendly, no decision-file needed)."""
    settings = get_settings()
    payload: dict[str, object] = {
        "packet_id": packet_id,
        "decision": decision,
        "analyst": analyst,
        "reason": reason,
        "decision_source": analyst,
    }
    packet = get_review_packet(settings.database_path, packet_id)

    try:
        updated = apply_decision(settings.database_path, payload)
    except DecisionValidationError as exc:
        typer.secho(f"decision validation failed: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc

    if updated != 1:
        typer.secho(
            "review decide failed: packet not found or not pending",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=3)

    typer.echo(f"review decide completed (updated={updated})")
    if notify:
        notify_payload = {k: str(v) for k, v in payload.items()}
        _send_review_notification(settings, notify_payload, packet=packet)


@review_app.command("apply")
def review_apply(
    decision_file: Path = typer.Option(  # noqa: B008
        ..., "--decision-file", exists=True, readable=True
    ),
    notify: bool = typer.Option(False, "--notify", help="Send NTFY notification when applied."),
) -> None:
    """Apply review decision JSON payload to pending queue packet."""
    settings = get_settings()
    try:
        payload = json.loads(decision_file.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        typer.secho(
            f"decision validation failed: invalid JSON ({exc})",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2) from exc

    try:
        packet_id_obj = payload.get("packet_id")
        packet = (
            get_review_packet(settings.database_path, packet_id_obj)
            if isinstance(packet_id_obj, str)
            else None
        )
        updated = apply_decision(settings.database_path, payload)
    except DecisionValidationError as exc:
        typer.secho(f"decision validation failed: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc

    if updated != 1:
        typer.secho(
            "review apply failed: packet not found or not pending",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=3)

    typer.echo(f"review apply completed (updated={updated})")
    if notify:
        notify_payload = {k: str(v) for k, v in payload.items() if isinstance(k, str)}
        _send_review_notification(settings, notify_payload, packet=packet)


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


@ops_app.command("autopilot")
def ops_autopilot(
    once: bool = typer.Option(
        False,
        "--once/--loop",
        help="Run one cycle or keep running in background loop.",
    ),
    interval: int = typer.Option(
        300,
        "--interval",
        min=10,
        help="Seconds between cycles when looping.",
    ),
    poll_max_items: int = typer.Option(40, "--poll-max-items", min=1, max=200),
    enrich_limit: int = typer.Option(100, "--enrich-limit", min=1, max=1000),
    enqueue_limit: int = typer.Option(100, "--enqueue-limit", min=1, max=2000),
    decision_limit: int = typer.Option(200, "--decision-limit", min=1, max=5000),
    decision_engine: str = typer.Option("quant", "--decision-engine", help="quant|rules"),
    approve_score_min: float = typer.Option(90.0, "--approve-score-min"),
    approve_net_buy_shares_min: float = typer.Option(0.0, "--approve-net-buy-shares-min"),
    reject_score_max: float = typer.Option(35.0, "--reject-score-max"),
    quant_agent_id: str = typer.Option("quant-insider", "--quant-agent-id"),
    quant_thinking: str = typer.Option("low", "--quant-thinking"),
    quant_timeout_seconds: int = typer.Option(120, "--quant-timeout-seconds", min=10, max=900),
    quant_batch_size: int = typer.Option(8, "--quant-batch-size", min=1, max=200),
    quant_min_confidence: float = typer.Option(0.7, "--quant-min-confidence"),
    quant_require_isolated_agent: bool = typer.Option(
        True,
        "--quant-require-isolated-agent/--no-quant-require-isolated-agent",
    ),
    quant_fallback_to_rules: bool = typer.Option(
        False,
        "--quant-fallback-to-rules/--no-quant-fallback-to-rules",
    ),
    analyst: str = typer.Option("quant", "--analyst"),
    notify: bool = typer.Option(True, "--notify/--no-notify"),
    notify_approve_only: bool = typer.Option(
        True,
        "--notify-approve-only/--notify-all-decisions",
    ),
) -> None:
    """
    Run SEC ingestion + auto-decision loop and notify for approved signals by default.
    """
    settings = get_settings()
    decision_engine = decision_engine.strip().lower()
    if decision_engine not in {"quant", "rules"}:
        typer.secho(
            "invalid --decision-engine (expected quant|rules)",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2)
    quant_thinking = quant_thinking.strip().lower()
    if quant_thinking not in {"off", "minimal", "low", "medium", "high"}:
        typer.secho(
            "invalid --quant-thinking (expected off|minimal|low|medium|high)",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2)
    quant_agent_id = quant_agent_id.strip()
    if (
        decision_engine == "quant"
        and quant_require_isolated_agent
        and quant_agent_id.lower() == "main"
    ):
        typer.secho(
            "unsafe quant agent: 'main' is blocked in isolated mode; "
            "use a dedicated agent id (for example, quant-insider) or pass "
            "--no-quant-require-isolated-agent",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2)

    def _run_cycle() -> AutoPilotCycleResult:
        poll_result = run_sec_poll_once(settings, max_items=poll_max_items, dry_run=False)
        enrich_result = enrich_filings_with_xml_url(settings, limit=enrich_limit)
        enqueue_result = enqueue_review_packets(settings, limit=enqueue_limit)
        pending = list_pending_review_packets(settings.database_path, limit=decision_limit)
        quant_decisions: dict[str, AutoDecisionRuleResult] = {}
        quant_error: str | None = None
        if decision_engine == "quant" and pending:
            quant_decisions, quant_error = _decide_packets_with_quant(
                pending,
                quant_agent_id=quant_agent_id,
                quant_timeout_seconds=quant_timeout_seconds,
                quant_thinking=quant_thinking,
                quant_batch_size=quant_batch_size,
            )

        decided = 0
        approved = 0
        rejected = 0
        escalated = 0
        deadlettered = 0
        notified = 0
        seen_decision_keys: set[str] = set()

        for packet in pending:
            packet_id_obj = packet.get("packet_id")
            if not isinstance(packet_id_obj, str):
                continue

            decision_key = _packet_decision_key(packet)
            if decision_key is not None and decision_key in seen_decision_keys:
                rule = AutoDecisionRuleResult(
                    decision="deadletter",
                    reason=f"safety dedupe: duplicate pending packet key={decision_key}",
                    source="safety",
                    confidence=None,
                )
            else:
                if decision_key is not None:
                    seen_decision_keys.add(decision_key)
                if decision_engine == "quant":
                    quant_rule = quant_decisions.get(packet_id_obj)
                    if quant_rule is not None:
                        rule = quant_rule
                    elif quant_error is not None and quant_fallback_to_rules:
                        rule = _auto_decide_packet(
                            packet,
                            approve_score_min=approve_score_min,
                            approve_net_buy_shares_min=approve_net_buy_shares_min,
                            reject_score_max=reject_score_max,
                        )
                    else:
                        reason = (
                            f"quant unavailable for packet={packet_id_obj}: {quant_error}"
                            if quant_error
                            else f"quant missing decision for packet={packet_id_obj}"
                        )
                        rule = AutoDecisionRuleResult(
                            decision="escalate",
                            reason=reason,
                            source="quant-fallback",
                            confidence=None,
                        )
                else:
                    rule = _auto_decide_packet(
                        packet,
                        approve_score_min=approve_score_min,
                        approve_net_buy_shares_min=approve_net_buy_shares_min,
                        reject_score_max=reject_score_max,
                    )
                rule = _apply_approve_guardrails(
                    rule,
                    packet,
                    approve_score_min=approve_score_min,
                    approve_net_buy_shares_min=approve_net_buy_shares_min,
                    quant_min_confidence=quant_min_confidence,
                )

            payload: dict[str, object] = {
                "packet_id": packet_id_obj,
                "decision": rule.decision,
                "analyst": analyst,
                "reason": rule.reason,
                "decision_source": rule.source,
            }
            if rule.confidence is not None:
                payload["confidence"] = round(rule.confidence, 4)

            try:
                updated = apply_decision(settings.database_path, payload)
            except DecisionValidationError as exc:
                typer.secho(
                    f"autopilot decision failed for packet={packet_id_obj}: {exc}",
                    fg=typer.colors.RED,
                    err=True,
                )
                continue

            if updated != 1:
                continue

            decided += 1
            if rule.decision == "approve":
                approved += 1
            elif rule.decision == "reject":
                rejected += 1
            elif rule.decision == "deadletter":
                deadlettered += 1
            else:
                escalated += 1

            should_notify = notify and (not notify_approve_only or rule.decision == "approve")
            if should_notify:
                try:
                    notify_payload = {k: str(v) for k, v in payload.items()}
                    _send_review_notification(
                        settings,
                        notify_payload,
                        packet=packet,
                        dry_message=rule.reason,
                    )
                    notified += 1
                except NtfyNotificationError as exc:
                    typer.secho(
                        f"autopilot notification failed for packet={packet_id_obj}: {exc}",
                        fg=typer.colors.RED,
                        err=True,
                    )

        cycle = AutoPilotCycleResult(
            fetched=poll_result.fetched,
            inserted=poll_result.inserted,
            skipped_existing=poll_result.skipped_existing,
            enriched_scanned=enrich_result.scanned,
            enriched_updated=enrich_result.updated,
            enqueue_processed=enqueue_result.processed,
            enqueue_enqueued=enqueue_result.enqueued,
            pending_seen=len(pending),
            decided=decided,
            approved=approved,
            rejected=rejected,
            escalated=escalated,
            deadlettered=deadlettered,
            notified=notified,
        )
        typer.echo(
            "ops autopilot cycle completed "
            f"(fetched={cycle.fetched}, inserted={cycle.inserted}, "
            f"skipped_existing={cycle.skipped_existing}, "
            f"enrich_scanned={cycle.enriched_scanned}, enrich_updated={cycle.enriched_updated}, "
            f"enqueue_processed={cycle.enqueue_processed}, "
            f"enqueue_enqueued={cycle.enqueue_enqueued}, "
                f"pending_seen={cycle.pending_seen}, decided={cycle.decided}, "
                f"approved={cycle.approved}, rejected={cycle.rejected}, "
                f"escalated={cycle.escalated}, deadlettered={cycle.deadlettered}, "
                f"notified={cycle.notified})"
        )
        return cycle

    _run_cycle()
    if not once:
        while True:
            time.sleep(interval)
            _run_cycle()


if __name__ == "__main__":
    app()
