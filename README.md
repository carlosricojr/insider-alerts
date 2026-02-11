# insider-alerts

Insider-alert workflow for SEC Form 4 ingestion, XML extraction/parsing, scoring, human review, and notification.

## Implemented scope (Sprints 0-7)

- SEC RSS poll + idempotent filing storage
- Filing index/document-page parsing to locate Form 4 XML URLs
- Robust Form 4 XML parser to canonical facts
- Deterministic signal scoring + review packet queue
- Decision apply validation/merge semantics + optional NTFY notification
- Deadletter listing/replay tooling
- OpenClaw skill docs (`skills/ntfy-notify`, `skills/insider-review`)
- Runbook, threat model, and deployment docs

## Quick start

```bash
uv sync --dev
uv run python -m insider_alerts.cli --help
```

## Core commands

```bash
uv run python -m insider_alerts.cli sec poll --once --max-items 40
uv run python -m insider_alerts.cli sec enrich --limit 100
uv run python -m insider_alerts.cli review enqueue --limit 100
uv run python -m insider_alerts.cli review apply --decision-file decision.json --notify
uv run python -m insider_alerts.cli ops deadletter-list
uv run python -m insider_alerts.cli ops deadletter-replay --packet-id <id>
```

## SEC compliance & privacy guardrails

- Explicit SEC user-agent and conservative rate-limit controls.
- Retry/backoff for transient HTTP failures.
- Decision and notification flows avoid secrets/PII.
- Deadletter trail preserves auditability for parser/drift failures.
