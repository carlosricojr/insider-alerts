# Operations Runbook

## Daily flow
1. `uv run python -m insider_alerts.cli sec poll --once`
2. `uv run python -m insider_alerts.cli sec enrich --limit 100`
3. `uv run python -m insider_alerts.cli review enqueue --limit 100`
4. Apply analyst decisions with `review apply`.

## Failure handling
- Inspect deadletters:
  - `uv run python -m insider_alerts.cli ops deadletter-list`
- Replay packet:
  - `uv run python -m insider_alerts.cli ops deadletter-replay --packet-id <id>`

## SEC compliance guardrails
- Keep `SEC_USER_AGENT` explicit and contactable alias.
- Respect `SEC_RATE_LIMIT_PER_SECOND <= 10`.
- Use retries/backoff; avoid tight loops.
