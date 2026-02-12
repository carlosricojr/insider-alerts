# Operations Runbook

## Daily flow
1. `uv run python -m insider_alerts.cli sec poll --once`
2. `uv run python -m insider_alerts.cli sec enrich --limit 100`
3. `uv run python -m insider_alerts.cli review enqueue --limit 100`
4. List pending packets with `uv run python -m insider_alerts.cli review pending --limit 50`.
5. Apply analyst decisions with `review decide` or `review apply`.

## Background autopilot
- Create dedicated Quant agent once:
  - `& "$env:APPDATA\npm\openclaw.cmd" agents add quant-insider --workspace "$PWD\ops\quant-insider-workspace" --non-interactive`
- Start continuous background loop:
  - `uv run python -m insider_alerts.cli ops autopilot --loop --interval 300 --decision-engine quant --quant-agent-id quant-insider --quant-batch-size 8`
- Default auto decision rules:
  - approve if `score >= 90` and `net_buy_shares > 0`
  - reject if `score <= 35` or `net_buy_shares < 0`
  - otherwise escalate
- Quant safety mode:
  - blocks `--quant-agent-id main` by default (`--quant-require-isolated-agent`)
  - use a dedicated isolated agent id for Quant decisions
- Notifications:
  - default is enabled and approve-only (`--notify --notify-approve-only`)
  - use `--notify-all-decisions` only if desired

## Quant-safe decision protocol
- Read-only inspect queue:
  - `uv run python -m insider_alerts.cli review pending --limit 50`
- Apply one explicit decision:
  - `uv run python -m insider_alerts.cli review decide --packet-id "0000320193-24-000123|0000320193|4" --decision approve --reason "Quant thesis..." --analyst quant --notify`
- Automation guardrails:
  - Decision payloads are validated (`packet_id`, `decision`, `analyst`, `reason`).
  - Only `pending` packets can be updated.
  - Duplicate pending packets for the same accession/form are auto-deadlettered.
  - `review decide` and `review apply` return exit code `3` when packet is not pending/found.
  - NTFY notification only sends after a successful update.

## Failure handling
- Inspect deadletters:
  - `uv run python -m insider_alerts.cli ops deadletter-list`
- Replay packet:
  - `uv run python -m insider_alerts.cli ops deadletter-replay --packet-id <id>`

## SEC compliance guardrails
- Keep `SEC_USER_AGENT` explicit and contactable alias.
- Respect `SEC_RATE_LIMIT_PER_SECOND <= 10`.
- Use retries/backoff; avoid tight loops.
