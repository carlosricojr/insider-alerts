# Skill: insider-review

Purpose: apply analyst review decisions to queued insider-alert packets.

## Decision schema
```json
{
  "packet_id": "<accession|cik|form_type>",
  "decision": "approve|reject|escalate|deadletter",
  "analyst": "<alias>",
  "reason": "<short rationale>"
}
```

## Guardrails
- Validate schema before apply.
- Only pending packets may be updated.
- Deadletter decisions must remain replayable.

## Recommended command flow for agents
1. Inspect actionable packets (read-only):
   - `uv run python -m insider_alerts.cli review pending --limit 50`
2. Apply one explicit decision:
   - `uv run python -m insider_alerts.cli review decide --packet-id "0000320193-24-000123|0000320193|4" --decision approve --reason "<why>" --analyst quant --notify`
3. If command exits with code `3`, packet was not pending/found; do not retry blindly.
