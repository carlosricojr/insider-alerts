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
