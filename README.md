# insider-alerts

Autonomous insider-trade signal pipeline for SEC Form 4 filings.

It continuously:
1. Polls SEC Form 4 feed.
2. Enriches filings with raw Form 4 XML URLs.
3. Parses/scorers insider transactions.
4. Asks Quant (OpenClaw agent) to decide `approve|reject|escalate`.
5. Sends NTFY notifications for approved trade signals.

## 1) What this does

- Monitors new SEC Form 4 filings.
- Converts filings into review packets with a deterministic score + rationale inputs.
- Uses a dedicated Quant LLM agent for final decisioning.
- Applies safety guardrails before approval.
- Emits high-signal NTFY alerts with ticker + reason for approved ideas.

## 2) How it works

Pipeline per cycle:

1. `sec poll`
2. `sec enrich`
3. `review enqueue`
4. `ops autopilot` decision phase
5. NTFY notify on approvals

Core safety behavior:

- Quant runs through an isolated agent (`quant-insider`), not `main`.
- `main` agent is blocked by default in quant mode.
- Approval guardrails require strong score + positive net insider buy.
- Duplicate packets (same accession/form) are deadlettered to reduce noise.
- Non-parseable/bad SEC payloads are skipped without crashing the cycle.

## 3) Setup from 0 -> 1

### Prereqs

- Python + `uv`
- OpenClaw CLI installed and authenticated
- NTFY app subscribed to your topic

### A. Clone and install

```powershell
git clone <your-repo-url>
cd insider-alerts
uv sync --dev
```

### B. Configure `.env`

Create `.env` in repo root:

```env
NTFY_BASE_URL=https://ntfy.sh
NTFY_TOPIC=insider-alerts-0808
NTFY_TOKEN=
SEC_USER_AGENT=insider-alerts/0.2 (contact: your-email@example.com)
DATABASE_PATH=data/insider_alerts.db
```

Notes:
- `NTFY_TOPIC` is what you subscribe to in the NTFY app.
- Keep `SEC_USER_AGENT` explicit/contactable for SEC compliance.

### C. Create isolated Quant agent (one-time)

```powershell
& "$env:APPDATA\npm\openclaw.cmd" agents add quant-insider --workspace "$PWD\ops\quant-insider-workspace" --non-interactive
```

### D. Smoke test notification

```powershell
uv run python -m insider_alerts.cli notify test
```

### E. Run one autopilot cycle (no notify)

```powershell
uv run python -m insider_alerts.cli ops autopilot --once --decision-engine quant --quant-agent-id quant-insider --quant-batch-size 8 --no-notify
```

### F. Start continuous monitoring

```powershell
uv run python -m insider_alerts.cli ops autopilot --loop --interval 300 --decision-engine quant --quant-agent-id quant-insider --quant-batch-size 8
```

Default notify mode is approve-only, so you only get trade-signal alerts.

## Example trade alert

NTFY title:

```text
TRADE SIGNAL: CEG
```

Body:

```text
ticker=CEG
packet=0000905148-26-000640|0001824653|4
owner=Hanson Bryan Craig
score=100.00
net_buy_shares=4754.00
gross_value=13268128.95
source=quant:quant-insider
why=Quant thesis: unusual-size insider accumulation with strong buy skew.
```

## Useful commands

```powershell
uv run python -m insider_alerts.cli review pending --limit 50
uv run python -m insider_alerts.cli review decide --packet-id "0000320193-24-000123|0000320193|4" --decision approve --reason "Quant thesis..." --analyst quant --notify
uv run python -m insider_alerts.cli ops deadletter-list
uv run python -m insider_alerts.cli ops deadletter-replay --packet-id <id>
uv run python -m insider_alerts.cli ops backtest --start-date 2024-01-01 --end-date 2026-12-31 --output-json reports/backtest_latest.json
```

## Troubleshooting

- `ModuleNotFoundError: insider_alerts`:
  - run commands from repo root (`cd ...\insider-alerts`).
- frequent `quant-fallback` escalations:
  - ensure autopilot uses `--quant-agent-id quant-insider --quant-batch-size 8`.
  - verify OpenClaw agent works:
    - `openclaw agent --agent quant-insider --message "Reply exactly OK" --json`

## Further docs

- `docs/runbook/OPERATIONS.md`
- `docs/runbook/BACKTESTING.md`
- `skills/insider-review/SKILL.md`
