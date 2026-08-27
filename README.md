# insider-alerts

Autonomous insider-trade signal pipeline for SEC Form 4 filings.

It continuously:
1. Polls SEC Form 4 feed.
2. Enriches filings with raw Form 4 XML URLs.
3. Parses/scorers insider transactions.
4. Asks an isolated local Quant CLI to decide `approve|reject|escalate`.
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

- Quant uses native Codex first and native Claude as failover, with no tools or write access.
- A judge infrastructure failure leaves the immutable packet pending for retry; it is not treated
  as a market judgment.
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
MARKET_DATA_RATE_LIMIT_PER_SECOND=1.0
MARKET_DATA_RETRY_ATTEMPTS=3
DATABASE_PATH=data/insider_alerts.db
```

Notes:
- `NTFY_TOPIC` is what you subscribe to in the NTFY app.
- Keep `SEC_USER_AGENT` explicit/contactable for SEC compliance.

### C. Verify a local Quant CLI (one-time)

```powershell
codex --version
claude --version
```

Only one backend is required. On Windows the runtime resolves native executables so scheduled
`pythonw.exe` jobs never open a console. Set `INSIDER_QUANT_CODEX_MODEL` or
`INSIDER_QUANT_CLAUDE_MODEL` only when an explicit provider model override is required.

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
source=quant:quant-insider:codex:gpt-5.6-sol:low
why=Quant thesis: unusual-size insider accumulation with strong buy skew.
```

## Useful commands

```powershell
uv run python -m insider_alerts.cli review pending --limit 50
uv run python -m insider_alerts.cli review decide --packet-id "0000320193-24-000123|0000320193|4" --decision approve --reason "Quant thesis..." --analyst quant --notify
uv run python -m insider_alerts.cli ops deadletter-list
uv run python -m insider_alerts.cli ops deadletter-replay --packet-id <id>
# Historical SEC filing reference backfill for a specific window:
uv run python -m insider_alerts.cli sec backfill --start-date 2025-01-01 --end-date 2025-12-31
# Default window is last 365 days ending today.
uv run python -m insider_alerts.cli ops backtest --output-json reports/backtest_latest.json
# Optional explicit window (must provide both dates together):
uv run python -m insider_alerts.cli ops backtest --start-date 2025-02-13 --end-date 2026-02-13 --output-json reports/backtest_latest.json
```

## Troubleshooting

- `ModuleNotFoundError: insider_alerts`:
  - run commands from repo root (`cd ...\insider-alerts`).
- nonzero `quant_deferred` counts:
  - verify at least one of `codex --version` or `claude --version` succeeds for the scheduled-task
    user;
  - inspect the labeled backend error in `logs/autopilot.err.log`;
  - packets remain pending and are retried oldest-first, so do not manually convert an
    infrastructure error into `escalate`.

## Further docs

- `docs/runbook/OPERATIONS.md`
- `docs/runbook/BACKTESTING.md`
- `docs/runbook/LIVE_CANARY.md`
- `skills/insider-review/SKILL.md`
