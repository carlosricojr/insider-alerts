# insider-alerts

Sprint 0/1 scaffold for Insider Alerts.

## Features (Sprint 0/1)

- Production-style `src/` Python package layout
- Typer CLI with `insider-alerts` entrypoint
- Environment-based configuration via `pydantic-settings`
- NTFY notifier with retries, timeout handling, and robust errors
- Deterministic tests with HTTP mocking (no network calls)
- CI workflow with lint, type-check, and test/coverage gates

## Quick start

```bash
uv sync --dev
uv run python -m insider_alerts.cli --help
```

## Configuration

Copy `.env.example` to `.env` and adjust values.

```env
NTFY_BASE_URL=https://ntfy.sh
NTFY_TOPIC=insider-alerts
NTFY_TOKEN=
NTFY_TIMEOUT_SECONDS=10.0
NTFY_RETRY_ATTEMPTS=3
NTFY_RETRY_MIN_SECONDS=0.5
NTFY_RETRY_MAX_SECONDS=3.0

SEC_RSS_URL=https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&start=-1&count=40&output=rss
SEC_USER_AGENT=insider-alerts/0.2 (contact: sec-access@example.com)
SEC_RATE_LIMIT_PER_SECOND=5
SEC_TIMEOUT_SECONDS=15
SEC_RETRY_ATTEMPTS=4
SEC_RETRY_MIN_SECONDS=0.25
SEC_RETRY_MAX_SECONDS=3.0

DATABASE_PATH=data/insider_alerts.db
```

## CLI

```bash
uv run python -m insider_alerts.cli --help
uv run python -m insider_alerts.cli notify test
uv run python -m insider_alerts.cli sec poll --once --max-items 40
```

- `notify test` sends a test NTFY message using configured environment values.
- `sec poll` ingests SEC Form 4 RSS references and writes idempotently to SQLite.

## Notes

- Sprint 0/1 only. Future features should be added incrementally.
- TODOs for later sprints are intentionally left as placeholders in code.
