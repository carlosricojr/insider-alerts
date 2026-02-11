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
python -m venv .venv
source .venv/bin/activate  # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -e .[dev]
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
```

## CLI

```bash
insider-alerts --help
insider-alerts notify test
```

`notify test` sends a test NTFY message using configured environment values.

## Notes

- Sprint 0/1 only. Future features should be added incrementally.
- TODOs for later sprints are intentionally left as placeholders in code.
