# Deployment

## Prerequisites
- Python 3.11+
- uv

## Setup
1. `uv sync --dev`
2. Configure environment from `.env.example`.
3. Validate:
   - `uv run python -m ruff check .`
   - `uv run python -m mypy src`
   - `uv run python -m pytest`

## Production checklist
- SEC user-agent and contact alias configured.
- DB path on persistent storage.
- Notification topic/token validated.
- Scheduled poll/enrich/enqueue cadence defined.
