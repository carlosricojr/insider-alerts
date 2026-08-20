# Sprint 9 Implementation Checklist (Compact)

Reference: `docs/sprints/SPRINT-09-REVIEW.md`

## Execution Rules
- Implement strictly in ticket order: `S09-001 -> ... -> S09-007`.
- Red-Green-Refactor per ticket (no code before failing tests).
- No scope creep beyond Sprint 9.
- Do not mark a ticket complete until all listed acceptance checks pass.

## Environment Guardrails
- Use workspace-local cache to avoid permission issues:
  - PowerShell: `$env:UV_CACHE_DIR='.uvcache'`
- Use a dedicated DB copy for research runs:
  - `data/insider_alerts_actionable.db`

## Ticket Order (Atomic)
1. `S09-001` readiness audit
   - Add: `src/insider_alerts/backtest/readiness.py`
   - Add tests: `tests/test_backtest_readiness.py`
2. `S09-002` canonical event extraction + dedupe
   - Add: `src/insider_alerts/backtest/event_data.py`
   - Add tests: `tests/test_backtest_event_data.py`
3. `S09-003` forward return/alpha engine
   - Add: `src/insider_alerts/backtest/event_study.py`
   - Add tests: `tests/test_backtest_event_study.py`
4. `S09-004` OOS fold runner
   - Extend: `src/insider_alerts/backtest/event_study.py`
   - Extend tests in `tests/test_backtest_event_study.py`
5. `S09-005` inference + false-positive controls
   - Add/extend stats utilities in backtest modules
   - Add tests for bootstrap/FDR/negative controls
6. `S09-006` CLI command + report schema
   - Extend: `src/insider_alerts/cli.py`
   - Extend tests: `tests/test_cli_extended.py`
7. `S09-007` runbook update
   - Update: `docs/runbook/BACKTESTING.md`

## Per-Ticket Quality Gate
- Lint touched files:
  - `uv run ruff check <touched files>`
- Run ticket tests:
  - `uv run pytest <ticket tests> -q`
- Ensure deterministic behavior with explicit random seed where applicable.

## End-of-Sprint Gate
- Full relevant tests pass (all new/modified suites).
- `ops event-study` produces:
  - readiness verdict,
  - dedupe diagnostics,
  - skip diagnostics,
  - fold-level and aggregate OOS outputs,
  - confidence + FDR + negative-control results,
  - explicit go/no-go verdict.
- Gate thresholds and run metadata are present in report output.

## First Command Set (Start S09-001)
```powershell
$env:UV_CACHE_DIR='.uvcache'
uv run ruff check src/insider_alerts/backtest tests
uv run pytest tests/test_backtest_readiness.py -q
```
