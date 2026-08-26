# Repository working agreements

This repository operates a live IBKR canary. The root checkout at
`C:\Users\carlo\Repositories\insider-alerts` is the deployment checkout and must remain on a
clean, synced `main` branch.

## Safety and scope

- Make every production change on a separate branch in an isolated Git worktree. Never edit
  source or research policy in the deployment checkout.
- Keep one pull request to one objective. Deploy only a reviewed commit merged to `origin/main`.
- Preserve the frozen E07/F00 live-canary policy, cash-account mode, two nominal $200 live slots,
  and all broker safety gates. Changing policy, capital, account type, or order behavior requires
  explicit user authorization.
- Background processes must use `pythonw.exe` and Windows no-window creation flags. Never create
  scheduled actions that open visible consoles.
- Treat broker, SEC, and market-data failures as expected boundary cases. Fail closed for live
  orders and fail isolated for research enrichment; record why evidence is missing.

## Research integrity

- Follow [the research execution plan](docs/research/RESEARCH-PROGRAM-EXECUTION-PLAN.md) and the
  active machine-readable registry. Do not add, tune, reinterpret, or promote a hypothesis after
  outcome inspection.
- All model features must be point-in-time. Store source/as-of/observation timestamps and explicit
  missingness. Never backfill an unavailable historical option chain, fundamental, notification
  latency, or revised fact and present it as contemporaneous.
- Evidence snapshots are append-only and content-addressed. Corrections create a new record that
  references the superseded record; they do not overwrite history.
- Exploratory results are not confirmatory evidence. A new filter or endpoint requires a registry
  amendment and a fresh, non-overlapping prospective sample unless the active preregistration
  explicitly included it.
- No strategy promotion, capital increase, or live-order change is automatic. A passing research
  decision is a recommendation for explicit human approval.

## Workflow and verification

- For complex work, maintain a checked-in execution plan with outcome, constraints, verification,
  and handoff state. Keep this file concise and put task detail in the plan.
- Before implementation, run a read-only `claude -p` design challenge when available. After the
  diff settles, run local lint, strict typing, tests, and an adversarial review.
- Every PR must follow [the CodeRabbit review loop](docs/agents/coderabbit-pr-review-loop.md), bind
  evidence to the exact head SHA, address all findings, and merge only with green required gates.
- Use `uv run ruff check .`, `uv run mypy src`, and `uv run pytest` for the complete Python gate.
  Bootstrap an isolated worktree with `uv sync --extra dev`. Add focused tests for changed
  behavior and re-run the full gate before merge.
- After deployment, verify the installed revision, fresh cycle heartbeats, no cycle error, broker
  reconciliation, task/process invisibility, and a clean `main == origin/main` checkout.

Nested `AGENTS.md` or `AGENTS.override.md` files may add more specific instructions. The closest
applicable file wins.
