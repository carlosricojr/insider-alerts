# Completed-session ghost repair

Outcome: prevent unfinished daily bars from settling the operational ghost book; preserve and
flag legacy evidence. Base: d58db60c1a1e6bca1963e7977d84482e89e1c60d.

Context checked: live task resolves the deployment checkout; frozen policy document and registry
are hash-bound, shared pure E07 kernel is used by research, and diagnostics use existing 20-slot
selection records. Prior audit found 18 premature time exits among 40 closed ghost records.

Scope: one PR. Filter only `_settle_shadow` inputs to dates strictly before the current New York
date. Publish daily-bar outcomes on the following local day, preserving the economic exit date.
Do not change broker data returned to live discovery/orders, shared kernel, slots, sizing, cash,
research artifacts, existing ghost membership, or historical values. Deferring even an early-close
day's result until the next local day avoids provider finalization races and new calendar bounds.

Legacy handling: read-only audit emits content-addressed annotations referencing original row
hashes and preserves source rows in an append-only artifact. No DB migration, replay, reopening,
corrected returns, or reconstructed contemporaneous data. Status exposes counts and explicitly
states the operational book is not validated performance. A failed audit/status cannot send an
order. Full uncapped coverage is a separate observer objective, not a change to this frozen book.

Verification: Claude read-only design challenge attempted; unavailable (Fable limit). Focused
tests cover unfinished tenth day, late stop after early target, future bars, next-day completion,
weekends/holidays/early closes, audit immutability and status warnings. Full ruff/mypy/pytest,
independent adversarial review and exact-head CodeRabbit loop before merge. Preserve predeployment
row hashes and frozen research artifact hashes; verify postdeploy task invisibility, broker
reconciliation, fresh cycle/no error and clean main == origin/main.

Rollback: stop/restart hidden worker using reviewed prior commit only with an explicit rollback
decision; do not delete audit artifacts or rewrite shadow history. Prior revision reintroduces
premature results, so forward repair is preferred. No strategy/capital promotion is authorized.

Handoff: implementation pending. No source changes in deployment checkout.
