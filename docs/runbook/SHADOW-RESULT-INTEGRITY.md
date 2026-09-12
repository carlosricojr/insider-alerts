# Operational ghost-result integrity

The ghost evaluator accepts daily bars only from **prior America/New_York calendar days**.
It publishes a session's result on the following local day, including after early closes.
This avoids treating a current daily bar as final and preserves the frozen stop-first convention
when a target observed earlier in the day is followed by a stop. Economic entry/exit dates are
unchanged; broker live fills, protection, time exits and the shared research kernel are untouched.

This fixes future materialization, not historical data or portfolio validity. The original
20-slot selection history can inherit earlier premature exits. No old closed position is reopened,
no capacity/overlap decision is replayed, and a post-fix result is not automatically valid evidence.
The frozen challenger remains separate and INVALID; this release neither resets nor repairs it.

`ops live-canary-status` now includes `shadow_integrity`: record classifications and an explicit
`portfolio_performance_validated: false`. Classification uses recorded materialization timestamps
and the published 2026 exchange calendar. Unknown years, holidays and malformed timestamps are
unverifiable, never presumed valid. Intraday barrier records are unverified, not automatically
wrong; premature time exits are invalid. No aggregate returns are calculated.

Preserve content-addressed annotations without writing to the broker or ledger:

```powershell
uv run python -m insider_alerts.execution.shadow_audit --ledger data/live_canary.db --output data/shadow-integrity
```

Each artifact contains original ghost rows, their RFC-8785/SHA-256 hashes, annotations and an
observation time. Publication is atomic and refuses to replace existing files. Repeat audits
create new immutable evidence; they never substitute later prices into historical records.
Keep artifacts through rollback. The command has no research-store or broker access.

Next objective: separately design an order-incapable operational observer with uncapped candidate
coverage and explicit missingness. Do not expand/reset the frozen 20-slot book or infer approval
for live broker previews, capital changes, new endpoints or research promotion from this repair.
