# Prospective control diagnostics

The OPP-E07-V1 trial uses the existing live-canary E07/F00 shadow ledger as its only full-control
selection authority. `insider_alerts.research.diagnostics` copies no orders and runs no selection
policy. It records content-addressed, append-only bindings in
`data/research/diagnostics.db` for:

- the stable canary selection projection and the distinct packet-created and approval timestamps;
- the ten-session schedule horizon as known at the canary approval time;
- the first point-in-time owner-classification snapshot and routine-subgroup disposition;
- a final canary shadow state and optional shadow-trade agreement record;
- a research-feed-authoritative E07 outcome plus an atomic per-candidate disposition receipt; and
- typed reconciliation evidence when a join or immutable projection disagrees.

Eligible control candidates request both stock and SPY completed daily bars from the shared
append-only bar feed. The request begins 120 calendar days before the packet was first observed and
ends on the bound tenth session. A later schedule revision cannot rewrite the horizon.

Only a canary candidate whose frozen shadow state is `closed` produces a control trade. Rejected,
overlap-suppressed, and capacity-suppressed records receive explicit `not_traded` receipts rather
than disappearing. A closed record is recomputed only after the frozen tenth session closes and
healthy stock and SPY receipts prove the required first-observed bars. The shared pure outcome-proof
kernel is also used by the challenger materializer. Missing terminal inputs become an immutable,
typed `unavailable` receipt for that candidate and do not block later candidates or change
`trial.db`. The frozen canary shadow trade is compared only after the research outcome is computed;
a disagreement preserves the independently computed research record for audit, emits append-only
reconciliation evidence, and marks the candidate `unavailable` so it cannot enter a valid terminal
diagnostic cohort. Agreement includes the frozen stop and target prices, not only the realized exit.

Outcome and receipt records are appended atomically with full synchronization. Integrity validation
requires every evidence, state, outcome, and receipt record to have an owning diagnostic candidate,
and checks all candidate/state/evidence/outcome digest links. Health writes are monotonic so an
overlapping older worker cannot publish a stale heartbeat over a newer cycle.

The time-sensitive confirmatory phases run before the diagnostic phases in the hidden trial worker,
so diagnostic lock waits or latency cannot change challenger enrollment. Every diagnostic exception
is logged best-effort and written only to diagnostic health. Capture and outcome failures are
isolated from each other, including when the diagnostic error log itself is unavailable. Diagnostic
failure therefore cannot mutate `trial.db`, change a challenger integrity gate, or prevent
challenger capture. The registry remains draft until the separate
activation procedure, so the deployed phase initially writes only an `idle_registry_draft`
heartbeat and no candidates or bar requests.

Read blinded health and counts without opening a broker connection:

```powershell
.\.venv\Scripts\python.exe -m insider_alerts.cli ops research-diagnostics-status
```

The status command does not expose individual returns or aggregate performance. It includes only
blinded counts for available, not-traded, and unavailable outcome receipts. Missing, unreadable, or
corrupt storage, a never-run or stale heartbeat, and a degraded worker all exit with code 3. The
production scheduled task must pass absolute paths for the diagnostic database, canary ledger,
source database, evidence database, bar feed, session feed, and registry, and must continue to
execute with `pythonw.exe` and hidden task settings.
