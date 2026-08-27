# Prospective control diagnostics

The OPP-E07-V1 trial uses the existing live-canary E07/F00 shadow ledger as its only full-control
selection authority. `insider_alerts.research.diagnostics` copies no orders and runs no selection
policy. It records content-addressed, append-only bindings in
`data/research/diagnostics.db` for:

- the stable canary selection projection and the distinct packet-created and approval timestamps;
- the ten-session schedule horizon as known at the canary approval time;
- the first point-in-time owner-classification snapshot and routine-subgroup disposition;
- a final canary shadow state and optional shadow-trade agreement record; and
- typed reconciliation evidence when a join or immutable projection disagrees.

Eligible control candidates request both stock and SPY completed daily bars from the shared
append-only bar feed. The request begins 120 calendar days before the packet was first observed and
ends on the bound tenth session. A later schedule revision cannot rewrite the horizon.

The diagnostic phase runs before the confirmatory phases in the hidden trial worker. Every
diagnostic exception is logged and written only to diagnostic health; the worker then continues the
confirmatory phases. Diagnostic failure therefore cannot mutate `trial.db`, change a challenger
integrity gate, or prevent challenger capture. The registry remains draft until the separate
activation procedure, so the deployed phase initially writes only an `idle_registry_draft`
heartbeat and no candidates or bar requests.

Read blinded health and counts without opening a broker connection:

```powershell
.\.venv\Scripts\python.exe -m insider_alerts.cli ops research-diagnostics-status
```

The status command does not expose individual returns or aggregate performance. Missing, unreadable,
or corrupt storage, a never-run or stale heartbeat, and a degraded worker all exit with code 3. The
production scheduled task must pass absolute paths for the diagnostic database, canary ledger,
source database, evidence database, bar feed, session feed, and registry, and must continue to
execute with `pythonw.exe` and hidden task settings.
