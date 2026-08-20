# IBKR Live Canary Runbook

This process runs preregistered strategy `E07/F00` prospectively. It maintains a complete
20-slot shadow portfolio for inference and a two-slot, whole-share live portfolio for execution
learning. The live account remains a cash account.

## Frozen policy

- Source: only future live `sec_rss` approvals after the ledger activation timestamp.
- Entry: next regular-session opening auction; signals seen at/after 09:20 ET are deferred.
- Eligibility: prior close from $2 through $200 and median 20-session dollar volume of at least
  $500,000. No post-selection filter is added.
- Duplicate exposure: suppress overlapping positions in the same symbol.
- Exit: 10% stop, 10% target, otherwise the close of session 10. A same-bar shadow collision is
  charged to the stop.
- Live sizing: two nominal $200 slots, whole shares, $50 cash reserve, and a 5% market-order cash
  cushion. Planned simultaneous stop risk is about $40; gaps can lose more.
- Capacity: SHA-256 rank fixed by the policy salt, session, packet, accession, and symbol. Orders
  are sent only during 09:18-09:20 ET so the known candidate set is ranked once near the cutoff.

## Fail-closed broker gates

New buys are disabled if any of these is true:

- `--live` and the exact `INSIDER_LIVE_ARM=I_ACCEPT_LIVE_CANARY_RISK` process variable are not
  both present;
- the Gateway exposes multiple accounts without an explicit account selection;
- cash is insufficient or unsettled;
- a position or order exists that the canary ledger cannot reconcile;
- the conservative commission preview exceeds $0.75 one-way or 50 bps;
- IBKR returns any warning/rejection during the preflight.

The portal pricing setting is not trusted by itself. The per-order what-if response is the runtime
authority. A finite exact commission is used when IBKR supplies one. Under Tiered pricing IBKR may
instead leave the exact field unset and supply a route-dependent minimum/maximum range; the canary
budgets against that range's validated upper bound. A missing, malformed, non-USD, or warned range
is rejected. The generic hard-cap fallback is disabled in the installed live task.

Protective exits are server-held GTC OCA orders and are submitted stop-first after the opening
fill. The timed exit joins the same OCA group as a market-on-close order on session 10.

## Commands

Read-only one-cycle validation:

```powershell
.\.venv\Scripts\python.exe -m insider_alerts.cli ops live-canary --once
```

Install or refresh the invisible per-user Windows watchdog (logon start plus one-minute recovery):

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\ops\windows\install-live-canary-task.ps1 -Start
```

Read state without connecting to IBKR:

```powershell
.\.venv\Scripts\python.exe -m insider_alerts.cli ops live-canary-status
```

The status includes a runtime source fingerprint. `source_revision_current: false` means files
changed after the worker started. New workers detect later source drift themselves, exit cleanly,
and are relaunched invisibly by the watchdog's next recovery trigger. Running the installer with
`-Start` performs an immediate controlled restart so the registered worker always loads the
current source.

Logs are `logs/live-canary.out.log` and `logs/live-canary.err.log`; the independent durable ledger
is `data/live_canary.db`. Stop the background canary before manually trading in this same IBKR
account. If the process detects manual activity anyway, it blocks new entries.

## Recovery

After a restart, the same API client ID must be used so IBKR can bind and reconcile its orders.
The daemon re-reads the live account, canary-tagged orders, and its ledger each cycle. If broker
state is ambiguous it records a critical event and does not initiate another position. Never
delete or replace `data/live_canary.db` while canary orders or positions exist.
