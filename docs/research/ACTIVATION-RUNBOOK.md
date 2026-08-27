# OPP-E07-V1 activation runbook

Activation is an irreversible, two-PR cutover. The existing E07/F00 live canary, its capital,
orders, and policy are outside this procedure and continue unchanged.

## Phase A: deploy the activation machinery while draft

Merge and deploy the reviewed activation implementation with the registry still `draft`. Confirm
the production checkout is clean `main == origin/main`, all research tasks use hidden
`pythonw.exe`, IB Gateway is listening, and the evidence, trial, diagnostic, and seal stores pass
integrity checks with zero scientific rows. A source capture job may exist before activation; its
source-first-observed timestamp keeps it outside the future cohort.

## Phase B: prepare one future boundary

Choose a UTC boundary at least two hours in the future. Prefer the next midnight in
`America/New_York`, leaving enough time for a registry-only PR, review, merge, deployment, and
post-deployment verification. From the clean production checkout run:

```powershell
.venv\Scripts\python.exe -m insider_alerts.research.activation prepare `
  --activated-at-utc 2026-08-28T04:00:00.000000Z
```

The command holds write-preventing locks over every scientific store, proves each is empty, and
commits one append-only activation receipt before publishing a canonical, content-addressed active
registry artifact. It never imports broker or order code. An identical retry replays the stored
registry bytes; another timestamp or definition is rejected.

Copy the artifact's exact bytes into `docs/research/registry/OPP-E07-V1.json` on a new registry-only
branch. Do not regenerate or hand-edit the activation block. Review, merge, and deploy that second
PR before the boundary. Stop only the research tasks during the fast-forward pull, then restart
them hidden; the live canary remains running. A draft registry is valid before the prepared
boundary but becomes fail-closed invalid at the boundary. An active registry without the matching
local append-only receipt is also invalid, so deployment cannot invent or shift the cohort start.
Before the sealed instant, consumers report `idle_registry_armed` and cannot append scientific
rows; they transition to collecting against the same clock exactly at the boundary.

Verify before the boundary:

```powershell
.venv\Scripts\python.exe -m insider_alerts.research.activation status
.venv\Scripts\python.exe -m insider_alerts.cli ops research-capture-status
.venv\Scripts\python.exe -m insider_alerts.cli ops research-trial-status
.venv\Scripts\python.exe -m insider_alerts.cli ops research-diagnostics-status
```

Required state is `active`, exact receipt/registry agreement, zero scientific rows, fresh healthy
heartbeats, and a clean synced production checkout. If the active registry is not deployed before
the boundary, do not choose another time or activate late: the receipt remains evidence of the
failed attempt and `OPP-E07-V1` must be retired in favor of a new ID and fresh sample.
