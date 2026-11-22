# E07 volatility custody rehearsal

This release runs the evidence-capture path for two upcoming exchange sessions.
It observes candidates through the same live ledger's read-only projection.
It records the first durable pre-open read of a verified 21-close response
and takes daily price snapshots through each admitted case's ten-session
horizon. It never enrolls a scientific cohort, calculates portfolio outcomes,
previews or sends an order, or writes to the live ledger.

## Operational boundaries

The rehearsal uses the reviewed calendar version already on disk and its
official close for each session. A changed version stops the rehearsal.
The broker adapter is the existing bounded market-data-only Gateway client.
Its requests run from 18:00 to 08:00 New York, spaced by at least five minutes
across restarts. The previous acquisition task must be disabled, without a
running commissioning process. Its empty network coordination database
serializes access to client 179. The archived evidence and calendar remain
read-only. Other tasks and their settings stay unchanged.

The separate journal binds the activation inventory, resolved contract ID,
SEC directory bytes and clock, provider response and original source version.
Require a unique SEC issuer and venue match for a positive feature receipt.
A read only counts as timely when its durable write returns before 09:18.
An interrupted read loses that proof. Missing input has an explicit 10%
fallback and remains visible in the denominator. A candidate seen after
cutoff is recorded without reconstructing a decision it missed.

Post-close snapshots bind the original contract ID and document the observed
IBKR price projection. Corporate-action and as-traded qualification are still
required before these observations can enter a separate historical evaluation.
Keep this rehearsal out of any confirmatory sample.

## Deployment and handoff

Deploy only from clean synced `main` after exact-head review. Verify the
existing live task's checkout, default ledger and hidden launch. Explicitly
initialize the new journal, selecting the first reviewed future exchange
session with at least two hours before its cutoff. Supply the separately
authorized SEC contact if deployment settings have none. Keep that value
out of source control and status output.

```powershell
.\.venv\Scripts\python.exe -B -m ops.research.e07_volatility_rehearsal --activate <YYYY-MM-DD> --review-reference <reviewed-PR> --sec-contact <contact>
.\ops\windows\install-volatility-rehearsal-task.ps1 -Start
.\.venv\Scripts\python.exe -B -m ops.research.e07_volatility_rehearsal --status
```

Verify a fresh hidden task heartbeat, the source and dependency digest, no
cycle error, the task's registration and the unchanged live reconciliation
cycle. After a target session verify actual pre-open observation or typed
missingness and the expected daily outcomes. Rollback stops and disables
only the rehearsal task. Retain its evidence.

The design challenge was completed before implementation. Verify full Python
gates, strict typing of the operator modules, read-only custody, exact dates,
clock discontinuity, uninterrupted and interrupted durable reads, source
pacing across restart, contract pinning, SEC mismatch, early closes and
the Windows installer. The separate historical qualification, final
preregistration and shadow trial remain open.
