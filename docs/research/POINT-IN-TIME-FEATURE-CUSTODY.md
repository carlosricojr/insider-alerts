# Point-in-time Companyfacts custody

This stream closes the remaining irrecoverable input gap for future market-cap research. It is
capture-only telemetry. It does not amend `OPP-E07-V1`, participate in its enrollment or terminal
look, read its outcomes, affect the E07/F00 canary, or call IBKR.

## Causal boundary

The worker watches the immutable identity fields of approved `research_capture_jobs` independently
of the slower option evidence snapshot. Only jobs whose decision timestamp is at or after the
separately sealed feature-stream activation are admitted. A request must begin no more than 900
seconds after that decision. An older job becomes typed missingness; it is never backfilled and
presented as contemporaneous.

Every successful SEC response is stored byte-for-byte in a content-addressed `.bin` artifact. Its
receipt binds the request and response times, endpoint, status, selected HTTP metadata, raw digest,
source-job digest, policy digest, runtime commit, issuer identity, and deterministic selection
provenance. Invalid UTF-8, invalid JSON, an issuer mismatch, no eligible fact, deadline expiry, and
request failure are distinct terminal results. An artifact-write failure is retried before becoming
distinct terminal missingness. Expected missingness completes the scheduled run successfully so
enrichment cannot masquerade as a service failure.

The reviewed policy is
[`companyfacts-capture-v1.json`](contracts/companyfacts-capture-v1.json). The first initialization
seals its exact byte digest, activation timestamp, and selection-code digest in an immutable
configuration row. A reinstall or runtime that changes any of those values fails closed.

## Conservative shares selection

Companyfacts reports only a filing date for each unit observation, not a time of day. A fact filed
on the signal date might therefore have become public after the decision. The frozen selector uses
only facts whose `filed` date is **strictly before** the UTC decision date and whose period end is no
later than that date. It prefers `dei:EntityCommonStockSharesOutstanding`, then
`us-gaap:CommonStockSharesOutstanding`, and applies the fixed ordering in the policy. Every relevant
candidate and rejection reason remains in the receipt; the unmodified response remains authoritative
if a later, separately preregistered study needs a different ex-ante definition.

This captures shares outstanding, not a market cap by itself. A future fresh-sample study can bind
the selected shares observation to the already-immutable pre-signal closing-price bar. No such join,
filter, threshold, or outcome analysis is allowed in the active confirmatory trial.

## Hidden Windows task

Choose the activation once, at deployment, after the reviewed commit is on clean synced `main`:

```powershell
.\ops\windows\install-feature-capture-task.ps1 `
  -ActivationAtUtc 2026-08-28T12:34:56.000000Z `
  -Start
```

The installer preflights and seals the configuration before registering a direct hidden
`pythonw.exe` action with `MultipleInstances IgnoreNew`. It refuses to move an existing task's
activation boundary. Inspect health and full store integrity without making an SEC request:

```powershell
.\.venv\Scripts\python.exe -m insider_alerts.research.feature_worker `
  --feature-db data\research\feature_evidence.db `
  --artifact-root data\research\artifacts\companyfacts `
  --status
```

Rollback disables the scheduled task. Do not delete or rewrite the feature database or artifacts.
