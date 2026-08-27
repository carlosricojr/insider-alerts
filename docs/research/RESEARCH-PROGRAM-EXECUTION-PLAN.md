# Prospective insider-strategy research program

Status: active execution plan<br>
Started: 2026-08-26<br>
Owner: repository operator<br>
Production baseline at start: `6fedf7d07b549180e3575d4bbf18d1f13fdeeb0a`

## Outcome and definition of done

Build and operate a prospective research system that can make a defensible **promote or kill**
decision about a simple opportunistic-insider challenger without changing the existing E07/F00
live canary or its capital. Work is complete only when:

1. irrecoverable point-in-time signal, transport, market, option, model, and provenance evidence is
   captured durably with explicit missingness;
2. SEC owner/issuer history can be reconstructed without future information;
3. the registered challenger produces a complete shadow book under the existing execution policy;
4. inference, falsification, concentration, capacity, and final decision gates run from frozen code
   and a non-overlapping prospective sample;
5. monitoring proves the system is healthy and no visible terminals are created; and
6. every milestone is reviewed, merged, deployed from `origin/main`, and leaves all worktrees and
   repositories clean.

The existing live E07/F00 sleeve remains the execution-learning control. The new challenger is
shadow-only until a completed trial supports promotion and the user explicitly authorizes a live
change.

## Why this is the next experiment

The earlier bounded 168-hypothesis study did not establish an edge after Bonferroni correction.
It did identify E07/F00 as the only credible forward-paper candidate, so changing its exits or
optimizing another grid would spend more hypotheses on the same limited data. The next experiment
instead uses one ex-ante filter from published primary research: Cohen, Malloy, and Pomorski's
routine-versus-opportunistic insider classification. Their result assigns the predictive content
to opportunistic trades, while routine trades are largely uninformative.

This is a prior, not proof that the filter works for this service, latency, costs, or ten-session
barrier execution. The repository therefore tests it once on fresh data under
[a frozen preregistration](OPPORTUNISTIC-PROSPECTIVE-TRIAL-2026-08-26-PREREG.md). The conservative
alpha allocation and fixed terminal look protect against the broader history of attempted filters.

Primary references:

- [Cohen, Malloy, and Pomorski, *Decoding Inside Information*](https://www.nber.org/papers/w16454)
- [Harvey, Liu, and Zhu, *... and the Cross-Section of Expected Returns*](https://www.nber.org/papers/w20592)
- [SEC Insider Transactions Data Sets](https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets)

## Non-negotiable boundaries

- The deployment checkout stays on clean, synced `main`; development happens in isolated
  worktrees.
- Only merged `origin/main` is deployable. Source-fingerprint restarts are a safety mechanism, not
  a deployment mechanism.
- The E07/F00 live policy, two nominal $200 slots, cash-account requirement, and broker gates do
  not change in this program.
- No outcome-driven filter expansion. Market cap, trend, realized/implied volatility, and option
  surface features are capture-only exploratory fields in this trial.
- No retrospective substitution for missing point-in-time data. Missing evidence is a recorded
  result.
- No automatic promotion or capital change.

## Architecture and data flow

```text
SEC live filing -> review approval -> immutable evidence snapshot -> existing E07/F00 canary
                                      |                         \-> shadow control book
                                      +-> owner-history join    \-> opportunistic shadow book
                                      +-> market/options capture

SEC quarterly bulk -> raw immutable archive -> point-in-time owner history -> yearly classifier

registry + snapshots + fills/outcomes -> frozen inference report -> promote-or-kill recommendation
```

The decision path consumes only registered fields. Enrichment failures must never prevent the
existing canary from discovering, persisting, or managing an eligible candidate. Capture occurs
before or independently of recoverable downstream enrichment because historical option surfaces,
quotes, transport timing, and model state cannot be recreated later.

## Milestones

### M0 — Runtime and deployment safety (complete)

- Reject unqualifiable SEC symbol sentinels.
- Quarantine terminal contract failures per candidate while retrying transient broker failures.
- Make candidate/evidence persistence atomic.
- Restore the deployment checkout to clean `main` and use invisible `pythonw.exe` tasks.

Evidence: merged PRs #9 and #10; production baseline above.

### M1 — Governance and contracts (complete)

- Add concise repository-wide agent/deployment/research rules.
- Freeze this plan and the challenger preregistration.
- Add JSON Schemas for hypothesis registry entries and evidence snapshots.
- Validate the registered hypothesis in tests.

Exit gate: design critique, full local checks, reviewed PR, merge, clean/synced repositories. No
runtime deployment is required for docs/contracts alone.

### M2 — Irrecoverable point-in-time capture (complete)

- Create an append-only evidence store separate from operational canary state.
- Capture SEC accession/CIKs and filing/event/first-observed/decision times; notification request
  and provider response timing where available; monotonic duration; policy, source, model, prompt,
  and configuration hashes; contemporaneous quote/bar state; and all capture errors.
- Ask `alpha-core` for a timestamped option-surface artifact using an explicit timeout. Persist the
  surface by reference and digest, or persist a typed failure. It cannot block candidate handling.
- Canonicalize JSON with RFC 8785. Each evidence record hashes its complete envelope excluding only
  `record_sha256`; the activation record stores the registry definition digest (excluding `status`
  and `activation`) and separate preregistration, schema, inference-executable, and lockfile digests.
  Confirmatory sequence 1, not the activation record, seals the first enrolled position accession.
- Add a semantic validator for canonical UTC timestamps and ordering, active-registry membership,
  typed opportunistic eligibility, gap-free enrollment, supersession chains, and record digests.
- Add hidden watchdog/task installation and health telemetry without visible consoles.

Exit gate: fault-injection tests prove a dead option service, invalid surface, locked evidence DB,
broker outage, and process restart cannot create an order, lose a signal silently, mutate a prior
snapshot, or stop the existing canary from managing positions.

### M3 — SEC history and point-in-time classifier (complete)

- Download SEC quarterly archives from 2006 onward into a raw content-addressed cache, preserving
  retrieval metadata and upstream hashes when supplied. Fix `2006-01-01` as the observable-history
  boundary, bind each classification to an immutable gap-free snapshot, and disclose earlier
  history as left-censored rather than falsely claim lifetime completeness.
- Normalize submissions, reporting owners, and non-derivative open-market P/S transactions without
  destroying as-filed records. Resolve amendments by filing time; do not replace the past.
- Build annual owner classifications using only filings public before each classification cutoff.
- Return `routine`, `opportunistic`, `unpartitionable`, or `ambiguous_multi_owner`; never infer an
  owner from a name when reporting-owner CIK is absent.

Exit gate: hand-worked fixtures, boundary dates, amendments, same-month intersections, incomplete
years, left-censor cases, multi-owner filings, duplicate accessions, and replay-as-of tests all pass.

### M4 — Frozen inference executable (complete)

- Implement the exact endpoint, PRNG, circular block sampler, ordering and tie rules, sample freeze,
  18-month deadline, economic gates, decision states, and machine-readable report while the registry
  is still draft.
- Run synthetic positive, null, insufficient-enrollment, clustered, boundary-date, and adversarial
  fixtures. Content-bind the reviewed executable and dependency lock in the future activation record.

Exit gate: frozen fixtures deterministically promote or kill; the exact implementation is reviewed,
merged, deployed, and hashable before any confirmatory enrollment.

### M5 — Activate challenger, collect, and decide (current)

The order-incapable challenger runtime, point-in-time session/bar feeds, outcome materializer, and
prospective control-diagnostic capture are deployed. Deployment of the diagnostic outcome
materializer and terminal dataset/seal tooling remains an activation prerequisite; the terminal
dataset itself can exist only after the prospective sample freezes. The registry is still draft,
so no confirmatory enrollment has begun.

- Activate `OPP-E07-V1` only after M2, M3, and M4 are deployed from known merged commits and every
  required artifact digest is sealed.
- Preserve the complete E07/F00 candidate ledger and 20-slot control shadow book.
- Add a separately ranked, 20-slot opportunistic-only shadow book with identical entry,
  eligibility, costs, duplicate, capacity, barrier, and exit semantics.
- Store every inclusion/exclusion and classifier input digest. Never backfill a candidate into the
  confirmatory cohort after its decision timestamp.
- Resolve append-only enrollment transitions after deterministic entry-date ranking. Assign a
  gap-free sequence only to admitted positions; drain all pre-deadline pending records before an
  insufficient-enrollment decision.
- Continuously run only blinded counts, timestamp/hash integrity, coverage, missingness, and
  reconciliation diagnostics. Compute concentration, cost, split-period, best-trade, best-month,
  symbol, capacity-return, and inferential outputs only after terminal sealing.
- Produce a machine-readable decision artifact plus an operator-readable report at the single look,
  or a no-outcome `KILL/insufficient_enrollment` artifact at the frozen deadline.

Exit gate: deterministic replay matches live decisions; restarts are idempotent; broker interaction
is absent from the challenger; activation and first enrollment are sealed; production reaches
exactly one of `COLLECTING`, `PROMOTE_RECOMMENDED`, `KILL`, or `INVALID` with reasons.

### M6 — Sustained operation and handoff

- Monitor through enough fresh observations for the terminal decision.
- Confirm heartbeats, source revision, evidence lag, history coverage, shadow reconciliation, and
  invisible tasks.
- At each release, verify `main == origin/main`, no untracked worktree artifacts, and all related
  repositories clean.

Exit gate: the trial is genuinely running and can reach a valid decision without discretionary
analysis. `PROMOTE_RECOMMENDED` still requires explicit user approval before any live change.

## PR and context protocol

Each milestone is split into one-objective PRs. A handoff records: objective, constraints, exact
base/head SHAs, contracts changed, tests and adversarial cases, review evidence, deployed SHA,
runtime health, unresolved risks, and the next smallest step. Stable policy belongs in `AGENTS.md`
or the preregistration; transient investigation details stay in PRs and reports. Do not load broad
repository context when a named contract and its direct callers are sufficient.

Before coding a milestone, ask `claude -p` to challenge the design and verify each finding against
source. After implementation, run focused and full gates, then follow the repository's CodeRabbit
review-of-record race on the settled head. A later production-code change reopens review.

## Operational scorecard

Monitor without evaluating the confirmatory outcome early:

| Category | Required evidence |
| --- | --- |
| Runtime | fresh success heartbeat; no cycle error; current source fingerprint |
| Broker | one account selected; orders/positions/ledger reconciled; no unknown activity |
| Capture | snapshot count matches eligible decisions; lag bounded; failures typed |
| History | quarter coverage complete; archive and normalized digests valid |
| Trial | activation sealed; eligible/excluded counts reconcile; no outcome peek |
| Windows | worker/watchdog hidden; direct `pythonw.exe`; legacy visible task disabled |
| Git | deployment checkout clean; `main == origin/main`; temporary worktrees removed |

An alert on data quality or capture health pauses new confirmatory enrollment, not existing
position management. The event and reason are recorded; resumption cannot retroactively enroll
missed signals.
