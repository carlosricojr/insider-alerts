# Frozen OPP-E07-V1 inference executable

`python -m insider_alerts.research.terminal_builder` is the single production wrapper for terminal
sealing and the separately invoked decision. It calls the frozen
`insider_alerts.research.inference` decision engine; neither module has broker imports or order
capability. Production execution requires an active
registry whose definition digest and canonical-content `inference.py` and `terminal_builder.py`
digests match the activation record. Runtime entry points additionally require the deployed active
registry to equal the exact canonical bytes in the append-only local activation receipt and a
matching append-only attestation proving those bytes were observed before the activation boundary.

The strict input object contains the activation/evaluation instants, ordered append-only entry-date
completion receipts, the complete candidate enrollment projection, fixed integrity checks, and
either no terminal dataset or one sealed terminal dataset. Every candidate fixes its planned entry
date, rank, identity, evidence digest, and observation time before resolution. Candidate order is
first-observed UTC then ID; enrollment sequences must be gap-free in entry-date/rank/ID order. A
freeze includes every enrolled trade on the first complete date reaching 100 trades and 60 dates.
Before freeze or outcome maturity, the report contains no return aggregate. At the 18-month
deadline, an append-only receipt binds the immutable candidate universe; pre-deadline pending
entries then drain before an outcome-free `KILL/insufficient_enrollment` result.

Trade timestamps are the official exchange RTH open and close boundaries. Under the frozen
daily-bar policy, SPY is measured from entry-session open through exit-session close; a stop or
target changes the stock exit price but does not invent an unavailable intraday barrier-hit time.

The terminal dataset contains the frozen challenger outcomes plus full-control and routine
diagnostic outcomes with explicit group-level completeness accounting. Its RFC 8785 SHA-256 is
checked before any outcome is parsed into an aggregate;
challenger IDs, evidence digests, ranks, dates, and sequences must exactly equal the frozen cohort.
All lists must already be in the registered deterministic order. False integrity checks produce
`INVALID`, never `KILL`. A malformed diagnostic is typed `unavailable` and cannot affect the
confirmatory decision.
Diagnostic membership or receipt catch-up never delays the primary seal. At challenger terminal
readiness, incomplete diagnostic material becomes an empty, explicitly unavailable group; status
never predeclares diagnostics clean and instead states that they are assessed non-blockingly under
the seal's multi-store locks, so an operator cannot optionally time the primary look around
diagnostic completion.

The SQLite seal store uses full synchronization and append-only triggers. Terminal sealing is a
separate command that does no aggregation. It binds the terminal dataset, complete candidate
projection, and immutable candidate-universe digests. The decision command refuses an unsealed
terminal dataset, alternate receipt, or second report. A sealed final decision is returned unchanged
on later invocations, regardless of replacement input.

Every report is RFC 8785 content-addressed after removing only `report_sha256`. Exit codes are 0 for
`COLLECTING` or `PROMOTE_RECOMMENDED`, 2 for `KILL`, and 3 for `INVALID`. A promotion recommendation
is inert and cannot alter the canary, orders, or capital.

The deployed operational protocol uses
`python -m insider_alerts.research.terminal_coordinator`. It is an unbound wrapper around the
frozen builder and inference modules; it does not change their bytes, registry, hypothesis, or
decision rule. On the inferential path, one hidden daily invocation may make at most one state
transition: a ready cohort is sealed without aggregation, and only a later invocation can run the single look. At the frozen
18-month deadline, the same wrapper constructs the registered no-dataset payload, seals the
immutable candidate universe, waits for pre-deadline pending entries to drain, and records the
outcome-free `KILL/insufficient_enrollment` report. Once no entries remain pending, that deadline
receipt and no-outcome report may be committed in the same invocation because no aggregate is
calculated. Its append-only operational log emits only
counts, state, reasons, and content digests. Scientific `KILL` is an operationally successful run;
retryable degradation returns 2, while persistent operational failure and scientific `INVALID`
return 3. Task Scheduler does not automatically restart any nonzero result, so a post-transition
logging failure cannot advance another terminal state before the next daily invocation.
Transitions are restricted to the daily after-hours window so
terminal reconciliation locks cannot interfere with live position management.

Manual recovery remains available through the frozen production wrapper (the producer emits only
counts, digests, and state; it never displays aggregate outcomes):

```powershell
.venv\Scripts\python.exe -m insider_alerts.research.terminal_builder seal `
  --seal-db data\research\trial_seals.db

.venv\Scripts\python.exe -m insider_alerts.research.terminal_builder decide `
  --seal-db data\research\trial_seals.db
```

The output path is created exclusively and is never overwritten. `activation_git_commit` identifies
the reviewed implementation commit and must itself contain the exact registry definition and bound
artifacts. Later deployment commits are allowed only when that commit remains an ancestor and every
bound text artifact still matches after platform-stable CRLF-to-LF canonicalization. After the seal
command, refresh the outer trial input's `evaluated_at_utc` and integrity state without changing the
sealed terminal dataset or candidate projection; the decision time must be at or after the terminal
receipt time. Production CLI input rejects an activation time later than current UTC and allows at
most one minute of positive clock skew for the evaluation time. Activation validation requires the
reviewed Git repository checkout (including its registry, schemas, policy, and lockfile); a wheel
without that checkout is intentionally not an authoritative decision environment.
