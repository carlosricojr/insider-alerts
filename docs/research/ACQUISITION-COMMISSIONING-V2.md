# Acquisition commissioning V2

This collector measures whether a future strategy comparison can obtain its
required inputs. It computes no returns, selects no profitable subsets, and never
places or previews an order. The E07/F00 live canary and INVALID OPP-E07-V1 trial
remain separate. Commissioning observations cannot enter a later confirmatory
sample. This is not evidence of profitability or permission to increase capital.

## Activation and operation

Deploy the complete reviewed PR from clean, synced `main`. Inspect the existing
`Insider Alerts Live Canary Worker` executable, arguments and working directory;
the new task must use that same checkout and its `pythonw.exe`. Verify broker
client 179 is not assigned to another application before initial activation.

From that checkout, explicitly create the commissioning journals. Supply a reviewed
PR reference and an aware UTC boundary at least two hours in the future:

```powershell
uv run python -m insider_alerts.research.commissioning_worker --activate-at <future-UTC-time> --review-reference <reviewed-PR-URL>
& .\ops\windows\install-commissioning-task.ps1 -Start
uv run python -m insider_alerts.research.commissioning_worker --status
```

Activation fetches `origin/main`, checks a clean matching branch/revision, archives
the preexisting packet-ID inventory, and binds the source ledger's path and file
identity, implementation/dependency digest, NYSE source hashes and calendar version.
It creates `data/research-commissioning-v2/evidence.db` and `calendar.db`
exclusively. Default execution and status cannot initialize a missing journal.
The installer refuses to replace any existing scheduled task.

The hidden task runs every minute and at logon, under the current user's limited
interactive token. Parallel instances allow source-field capture while another
instance waits for IBKR. Separate nonblocking custody/network locks are released
by the OS after process death. Each instance has a two-minute Task Scheduler limit;
broker I/O has a 50-second budget and the earlier off-hours/recovery deadline.
All subprocesses use no-window flags; the task executable is `pythonw.exe`.

Source reads project only the documented candidate fields, close their read-only
SQLite transaction, and then append changed observations. Broker requests operate
only between 18:00 and 08:00 New York, with a durable 300-second minimum between
attempts. An interrupted attempt remains recorded and cannot defeat pacing.
Infrastructure outages use global backoff up to 30 minutes; content gaps use
per-job backoff up to six hours. Retries stop at the fixed recovery deadline,
not after an arbitrary number of logouts or failed responses.
Within the bounded benchmark reservation, reachable deadlines precede expired
backlog, then fresh requests precede retries. This ordering uses no outcomes.

## What is frozen

The admission clock is E07's unchanged 09:20 cutoff. The window contains ten entry
sessions; each case needs its 20 policy-completed pre-signal sessions and ten-session
horizon. Late observations keep their original window. Invalid symbols and unknown
admissions remain counted. Preexisting records are separately inventoried, including
old malformed timestamps. Revisions preserve both the original admission and the
actual later observation; they never make a corrected feature contemporaneous.
First observations outside the window are also retained, so a later correction
cannot silently admit a previously excluded record.

R1 acquires history. R2 follows the full horizon. R3 follows a later exchange close
than the first complete R2 receipt. The first complete R2/R3 pair is fixed, including
when it disagrees; the collector never hunts for a convenient matching pair. Requests
follow each case's originally qualified contract ID even after symbol drift. A new
case must establish its own identity and cannot inherit a reused ticker's old ID.
Compatible due ranges are combined. SPY receives R1, a W10 checkpoint, W19 R2 and
a later-close R3, plus completed-session SCHEDULE checks through W20.

The recovery deadline is fixed at activation: 08:00 New York on the later of W19
plus five calendar days and W20 plus three calendar days. No data-dependent
extension occurs. Later calendar versions are compared with the frozen version;
contradictions are reported without rewriting admission or historical bindings.

## Reading status

- Acquisition counts and per-date missing reasons describe received coverage.
  A malformed extra bar cannot invalidate unrelated valid dates. Instrument,
  clock and request mismatches invalidate the receipt as a whole.
- Source observation before 09:20 and journal-record construction before 09:20
  are separate diagnostics. Neither proves a pre-cutoff durable read. A future
  trial must bind its own actual decision snapshot.
- Repeated identical SDK bars show empirical stability, not provider finality,
  official auction fills, dividend adjustments or historical issuer identity.
- Custody continuity has five-minute recorded-poll resolution. Overnight attempt
  counts include nights with no attempts; unused capacity is not automatically
  attributed to failure. Attempts without responses may still be in flight.
- SDK/server versions, broker codes, farm notices, original-pin receipts, symbol
  re-resolution and calendar contradictions remain visible.
- Owner-history readiness, execution/cost realism, historical issuer mapping,
  extraordinary-calendar freshness and scientific yield/power remain unverified.
  `strategy_trial_ready` is always false in this collector.

After the deadline, the first invocation holding both locks appends an immutable
seal with the actual record-construction time, report as-of clock, report digest,
prior sequence/hash watermark and calendar sequence/hash. The PC may have been off at the deadline;
that cannot extend the window. Final status replays the fixed prefix and verifies
the digest, so later status times and calendar notices cannot change the result.
This includes notices appended later with a backdated observation clock.
After verifying the seal and retaining its report/evidence, disable this collector's
own task; it never makes further broker requests after the deadline or seal.

## Failures and recovery

Broker logouts and market-data errors are expected and recorded. Reauthenticate
IBKR normally; retries resume without resetting evidence. Errors outside the journal
are logged to `logs/commissioning-v2.err.log`; status reports clock regression with
a distinct state. A changed implementation/dependency or replaced source ledger
stops the run, including custody. Do not edit manifests, move boundaries, remove
failed cases, or restore old timestamps to make a run pass.
The digest conservatively covers every Python module in the package, project and
lock files, source archives and runtime versions. Any package code edit ends the
run, including edits to modules loaded indirectly by package initializers.

If an implementation correction is necessary, stop only this collector's task,
verify its processes have exited, preserve the entire run and hashes, and deploy
the reviewed correction. A new future commissioning window requires an explicit
new activation after preserving the previous fixed-path directory as an archive.
Verify all archive paths remain inside the intended data directory before moving
anything. Never overwrite a journal or reset/reuse the old confirmatory trial.
Rollback of this release stops/removes only its new task; retain its evidence and
leave the existing live, SEC and research workers operational.

See [the execution plan](../sprints/RESEARCH-ACQUISITION-COMMISSIONING-PLAN.md)
for design-review findings, tests and the remaining broader research work.
