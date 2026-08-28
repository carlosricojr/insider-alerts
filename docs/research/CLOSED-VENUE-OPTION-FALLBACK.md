# Closed-venue option evidence fallback

The prospective research capture worker always asks alpha-core for the richer live type-1
option surface first. When alpha-core explicitly reports that the option venue is closed, the
same worker immediately makes one historical request using the durable decision timestamp as
the information cutoff.

The fallback is research-only. It has no trading authority, cannot select an order, and cannot
backfill a signal after the fact. Its chain candidate set must come from the separately captured
option-chain feed at or before the decision cutoff. The resulting artifact is accepted only when
its fixed identity, request ID, symbol, cutoff, timestamps, pre-cutoff chain snapshot, pre-cutoff
bars, four-target result partition, digest, and pacing accounting satisfy the local contract.

Historical IBKR requests consume a shared fail-closed pacing ledger. For that reason, every
historical launch is a one-shot operation: a launch failure, non-zero exit, timeout, or invalid
artifact is recorded as terminal typed missingness and is never retried automatically. This
avoids double-spending pacing capacity after an ambiguous process failure. Ordinary transient
failures from the initial live request retain the existing bounded retry policy; only an explicit
closed-venue response enters the historical path.

Successful artifacts are published through the existing create-once, content-addressed option
artifact store. The evidence snapshot identifies the capture mode, exact cutoff, fixed target and
result counts, historical request count, pacing units, chain observation timestamp, and
underlying reference. No predictive claim or trial enrollment rule changes as a result of this
transport fallback.

The Windows task remains a direct hidden `pythonw.exe` action with `IgnoreNew` overlap behavior.
The installer confines the chain and pacing databases beneath `data/research`, rejects existing
database reparse points, and passes the reviewed alpha-core historical entrypoint explicitly.

Rollback is operationally simple: reinstall the preceding reviewed research-capture task action.
Already published evidence and pacing records remain immutable custody and must not be deleted.
