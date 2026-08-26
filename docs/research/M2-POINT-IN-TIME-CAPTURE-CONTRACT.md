# M2 point-in-time capture contract

## Boundary

This milestone captures future approved signals only. The `pending -> approve` database transition
atomically inserts one deterministic `insider-evidence-capture-v1` outbox job. Installing the schema
does not enqueue historical approvals. The frozen E07/F00 canary, its broker client, policy, capital,
and order path are unchanged.

The scheduled worker imports no broker execution module. It claims at most one job in a short
`BEGIN IMMEDIATE` transaction, releases the source database before external work, and invokes only
alpha-core's reviewed research-only option-surface executable on dedicated IBKR client 48.

## Timing and failure semantics

- Capture becomes eligible 20 seconds after the original decision timestamp so notification and
  canary context can settle; the original timestamp, not a retry time, anchors the ten-minute limit.
- A process timeout kills the complete child tree with no visible window. Retryable boundary errors
  receive at most three attempts before the original deadline. Unknown and invalid-artifact errors
  fail terminally.
- A terminal option failure produces explicit missing/error evidence; it never discards the signal.
  Evidence/database failures never execute in the canary process and cannot stop position handling.
- Claims use expiring leases. A restart after evidence append verifies and reuses the exact stored
  record rather than re-querying the market.

## Integrity

The evidence SQLite database is separate from operational canary state and uses WAL. Snapshot rows
and attempt rows reject update/delete. Option artifacts and complete RFC 8785 envelopes are
create-only and content-addressed. `record_sha256` covers the canonical envelope after removing only
that field; a second digest covers the exact stored bytes. Runtime JSON Schema validation plus
timestamp, registry-membership, draft-enrollment, and digest checks run before append.

Until M3/M4 are merged and a later activation record binds every required digest, snapshots remain
`pending_entry_selection`, classification remains `unpartitionable`, and all market/options fields
are exploratory capture-only data. This milestone contains no activation or outcome analysis.
