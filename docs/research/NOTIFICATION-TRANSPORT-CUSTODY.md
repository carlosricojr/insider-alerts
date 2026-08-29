# Notification transport custody

This capture-only stream preserves the request and provider-response timing that cannot be
reconstructed after an alert. It is separate from the active `OPP-E07-V1` evidence snapshots and
cannot enter that trial's cohort, inference, or decision. It does not call IBKR or change the live
canary, notification payload, headers, retry policy, or capital.

## What the timestamps mean

For every existing ntfy HTTP attempt, the notifier emits one local `request_started` event
immediately before `POST` and one terminal event:

- `response_received` means the ntfy server returned HTTP. Its `id` and `time`, when valid, are a
  provider acceptance identity—not proof that a phone displayed or read the notification.
- `transport_failed` means the local HTTP attempt raised a transport exception. Only the exception
  class is retained.
- a persisted start without a terminal is `outcome_unknown_after_crash_or_observer_failure`.
- `client_received` remains unavailable because this installation has no instrumented subscriber.

These semantics follow ntfy's official [publish response](https://docs.ntfy.sh/publish/) and
[subscription API](https://docs.ntfy.sh/subscribe/api/). The service returns a publish message ID
and server timestamp, while client delivery requires a subscriber stream; there is no documented
end-device receipt acknowledgement in the integration used here.

## Custody and isolation

The first deployment seals a one-time activation timestamp plus the exact reviewed policy bytes in
`data/research/notification_transport.db`. Pre-activation callbacks are ignored and never backfilled.
Events are RFC 8785 canonical, SHA-256 bound, gap-free, and protected by update/delete triggers.
Status validation checks activation and policy custody, row/envelope binding, sequence integrity,
attempt ordering, orphan terminals, unmatched starts, and health metadata against the latest event.
The configured policy is confined beneath the reviewed `docs/research/contracts` directory;
absolute/traversal escapes and symlink or Windows reparse-point paths are rejected before reading.

The journal never stores the URL, topic, Authorization header, token, message, title, tags, raw
response, or exception text. It stores only packet/attempt identity, phase and timestamp, status,
provider ID/time, content and route digests, and reviewed runtime/policy provenance.

Journal writes use a 100 ms busy timeout. An unavailable or locked journal is fail-isolated and
recorded in `logs/notification-transport.err.log`; notification delivery proceeds unchanged. This
trade-off protects the operational alert path. Missing journal data remains missing and cannot be
retrospectively represented as observed.

## Coverage reconciliation

The operational source now appends a content-addressed `notification_delivery_acks` record in the
same SQLite transaction that sets `notification_sent_at`. It binds the exact decision digest,
transport ID, successful retry number, response time, request-body/route digests, and 2xx status.
If journal setup failed, the acknowledgement is still durable with a null transport ID so the
notification path remains fail-isolated and the missing custody becomes detectable.

`data/research/notification_coverage.db` seals a separate operational activation. The activation
fully materializes and closes a read-only source snapshot before opening the journal snapshot. It
stores item-level content digests and immutable `covered`/`missing` classifications for all visible
pre-boundary delivered rows. `covered` requires an exact atomic acknowledgement plus its exact
journal attempt. Older packet-level journal matches are deliberately insufficient, so unbindable
legacy rows—including the known Form 4/A observer failure—remain missing forever. A later resend
cannot reclassify them or create historical evidence.

Future membership uses the append-only acknowledgement sequence after the sealed watermark, never
`notification_sent_at` comparisons. This prevents a transaction that assigns its timestamp before
the boundary but commits afterward from escaping the monitor. Each future acknowledgement must
match one exact request/2xx-response attempt, including packet, body, route, retry, ordering, and
timestamps. Deterministic failures become append-only gap records. Locked, unreadable, or malformed
stores are reported as degraded and are never mislabeled as observed missingness. Baseline
missingness is visible but does not make future monitoring permanently unhealthy.

The monitor is capture-health-only. It imports neither broker nor trial code, cannot place orders,
cannot alter notification delivery, does not read outcomes, and cannot enter `OPP-E07-V1` evidence,
enrollment, inference, or decisions.

Before activation, the notifier checks only for the journal file and does not resolve source
provenance. After activation, the loaded Git revision is resolved at most once per process with a
one-second hidden-process timeout and reused. A one-shot manual notification can therefore pay one
bounded lookup; a long-running worker does not spawn a subprocess for every alert.

## Deployment and operation

After the reviewed merge is present on clean synced `main`, seal the boundary before restarting or
waiting for the hidden autopilot to reload the new source:

```powershell
.\.venv\Scripts\python.exe -m insider_alerts.cli ops notification-journal-activate `
  --activation-at-utc 2026-08-28T12:34:56.000000Z
```

Validate the transport journal without sending a notification:

```powershell
.\.venv\Scripts\python.exe -m insider_alerts.cli ops notification-journal-status
```

After the coverage implementation is merged and deployed, create the acknowledgement schema, wait
until autopilot has restarted on the deployed source fingerprint, seal the source-snapshot boundary,
and install the invisible monitor:

```powershell
.\.venv\Scripts\python.exe -m insider_alerts.research.notification_coverage_worker `
  --initialize-source-schema
.\.venv\Scripts\python.exe -m insider_alerts.cli ops notification-coverage-activate
.\ops\windows\install-notification-coverage-task.ps1 -Start
.\.venv\Scripts\python.exe -m insider_alerts.cli ops notification-coverage-status
```

The scheduled action is direct hidden `pythonw.exe`, runs once per minute with `IgnoreNew`, and
records durable freshness/error health. Strict status exits nonzero for missing activation, stale
execution, structural degradation, or any post-boundary gap. Rollback restores the preceding
reviewed source but does not delete the immutable journal, acknowledgement, baseline, or gap
records. The installer resolves the deployment checkout from `Insider Alerts Live Canary Worker`,
refuses any other worktree or a dirty/non-synced branch, and validates the sealed paths before task
registration. If the live configuration uses non-default database paths, pass the matching
`-SourceDatabase`, `-JournalDatabase`, and `-CoverageDatabase` values to the installer.
