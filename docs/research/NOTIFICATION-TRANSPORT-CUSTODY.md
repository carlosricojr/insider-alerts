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
attempt ordering, orphan terminals, and unmatched starts.

The journal never stores the URL, topic, Authorization header, token, message, title, tags, raw
response, or exception text. It stores only packet/attempt identity, phase and timestamp, status,
provider ID/time, content and route digests, and reviewed runtime/policy provenance.

Journal writes use a 100 ms busy timeout. An unavailable or locked journal is fail-isolated and
recorded in `logs/notification-transport.err.log`; notification delivery proceeds unchanged. This
trade-off protects the operational alert path. Missing journal data remains missing and cannot be
retrospectively represented as observed.

Before activation, the notifier checks only for the journal file and does not resolve source
provenance. After activation, the loaded Git revision is resolved once per process and reused; it
does not spawn a subprocess for every alert.

## Deployment and operation

After the reviewed merge is present on clean synced `main`, seal the boundary before restarting or
waiting for the hidden autopilot to reload the new source:

```powershell
.\.venv\Scripts\python.exe -m insider_alerts.cli ops notification-journal-activate `
  --activation-at-utc 2026-08-28T12:34:56.000000Z
```

Validate without sending a notification:

```powershell
.\.venv\Scripts\python.exe -m insider_alerts.cli ops notification-journal-status
```

No new scheduled task or console process is installed. The existing hidden autopilot emits events
only when it already sends a review notification. Rollback restores the preceding reviewed source;
the immutable journal and activation record remain in place.
