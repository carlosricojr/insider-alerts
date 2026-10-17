# MOC rejection diagnostics

## Outcome and constraints

Repeated closing-auction rejections currently discard the broker reason. Capture
order-correlated, bounded, account-redacted evidence in the existing exception and
append-only process error log. This does not establish the underlying cause or a
profit improvement. Preserve E07/F00, all order fields, OCA type 1, timing, retries,
cash mode, capital, and the existing 15:45 market fallback. No live test orders.

## Design and verification

- Read-only Claude design challenge completed before implementation: no proven
  execution fix without the reject reason; use raw order-matched errorEvent text,
  not trade/contract reprs. IBAsync 2.1.0 shares a monotonic request/order ID counter;
  connection setup clears the existing bounded error buffer.
- Keep the existing exception type/prefix. Record at most three matching errors;
  strip controls and redact identifiers before truncation. Missing evidence is
  explicit. Do not change reconciliation or collect unrelated request errors.
  Prefer over-redaction of account-like substrings even inside longer tokens;
  this can remove incidental diagnostic text but avoids leaking identifiers.
  Deliberately exclude unbounded advancedOrderRejectJson / trade.advancedError.
- Test rejection statuses, correlation, malformed/missing evidence, privacy,
  bounds, unchanged order fields/sleep/idempotency, accepted statuses, and fallback.
- Run ruff, strict mypy, full pytest, independent adversarial review, and canonical
  review-of-record on the settled SHA. Merge only with green applicable gates.

## Handoff

Local ruff, strict mypy, and full pytest passed. Independent review confirmed
unchanged order behavior and valid correlation; its truncation-boundary test gap
was corrected with an identifier explicitly straddling character 240. Canonical
PR review and deployment verification remain required. Deploy reviewed merged main; verify
fresh heartbeat, revision, reconciliation, invisible tasks, and clean synced main.
The next natural fast MOC rejection should supply the broker reason. Rejections
arriving after the existing 0.5-second check are outside this narrow change; no
claim of complete asynchronous rejection capture. Error log is durable even when
the next successful cycle clears last_cycle_error. Rollback through a reviewed
revert, without changing broker-held protection. No automatic strategy promotion.
