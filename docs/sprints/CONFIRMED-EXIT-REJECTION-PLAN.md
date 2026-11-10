# Confirmed timed-exit rejection isolation

Outcome: one positively rejected new timed exit cannot starve another independently
validated due exit. Any rejection still fails the cycle and prevents new entries.
Baseline: c791c4f. Frozen E07/F00, cash account, two $200 slots, capital, policy,
commissioning and existing broker gates remain unchanged. No measured P&L claim.

Design challenge: parent attempted read-only Claude; unavailable at spend limit.
Independent read-only adapter/runner audit completed before implementation. SDK
Cancelled can be synthesized for a refused modification; it is not rejection proof.
Only a unique NEW MOC/MKT submission with a post-submit 201, zero status/local/
queried executions, fresh complete positions/orders and unchanged protection can
produce the typed exception. Existing orders, target modifications and QTYFAIL
remain outside isolation. All uncertainty, transport and persistence failures abort.

Implementation: bounded adapter proof and fresh per-dispatch validation; durable
rejection records separate from accepted pointers; continue only that typed case;
bind accepted exits atomically, then aggregate-raise before entries.

Independent review reproduced stale OCA fields in the installed SDK's cached
Trade projection. The new proof therefore owns one bounded all-open-orders
request, records decoded openOrder and orderStatus callbacks, requires its exact
openOrderEnd/completeness/identity, and restores every original callback. Both raw
accepted status and every fresh zero-fill/full-remaining protective receipt are
required; SDK caches are conservative overlays. No cache rewrite or SDK fork.
Transmission is proven by fresh accepted status: transmit is not decoded by
openOrder. UNSET_DOUBLE is unavailable price evidence even though finite.

Verification: adversarial multi-position, identity/partial/fill/cancel/refresh/race,
deadline and cleanup tests; full Ruff, strict mypy and pytest; independent settled
review; canonical exact-head review of record and green CI. Parent reviews readiness
before merge/deployment. Deploy only reviewed merged main through canonical hidden
task; verify installed revision, fresh management/cycle heartbeat, reconciliation,
protection and clean main == origin/main. No live fault injection.

Primary references:
- https://www.interactivebrokers.com/docs/tws-api/doc/order-management/execution-details
- https://www.interactivebrokers.com/docs/tws-api/doc/order-management/order-status
- https://interactivebrokers.github.io/tws-api/message_codes.html

Read-only Gateway smoke verified fresh raw order/status receipt availability and
exact callback restoration; the temporary client disconnected. Native installed-
Wrapper regressions cover stale OCA/account/action/tif/contract/status, missing
fresh status, partial-then-zero receipt, missing/extra/end/overlap/callback drift,
capture failure and cancellation/timeout restoration.

Handoff: implementation complete; final exact-head validation/review pending.
No production checkout edits, merge or deployment before parent readiness check.
