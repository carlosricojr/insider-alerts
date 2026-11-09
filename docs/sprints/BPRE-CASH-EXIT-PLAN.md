# BPRE cash-account exit rejection

Live code201 after PR70 confirms projected margin insufficiency for the additional
OVERDUE SELL16 while the broker still holds16 BPRE and live STOP/TARGET16 OCA1.
The broker risk model rejects the intended close despite unchanged positive shares.
A what-if independently reproduced equity163.61 vs margin278.14. Do not add capital,
change account mode, weaken checks, or infer a profitable strategy change.

Investigate reusing the existing target order for the already-due market exit,
with exact account/client/qualified-contract/OCA/quantity ownership and fresh
regular-session bounds. Preserve STOP and OCA overfill protection; no extra sell.
IBKR modifying-orders reference supports same-id modification but discourages
changing fields other than price/size/TIF; a read-only independent design challenge
must decide whether type change is safe and how to fail on rejection/ambiguity.
Source: https://interactivebrokers.github.io/tws-api/modifying_orders.html

Required verification: broker adapter tests including same-order modification,
rejection, partial/foreign/identity conflicts, crashes and no duplicate sell;
full Python gates; independent adversarial review and canonical exact-head PR review.
Deploy only reviewed main with research collector disabled and original journals
preserved; verify actual BPRE fill, attribution, NPB protection, heartbeat and clean
synced deployment. Roll back reviewed code if necessary while retaining protections.

Design challenge rejected automatic LMT-to-MKT type conversion: broker support
and cached rejection handling are unproven. Use its preferred price-only same-ID
alternative. The existing due market liquidation accepts any available positive
price; a one-cent limit adds a positive floor. Retain order type, TIF, reference,
quantity, permanent/client/order identity and OCA. No new sell or cancellation.
Require one matching target and stop and strict fresh position/identity checks;
require observed price acknowledgement, and disconnect/reconcile on rejection or
timeout so SDK rejection status cannot trigger duplicate protection repair.
Only already-due market exit dispatch uses this path; MOC behavior stays intact.
This is execution repair under the user's explicit end-to-end BPRE fix request;
no new strategy trigger, sizing, capital or account-type change.

Handoff: implementing price-only route with adapter tests and settled review.
