# Repair the volatility rehearsal's issuer identity

The live candidate ledger stores the filing routing CIK. The volatility
rehearsal needs the issuer CIK parsed from the ownership document. Among
75 eligible archive cases, all 75 had matching immutable approval records,
and 12 had different routing and issuer CIKs.

Read the immutable Form 4 approval copy in a short read-only snapshot
before the pre-open decision. Bind the configured database identity, exact
packet and decision clocks, normalized issuer symbol, issuer CIK and approval
content digests. The independent SEC directory and broker venue, contract
and historical-close requirements still apply. A missing or inconsistent
approval leaves the fixed-width fallback. Keep the existing candidate
selection, source pacing and two scheduled entry dates.

After exact-head review, pause only the hidden rehearsal task. A new v2
journal may supersede v1 only while the older journal has just its
activation record, the calendar and entry dates agree, and the candidate
inventory has not changed. Preserve the v1 database and heartbeat. Resume
the task after v2 binds the prior activation and source database. Verify
clean synced main, a fresh v2 heartbeat and successful live reconciliation.

Test an owner-routed approval linked to the true issuer, typed missingness
and immutable approval field validation, read-only access, cutoff behavior,
and refusal to supersede used evidence. This release addresses rehearsal
identity. The historical price and corporate-action qualification, final
preregistration and prospective study remain pending.
