# Settlement-calendar evidence for the E07 prospective replay

## Outcome

Bind regular-way U.S. equity settlement and cash-release dates into
2027 using original DTC and NSCC annual notices. Apply the extension
through the offline replay entry points. Keep the default Q4 2026
contract and both legacy binding digests stable. The prospective caller opts
into the new schedule and binds that choice before opening outcomes.

## Source and scope

DTC notice 25243-26 and NSCC notice a9854 were published November 9,
2026. The DTC table identifies seven full closures, two weekdays with
no settlement services, and three dates with limited settlement
services. The NSCC table directs participants to date-specific notices.
Treat March 26, June 18 and December 24 as unresolved for equity
settlement until service-specific evidence resolves them. Reject a
whole replay binding when the earliest potentially valid settlement for any
required session is unresolved.
The original PDF and provenance for each source belong in calendar-sources.

Preserve the separate NYSE session calendar. Use October 1, 2026 through
December 31, 2027 as the maximum coverage, with each release date also
inside that coverage. Keep both existing cash-release conventions. A
limited or unverified future date remains an explicit input failure.
This is a schedule projection. Broker reuse timing and the actual trial
sample require separate qualification.

## Verification and handoff

Challenge the design independently before implementation. Prove the
original digest and default coverage survive. Verify year rollover,
settlement dates separate from exchange dates, known 2027 closures, all
limited dates, the final boundary, and propagation through both replay
entry points. Keep live runtime and rehearsal fingerprints outside the
diff. Complete strict Python gates, exact-head review, merge and
installed verification. The prospective trial still requires the
Monday custody witness, input coverage, and a sealed preregistration.

The legacy digest fixture was evaluated directly against the original
settlement adapter retrieved from commit `e78f2ac` and the updated
default binder. Both release rules returned identical digests and maps.
