# Status — 2026-11-15

**Phase:** the approved offline feasibility slice of merged isolation plan #75.
Implementation changes tests, their CI selection and a mapping/update-policy
proposal only. Full hosted Linux/Windows gates passed at 7828bdd; settled-head
independent/canonical review, matching-head checks and source merge remain due.

**Ball is with:** coordinator for current-head independent/canonical review,
applicable CI/readiness and authorized source merge. Carlos retains all
later trust, pinned-root exception, interim-posture, cutover and activation
choices. No such decision is enacted here; #66 remains BLOCKED.

**Next action:** review the settled offline result and proposals; retain actual
matching-head hosted checks before merge. Preserve negative feasibility results. The environment-only
trust variant is not proposed for admission: urllib can add trust sources beyond
the environment file. A passing fixture never certifies a frozen online writer
or an actual installed trust bundle as safe.

Initial hosted run43789522143 failed in the new TLS fixture. Corrections use a
positive rate/deterministic clock and isolate fresh urllib opener initialization
per trust profile, with restoration. A separate retained-context control keeps
the genuine inability to reload trust by environment mutation explicit. Initial
Linux14/Windows16 failures remain recorded. Corrected run43791145901 passed both
quality and Windows boundaries at7828bdd; no operational admission follows.

**Delivered for verification:**

- [Execution/results plan and mapping/pinned-root policy proposal](docs/deployment/2026-11-11-offline-isolation-feasibility.md).
- Cwd/module/Git-authority, missing-file and registry-versus-commissioning
  line-ending guard tests; additive actual Windows confinement cases.
- Four real client paths against owned loopback HTTPS with ephemeral synthetic
  cross-sign fixtures, strict X509 controls and real verification-error evidence.
  No keys, operational CA files or historical evidence are committed.
- All existing Linux full quality/coverage and Windows boundary checks retained.
  No suite or crypto fixture generation has run on the Mac for this slice;
  validation is hosted. Small syntax/source checks are not a passing full gate.

**Unchanged boundaries:** cohort/registry, sealed lock, production source,
receipt/armed/custody records, task/store mapping and trading controls. Actual
process trust and blinded Trial-integrity evidence remain incomplete. Carlos-PC
hold ACTIVE: zero host calls/probes/deployment/activation. Browser and runtime
deployment are N/A for this offline test/document slice; merged source and all
applicable actual hosted gates still require verification.
