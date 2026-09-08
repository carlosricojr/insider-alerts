# Trial sentinel recovery plan

Outcome: restore `OPP-E07-V1` collection after three SEC `issuerTradingSymbol=N/A` records crossed
the evidence boundary even though the frozen live-canary loader excludes them.

Constraints:

- Preserve every evidence snapshot and original `invalid` disposition byte-for-byte.
- Apply only the preregistered pre-terminal, no-outcome-access correction path.
- Do not change E07/F00, capital, broker behavior, registered sample targets, statistical tests,
  or decision gates. Elapsed entries remain missed; never backdate enrollment.
- Keep confirmatory failures fail-closed and nonconfirmatory diagnostics failure-isolated.

Implementation:

- Reuse one canonical base-cohort symbol normalizer in the live loader and trial importer.
- Add content-addressed, append-only disposition corrections with parent-record and manifest
  bindings; require zero stored outcomes, no terminal artifacts, and an explicit blindness
  attestation before correction.
- Preserve exact manifest bytes inside each correction record. Validate the original store and
  corrected store under the same transaction; reject unrelated faults and clocks before parents.
- This narrow one-correction-per-disposition path is for base-symbol exclusions only. It does
  not repair other invalidities or amend a correction. Zero stored outcomes is deliberately
  stricter than the preregistration's no-outcome-access requirement.
- Correct only the three reviewed Andalusian Credit Company records listed in the checked-in
  manifest, from effective `invalid` to `excluded`.
- Continue isolated diagnostic capture when the confirmatory runtime is degraded or invalid.

Verification and handoff:

- Focused tests cover sentinel exclusion, correction atomicity/idempotence/tamper rejection,
  terminal/outcome guards, effective counts, and diagnostic continuity.
- Run Ruff, MyPy, the full Pytest suite, an adversarial review, and the CodeRabbit review loop.
- Merge only a reviewed green PR, fast-forward deployment `main`, apply the reviewed manifest,
  and verify fresh healthy task heartbeats, clean stores, invisible processes, and
  `main == origin/main`.
- Stop the research worker and terminal coordinator during cutover; apply the correction before
  resuming them so read-only consumers never encounter the not-yet-created correction table.

Preflight (2026-09-07 ET; base `80f394f7d7dd456ffbba21b3f24f846283d49964`):

- Three manifest evidence/parent digests match production. Trial faults, stored challenger
  outcomes, receipts, pending terminal datasets, and decision reports are all zero.
- Seven imported candidates remain unresolved. Recovery must record elapsed entry dates as
  missed; the original three enrollments and all original evidence remain unchanged.
- After the operator's PC restart, IB Gateway recovered and live cycles report `account_ready`
  with no current cycle error. No broker change is part of this repair.
- Initial draft passed lint, strict typing, and the full suite. Independent read-only Claude
  challenge verified terminal locking and frozen loader behavior, and identified atomicity,
  manifest custody, fault reporting, migration sequencing, and missing-entry handoff gaps.
  Safeguard fixes are complete. Updated lint, strict typing, the full suite, and ten focused
  correction/exclusion tests pass. Final independent/CodeRabbit review and deployment remain pending.
- Independent review of the complete repair found the six core invariants safe. Its CLI error
  envelope finding is addressed with typed catches and regression tests. Its proposed addition
  to `activation.TRIAL_TABLES` is deferred: `activation.py` is digest-sealed by the active registry,
  and editing it invalidates the active trial. Existing parent disposition rows already make a
  corrected store nonempty; future activation governance belongs to a new registered trial.
- The importer now uses the same symbol admission/normalization as the frozen base loader,
  including its existing dot-to-dash and exchange-prefix handling. Existing candidate records and
  ranks are immutable. The manifest path's preregistration line pointer is contextual; the
  operative no-outcome-access correction authority is at lines 343-344.
