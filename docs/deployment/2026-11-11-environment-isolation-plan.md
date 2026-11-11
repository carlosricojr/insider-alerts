# Research/runtime environment isolation plan — 2026-11-11

Status: proposal for independent design review. Planning is authorized; runtime
activation, a trust-store override and a pinned research-checkout exception are
not approved by this document. Certifi PR #66 remains blocked.

## Outcome and boundaries

Preserve the current OPP-E07-V1 cohort and its sealed records while defining a
separately maintained runtime environment. A successful plan identifies every
writer and inference consumer, its source/interpreter/lock/store identity, the
safe network boundary, executable validation, and a rollback that preserves new
valid appends. It does not declare a scientific transition or restart a service.

Do not edit the active registry, preregistration, schemas, inference/terminal/
activation artifacts, frozen policy, dependency-lock digest, receipts or armed
attestation to make an updated lock pass. No outcome inspection, re-signing,
retrospective enrollment, new cohort, promotion, capital/account/order-policy
change or weakening of a validator/test is in scope. E07/F00, cash-account mode,
two nominal $200 slots and broker safety gates remain as governed by AGENTS.md.

## Evidence and present binding

Source basis: fetched main c791c4f3c4d7256bac4905271c702d66ce93091d. PR #66
changes only uv.lock, its new Certifi regression, deployment note and STATUS;
the consumer source inspected here is identical to that source review.

Read-only Windows task metadata at 2026-11-11T17:50Z found 16 Insider Alerts
tasks sharing C:\Users\carlo\Repositories\insider-alerts. That checkout was
clean main at c791c4f, equal to its origin/main and the fetched default. The live
canary, Autopilot Worker, SEC Ingestion Worker and Notification Coverage were
Running; no worker was stopped or invoked. Legacy Autopilot and Commissioning
V2 were Disabled. The autopilot action also names an alpha-core child in
`alpha-core-worktrees\insider-evidence-surface-runtime`; this is a separate
interpreter/source boundary, not proof that all execution uses one virtualenv.
Task Scheduler result 267009 denotes a running task, not a failed exit. Research
Trial's last result was 2. A read-only, error-code-only log tail at
2026-11-11T18:02Z reports candidate-runtime `invalid_evidence_dispositions` and
`faults`; no counts, outcomes or scientific records were opened. The underlying
integrity fault and fresh blinded health remain unresolved. G-TRIAL below is a
hard gate before an activation decision. Do not enable Commissioning V2.
Task metadata is not proof of fresh heartbeats or the environment of every
already-running process.

Windows metadata at 2026-11-11T17:52Z found installed Certifi 2026.1.4. CRLF-to-LF
normalization of its checkout uv.lock gives the sealed registry's exact digest:
`a22e35daa8d899db6533ed2f6a8500f2ab85ec899c1f99ebb95be8fdd4a5b0ed`.
PR #66's proposed lock instead gives
`49ab25e7f3eee0aa5632a56f00b7d175ef82b92af3ca68dac01ba6d901d6f723`.
User/machine SSL_CERT_FILE, SSL_CERT_DIR, REQUESTS_CA_BUNDLE and HTTPS_PROXY were
absent. These reads do not establish per-process inherited/task-local settings,
the actual loaded trust bundle, or current TLS safety. Raw task argument hashes,
paths and local receipts are retained outside Git; no secret/config contents or
scientific outcomes are copied into this plan.

The validator searches cwd before module ancestors for an authoritative Git
checkout, then verifies bound artifacts in both that checkout and the activation
commit. A different virtualenv with a patched checkout lock still fails. Later
commits are allowed only with activation ancestry and identical bound artifacts.
See [inference.py](../../src/insider_alerts/research/inference.py),
`_reviewed_repo_root` / `_validate_registry`, and
[the inference contract](../research/INFERENCE-EXECUTABLE.md).
Commissioning additionally binds the entire Python package, lock, project file,
calendar implementation/runtime and source path/device/inode; copying its store
or root is not automatically valid. See
[commissioning_worker.py](../../src/insider_alerts/research/commissioning_worker.py),
`implementation_files`, `implementation_digest` and `verify_source`.

## Consumer/environment matrix

Placement below is conditional, not an installed split. All observed Insider
tasks currently use the shared root. Child alpha/quant environments and loaded
process state must be inventoried independently before approval.

| Consumer | Writes / external boundary | Required binding and proposed placement |
| --- | --- | --- |
| Research evidence capture / history | Snapshots, queues; alpha/IBKR and SEC HTTPS | Frozen checkout/lock, registry/receipt/armed custody and child provenance; retain original scientific root. |
| Research trial | Trial/diagnostic/bar requests; local stores | Strict registry artifact and activation checks; original scientific root. |
| Terminal coordinator / manual seal and inference | Reconciliation, seals and terminal outputs; local stores | Original bound checkout/ancestry and multi-store custody; never execute to inspect outcomes for maintenance. |
| Bar and session feeds | Append-only observations; IBKR | Preserve feed identity and provenance; proposed scientific root until admission proves otherwise. |
| Point-in-time features | Capture-only features; SEC HTTPS | Distinct feature policy/activation/source admission; retain root and do not backfill missing captures. |
| Commissioning V2 | Custody journal; IBKR/source stores | Whole implementation, runtime and source inode/path binding; leave current Disabled state and original root untouched. |
| SEC Ingestion Worker + SEC Ingestion Watchdog | Source discovery/acquisition and durable stage progress; SEC HTTPS | Explicit patched-runtime candidate; one acquisition owner, coordinated with the decision pair. |
| Autopilot Worker + Autopilot Watchdog | Source admissions, decisions, notification transport; SEC/ntfy, alpha/quant children | Patched runtime candidate only after original-root chain admission and producer identity are proven compatible; leave legacy Autopilot Disabled. |
| Live canary + watchdog | Order-capable worker and ledger; IBKR/ntfy | Preserve E07/F00/broker gates; candidate runtime must remain clean synced main. No armed restart as a test. |
| Observer | Operational observations | Same-root relationship with live canary must remain valid; proposed patched runtime. |
| Notification coverage | Transport/coverage evidence | Same-root producer/canary arguments and custody are bound; proposed runtime is conditional on this complete chain. |
| CI / development probes | Synthetic fixtures only | Independent frozen-contract and patched-runtime gates; no real writers, orders, messages or terminal inference. |

Source launchers are under [ops/windows](../../ops/windows), notably the
`install-research-*-task.ps1`, `research-capture-task-action.ps1`,
`install-feature-capture-task.ps1`, `install-commissioning-task.ps1`,
`install-autopilot-task.ps1`, `install-live-canary-task.ps1`,
`install-observer-task.ps1` and `install-notification-coverage-task.ps1` files.
[research-path-validation.ps1](../../ops/windows/research-path-validation.ps1)
rejects escaped/reparse runtime and store paths. A symlink/alias or copied
database is not a substitute for the original identity. These are specific
contracts: Assert-ResearchRuntimePath confines executables/scripts to their
configured checkout, while Initialize-ResearchDatabaseParent confines option
databases to ResearchRoot. They do not prove that every database is confined
by that same function. Observer requires the canary checkout; coverage requires
its canary and producer checkout and accepts explicit resolved database paths.
Map each actual validator and database independently; do not broaden any path
check merely to make a split pass.

## Store topology and source-custody gates

G-STORE keeps the existing scientific stores and their path/device/inode identities
at the original root. Operational source/canary/observer/transport stores also
remain their single existing authority unless a separately reviewed migration is
explicitly authorized. No second store, symlink escape or stale snapshot copy
makes the split valid. The patched runtime must reach each existing store through
its applicable validated interface; if current confinement/same-root rules reject
that topology, implementation must preserve their security contract or reject A.

G-SRC must enumerate every emitted provenance field, not just the checkout lock:

| Record / field | Current producer and proof required |
| --- | --- |
| Evidence payload.versions.git_commit, source_fingerprint_sha256, policy_sha256 | capture.py _append_snapshot: configured Insider commit, whole-package research_source_fingerprint and capture-window policy. Verify consumer validation/ancestry and exact producer before/after; no silent provenance substitution. |
| Evidence versions.classifier_version, model_id, prompt_sha256, configuration_sha256 | Capture joins history and upstream decision payload; _configuration_sha includes operational values and research/alpha script, interpreter, DB and policy paths. Enumerate every upstream writer and hashed config component. |
| Source filing/decision/notification provenance entering observations | SEC acquisition and decision/transport producers; trace source admission, configuration fingerprints and sequence ownership through capture. Moving a producer cannot silently change enrolled evidence. |
| Registry/activation/armed receipts and artifact/lock digests | activation.py, inference.py and trial_runtime.py; retain exact receipt bytes and activation ancestry. Inventory whether launcher/env changes are bound before proposing one. |
| Commissioning implementation_sha256 and source identity | commissioning_worker.py hashes all package Python, project/lock/calendar/runtime and verifies path/device/inode; no relocation or reactivation. |
| Coverage runtime_git_commit, producer args/config fingerprints and custody | notification_coverage_worker.py and its installer; preserve same-root producer/canary relationship, effective source/journal paths and existing sealed transport/coverage policy. |

This is the initial source inventory, not a claim of exhaustive loaded-process
proof. Before implementation admission, resolve every field to its direct and
upstream writer, its accepted-value rule and its task/store identity. Reject a
candidate that requires altering a sealed value, reinterpretation or validators.
A changed runtime commit is not automatically forbidden or accepted: verify the
field's actual contract. Never rewrite historical provenance to match a new root.

## Candidate A: preserve the scientific root; separate patched runtime

Recommended direction for further design, conditional on every gate below:

1. Leave the authoritative scientific checkout, bound artifacts, store paths and
   records in place. Record its reviewed pinned source and exact dependencies.
   Define an explicit reviewed exception to the current clean-synced-main
   convention for scientific tasks only; do not silently change that policy.
2. Establish one clean synced-main runtime checkout/interpreter for the
   operational canary/autopilot/observer/coverage family. No second scientific
   writer/store authority, copied cohort or schema migration is introduced.
3. Resolve producer, child-process, same-root, commissioning and durable source
   admission constraints before selecting task mappings. Keep each old mapping
   until its replacement passes synthetic admission and independent review.
   If a valid split requires changing sealed bytes or bypassing a validator,
   reject this candidate under the current authorization.
4. The proposed destination of the live canary is the patched runtime, not the
   scientific root. That is an order-capable operational handoff, not a
   research-only pause. G-OPS must prove ledger/store identity, watchdog fencing,
   exactly one active position manager and uninterrupted existing management
   without an armed restart used as a maintenance test. No installer/cutover is
   authorized by this document. If the handoff cannot meet the existing controls,
   retain the current operational mapping and reject activation. Do not place,
   cancel or liquidate orders to manufacture a maintenance window.
5. Establish safe HTTPS for any continuing frozen-environment online writer.
   Do not assume freezing an interpreter makes ongoing research offline.
   No installer, worker restart or trust mutation follows from this plan.

G-POLICY requires a separately approved, explicit scientific-root exception:
clean pinned merged source and exact bound artifacts replace main==origin/main
for those tasks only; the operational canary still requires clean synced main.
Specify how this differs from deploy-only-origin/main and release cleanliness
checks. No source-fingerprint restart deploys an exception. The pinned root must
have an owner and vulnerability/update review: later reviewed commits require
activation ancestry, identical bound artifacts and all relevant custody checks.
If a security fix cannot meet them, fail closed/seek an approved research-only
posture rather than silently freeze online risk forever or amend the cohort.
This proposal changes no governing policy itself.

The minimum implementation change surface is the Windows task installers and
path validation plus deployment/runbook and synthetic CI/test contracts. Changes
to CLI source/child admission may be required but are not pre-approved. Any
change touching bound Python/package content must prove it does not invalidate
the frozen consumer; do not update its digest in response to a failed test.

## Alternative B: offline frozen inference with patched acquisition

This is a larger, conditional interface design, not a shortcut. It would require
proving that patched acquisition can hand off the exact timely admitted evidence
to the original frozen inference/store authority without changing scientific
writer bytes, custody, provenance, sequence or source identity. Current capture
and trial still enforce the checkout lock. If the handoff needs a bound-artifact
change or a new scientific transition, it is outside this cycle and rejected.
No bridge, queue, new cohort or retrospective enrollment is implemented here.

## TLS/security admission

Old default Certifi is not certified safe for online use. The upstream
[GHSA-6q48-6c7w-9gw2 advisory](https://github.com/certifi/python-certifi/security/advisories/GHSA-6q48-6c7w-9gw2),
published 2026-10-20 with fixed boundary 2026.10.18, and
[Certifi's 2026.10.19–2026.10.23 change](https://github.com/certifi/python-certifi/compare/2026.10.19...2026.10.23)
support the scoped removal. [PR #66's exact regression](https://github.com/carlosricojr/insider-alerts/blob/54e3818aedf28c975d919033c726d196e62192fc/tests/test_certifi_trust_store.py)
checks emSign ECC Root CA-G3, Root CA-C1 and ECC Root CA-C3 DER fingerprints.
G-TLS requires excluding that exact upstream-bound set in every effective loaded
trust path (including OS stores), not merely rejecting a generic untrusted cert.
No compromised key or live interception test is used. Successful
HTTPS/TLS handshakes only demonstrate compatibility, not distrust enforcement.

[SEC HTTPX](../../src/insider_alerts/sec/client.py) and
[ntfy HTTPX](../../src/insider_alerts/notify/ntfy.py) clients do not supply an
explicit verify/trust_env override. The installed HTTPX implementation checks
SSL_CERT_FILE/SSL_CERT_DIR, otherwise Certifi; urllib clients use Python's
default context. Alpha/quant child processes have separate trust surfaces.

G-TLS client inventory, to complete per actual process before admission:

| Client/process boundary | Trust source to verify | Required proof/control |
| --- | --- | --- |
| SEC HTTPX parent/history/features/ingestion | trust_env selection and effective Certifi/env context | Loaded bundle/context identity; exact removed-root exclusion; offline approved-chain and untrusted/hostname/invalid-chain negatives. |
| ntfy HTTPX operational producer/canary | Same library, separate process/task environment | Same distrust controls; stub sender only, no real notification. |
| urllib review/market_context.py | Python default SSL context and effective environment/OS roots | Identify loaded stores and exact exclusions; offline request fixture. |
| urllib backtest/prices.py | Its own Python default context | Same proof, without live price requests or backtests. |
| alpha child interpreter/scripts | Independently installed libraries, task args and env inheritance | Enumerate actual HTTPS clients/stores and loaded bundle; synthetic child/context tests, no broker/order call. |
| quant child interpreter/scripts | Independent child source and environment | Same inventory and distrust proof; no inference/model/network execution. |

Any task wrapper/environment proposal must first prove whether launcher bytes,
arguments, env selection or source/config fingerprints are receipt/armed/custody
bound. A change to a bound surface is not made safe by leaving uv.lock unchanged.

A task-scoped immutable vetted CA file is only a proposal for security and
scientific review: it preserves lock bytes but changes the effective environment.
It requires an external manifest with exact provenance/hash, confinement and
fail-closed preflight, and negative TLS tests for every actual HTTPX/urllib/child
client. Missing/tampered file, untrusted root, hostname mismatch and invalid chain
must fail; approved chains must pass. Prove the target process actually loads it.
Never disable verification, modify Certifi in place, restore distrusted roots
online or count a successful handshake as security clearance.

If no admissible trust boundary can be established while preserving the cohort,
the owner must decide a research-only collection pause and prospective missingness
handling. No pause is executed here, no past record is repaired and existing
position management must not be stopped. A pause is not itself scientific proof
that the cohort remains valid; follow its existing integrity/deadline rules.

## Interim posture and blinded health

Current observed workers remain unchanged; that is an observation, not a security
approval or a promise of healthy scientific collection. The maintenance agent
has not paused, restarted or changed trust for any process. SEC history/features,
operational SEC ingestion, ntfy and urllib/child clients require distinct exposure
mapping; a research-only pause must not stop operational discovery, canary
position management or notifications merely because they share HTTP libraries.

G-TRIAL blocks activation until blinded error/health diagnostics explain result 2
and its invalid_evidence_dispositions/faults condition under existing integrity
rules. Read only error codes/heartbeat/integrity metadata; never execute the trial,
terminal coordinator, outcome finalizer, inference or query return/outcome rows.
Do not fix a failed integrity gate by editing records, enrolling missed signals,
re-signing custody or changing code hashes. An unresolved fault cannot support an
interim choice that assumes collection is valid.

Carlos must explicitly choose a dated interim posture after the bounded exposure
and blinded-health evidence is reviewed. Recommended candidate: an approved,
precisely isolated pause of affected research-only HTTPS writers under existing
missingness rules if that can preserve operational management. Alternative:
explicit time-bounded continuation with the known trust uncertainty and a named
review date. Neither is executed by this plan; no elapsed time is approval and no
risk acceptance can waive scientific or TLS acceptance gates. If no safe isolated
pause is possible, return that exact conflict rather than stop shared live tasks.

## Ordered PR sequence and stop conditions

| Step | Inputs and deliverable | Pass / stop rule |
| --- | --- | --- |
| 0: read-only inventory | Current task/process/store/child mapping, effective trust proof and G-TRIAL/G-SRC field ledger; no live worker invocation | Every consumer and bound field accounted for; unknown or unsafe-to-read evidence remains a blocker. No raw secrets/outcomes in reports. |
| 1: offline feasibility | Synthetic mixed-root/cwd/import/lock/path and per-client TLS fixtures plus explicit proposed task/store mapping | Preserve all existing rejection guards and sealed bytes; reject A/B if success requires bypass or new scientific transition. Owner architecture permission is distinct from activation. |
| 2: bounded installer/launcher implementation | Approved mapping and policy exception, demonstrated feasibility, current-head tests/reviews | Only required selectors, confinement and fail-closed manifest controls; any new custody/operational conflict stops this PR. No installation merely to test. |
| 3: rehearsed runbook and activation proposal | Reviewed implementation, synthetic cutover/rollback rehearsal, current health and exposure evidence | Independently verify G-OPS/G-STORE/G-TLS/G-TRIAL and rollback; Carlos then receives the exact activation/window decision. No armed maintenance restart. |

Each is a separate objective reviewed before use; step 1 is the next smallest
implementation proposal after plan/architecture review, not authorization to
start step 2 or activate anything. A negative gate retains the current mapping
and blocked PR #66; it never mutates the cohort to fit the implementation.

## Verification and staged delivery

1. Complete read-only installed-process/module/trust and absolute store/custody
   mapping. Check blinded Trial result 2 diagnostics without outcomes, and source
   child admission. Do not infer health from task state or copy an active DB.
2. Obtain the repo-required available Claude design challenge and independent
   adversarial review of this plan. Record availability/failure honestly.
   Obtain architecture permission for the concrete offline feasibility/mapping
   slice before implementation; reserve evidence-dependent trust/activation and
   operational-window decisions until the implementation/rollback is reviewable.
3. Add synthetic tests for mixed cwd/import/interpreter/root/lock rejection and
   the intended split's success. Retain changed-lock rejection, registry,
   receipt/armed, no-retry, concurrency, Windows confinement/hidden-process and
   source identity tests. Add per-client offline TLS negatives/positive controls.
4. Run pinned Linux full quality/coverage and Windows boundary gates for an
   implementation. For this plan-only PR, retain structural/link checks and
   actual repo check results; trusted CI excludes documentation paths by design.
   The unchanged-base Mac run on 2026-11-11 passed Ruff/Mypy/coverage but failed 35
   capture cases (1,284 passed, 13 skipped). A synthetic fixture records the code
   failing closed because Linux /proc/self/fd handle-relative access is absent.
   This is not a passing full suite. Existing Docker/Colima engines were
   unavailable; no new VM/service was started to disguise that limit. Current
   supported-platform gates or a legitimate documented doc-applicability
   procedure must be established before merge; no new waiver is granted here.
   Do not weaken a workflow or silently label a failing required gate passed.
5. After separate implementation review/merge and owner activation decision,
   snapshot task XML/args/principals, package/import/CA provenance and bound
   record hashes, locks, source inode/path, store/WAL and backup rules. Establish
   single-writer quiescence through the approved per-store protocol: fence its
   watchdog and old writer, verify both inactive, perform blinded read-only
   sequence/hash-chain/WAL-aware integrity checks, start the admitted replacement
   only once, and verify continuity plus its first valid append. On failure,
   restore only an accepted compatible mapping, never an old DB snapshot.
   This is a proposed research protocol, not permission to stop any task now.
   Separately, G-OPS handles the operational canary and four-task acquisition/
   decision handoff. Verify acquisition first, then decisions, with no duplicate
   acquisition owner, durable admission replay or gap hidden by catch-up.
   Preserve canary position management and all safety controls.
6. Verify exact reviewed task/source/environment and trust mapping, unchanged
   sealed records, valid future append continuity, hidden fresh heartbeats,
   no cycle errors and order-free broker reconciliation. Do not run an armed
   worker or terminal decision for maintenance acceptance. No live change is
   inferred from this document's merge.

SQLite/WAL/checkpoint activity and valid appends can change DB container bytes;
verify sealed record identity and custody rather than require a forever-static
container hash. Keep one terminal authority. No late enrollment repairs a gap.

## Rollback

Before implementation activation, retain independently verified task/config/env
mapping and a reviewed compatible fallback. A research-only controlled pause may
be needed to prevent overlap, but requires the explicit activation decision.
Restore a compatible mapping; never overwrite an active store with an older DB
snapshot, delete/replay admissions or re-sign a cohort. Rollback cannot restore
distrusted roots online: isolate/fail closed an affected HTTPS path and preserve
the genuine collection gap. If safe rollback or current-cohort continuity cannot
be proved, do not activate the split.

## Carlos decisions after design review

| Decision | Recommendation / alternative | Impact and exact missing input |
| --- | --- | --- |
| Architecture permission before implementation | Candidate A first; B only with unchanged-contract handoff | Approve or reject the bounded offline feasibility/mapping slice and explicit pinned-root policy proposal. Final runtime mapping/activation is not approved at this stage. |
| Interim posture | Isolated affected research-only HTTPS pause if safely separable; alternative explicit time-bounded continuation | Choose posture, owner and review/expiry date after blinded G-TRIAL/exposure evidence. No shared operational task or position manager is paused by this plan. |
| Frozen online trust | Require separately reviewed per-client distrust evidence before any vetted-CA proposal is admitted | Decide scientific admissibility only after that evidence; if it cannot pass, choose a research-only pause under existing missingness rules. No effective-trust change is currently authorized. |
| Later activation / continuity | Separate reviewed scientific and operational handoffs, one writer/terminal authority and one position manager | After concrete implementation, select the exact mapping, research pause/window and safe operational handoff with rollback. If continued management without an armed maintenance-test restart cannot be proved, reject activation. No policy/order/capital or new-cohort change is requested. |

## Evidence index

The following receipts are local operational evidence outside Git, not portable
repository artifacts. Their SHA256 pins let a reviewer request the exact captured
bytes; raw task arguments/secrets/scientific outcomes are not published. Source
citations above resolve against base c791c4f; proposed deployment mapping is not
installed. JSON stdout inside each remote receipt is decoded once for selectors.

| Receipt under .maintenance | SHA256 | Claim/selector |
| --- | --- | --- |
| `evidence/insider-isolation-plan-2026-11-11/windows-task-environment-map.json` | `f22538302da25f0935d9e82bce4783ee04d2d7984893f659717055df8b1a1660` | stdout.tasks / roots; task states, paths and checkout identity |
| `evidence/insider-isolation-plan-2026-11-11/windows-lock-and-trust-metadata.json` | `27272775abab0d937c59dc6978d14d48341e5e2f9cb638e837fb275237f35c2a` | stdout.normalized_lock_sha256 / registry_lock / certifi_metadata_version / trust_env |
| `evidence/insider-isolation-plan-2026-11-11/windows-trial-blinded-diagnostic.json` | `a1177075acec4af67712353a98aedd2baa638666f00d8cfb187f84aff4f3b3fb` | stdout.errors code-only tail; no outcomes or fault counts |
| `results/insider-isolation-claude-design.json` | `e79a67002f108f615db1542425d4ca4a3b79b803402ee6300c832da473a5a2ed` | Required available-Claude design challenge; revisions addressed, not activation approval |
| `results/insider-isolation-sol-design.json` | `7fb5554c4c6ad22a8002e9fca0b705898a462049cdd5100818a8d9aaa245c358` | Independent source/scientific/security design assessment; revisions addressed |
| `evidence/insider-isolation-plan-2026-11-11/quality-progress.json` | `a5e83e7a884541eef65f4e5c94dd470a9bc474093c21762b4c5736ecd4c3bc78` | Unchanged-base actual pinned quality exits; no failed gate relabeled passed |

Artifact hashing for the lock uses only raw CRLF-pair replacement with LF, as
[inference.py _artifact_sha256](../../src/insider_alerts/research/inference.py)
requires; no other text transform is permitted.

Planning completion requires a reviewed durable plan and explicit unresolved
gates. PR #66 is not made merge-ready by planning, a virtualenv split, docs CI
exclusion or owner approval of this direction. Activation remains a later,
concrete and reviewable decision.
