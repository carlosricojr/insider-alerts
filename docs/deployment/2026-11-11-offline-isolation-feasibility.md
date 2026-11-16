# Offline environment isolation: execution plan and proposal

Updated: 2026-11-15. Base: merged plan f05ef61, PR #75. Carlos approved only
offline synthetic feasibility, proposed environment mapping and a proposed
pinned-root update policy on 2026-11-11. This document enacts none of them.
PR #66 remains blocked. Carlos-PC is under an incident hold; no host call,
deployment, trust change, task cutover, new cohort or record re-signing is part
of this slice.

## Outcome, constraints and verification

The [merged plan](2026-11-11-environment-isolation-plan.md) remains authoritative.
This is its step 1. Actual installed-process trust, blinded Trial-integrity,
custody and canonical-store evidence are still incomplete and held. Offline
fixtures can expose a proposal's failure without establishing operational safety.

The required read-only Claude design challenge requested revisions before
implementation. The tests address its cwd/Git distinction, differing line-ending
digests, strict X509/cross-sign controls, proxy/retry isolation and additive
urllib trust. Root/digest tests call metadata guards only; TLS tests call the listed low-level
clients against owned loopback servers. Neither invokes a Trial, terminal,
inference entry point, installer, live writer or broker.
Existing synthetic unit suites remain required CI gates; no study outcomes or
real historical records are read to obtain maintenance evidence.

| Boundary | Expected result | Evidence status |
| --- | --- | --- |
| Cwd versus module root | Complete cwd markers win; incomplete cwd falls back. Neither proves Git authority. | Passed at 7828bdd in hosted run43791145901. |
| Fake `.git` marker | Root selection can pass, but Git artifact admission must reject it. | Metadata-only rejection passed at 7828bdd. |
| LF versus CRLF lock | Registry-normalized digest remains equal; raw commissioning digest changes. | Passed on a disposable synthetic lock at 7828bdd. |
| Changed lock contents | Both digests change; missing bound files fail. | Rejection passed; real sealed lock unchanged. |
| Windows runtime path | Existing helper accepts an in-root file and rejects sibling, missing and directory paths. | Hosted Windows passed; previous junction tests retained. |
| SEC and ntfy HTTPX | Explicit good bundle accepts its leaf; both bad chains and wrong host fail verification. | Actual loopback client controls passed at 7828bdd. |
| urllib market context and prices | Good-only file plus an additional bad CA directory can still accept the synthetic bad chain. | Negative feasibility result demonstrated at 7828bdd. |
| Windows store enumeration | Labelled synthetic enumeration can add trust to urllib's default context. | Hosted Windows model passed; no real-store inventory/change. |
| Missing/malformed/empty bundle | Distinguish HTTPX explicit-file errors from default-path behavior; no generic exception is verification proof. | Positive/error controls passed; does not prove operational safety. |
| Already initialized urllib opener | A retained context can continue trusting the previous anchor after environment mutation. | Retained-context negative demonstrated at 7828bdd. |

Initial hosted run `43789522143` at `7521b45` failed: Linux recorded 14 failed,
1,341 passed and 21 skipped; Windows recorded 16 failed, 195 passed and 5 skipped.
All failures were in the new TLS module. The fixture supplied a zero rate to a
client that divides by it, preventing those TLS controls from running. Other
controls changed the CA environment after urllib had cached its HTTPS context,
so they did not test the newly selected profile. Those failures remain evidence;
they are not a passing trust result.

Corrected profile tests supply a valid positive rate with a deterministic clock
and forbidden sleeps. Each urllib profile initializes a fresh test-local opener,
restoring its predecessor afterward. This models initialization under that
profile, not a production patch or live trust reload. A separate retained-opener
control deliberately preserves the cache and demonstrates that an environment
change need not update a running client's trust when its context is retained.
Context lifetime differs across supported Python versions. The test observes
the actual HTTPS handler before the second request: a retained context must
keep its previous anchor; a handler creating fresh contexts must reject the bad
chain after the trust-profile change. Both branches require actual connections
and verification controls; neither is skipped. An additional hosted Python 3.11
TLS gate verifies compatibility with the declared minimum minor version, and CI
provenance records the observed context lifetime. The existing mapping therefore
remains unadmitted; no restart, trust update or runtime cutover follows.

Hosted run [43791145901](https://github.com/carlosricojr/insider-alerts/actions/runs/43791145901)
at exact source head `7828bddf66b3e2a30fa736250913b349c52cfd67` passed full Linux
Ruff/Mypy/pytest/coverage and Windows boundary jobs. These are fixture results,
not final merge or operational acceptance. A later head still needs its actual
applicable checks and reviews before merge. The environment-only trust variant
is **not proposed for admission**: urllib's default
context can combine other trust sources with an environment bundle. A virtualenv
split also cannot make a changed checkout lock satisfy sealed artifact checks.
Do not patch a guard or historical record to make either proposal succeed.

## Offline TLS fixture procedure and limits

Fixture setup uses the existing disposable CI image's OpenSSL executable with
fixed argument lists, finite command deadlines and bounded output. No Python
dependency or lock is added. It creates unrelated synthetic good/bad roots, one
synthetic intermediate cross-signed by two bad roots, and strictly formed leaves.
OpenSSL verifies each positive chain with `-x509_strict` before client tests.
Signing keys are removed before any client test; only ephemeral leaf keys remain
in the disposable temporary directory. No PEM or private key is committed.
Generator source, tool/Python/OpenSSL versions and LF-normalized public-fixture
hashes are recorded in the CI provenance summary. Different random fixture keys
are expected between isolated runs; their identity is never operational evidence.

Each negative has a positive control against the same synthetic server. Accepted
connections are counted before TLS verification, and actual verification codes
are observed while original errors/results propagate unchanged. Client proxies
are removed, `NO_PROXY=*` bypasses registry proxies, retries are one, and socket
and DNS guards reject every non-loopback destination. ntfy posts only synthetic
content to the owned loopback receiver. Market/price tests call low-level download
methods only; they do not start a research/backtest/market pipeline. Owned servers
have finite socket and shutdown waits. No OS certificate store is edited.

The cross-sign model does not identify a real compromised certificate, prove
all operational alternate paths rejected, or establish loaded trust of any
running process. Alpha/quant children are not executed. A future trust proposal
still needs the merged plan's identity, custody, per-client and G-TLS evidence.

Primary contracts: [HTTPX SSL configuration](https://www.python-httpx.org/advanced/ssl/),
[Python 3.12 default contexts](https://docs.python.org/3.12/library/ssl.html#ssl.create_default_context),
[Windows certificate enumeration](https://docs.python.org/3.12/library/ssl.html#ssl.enum_certificates).
[CPython HTTPS handler](https://github.com/python/cpython/blob/v3.12.10/Lib/urllib/request.py#L1264)
constructs and retains its context; the default opener is cached by `urlopen`.
[Python 3.11's handler](https://github.com/python/cpython/blob/v3.11.10/Lib/urllib/request.py#L1268)
can instead leave context creation to each connection. Runtime-specific fixture
results are not permission to change a live writer's effective trust.
Observed tests must be tied to the frozen installed versions, not inferred from
documentation for a different runtime.

## Proposed mapping: no task is moved

`S` means the preserved scientific checkout and its sealed cohort authority.
`R` means a proposed, separately reviewed patched operational checkout. These
are logical selectors, not installed paths or an approved runtime layout.
Observed task states are only the dated observations in the merged plan.

| Consumer or paired owners | Proposed environment | Admission conflict / retained authority |
| --- | --- | --- |
| Research capture/history, Trial, terminal/manual seal/inference | S | Same sealed bytes, activation ancestry and evidence authority; no maintenance execution to inspect outcomes. |
| Bar/session feeds and point-in-time feature capture | S pending producer admission | Preserve observation provenance, append-only sequence and single-writer ownership. |
| Commissioning V2 | Existing root and state | Whole-package/runtime digest and path/device/inode custody prohibit silently transplanting it. |
| SEC Ingestion Worker + SEC Ingestion Watchdog | R, only as a pair | Coordinate acquisition ownership with the Autopilot decision pair; no second writer or empty replacement store. |
| Autopilot Worker + Autopilot Watchdog | R, conditional | Source/decision/notification chain and alpha/quant child admission must remain compatible; do not start legacy disabled workers. |
| Live canary + watchdog, Observer, notification coverage | R together, conditional | Order-capable cutover needs its own approval, original ledger custody, one position manager and all E07/F00/broker gates. |
| Alpha/quant child processes | Their existing reviewed environments pending proof | Do not infer interpreter, trust or source compatibility from a parent virtualenv. |
| Development/CI | Disposable synthetic roots | No secrets, live store mounts, writers or study outcomes. |

Canonical existing stores are not relocated, copied, reset, shared through a
junction or reassigned by this proposal. The current path helper confines runtime
files to their own checkout. A proposed R cannot simply reuse out-of-root paths;
the table is **not admitted** until a later approved selector/store/custody design
satisfies the guards. Same-root consumer relationships and complete producer
version/source fields remain the merged plan's G-SRC/G-STORE gates. No new
receipt, armed attestation or policy exception is manufactured here.

## Pinned-root update policy: proposal only

| Change | Proposed rule, requiring later owner approval where applicable |
| --- | --- |
| Any of the eight registry-bound artifacts | Preserve the active cohort's bytes and sealed digests. A patched lock is not a compatible update merely because code imports. |
| Other Python, project/runtime/calendar or source identity | Treat commissioning/custody bindings separately; absence from the eight-artifact list is not permission to change it. |
| Line endings | Registry CRLF normalization does not waive raw commissioning binding. Never normalize and re-sign historical records. |
| Tests and documentation outside bound artifacts | Review normally in an isolated checkout; this does not establish that a live pinned checkout may diverge from `origin/main`. |
| Pinned S versus current main | Keep the existing clean-main rule in force. Any named pinned-root exception needs explicit scope, security/update process, evidence and approval; no automatic exception is enacted. |
| Security maintenance for an online scientific writer | A frozen environment is not certified safe by TLS success. The demonstrated environment-only narrowing is inadequate for urllib. Require blinded exposure/integrity and actual per-client trust/custody evidence before deciding interim posture. |
| Urgent incompatibility | Stop the proposed update/admission and present a separate reviewed resolution; never bypass validation, mutate the cohort or restart an armed worker. |

## Delivery and rollback

Run existing full Linux Ruff/Mypy/pytest/71% coverage and Windows boundary gates,
adding both new test modules without removing any previous check. Under the
current compute policy these run in already-authorized hosted CI, not on the Mac
or Carlos-PC. Update this result ledger with actual exact-head evidence before
final independent/canonical review and authorized source merge. A negative
feasibility result must be reported as such; it is not a gate waiver for PR #66.

This PR changes tests, their CI selection and this proposal only. No service,
CLI behavior or deployment target changes. Browser and host deployment are N/A
for this offline test/document slice; verify the merged source and hosted gates.
Rollback is a source revert of this slice; there is no runtime cutover to undo.
Later installer, trust, interim-posture and activation choices remain separate.
