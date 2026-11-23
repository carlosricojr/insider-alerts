# Restore the default Form 4 RSS query

## Result and constraints

EDGAR's current filings endpoint now returns HTTP 422 for the negative
`start` in the stock query. The same declared client received 40 Form 4
entries with `start=0`. The existing reader accepted every entry. The
response with `start` omitted reported the same accession, CIK and form
type set. Keep the configured explicit URI authoritative and update the
stock default and example.

The deployment has an explicit `SEC_RSS_URL` setting in its ignored
dotenv. A default change alone will not restore that worker. The local
production setting requires a separately approved cutover. The rehearsal
pins all Python package bytes, so deploying this patch before rehearsal
closure would invalidate that rehearsal.

The first recovered RSS page was entirely from the prior Friday and the
production queue still had eight older eligible enrichment inputs. A
new approval can be interpreted by the live consumer at its new approval
time. Require an approved cutover that excludes stale source rows and
unprocessed backlog from the live path. Preserve the outage as a gap.
Historical recovery from the master index is a separate research action.

## Verification and handoff

Check the default and custom override with local pytest. Check the
actual first-page response through the existing dry-run parser without
writing any production filing or approval. Run the full local Python
gate and an independent review. Keep this branch isolated until the
rehearsal and operator cutover constraints are satisfied.

## Forward-only recovery option

Configure an explicit timezone-aware UTC acceptance boundary only for an
approved recovery. Apply the same lower bound and a current observation
upper bound to fresh RSS admissions, the XML and review queues and the
automated pending-packet read. Evaluate each queue predicate before its
limit and use the exact original RSS accession, CIK and form type to
qualify pending packets. An unconfigured cutoff preserves normal
behavior. Historical rows, outstanding approvals and notification
receipts stay in their existing custody. Normal automated processing in
recovery scope consumes only qualifying live RSS filings.

Use one datetime comparison for memory and SQLite. Fail closed on a
missing, malformed or naive filing clock. Bound the queued records
by the local insertion clock's one-second precision and the current
observation. Preserve acceptance microseconds and offsets. Dates alone
allow known same-day pre-boundary rows. Report
cutoff exclusions separately from parser errors. Tests must cover prior
day, same-day, exact boundary, offsets, future records and preexisting
pending input. Prove that excluded rows cannot exhaust selection limits.

No production deployment is authorized by this isolated preparation.
Keep the frozen rehearsal binding and deployment checkout intact.
Before any later approved rollout, verify both the SEC ingestion and
automatic review processes load the same settings. The corrected URL
must replace the explicit production override at that point. Keep the
configured lower bound in effect so old rows cannot reappear by restart
or token refresh. Verify the cutoff logged by each worker. The fixed
boundary excludes records from the initial outage. For a later outage,
measure its source gap and establish a new approved forward boundary
during recovery before re-enabling automatic review.
