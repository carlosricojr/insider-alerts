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
