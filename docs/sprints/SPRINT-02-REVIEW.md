# Sprint 2 Review — SEC RSS Ingestion (Form 4)

Status: **READY FOR IMPLEMENTATION (after checklist sign-off)**

## 1) Sprint objective
Build a production-safe ingestion layer that polls SEC EDGAR latest Form 4 RSS, normalizes references into canonical `FilingRef` records, and stores them idempotently with full provenance.

This sprint is the trusted-data foundation for all later stages (XML fetch, parsing, scoring, review queue, notification).

## 2) Exact fit for our use case
Our use case is **skeptical signal intake** where SEC filings are the source of truth and Reddit/manual claims are untrusted. Sprint 2 must therefore optimize for:

1. **Reliable capture of new Form 4 filings** without duplicates.
2. **Compliance posture** (SEC fair-access friendly behavior).
3. **Auditability** so each downstream signal can point to exact filing provenance.
4. **Deterministic behavior** under feed changes and transient failures.

## 3) In scope
- `sec/client.py`: shared SEC HTTP client with:
  - Explicit User-Agent from config
  - `Accept-Encoding: gzip, deflate`
  - Request rate limiting (target <= 5 rps default; hard max 10 rps)
  - Retry/backoff + jitter on 403/429/5xx
  - Timeout + typed error handling
- `sec/rss.py`:
  - Poll RSS endpoint for Form 4
  - Parse entries into canonical `FilingRef` candidates
  - Graceful tolerance for feed schema drift/missing fields
- Storage/idempotency:
  - Persist `FilingRef` records with uniqueness constraints
  - Stable dedupe key (`accession_number` + `cik` + `form_type`)
  - Raw RSS entry retained for provenance
- CLI integration:
  - `insider-alerts sec poll --once --max-items --dry-run`
- Observability:
  - Structured logs for counts + failure reasons

## 4) Out of scope (must not slip in)
- Filing index fetch / XML retrieval (Sprint 3)
- Form 4 XML parsing and transaction classification (Sprint 4)
- Signal scoring and packet generation (Sprint 5)
- OpenClaw decision merge/notify pipeline (Sprint 6)

## 5) Data contract for this sprint
Minimum `FilingRef` shape (internal model for Sprint 2):
- `source`: `"sec_rss"`
- `cik`: zero-padded string
- `accession_number`: normalized accession
- `form_type`: `"4"` or `"4/A"`
- `filed_at`: UTC datetime (best effort from feed)
- `filing_detail_url`: SEC filing detail URL
- `primary_doc_url`: optional (if extractable)
- `raw_rss_entry`: JSON object for audit

## 6) Compliance + privacy requirements
- Do **not** place personal PII in User-Agent.
- Use neutral identifier format (example):
  - `insider-alerts/0.2 (contact: sec-access@<domain>)`
- Keep request rates conservative and cache where practical.
- Logs must avoid secrets and avoid unnecessary raw untrusted text.

## 7) TDD plan and acceptance criteria

### Story A — SEC client policy enforcement
**Tests first:**
- Adds required headers (`User-Agent`, `Accept-Encoding`).
- Applies configured timeout.
- Retries on 403/429/5xx with bounded attempts.
- Enforces configured rate limiter.

**Accept when:** all policy tests pass and client emits clear typed errors.

### Story B — RSS parser robustness
**Tests first:**
- Parses standard fixture into expected number of records.
- Handles missing optional fields without crashing.
- Rejects malformed entries with warning + skip.
- Correctly normalizes CIK/accession/form type.

**Accept when:** parser is deterministic and resilient to minor feed format drift.

### Story C — Idempotent storage
**Tests first:**
- First poll inserts new filings.
- Second poll with same fixture inserts zero new rows.
- Mixed new+existing fixture inserts only delta.

**Accept when:** duplicate-safe persistence works via DB uniqueness + conflict handling.

### Story D — CLI poll workflow
**Tests first:**
- `sec poll --once` runs end-to-end with mocked HTTP.
- `--dry-run` parses and reports counts without writes.
- Exit codes and summary output are stable.

**Accept when:** operator can safely run one-shot polls and inspect outcomes.

## 8) Risk register (Sprint 2)
1. **RSS feed drift** → Mitigate with tolerant parsing + deadletter logging.
2. **Rate-limit blocks (403/429)** → Mitigate with conservative rps + jitter backoff.
3. **Duplicate ingestion** → Mitigate with DB uniqueness + upsert/ignore semantics.
4. **Time parsing ambiguity** → Mitigate with UTC normalization + explicit fallback policy.
5. **Operational blind spots** → Mitigate with structured counters in logs.

## 9) Definition of done
- All Sprint 2 tests green locally and in CI.
- Coverage for new code paths >= existing repo gate.
- CLI command and docs updated.
- No scope bleed into Sprint 3+.
- Sprint summary documented in `docs/sprints/`.

## 10) Go / no-go gate before coding
Proceed only after all are true:
- [ ] User-Agent format approved (non-PII alias).
- [ ] Poll interval + max-items defaults approved.
- [ ] Dedupe key approved (`accession_number` + `cik` + `form_type`).
- [ ] Dry-run semantics approved.
- [ ] Error handling policy approved (skip+log vs fail-fast per case).

---

If approved, implementation begins in a dedicated Sprint 2 branch with strict TDD ordering: tests → minimal code → refactor → docs update.
