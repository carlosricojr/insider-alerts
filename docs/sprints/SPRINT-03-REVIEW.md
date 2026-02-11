# Sprint 3 Review — Filing Index Fetch + Form 4 XML Location

Status: **READY FOR IMPLEMENTATION**

## Objective
Extend ingestion from RSS references to filing-detail index retrieval and deterministic Form 4 XML document URL discovery.

## Scope
- Add filing detail/index fetch utility using SEC client policy.
- Parse filing documents and locate best Form 4 XML candidate URL.
- Persist discovered XML URL against filing rows.
- Add tests for parsing success/fallback/error paths.

## Acceptance Criteria
- Given fixture filing index HTML, locator returns absolute SEC XML URL.
- Non-XML/missing index returns `None` without crash.
- DB update is idempotent and auditable.
- SEC fair-access defaults remain explicit.

## Tests (TDD)
- Locator unit tests for common and drifted table structures.
- Pipeline/storage tests for update semantics.
- CLI test for enrichment command summary output.

## Risks
- SEC HTML schema drift.
- Relative URL normalization errors.
- Over-fetching from SEC.

## Go/No-Go
- [ ] Locator heuristic reviewed.
- [ ] Update semantics (no overwrite unless empty) confirmed.
- [ ] Logging avoids raw sensitive payloads.

## Outcome
Implemented: index/XML locator (`sec/index.py`), DB xml-url enrichment helpers, `sec enrich` CLI, and tests.
