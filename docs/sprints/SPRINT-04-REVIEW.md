# Sprint 4 Review — Form 4 XML Parser to Canonical Facts

Status: **READY FOR IMPLEMENTATION**

## Objective
Implement robust Form 4 XML parsing into canonical transaction facts for downstream scoring.

## Scope
- Parse owner/issuer metadata and transaction rows.
- Normalize transaction direction, share count, price, code, and post-transaction holdings.
- Tolerate optional/missing fields with safe defaults.
- Add fixtures and deterministic parser tests.

## Acceptance Criteria
- Canonical facts emitted for representative Form 4 fixture.
- Amendments and non-derivative transactions parsed safely.
- Malformed XML fails with typed parser error.

## Tests (TDD)
- Happy-path fixture with multiple transactions.
- Missing optional fields fixture.
- Invalid XML fixture.

## Risks
- XML namespace and structure variations.
- Ambiguous signs for disposition/acquisition.

## Go/No-Go
- [ ] Canonical schema finalized.
- [ ] Numeric/date normalization policy approved.

## Outcome
Implemented: canonical Form 4 parser (`sec/form4.py`) with robust optional-field handling and fixtures/tests.
