# Sprint 5 Review — Signal Scoring + Review Packet Queue

Status: **READY FOR IMPLEMENTATION**

## Objective
Convert canonical Form 4 facts into scored signals and enqueue review packets for human decisioning.

## Scope
- Deterministic score model from parsed facts.
- Review packet persistence with queue state.
- Enqueue pipeline from filings with discovered XML.
- Tests for scoring and idempotent queue writes.

## Acceptance Criteria
- Score and rationale generated for parsed facts.
- Review packet persisted with stable key and JSON payload.
- Re-running enqueue does not duplicate pending packets.

## Tests (TDD)
- Unit tests for scoring weights and edge-cases.
- Store tests for enqueue dedupe.
- Pipeline tests for end-to-end packet creation.

## Risks
- Overly brittle scoring assumptions.
- Queue duplication due to key mismatch.

## Go/No-Go
- [ ] Scoring rubric documented.
- [ ] Queue schema approved.

## Outcome
Implemented: signal scoring (`review/scoring.py`), review queue persistence/ids, enqueue pipeline and tests.
