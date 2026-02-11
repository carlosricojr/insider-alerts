# Sprint 6 Review — OpenClaw Skills + Decision Apply + Notify Integration

Status: **READY FOR IMPLEMENTATION**

## Objective
Deliver review-decision workflow with explicit decision schema validation, merge/apply logic, and OpenClaw skill guidance.

## Scope
- Provide OpenClaw skill files: `ntfy-notify`, `insider-review`.
- Decision schema and validator.
- Apply review decisions to queued packets with validation + conflict checks.
- Optional notification dispatch on applied decisions.

## Acceptance Criteria
- Invalid decision payloads rejected with actionable errors.
- Valid decision updates queue status and stores merged metadata.
- Skill docs are present and operationally clear.

## Tests (TDD)
- Decision schema validation tests.
- Apply/merge tests (approve/reject/escalate).
- CLI tests for decision apply command.

## Risks
- Ambiguous merge semantics.
- Operator errors in manual JSON decisions.

## Go/No-Go
- [ ] Decision statuses finalized.
- [ ] Notification policy approved.

## Outcome
Implemented: OpenClaw skills, decision schema file, validated apply/merge, and `review apply --notify` integration with tests.
