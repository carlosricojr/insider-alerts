# Sprint 7 Review — Hardening, Runbook, Threat Model, Deadletter/Replay, Deployment

Status: **READY FOR IMPLEMENTATION**

## Objective
Harden operations and safety with runbook/threat-model documentation plus deadletter and replay tooling.

## Scope
- Runbook and deployment docs.
- Threat model with SEC compliance/privacy controls.
- Deadletter table + replay CLI for failed processing.
- Tests for deadletter/replay flows.

## Acceptance Criteria
- Clear operator runbook and deployment checklist exist.
- Threat model documents abuse/failure controls.
- Failed packets can be listed and replayed safely.

## Tests (TDD)
- Deadletter store unit tests.
- CLI tests for deadletter list/replay.

## Risks
- Replay loops or duplicate enqueue.
- Insufficient operational observability.

## Go/No-Go
- [ ] Replay idempotency verified.
- [ ] Compliance guardrails explicit in docs and config.

## Outcome
Implemented: runbook/threat-model/deployment docs plus deadletter list/replay commands and tests.
