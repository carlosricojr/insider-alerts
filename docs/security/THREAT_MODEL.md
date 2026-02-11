# Threat Model (Insider Alerts)

## Assets
- Filing references and parsed transaction facts.
- Review decisions and audit history.

## Key threats
- Feed/schema drift causes silent data loss.
- Excess SEC request rates trigger blocking.
- Decision tampering or malformed decision payloads.
- Sensitive metadata leakage in logs/notifications.

## Controls
- Typed parse/validation errors and deadletter capture.
- SEC client rate-limit + retry policy.
- Decision schema enforcement and pending-only updates.
- Minimal logging and explicit no-secret notification policy.
