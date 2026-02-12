# Sprint 8 Review - Alpha Edge Filtering and Decision Quality Hardening

Status: **READY FOR IMPLEMENTATION**

## Objective
Increase expected value (EV) of live trade alerts by reducing "technically valid but low-edge" approvals, while preserving true high-conviction discretionary insider buys.

This sprint is driven by recent live examples:
- `MAT` alert: legitimate discretionary CEO open-market buy, likely useful.
- `SPGI` alert: legitimate buy, but likely over-scored for edge due director-only role and inflated holding-change ratio from tiny starting position.

## Scope
- Rework scoring to better represent informational edge, not just raw buy strength.
- Add market context features (liquidity and regime) for EV filtering.
- Tighten Quant decision contract and safety checks.
- Add regression fixtures for MAT/SPGI to prevent recurrence of known misclassification patterns.
- Add rollout controls for safe migration.

## Architecture
### 1) Scoring V2 - componentized and explainable
- Keep current hard invalid/noise checks (10b5-1, comp/tax patterns, no open-market buy).
- Replace monolithic additive score with explicit components:
  - `role_component_v2`:
    - Distinguish role tiers (CEO/CFO/COO > officer > director > non-exec entity).
    - Stop treating director and officer as equivalent.
  - `position_impact_component_v2`:
    - Use both absolute post-trade holdings and change ratio.
    - Apply shrinkage for tiny pre-trade holdings to prevent ratio explosions.
  - `liquidity_component_v2`:
    - Add trade size vs market liquidity:
      - `% daily volume`
      - `% daily dollar turnover`
  - `regime_component_v2`:
    - Penalize signals in immediate post-shock regimes (for example, large earnings gap-down windows) unless trade conviction is exceptional.
  - `novelty_component_v2`:
    - Keep and refine novelty penalties already in place.
- Persist component breakdown in `payload_json.rationale` for auditability.

### 2) Market context adapter
- Add `market_context` module with provider interface.
- Initial provider: daily OHLCV pull for symbol/date (minimal data needed for liquidity and shock flags).
- Cache/store snapshots in SQLite for deterministic replays and testability.
- Fail-closed behavior:
  - Missing market context must not silently inflate score.
  - Either apply conservative penalty or force `escalate`.

### 3) Quant decision contract V2
- Replace loose free-text criteria with strict JSON rubric:
  - Required fields: `decision`, `why`, `confidence`, `edge_hypothesis`, `risk_flags`, `evidence`.
  - Evidence must reference structured inputs (role tier, open-market code, liquidity impact, novelty, regime flag).
- If schema is invalid or evidence is missing, auto-`escalate`.
- Update prompt policy to explicitly favor informational edge over generic insider-buy strength.

### 4) Guardrails V2 (approval gate)
- Approval requires all:
  - Discretionary open-market buy evidence.
  - No disqualifying flow flags (planned/comp/tax-only patterns).
  - Score above threshold with minimum component floors.
  - Minimum Quant confidence and valid evidence payload.
  - No severe regime risk unless conviction exceeds elevated threshold.

### 5) Observability
- Record decision reason codes in addition to human-readable reason text.
- Add per-cycle counters for:
  - `approved_high_edge`
  - `rejected_low_edge`
  - `escalated_missing_context`
  - `escalated_schema_invalid`
- Include component summary in notification payload for approved alerts.

## Data and Schema Changes
- `review_packets.payload_json.rationale` additions:
  - `role_tier`
  - `pre_trade_shares_estimate`
  - `post_trade_shares`
  - `position_change_shrinkage_factor`
  - `trade_pct_daily_volume`
  - `trade_pct_daily_turnover`
  - `regime_earnings_shock_flag`
  - `component_scores_v2` object
- New table:
  - `market_snapshots(symbol, date, close, volume, dollar_turnover, source, created_at, PRIMARY KEY(symbol,date))`

## Implementation Plan (TDD First)
1. Add schema and migration for `market_snapshots`.
2. Build market context client + snapshot store + deterministic fallback behavior.
3. Implement scoring V2 components and wire into enqueue payload rationale.
4. Update Quant prompt + parser + schema validation logic.
5. Update autopilot guardrails for V2 requirements.
6. Add structured reason-code metrics in cycle summary.
7. Update docs/runbook and alert examples.

## Tests (TDD)
- Unit tests:
  - Role tier classification.
  - Shrinkage behavior for tiny pre-trade holdings.
  - Liquidity component math.
  - Regime penalty behavior.
  - Quant response schema validator.
- Regression fixtures:
  - MAT-like case must remain `approve` with high-edge rationale.
  - SPGI-like case must not be top-tier auto-approve solely due ratio inflation.
- Integration tests:
  - Autopilot cycle with market context available.
  - Autopilot cycle with market context unavailable (conservative outcome).
  - Notification includes ticker, why, and compact component evidence.

## Acceptance Criteria
- MAT/SPGI regression tests pass with intended classification behavior.
- No approval can occur with invalid Quant schema output.
- Approval set quality improves in shadow comparison:
  - lower noise approval rate
  - higher concentration of discretionary high-edge patterns
- System remains resilient in loop mode under transient SEC/market data failures.

## Risks
- Over-filtering may reduce recall and miss some true positives.
- External market data availability may introduce latency/failure modes.
- Calibration drift if thresholds are not tuned with replay data.

## Rollout Strategy
- Add feature flag for V2 scoring/guardrails (`ALPHA_FILTER_V2_ENABLED`).
- Run shadow mode for at least 1 trading week:
  - compute both V1 and V2 decisions
  - alert only on existing policy until go-live gate passes
- Promote to active mode after go/no-go checklist passes.

## Go/No-Go Checklist
- [ ] MAT and SPGI fixtures produce expected outcomes.
- [ ] Quant schema validation blocks malformed decisions.
- [ ] Shadow metrics show improved precision without unacceptable recall loss.
- [ ] Runbook updated with fallback behavior and incident handling.

## Outcome
Pending implementation.
