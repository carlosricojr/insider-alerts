from __future__ import annotations

from dataclasses import dataclass

from insider_alerts.sec.form4 import Form4Facts


@dataclass(slots=True)
class ScoreResult:
    score: float
    rationale: dict[str, float]


def score_form4_signal(facts: Form4Facts) -> ScoreResult:
    net_buy_shares = 0.0
    gross_value = 0.0
    for tx in facts.transactions:
        if tx.direction == "buy":
            signed = tx.shares
        elif tx.direction == "sell":
            signed = -tx.shares
        else:
            signed = 0.0
        net_buy_shares += signed
        if tx.price_per_share is not None:
            gross_value += abs(tx.shares * tx.price_per_share)

    role_bonus = 10.0 if facts.is_director or facts.is_officer else 0.0
    value_component = min(gross_value / 100000.0, 35.0)
    flow_component = max(min(net_buy_shares / 100.0, 45.0), -45.0)

    raw = 50.0 + role_bonus + value_component + flow_component
    score = max(0.0, min(100.0, raw))

    return ScoreResult(
        score=score,
        rationale={
            "net_buy_shares": net_buy_shares,
            "gross_value": gross_value,
            "role_bonus": role_bonus,
        },
    )
