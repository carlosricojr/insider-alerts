from __future__ import annotations

import math
import re
from dataclasses import dataclass

from insider_alerts.sec.form4 import Form4Facts


@dataclass(slots=True)
class ScoreResult:
    score: float
    rationale: dict[str, object]


ENTITY_HINTS = {
    "advisors",
    "asset",
    "bank",
    "capital",
    "co",
    "company",
    "corp",
    "corporation",
    "fund",
    "group",
    "holdings",
    "inc",
    "insurance",
    "investment",
    "investments",
    "llc",
    "ltd",
    "management",
    "partners",
    "trust",
}
NON_WORD_RE = re.compile(r"[^a-z0-9]+")


def _is_likely_entity(name: str | None) -> bool:
    if name is None:
        return False
    lowered = NON_WORD_RE.sub(" ", name.lower()).strip()
    if not lowered:
        return False
    tokens = set(lowered.split())
    if tokens.intersection(ENTITY_HINTS):
        return True
    return any(char.isdigit() for char in name)


def score_form4_signal(facts: Form4Facts) -> ScoreResult:
    net_buy_shares = 0.0
    gross_value = 0.0
    open_market_gross_value = 0.0
    total_buy_shares = 0.0
    total_sell_shares = 0.0
    open_market_buy_shares = 0.0
    open_market_sell_shares = 0.0
    holdings_before_estimate: float | None = None
    holdings_after_estimate: float | None = None
    code_counts: dict[str, int] = {}

    for tx in facts.transactions:
        code = tx.transaction_code.upper().strip()
        if code:
            code_counts[code] = code_counts.get(code, 0) + 1
        if tx.direction == "buy":
            signed = tx.shares
            total_buy_shares += tx.shares
            if code == "P":
                open_market_buy_shares += tx.shares
        elif tx.direction == "sell":
            signed = -tx.shares
            total_sell_shares += tx.shares
            if code == "S":
                open_market_sell_shares += tx.shares
        else:
            signed = 0.0
        net_buy_shares += signed
        if tx.price_per_share is not None:
            gross_value += abs(tx.shares * tx.price_per_share)
            if code in {"P", "S"}:
                open_market_gross_value += abs(tx.shares * tx.price_per_share)
        if tx.shares_following is not None and signed != 0:
            before = max(tx.shares_following - signed, 0.0)
            if holdings_before_estimate is None:
                holdings_before_estimate = before
            holdings_after_estimate = tx.shares_following

    holding_change_ratio: float | None = None
    if (
        holdings_before_estimate is not None
        and holdings_before_estimate > 0
        and holdings_after_estimate is not None
    ):
        holding_change_ratio = (
            holdings_after_estimate - holdings_before_estimate
        ) / holdings_before_estimate

    owner_is_exec = facts.is_director or facts.is_officer
    owner_is_entity = _is_likely_entity(facts.reporting_owner_name)
    owner_is_strategic = facts.is_ten_percent_owner and not owner_is_exec
    open_market_net_shares = open_market_buy_shares - open_market_sell_shares
    has_option_exercise = code_counts.get("M", 0) > 0
    has_award_code = code_counts.get("A", 0) > 0
    non_open_market_buy_shares = max(total_buy_shares - open_market_buy_shares, 0.0)

    size_component = min(math.log10(open_market_gross_value + 1.0) * 5.0, 20.0)
    flow_component = max(min((open_market_net_shares / 5000.0) * 30.0, 30.0), -30.0)
    if holding_change_ratio is None:
        holding_component = 0.0
    elif holding_change_ratio <= 0 and open_market_buy_shares > 0:
        holding_component = -15.0
    elif open_market_buy_shares <= 0:
        holding_component = 0.0
    else:
        holding_component = min(holding_change_ratio * 1200.0, 20.0)

    role_component = 15.0 if owner_is_exec else 0.0
    alpha_bonus = 0.0
    if (
        owner_is_exec
        and not facts.has_10b5_1_plan
        and not facts.has_equity_comp_event
        and open_market_buy_shares > 0
        and open_market_net_shares > 0
        and (holding_change_ratio is None or holding_change_ratio >= 0.005)
    ):
        alpha_bonus = 10.0

    novelty_penalty = 0.0
    if open_market_buy_shares <= 0:
        novelty_penalty += 45.0
    if facts.has_10b5_1_plan:
        novelty_penalty += 35.0
    if facts.has_equity_comp_event and open_market_buy_shares <= 0:
        novelty_penalty += 35.0
    if facts.has_tax_withholding_language and open_market_buy_shares <= 0:
        novelty_penalty += 20.0
    if owner_is_strategic:
        novelty_penalty += 20.0
    if owner_is_entity and not owner_is_exec:
        novelty_penalty += 12.0
    if facts.has_13d_reference:
        novelty_penalty += 15.0
    if net_buy_shares <= 0:
        novelty_penalty += 25.0
    if has_option_exercise and open_market_buy_shares <= 0:
        novelty_penalty += 20.0
    if has_award_code and open_market_buy_shares <= 0:
        novelty_penalty += 20.0
    if non_open_market_buy_shares > 0 and open_market_buy_shares <= 0:
        novelty_penalty += min(non_open_market_buy_shares / 5000.0 * 8.0, 18.0)
    if holding_change_ratio is not None and holding_change_ratio < 0.002:
        novelty_penalty += 12.0
    if gross_value < 250000.0:
        novelty_penalty += 8.0

    raw = (
        35.0
        + size_component
        + flow_component
        + holding_component
        + role_component
        + alpha_bonus
        - novelty_penalty
    )
    score = max(0.0, min(100.0, raw))

    return ScoreResult(
        score=score,
        rationale={
            "net_buy_shares": net_buy_shares,
            "gross_value": gross_value,
            "total_buy_shares": total_buy_shares,
            "total_sell_shares": total_sell_shares,
            "open_market_buy_shares": open_market_buy_shares,
            "open_market_sell_shares": open_market_sell_shares,
            "open_market_net_shares": open_market_net_shares,
            "open_market_gross_value": open_market_gross_value,
            "holding_change_ratio": holding_change_ratio,
            "owner_is_exec": owner_is_exec,
            "owner_is_ten_percent_owner": facts.is_ten_percent_owner,
            "owner_is_entity": owner_is_entity,
            "has_10b5_1_plan": facts.has_10b5_1_plan,
            "has_13d_reference": facts.has_13d_reference,
            "has_equity_comp_event": facts.has_equity_comp_event,
            "has_tax_withholding_language": facts.has_tax_withholding_language,
            "has_option_exercise": has_option_exercise,
            "has_award_code": has_award_code,
            "transaction_code_counts": code_counts,
            "novelty_penalty": novelty_penalty,
            "alpha_bonus": alpha_bonus,
        },
    )
