from datetime import date

from insider_alerts.review.scoring import score_form4_signal
from insider_alerts.sec.form4 import Form4Facts, Form4Transaction


def test_score_form4_signal_prefers_net_buying() -> None:
    facts = Form4Facts(
        issuer_cik="0000320193",
        issuer_name="Apple Inc.",
        issuer_symbol="AAPL",
        reporting_owner_name="DOE JOHN",
        reporting_owner_cik="0001111111",
        is_director=True,
        is_officer=False,
        officer_title=None,
        transactions=[
            Form4Transaction(
                transaction_date=date(2026, 2, 10),
                transaction_code="P",
                direction="buy",
                shares=1000.0,
                price_per_share=100.0,
                shares_following=2000.0,
                security_title="Common Stock",
            )
        ],
    )
    result = score_form4_signal(facts)
    assert result.score > 0
    assert "net_buy_shares" in result.rationale


def test_score_form4_signal_unknown_direction_does_not_change_flow() -> None:
    facts = Form4Facts(
        issuer_cik="0000320193",
        issuer_name="Apple Inc.",
        issuer_symbol="AAPL",
        reporting_owner_name="DOE JOHN",
        reporting_owner_cik="0001111111",
        is_director=False,
        is_officer=False,
        officer_title=None,
        transactions=[
            Form4Transaction(
                transaction_date=date(2026, 2, 10),
                transaction_code="G",
                direction="unknown",
                shares=500.0,
                price_per_share=None,
                shares_following=None,
                security_title="Common Stock",
            )
        ],
    )
    result = score_form4_signal(facts)
    assert result.rationale["net_buy_shares"] == 0.0
