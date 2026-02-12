from datetime import date

from insider_alerts.review.market_context import MarketSnapshot
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


def test_score_form4_signal_penalizes_planned_strategic_flow() -> None:
    facts = Form4Facts(
        issuer_cik="0000011544",
        issuer_name="W.R. Berkley Corporation",
        issuer_symbol="WRB",
        reporting_owner_name="MITSUI SUMITOMO INSURANCE CO LTD",
        reporting_owner_cik="0009999999",
        is_director=False,
        is_officer=False,
        officer_title=None,
        transactions=[
            Form4Transaction(
                transaction_date=date(2026, 2, 11),
                transaction_code="P",
                direction="buy",
                shares=97996.0,
                price_per_share=69.68,
                shares_following=56556652.0,
                security_title="Common Stock",
            )
        ],
        is_ten_percent_owner=True,
        has_10b5_1_plan=True,
        has_13d_reference=True,
    )
    result = score_form4_signal(facts)
    assert result.score < 45
    assert result.rationale["has_10b5_1_plan"] is True
    assert result.rationale["owner_is_ten_percent_owner"] is True
    assert result.rationale["owner_is_entity"] is True


def test_score_form4_signal_rewards_discretionary_exec_buy() -> None:
    facts = Form4Facts(
        issuer_cik="0007777777",
        issuer_name="Example Corp",
        issuer_symbol="EXM",
        reporting_owner_name="SMITH JANE",
        reporting_owner_cik="0002222222",
        is_director=True,
        is_officer=True,
        officer_title="Chief Executive Officer",
        transactions=[
            Form4Transaction(
                transaction_date=date(2026, 2, 11),
                transaction_code="P",
                direction="buy",
                shares=50000.0,
                price_per_share=25.0,
                shares_following=250000.0,
                security_title="Common Stock",
            )
        ],
        is_ten_percent_owner=False,
        has_10b5_1_plan=False,
        has_13d_reference=False,
    )
    result = score_form4_signal(facts)
    assert result.score >= 80
    assert result.rationale["owner_is_exec"] is True
    assert result.rationale["open_market_buy_shares"] == 50000.0


def test_score_form4_signal_penalizes_compensation_vesting_noise() -> None:
    facts = Form4Facts(
        issuer_cik="0001631574",
        issuer_name="Wave Life Sciences Ltd.",
        issuer_symbol="WVE",
        reporting_owner_name="Moran Kyle",
        reporting_owner_cik="0000000001",
        is_director=False,
        is_officer=True,
        officer_title="Chief Financial Officer",
        transactions=[
            Form4Transaction(
                transaction_date=date(2026, 2, 5),
                transaction_code="A",
                direction="buy",
                shares=45625.0,
                price_per_share=0.0,
                shares_following=138149.0,
                security_title="RSU",
            ),
            Form4Transaction(
                transaction_date=date(2026, 2, 9),
                transaction_code="S",
                direction="sell",
                shares=3588.0,
                price_per_share=13.45,
                shares_following=134561.0,
                security_title="Common Stock",
            ),
        ],
        has_equity_comp_event=True,
        has_tax_withholding_language=True,
    )
    result = score_form4_signal(facts)
    assert result.score < 40
    assert result.rationale["open_market_buy_shares"] == 0.0
    assert result.rationale["has_equity_comp_event"] is True
    assert result.rationale["has_tax_withholding_language"] is True


def test_score_form4_signal_downweights_director_low_liquidity_buy() -> None:
    facts = Form4Facts(
        issuer_cik="0000064040",
        issuer_name="S&P Global Inc.",
        issuer_symbol="SPGI",
        reporting_owner_name="Joly Hubert",
        reporting_owner_cik="0001467638",
        is_director=True,
        is_officer=False,
        officer_title=None,
        transactions=[
            Form4Transaction(
                transaction_date=date(2026, 2, 11),
                transaction_code="P",
                direction="buy",
                shares=2301.0,
                price_per_share=398.94,
                shares_following=2466.0,
                security_title="Common Stock",
            ),
            Form4Transaction(
                transaction_date=date(2026, 2, 11),
                transaction_code="P",
                direction="buy",
                shares=199.0,
                price_per_share=399.49,
                shares_following=2665.0,
                security_title="Common Stock",
            ),
        ],
    )
    snapshot = MarketSnapshot(
        symbol="SPGI",
        trade_date=date(2026, 2, 11),
        close=390.76,
        volume=5_174_841.0,
        dollar_turnover=390.76 * 5_174_841.0,
        prior_close=401.08,
        return_1d=(390.76 / 401.08) - 1.0,
        earnings_shock_flag=False,
    )
    result = score_form4_signal(facts, market_snapshot=snapshot)
    assert result.score < 90
    assert result.rationale["role_tier"] == "director"
    assert result.rationale["trade_pct_daily_turnover"] is not None


def test_score_form4_signal_keeps_high_conviction_ceo_buy_high() -> None:
    facts = Form4Facts(
        issuer_cik="0000063276",
        issuer_name="Mattel Inc.",
        issuer_symbol="MAT",
        reporting_owner_name="Kreiz Ynon",
        reporting_owner_cik="0001708842",
        is_director=True,
        is_officer=True,
        officer_title="Chairman & CEO",
        transactions=[
            Form4Transaction(
                transaction_date=date(2026, 2, 12),
                transaction_code="P",
                direction="buy",
                shares=65000.0,
                price_per_share=15.5277,
                shares_following=1794217.0,
                security_title="Common Stock",
            )
        ],
    )
    snapshot = MarketSnapshot(
        symbol="MAT",
        trade_date=date(2026, 2, 12),
        close=15.85,
        volume=15_389_174.0,
        dollar_turnover=15.85 * 15_389_174.0,
        prior_close=15.8,
        return_1d=(15.85 / 15.8) - 1.0,
        earnings_shock_flag=False,
    )
    result = score_form4_signal(facts, market_snapshot=snapshot)
    assert result.score >= 90
    assert result.rationale["role_tier"] == "chief_exec"
    assert result.rationale["trade_pct_daily_turnover"] is not None
