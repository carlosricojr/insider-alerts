from __future__ import annotations

from datetime import date

from insider_alerts.backtest.fundamentals import shares_outstanding_as_of


def test_shares_outstanding_as_of_never_uses_later_filing() -> None:
    payload = {
        "facts": {
            "dei": {
                "EntityCommonStockSharesOutstanding": {
                    "units": {
                        "shares": [
                            {
                                "end": "2026-02-01",
                                "filed": "2026-02-10",
                                "form": "10-K",
                                "val": 100_000_000,
                            },
                            {
                                "end": "2026-05-01",
                                "filed": "2026-05-10",
                                "form": "10-Q",
                                "val": 125_000_000,
                            },
                        ]
                    }
                }
            }
        }
    }
    assert shares_outstanding_as_of(payload, as_of=date(2026, 4, 1)) == 100_000_000
    assert shares_outstanding_as_of(payload, as_of=date(2026, 6, 1)) == 125_000_000


def test_shares_outstanding_as_of_falls_back_to_us_gaap_and_rejects_future_period() -> None:
    payload = {
        "facts": {
            "us-gaap": {
                "CommonStockSharesOutstanding": {
                    "units": {
                        "shares": [
                            {
                                "end": "2026-03-31",
                                "filed": "2026-04-20",
                                "form": "10-Q",
                                "val": 50_000_000,
                            },
                            {
                                "end": "2026-12-31",
                                "filed": "2026-04-01",
                                "form": "10-Q",
                                "val": 999_000_000,
                            },
                        ]
                    }
                }
            }
        }
    }
    assert shares_outstanding_as_of(payload, as_of=date(2026, 5, 1)) == 50_000_000


def test_shares_outstanding_as_of_returns_none_for_invalid_values() -> None:
    payload = {
        "facts": {
            "dei": {
                "EntityCommonStockSharesOutstanding": {
                    "units": {
                        "shares": [
                            {
                                "end": "2026-01-01",
                                "filed": "2026-01-02",
                                "val": -1,
                            }
                        ]
                    }
                }
            }
        }
    }
    assert shares_outstanding_as_of(payload, as_of=date(2026, 2, 1)) is None
