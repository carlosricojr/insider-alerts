from datetime import date

import pytest

from insider_alerts.review import market_context as market_context_module
from insider_alerts.review.market_context import (
    DailyMarketDataClient,
    MarketContextError,
    MarketSnapshot,
    get_market_snapshot,
    upsert_market_snapshot,
)


def test_market_snapshot_round_trip(tmp_path) -> None:
    db_path = str(tmp_path / "db.sqlite3")
    snapshot = MarketSnapshot(
        symbol="SPGI",
        trade_date=date(2026, 2, 11),
        close=390.76,
        volume=5_174_841.0,
        dollar_turnover=2_022_104_281.16,
        prior_close=401.08,
        return_1d=-0.025730527575545995,
        earnings_shock_flag=False,
    )
    upsert_market_snapshot(db_path, snapshot)
    loaded = get_market_snapshot(db_path, symbol="SPGI", trade_date=date(2026, 2, 11))
    assert loaded is not None
    assert loaded.symbol == "SPGI"
    assert loaded.trade_date == date(2026, 2, 11)
    assert loaded.close == 390.76
    assert loaded.volume == 5_174_841.0
    assert loaded.earnings_shock_flag is False


def test_daily_market_data_client_marks_shock_day() -> None:
    class _FakeClient(DailyMarketDataClient):
        def _download_csv_text(self, symbol: str) -> str:
            assert symbol == "SPGI"
            return (
                "Date,Open,High,Low,Close,Volume\n"
                "2026-02-10,418.97,424.80,395.88,401.08,10888451,ignored\n"
                "2026-02-11,406.70,413.99,390.73,390.76,5174841,ignored\n"
            )

    client = _FakeClient(
        user_agent="insider-alerts/0.2 (contact: sec-access@example.com)",
        timeout_seconds=5.0,
        shock_drop_threshold=0.02,
    )
    snapshot = client.fetch_snapshot("SPGI", trade_date=date(2026, 2, 11))
    assert snapshot is not None
    assert snapshot.symbol == "SPGI"
    assert snapshot.return_1d is not None
    assert snapshot.return_1d < 0
    assert snapshot.earnings_shock_flag is True


def test_daily_market_data_client_download_validates_response_bytes(monkeypatch) -> None:
    class _Response:
        def __init__(self, body: object) -> None:
            self.body = body

        def __enter__(self) -> "_Response":
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

        def read(self) -> object:
            return self.body

    client = DailyMarketDataClient(
        user_agent="insider-alerts/0.2 (contact: sec-access@example.com)",
        timeout_seconds=5.0,
    )

    monkeypatch.setattr(market_context_module, "urlopen", lambda req, timeout: _Response(b"Date\n"))
    assert client._download_csv_text("SPGI") == "Date\n"

    monkeypatch.setattr(market_context_module, "urlopen", lambda req, timeout: _Response("Date\n"))
    with pytest.raises(MarketContextError, match="response was not bytes"):
        client._download_csv_text("SPGI")
