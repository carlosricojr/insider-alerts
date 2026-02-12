from datetime import date

from insider_alerts.backtest.models import DailyBar
from insider_alerts.backtest.prices import (
    StooqPriceClient,
    get_price_bars,
    refresh_price_bars,
)


def test_stooq_price_client_parses_csv() -> None:
    class _FakeClient(StooqPriceClient):
        def _download_csv(self, symbol: str) -> str:
            assert symbol == "MAT"
            return (
                "Date,Open,High,Low,Close,Volume\n"
                "2026-02-11,15.075,16.4599,15.05,15.8,40089442\n"
                "2026-02-12,16,16.22,14.745,15.835,13132123\n"
            )

    client = _FakeClient(
        user_agent="insider-alerts/0.2 (contact: sec-access@example.com)",
        timeout_seconds=5.0,
    )
    bars = client.fetch_history("MAT")
    assert len(bars) == 2
    assert bars[0].symbol == "MAT"
    assert bars[0].trade_date == date(2026, 2, 11)
    assert bars[1].close == 15.835


def test_refresh_and_get_price_bars_round_trip(tmp_path) -> None:
    db_path = str(tmp_path / "db.sqlite3")
    bars = [
        DailyBar(
            symbol="SPGI",
            trade_date=date(2026, 2, 11),
            open=406.7,
            high=413.991,
            low=390.73,
            close=390.76,
            volume=5174841.0,
        ),
        DailyBar(
            symbol="SPGI",
            trade_date=date(2026, 2, 12),
            open=390.01,
            high=399.9499,
            low=381.605,
            close=397.2,
            volume=3986132.0,
        ),
    ]
    refresh_price_bars(db_path, symbol="SPGI", bars=bars)
    loaded = get_price_bars(
        db_path,
        symbol="SPGI",
        start_date=date(2026, 2, 11),
        end_date=date(2026, 2, 12),
    )
    assert len(loaded) == 2
    assert loaded[0].open == 406.7
    assert loaded[1].close == 397.2
