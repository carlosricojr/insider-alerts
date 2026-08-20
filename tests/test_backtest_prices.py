import gzip
from datetime import date
from http.client import InvalidURL
from types import TracebackType
from urllib.error import HTTPError

import pytest

from insider_alerts.backtest import prices as prices_module
from insider_alerts.backtest.models import DailyBar
from insider_alerts.backtest.prices import (
    PriceDataError,
    StooqPriceClient,
    get_price_bar_bounds,
    get_price_bars,
    normalize_backtest_symbol,
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


def test_stooq_price_client_rejects_non_tradable_symbol() -> None:
    client = StooqPriceClient(
        user_agent="insider-alerts/0.2 (contact: sec-access@example.com)",
        timeout_seconds=7.0,
    )
    with pytest.raises(PriceDataError, match="unsupported ticker format"):
        client.fetch_history("Z AND ZG")


def test_stooq_price_client_prefers_yahoo_when_enabled() -> None:
    class _FakeClient(StooqPriceClient):
        def _download_yahoo_chart(self, symbol: str) -> str:
            assert symbol == "MAT"
            return (
                '{"chart":{"result":[{"timestamp":[1707523200,1707609600],'
                '"indicators":{"quote":[{"open":[15.0,16.0],"high":[16.0,17.0],'
                '"low":[14.5,15.5],"close":[15.8,16.2],"volume":[1000,1200]}]}}],'
                '"error":null}}'
            )

    client = _FakeClient(
        user_agent="insider-alerts/0.2 (contact: sec-access@example.com)",
        timeout_seconds=5.0,
        prefer_yahoo=True,
    )
    bars = client.fetch_history("MAT")
    assert len(bars) == 2
    assert bars[0].symbol == "MAT"
    assert bars[1].close == 16.2


def test_yahoo_download_requests_daily_period_instead_of_downsampled_max_range() -> None:
    captured_url = ""

    class _FakeClient(StooqPriceClient):
        def _download_text(self, *, url: str, symbol: str) -> str:
            nonlocal captured_url
            captured_url = url
            assert symbol == "SPY"
            return "{}"

    client = _FakeClient(user_agent="test", timeout_seconds=5.0)
    client._download_yahoo_chart("SPY")
    assert "interval=1d" in captured_url
    assert "period1=0" in captured_url
    assert "period2=" in captured_url
    assert "range=max" not in captured_url


def test_yahoo_parser_rejects_silently_downsampled_monthly_response() -> None:
    client = StooqPriceClient(user_agent="test", timeout_seconds=5.0)
    payload = (
        '{"chart":{"result":[{"meta":{"dataGranularity":"1mo"},'
        '"timestamp":[1707523200],"indicators":{"quote":[{"open":[15.0],'
        '"high":[16.0],"low":[14.5],"close":[15.8],"volume":[1000]}]}}]}}'
    )
    with pytest.raises(PriceDataError, match="daily granularity"):
        client._parse_yahoo_chart("MAT", payload)


def test_yahoo_parser_uses_adjusted_close_factor_for_ohlc() -> None:
    client = StooqPriceClient(user_agent="test", timeout_seconds=5.0)
    payload = (
        '{"chart":{"result":[{"meta":{"dataGranularity":"1d"},'
        '"timestamp":[1707523200],"indicators":{"quote":[{"open":[100.0],'
        '"high":[110.0],"low":[90.0],"close":[100.0],"volume":[1000]}],'
        '"adjclose":[{"adjclose":[50.0]}]}}]}}'
    )
    bars = client._parse_yahoo_chart("MAT", payload)
    assert bars[0].open == 50.0
    assert bars[0].high == 55.0
    assert bars[0].low == 45.0
    assert bars[0].close == 50.0
    assert bars[0].volume == 2_000.0


def test_normalize_backtest_symbol_filters_and_normalizes() -> None:
    assert normalize_backtest_symbol("brk.b") == "BRK-B"
    assert normalize_backtest_symbol("(calx)") == "CALX"
    assert normalize_backtest_symbol("NYSE: KRC") == "KRC"
    assert normalize_backtest_symbol("Z AND ZG") is None
    assert normalize_backtest_symbol("WLY/WLYB") is None


def test_stooq_price_client_wraps_invalid_url_error(monkeypatch) -> None:
    def _fake_urlopen(req, timeout):
        raise InvalidURL("bad url")

    monkeypatch.setattr(prices_module, "urlopen", _fake_urlopen)
    client = StooqPriceClient(
        user_agent="insider-alerts/0.2 (contact: sec-access@example.com)",
        timeout_seconds=5.0,
    )
    with pytest.raises(PriceDataError, match="price request failed for MAT"):
        client.fetch_history("MAT")


def test_stooq_price_client_retries_retryable_http_error(monkeypatch) -> None:
    class _FakeResponse:
        def __enter__(self) -> "_FakeResponse":
            return self

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            tb: TracebackType | None,
        ) -> None:
            return None

        def read(self) -> bytes:
            return b"Date,Open,High,Low,Close,Volume\n2026-02-11,10,11,9,10.5,1000\n"

    calls = {"count": 0}
    sleeps: list[float] = []

    def _fake_urlopen(req, timeout):
        calls["count"] += 1
        if calls["count"] == 1:
            raise HTTPError(req.full_url, 429, "Too Many Requests", hdrs=None, fp=None)
        return _FakeResponse()

    monkeypatch.setattr(prices_module, "urlopen", _fake_urlopen)
    client = StooqPriceClient(
        user_agent="insider-alerts/0.2 (contact: sec-access@example.com)",
        timeout_seconds=5.0,
        rate_limit_per_second=100.0,
        retry_attempts=2,
        retry_min_seconds=0.5,
        retry_max_seconds=0.5,
        now_fn=iter([0.0, 1.0, 2.0, 3.0]).__next__,
        sleep_fn=sleeps.append,
    )
    bars = client.fetch_history("MAT")
    assert len(bars) == 1
    assert calls["count"] == 2
    assert 0.5 in sleeps


def test_stooq_price_client_decodes_gzip_payload(monkeypatch) -> None:
    class _FakeResponse:
        headers = {"Content-Encoding": "gzip"}

        def __enter__(self) -> "_FakeResponse":
            return self

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            tb: TracebackType | None,
        ) -> None:
            return None

        def read(self) -> bytes:
            csv_bytes = b"Date,Open,High,Low,Close,Volume\n2026-02-11,10,11,9,10.5,1000\n"
            return gzip.compress(csv_bytes)

    monkeypatch.setattr(prices_module, "urlopen", lambda req, timeout: _FakeResponse())
    client = StooqPriceClient(
        user_agent="insider-alerts/0.2 (contact: sec-access@example.com)",
        timeout_seconds=5.0,
    )
    bars = client.fetch_history("MAT")
    assert len(bars) == 1
    assert bars[0].symbol == "MAT"
    assert bars[0].close == 10.5


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


def test_get_price_bar_bounds_round_trip(tmp_path) -> None:
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
    start_date, end_date = get_price_bar_bounds(db_path, symbol="SPGI")
    assert start_date == date(2026, 2, 11)
    assert end_date == date(2026, 2, 12)
