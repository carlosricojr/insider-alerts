import gzip
import json
from datetime import UTC, date, datetime
from http.client import InvalidURL
from types import SimpleNamespace, TracebackType
from urllib.error import HTTPError

import pytest

from insider_alerts.review import market_context as market_context_module
from insider_alerts.review.market_context import (
    DailyMarketDataClient,
    MarketContextError,
    MarketSnapshot,
    get_market_snapshot,
    upsert_market_snapshot,
)

_ORIGINAL_IB_FETCH = market_context_module._IBBarSource.fetch.__func__


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


def _yahoo_body(rows: list[tuple[date, float, float]]) -> bytes:
    """Build a Yahoo chart-API payload for the fallback HTTP path."""
    stamps = [
        int(datetime(d.year, d.month, d.day, tzinfo=UTC).timestamp()) for d, _, _ in rows
    ]
    return json.dumps(
        {
            "chart": {
                "error": None,
                "result": [
                    {
                        "timestamp": stamps,
                        "indicators": {
                            "quote": [
                                {
                                    "close": [c for _, c, _ in rows],
                                    "volume": [v for _, _, v in rows],
                                }
                            ]
                        },
                    }
                ],
            }
        }
    ).encode()


@pytest.fixture(autouse=True)
def _no_live_ib(monkeypatch):
    """Keep these unit tests hermetic: never touch the real IB Gateway on this host."""
    monkeypatch.setattr(
        market_context_module._IBBarSource, "fetch", classmethod(lambda cls, symbol: {})
    )
    monkeypatch.setattr(market_context_module._IBBarSource, "_unavailable_reason", None)


def test_daily_market_data_client_marks_shock_day() -> None:
    class _FakeClient(DailyMarketDataClient):
        def _fetch_bars(self, symbol: str):
            assert symbol == "SPGI"
            return (
                {
                    date(2026, 2, 10): (401.08, 10888451.0),
                    date(2026, 2, 11): (390.76, 5174841.0),
                },
                "test",
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


def test_daily_market_data_client_configures_ib_endpoint() -> None:
    DailyMarketDataClient(
        user_agent="test",
        timeout_seconds=1.0,
        ib_gateway_host="custom-gateway",
        ib_gateway_port=4999,
        ib_client_id=222,
    )

    assert market_context_module._IBBarSource._host == "custom-gateway"
    assert market_context_module._IBBarSource._port == 4999
    assert market_context_module._IBBarSource._client_id == 222


def test_ib_connection_sets_a_bounded_synchronous_request_timeout(monkeypatch) -> None:
    import ib_async

    class FakeIB:
        def __init__(self) -> None:
            self.RequestTimeout = 0.0

        def connect(self, *_args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            assert kwargs["timeout"] == 10

        def reqMarketDataType(self, market_data_type: int) -> None:
            assert market_data_type == 1

        def isConnected(self) -> bool:
            return True

    monkeypatch.setattr(ib_async, "IB", FakeIB)
    monkeypatch.setattr(market_context_module._IBBarSource, "_ib", None)

    ib = market_context_module._IBBarSource._connect()

    assert ib is not None
    assert ib.RequestTimeout == 10.0


def test_ib_market_requests_are_bounded_for_watchdog_budget(monkeypatch) -> None:
    historical_timeouts: list[float] = []

    class FakeIB:
        RequestTimeout = 0.0

        def isConnected(self) -> bool:
            return True

        def qualifyContracts(self, contract):  # type: ignore[no-untyped-def]
            contract.conId = 123
            return [contract]

        def reqHistoricalData(self, *_args, **kwargs):  # type: ignore[no-untyped-def]
            historical_timeouts.append(float(kwargs["timeout"]))
            return [
                SimpleNamespace(
                    date=date(2026, 2, 11),
                    close=41.0,
                    volume=200.0,
                )
            ]

    fake_ib = FakeIB()
    monkeypatch.setattr(
        market_context_module._IBBarSource,
        "_connect",
        classmethod(lambda cls: fake_ib),
    )

    bars = _ORIGINAL_IB_FETCH(market_context_module._IBBarSource, "SPGI")

    assert bars[date(2026, 2, 11)] == (41.0, 200.0)
    assert historical_timeouts == [10.0]


def test_daily_market_data_client_url_encodes_symbol_with_spaces(monkeypatch) -> None:
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
            return _yahoo_body(
                [
                    (date(2026, 2, 10), 40.0, 100.0),
                    (date(2026, 2, 11), 41.0, 200.0),
                ]
            )

    captured: dict[str, object] = {}

    def _fake_urlopen(req, timeout):
        captured["url"] = req.full_url
        captured["timeout"] = timeout
        return _FakeResponse()

    monkeypatch.setattr(market_context_module, "urlopen", _fake_urlopen)
    client = DailyMarketDataClient(
        user_agent="insider-alerts/0.2 (contact: sec-access@example.com)",
        timeout_seconds=3.0,
        shock_drop_threshold=0.08,
    )
    snapshot = client.fetch_snapshot("Z AND ZG", trade_date=date(2026, 2, 11))
    assert captured["url"] == (
        "https://query1.finance.yahoo.com/v8/finance/chart/Z%20AND%20ZG?range=10d&interval=1d"
    )
    assert captured["timeout"] == 3.0
    assert snapshot is not None
    assert snapshot.symbol == "Z AND ZG"
    assert snapshot.source == "yahoo"


def test_daily_market_data_client_decodes_gzip_response(monkeypatch) -> None:
    payload = _yahoo_body(
        [
            (date(2026, 2, 10), 40.0, 100.0),
            (date(2026, 2, 11), 41.0, 200.0),
        ]
    )

    class _FakeResponse:
        headers = {"Content-Encoding": "gzip"}

        def __enter__(self) -> "_FakeResponse":
            return self

        def __exit__(self, *args) -> None:
            return None

        def read(self) -> bytes:
            return gzip.compress(payload)

    monkeypatch.setattr(market_context_module, "urlopen", lambda *args, **kwargs: _FakeResponse())
    client = DailyMarketDataClient(
        user_agent="insider-alerts/0.2 (contact: sec-access@example.com)",
        timeout_seconds=3.0,
        shock_drop_threshold=0.08,
    )

    snapshot = client.fetch_snapshot("SPGI", trade_date=date(2026, 2, 11))

    assert snapshot is not None
    assert snapshot.source == "yahoo"


def test_daily_market_data_client_retries_truncated_gzip(monkeypatch) -> None:
    calls = 0

    class _FakeResponse:
        headers = {"Content-Encoding": "gzip"}

        def __enter__(self) -> "_FakeResponse":
            return self

        def __exit__(self, *args) -> None:
            return None

        def read(self) -> bytes:
            return b"\x1f\x8b"

    def _fake_urlopen(*args, **kwargs):
        nonlocal calls
        calls += 1
        return _FakeResponse()

    monkeypatch.setattr(market_context_module, "urlopen", _fake_urlopen)
    client = DailyMarketDataClient(
        user_agent="test",
        timeout_seconds=1.0,
        retry_attempts=2,
        retry_min_seconds=0.0,
    )

    with pytest.raises(MarketContextError, match="market data request failed"):
        client._download_text("https://example.test", symbol="TEST")

    assert calls == 2


def test_daily_market_data_client_applies_retry_policy_to_ib(monkeypatch) -> None:
    calls: list[str] = []
    sleeps: list[float] = []

    def _fake_fetch(cls, symbol):  # type: ignore[no-untyped-def]
        calls.append(symbol)
        if len(calls) == 1:
            cls._unavailable_reason = "transient IB failure"
            return {}
        cls._unavailable_reason = None
        return {date(2026, 2, 11): (41.0, 200.0)}

    ticks = iter(float(value) for value in range(20))
    monkeypatch.setattr(market_context_module._IBBarSource, "fetch", classmethod(_fake_fetch))
    client = DailyMarketDataClient(
        user_agent="insider-alerts/0.2 (contact: sec-access@example.com)",
        timeout_seconds=3.0,
        rate_limit_per_second=100.0,
        retry_attempts=2,
        retry_min_seconds=0.5,
        retry_max_seconds=0.5,
        now_fn=ticks.__next__,
        sleep_fn=sleeps.append,
    )

    bars, source = client._fetch_bars("SPGI")

    assert calls == ["SPGI", "SPGI"]
    assert source == "ibkr"
    assert bars[date(2026, 2, 11)] == (41.0, 200.0)
    assert 0.5 in sleeps


def test_daily_market_data_client_wraps_invalid_url_error(monkeypatch) -> None:
    def _fake_urlopen(req, timeout):
        raise InvalidURL("bad url")

    monkeypatch.setattr(market_context_module, "urlopen", _fake_urlopen)
    client = DailyMarketDataClient(
        user_agent="insider-alerts/0.2 (contact: sec-access@example.com)",
        timeout_seconds=5.0,
    )
    with pytest.raises(MarketContextError, match="market data request failed for MAT"):
        client.fetch_snapshot("MAT", trade_date=date(2026, 2, 11))


def test_daily_market_data_client_retries_retryable_http_error(monkeypatch) -> None:
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
            return _yahoo_body(
                [
                    (date(2026, 2, 10), 40.0, 100.0),
                    (date(2026, 2, 11), 41.0, 200.0),
                ]
            )

    calls = {"count": 0}
    sleeps: list[float] = []

    def _fake_urlopen(req, timeout):
        calls["count"] += 1
        if calls["count"] == 1:
            raise HTTPError(req.full_url, 429, "Too Many Requests", hdrs=None, fp=None)
        return _FakeResponse()

    monkeypatch.setattr(market_context_module, "urlopen", _fake_urlopen)
    tick = 0

    def _now() -> float:
        nonlocal tick
        tick += 1
        return float(tick)

    client = DailyMarketDataClient(
        user_agent="insider-alerts/0.2 (contact: sec-access@example.com)",
        timeout_seconds=5.0,
        rate_limit_per_second=100.0,
        retry_attempts=2,
        retry_min_seconds=0.5,
        retry_max_seconds=0.5,
        now_fn=_now,
        sleep_fn=sleeps.append,
    )
    snapshot = client.fetch_snapshot("MAT", trade_date=date(2026, 2, 11))
    assert snapshot is not None
    assert calls["count"] == 2
    assert 0.5 in sleeps
