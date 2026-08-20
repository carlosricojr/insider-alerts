from datetime import UTC, date, datetime

from insider_alerts.backtest.intraday_prices import (
    _cached_session_complete,
    _mark_session_complete,
    completed_minute_bar_sessions,
    filter_completed_minute_bars,
    upsert_minute_bars,
)
from insider_alerts.backtest.signal_study import MinuteBar


def _bar(minute: int) -> MinuteBar:
    return MinuteBar(
        symbol="MAT",
        timestamp=datetime(2026, 1, 5, 14, minute, tzinfo=UTC),
        open=10.0,
        high=10.1,
        low=9.9,
        close=10.0,
        volume=100.0,
    )


def test_minute_session_requires_completion_marker_and_full_recorded_count(tmp_path) -> None:
    db_path = str(tmp_path / "minute.db")
    session_date = date(2026, 1, 5)
    upsert_minute_bars(db_path, bars=[_bar(30)])

    assert not _cached_session_complete(db_path, symbol="MAT", session_date=session_date)

    _mark_session_complete(
        db_path,
        symbol="MAT",
        session_date=session_date,
        bar_count=2,
    )
    assert not _cached_session_complete(db_path, symbol="MAT", session_date=session_date)

    upsert_minute_bars(db_path, bars=[_bar(31)])

    assert _cached_session_complete(db_path, symbol="MAT", session_date=session_date)
    assert completed_minute_bar_sessions(
        db_path,
        requests=[("MAT", session_date), ("SPY", session_date)],
    ) == {("MAT", session_date)}


def test_filter_completed_minute_bars_excludes_unverified_sessions() -> None:
    complete = _bar(30)
    incomplete = MinuteBar(
        symbol="MAT",
        timestamp=datetime(2026, 1, 6, 14, 30, tzinfo=UTC),
        open=10.0,
        high=10.1,
        low=9.9,
        close=10.0,
        volume=100.0,
    )

    filtered = filter_completed_minute_bars(
        {"MAT": [complete, incomplete]},
        completed_sessions={("MAT", date(2026, 1, 5))},
    )

    assert filtered == {"MAT": [complete]}
