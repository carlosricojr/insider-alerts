from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from e07_volatility_exits.auction import Auction, AuctionFill, EntryConvention, EntryPath
from e07_volatility_exits.features import BASE_WIDTH, FEATURE_CUTOFF, ExitWidths
from e07_volatility_exits.replay import ReplayAgreementError, ReplayCandidate, ReplayCosts
from insider_alerts.backtest.models import DailyBar
from insider_alerts.research import calendar_v2
from ops.research.e07_volatility_settlement import (
    DTC_SCHEDULE_SHA256,
    NSCC_VETERANS_DAY_SHA256,
    SOURCE_STATUS,
    bind_settlement,
    compare_primary_with_settlement,
    compare_with_settlement,
)

SOURCE_DIR = Path(__file__).resolve().parents[1] / "docs/research/calendar-sources"


@pytest.fixture
def calendar_journal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> calendar_v2.CalendarJournal:
    observed = datetime(2026, 10, 26, 20, tzinfo=UTC)
    monkeypatch.setattr(calendar_v2, "_now", lambda: observed + timedelta(days=40))
    journal = tmp_path / "exchange.db"
    calendar_v2.create_journal(
        journal,
        baseline_pdf=(SOURCE_DIR / "nyse-holidays-2026-2028.pdf").read_bytes(),
        review_reference="test-fixture",
        observed_at=observed,
    )
    return calendar_v2.CalendarJournal(journal)


@pytest.fixture
def version(calendar_journal: calendar_v2.CalendarJournal) -> calendar_v2.Version:
    return calendar_journal.versions()[0]


def test_archived_notices_are_bound_to_the_projection() -> None:
    expected = {
        "dtc-settlement-holidays-2026.pdf": DTC_SCHEDULE_SHA256,
        "nscc-veterans-day-risk-2026.pdf": NSCC_VETERANS_DAY_SHA256,
    }
    path = SOURCE_DIR / "e07-cash-settlement-2026.provenance.json"
    provenance = json.loads(path.read_bytes())
    assert provenance["historical_possession_claimed"] is False
    assert provenance["schedule_status"] == SOURCE_STATUS
    assert {source["artifact"]: source["sha256"] for source in provenance["sources"]} == expected
    for source in provenance["sources"]:
        raw = (SOURCE_DIR / source["artifact"]).read_bytes()
        assert len(raw) == source["bytes"]
        assert hashlib.sha256(raw).hexdigest() == source["sha256"]


@pytest.mark.parametrize(
    ("previous", "trade", "settled"),
    [
        ("2026-10-08", "2026-10-09", "2026-10-13"),
        ("2026-10-09", "2026-10-12", "2026-10-13"),
        ("2026-11-09", "2026-11-10", "2026-11-12"),
        ("2026-11-10", "2026-11-11", "2026-11-12"),
        ("2026-11-24", "2026-11-25", "2026-11-27"),
        ("2026-11-25", "2026-11-27", "2026-11-30"),
        ("2026-12-23", "2026-12-24", "2026-12-28"),
    ],
)
def test_regular_way_clearing_dates(
    version: calendar_v2.Version, previous: str, trade: str, settled: str
) -> None:
    prev, trade_day, settlement = map(date.fromisoformat, (previous, trade, settled))
    binding = bind_settlement(
        version, (trade_day,), previous_session=prev, release="settlement_date"
    )
    assert binding.settlement_dates == binding.cash_release_dates == {trade_day: settlement}
    assert binding.exchange_version == version.digest
    assert binding.previous_session == prev
    assert binding.source_status == "anticipated_annual_schedule_operational_notices_unverified"
    assert binding == bind_settlement(
        version, (trade_day,), previous_session=prev, release="settlement_date"
    )
    assert (
        binding.digest
        != bind_settlement(
            replace(version, digest="0" * 64),
            (trade_day,),
            previous_session=prev,
            release="settlement_date",
        ).digest
    )


def test_missing_session_previous_session_and_coverage_fail_closed(
    version: calendar_v2.Version,
) -> None:
    with pytest.raises(ValueError, match="consecutive exchange sessions"):
        compare_with_settlement(
            version,
            (date(2026, 11, 10), date(2026, 11, 12)),
            (),
            {},
            action_coverage={},
            opening_evidence={},
            entry_convention=EntryConvention(timedelta(seconds=1), timedelta(), False),
            initial_cash=Decimal("475"),
            costs=ReplayCosts(Decimal("0"), Decimal("0")),
            previous_session=date(2026, 11, 9),
            release="settlement_date",
        )
    with pytest.raises(ValueError, match="consecutive exchange sessions"):
        bind_settlement(
            version,
            (date(2026, 11, 12),),
            previous_session=date(2026, 11, 10),
            release="settlement_date",
        )
    with pytest.raises(ValueError, match="unknown exchange session"):
        bind_settlement(
            version,
            (date(2026, 10, 1),),
            previous_session=date(2025, 12, 31),
            release="settlement_date",
        )
    with pytest.raises(ValueError, match="replay sessions outside reviewed settlement coverage"):
        bind_settlement(
            version,
            (date(2026, 9, 30),),
            previous_session=date(2026, 9, 29),
            release="settlement_date",
        )
    with pytest.raises(ValueError, match="settlement after 2026-12-31 exceeds"):
        bind_settlement(
            version,
            (date(2026, 12, 31),),
            previous_session=date(2026, 12, 30),
            release="settlement_date",
        )
    with pytest.raises(ValueError, match="cash release after 2026-12-31 exceeds"):
        bind_settlement(
            version,
            (date(2026, 12, 30),),
            previous_session=date(2026, 12, 29),
            release="next_exchange_open_after_settlement",
        )


@pytest.mark.parametrize("state", ["closed", "unknown"])
def test_requires_clearing_evidence_for_later_exchange_exception(
    calendar_journal: calendar_v2.CalendarJournal, state: calendar_v2.NoticeKind
) -> None:
    version = calendar_journal.versions()[0]
    cutoff = datetime(2026, 11, 10, 22, tzinfo=UTC)
    notice = calendar_v2.Notice(
        state,
        (date(2026, 11, 12),),
        "synthetic exchange notice",
        b"synthetic test fixture",
        "https://www.nyse.com/synthetic-test",
        cutoff.date(),
        "test-fixture",
    )
    calendar_journal.append_notice(notice, expected_parent=version.digest, observed_at=cutoff)
    corrected = calendar_journal.versions()[-1]
    with pytest.raises(ValueError, match="settlement on 2026-11-12 requires separate"):
        bind_settlement(
            corrected,
            (date(2026, 11, 10),),
            previous_session=date(2026, 11, 9),
            release="settlement_date",
        )


def test_replay_gates_cash_on_the_holiday_and_extra_session_stress(
    version: calendar_v2.Version,
) -> None:
    sessions = tuple(
        row.day for row in calendar_v2.bind(version, date(2026, 11, 10), count=14).sessions
    )
    previous = date(2026, 11, 9)
    primary = bind_settlement(
        version, sessions, previous_session=previous, release="settlement_date"
    )
    delayed = bind_settlement(
        version, sessions, previous_session=previous, release="next_exchange_open_after_settlement"
    )
    assert primary.digest != delayed.digest
    assert primary.settlement_dates == delayed.settlement_dates
    assert delayed.cash_release_dates[sessions[0]] == date(2026, 11, 13)

    def candidate(day: date, index: int) -> ReplayCandidate:
        instrument = str(index)
        cutoff = datetime.combine(day, FEATURE_CUTOFF, calendar_v2.NY)
        return ReplayCandidate(
            f"case-{index}",
            f"0000000001-26-{index:06}",
            f"ABC{index}",
            datetime.combine(day, time(9), calendar_v2.NY),
            instrument,
            day,
            10.0,
            ExitWidths(instrument, cutoff, BASE_WIDTH, BASE_WIDTH, None),
        )

    candidates = [candidate(day, index) for index, day in enumerate(sessions[:4], 1)]
    bars = {
        (row.instrument, day): DailyBar(row.symbol, day, 10.0, 10.0, 10.0, 10.0, 100_000)
        for row in candidates
        for day in sessions
        if day >= row.entry_day
    }
    key = (candidates[0].instrument, sessions[0])
    bars[key] = replace(bars[key], high=11.0)
    delay = timedelta(seconds=1)
    opening: dict[tuple[str, date], Auction] = {}
    for case in candidates:
        day = version.day(case.entry_day)
        assert day.opens_at is not None and day.closes_at is not None
        entry_key = (case.instrument, case.entry_day)
        bar = bars[entry_key]
        opening[entry_key] = AuctionFill(
            "9" * 64,
            10.0,
            day.opens_at,
            day.opens_at + delay,
            day.opens_at + delay,
            day.closes_at,
            day.closes_at,
            EntryPath(
                case.instrument,
                case.symbol,
                case.entry_day,
                bar.open,
                bar.open,
                bar.low,
                bar.high,
                bar.close,
            ),
        )
    observed = []
    for binding in (primary, delayed):
        inputs = dict(
            action_coverage={
                (candidate.instrument, day): () for candidate in candidates for day in sessions
            },
            opening_evidence=opening,
            entry_convention=EntryConvention(delay, timedelta(), False),
            initial_cash=Decimal("265"),
            costs=ReplayCosts(Decimal("0"), Decimal("0")),
            previous_session=previous,
            release=binding.release,
        )
        result_with_binding = compare_with_settlement(version, sessions, candidates, bars, **inputs)
        primary_result = compare_primary_with_settlement(
            version, sessions, candidates, bars, **inputs
        )
        result_with_binding.require_primary_agreement(primary_result)
        assert result_with_binding.settlement.digest == binding.digest
        assert result_with_binding.replay.paired_daily_pnl == primary_result.replay.paired_daily_pnl
        other_binding = delayed if binding == primary else primary
        with pytest.raises(ReplayAgreementError, match="different settlement binding"):
            result_with_binding.require_primary_agreement(
                replace(primary_result, settlement=other_binding)
            )
        result = result_with_binding.replay
        observed.append(tuple(row.entries[0].state for row in result.fixed.sessions[:4]))
        assert result.fixed.sessions[0].unsettled_cash == Decimal("220")
    assert observed[0] == ("opened", "cash_suppressed", "opened", "cash_suppressed")
    assert observed[1] == ("opened", "cash_suppressed", "cash_suppressed", "opened")
