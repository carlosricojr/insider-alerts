from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

from e07_volatility_exits.auction import Auction, AuctionFill, EntryConvention, EntryPath
from e07_volatility_exits.features import BASE_WIDTH, FEATURE_CUTOFF, ExitWidths
from e07_volatility_exits.replay import ReplayAgreementError, ReplayCandidate, ReplayCosts
from insider_alerts.backtest.models import DailyBar
from insider_alerts.research import calendar_v2
from ops.research.e07_volatility_settlement import (
    DTC_2027_SCHEDULE_SHA256,
    DTC_SCHEDULE_SHA256,
    NON_SETTLEMENT_WEEKDAYS_2027,
    NSCC_2027_SCHEDULE_SHA256,
    NSCC_VETERANS_DAY_SHA256,
    SOURCE_STATUS,
    SOURCE_STATUS_2027,
    UNRESOLVED_SETTLEMENT_WEEKDAYS_2027,
    CashRelease,
    SettlementSchedule,
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


def test_archived_2027_notices_and_carried_sources_are_bound() -> None:
    path = SOURCE_DIR / "e07-cash-settlement-2027.provenance.json"
    provenance = json.loads(path.read_bytes())
    assert provenance["historical_possession_claimed"] is False
    assert provenance["contract"] == "e07-published-regular-way-us-equity-t1-v2"
    assert provenance["schedule_status"] == SOURCE_STATUS_2027
    assert provenance["source_status_by_period"] == {
        "2026_q4": SOURCE_STATUS,
        "2027": SOURCE_STATUS_2027,
    }
    assert provenance["base_source_sha256"] == [DTC_SCHEDULE_SHA256, NSCC_VETERANS_DAY_SHA256]
    assert provenance["covering_from"] == "2026-10-01"
    assert provenance["covering_through"] == "2027-12-31"
    expected = {
        "dtc-settlement-holidays-2027.pdf": DTC_2027_SCHEDULE_SHA256,
        "nscc-equity-clearing-holidays-2027.pdf": NSCC_2027_SCHEDULE_SHA256,
    }
    assert {source["artifact"]: source["sha256"] for source in provenance["sources"]} == expected
    for source in provenance["sources"]:
        raw = (SOURCE_DIR / source["artifact"]).read_bytes()
        assert len(raw) == source["bytes"]
        assert hashlib.sha256(raw).hexdigest() == source["sha256"]
        assert source["published_date"] == "2026-11-09"
        assert datetime.fromisoformat(source["retrieved_file_completed_at_utc"]) > datetime(
            2026, 11, 9, tzinfo=UTC
        )
    assert set(map(str, NON_SETTLEMENT_WEEKDAYS_2027)) == (
        set(provenance["published_2027_full_closures"])
        | set(provenance["published_2027_no_settlement_services"])
    )
    assert (
        list(map(str, UNRESOLVED_SETTLEMENT_WEEKDAYS_2027))
        == provenance["published_2027_limited_unresolved"]
    )
    assert set(NON_SETTLEMENT_WEEKDAYS_2027).isdisjoint(UNRESOLVED_SETTLEMENT_WEEKDAYS_2027)


@pytest.mark.parametrize(
    ("release", "digest"),
    [
        ("settlement_date", "7721684a1f301c965fde90ca65b62668f3ad9b235735914412d2b01eba2bce58"),
        (
            "next_exchange_open_after_settlement",
            "67569565cfbe1f4cadd5a3f4d197d6691cf03b8787fe3fc3c0eac65d169bc83f",
        ),
    ],
)
def test_default_legacy_settlement_digest_is_stable(
    version: calendar_v2.Version, release: CashRelease, digest: str
) -> None:
    days = (date(2026, 11, 10), date(2026, 11, 11))
    previous = date(2026, 11, 9)
    old = bind_settlement(version, days, previous_session=previous, release=release)
    assert old.digest == digest
    assert old == bind_settlement(
        version, days, previous_session=previous, release=release, schedule="q4_2026"
    )
    extended = bind_settlement(
        version, days, previous_session=previous, release=release, schedule="through_2027"
    )
    assert extended.settlement_dates == old.settlement_dates
    assert extended.cash_release_dates == old.cash_release_dates
    assert extended.digest != old.digest


@pytest.mark.parametrize(
    ("previous", "trade", "settled", "stressed_release"),
    [
        ("2026-12-29", "2026-12-30", "2026-12-31", "2027-01-04"),
        ("2026-12-30", "2026-12-31", "2027-01-04", "2027-01-05"),
        ("2027-01-14", "2027-01-15", "2027-01-19", "2027-01-20"),
        ("2027-02-11", "2027-02-12", "2027-02-16", "2027-02-17"),
        ("2027-03-23", "2027-03-24", "2027-03-25", "2027-03-29"),
        ("2027-05-27", "2027-05-28", "2027-06-01", "2027-06-02"),
        ("2027-06-15", "2027-06-16", "2027-06-17", "2027-06-21"),
        ("2027-07-01", "2027-07-02", "2027-07-06", "2027-07-07"),
        ("2027-09-02", "2027-09-03", "2027-09-07", "2027-09-08"),
        ("2027-10-07", "2027-10-08", "2027-10-12", "2027-10-13"),
        ("2027-10-08", "2027-10-11", "2027-10-12", "2027-10-13"),
        ("2027-11-09", "2027-11-10", "2027-11-12", "2027-11-15"),
        ("2027-11-10", "2027-11-11", "2027-11-12", "2027-11-15"),
        ("2027-11-23", "2027-11-24", "2027-11-26", "2027-11-29"),
        ("2027-12-21", "2027-12-22", "2027-12-23", "2027-12-27"),
    ],
)
def test_extended_schedule_distinguishes_settlement_and_exchange_release(
    version: calendar_v2.Version, previous: str, trade: str, settled: str, stressed_release: str
) -> None:
    prev, trade_day, due, later = map(
        date.fromisoformat, (previous, trade, settled, stressed_release)
    )
    baseline = bind_settlement(
        version,
        (trade_day,),
        previous_session=prev,
        release="settlement_date",
        schedule="through_2027",
    )
    stressed = bind_settlement(
        version,
        (trade_day,),
        previous_session=prev,
        release="next_exchange_open_after_settlement",
        schedule="through_2027",
    )
    assert baseline.settlement_dates == stressed.settlement_dates == {trade_day: due}
    assert baseline.cash_release_dates == {trade_day: due}
    assert stressed.cash_release_dates == {trade_day: later}
    assert baseline.source_status == stressed.source_status == SOURCE_STATUS_2027
    assert baseline.digest != stressed.digest


@pytest.mark.parametrize("release", ["settlement_date", "next_exchange_open_after_settlement"])
@pytest.mark.parametrize(
    ("trade", "limited"),
    [
        ("2027-03-25", "2027-03-26"),
        ("2027-06-17", "2027-06-18"),
        ("2027-12-23", "2027-12-24"),
    ],
)
def test_first_possible_limited_day_requires_service_specific_notice(
    version: calendar_v2.Version, release: CashRelease, trade: str, limited: str
) -> None:
    trade_day = date.fromisoformat(trade)
    with pytest.raises(
        ValueError, match=f"equity settlement on {limited} requires a service-specific notice"
    ):
        bind_settlement(
            version,
            (trade_day,),
            previous_session=trade_day - timedelta(days=1),
            release=release,
            schedule="through_2027",
        )


def test_extended_schedule_keeps_strict_coverage_limits(version: calendar_v2.Version) -> None:
    inputs = {"release": "settlement_date", "schedule": "through_2027"}
    with pytest.raises(ValueError, match="outside reviewed settlement coverage"):
        bind_settlement(version, (date(2026, 9, 30),), previous_session=date(2026, 9, 29), **inputs)
    with pytest.raises(ValueError, match="outside reviewed settlement coverage"):
        bind_settlement(version, (date(2028, 1, 3),), previous_session=date(2027, 12, 31), **inputs)
    with pytest.raises(ValueError, match="settlement after 2027-12-31 exceeds"):
        bind_settlement(
            version, (date(2027, 12, 31),), previous_session=date(2027, 12, 30), **inputs
        )
    end = bind_settlement(
        version, (date(2027, 12, 30),), previous_session=date(2027, 12, 29), **inputs
    )
    assert end.cash_release_dates == {date(2027, 12, 30): date(2027, 12, 31)}
    with pytest.raises(ValueError, match="cash release after 2027-12-31 exceeds"):
        bind_settlement(
            version,
            (date(2027, 12, 30),),
            previous_session=date(2027, 12, 29),
            release="next_exchange_open_after_settlement",
            schedule="through_2027",
        )
    with pytest.raises(ValueError, match="unknown settlement schedule"):
        bind_settlement(
            version,
            (date(2027, 12, 30),),
            previous_session=date(2027, 12, 29),
            release="settlement_date",
            schedule=cast(SettlementSchedule, "unreviewed"),
        )


@pytest.mark.parametrize("compare", [compare_with_settlement, compare_primary_with_settlement])
def test_replays_require_full_calendar_coverage_through_limited_service_dates(
    version: calendar_v2.Version, compare: Callable[..., object]
) -> None:
    days = tuple(row.day for row in calendar_v2.bind(version, date(2027, 3, 18), count=12).sessions)
    with pytest.raises(ValueError, match="equity settlement on 2027-03-26 requires"):
        compare(
            version,
            days,
            (),
            {},
            action_coverage={},
            opening_evidence={},
            entry_convention=EntryConvention(timedelta(seconds=1), timedelta(), False),
            initial_cash=Decimal("475"),
            costs=ReplayCosts(Decimal("0"), Decimal("0")),
            previous_session=date(2027, 3, 17),
            release="settlement_date",
            schedule="through_2027",
        )


@pytest.mark.parametrize("start", [date(2026, 11, 10), date(2026, 12, 24)])
def test_primary_and_diagnostic_share_the_selected_schedule(
    version: calendar_v2.Version, start: date
) -> None:
    days = tuple(row.day for row in calendar_v2.bind(version, start, count=12).sessions)
    inputs = dict(
        action_coverage={},
        opening_evidence={},
        entry_convention=EntryConvention(timedelta(seconds=1), timedelta(), False),
        initial_cash=Decimal("475"),
        costs=ReplayCosts(Decimal("0"), Decimal("0")),
        previous_session=start - timedelta(days=1),
        release="next_exchange_open_after_settlement",
    )
    primary = compare_primary_with_settlement(
        version, days, (), {}, schedule="through_2027", **inputs
    )
    diagnostic = compare_with_settlement(version, days, (), {}, schedule="through_2027", **inputs)
    diagnostic.require_primary_agreement(primary)
    if start.month == 11:
        legacy = compare_with_settlement(version, days, (), {}, **inputs)
        with pytest.raises(ReplayAgreementError, match="different settlement binding"):
            legacy.require_primary_agreement(primary)
    else:
        with pytest.raises(ValueError, match="outside reviewed settlement coverage"):
            compare_with_settlement(version, days, (), {}, **inputs)
