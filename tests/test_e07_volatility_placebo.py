from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone

import pytest

from e07_volatility_exits.features import BASE_WIDTH, ExitWidths
from e07_volatility_exits.placebo import ConflictingListingWidths, match_widths_within_session
from insider_alerts.strategy.e07 import NEW_YORK

CUTOFF = datetime(2026, 11, 23, 9, 18, tzinfo=NEW_YORK)


def _cohort() -> dict[str, ExitWidths]:
    return {
        packet: ExitWidths(f"ibkr:{packet}", CUTOFF, width, BASE_WIDTH, None)
        for packet, width in (("11", 0.20), ("22", 0.05), ("33", 0.12))
    }


def test_assignment_is_frozen_and_preserves_each_listings_width_multiset() -> None:
    cases = _cohort()
    sealed = frozenset(cases)
    result = match_widths_within_session(CUTOFF, cases, qualified_packets=sealed)
    assert {packet: row.matched for packet, row in result.items()} == {
        "11": 0.05,
        "22": 0.12,
        "33": 0.20,
    }
    assert sorted(row.matched for row in result.values()) == sorted(
        row.scaled for row in cases.values()
    )
    assert all(
        result[packet] == replace(row, matched=result[packet].matched)
        for packet, row in cases.items()
    )

    reordered = dict(reversed(tuple(cases.items())))
    assert (
        match_widths_within_session(CUTOFF.astimezone(UTC), reordered, qualified_packets=sealed)
        == result
    )


def test_repeated_listings_take_one_width_and_cases_outside_the_receipt_are_untouched() -> None:
    cases = _cohort()
    sealed = frozenset(cases)
    expected = match_widths_within_session(CUTOFF, cases, qualified_packets=sealed)
    cases["duplicate"] = cases["11"]
    cases["late_snapshot"] = ExitWidths("ibkr:11", CUTOFF, BASE_WIDTH, 0.20, "late_snapshot")
    cases["outside"] = ExitWidths("ibkr:11", CUTOFF, 0.055, 0.20, None)

    result = match_widths_within_session(CUTOFF, cases, qualified_packets=sealed | {"duplicate"})
    assert all(result[packet] == row for packet, row in expected.items())
    assert result["duplicate"] == result["11"]
    assert result["late_snapshot"].matched == BASE_WIDTH
    assert result["outside"].matched == cases["outside"].scaled

    cases["duplicate"] = replace(cases["11"], scaled=0.19)
    with pytest.raises(ConflictingListingWidths) as error:
        match_widths_within_session(CUTOFF, cases, qualified_packets=sealed | {"duplicate"})
    assert error.value.reason == "conflicting_listing_widths"


@pytest.mark.parametrize(
    ("rows", "qualified"),
    [
        ({}, set()),
        ({"11": ExitWidths("ibkr:11", CUTOFF, 0.20, BASE_WIDTH, None)}, {"11"}),
        (
            {
                "1": ExitWidths("ibkr:1", CUTOFF, 0.07, BASE_WIDTH, None),
                "2": ExitWidths("ibkr:2", CUTOFF, 0.07, BASE_WIDTH, None),
            },
            {"1", "2"},
        ),
    ],
)
def test_no_eligible_degree_of_freedom_preserves_scaled_widths(
    rows: dict[str, ExitWidths], qualified: set[str]
) -> None:
    result = match_widths_within_session(CUTOFF, rows, qualified_packets=qualified)
    assert result == {packet: replace(row, matched=row.scaled) for packet, row in rows.items()}


def test_rejects_mixed_cohorts_and_membership_without_point_in_time_widths() -> None:
    cases = _cohort()
    with pytest.raises(ValueError, match="belong to the replay roster"):
        match_widths_within_session(CUTOFF, cases, qualified_packets={"absent"})
    for invalid in (
        replace(cases["11"], cutoff=CUTOFF + timedelta(days=1)),
        replace(cases["11"], cutoff=CUTOFF.replace(tzinfo=None)),
        replace(cases["11"], scaled=float("nan")),
    ):
        with pytest.raises(ValueError, match="cutoff, listing or scaled width"):
            match_widths_within_session(CUTOFF, {"11": invalid}, qualified_packets={"11"})

    fallback = replace(cases["11"], scaled=BASE_WIDTH, missing="no_snapshot")
    with pytest.raises(ValueError, match="requires a usable forecast"):
        match_widths_within_session(CUTOFF, {"11": fallback}, qualified_packets={"11"})
    with pytest.raises(ValueError, match="fixed fallback"):
        match_widths_within_session(
            CUTOFF, {"11": replace(fallback, scaled=0.20)}, qualified_packets=set()
        )

    winter_offset_during_summer = datetime(2026, 7, 1, 9, 18, tzinfo=timezone(timedelta(hours=-5)))
    for wrong_cutoff in (
        CUTOFF.replace(tzinfo=None),
        CUTOFF + timedelta(minutes=1),
        winter_offset_during_summer,
    ):
        with pytest.raises(ValueError, match="aware 09:18"):
            match_widths_within_session(wrong_cutoff, {}, qualified_packets=set())
