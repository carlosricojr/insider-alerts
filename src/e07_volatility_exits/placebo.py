"""Same-session width placebo for a separately registered E07 shadow comparison."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Set
from dataclasses import replace
from datetime import UTC, datetime

import rfc8785

from e07_volatility_exits.features import (
    BASE_WIDTH,
    FEATURE_CUTOFF,
    MAX_WIDTH,
    MIN_WIDTH,
    ExitWidths,
)
from insider_alerts.strategy.e07 import NEW_YORK

SESSION_PLACEBO = "E07-VOL-EXITS-SESSION-PLACEBO-V1"


class ConflictingListingWidths(ValueError):
    """Only the same-session placebo is unavailable for this session."""

    reason = "conflicting_listing_widths"


def match_widths_within_session(
    cutoff: datetime,
    widths_by_packet: Mapping[str, ExitWidths],
    *,
    qualified_packets: Set[str],
) -> dict[str, ExitWidths]:
    """Reassign a sealed pre-open cohort's width multiset across its listings.

    The caller proves and freezes the qualified packet set at the cutoff.
    Packets outside that set get their own scaled width as the placebo width.
    """
    if cutoff.utcoffset() is None:
        raise ValueError("the placebo requires an aware 09:18 New York cutoff")
    local_cutoff = cutoff.astimezone(NEW_YORK)
    if local_cutoff.time() != FEATURE_CUTOFF:
        raise ValueError("the placebo requires an aware 09:18 New York cutoff")
    if not qualified_packets <= widths_by_packet.keys():
        raise ValueError("qualified placebo packets must belong to the replay roster")

    cutoff_utc = cutoff.astimezone(UTC)
    widths_by_listing: dict[str, float] = {}
    for packet, widths in widths_by_packet.items():
        if (
            not packet
            or not widths.instrument
            or widths.cutoff.utcoffset() is None
            or widths.cutoff.astimezone(UTC) != cutoff_utc
            or not MIN_WIDTH <= widths.scaled <= MAX_WIDTH
        ):
            raise ValueError("invalid placebo packet, cutoff, listing or scaled width")
        if widths.missing is not None and widths.scaled != BASE_WIDTH:
            raise ValueError("missing forecasts require the fixed fallback width")
        if packet in qualified_packets:
            if widths.missing is not None:
                raise ValueError("a qualified placebo packet requires a usable forecast")
            prior = widths_by_listing.setdefault(widths.instrument, widths.scaled)
            if prior != widths.scaled:
                raise ConflictingListingWidths("conflicting scaled widths for the same listing")

    day = local_cutoff.date().isoformat()

    def listing_order(instrument: str) -> tuple[bytes, str]:
        canonical = rfc8785.dumps([SESSION_PLACEBO, day, instrument])
        return hashlib.sha256(canonical).digest(), instrument

    assignments = dict(
        zip(
            sorted(widths_by_listing, key=listing_order),
            sorted(widths_by_listing.values()),
            strict=True,
        )
    )
    return {
        packet: replace(
            widths,
            matched=assignments[widths.instrument]
            if packet in qualified_packets
            else widths.scaled,
        )
        for packet, widths in widths_by_packet.items()
    }
