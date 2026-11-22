"""Point-in-time exit widths for a separate E07 shadow research variant."""

from __future__ import annotations

import hashlib
import math
import statistics
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from itertools import pairwise
from typing import Literal

from insider_alerts.strategy.e07 import NEW_YORK

LOOKBACK_CLOSES = 21
BASE_WIDTH = 0.10
MIN_WIDTH = 0.05
MAX_WIDTH = 0.20
FEATURE_CUTOFF = time(9, 18)
CONTROL_DOMAIN = b"E07-VOL-EXITS-V1|matched-width|"

MissingVolatility = Literal[
    "no_snapshot",
    "instrument_mismatch",
    "late_snapshot",
    "invalid_snapshot_clock",
    "missing_or_unexpected_sessions",
    "invalid_close",
    "zero_volatility",
]


@dataclass(frozen=True, slots=True)
class CloseSnapshot:
    """The qualified closes from one source response, on one adjustment basis."""

    instrument: str
    received_at: datetime
    closes: tuple[tuple[date, float], ...]


@dataclass(frozen=True, slots=True)
class VolatilityEstimate:
    instrument: str
    cutoff: datetime
    daily: float | None
    missing: MissingVolatility | None


def estimate_volatility(
    instrument: str,
    decision_at: datetime,
    completed_sessions: Sequence[date],
    snapshot: CloseSnapshot | None,
    *,
    previous_session_close: datetime,
) -> VolatilityEstimate:
    """Apply sample standard deviation to 20 adjacent close-to-close log returns.

    The caller supplies consecutive sessions, the official previous exchange
    close, and proof of the response's
    identity and price basis. Data gaps fall back at the width decision, never
    by shortening the window, filling a hole, or taking a later observation.
    """
    if not instrument or decision_at.utcoffset() is None:
        raise ValueError("a qualified instrument and aware decision clock are required")
    local_cutoff = decision_at.astimezone(NEW_YORK)
    if local_cutoff.time() != FEATURE_CUTOFF:
        raise ValueError("expected the 09:18 New York feature cutoff")
    if previous_session_close.utcoffset() is None:
        raise ValueError("previous session close requires an aware clock")
    local_close = previous_session_close.astimezone(NEW_YORK)
    if not time(13) <= local_close.time() <= time(16):
        raise ValueError("previous session close outside supported regular-hours range")
    sessions = tuple(completed_sessions)
    if (
        len(sessions) != LOOKBACK_CLOSES
        or sessions != tuple(sorted(set(sessions)))
        or sessions[-1] != local_close.date()
        or sessions[-1] >= local_cutoff.date()
    ):
        raise ValueError("expected an ordered calendar of 21 prior completed sessions")

    if snapshot is None:
        return VolatilityEstimate(instrument, decision_at, None, "no_snapshot")
    if snapshot.instrument != instrument:
        return VolatilityEstimate(instrument, decision_at, None, "instrument_mismatch")
    finalized_at = previous_session_close.astimezone(UTC) + timedelta(minutes=1)
    if snapshot.received_at.utcoffset() is None:
        return VolatilityEstimate(instrument, decision_at, None, "invalid_snapshot_clock")
    received_at = snapshot.received_at.astimezone(UTC)
    if received_at < finalized_at:
        return VolatilityEstimate(instrument, decision_at, None, "invalid_snapshot_clock")
    if received_at >= decision_at.astimezone(UTC):
        return VolatilityEstimate(instrument, decision_at, None, "late_snapshot")
    if tuple(session for session, _ in snapshot.closes) != sessions:
        return VolatilityEstimate(instrument, decision_at, None, "missing_or_unexpected_sessions")

    closes = tuple(close for _, close in snapshot.closes)
    if any(not math.isfinite(close) or close <= 0.0 for close in closes):
        return VolatilityEstimate(instrument, decision_at, None, "invalid_close")
    logs = tuple(math.log(close) for close in closes)
    sigma = statistics.stdev(later - earlier for earlier, later in pairwise(logs))
    if sigma == 0.0:
        return VolatilityEstimate(instrument, decision_at, None, "zero_volatility")
    return VolatilityEstimate(instrument, decision_at, sigma, None)


def _width(sigma: float, reference: float) -> float:
    return min(MAX_WIDTH, max(MIN_WIDTH, BASE_WIDTH * (sigma / reference)))


@dataclass(frozen=True, slots=True)
class ExitCalibration:
    """A fitted reference and control distribution from a closed development prefix."""

    daily_reference: float
    matched_widths: tuple[float, ...]

    def __post_init__(self) -> None:
        if not math.isfinite(self.daily_reference) or self.daily_reference <= 0:
            raise ValueError("invalid calibration reference")
        if not self.matched_widths or any(
            not MIN_WIDTH <= width <= MAX_WIDTH for width in self.matched_widths
        ):
            raise ValueError("invalid matched-width distribution")
        if self.matched_widths != tuple(sorted(self.matched_widths)):
            raise ValueError("matched-width distribution must be in canonical order")


def calibrate_exits(development: Sequence[float]) -> ExitCalibration:
    """Fit one median using daily volatility estimates alone."""
    if not development or any(not math.isfinite(value) or value <= 0 for value in development):
        raise ValueError("calibration requires qualified positive development estimates")
    reference = statistics.median(development)
    return ExitCalibration(
        reference, tuple(sorted(_width(value, reference) for value in development))
    )


@dataclass(frozen=True, slots=True)
class ExitWidths:
    instrument: str
    cutoff: datetime
    scaled: float
    matched: float
    missing: MissingVolatility | None


def decide_widths(
    calibration: ExitCalibration,
    estimate: VolatilityEstimate,
    packet_id: str,
) -> ExitWidths:
    """Choose immutable widths keyed to the same ordinal-free candidate identity."""
    if not packet_id:
        raise ValueError("canonical packet identity is required")
    sigma = estimate.daily
    if sigma is None and estimate.missing is not None:
        return ExitWidths(
            estimate.instrument, estimate.cutoff, BASE_WIDTH, BASE_WIDTH, estimate.missing
        )
    if sigma is None or not math.isfinite(sigma) or sigma <= 0 or estimate.missing is not None:
        raise ValueError("inconsistent volatility estimate")

    draw = int.from_bytes(hashlib.sha256(CONTROL_DOMAIN + packet_id.encode("utf-8")).digest())
    return ExitWidths(
        estimate.instrument,
        estimate.cutoff,
        _width(sigma, calibration.daily_reference),
        calibration.matched_widths[draw % len(calibration.matched_widths)],
        None,
    )
