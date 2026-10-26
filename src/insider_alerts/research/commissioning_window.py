"""Frozen acquisition dates and denominators, without strategy outcome evaluation."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

from insider_alerts.execution.observer import valid_symbol
from insider_alerts.research import calendar_v2 as calendar
from insider_alerts.research.commissioning_source import timestamp
from insider_alerts.strategy.e07 import entry_session

FORMAT = "acquisition-commissioning-window-v1"
HOURS_SHA256 = "3960e64cd7c9581c2e7a6d3f602342344eba1fac7b7476d51ddabb32b343fe11"
HOURS_OBSERVED = "2026-10-26T20:23:48.162482Z"


@dataclass(frozen=True)
class FrozenE07:
    """Protocol values of the unchanged control; only its entry clock is used here."""

    lottery_salt: str = "E07-F00-live-canary-v1"
    entry_submission_deadline: time = time(9, 20)
    min_price: float = 2.0
    max_price: float = 200.0
    min_median_dollar_volume_20d: float = 500_000.0
    slot_budget: float = 200.0
    stop_loss_pct: float = 0.10
    take_profit_pct: float = 0.10
    max_sessions: int = 10


def parse(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    timestamp(parsed)
    return parsed.astimezone(UTC)


def session_for(version: calendar.Version, signal: datetime) -> date:
    """Never move a late observation into a later, more convenient entry window."""
    timestamp(signal)
    day = signal.astimezone(calendar.NY).date()
    for offset in range(32):
        current = version.day(day + timedelta(days=offset))
        if current.state == "unknown":
            raise ValueError(f"unknown entry calendar: {current.day}")
        if current.state == "open":
            chosen = entry_session(FrozenE07(), signal, [current.day], now=signal)
            if chosen is not None:
                return chosen
    raise ValueError("no entry session in bounded calendar search")


def history_dates(version: calendar.Version, signal: datetime) -> tuple[date, ...]:
    """E07 treats same-day bars as complete at 16:00, including early-close days."""
    timestamp(signal)
    local = signal.astimezone(calendar.NY)
    day = local.date() - timedelta(days=local.time() < time(16))
    found: list[date] = []
    for offset in range(80):
        current = version.day(day - timedelta(days=offset))
        if current.state == "unknown":
            raise ValueError(f"unknown pre-signal calendar: {current.day}")
        if current.state == "open":
            found.append(current.day)
            if len(found) == 20:
                return tuple(reversed(found))
    raise ValueError("insufficient pre-signal sessions")


def manifest(
    version: calendar.Version,
    *,
    boundary: datetime,
    created_at: datetime,
    revision: str,
    review_reference: str,
    implementation_sha256: str,
    inventory: dict[str, Any],
) -> dict[str, Any]:
    """Deterministic contract; activation must separately enforce current wall time."""
    timestamp(created_at)
    timestamp(boundary)
    if (
        boundary < created_at + timedelta(hours=2)
        or version.observed_at > created_at
        or parse(HOURS_OBSERVED) > created_at
        or re.fullmatch(r"[0-9a-f]{40}", revision) is None
        or not review_reference.strip()
        or re.fullmatch(r"[0-9a-f]{64}", implementation_sha256) is None
    ):
        raise ValueError("invalid prospective commissioning provenance")
    try:
        if (
            set(inventory)
            != {
                "source_path",
                "source_device",
                "source_inode",
                "read_started_at",
                "observed_at",
                "packet_id_hashes",
            }
            or not isinstance(inventory["source_path"], str)
            or not inventory["source_path"]
            or any(
                re.fullmatch(r"[0-9]+", inventory[key]) is None
                for key in ("source_device", "source_inode")
            )
            or not parse(inventory["read_started_at"])
            <= parse(inventory["observed_at"])
            <= created_at
            or not isinstance(inventory["packet_id_hashes"], list)
            or inventory["packet_id_hashes"] != sorted(set(inventory["packet_id_hashes"]))
            or any(
                re.fullmatch(r"[0-9a-f]{64}", key) is None for key in inventory["packet_id_hashes"]
            )
        ):
            raise ValueError("invalid pre-boundary source inventory")
    except (KeyError, TypeError) as exc:
        raise ValueError("malformed pre-boundary source inventory") from exc
    anchor = session_for(version, boundary)
    binding = calendar.bind(version, anchor, 20)
    prehistory = history_dates(version, boundary)
    sessions = binding.sessions
    deadline_day = max(sessions[18].day + timedelta(days=5), sessions[19].day + timedelta(days=3))
    return {
        "format": FORMAT,
        "purpose": "OUTCOME_BLIND_ACQUISITION_ONLY",
        "created_at": timestamp(created_at),
        "boundary": timestamp(boundary),
        "reviewed_revision": revision,
        "review_reference": review_reference,
        "implementation_sha256": implementation_sha256,
        "preexisting_inventory": inventory,
        "calendar_version": version.digest,
        "calendar_observed_at": timestamp(version.observed_at),
        "calendar_binding": binding.digest,
        "annual_source_sha256": calendar.PDF_SHA256,
        "hours_source_sha256": HOURS_SHA256,
        "hours_source_observed_at": HOURS_OBSERVED,
        "sessions": [item.record() for item in sessions],
        "entry_sessions": [str(item.day) for item in sessions[:10]],
        "earliest_history": str(prehistory[0]),
        "enrollment_end": timestamp(datetime.combine(sessions[9].day, time(9, 20), calendar.NY)),
        "recovery_deadline": timestamp(datetime.combine(deadline_day, time(8), calendar.NY)),
        "network_minimum_seconds": 300,
        "maximum_attempts_per_job": None,
        "retry_rule": "until_fixed_deadline_content_backoff_300_times_2_to_n_minus_1_cap_21600",
        "infrastructure_retry_rule": "global_300_times_2_to_n_minus_1_cap_1800_no_job_exhaustion",
        "request_priority": "benchmark_reserved_one_in_five_then_fresh_jobs_then_earliest_deadline",
        "stage_rule": (
            "R1_history_R2_full_horizon_R3_after_later_exchange_close_first_complete_receipts"
        ),
        "benchmark_rule": "SPY_R1_checkpoint_W10_R2_W19_R3_later_close_schedule_W1_through_W20",
        "source_client_id": 179,
        "commissioning_excluded_from_confirmatory_samples": True,
        "strategy_trial_ready": False,
    }


def validate_manifest(value: dict[str, Any], version: calendar.Version) -> None:
    try:
        expected = manifest(
            version,
            boundary=parse(value["boundary"]),
            created_at=parse(value["created_at"]),
            revision=value["reviewed_revision"],
            review_reference=value["review_reference"],
            implementation_sha256=value["implementation_sha256"],
            inventory=value["preexisting_inventory"],
        )
        # Python considers True == 1; canonical JSON does not.
        import rfc8785

        if rfc8785.dumps(expected) != rfc8785.dumps(value):
            raise ValueError("commissioning manifest differs from its frozen derivation")
    except (KeyError, TypeError, AttributeError) as exc:
        raise ValueError("malformed commissioning manifest") from exc


def admit(
    row: dict[str, Any],
    frozen: dict[str, Any],
    version: calendar.Version,
    *,
    first_observed_at: datetime,
) -> dict[str, Any]:
    """Every assignable in-window row counts, including unsupported symbols."""
    timestamp(first_observed_at)
    result: dict[str, Any] = {
        "membership": "unassignable",
        "reason": None,
        "symbol_valid": valid_symbol(row.get("symbol")),
        "first_observed_at": timestamp(first_observed_at),
        "observed_before_decision": False,
        "historical_issuer_identity_verified": False,
    }
    try:
        signal = parse(row["signal_at"])
        if signal > first_observed_at:
            result["reason"] = "signal_clock_in_future"
            return result
        if signal < parse(frozen["boundary"]):
            result.update(membership="outside", reason="before_boundary")
            return result
        if signal >= parse(frozen["enrollment_end"]):
            result.update(membership="outside", reason="after_enrollment_cutoff")
            return result
        entry = session_for(version, signal)
        if str(entry) not in frozen["entry_sessions"]:
            result.update(membership="outside", reason="entry_outside_window")
            return result
        # Membership is fixed before history validation; missing inputs do not
        # remove an in-window candidate from the denominator.
        cutoff = datetime.combine(entry, time(9, 20), calendar.NY)
        result.update(
            membership="admitted",
            reason="included" if result["symbol_valid"] else "invalid_symbol",
            entry_session=str(entry),
            recorded_entry_agrees=row.get("entry_session") == str(entry),
            decision_cutoff=timestamp(cutoff),
            observed_before_decision=first_observed_at < cutoff,
        )
        result.update(
            history_dates=[str(day) for day in history_dates(version, signal)],
            horizon_dates=[str(item.day) for item in calendar.bind(version, entry, 10).sessions],
            stability_session=str(calendar.bind(version, entry, 11).sessions[-1].day),
        )
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        result["reason"] = f"invalid_source_or_calendar:{type(exc).__name__}"
    return result
