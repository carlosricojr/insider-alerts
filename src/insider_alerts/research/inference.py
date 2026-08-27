"""Frozen, order-incapable inference executable for the OPP-E07-V1 trial."""

from __future__ import annotations

import argparse
import calendar
import contextlib
import hashlib
import json
import math
import sqlite3
import subprocess
import sys
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Literal
from zoneinfo import ZoneInfo

import rfc8785
from jsonschema import Draft202012Validator, FormatChecker, ValidationError

from insider_alerts.research.sec_history import CLASSIFIER_VERSION

HYPOTHESIS_ID = "OPP-E07-V1"
REPORT_SCHEMA_VERSION = 1
TRIAL_INPUT_SCHEMA_VERSION = 1
TERMINAL_DATASET_SCHEMA_VERSION = 2
BOOTSTRAP_RESAMPLES = 10_000
BOOTSTRAP_SEED = 260_826
BLOCK_LENGTH = 10
ALPHA_THRESHOLD = 0.025
TARGET_ENROLLED_TRADES = 100
MINIMUM_DISTINCT_ENTRY_DATES = 60
PRIMARY_COST = 0.002
STRESS_COST = 0.005
PRNG_NAME = "sha256_counter_u64_rejection_v1"
PRNG_DOMAIN = b"OPP-E07-V1|circular-moving-block-bootstrap|v1"
CAPACITY_RANK_SALT = "E07-F00-live-canary-v1"
ENTRY_STATES = {
    "pending_entry_selection",
    "enrolled",
    "ineligible",
    "overlap_suppressed",
    "capacity_suppressed",
    "missed",
}
INTEGRITY_CHECKS = (
    "timestamp_ordering",
    "snapshot_hashes",
    "sec_archive_coverage",
    "classification_provenance",
    "enrollment_reconciliation",
    "outcome_blinding",
    "outcome_completeness",
    "shadow_book_reconciliation",
)
TERMINAL_ONLY_CHECKS = {"outcome_completeness", "shadow_book_reconciliation"}
SHA256_CHARS = frozenset("0123456789abcdef")
NEW_YORK = ZoneInfo("America/New_York")


class TrialInvalid(ValueError):
    """A preregistration, integrity, or input-contract violation."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True, slots=True)
class Candidate:
    candidate_id: str
    packet_id: str
    accession_number: str
    symbol: str
    evidence_record_sha256: str
    source_first_observed_at: datetime
    entry_date: date
    entry_rank_sha256: str
    enrollment_state: str
    sequence: int | None


@dataclass(frozen=True, slots=True)
class Trade:
    trade_id: str
    sequence: int | None
    evidence_record_sha256: str | None
    entry_rank_sha256: str | None
    symbol: str
    entry_date: date
    entry_at: datetime
    exit_at: datetime
    gross_return: float
    spy_return: float

    @property
    def absolute_20(self) -> float:
        return self.gross_return - PRIMARY_COST

    @property
    def absolute_50(self) -> float:
        return self.gross_return - STRESS_COST

    @property
    def alpha_20(self) -> float:
        return self.gross_return - self.spy_return - PRIMARY_COST

    @property
    def alpha_50(self) -> float:
        return self.gross_return - self.spy_return - STRESS_COST


class Sha256CounterRng:
    """Exact counter-mode SHA-256 stream with unbiased bounded integer draws."""

    def __init__(self, seed: int, domain: bytes) -> None:
        if seed < 0 or seed >= 2**64:
            raise ValueError("seed must fit an unsigned 64-bit integer")
        self._seed = seed.to_bytes(8, "big")
        self._domain = domain
        self._counter = 0

    def bounded(self, upper: int) -> int:
        if upper < 1 or upper > 2**64:
            raise ValueError("upper must be in [1, 2**64]")
        limit = 2**64 - ((2**64) % upper)
        while True:
            payload = self._domain + b"\0" + self._seed + self._counter.to_bytes(16, "big")
            self._counter += 1
            value = int.from_bytes(hashlib.sha256(payload).digest()[:8], "big")
            if value < limit:
                return value % upper


def _canonical_sha256(value: Mapping[str, Any]) -> str:
    try:
        encoded = rfc8785.dumps(dict(value))
    except (ValueError, OverflowError) as exc:
        raise TrialInvalid("canonical_json_numeric_invalid") from exc
    return hashlib.sha256(encoded).hexdigest()


def registry_definition_sha256(registry: Mapping[str, Any]) -> str:
    definition = dict(registry)
    definition.pop("status", None)
    definition.pop("activation", None)
    return _canonical_sha256(definition)


def inference_artifact_sha256() -> str:
    return _artifact_sha256(Path(__file__).read_bytes())


def _artifact_sha256(content: bytes) -> str:
    """Hash text artifacts after platform-stable CRLF-to-LF canonicalization."""

    return hashlib.sha256(content.replace(b"\r\n", b"\n")).hexdigest()


def _file_sha256(path: Path) -> str:
    if not path.is_file():
        raise TrialInvalid("activation_artifact_missing")
    return _artifact_sha256(path.read_bytes())


def _git_blob(repo_root: Path, commit: str, relative_path: Path) -> bytes:
    commit = _git_commit_sha(commit)
    try:
        completed = subprocess.run(
            ["git", "show", f"{commit}:{relative_path.as_posix()}"],
            cwd=repo_root,
            check=False,
            capture_output=True,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise TrialInvalid("activation_git_artifact_unverifiable") from exc
    if completed.returncode != 0:
        raise TrialInvalid("activation_git_artifact_missing")
    return completed.stdout


def _exact_keys(value: Mapping[str, Any], expected: set[str], context: str) -> None:
    if set(value) != expected:
        raise TrialInvalid(f"{context}_keys_invalid")


def _mapping(value: Any, context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TrialInvalid(f"{context}_not_object")
    return value


def _list(value: Any, context: str) -> list[Any]:
    if not isinstance(value, list):
        raise TrialInvalid(f"{context}_not_array")
    return value


def _text(value: Any, context: str) -> str:
    if not isinstance(value, str) or not value:
        raise TrialInvalid(f"{context}_not_string")
    return value


def _sha256(value: Any, context: str) -> str:
    text = _text(value, context)
    if len(text) != 64 or any(char not in SHA256_CHARS for char in text):
        raise TrialInvalid(f"{context}_not_sha256")
    return text


def _git_commit_sha(value: Any) -> str:
    text = _text(value, "activation_git_commit")
    if len(text) != 40 or any(char not in SHA256_CHARS for char in text):
        raise TrialInvalid("activation_git_commit_invalid")
    return text


def _utc(value: Any, context: str) -> datetime:
    text = _text(value, context)
    if not text.endswith("Z"):
        raise TrialInvalid(f"{context}_not_canonical_utc")
    try:
        parsed = datetime.fromisoformat(text[:-1] + "+00:00")
    except ValueError as exc:
        raise TrialInvalid(f"{context}_not_datetime") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise TrialInvalid(f"{context}_not_canonical_utc")
    return parsed.astimezone(UTC)


def _date(value: Any, context: str) -> date:
    text = _text(value, context)
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise TrialInvalid(f"{context}_not_date") from exc
    if parsed.isoformat() != text:
        raise TrialInvalid(f"{context}_not_canonical_date")
    return parsed


def _finite(value: Any, context: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TrialInvalid(f"{context}_not_number")
    result = float(value)
    if not math.isfinite(result):
        raise TrialInvalid(f"{context}_not_finite")
    return result


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _valid_local(naive: datetime, fold: int) -> tuple[datetime, datetime] | None:
    aware = naive.replace(tzinfo=NEW_YORK, fold=fold)
    utc_value = aware.astimezone(UTC)
    round_trip = utc_value.astimezone(NEW_YORK)
    if round_trip.replace(tzinfo=None) != naive:
        return None
    return aware, utc_value


def enrollment_deadline(activated_at: datetime) -> datetime:
    """Add exactly 18 New York calendar months using the preregistered DST rules."""

    local = activated_at.astimezone(NEW_YORK)
    absolute_month = local.year * 12 + local.month - 1 + 18
    target_year, month_zero = divmod(absolute_month, 12)
    target_month = month_zero + 1
    target_day = min(local.day, calendar.monthrange(target_year, target_month)[1])
    naive = datetime(
        target_year,
        target_month,
        target_day,
        local.hour,
        local.minute,
        local.second,
        local.microsecond,
    )
    while True:
        candidates = [value for fold in (0, 1) if (value := _valid_local(naive, fold))]
        if candidates:
            distinct = {candidate[1] for candidate in candidates}
            return max(distinct).astimezone(UTC) if len(distinct) > 1 else next(iter(distinct))
        naive += timedelta(seconds=1)


def _parse_candidate(raw: Any, index: int) -> Candidate:
    value = _mapping(raw, f"candidate_{index}")
    _exact_keys(
        value,
        {
            "candidate_id",
            "packet_id",
            "accession_number",
            "symbol",
            "evidence_record_sha256",
            "source_first_observed_at_utc",
            "entry_date",
            "entry_rank_sha256",
            "enrollment_state",
            "confirmatory_enrollment_sequence",
        },
        f"candidate_{index}",
    )
    state = _text(value["enrollment_state"], f"candidate_{index}_state")
    if state not in ENTRY_STATES:
        raise TrialInvalid("candidate_state_invalid")
    entry_day = (
        None
        if value["entry_date"] is None
        else _date(value["entry_date"], f"candidate_{index}_entry_date")
    )
    rank = (
        None
        if value["entry_rank_sha256"] is None
        else _sha256(value["entry_rank_sha256"], f"candidate_{index}_rank")
    )
    sequence_raw = value["confirmatory_enrollment_sequence"]
    sequence = None
    if sequence_raw is not None:
        if isinstance(sequence_raw, bool) or not isinstance(sequence_raw, int) or sequence_raw < 1:
            raise TrialInvalid("candidate_sequence_invalid")
        sequence = sequence_raw
    if entry_day is None or rank is None:
        raise TrialInvalid("candidate_missing_planned_entry_provenance")
    if state == "pending_entry_selection":
        if sequence is not None:
            raise TrialInvalid("pending_candidate_has_enrollment_sequence")
    elif state in {"enrolled", "overlap_suppressed", "capacity_suppressed"}:
        if (state == "enrolled") != (sequence is not None):
            raise TrialInvalid("candidate_sequence_state_mismatch")
    elif sequence is not None:
        raise TrialInvalid("non_enrolled_candidate_has_sequence")
    packet_id = _text(value["packet_id"], f"candidate_{index}_packet_id")
    accession_number = _text(value["accession_number"], f"candidate_{index}_accession_number")
    symbol = _text(value["symbol"], f"candidate_{index}_symbol").upper()
    if symbol != value["symbol"] or not symbol.isascii():
        raise TrialInvalid("candidate_symbol_not_canonical")
    if entry_day is not None and rank is not None:
        material = (
            f"{CAPACITY_RANK_SALT}|{entry_day.isoformat()}|{packet_id}|{accession_number}|{symbol}"
        )
        if hashlib.sha256(material.encode()).hexdigest() != rank:
            raise TrialInvalid("candidate_capacity_rank_mismatch")
    return Candidate(
        candidate_id=_text(value["candidate_id"], f"candidate_{index}_id"),
        packet_id=packet_id,
        accession_number=accession_number,
        symbol=symbol,
        evidence_record_sha256=_sha256(
            value["evidence_record_sha256"], f"candidate_{index}_evidence_sha"
        ),
        source_first_observed_at=_utc(
            value["source_first_observed_at_utc"], f"candidate_{index}_observed_at"
        ),
        entry_date=entry_day,
        entry_rank_sha256=rank,
        enrollment_state=state,
        sequence=sequence,
    )


def _candidate_order(candidate: Candidate) -> tuple[datetime, str]:
    return candidate.source_first_observed_at, candidate.candidate_id


def _enrollment_order(candidate: Candidate) -> tuple[date, str, str]:
    return candidate.entry_date, candidate.entry_rank_sha256, candidate.candidate_id


def _entry_date_completions(raw: Any, evaluated_at: datetime) -> dict[date, datetime]:
    result: dict[date, datetime] = {}
    previous: date | None = None
    for index, item in enumerate(_list(raw, "entry_date_completions")):
        value = _mapping(item, f"entry_date_completion_{index}")
        _exact_keys(
            value,
            {"entry_date", "completed_at_utc"},
            f"entry_date_completion_{index}",
        )
        entry_day = _date(value["entry_date"], f"entry_date_completion_{index}_date")
        completed_at = _utc(value["completed_at_utc"], f"entry_date_completion_{index}_at")
        if previous is not None and entry_day <= previous:
            raise TrialInvalid("entry_date_completions_not_strictly_ordered")
        if completed_at > evaluated_at:
            raise TrialInvalid("entry_date_completion_in_future")
        if completed_at.astimezone(NEW_YORK).date() != entry_day:
            raise TrialInvalid("entry_date_completion_date_mismatch")
        result[entry_day] = completed_at
        previous = entry_day
    return result


def cohort_freeze_boundary(
    enrolled_entry_dates: Sequence[date],
    completions: Mapping[date, datetime],
) -> tuple[date, datetime] | None:
    """Return the first immutable complete-date boundary meeting the frozen sample floor."""

    counts: Counter[date] = Counter(
        entry_date for entry_date in enrolled_entry_dates if entry_date in completions
    )
    cumulative = 0
    for distinct_dates, entry_day in enumerate(sorted(counts), start=1):
        cumulative += counts[entry_day]
        if cumulative >= TARGET_ENROLLED_TRADES and distinct_dates >= MINIMUM_DISTINCT_ENTRY_DATES:
            return entry_day, completions[entry_day]
    return None


def _freeze_boundary(
    candidates: Sequence[Candidate], completions: Mapping[date, datetime]
) -> tuple[date, datetime] | None:
    return cohort_freeze_boundary(
        [
            candidate.entry_date
            for candidate in candidates
            if candidate.enrollment_state == "enrolled" and candidate.entry_date is not None
        ],
        completions,
    )


def _candidate_projection_sha256(candidates: Sequence[Candidate]) -> str:
    projection = {
        "hypothesis_id": HYPOTHESIS_ID,
        "candidates": [
            {
                "candidate_id": candidate.candidate_id,
                "packet_id": candidate.packet_id,
                "accession_number": candidate.accession_number,
                "symbol": candidate.symbol,
                "evidence_record_sha256": candidate.evidence_record_sha256,
                "source_first_observed_at_utc": _utc_text(candidate.source_first_observed_at),
                "entry_date": candidate.entry_date.isoformat(),
                "entry_rank_sha256": candidate.entry_rank_sha256,
                "enrollment_state": candidate.enrollment_state,
                "confirmatory_enrollment_sequence": candidate.sequence,
            }
            for candidate in sorted(candidates, key=_candidate_order)
        ],
    }
    return _canonical_sha256(projection)


def _candidate_universe_sha256(candidates: Sequence[Candidate]) -> str:
    """Digest immutable candidate identity/provenance across deadline resolution."""

    universe = {
        "hypothesis_id": HYPOTHESIS_ID,
        "candidates": [
            {
                "candidate_id": candidate.candidate_id,
                "packet_id": candidate.packet_id,
                "accession_number": candidate.accession_number,
                "symbol": candidate.symbol,
                "evidence_record_sha256": candidate.evidence_record_sha256,
                "source_first_observed_at_utc": _utc_text(candidate.source_first_observed_at),
                "planned_entry_date": candidate.entry_date.isoformat(),
                "entry_rank_sha256": candidate.entry_rank_sha256,
            }
            for candidate in sorted(candidates, key=_candidate_order)
        ],
    }
    return _canonical_sha256(universe)


def _parse_trade(raw: Any, index: int, group: str) -> Trade:
    value = _mapping(raw, f"{group}_trade_{index}")
    _exact_keys(
        value,
        {
            "trade_id",
            "confirmatory_enrollment_sequence",
            "evidence_record_sha256",
            "entry_rank_sha256",
            "symbol",
            "entry_date",
            "entry_at_utc",
            "exit_at_utc",
            "gross_return",
            "spy_return",
        },
        f"{group}_trade_{index}",
    )
    sequence_raw = value["confirmatory_enrollment_sequence"]
    sequence = None
    if sequence_raw is not None:
        if isinstance(sequence_raw, bool) or not isinstance(sequence_raw, int) or sequence_raw < 1:
            raise TrialInvalid(f"{group}_trade_sequence_invalid")
        sequence = sequence_raw
    if group == "challenger" and sequence is None:
        raise TrialInvalid("challenger_trade_sequence_missing")
    if group != "challenger" and sequence is not None:
        raise TrialInvalid("diagnostic_trade_sequence_present")
    evidence_sha = (
        None
        if value["evidence_record_sha256"] is None
        else _sha256(value["evidence_record_sha256"], f"{group}_trade_evidence_sha")
    )
    rank_sha = (
        None
        if value["entry_rank_sha256"] is None
        else _sha256(value["entry_rank_sha256"], f"{group}_trade_rank_sha")
    )
    if group == "challenger" and (evidence_sha is None or rank_sha is None):
        raise TrialInvalid("challenger_trade_provenance_missing")
    if group != "challenger" and (evidence_sha is not None or rank_sha is not None):
        raise TrialInvalid("diagnostic_trade_confirmatory_provenance_present")
    entry_day = _date(value["entry_date"], f"{group}_trade_{index}_entry_date")
    entry_at = _utc(value["entry_at_utc"], f"{group}_trade_{index}_entry_at")
    exit_at = _utc(value["exit_at_utc"], f"{group}_trade_{index}_exit_at")
    if exit_at <= entry_at:
        raise TrialInvalid(f"{group}_trade_timestamp_order_invalid")
    if entry_at.astimezone(NEW_YORK).date() != entry_day:
        raise TrialInvalid(f"{group}_trade_entry_date_mismatch")
    symbol = _text(value["symbol"], f"{group}_trade_{index}_symbol").upper()
    if symbol != value["symbol"] or not symbol.isascii():
        raise TrialInvalid(f"{group}_trade_symbol_not_canonical")
    gross = _finite(value["gross_return"], f"{group}_trade_{index}_gross_return")
    spy = _finite(value["spy_return"], f"{group}_trade_{index}_spy_return")
    if gross < -1 or spy < -1:
        raise TrialInvalid(f"{group}_trade_return_below_minus_one")
    if not all(
        math.isfinite(value)
        for value in (
            gross - PRIMARY_COST,
            gross - STRESS_COST,
            gross - spy - PRIMARY_COST,
            gross - spy - STRESS_COST,
        )
    ):
        raise TrialInvalid(f"{group}_trade_derived_return_not_finite")
    return Trade(
        trade_id=_text(value["trade_id"], f"{group}_trade_{index}_id"),
        sequence=sequence,
        evidence_record_sha256=evidence_sha,
        entry_rank_sha256=rank_sha,
        symbol=symbol,
        entry_date=entry_day,
        entry_at=entry_at,
        exit_at=exit_at,
        gross_return=gross,
        spy_return=spy,
    )


def _trade_order(trade: Trade) -> tuple[date, datetime, int, str]:
    return trade.entry_date, trade.entry_at, trade.sequence or 2**63, trade.trade_id


def _mean(values: Sequence[float]) -> float:
    if not values:
        raise TrialInvalid("mean_of_empty_values")
    return math.fsum(values) / len(values)


def _percentile_type7(sorted_values: Sequence[float], probability: float) -> float:
    if not sorted_values:
        raise TrialInvalid("percentile_of_empty_values")
    position = (len(sorted_values) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return sorted_values[lower]
    weight = position - lower
    return math.fsum((sorted_values[lower] * (1.0 - weight), sorted_values[upper] * weight))


def _sampled_cluster_indices(rng: Sha256CounterRng, cluster_count: int) -> list[int]:
    indices: list[int] = []
    while len(indices) < cluster_count:
        start = rng.bounded(cluster_count)
        for offset in range(min(BLOCK_LENGTH, cluster_count)):
            if len(indices) == cluster_count:
                break
            indices.append((start + offset) % cluster_count)
    return indices


def _bootstrap(
    trades: Sequence[Trade],
    *,
    value_name: Literal["alpha_20"],
    domain: bytes,
    include_test: bool,
) -> dict[str, Any]:
    ordered = sorted(trades, key=_trade_order)
    by_date: dict[date, list[float]] = defaultdict(list)
    for trade in ordered:
        by_date[trade.entry_date].append(float(getattr(trade, value_name)))
    dates = sorted(by_date)
    if not dates:
        return {
            "trade_count": 0,
            "distinct_entry_dates": 0,
            "mean": None,
            "confidence_interval_95": None,
            "p_value": None,
            "exceedances": None,
        }
    observed = _mean([value for entry_day in dates for value in by_date[entry_day]])
    centered = {
        entry_day: [value - observed for value in by_date[entry_day]] for entry_day in dates
    }
    rng = Sha256CounterRng(BOOTSTRAP_SEED, domain)
    uncentered_means: list[float] = []
    exceedances = 0
    for _ in range(BOOTSTRAP_RESAMPLES):
        indices = _sampled_cluster_indices(rng, len(dates))
        uncentered_values = [value for index in indices for value in by_date[dates[index]]]
        uncentered_means.append(_mean(uncentered_values))
        if include_test:
            null_values = [value for index in indices for value in centered[dates[index]]]
            if _mean(null_values) >= observed:
                exceedances += 1
    uncentered_means.sort()
    return {
        "trade_count": len(ordered),
        "distinct_entry_dates": len(dates),
        "mean": observed,
        "confidence_interval_95": [
            _percentile_type7(uncentered_means, 0.025),
            _percentile_type7(uncentered_means, 0.975),
        ],
        "p_value": ((1 + exceedances) / (1 + BOOTSTRAP_RESAMPLES) if include_test else None),
        "exceedances": exceedances if include_test else None,
    }


def _profit_factor(values: Sequence[float]) -> dict[str, Any]:
    gains = math.fsum(value for value in values if value > 0)
    losses = abs(math.fsum(value for value in values if value < 0))
    if losses > 0:
        return {"value": gains / losses, "is_positive_infinity": False}
    if gains > 0:
        return {"value": None, "is_positive_infinity": True}
    return {"value": 0.0, "is_positive_infinity": False}


def _economic_metrics(trades: Sequence[Trade]) -> tuple[dict[str, Any], dict[str, bool]]:
    ordered = sorted(trades, key=_trade_order)
    absolute_20 = [trade.absolute_20 for trade in ordered]
    absolute_50 = [trade.absolute_50 for trade in ordered]
    alpha_20 = [trade.alpha_20 for trade in ordered]
    alpha_50 = [trade.alpha_50 for trade in ordered]
    dates = sorted({trade.entry_date for trade in ordered})
    split_index = len(dates) // 2
    first_dates = set(dates[:split_index])
    second_dates = set(dates[split_index:])
    first_half = [trade.alpha_50 for trade in ordered if trade.entry_date in first_dates]
    second_half = [trade.alpha_50 for trade in ordered if trade.entry_date in second_dates]
    best_index = max(range(len(ordered)), key=lambda index: ordered[index].alpha_50)
    without_best_trade = [
        trade.alpha_50 for index, trade in enumerate(ordered) if index != best_index
    ]
    by_month_50: dict[str, list[float]] = defaultdict(list)
    by_month_20: dict[str, list[float]] = defaultdict(list)
    by_symbol_20: dict[str, list[float]] = defaultdict(list)
    for trade in ordered:
        month = trade.entry_date.strftime("%Y-%m")
        by_month_50[month].append(trade.alpha_50)
        by_month_20[month].append(trade.alpha_20)
        by_symbol_20[trade.symbol].append(trade.alpha_20)
    month_totals_50 = {month: math.fsum(values) for month, values in by_month_50.items()}
    best_month = max(sorted(month_totals_50), key=month_totals_50.__getitem__)
    without_best_month = [
        trade.alpha_50 for trade in ordered if trade.entry_date.strftime("%Y-%m") != best_month
    ]
    symbol_positive = {
        symbol: max(0.0, math.fsum(values)) for symbol, values in by_symbol_20.items()
    }
    total_symbol_positive = math.fsum(symbol_positive.values())
    top_symbol_share = (
        max(symbol_positive.values()) / total_symbol_positive if total_symbol_positive > 0 else None
    )
    month_net = {month: math.fsum(values) for month, values in by_month_20.items()}
    total_net_alpha = math.fsum(alpha_20)
    top_month_share = max(month_net.values()) / total_net_alpha if total_net_alpha > 0 else None
    profit_factor = _profit_factor(absolute_20)
    metrics: dict[str, Any] = {
        "mean_absolute_return_20bps": _mean(absolute_20),
        "mean_alpha_20bps": _mean(alpha_20),
        "mean_alpha_50bps": _mean(alpha_50),
        "profit_factor_absolute_return_20bps": profit_factor,
        "chronological_half_split_second_half_start": dates[split_index].isoformat(),
        "first_half_mean_alpha_50bps": _mean(first_half),
        "second_half_mean_alpha_50bps": _mean(second_half),
        "removed_best_trade_id": ordered[best_index].trade_id,
        "mean_alpha_50bps_without_best_trade": _mean(without_best_trade),
        "removed_best_entry_month": best_month,
        "mean_alpha_50bps_without_best_entry_month": (
            _mean(without_best_month) if without_best_month else None
        ),
        "top_symbol_positive_alpha_20bps_share": top_symbol_share,
        "top_entry_month_net_alpha_20bps_share": top_month_share,
        "twenty_slot_replay_total_absolute_return_50bps": math.fsum(absolute_50),
    }
    profit_pass = bool(
        profit_factor["is_positive_infinity"]
        or (profit_factor["value"] is not None and profit_factor["value"] > 1.0)
    )
    gates = {
        "positive_mean_absolute_return_at_20bps": metrics["mean_absolute_return_20bps"] > 0,
        "positive_mean_alpha_at_50bps": metrics["mean_alpha_50bps"] > 0,
        "profit_factor_above_one_at_20bps": profit_pass,
        "positive_mean_50bps_alpha_in_both_chronological_halves": (
            metrics["first_half_mean_alpha_50bps"] > 0
            and metrics["second_half_mean_alpha_50bps"] > 0
        ),
        "positive_mean_50bps_alpha_without_best_trade": (
            metrics["mean_alpha_50bps_without_best_trade"] > 0
        ),
        "positive_mean_50bps_alpha_without_best_calendar_month": (
            metrics["mean_alpha_50bps_without_best_entry_month"] is not None
            and metrics["mean_alpha_50bps_without_best_entry_month"] > 0
        ),
        "symbol_positive_pnl_contribution_at_most_25pct": (
            top_symbol_share is not None and top_symbol_share <= 0.25
        ),
        "calendar_month_net_pnl_contribution_at_most_50pct": (
            top_month_share is not None and top_month_share <= 0.50
        ),
        "positive_20_slot_replay_return_at_50bps": (
            metrics["twenty_slot_replay_total_absolute_return_50bps"] > 0
        ),
        "all_integrity_and_reconciliation_checks_pass": True,
    }
    return metrics, gates


def _sealed_report(report: dict[str, Any]) -> dict[str, Any]:
    report["report_sha256"] = ""
    unsigned = dict(report)
    unsigned.pop("report_sha256")
    report["report_sha256"] = _canonical_sha256(unsigned)
    return report


def _base_report(
    *,
    evaluated_at: datetime | None,
    activated_at: datetime | None,
    deadline: datetime | None,
    state: str,
    reasons: Sequence[str],
    counts: Mapping[str, int] | None = None,
    freeze_boundary: date | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "hypothesis_id": HYPOTHESIS_ID,
        "state": state,
        "reason_codes": list(reasons),
        "evaluated_at_utc": _utc_text(evaluated_at) if evaluated_at else None,
        "activated_at_utc": _utc_text(activated_at) if activated_at else None,
        "enrollment_deadline_utc": _utc_text(deadline) if deadline else None,
        "candidate_state_counts": dict(sorted((counts or {}).items())),
        "freeze_boundary_entry_date": freeze_boundary.isoformat() if freeze_boundary else None,
        "terminal_dataset_sha256": None,
        "candidate_projection_sha256": None,
        "terminal_seal_receipt_sha256": None,
        "deadline_miss_receipt_sha256": None,
        "inference": None,
        "economic_metrics": None,
        "economic_gates": None,
        "falsification_context": None,
        "report_sha256": "",
    }


def _invalid_report(payload: Mapping[str, Any], code: str) -> dict[str, Any]:
    evaluated = None
    activated = None
    with contextlib.suppress(TrialInvalid):
        evaluated = _utc(payload.get("evaluated_at_utc"), "evaluated_at")
    with contextlib.suppress(TrialInvalid):
        activated = _utc(payload.get("activated_at_utc"), "activated_at")
    deadline = enrollment_deadline(activated) if activated else None
    return _sealed_report(
        _base_report(
            evaluated_at=evaluated,
            activated_at=activated,
            deadline=deadline,
            state="INVALID",
            reasons=[code],
        )
    )


def _reviewed_repo_root() -> Path:
    required = (
        Path(".git"),
        Path("uv.lock"),
        Path("docs/research/contracts/hypothesis-registry.schema.json"),
    )
    seen: set[Path] = set()
    starts = (Path.cwd().resolve(), Path(__file__).resolve().parent)
    for start in starts:
        for candidate in (start, *start.parents):
            if candidate in seen:
                continue
            seen.add(candidate)
            if all((candidate / relative).exists() for relative in required):
                return candidate
    raise TrialInvalid("reviewed_repository_checkout_required")


def _validate_registry(registry: Mapping[str, Any], *, allow_draft: bool) -> None:
    repo_root = _reviewed_repo_root()
    schema_path = repo_root / "docs/research/contracts/hypothesis-registry.schema.json"
    try:
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        Draft202012Validator(schema, format_checker=FormatChecker()).validate(dict(registry))
    except (OSError, ValueError, ValidationError) as exc:
        raise TrialInvalid("registry_schema_validation_failed") from exc
    if registry.get("hypothesis_id") != HYPOTHESIS_ID:
        raise TrialInvalid("registry_hypothesis_mismatch")
    inference = _mapping(registry.get("inference"), "registry_inference")
    expected = {
        "test": "null_centered_circular_moving_block_bootstrap_trade_weighted_mean",
        "side": "one_sided_greater",
        "cluster": "entry_date",
        "block_length_clusters": BLOCK_LENGTH,
        "resamples": BOOTSTRAP_RESAMPLES,
        "seed": BOOTSTRAP_SEED,
        "p_value_formula": "(1 + exceedances) / (1 + resamples)",
        "interim_looks": 0,
        "prng": PRNG_NAME,
        "prng_domain": PRNG_DOMAIN.decode(),
        "bounded_integer_sampling": "u64_big_endian_rejection_before_modulo",
        "cluster_order": "entry_date_ascending",
        "trade_order": "entry_at_utc_sequence_trade_id_ascending",
        "confidence_interval": "two_sided_95pct_uncentered_same_resamples_type7",
        "exceedance_tie_rule": "null_mean_greater_than_or_equal_observed_mean",
    }
    if dict(inference) != expected:
        raise TrialInvalid("registry_inference_not_frozen")
    status = registry.get("status")
    if status == "draft":
        if not allow_draft or registry.get("activation") is not None:
            raise TrialInvalid("registry_not_active")
        return
    if status != "active":
        raise TrialInvalid("registry_status_invalid")
    activation = _mapping(registry.get("activation"), "registry_activation")
    receipt_material = {
        "schema_version": 1,
        "hypothesis_id": HYPOTHESIS_ID,
        "kind": "prospective_activation",
        **{
            str(key): value
            for key, value in activation.items()
            if key != "activation_receipt_sha256"
        },
    }
    if activation.get("activation_receipt_sha256") != _canonical_sha256(receipt_material):
        raise TrialInvalid("activation_receipt_digest_mismatch")
    prepared_at = _utc(activation.get("activation_prepared_at_utc"), "activation_prepared_at")
    activated_at = _utc(activation.get("activated_at_utc"), "registry_activated_at")
    if prepared_at >= activated_at:
        raise TrialInvalid("activation_not_prospective")
    if activation.get("registry_definition_sha256") != registry_definition_sha256(registry):
        raise TrialInvalid("registry_definition_digest_mismatch")
    if activation.get("inference_artifact_sha256") != inference_artifact_sha256():
        raise TrialInvalid("inference_artifact_digest_mismatch")
    artifact_expectations = {
        "preregistration_sha256": Path(str(registry["preregistration"])),
        "hypothesis_schema_sha256": Path("docs/research/contracts/hypothesis-registry.schema.json"),
        "evidence_schema_sha256": Path("docs/research/contracts/evidence-snapshot.schema.json"),
        "inference_artifact_sha256": Path("src/insider_alerts/research/inference.py"),
        "terminal_builder_artifact_sha256": Path("src/insider_alerts/research/terminal_builder.py"),
        "activation_artifact_sha256": Path("src/insider_alerts/research/activation.py"),
        "dependency_lock_sha256": Path("uv.lock"),
        "policy_sha256": Path(str(registry["strategy"]["policy_artifact"])),
    }
    activation_commit = _git_commit_sha(activation.get("activation_git_commit"))
    for field, relative_path in artifact_expectations.items():
        expected_digest = activation.get(field)
        if expected_digest != _file_sha256(repo_root / relative_path):
            raise TrialInvalid(f"activation_artifact_digest_mismatch:{field}")
        if (
            _artifact_sha256(_git_blob(repo_root, activation_commit, relative_path))
            != expected_digest
        ):
            raise TrialInvalid(f"activation_git_artifact_digest_mismatch:{field}")
    try:
        registry_at_activation = json.loads(
            _git_blob(
                repo_root,
                activation_commit,
                Path("docs/research/registry/OPP-E07-V1.json"),
            )
        )
    except (UnicodeDecodeError, ValueError) as exc:
        raise TrialInvalid("activation_git_registry_invalid") from exc
    if not isinstance(registry_at_activation, Mapping) or registry_definition_sha256(
        registry_at_activation
    ) != activation.get("registry_definition_sha256"):
        raise TrialInvalid("activation_git_registry_definition_mismatch")
    if activation.get("classifier_version") != CLASSIFIER_VERSION:
        raise TrialInvalid("activation_classifier_version_mismatch")
    if activation.get("enrollment_start_sequence") != 1:
        raise TrialInvalid("activation_enrollment_sequence_invalid")
    try:
        completed = subprocess.run(
            [
                "git",
                "merge-base",
                "--is-ancestor",
                activation_commit,
                "HEAD",
            ],
            cwd=repo_root,
            check=False,
            capture_output=True,
            text=True,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise TrialInvalid("activation_git_commit_unverifiable") from exc
    if completed.returncode != 0:
        raise TrialInvalid("activation_git_commit_mismatch")


def _integrity(raw: Any, *, terminal: bool) -> dict[str, bool | None]:
    value = _mapping(raw, "integrity_checks")
    _exact_keys(value, set(INTEGRITY_CHECKS), "integrity_checks")
    result: dict[str, bool | None] = {}
    for name in INTEGRITY_CHECKS:
        state = value[name]
        if state is not None and not isinstance(state, bool):
            raise TrialInvalid("integrity_check_not_boolean_or_null")
        if name not in TERMINAL_ONLY_CHECKS and state is None:
            raise TrialInvalid("preterminal_integrity_check_missing")
        if terminal and state is None:
            raise TrialInvalid("terminal_integrity_check_missing")
        result[name] = state
    return result


def _diagnostic_group_status(raw: Any) -> dict[str, dict[str, Any]]:
    value = _mapping(raw, "diagnostic_group_status")
    _exact_keys(value, {"control", "routine"}, "diagnostic_group_status")
    result: dict[str, dict[str, Any]] = {}
    for group in ("control", "routine"):
        record = _mapping(value[group], f"{group}_group_status")
        _exact_keys(
            record,
            {
                "status",
                "error_code",
                "membership_count",
                "available_trade_count",
                "not_traded_count",
                "unavailable_count",
            },
            f"{group}_group_status",
        )
        status = _text(record["status"], f"{group}_group_state")
        if status not in {"available", "unavailable"}:
            raise TrialInvalid(f"{group}_group_state_invalid")
        counts: dict[str, int] = {}
        for name in (
            "membership_count",
            "available_trade_count",
            "not_traded_count",
            "unavailable_count",
        ):
            count = record[name]
            if isinstance(count, bool) or not isinstance(count, int) or count < 0:
                raise TrialInvalid(f"{group}_group_{name}_invalid")
            counts[name] = count
        if counts["membership_count"] != sum(
            counts[name]
            for name in ("available_trade_count", "not_traded_count", "unavailable_count")
        ):
            raise TrialInvalid(f"{group}_group_accounting_mismatch")
        error_code = record["error_code"]
        if status == "available":
            if error_code is not None or counts["unavailable_count"] != 0:
                raise TrialInvalid(f"{group}_available_group_metadata_invalid")
        elif not isinstance(error_code, str) or not error_code:
            raise TrialInvalid(f"{group}_unavailable_group_error_missing")
        result[group] = {"status": status, "error_code": error_code, **counts}
    return result


def _parse_terminal(
    raw: Any,
    *,
    freeze_boundary: date,
    frozen_candidates: Sequence[Candidate],
    candidate_projection_sha256: str,
    evaluated_at: datetime,
) -> tuple[
    str,
    list[Trade],
    list[Trade],
    list[Trade],
    dict[str, dict[str, Any]],
    datetime,
]:
    value = _mapping(raw, "terminal_dataset")
    _exact_keys(
        value,
        {
            "schema_version",
            "hypothesis_id",
            "freeze_boundary_entry_date",
            "sealed_at_utc",
            "candidate_projection_sha256",
            "challenger_trades",
            "control_trades",
            "routine_trades",
            "diagnostic_group_status",
            "dataset_sha256",
        },
        "terminal_dataset",
    )
    if value["schema_version"] != TERMINAL_DATASET_SCHEMA_VERSION:
        raise TrialInvalid("terminal_dataset_schema_invalid")
    if value["hypothesis_id"] != HYPOTHESIS_ID:
        raise TrialInvalid("terminal_dataset_hypothesis_mismatch")
    digest = _sha256(value["dataset_sha256"], "terminal_dataset_sha")
    unsigned = dict(value)
    unsigned.pop("dataset_sha256")
    if _canonical_sha256(unsigned) != digest:
        raise TrialInvalid("terminal_dataset_digest_mismatch")
    if _date(value["freeze_boundary_entry_date"], "terminal_freeze_boundary") != freeze_boundary:
        raise TrialInvalid("terminal_freeze_boundary_mismatch")
    if (
        _sha256(value["candidate_projection_sha256"], "terminal_candidate_projection_sha")
        != candidate_projection_sha256
    ):
        raise TrialInvalid("terminal_candidate_projection_digest_mismatch")
    sealed_at = _utc(value["sealed_at_utc"], "terminal_sealed_at")
    if sealed_at > evaluated_at:
        raise TrialInvalid("terminal_dataset_sealed_in_future")
    groups: dict[str, list[Trade]] = {}
    for key, group in (("challenger_trades", "challenger"),):
        trades = [
            _parse_trade(item, index, group) for index, item in enumerate(_list(value[key], key))
        ]
        if trades != sorted(trades, key=_trade_order):
            raise TrialInvalid(f"{group}_trade_order_invalid")
        if len({trade.trade_id for trade in trades}) != len(trades):
            raise TrialInvalid(f"{group}_trade_id_duplicate")
        if any(trade.exit_at > sealed_at for trade in trades):
            raise TrialInvalid(f"{group}_trade_exit_after_seal")
        groups[group] = trades
    diagnostic_status = _diagnostic_group_status(value["diagnostic_group_status"])
    for key, group in (("control_trades", "control"), ("routine_trades", "routine")):
        raw_trades = _list(value[key], key)
        metadata = diagnostic_status[group]
        if metadata["status"] == "unavailable":
            if raw_trades:
                raise TrialInvalid(f"{group}_unavailable_group_has_trades")
            groups[group] = []
            continue
        trades = [_parse_trade(item, index, group) for index, item in enumerate(raw_trades)]
        if trades != sorted(trades, key=_trade_order):
            raise TrialInvalid(f"{group}_trade_order_invalid")
        if len({trade.trade_id for trade in trades}) != len(trades):
            raise TrialInvalid(f"{group}_trade_id_duplicate")
        if any(trade.exit_at > sealed_at for trade in trades):
            raise TrialInvalid(f"{group}_trade_exit_after_seal")
        if any(trade.entry_date > freeze_boundary for trade in trades):
            raise TrialInvalid(f"{group}_trade_after_freeze")
        if len(trades) != metadata["available_trade_count"]:
            raise TrialInvalid(f"{group}_available_trade_count_mismatch")
        groups[group] = trades
    expected = {candidate.candidate_id: candidate for candidate in frozen_candidates}
    challenger = groups["challenger"]
    if {trade.trade_id for trade in challenger} != set(expected):
        raise TrialInvalid("challenger_outcomes_do_not_match_frozen_cohort")
    for trade in challenger:
        candidate = expected[trade.trade_id]
        if trade.sequence != candidate.sequence or trade.entry_date != candidate.entry_date:
            raise TrialInvalid("challenger_outcome_enrollment_mismatch")
        if trade.entry_at <= candidate.source_first_observed_at:
            raise TrialInvalid("challenger_entry_not_after_source_observation")
        if (
            trade.evidence_record_sha256 != candidate.evidence_record_sha256
            or trade.entry_rank_sha256 != candidate.entry_rank_sha256
        ):
            raise TrialInvalid("challenger_outcome_provenance_mismatch")
    return (
        digest,
        challenger,
        groups["control"],
        groups["routine"],
        diagnostic_status,
        sealed_at,
    )


def _receipt_sha256(receipt: Mapping[str, Any], kind: str) -> str:
    value = dict(receipt)
    _exact_keys(
        value,
        {
            "schema_version",
            "hypothesis_id",
            "kind",
            "recorded_at_utc",
            "enrollment_deadline_utc",
            "terminal_dataset_sha256",
            "candidate_projection_sha256",
            "candidate_universe_sha256",
            "receipt_sha256",
        },
        f"{kind}_receipt",
    )
    if value["schema_version"] != 1 or value["hypothesis_id"] != HYPOTHESIS_ID:
        raise TrialInvalid(f"{kind}_receipt_identity_invalid")
    if value["kind"] != kind:
        raise TrialInvalid(f"{kind}_receipt_kind_invalid")
    _utc(value["recorded_at_utc"], f"{kind}_receipt_recorded_at")
    _utc(value["enrollment_deadline_utc"], f"{kind}_receipt_deadline")
    _sha256(value["candidate_projection_sha256"], "receipt_candidate_projection_sha")
    if kind == "terminal_seal":
        _sha256(value["terminal_dataset_sha256"], "receipt_terminal_dataset_sha")
    elif value["terminal_dataset_sha256"] is not None:
        raise TrialInvalid("deadline_receipt_has_terminal_digest")
    _sha256(value["candidate_universe_sha256"], "receipt_candidate_universe_sha")
    expected = _sha256(value["receipt_sha256"], f"{kind}_receipt_sha")
    value.pop("receipt_sha256")
    if _canonical_sha256(value) != expected:
        raise TrialInvalid(f"{kind}_receipt_digest_mismatch")
    return expected


def _build_receipt(
    *,
    kind: Literal["deadline_miss", "terminal_seal"],
    recorded_at: datetime,
    deadline: datetime,
    terminal_dataset_sha256: str | None,
    candidate_projection_sha256: str,
    candidate_universe_sha256: str,
) -> dict[str, Any]:
    receipt: dict[str, Any] = {
        "schema_version": 1,
        "hypothesis_id": HYPOTHESIS_ID,
        "kind": kind,
        "recorded_at_utc": _utc_text(recorded_at),
        "enrollment_deadline_utc": _utc_text(deadline),
        "terminal_dataset_sha256": terminal_dataset_sha256,
        "candidate_projection_sha256": candidate_projection_sha256,
        "candidate_universe_sha256": candidate_universe_sha256,
        "receipt_sha256": "",
    }
    unsigned = dict(receipt)
    unsigned.pop("receipt_sha256")
    receipt["receipt_sha256"] = _canonical_sha256(unsigned)
    return receipt


class TrialSealStore:
    """Append-only deadline, terminal-seal, and single-look report receipts."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with contextlib.closing(self._connect()) as conn, conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS trial_receipts(
                  kind TEXT PRIMARY KEY CHECK(kind IN ('deadline_miss','terminal_seal')),
                  receipt_json BLOB NOT NULL,
                  receipt_sha256 TEXT NOT NULL UNIQUE
                );
                CREATE TABLE IF NOT EXISTS decision_report(
                  singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                  report_json BLOB NOT NULL,
                  report_sha256 TEXT NOT NULL UNIQUE
                );
                CREATE TABLE IF NOT EXISTS terminal_pending(
                  singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                  terminal_json BLOB NOT NULL,
                  terminal_dataset_sha256 TEXT NOT NULL UNIQUE,
                  candidate_projection_sha256 TEXT NOT NULL,
                  candidate_universe_sha256 TEXT NOT NULL,
                  enrollment_deadline_utc TEXT NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS trial_receipts_no_update
                BEFORE UPDATE ON trial_receipts
                BEGIN SELECT RAISE(ABORT,'trial receipts are append-only'); END;
                CREATE TRIGGER IF NOT EXISTS trial_receipts_no_delete
                BEFORE DELETE ON trial_receipts
                BEGIN SELECT RAISE(ABORT,'trial receipts are append-only'); END;
                CREATE TRIGGER IF NOT EXISTS trial_receipts_mutually_exclusive
                BEFORE INSERT ON trial_receipts
                WHEN EXISTS(SELECT 1 FROM trial_receipts WHERE kind<>NEW.kind)
                BEGIN SELECT RAISE(ABORT,'terminal receipt kinds are mutually exclusive'); END;
                CREATE TRIGGER IF NOT EXISTS decision_report_no_update
                BEFORE UPDATE ON decision_report
                BEGIN SELECT RAISE(ABORT,'decision report is append-only'); END;
                CREATE TRIGGER IF NOT EXISTS decision_report_no_delete
                BEFORE DELETE ON decision_report
                BEGIN SELECT RAISE(ABORT,'decision report is append-only'); END;
                CREATE TRIGGER IF NOT EXISTS terminal_pending_no_update
                BEFORE UPDATE ON terminal_pending
                BEGIN SELECT RAISE(ABORT,'pending terminal dataset is append-only'); END;
                CREATE TRIGGER IF NOT EXISTS terminal_pending_no_delete
                BEFORE DELETE ON terminal_pending
                BEGIN SELECT RAISE(ABORT,'pending terminal dataset is append-only'); END;
                """
            )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        return conn

    def _put_receipt(self, receipt: Mapping[str, Any]) -> dict[str, Any]:
        kind = _text(receipt.get("kind"), "receipt_kind")
        digest = _receipt_sha256(receipt, kind)
        encoded = rfc8785.dumps(dict(receipt))
        with contextlib.closing(self._connect()) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            if kind == "deadline_miss" and conn.execute(
                "SELECT 1 FROM terminal_pending WHERE singleton=1"
            ).fetchone() is not None:
                raise TrialInvalid("terminal_receipt_kind_conflict")
            row = conn.execute(
                "SELECT receipt_json FROM trial_receipts WHERE kind=?", (kind,)
            ).fetchone()
            if row is not None:
                existing = json.loads(bytes(row["receipt_json"]))
                if existing != dict(receipt):
                    if kind == "terminal_seal" and all(
                        existing.get(name) == receipt.get(name)
                        for name in (
                            "schema_version",
                            "hypothesis_id",
                            "kind",
                            "enrollment_deadline_utc",
                            "terminal_dataset_sha256",
                            "candidate_projection_sha256",
                            "candidate_universe_sha256",
                        )
                    ):
                        return dict(existing)
                    raise TrialInvalid(f"alternate_{kind}_receipt_prohibited")
                return dict(existing)
            if (
                conn.execute("SELECT 1 FROM trial_receipts WHERE kind<>?", (kind,)).fetchone()
                is not None
            ):
                raise TrialInvalid("terminal_receipt_kind_conflict")
            conn.execute(
                "INSERT INTO trial_receipts(kind,receipt_json,receipt_sha256) VALUES(?,?,?)",
                (kind, encoded, digest),
            )
        return dict(receipt)

    def receipt(self, kind: Literal["deadline_miss", "terminal_seal"]) -> dict[str, Any] | None:
        with contextlib.closing(self._connect()) as conn, conn:
            row = conn.execute(
                "SELECT receipt_json FROM trial_receipts WHERE kind=?", (kind,)
            ).fetchone()
        if row is None:
            return None
        parsed = json.loads(bytes(row["receipt_json"]))
        if not isinstance(parsed, dict):
            raise TrialInvalid("stored_receipt_not_object")
        _receipt_sha256(parsed, kind)
        return parsed

    def seal_deadline_miss(
        self, payload: Mapping[str, Any], *, recorded_at: datetime
    ) -> dict[str, Any]:
        if self.pending_terminal() is not None:
            raise TrialInvalid("terminal_receipt_kind_conflict")
        activated_at = _utc(payload.get("activated_at_utc"), "activated_at")
        candidates = [
            _parse_candidate(item, index)
            for index, item in enumerate(_list(payload.get("candidates"), "candidates"))
        ]
        receipt = _build_receipt(
            kind="deadline_miss",
            recorded_at=recorded_at,
            deadline=enrollment_deadline(activated_at),
            terminal_dataset_sha256=None,
            candidate_projection_sha256=_candidate_projection_sha256(candidates),
            candidate_universe_sha256=_candidate_universe_sha256(candidates),
        )
        return self._put_receipt(receipt)

    def pending_terminal(self) -> dict[str, Any] | None:
        """Return and verify the first durably staged terminal dataset, if any."""

        with contextlib.closing(self._connect()) as conn:
            row = conn.execute("SELECT * FROM terminal_pending WHERE singleton=1").fetchone()
        if row is None:
            return None
        raw = bytes(row["terminal_json"])
        parsed = json.loads(raw)
        if not isinstance(parsed, dict) or rfc8785.dumps(parsed) != raw:
            raise TrialInvalid("pending_terminal_dataset_not_canonical")
        dataset_digest = _sha256(parsed.get("dataset_sha256"), "terminal_dataset_sha")
        unsigned = dict(parsed)
        unsigned.pop("dataset_sha256", None)
        if (
            _canonical_sha256(unsigned) != dataset_digest
            or row["terminal_dataset_sha256"] != dataset_digest
            or row["candidate_projection_sha256"]
            != parsed.get("candidate_projection_sha256")
        ):
            raise TrialInvalid("pending_terminal_dataset_invalid")
        _sha256(row["candidate_universe_sha256"], "pending_candidate_universe_sha")
        _utc(row["enrollment_deadline_utc"], "pending_enrollment_deadline")
        return parsed

    def stage_terminal(
        self,
        registry: Mapping[str, Any],
        payload: Mapping[str, Any],
        *,
        allow_draft: bool = False,
    ) -> dict[str, Any]:
        """Durably bind the first valid terminal bytes before artifact publication."""

        validation = evaluate_trial(
            registry,
            payload,
            allow_draft=allow_draft,
            terminal_validation_only=True,
        )
        if validation.get("reason_codes") != ["terminal_payload_validated_not_evaluated"]:
            reasons = validation.get("reason_codes")
            reason = reasons[0] if isinstance(reasons, list) and reasons else "terminal_invalid"
            raise TrialInvalid(f"terminal_preseal_validation_failed:{reason}")
        terminal = dict(_mapping(payload.get("terminal_dataset"), "terminal_dataset"))
        dataset_digest = _sha256(terminal.get("dataset_sha256"), "terminal_dataset_sha")
        unsigned = dict(terminal)
        unsigned.pop("dataset_sha256", None)
        if _canonical_sha256(unsigned) != dataset_digest:
            raise TrialInvalid("terminal_dataset_digest_mismatch")
        candidates = [
            _parse_candidate(item, index)
            for index, item in enumerate(_list(payload.get("candidates"), "candidates"))
        ]
        projection_digest = _candidate_projection_sha256(candidates)
        if terminal.get("candidate_projection_sha256") != projection_digest:
            raise TrialInvalid("terminal_candidate_projection_digest_mismatch")
        universe_digest = _candidate_universe_sha256(candidates)
        deadline = enrollment_deadline(_utc(payload.get("activated_at_utc"), "activated_at"))
        encoded = rfc8785.dumps(terminal)
        with contextlib.closing(self._connect()) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            conflicting = conn.execute(
                "SELECT kind FROM trial_receipts WHERE kind='deadline_miss'"
            ).fetchone()
            if conflicting is not None:
                raise TrialInvalid("terminal_receipt_kind_conflict")
            row = conn.execute("SELECT * FROM terminal_pending WHERE singleton=1").fetchone()
            if row is not None:
                if (
                    bytes(row["terminal_json"]) != encoded
                    or row["terminal_dataset_sha256"] != dataset_digest
                    or row["candidate_projection_sha256"] != projection_digest
                    or row["candidate_universe_sha256"] != universe_digest
                    or _utc(row["enrollment_deadline_utc"], "pending_enrollment_deadline")
                    != deadline
                ):
                    raise TrialInvalid("alternate_terminal_dataset_prohibited")
                return terminal
            existing_receipt = conn.execute(
                "SELECT receipt_json FROM trial_receipts WHERE kind='terminal_seal'"
            ).fetchone()
            if existing_receipt is not None:
                existing = json.loads(bytes(existing_receipt["receipt_json"]))
                if (
                    existing.get("terminal_dataset_sha256") != dataset_digest
                    or existing.get("candidate_projection_sha256") != projection_digest
                    or existing.get("candidate_universe_sha256") != universe_digest
                ):
                    raise TrialInvalid("alternate_terminal_seal_receipt_prohibited")
            conn.execute(
                "INSERT INTO terminal_pending VALUES(1,?,?,?,?,?)",
                (
                    encoded,
                    dataset_digest,
                    projection_digest,
                    universe_digest,
                    _utc_text(deadline),
                ),
            )
        return terminal

    def seal_terminal(
        self,
        registry: Mapping[str, Any],
        payload: Mapping[str, Any],
        *,
        recorded_at: datetime | None = None,
        allow_draft: bool = False,
    ) -> dict[str, Any]:
        terminal = self.stage_terminal(registry, payload, allow_draft=allow_draft)
        dataset_digest = _sha256(terminal.get("dataset_sha256"), "terminal_dataset_sha")
        candidates = [
            _parse_candidate(item, index)
            for index, item in enumerate(_list(payload.get("candidates"), "candidates"))
        ]
        projection_digest = _candidate_projection_sha256(candidates)
        activated_at = _utc(payload.get("activated_at_utc"), "activated_at")
        candidate_universe_digest = _candidate_universe_sha256(candidates)
        existing = self.receipt("terminal_seal")
        if existing is not None:
            if (
                existing["terminal_dataset_sha256"] != dataset_digest
                or existing["candidate_projection_sha256"] != projection_digest
                or existing["candidate_universe_sha256"] != candidate_universe_digest
                or _utc(existing["enrollment_deadline_utc"], "terminal_receipt_deadline")
                != enrollment_deadline(activated_at)
            ):
                raise TrialInvalid("alternate_terminal_seal_receipt_prohibited")
            return existing
        receipt_recorded_at = recorded_at or datetime.now(UTC)
        if receipt_recorded_at.tzinfo is None:
            raise TrialInvalid("terminal_seal_recorded_at_naive")
        receipt_recorded_at = receipt_recorded_at.astimezone(UTC)
        terminal_sealed_at = _utc(terminal.get("sealed_at_utc"), "terminal_sealed_at")
        evaluated_at = _utc(payload.get("evaluated_at_utc"), "evaluated_at")
        if not terminal_sealed_at <= receipt_recorded_at <= evaluated_at:
            raise TrialInvalid("terminal_seal_recorded_at_invalid")
        receipt = _build_receipt(
            kind="terminal_seal",
            recorded_at=receipt_recorded_at,
            deadline=enrollment_deadline(activated_at),
            terminal_dataset_sha256=dataset_digest,
            candidate_projection_sha256=projection_digest,
            candidate_universe_sha256=candidate_universe_digest,
        )
        return self._put_receipt(receipt)

    def existing_report(self) -> dict[str, Any] | None:
        with contextlib.closing(self._connect()) as conn, conn:
            row = conn.execute(
                "SELECT report_json,report_sha256 FROM decision_report WHERE singleton=1"
            ).fetchone()
        if row is None:
            return None
        parsed = json.loads(bytes(row["report_json"]))
        if not isinstance(parsed, dict) or parsed.get("report_sha256") != row["report_sha256"]:
            raise TrialInvalid("stored_decision_report_invalid")
        unsigned = dict(parsed)
        unsigned.pop("report_sha256", None)
        if _canonical_sha256(unsigned) != row["report_sha256"]:
            raise TrialInvalid("stored_decision_report_digest_mismatch")
        return parsed

    def record_report(self, report: Mapping[str, Any]) -> dict[str, Any]:
        digest = _sha256(report.get("report_sha256"), "report_sha")
        unsigned = dict(report)
        unsigned.pop("report_sha256", None)
        if _canonical_sha256(unsigned) != digest:
            raise TrialInvalid("decision_report_digest_mismatch")
        encoded = rfc8785.dumps(dict(report))
        with contextlib.closing(self._connect()) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT report_json FROM decision_report WHERE singleton=1"
            ).fetchone()
            if row is not None:
                existing = json.loads(bytes(row["report_json"]))
                if existing != dict(report):
                    raise TrialInvalid("second_terminal_look_prohibited")
                return dict(existing)
            conn.execute(
                "INSERT INTO decision_report(singleton,report_json,report_sha256) VALUES(1,?,?)",
                (encoded, digest),
            )
        return dict(report)


def evaluate_trial(
    registry: Mapping[str, Any],
    payload: Mapping[str, Any],
    *,
    allow_draft: bool = False,
    terminal_receipt: Mapping[str, Any] | None = None,
    deadline_miss_receipt: Mapping[str, Any] | None = None,
    terminal_validation_only: bool = False,
) -> dict[str, Any]:
    """Evaluate one blinded monitoring or sealed terminal input deterministically."""

    try:
        _validate_registry(registry, allow_draft=allow_draft)
        _exact_keys(
            payload,
            {
                "schema_version",
                "hypothesis_id",
                "activated_at_utc",
                "evaluated_at_utc",
                "entry_date_completions",
                "candidates",
                "integrity_checks",
                "terminal_dataset",
            },
            "trial_input",
        )
        if payload["schema_version"] != TRIAL_INPUT_SCHEMA_VERSION:
            raise TrialInvalid("trial_input_schema_invalid")
        if payload["hypothesis_id"] != HYPOTHESIS_ID:
            raise TrialInvalid("trial_input_hypothesis_mismatch")
        activated_at = _utc(payload["activated_at_utc"], "activated_at")
        evaluated_at = _utc(payload["evaluated_at_utc"], "evaluated_at")
        if evaluated_at < activated_at:
            raise TrialInvalid("evaluation_before_activation")
        deadline = enrollment_deadline(activated_at)
        if registry.get("status") == "active":
            activation = _mapping(registry.get("activation"), "registry_activation")
            if _utc(activation.get("activated_at_utc"), "registry_activated_at") != activated_at:
                raise TrialInvalid("input_activation_mismatch")
        completions = _entry_date_completions(payload["entry_date_completions"], evaluated_at)
        candidates = [
            _parse_candidate(item, index)
            for index, item in enumerate(_list(payload["candidates"], "candidates"))
        ]
        if candidates != sorted(candidates, key=_candidate_order):
            raise TrialInvalid("candidate_order_invalid")
        if len({candidate.candidate_id for candidate in candidates}) != len(candidates):
            raise TrialInvalid("candidate_id_duplicate")
        if len({candidate.evidence_record_sha256 for candidate in candidates}) != len(candidates):
            raise TrialInvalid("candidate_evidence_digest_duplicate")
        if len({candidate.packet_id for candidate in candidates}) != len(candidates):
            raise TrialInvalid("candidate_packet_id_duplicate")
        if len({candidate.entry_rank_sha256 for candidate in candidates}) != len(candidates):
            raise TrialInvalid("candidate_entry_rank_duplicate")
        if len({(candidate.accession_number, candidate.symbol) for candidate in candidates}) != len(
            candidates
        ):
            raise TrialInvalid("candidate_accession_symbol_duplicate")
        if any(
            candidate.source_first_observed_at < activated_at
            or candidate.source_first_observed_at >= deadline
            for candidate in candidates
        ):
            raise TrialInvalid("candidate_outside_activation_window")
        if any(candidate.source_first_observed_at > evaluated_at for candidate in candidates):
            raise TrialInvalid("candidate_observed_after_evaluation")
        if any(
            candidate.entry_date is not None
            and candidate.entry_date
            < candidate.source_first_observed_at.astimezone(NEW_YORK).date()
            for candidate in candidates
        ):
            raise TrialInvalid("candidate_entry_before_observation")
        if completions and any(
            candidate.enrollment_state == "pending_entry_selection"
            and (candidate.entry_date is None or candidate.entry_date in completions)
            for candidate in candidates
        ):
            raise TrialInvalid("complete_entry_dates_skip_pending_candidate")
        enrolled = [
            candidate for candidate in candidates if candidate.enrollment_state == "enrolled"
        ]
        ordered_enrolled = sorted(enrolled, key=_enrollment_order)
        if [candidate.sequence for candidate in ordered_enrolled] != list(
            range(1, len(ordered_enrolled) + 1)
        ):
            raise TrialInvalid("enrollment_sequence_not_gap_free_or_ordered")
        freeze = _freeze_boundary(candidates, completions)
        freeze_boundary = freeze[0] if freeze else None
        freeze_completed_at = freeze[1] if freeze else None
        if freeze_boundary is not None and any(
            candidate.entry_date is not None and candidate.entry_date > freeze_boundary
            for candidate in enrolled
        ):
            raise TrialInvalid("enrollment_continued_after_freeze")
        state_counts = Counter(candidate.enrollment_state for candidate in candidates)
        candidate_universe_sha = _candidate_universe_sha256(candidates)
        terminal_present = payload["terminal_dataset"] is not None
        checks = _integrity(payload["integrity_checks"], terminal=terminal_present)
        failed_checks = sorted(name for name, state in checks.items() if state is False)
        if failed_checks:
            report = _base_report(
                evaluated_at=evaluated_at,
                activated_at=activated_at,
                deadline=deadline,
                state="INVALID",
                reasons=[f"integrity_failed:{name}" for name in failed_checks],
                counts=state_counts,
                freeze_boundary=freeze_boundary,
            )
            return _sealed_report(report)
        deadline_receipt_sha: str | None = None
        if deadline_miss_receipt is not None:
            deadline_receipt_sha = _receipt_sha256(deadline_miss_receipt, "deadline_miss")
            if (
                _utc(
                    deadline_miss_receipt["enrollment_deadline_utc"],
                    "deadline_receipt_deadline",
                )
                != deadline
            ):
                raise TrialInvalid("deadline_receipt_deadline_mismatch")
            deadline_recorded_at = _utc(
                deadline_miss_receipt["recorded_at_utc"],
                "deadline_receipt_recorded_at",
            )
            if deadline_recorded_at < deadline or deadline_recorded_at > evaluated_at:
                raise TrialInvalid("deadline_receipt_time_invalid")
            if deadline_miss_receipt["candidate_universe_sha256"] != candidate_universe_sha:
                raise TrialInvalid("deadline_receipt_candidate_universe_mismatch")
        deadline_missed = deadline_receipt_sha is not None or (
            evaluated_at >= deadline
            and (freeze_completed_at is None or freeze_completed_at >= deadline)
        )
        if deadline_missed:
            if deadline_receipt_sha is None:
                raise TrialInvalid("deadline_miss_receipt_required")
            if terminal_present:
                raise TrialInvalid("terminal_dataset_after_deadline_miss")
            state = "COLLECTING" if state_counts["pending_entry_selection"] else "KILL"
            reason = (
                "draining_predeadline_pending_entry_selection"
                if state == "COLLECTING"
                else "insufficient_enrollment"
            )
            report = _base_report(
                evaluated_at=evaluated_at,
                activated_at=activated_at,
                deadline=deadline,
                state=state,
                reasons=[reason],
                counts=state_counts,
            )
            report["deadline_miss_receipt_sha256"] = deadline_receipt_sha
            return _sealed_report(report)
        if freeze_boundary is None:
            if terminal_present:
                raise TrialInvalid("terminal_dataset_before_cohort_freeze")
            return _sealed_report(
                _base_report(
                    evaluated_at=evaluated_at,
                    activated_at=activated_at,
                    deadline=deadline,
                    state="COLLECTING",
                    reasons=["enrollment_thresholds_not_reached"],
                    counts=state_counts,
                )
            )
        frozen_candidates = [
            candidate
            for candidate in ordered_enrolled
            if candidate.entry_date is not None and candidate.entry_date <= freeze_boundary
        ]
        candidate_projection_sha = _candidate_projection_sha256(candidates)
        if not terminal_present:
            return _sealed_report(
                _base_report(
                    evaluated_at=evaluated_at,
                    activated_at=activated_at,
                    deadline=deadline,
                    state="COLLECTING",
                    reasons=["awaiting_frozen_outcomes_and_terminal_seal"],
                    counts=state_counts,
                    freeze_boundary=freeze_boundary,
                )
            )
        digest, challenger, control, routine, diagnostic_status, terminal_sealed_at = (
            _parse_terminal(
                payload["terminal_dataset"],
                freeze_boundary=freeze_boundary,
                frozen_candidates=frozen_candidates,
                candidate_projection_sha256=candidate_projection_sha,
                evaluated_at=evaluated_at,
            )
        )
        if terminal_validation_only:
            report = _base_report(
                evaluated_at=evaluated_at,
                activated_at=activated_at,
                deadline=deadline,
                state="COLLECTING",
                reasons=["terminal_payload_validated_not_evaluated"],
                counts=state_counts,
                freeze_boundary=freeze_boundary,
            )
            report["terminal_dataset_sha256"] = digest
            report["candidate_projection_sha256"] = candidate_projection_sha
            return _sealed_report(report)
        if terminal_receipt is None:
            raise TrialInvalid("terminal_seal_receipt_required")
        terminal_receipt_sha = _receipt_sha256(terminal_receipt, "terminal_seal")
        terminal_recorded_at = _utc(
            terminal_receipt["recorded_at_utc"], "terminal_receipt_recorded_at"
        )
        if (
            terminal_receipt["terminal_dataset_sha256"] != digest
            or terminal_receipt["candidate_projection_sha256"] != candidate_projection_sha
            or terminal_receipt["candidate_universe_sha256"] != candidate_universe_sha
            or _utc(
                terminal_receipt["enrollment_deadline_utc"],
                "terminal_receipt_deadline",
            )
            != deadline
            or terminal_recorded_at < terminal_sealed_at
            or terminal_recorded_at > evaluated_at
        ):
            raise TrialInvalid("terminal_seal_receipt_material_mismatch")
        primary = _bootstrap(
            challenger,
            value_name="alpha_20",
            domain=PRNG_DOMAIN,
            include_test=True,
        )
        metrics, gates = _economic_metrics(challenger)
        gates["all_integrity_and_reconciliation_checks_pass"] = all(
            state is True for state in checks.values()
        )
        falsification: dict[str, Any] = {}
        for name, trades in (("control", control), ("routine", routine)):
            metadata = diagnostic_status[name]
            if metadata["status"] == "unavailable":
                falsification[name] = {
                    "status": "unavailable",
                    "error_code": metadata["error_code"],
                    "accounting": metadata,
                }
            else:
                falsification[name] = {
                    "status": "available",
                    "error_code": None,
                    "accounting": metadata,
                    **_bootstrap(
                        trades,
                        value_name="alpha_20",
                        domain=PRNG_DOMAIN + f"|diagnostic|{name}".encode(),
                        include_test=False,
                    ),
                }
        statistical_pass = bool(
            primary["p_value"] is not None and primary["p_value"] <= ALPHA_THRESHOLD
        )
        failed_gates = sorted(name for name, passed in gates.items() if not passed)
        promote = statistical_pass and not failed_gates
        reasons = []
        if not statistical_pass:
            reasons.append("primary_p_value_above_0.025")
        reasons.extend(f"economic_gate_failed:{name}" for name in failed_gates)
        if promote:
            reasons = ["primary_and_all_cogates_passed"]
        report = _base_report(
            evaluated_at=evaluated_at,
            activated_at=activated_at,
            deadline=deadline,
            state="PROMOTE_RECOMMENDED" if promote else "KILL",
            reasons=reasons,
            counts=state_counts,
            freeze_boundary=freeze_boundary,
        )
        report["terminal_dataset_sha256"] = digest
        report["candidate_projection_sha256"] = candidate_projection_sha
        report["terminal_seal_receipt_sha256"] = terminal_receipt_sha
        report["inference"] = {
            "test": "null_centered_circular_moving_block_bootstrap_trade_weighted_mean",
            "side": "one_sided_greater",
            "cluster": "entry_date",
            "block_length_clusters": BLOCK_LENGTH,
            "resamples": BOOTSTRAP_RESAMPLES,
            "seed": BOOTSTRAP_SEED,
            "prng": PRNG_NAME,
            **primary,
        }
        report["economic_metrics"] = metrics
        report["economic_gates"] = gates
        report["falsification_context"] = falsification
        return _sealed_report(report)
    except TrialInvalid as exc:
        return _invalid_report(payload, exc.code)
    except ArithmeticError:
        return _invalid_report(payload, "numeric_arithmetic_invalid")


def evaluate_with_store(
    registry: Mapping[str, Any],
    payload: Mapping[str, Any],
    store: TrialSealStore,
    *,
    allow_draft: bool = False,
) -> dict[str, Any]:
    """Enforce append-only deadline state and exactly one persisted terminal report."""

    try:
        existing = store.existing_report()
        if existing is not None:
            return existing
        terminal_receipt = store.receipt("terminal_seal")
        deadline_receipt = store.receipt("deadline_miss")
        report = evaluate_trial(
            registry,
            payload,
            allow_draft=allow_draft,
            terminal_receipt=terminal_receipt,
            deadline_miss_receipt=deadline_receipt,
        )
        if report.get("reason_codes") == ["deadline_miss_receipt_required"]:
            evaluated_at = _utc(payload.get("evaluated_at_utc"), "evaluated_at")
            deadline_receipt = store.seal_deadline_miss(payload, recorded_at=evaluated_at)
            report = evaluate_trial(
                registry,
                payload,
                allow_draft=allow_draft,
                terminal_receipt=terminal_receipt,
                deadline_miss_receipt=deadline_receipt,
            )
        deadline_kill = report.get("reason_codes") == ["insufficient_enrollment"]
        inferential_decision = report.get("state") in {"PROMOTE_RECOMMENDED", "KILL"}
        if deadline_kill or (terminal_receipt is not None and inferential_decision):
            return store.record_report(report)
        return report
    except TrialInvalid as exc:
        return _invalid_report(payload, exc.code)


def _load_json(path: Path) -> Mapping[str, Any]:
    def reject_constant(value: str) -> None:
        raise ValueError(f"non-finite JSON constant: {value}")

    parsed = json.loads(path.read_text(encoding="utf-8"), parse_constant=reject_constant)
    if not isinstance(parsed, Mapping):
        raise ValueError(f"JSON root must be an object: {path}")
    return parsed


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Evaluate the frozen OPP-E07-V1 trial")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--seal-db", type=Path, required=True)
    parser.add_argument("--seal-terminal", action="store_true")
    parser.add_argument(
        "--registry",
        type=Path,
        default=Path("docs/research/registry/OPP-E07-V1.json"),
    )
    parser.add_argument("--output", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    payload: Mapping[str, Any] = {}
    try:
        registry = _load_json(args.registry)
        payload = _load_json(args.input)
        now = datetime.now(UTC)
        if _utc(payload.get("evaluated_at_utc"), "evaluated_at") > now + timedelta(minutes=1):
            raise TrialInvalid("production_evaluation_time_in_future")
        if _utc(payload.get("activated_at_utc"), "activated_at") > now:
            raise TrialInvalid("production_activation_time_in_future")
        store = TrialSealStore(args.seal_db)
        if args.seal_terminal:
            _validate_registry(registry, allow_draft=False)
            receipt = store.seal_terminal(registry, payload)
            print(rfc8785.dumps(receipt).decode("utf-8"))
            return 0
        report = evaluate_with_store(registry, payload, store)
        encoded = rfc8785.dumps(report)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            with args.output.open("xb") as stream:
                stream.write(encoded + b"\n")
        else:
            print(encoded.decode("utf-8"))
        return {"COLLECTING": 0, "PROMOTE_RECOMMENDED": 0, "KILL": 2, "INVALID": 3}[
            str(report["state"])
        ]
    except (OSError, ValueError, sqlite3.Error) as exc:
        code = exc.code if isinstance(exc, TrialInvalid) else "production_io_or_input_invalid"
        invalid = _invalid_report(payload, code)
        print(rfc8785.dumps(invalid).decode("utf-8"), file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
