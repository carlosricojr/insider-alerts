from __future__ import annotations

import asyncio
import contextlib
import csv
import hashlib
import json
import os
import re
import secrets
import shutil
import sqlite3
import sys
import time
from bisect import bisect_right
from collections.abc import Callable
from dataclasses import asdict, dataclass, is_dataclass
from datetime import UTC, date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Literal, cast

import typer

from insider_alerts.backtest.data import load_scored_signals
from insider_alerts.backtest.engine import (
    BacktestParams,
    evaluate_parameter_grid,
    run_backtest,
    run_walk_forward,
)
from insider_alerts.backtest.event_data import load_canonical_events
from insider_alerts.backtest.event_study import (
    CONVICTION_FEATURE_WEIGHTS,
    TradabilityConfig,
    run_oos_event_study,
)
from insider_alerts.backtest.fundamentals import (
    load_cached_companyfacts,
    refresh_companyfacts,
    shares_outstanding_as_of,
)
from insider_alerts.backtest.intraday_prices import (
    build_intraday_requests,
    completed_minute_bar_sessions,
    filter_completed_minute_bars,
    get_minute_bars,
    refresh_ibkr_minute_bars,
)
from insider_alerts.backtest.models import DailyBar
from insider_alerts.backtest.prices import (
    PriceDataError,
    StooqPriceClient,
    get_price_bar_bounds,
    get_price_bars,
    refresh_price_bars,
)
from insider_alerts.backtest.readiness import (
    EventStudyReadinessConfig,
    audit_event_study_readiness,
)
from insider_alerts.backtest.signal_study import (
    CONFIRMATORY_FAMILY_SIZE,
    DAILY_EXECUTION_RULES,
    collect_daily_strategy_observations,
    compute_point_in_time_features,
    evaluate_daily_hypothesis_family,
    fixed_slot_portfolio_summary,
    load_delivered_signals,
    load_historical_approved_replay,
    matched_random_date_control,
)
from insider_alerts.config import Settings, get_settings
from insider_alerts.execution.autopilot_watchdog import (
    AutopilotHealthStore,
    RuntimeOwnershipError,
    autopilot_health_status,
    autopilot_runtime_budget,
    run_autopilot_watchdog,
    sec_ingestion_runtime_budget,
    validate_sec_ingestion_stale_threshold,
    validate_stale_threshold,
)
from insider_alerts.execution.canary import (
    ARM_PHRASE,
    SOURCE_REVISION_CHECK_INTERVAL_SECONDS,
    CanaryConfig,
    CanaryRunner,
    poll_delay_seconds,
    runtime_source_fingerprint,
)
from insider_alerts.execution.canary import (
    status_report as live_canary_status_report,
)
from insider_alerts.execution.ibkr import IbkrBroker, IbkrExecutionError
from insider_alerts.execution.watchdog import append_watchdog_log, run_scheduled_task_watchdog
from insider_alerts.execution.windows_job import ensure_kill_on_close_process_tree
from insider_alerts.notify.ntfy import NtfyNotificationError, NtfyNotifier, NtfyTransportEvent
from insider_alerts.research.bar_feed import bar_feed_status
from insider_alerts.research.capture import (
    ProcessTreeCleanupError,
    capture_status,
    resolve_git_commit,
    run_hidden_process,
)
from insider_alerts.research.diagnostics import diagnostic_status
from insider_alerts.research.notification_transport import (
    NotificationJournalConfig,
    NotificationTransportJournal,
    activate_notification_journal,
    notification_journal_status,
    notification_transport_id,
)
from insider_alerts.research.option_chain_admission import (
    OptionChainAdmissionConfig,
    capture_predecision_option_chain,
)
from insider_alerts.research.session_feed import session_feed_status
from insider_alerts.research.trial_runtime import trial_runtime_status
from insider_alerts.review.queue import (
    DecisionValidationError,
    apply_decision,
    ensure_review_tables,
    get_review_packet,
    list_deadletters,
    list_notification_outbox,
    list_pending_review_packets,
    mark_notification_delivered,
    mark_notification_suppressed,
    replay_deadletter,
)
from insider_alerts.sec.client import SecHttpClient, SecHttpError
from insider_alerts.sec.pipeline import (
    BackfillResult,
    EnrichResult,
    PollResult,
    QueueResult,
    backfill_form4_filings,
    enqueue_review_packets,
    enrich_filings_with_xml_url,
    run_sec_poll_once,
)
from insider_alerts.sec.rss import SecRssParseError
from insider_alerts.sec.store import get_filing_date_bounds

app = typer.Typer(help="Insider alerts command-line interface.")
notify_app = typer.Typer(help="Notification commands.")
sec_app = typer.Typer(help="SEC ingestion commands.")
review_app = typer.Typer(help="Review queue commands.")
ops_app = typer.Typer(help="Operations commands.")
app.add_typer(notify_app, name="notify")
app.add_typer(sec_app, name="sec")
app.add_typer(review_app, name="review")
app.add_typer(ops_app, name="ops")


@dataclass(slots=True)
class AutoDecisionRuleResult:
    decision: str
    reason: str
    source: str
    confidence: float | None
    reason_code: str = "general"
    conviction_score: float | None = None
    conviction_holding_pct: float | None = None
    conviction_value_pct: float | None = None
    conviction_liquidity_pct: float | None = None


@dataclass(slots=True)
class AutoPilotCycleResult:
    fetched: int
    inserted: int
    skipped_existing: int
    enriched_scanned: int
    enriched_updated: int
    enqueue_processed: int
    enqueue_enqueued: int
    pending_seen: int
    decided: int
    approved: int
    rejected: int
    escalated: int
    deadlettered: int
    notified: int
    approved_high_edge: int
    rejected_low_edge: int
    escalated_missing_context: int
    escalated_schema_invalid: int
    quant_deferred: int
    notify_suppressed_duplicate: int = 0
    option_chain_succeeded: int = 0
    option_chain_skipped_cadence: int = 0
    option_chain_failed: int = 0
    option_chain_timed_out: int = 0
    option_chain_ambiguous: int = 0
    enrichment_http_failed: int = 0
    enrichment_xml_not_found: int = 0
    enqueue_http_failed: int = 0
    enqueue_parse_failed: int = 0
    enqueue_market_failed: int = 0
    outbox_notified: int = 0
    source_items_seen: int = 0
    source_boundary_rejected: int = 0
    source_invalid_items: int = 0


@dataclass(slots=True)
class SecIngestionCycleResult:
    fetched: int
    inserted: int
    skipped_existing: int
    enriched_scanned: int
    enriched_updated: int
    enqueue_processed: int
    enqueue_enqueued: int
    enrichment_http_failed: int = 0
    enrichment_xml_not_found: int = 0
    enqueue_http_failed: int = 0
    enqueue_parse_failed: int = 0
    enqueue_market_failed: int = 0
    source_items_seen: int = 0
    source_boundary_rejected: int = 0
    source_invalid_items: int = 0


_JSON_OBJECT_RE = re.compile(r"\{.*\}", re.DOTALL)
_CONVICTION_METRICS = (
    "holding_change_ratio",
    "open_market_gross_value",
    "trade_pct_daily_turnover",
)


@dataclass(slots=True)
class ConvictionHistory:
    by_role: dict[str, dict[str, list[float]]]
    global_metrics: dict[str, list[float]]
    sample_count: int


def _to_float(value: object) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return False


def _parse_float_grid(raw: str, *, min_value: float | None = None) -> list[float]:
    values: list[float] = []
    seen: set[float] = set()
    for token in raw.split(","):
        stripped = token.strip()
        if not stripped:
            continue
        try:
            value = float(stripped)
        except ValueError as exc:
            raise typer.BadParameter(f"invalid numeric value: {stripped}") from exc
        if min_value is not None and value < min_value:
            raise typer.BadParameter(f"value {value} must be >= {min_value}")
        if value in seen:
            continue
        seen.add(value)
        values.append(value)
    if not values:
        raise typer.BadParameter("grid cannot be empty")
    return sorted(values)


def _parse_int_grid(raw: str, *, min_value: int | None = None) -> list[int]:
    values: list[int] = []
    seen: set[int] = set()
    for token in raw.split(","):
        stripped = token.strip()
        if not stripped:
            continue
        try:
            value = int(stripped)
        except ValueError as exc:
            raise typer.BadParameter(f"invalid integer value: {stripped}") from exc
        if min_value is not None and value < min_value:
            raise typer.BadParameter(f"value {value} must be >= {min_value}")
        if value in seen:
            continue
        seen.add(value)
        values.append(value)
    if not values:
        raise typer.BadParameter("grid cannot be empty")
    return sorted(values)


def _normalize_role_tier(value: object) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip().lower()
    return "unknown"


def _empty_conviction_history() -> ConvictionHistory:
    return ConvictionHistory(
        by_role={},
        global_metrics={metric: [] for metric in _CONVICTION_METRICS},
        sample_count=0,
    )


def _load_conviction_history(
    db_path: str,
    *,
    as_of_date: date,
    lookback_days: int,
) -> ConvictionHistory:
    start_date = as_of_date - timedelta(days=max(lookback_days, 1))
    history = _empty_conviction_history()
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT
                    json_extract(rp.payload_json, '$.rationale.role_tier') AS role_tier,
                    json_extract(
                        rp.payload_json,
                        '$.rationale.holding_change_ratio'
                    ) AS holding_change_ratio,
                    json_extract(
                        rp.payload_json,
                        '$.rationale.open_market_gross_value'
                    ) AS open_market_gross_value,
                    json_extract(
                        rp.payload_json,
                        '$.rationale.trade_pct_daily_turnover'
                    ) AS trade_pct_daily_turnover,
                    json_extract(
                        rp.payload_json,
                        '$.rationale.open_market_buy_shares'
                    ) AS open_market_buy_shares,
                    json_extract(
                        rp.payload_json,
                        '$.rationale.open_market_net_shares'
                    ) AS open_market_net_shares
                FROM review_packets AS rp
                INNER JOIN filings AS f
                    ON f.accession_number = rp.accession_number
                    AND f.cik = rp.cik
                    AND f.form_type = rp.form_type
                WHERE date(f.filed_at) >= ?
                  AND date(f.filed_at) < ?
                """,
                (start_date.isoformat(), as_of_date.isoformat()),
            ).fetchall()
    except sqlite3.Error:
        return history

    for row in rows:
        open_market_buy_shares = _to_float(row["open_market_buy_shares"])
        open_market_net_shares = _to_float(row["open_market_net_shares"])
        if (
            open_market_buy_shares is None
            or open_market_buy_shares <= 0
            or open_market_net_shares is None
            or open_market_net_shares <= 0
        ):
            continue

        role_tier = _normalize_role_tier(row["role_tier"])
        role_entry = history.by_role.setdefault(
            role_tier,
            {metric: [] for metric in _CONVICTION_METRICS},
        )
        any_metric = False
        for metric in _CONVICTION_METRICS:
            value = _to_float(row[metric])
            if value is None:
                continue
            role_entry[metric].append(value)
            history.global_metrics[metric].append(value)
            any_metric = True
        if any_metric:
            history.sample_count += 1

    for metric in _CONVICTION_METRICS:
        history.global_metrics[metric].sort()
    for role_values in history.by_role.values():
        for metric in _CONVICTION_METRICS:
            role_values[metric].sort()
    return history


def _percentile_rank(sorted_values: list[float], value: float | None) -> float | None:
    if value is None or not sorted_values:
        return None
    rank = bisect_right(sorted_values, value)
    return (rank / len(sorted_values)) * 100.0


def _compute_conviction_metrics(
    packet: dict[str, object],
    *,
    history: ConvictionHistory,
    min_role_samples: int,
) -> tuple[float | None, float | None, float | None, float | None, int]:
    payload = packet.get("payload")
    payload_dict = payload if isinstance(payload, dict) else {}
    rationale = payload_dict.get("rationale")
    rationale_dict = rationale if isinstance(rationale, dict) else {}

    role_tier = _normalize_role_tier(rationale_dict.get("role_tier"))
    role_metrics = history.by_role.get(role_tier, {})

    def _resolve_distribution(metric: str) -> list[float]:
        role_dist = role_metrics.get(metric, [])
        if len(role_dist) >= min_role_samples:
            return role_dist
        global_dist = history.global_metrics.get(metric, [])
        if global_dist:
            return global_dist
        return role_dist

    holding_ratio = _to_float(rationale_dict.get("holding_change_ratio"))
    value_gross = _to_float(rationale_dict.get("open_market_gross_value"))
    liquidity_impact = _to_float(rationale_dict.get("trade_pct_daily_turnover"))

    holding_pct = _percentile_rank(_resolve_distribution("holding_change_ratio"), holding_ratio)
    value_pct = _percentile_rank(_resolve_distribution("open_market_gross_value"), value_gross)
    liquidity_pct = _percentile_rank(
        _resolve_distribution("trade_pct_daily_turnover"),
        liquidity_impact,
    )

    weighted_sum = 0.0
    weight_total = 0.0
    for metric_pct, weight in (
        (holding_pct, CONVICTION_FEATURE_WEIGHTS["holding_change_ratio"]),
        (value_pct, CONVICTION_FEATURE_WEIGHTS["open_market_gross_value"]),
        (liquidity_pct, CONVICTION_FEATURE_WEIGHTS["trade_pct_daily_turnover"]),
    ):
        if metric_pct is None:
            continue
        weighted_sum += metric_pct * weight
        weight_total += weight
    conviction_score = (weighted_sum / weight_total) if weight_total > 0 else None
    missing_count = sum(metric is None for metric in (holding_pct, value_pct, liquidity_pct))
    return conviction_score, holding_pct, value_pct, liquidity_pct, missing_count


def _metrics_to_dict(metrics: object) -> dict[str, object]:
    if is_dataclass(metrics) and not isinstance(metrics, type):
        return cast(dict[str, object], asdict(metrics))
    if hasattr(metrics, "__dict__"):
        return cast(dict[str, object], dict(vars(metrics)))
    return {}


def _params_to_dict(params: BacktestParams) -> dict[str, object]:
    return {
        "min_score": params.min_score,
        "hold_days": params.hold_days,
        "stop_loss_pct": params.stop_loss_pct,
        "take_profit_rr": params.take_profit_rr,
    }


def _file_sha256(path: Path) -> str | None:
    try:
        digest = hashlib.sha256()
        with path.open("rb") as file_obj:
            while True:
                chunk = file_obj.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
        return digest.hexdigest()
    except OSError:
        return None


def _write_event_study_csv(
    output_csv_path: Path,
    *,
    aggregate_bucket_metrics: list[dict[str, object]],
) -> None:
    output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "horizon_days",
        "bucket_index",
        "bucket_count",
        "bucket_score_min",
        "bucket_score_max",
        "total_events",
        "executed_events",
        "benchmark_available_events",
        "execution_coverage_rate",
        "benchmark_coverage_rate",
        "mean_alpha",
        "median_alpha",
        "win_rate",
        "mean_alpha_ci_low",
        "mean_alpha_ci_high",
        "alpha_p_value",
        "alpha_q_value",
    ]
    with output_csv_path.open("w", newline="", encoding="utf-8") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=fieldnames)
        writer.writeheader()
        for metric in aggregate_bucket_metrics:
            writer.writerow({field: metric.get(field) for field in fieldnames})


def _json_default(value: object) -> object:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _load_confirmatory_gate(
    report_path: Path | None,
    *,
    candidate_hypothesis: str,
    expected_database_path: Path,
) -> dict[str, object]:
    if report_path is None:
        return {
            "pass": False,
            "reason": "confirmatory_report_required",
            "candidate_hypothesis": candidate_hypothesis,
        }
    try:
        payload = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {
            "pass": False,
            "reason": f"confirmatory_report_unreadable:{type(exc).__name__}",
            "candidate_hypothesis": candidate_hypothesis,
            "report_path": str(report_path),
        }
    if not isinstance(payload, dict):
        return {
            "pass": False,
            "reason": "confirmatory_report_not_object",
            "candidate_hypothesis": candidate_hypothesis,
            "report_path": str(report_path),
        }
    survivors = payload.get("surviving_hypotheses")
    family_size = payload.get("family_size")
    schema_version = payload.get("schema_version")
    cohort = payload.get("cohort")
    requested_start_date = payload.get("requested_start_date")
    requested_end_date = payload.get("requested_end_date")
    reported_database_path = payload.get("database_path")
    reported_database_sha256 = payload.get("database_sha256")
    family_valid = family_size == CONFIRMATORY_FAMILY_SIZE
    schema_valid = schema_version == "signal-study-v1"
    cohort_valid = cohort == "live"
    date_window_valid = (
        requested_start_date == "2026-02-11" and requested_end_date == "2026-08-17"
    )
    expected_database_exists = expected_database_path.is_file()
    try:
        database_path_valid = (
            isinstance(reported_database_path, str)
            and Path(reported_database_path).resolve() == expected_database_path.resolve()
        )
    except OSError:
        database_path_valid = False
    expected_database_sha256 = (
        _file_sha256(expected_database_path) if expected_database_exists else None
    )
    database_hash_valid = (
        isinstance(reported_database_sha256, str)
        and reported_database_sha256 == expected_database_sha256
    )
    candidate_survived = (
        isinstance(survivors, list)
        and candidate_hypothesis in {str(item) for item in survivors}
    )
    passed = all(
        (
            family_valid,
            schema_valid,
            cohort_valid,
            date_window_valid,
            expected_database_exists,
            database_path_valid,
            database_hash_valid,
            candidate_survived,
        )
    )
    return {
        "pass": passed,
        "reason": "passed" if passed else "locked_confirmatory_result_failed",
        "candidate_hypothesis": candidate_hypothesis,
        "report_path": str(report_path),
        "family_size": family_size,
        "family_size_valid": family_valid,
        "schema_version": schema_version,
        "schema_valid": schema_valid,
        "cohort": cohort,
        "cohort_valid": cohort_valid,
        "requested_start_date": requested_start_date,
        "requested_end_date": requested_end_date,
        "date_window_valid": date_window_valid,
        "expected_database_path": str(expected_database_path.resolve()),
        "expected_database_exists": expected_database_exists,
        "database_path": reported_database_path,
        "database_path_valid": database_path_valid,
        "expected_database_sha256": expected_database_sha256,
        "database_sha256": reported_database_sha256,
        "database_hash_valid": database_hash_valid,
        "candidate_survived": candidate_survived,
    }


def _evaluate_event_study_gates(
    *,
    readiness: object,
    event_study: object,
    bucket_count: int,
    min_fold_count: int,
    min_test_events: int,
    max_missing_price_skip_rate: float,
    core_horizons: tuple[int, ...],
    ci_lower_bound_bps: float,
    fdr_q_threshold: float,
    confirmatory_gate: dict[str, object],
) -> dict[str, object]:
    readiness_ready = bool(getattr(readiness, "is_ready", False))
    folds = list(getattr(event_study, "folds", []))
    aggregate_bucket_metrics = list(getattr(event_study, "aggregate_bucket_metrics", []))
    monotonicity = list(getattr(event_study, "monotonicity", []))
    negative_control = list(getattr(event_study, "negative_control", []))

    fold_count_pass = len(folds) >= min_fold_count
    fold_test_min_pass = all(
        int(getattr(fold, "test_event_count", 0)) >= min_test_events for fold in folds
    )
    available_horizons = sorted(
        {int(getattr(metric, "horizon_days", 0)) for metric in aggregate_bucket_metrics}
    )
    core_horizons_present = [horizon for horizon in core_horizons if horizon in available_horizons]

    top_metrics_by_horizon: dict[int, object] = {}
    for metric in aggregate_bucket_metrics:
        horizon = int(getattr(metric, "horizon_days", 0))
        bucket_index = int(getattr(metric, "bucket_index", 0))
        if bucket_index == bucket_count:
            top_metrics_by_horizon[horizon] = metric

    top_bucket_min_events_pass = all(
        int(getattr(top_metrics_by_horizon.get(horizon), "total_events", 0)) >= min_test_events
        for horizon in core_horizons_present
    )

    coverage_by_horizon: dict[str, float] = {}
    execution_coverage_guard = True
    for horizon in available_horizons:
        metrics_for_horizon = [
            metric
            for metric in aggregate_bucket_metrics
            if int(getattr(metric, "horizon_days", 0)) == horizon
        ]
        total_events = sum(
            int(getattr(metric, "total_events", 0)) for metric in metrics_for_horizon
        )
        executed_events = sum(
            int(getattr(metric, "executed_events", 0)) for metric in metrics_for_horizon
        )
        coverage = (executed_events / total_events) if total_events > 0 else 0.0
        coverage_by_horizon[str(horizon)] = coverage
        missing_rate = 1.0 - coverage
        if missing_rate > max_missing_price_skip_rate:
            execution_coverage_guard = False

    diagnostic_ready = (
        readiness_ready
        and fold_count_pass
        and fold_test_min_pass
        and top_bucket_min_events_pass
        and execution_coverage_guard
    )
    confirmatory_pass = bool(confirmatory_gate.get("pass", False))
    decision_grade = diagnostic_ready and confirmatory_pass

    ci_floor = ci_lower_bound_bps / 10000.0
    positive_core_count = 0
    ci_core_count = 0
    q_pass_core = False
    negative_control_pass_count = 0
    for horizon in core_horizons_present:
        metric = top_metrics_by_horizon.get(horizon)
        if metric is None:
            continue
        mean_alpha = getattr(metric, "mean_alpha", None)
        if mean_alpha is not None and float(mean_alpha) > 0:
            positive_core_count += 1
        ci_low = getattr(metric, "mean_alpha_ci_low", None)
        if ci_low is not None and float(ci_low) > ci_floor:
            ci_core_count += 1
        q_value = getattr(metric, "alpha_q_value", None)
        if q_value is not None and float(q_value) <= fdr_q_threshold:
            q_pass_core = True

        neg = next(
            (item for item in negative_control if int(getattr(item, "horizon_days", 0)) == horizon),
            None,
        )
        if neg is None:
            continue
        actual = getattr(neg, "actual_top_bucket_mean_alpha", None)
        null_high = getattr(neg, "null_ci_high", None)
        null_mean = getattr(neg, "null_mean_alpha", None)
        if actual is None:
            continue
        reference = null_high if null_high is not None else null_mean
        if reference is None:
            continue
        if float(actual) > float(reference):
            negative_control_pass_count += 1

    monotonic_non_negative_count = sum(
        1 for item in monotonicity if bool(getattr(item, "non_negative", False))
    )
    monotonicity_majority_pass = len(monotonicity) > 0 and monotonic_non_negative_count > (
        len(monotonicity) / 2
    )

    required_core_horizons = 2
    positive_core_pass = positive_core_count >= required_core_horizons
    ci_core_pass = ci_core_count >= required_core_horizons
    negative_control_pass = negative_control_pass_count >= required_core_horizons
    edge_pass = (
        decision_grade
        and positive_core_pass
        and ci_core_pass
        and monotonicity_majority_pass
        and q_pass_core
        and negative_control_pass
    )

    label = "promising_edge" if edge_pass else "no_go"
    if not diagnostic_ready:
        label = "non_decision_grade"
    elif not confirmatory_pass:
        label = "confirmatory_gate_failed"
    return {
        "label": label,
        "decision_grade": decision_grade,
        "edge_pass": edge_pass,
        "hard_gates": {
            "diagnostic_ready": diagnostic_ready,
            "locked_confirmatory_result_pass": confirmatory_pass,
            "readiness_pass": readiness_ready,
            "min_fold_count_pass": fold_count_pass,
            "min_test_events_per_fold_pass": fold_test_min_pass,
            "top_bucket_min_events_pass": top_bucket_min_events_pass,
            "execution_coverage_guard_pass": execution_coverage_guard,
        },
        "edge_gates": {
            "positive_top_bucket_core_horizons_count": positive_core_count,
            "positive_top_bucket_core_horizons_pass": positive_core_pass,
            "ci_lower_bound_core_horizons_count": ci_core_count,
            "ci_lower_bound_core_horizons_pass": ci_core_pass,
            "monotonicity_majority_pass": monotonicity_majority_pass,
            "fdr_top_bucket_core_pass": q_pass_core,
            "negative_control_core_horizons_count": negative_control_pass_count,
            "negative_control_core_horizons_pass": negative_control_pass,
        },
        "coverage_by_horizon": coverage_by_horizon,
        "thresholds": {
            "min_fold_count": min_fold_count,
            "min_test_events": min_test_events,
            "max_missing_price_skip_rate": max_missing_price_skip_rate,
            "core_horizons": list(core_horizons),
            "ci_lower_bound_bps": ci_lower_bound_bps,
            "fdr_q_threshold": fdr_q_threshold,
        },
    }


def _auto_decide_packet(
    packet: dict[str, object],
    *,
    approve_score_min: float,
    approve_net_buy_shares_min: float,
    reject_score_max: float,
) -> AutoDecisionRuleResult:
    packet_id = str(packet.get("packet_id", "unknown"))
    payload_obj = packet.get("payload")
    if not isinstance(payload_obj, dict):
        return AutoDecisionRuleResult(
            decision="escalate",
            reason=f"auto rule: packet={packet_id} missing payload",
            source="rules",
            confidence=None,
            reason_code="rules_missing_payload",
        )

    score = _to_float(payload_obj.get("score"))
    rationale_obj = payload_obj.get("rationale")
    net_buy_shares = None
    open_market_buy_shares = None
    holding_change_ratio = None
    has_10b5_1_plan = False
    has_equity_comp_event = False
    has_tax_withholding_language = False
    owner_is_ten_percent_owner = False
    owner_is_exec = False
    owner_is_entity = False
    if isinstance(rationale_obj, dict):
        net_buy_shares = _to_float(rationale_obj.get("net_buy_shares"))
        open_market_buy_shares = _to_float(rationale_obj.get("open_market_buy_shares"))
        holding_change_ratio = _to_float(rationale_obj.get("holding_change_ratio"))
        has_10b5_1_plan = _to_bool(rationale_obj.get("has_10b5_1_plan"))
        has_equity_comp_event = _to_bool(rationale_obj.get("has_equity_comp_event"))
        has_tax_withholding_language = _to_bool(rationale_obj.get("has_tax_withholding_language"))
        owner_is_ten_percent_owner = _to_bool(rationale_obj.get("owner_is_ten_percent_owner"))
        owner_is_exec = _to_bool(rationale_obj.get("owner_is_exec"))
        owner_is_entity = _to_bool(rationale_obj.get("owner_is_entity"))

    if score is None or net_buy_shares is None:
        return AutoDecisionRuleResult(
            decision="escalate",
            reason=f"auto rule: packet={packet_id} missing score/net_buy_shares",
            source="rules",
            confidence=None,
            reason_code="rules_missing_features",
        )

    if has_10b5_1_plan:
        return AutoDecisionRuleResult(
            decision="reject",
            reason=f"auto rule: packet={packet_id} flagged as 10b5-1/planned flow",
            source="rules",
            confidence=None,
            reason_code="reject_planned_flow",
        )

    if open_market_buy_shares is None or open_market_buy_shares <= 0:
        return AutoDecisionRuleResult(
            decision="reject",
            reason=f"auto rule: packet={packet_id} no discretionary open-market buying",
            source="rules",
            confidence=None,
            reason_code="reject_no_open_market_buy",
        )

    if has_equity_comp_event and has_tax_withholding_language:
        return AutoDecisionRuleResult(
            decision="reject",
            reason=f"auto rule: packet={packet_id} appears compensation/tax-withholding driven",
            source="rules",
            confidence=None,
            reason_code="reject_comp_tax_flow",
        )

    if owner_is_ten_percent_owner and not owner_is_exec:
        return AutoDecisionRuleResult(
            decision="reject",
            reason=f"auto rule: packet={packet_id} passive ten-percent owner flow",
            source="rules",
            confidence=None,
            reason_code="reject_passive_owner",
        )

    if (
        owner_is_entity
        and not owner_is_exec
        and holding_change_ratio is not None
        and holding_change_ratio < 0.002
    ):
        return AutoDecisionRuleResult(
            decision="reject",
            reason=(
                "auto rule: "
                f"packet={packet_id} low-conviction entity accumulation "
                f"(holding_change_ratio={holding_change_ratio:.5f})"
            ),
            source="rules",
            confidence=None,
            reason_code="reject_low_edge",
        )

    if score >= approve_score_min and net_buy_shares > approve_net_buy_shares_min:
        return AutoDecisionRuleResult(
            decision="approve",
            reason=(
                "auto rule: "
                f"score={score:.2f} >= {approve_score_min:.2f} and "
                f"net_buy_shares={net_buy_shares:.2f} > {approve_net_buy_shares_min:.2f}"
            ),
            source="rules",
            confidence=None,
            reason_code="rules_high_edge",
        )

    if score <= reject_score_max or net_buy_shares < 0:
        return AutoDecisionRuleResult(
            decision="reject",
            reason=(
                "auto rule: "
                f"score={score:.2f}, net_buy_shares={net_buy_shares:.2f} "
                f"(reject if score <= {reject_score_max:.2f} or net_buy_shares < 0)"
            ),
            source="rules",
            confidence=None,
            reason_code="reject_low_edge",
        )

    return AutoDecisionRuleResult(
        decision="escalate",
        reason=(
            "auto rule: "
            f"score={score:.2f}, net_buy_shares={net_buy_shares:.2f} "
            "(between approve/reject thresholds)"
        ),
        source="rules",
        confidence=None,
        reason_code="rules_ambiguous",
    )


def _apply_conviction_baseline(
    preliminary_rule: AutoDecisionRuleResult,
    packet: dict[str, object],
    *,
    history: ConvictionHistory,
    min_history_samples: int,
    min_role_samples: int,
    conviction_min_score: float,
    conviction_reject_max: float,
    conviction_value_pct_min: float,
    conviction_holding_pct_min: float,
    conviction_liquidity_pct_min: float,
    director_turnover_min: float,
    shock_turnover_min: float,
) -> AutoDecisionRuleResult:
    if preliminary_rule.decision != "approve":
        return preliminary_rule

    payload = packet.get("payload")
    payload_dict = payload if isinstance(payload, dict) else {}
    rationale = payload_dict.get("rationale")
    rationale_dict = rationale if isinstance(rationale, dict) else {}
    packet_id = str(packet.get("packet_id", "unknown"))

    trade_pct_daily_turnover = _to_float(rationale_dict.get("trade_pct_daily_turnover"))
    regime_earnings_shock_flag = _to_bool(rationale_dict.get("regime_earnings_shock_flag"))
    role_tier = _normalize_role_tier(rationale_dict.get("role_tier"))

    if role_tier == "director" and (
        trade_pct_daily_turnover is None or trade_pct_daily_turnover < director_turnover_min
    ):
        reason_code = (
            "missing_market_context"
            if trade_pct_daily_turnover is None
            else "safety_low_edge_director"
        )
        return AutoDecisionRuleResult(
            decision="escalate",
            reason=(
                "baseline rule: "
                "director signal has low liquidity impact "
                f"(trade_pct_daily_turnover={trade_pct_daily_turnover})"
            ),
            source="rules",
            confidence=None,
            reason_code=reason_code,
        )

    if regime_earnings_shock_flag and (
        trade_pct_daily_turnover is None or trade_pct_daily_turnover < shock_turnover_min
    ):
        reason_code = (
            "missing_market_context"
            if trade_pct_daily_turnover is None
            else "safety_shock_regime_block"
        )
        return AutoDecisionRuleResult(
            decision="escalate",
            reason=(
                "baseline rule: "
                "post-shock regime requires stronger liquidity conviction "
                f"(trade_pct_daily_turnover={trade_pct_daily_turnover})"
            ),
            source="rules",
            confidence=None,
            reason_code=reason_code,
        )

    if history.sample_count < min_history_samples:
        return preliminary_rule

    (
        conviction_score,
        holding_pct,
        value_pct,
        liquidity_pct,
        missing_count,
    ) = _compute_conviction_metrics(
        packet,
        history=history,
        min_role_samples=min_role_samples,
    )

    if missing_count >= 2:
        return AutoDecisionRuleResult(
            decision="escalate",
            reason=(
                "baseline rule: insufficient conviction dimensions "
                f"(missing={missing_count}, packet={packet_id})"
            ),
            source="rules",
            confidence=None,
            reason_code="rules_missing_conviction_data",
            conviction_score=conviction_score,
            conviction_holding_pct=holding_pct,
            conviction_value_pct=value_pct,
            conviction_liquidity_pct=liquidity_pct,
        )

    if conviction_score is None:
        return AutoDecisionRuleResult(
            decision="escalate",
            reason=f"baseline rule: conviction score unavailable (packet={packet_id})",
            source="rules",
            confidence=None,
            reason_code="rules_missing_conviction_data",
            conviction_holding_pct=holding_pct,
            conviction_value_pct=value_pct,
            conviction_liquidity_pct=liquidity_pct,
        )

    value_gate = value_pct is not None and value_pct >= conviction_value_pct_min
    holding_gate = holding_pct is not None and holding_pct >= conviction_holding_pct_min
    liquidity_gate = liquidity_pct is not None and liquidity_pct >= conviction_liquidity_pct_min

    if conviction_score < conviction_reject_max:
        return AutoDecisionRuleResult(
            decision="reject",
            reason=(
                "baseline rule: low conviction "
                f"(conviction={conviction_score:.1f}, "
                f"holding_pct={holding_pct}, value_pct={value_pct}, "
                f"liquidity_pct={liquidity_pct})"
            ),
            source="rules",
            confidence=None,
            reason_code="reject_low_edge",
            conviction_score=conviction_score,
            conviction_holding_pct=holding_pct,
            conviction_value_pct=value_pct,
            conviction_liquidity_pct=liquidity_pct,
        )

    if conviction_score >= conviction_min_score and value_gate and (holding_gate or liquidity_gate):
        return AutoDecisionRuleResult(
            decision="approve",
            reason=(
                "baseline rule: pass "
                f"(conviction={conviction_score:.1f}, "
                f"holding_pct={holding_pct}, value_pct={value_pct}, "
                f"liquidity_pct={liquidity_pct})"
            ),
            source="rules",
            confidence=None,
            reason_code="rules_high_edge",
            conviction_score=conviction_score,
            conviction_holding_pct=holding_pct,
            conviction_value_pct=value_pct,
            conviction_liquidity_pct=liquidity_pct,
        )

    return AutoDecisionRuleResult(
        decision="escalate",
        reason=(
            "baseline rule: insufficient conviction to approve "
            f"(conviction={conviction_score:.1f}, "
            f"holding_pct={holding_pct}, value_pct={value_pct}, "
            f"liquidity_pct={liquidity_pct})"
        ),
        source="rules",
        confidence=None,
        reason_code="rules_ambiguous",
        conviction_score=conviction_score,
        conviction_holding_pct=holding_pct,
        conviction_value_pct=value_pct,
        conviction_liquidity_pct=liquidity_pct,
    )


def _extract_json_object(text: str) -> dict[str, object] | None:
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    decoder = json.JSONDecoder()
    for idx, char in enumerate(text):
        if char != "{":
            continue
        try:
            candidate, _ = decoder.raw_decode(text[idx:])
        except json.JSONDecodeError:
            continue
        if isinstance(candidate, dict):
            return candidate

    match = _JSON_OBJECT_RE.search(text)
    if match is None:
        return None
    try:
        parsed = json.loads(match.group(0))
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _compact_packet_for_quant(packet: dict[str, object]) -> dict[str, object]:
    payload = packet.get("payload")
    payload_dict = payload if isinstance(payload, dict) else {}
    rationale = payload_dict.get("rationale")
    rationale_dict = rationale if isinstance(rationale, dict) else {}
    return {
        "score": payload_dict.get("score"),
        "net_buy_shares": rationale_dict.get("net_buy_shares"),
        "gross_value": rationale_dict.get("gross_value"),
        "open_market_buy_shares": rationale_dict.get("open_market_buy_shares"),
        "open_market_sell_shares": rationale_dict.get("open_market_sell_shares"),
        "open_market_net_shares": rationale_dict.get("open_market_net_shares"),
        "open_market_gross_value": rationale_dict.get("open_market_gross_value"),
        "holding_change_ratio": rationale_dict.get("holding_change_ratio"),
        "pre_trade_shares_estimate": rationale_dict.get("pre_trade_shares_estimate"),
        "post_trade_shares": rationale_dict.get("post_trade_shares"),
        "trade_pct_daily_volume": rationale_dict.get("trade_pct_daily_volume"),
        "trade_pct_daily_turnover": rationale_dict.get("trade_pct_daily_turnover"),
        "role_tier": rationale_dict.get("role_tier"),
        "regime_earnings_shock_flag": _to_bool(rationale_dict.get("regime_earnings_shock_flag")),
        "owner_is_exec": _to_bool(rationale_dict.get("owner_is_exec")),
        "owner_is_ten_percent_owner": _to_bool(rationale_dict.get("owner_is_ten_percent_owner")),
        "owner_is_entity": _to_bool(rationale_dict.get("owner_is_entity")),
        "has_10b5_1_plan": _to_bool(rationale_dict.get("has_10b5_1_plan")),
        "has_13d_reference": _to_bool(rationale_dict.get("has_13d_reference")),
        "has_equity_comp_event": _to_bool(rationale_dict.get("has_equity_comp_event")),
        "has_tax_withholding_language": _to_bool(
            rationale_dict.get("has_tax_withholding_language")
        ),
        "has_option_exercise": _to_bool(rationale_dict.get("has_option_exercise")),
        "has_award_code": _to_bool(rationale_dict.get("has_award_code")),
        "novelty_penalty": rationale_dict.get("novelty_penalty"),
        "alpha_bonus": rationale_dict.get("alpha_bonus"),
    }


def _economic_event_key(packet: dict[str, object]) -> str | None:
    """Identify the underlying TRADE, independent of which reporting person filed it.

    A group/joint Form 4 reports ONE economic event under N reporting persons. On
    2026-08-10 a single MKZR purchase (33,400 sh, 66,600 -> 100,000, $53,340) arrived as
    three filings by three different owners and produced three separate alerts -- 3 of that
    day's 7 approvals were the same trade. ``_packet_decision_key`` cannot catch this: it is
    keyed on accession, and each co-filer has its own accession.

    Different owners sharing an identical *pre-trade* holding as well as an identical
    purchase is the signature of a jointly-reported position, so pre-trade shares are part
    of the key. Two genuinely independent insiders would have to hold the same position to
    the share for this to collapse a real signal.
    """
    payload = packet.get("payload")
    payload_dict = payload if isinstance(payload, dict) else {}
    symbol_obj = payload_dict.get("issuer_symbol")
    if not isinstance(symbol_obj, str) or not symbol_obj.strip():
        return None
    rationale = payload_dict.get("rationale")
    rationale_dict = rationale if isinstance(rationale, dict) else {}

    parts: list[str] = [symbol_obj.strip().upper()]
    for field in (
        "net_buy_shares",
        "pre_trade_shares_estimate",
        "post_trade_shares",
        "gross_value",
    ):
        value = _to_float(rationale_dict.get(field))
        if value is None:
            # Without the full economic fingerprint we cannot prove two filings are the same
            # trade, so fail open (alert) rather than risk suppressing a distinct signal.
            return None
        parts.append(f"{value:.4f}")
    return "|".join(parts)


def _recent_alerted_event_keys(
    db_path: str,
    *,
    lookback_days: int = 7,
) -> set[str]:
    """Economic-event keys with a confirmed notification delivery in the recent window.

    Group co-filings can land in different autopilot cycles minutes apart, so an in-cycle
    set is not enough -- the MKZR trio arrived across three cycles. The window is bounded so
    a legitimate repeat purchase in the same name weeks later still alerts.
    """
    cutoff = (datetime.now(UTC) - timedelta(days=lookback_days)).isoformat()
    keys: set[str] = set()
    try:
        ensure_review_tables(db_path)
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT packet_id, payload_json
                FROM review_packets
                WHERE notification_sent_at >= ?
                  AND json_extract(decision_json, '$.decision') = 'approve'
                """,
                (cutoff,),
            ).fetchall()
    except sqlite3.Error:
        return keys
    for row in rows:
        try:
            payload = json.loads(row["payload_json"])
        except (TypeError, ValueError):
            continue
        key = _economic_event_key({"packet_id": row["packet_id"], "payload": payload})
        if key is not None:
            keys.add(key)
    return keys


def _packet_decision_key(packet: dict[str, object]) -> str | None:
    packet_id_obj = packet.get("packet_id")
    if isinstance(packet_id_obj, str):
        parts = [part.strip() for part in packet_id_obj.split("|")]
        if len(parts) == 3 and parts[0] and parts[2]:
            return f"{parts[0]}|{parts[2]}"

    accession_obj = packet.get("accession_number")
    form_type_obj = packet.get("form_type")
    if isinstance(accession_obj, str) and isinstance(form_type_obj, str):
        accession = accession_obj.strip()
        form_type = form_type_obj.strip()
        if accession and form_type:
            return f"{accession}|{form_type}"
    return None


QUANT_SYSTEM_PROMPT = (
    "You are a quantitative analyst filtering SEC Form 4 insider filings for alpha-like "
    "signals. You classify only; you never browse, never call tools, and never ask questions.\n"
    "\n"
    "APPROVE only when there is likely non-routine discretionary conviction buying with a "
    "meaningful holdings change and a low novelty penalty. In practice this requires "
    "discretionary open-market buy evidence (transaction code P context).\n"
    "\n"
    "REJECT obvious non-signal flow: planned 10b5-1 activity, passive ten-percent owner "
    "accumulation, compensation grants or vesting, option exercises, and tax-withholding-only "
    "sales. A very small holding-change ratio by a non-executive entity is usually not alpha.\n"
    "\n"
    "Director-only buys should be approved only when liquidity impact is meaningful (a "
    "non-trivial percent of daily turnover/volume). Post-shock (large down-move) regimes "
    "require stronger conviction evidence.\n"
    "\n"
    "When uncertain, choose escalate rather than guessing.\n"
    "\n"
    "Respond with ONLY a single JSON object and no prose, no markdown fences, and no "
    "commentary. Emit exactly one entry per input packet_id, reusing the given packet_id "
    "verbatim."
)


# Local decision CLIs, in preference order. OpenClaw is no longer supported.
def _native_codex_from_shim(shim: Path) -> Path | None:
    """Resolve the native Windows Codex binary behind an npm shim, if installed."""

    if os.name != "nt":
        return None
    package_root = (
        shim.resolve().parent
        / "node_modules"
        / "@openai"
        / "codex"
        / "node_modules"
        / "@openai"
    )
    matches = sorted(package_root.glob("codex-win32-*/vendor/*/bin/codex.exe"))
    return matches[0] if matches else None


def _resolve_quant_cmds() -> list[tuple[str, str]]:
    """Return ordered, real-path-deduplicated local decision backends.

    Native executables are required on Windows. Passing a multiline prompt through an npm
    ``.cmd`` shim can truncate it, and launching a shim from ``pythonw`` can create a console.
    Codex is preferred because the local Claude default can be independently rate-limited; Claude
    remains a failover backend.
    """

    candidates: list[tuple[Path, str]] = []
    codex_names = ("codex.exe", "codex.cmd", "codex")
    for name in codex_names:
        found = shutil.which(name)
        if not found:
            continue
        path = Path(found)
        if os.name == "nt" and path.suffix.lower() != ".exe":
            native = _native_codex_from_shim(path)
            if native is not None:
                candidates.append((native, "codex"))
        else:
            candidates.append((path, "codex"))
    for name in ("claude.exe", "claude", "claude.cmd"):
        found = shutil.which(name)
        if found:
            path = Path(found)
            if os.name != "nt" or path.suffix.lower() == ".exe":
                candidates.append((path, "claude"))
    for candidate, flavor in (
        (Path.home() / ".local" / "bin" / "claude.exe", "claude"),
        (Path.home() / ".local" / "bin" / "claude", "claude"),
        (Path.home() / "AppData" / "Roaming" / "npm" / "codex.cmd", "codex"),
    ):
        if not candidate.exists():
            continue
        if flavor == "codex" and os.name == "nt":
            native = _native_codex_from_shim(candidate)
            if native is not None:
                candidates.append((native, flavor))
        elif os.name != "nt" or candidate.suffix.lower() == ".exe":
            candidates.append((candidate, flavor))

    resolved: list[tuple[str, str]] = []
    seen: set[str] = set()
    for candidate, flavor in candidates:
        key = os.path.normcase(os.path.realpath(candidate))
        if key in seen:
            continue
        seen.add(key)
        resolved.append((str(candidate), flavor))
    return resolved


_DEFAULT_QUANT_MODELS = {
    "codex": "gpt-5.6-sol",
    "claude": "claude-sonnet-5",
}


def _quant_model(flavor: str) -> str:
    return (
        os.environ.get(f"INSIDER_QUANT_{flavor.upper()}_MODEL", "").strip()
        or os.environ.get("INSIDER_QUANT_MODEL", "").strip()
        or _DEFAULT_QUANT_MODELS[flavor]
    )


def _quant_effort(quant_thinking: str) -> str:
    return "low" if quant_thinking in {"off", "minimal"} else quant_thinking


def _build_quant_args(
    cmd: str,
    flavor: str,
    prompt: str,
    *,
    quant_thinking: str,
) -> list[str]:
    """Build the argv for a single-shot, tool-free classification call."""
    model = _quant_model(flavor)
    effort = _quant_effort(quant_thinking)
    if flavor == "claude":
        args = [
            cmd,
            "-p",
            prompt,
            "--output-format",
            "json",
            "--system-prompt",
            QUANT_SYSTEM_PROMPT,
            # Pure reasoning: no tools, single turn. Keeps the call hermetic and fast.
            "--allowedTools",
            "",
            "--max-turns",
            "1",
            "--effort",
            effort,
        ]
        args += ["--model", model]
        return args
    # codex: exec runs a single non-interactive turn and prints the reply on stdout. The native
    # executable receives the prompt directly, avoiding npm-shim multiline truncation on Windows.
    args = [
        cmd,
        "exec",
        "--skip-git-repo-check",
        "--ephemeral",
        "--ignore-user-config",
        "--ignore-rules",
        "--sandbox",
        "read-only",
        "--color",
        "never",
        "-c",
        f'model_reasoning_effort="{effort}"',
        "--model",
        model,
    ]
    args.append(f"{QUANT_SYSTEM_PROMPT}\n\n{prompt}")
    return args


def _extract_quant_response_text(flavor: str, stdout: str) -> tuple[str | None, str | None]:
    """Pull the model's reply text out of a CLI-specific envelope.

    Returns ``(text, error)``; exactly one is non-None.
    """
    if flavor == "claude":
        outer = _extract_json_object(stdout)
        if outer is None:
            return None, "invalid JSON envelope"
        if outer.get("is_error") is True:
            detail = outer.get("result") or outer.get("error") or "unknown"
            return None, f"cli reported error: {str(detail)[:160]}"
        result_obj = outer.get("result")
        if isinstance(result_obj, str) and result_obj.strip():
            return result_obj, None
        # Defensive: some versions nest the reply under result.payloads[].text
        if isinstance(result_obj, dict):
            payloads = result_obj.get("payloads")
            if isinstance(payloads, list) and payloads:
                first = payloads[0]
                if isinstance(first, dict) and isinstance(first.get("text"), str):
                    return str(first["text"]), None
        return None, "missing result text"
    # codex prints the assistant reply directly; the JSON extractor tolerates surrounding prose.
    if stdout and stdout.strip():
        return stdout, None
    return None, "empty response"


def _decide_packets_with_quant(
    packets: list[dict[str, object]],
    *,
    quant_agent_id: str,
    quant_timeout_seconds: int,
    quant_thinking: str,
    quant_batch_size: int,
    progress_callback: Callable[[str], None] | None = None,
) -> tuple[dict[str, AutoDecisionRuleResult], str | None]:
    backends = _resolve_quant_cmds()
    if not backends:
        return {}, "no local quant CLI found (looked for claude, codex)"
    mapped: dict[str, AutoDecisionRuleResult] = {}
    errors: list[str] = []
    batch_size = max(1, quant_batch_size)

    for start in range(0, len(packets), batch_size):
        chunk = packets[start : start + batch_size]
        alias_to_packet_id: dict[str, str] = {}
        compact_packets: list[dict[str, object]] = []
        for offset, packet in enumerate(chunk):
            packet_id_obj = packet.get("packet_id")
            if not isinstance(packet_id_obj, str):
                continue
            alias = f"P{start + offset:05d}"
            alias_to_packet_id[alias] = packet_id_obj
            compact = _compact_packet_for_quant(packet)
            compact["packet_id"] = alias
            compact_packets.append(compact)
        if not compact_packets:
            continue

        compact_by_alias = {str(packet["packet_id"]): packet for packet in compact_packets}
        unresolved = set(alias_to_packet_id)
        for quant_cmd, quant_flavor in backends:
            if not unresolved:
                break
            request = {
                "packets": [
                    compact_by_alias[alias]
                    for alias in alias_to_packet_id
                    if alias in unresolved
                ]
            }
            prompt = (
                "Classify each Form 4 packet below.\n"
                "Return ONLY this JSON shape:\n"
                '{"decisions":[{"packet_id":"...","decision":"approve, reject, or escalate",'
                '"why":"max 240 chars","edge_hypothesis":"...","risk_flags":["..."],'
                '"evidence":{"role_tier":"...","open_market_buy_shares":0,'
                '"trade_pct_daily_turnover":0,"novelty_penalty":0,'
                '"regime_earnings_shock_flag":false},"confidence":0.0}]}\n'
                f"Input: {json.dumps(request, separators=(',', ':'))}"
            )
            args = _build_quant_args(
                quant_cmd,
                quant_flavor,
                prompt,
                quant_thinking=quant_thinking,
            )
            backend_label = f"{quant_flavor}:{Path(quant_cmd).name}"
            if progress_callback is not None:
                progress_callback(f"quant_backend_{start}_started")
            try:
                completed = run_hidden_process(
                    args,
                    cwd=Path.cwd(),
                    timeout_seconds=quant_timeout_seconds + 10,
                )
            except (OSError, UnicodeError) as exc:
                errors.append(
                    f"chunk[{start}:{start + len(chunk)}] {backend_label} failed: {exc}"
                )
                continue
            finally:
                if progress_callback is not None:
                    progress_callback(f"quant_backend_{start}_finished")

            if completed.timed_out:
                errors.append(
                    f"chunk[{start}:{start + len(chunk)}] {backend_label} failed: "
                    f"timed out after {quant_timeout_seconds + 10} seconds"
                )
                continue
            stdout = completed.stdout if isinstance(completed.stdout, str) else ""
            stderr = completed.stderr if isinstance(completed.stderr, str) else ""
            if completed.returncode != 0:
                _ignored, envelope_error = _extract_quant_response_text(
                    quant_flavor, stdout
                )
                detail = (
                    envelope_error
                    or stderr.strip()
                    or stdout.strip()[:160]
                    or "unknown error"
                )
                errors.append(
                    f"chunk[{start}:{start + len(chunk)}] {backend_label} non-zero: {detail}"
                )
                continue

            text_obj, envelope_error = _extract_quant_response_text(
                quant_flavor, stdout
            )
            if text_obj is None:
                errors.append(
                    f"chunk[{start}:{start + len(chunk)}] {backend_label} {envelope_error}"
                )
                continue

            inner = _extract_json_object(text_obj)
            if inner is None:
                errors.append(
                    f"chunk[{start}:{start + len(chunk)}] {backend_label} "
                    "invalid decision JSON"
                )
                continue

            decisions_obj = inner.get("decisions")
            if not isinstance(decisions_obj, list):
                errors.append(
                    f"chunk[{start}:{start + len(chunk)}] {backend_label} decisions missing"
                )
                continue

            resolved_here = 0
            for entry in decisions_obj:
                if not isinstance(entry, dict):
                    continue
                packet_id_obj = entry.get("packet_id")
                decision_obj = entry.get("decision")
                why_obj = entry.get("why")
                edge_hypothesis_obj = entry.get("edge_hypothesis")
                risk_flags_obj = entry.get("risk_flags")
                evidence_obj = entry.get("evidence")
                if (
                    not isinstance(packet_id_obj, str)
                    or packet_id_obj not in unresolved
                ):
                    continue
                original_packet_id = alias_to_packet_id.get(packet_id_obj)
                if original_packet_id is None:
                    continue
                if not isinstance(decision_obj, str) or decision_obj not in {
                    "approve",
                    "reject",
                    "escalate",
                }:
                    continue
                if not isinstance(why_obj, str) or not why_obj.strip():
                    continue
                if (
                    not isinstance(edge_hypothesis_obj, str)
                    or not edge_hypothesis_obj.strip()
                ):
                    continue
                if not isinstance(risk_flags_obj, list) or any(
                    not isinstance(flag, str) for flag in risk_flags_obj
                ):
                    continue
                if not isinstance(evidence_obj, dict):
                    continue
                required_evidence_keys = {
                    "role_tier",
                    "open_market_buy_shares",
                    "trade_pct_daily_turnover",
                    "novelty_penalty",
                    "regime_earnings_shock_flag",
                }
                if not required_evidence_keys.issubset(evidence_obj.keys()):
                    continue

                confidence = _to_float(entry.get("confidence"))
                if confidence is not None:
                    confidence = max(0.0, min(1.0, confidence))

                reason_code = "quant_decision"
                if decision_obj == "approve" and (
                    confidence is not None and confidence >= 0.85
                ):
                    reason_code = "quant_high_edge"
                elif decision_obj == "reject":
                    reason_code = "reject_low_edge"
                elif decision_obj == "escalate":
                    reason_code = "quant_escalate"

                mapped[original_packet_id] = AutoDecisionRuleResult(
                    decision=decision_obj,
                    reason=f"{why_obj.strip()} Edge: {edge_hypothesis_obj.strip()}"[:240],
                    source=(
                        f"quant:{quant_agent_id}:{quant_flavor}:"
                        f"{_quant_model(quant_flavor)}:{_quant_effort(quant_thinking)}"
                    ),
                    confidence=confidence,
                    reason_code=reason_code,
                )
                unresolved.remove(packet_id_obj)
                resolved_here += 1

            if unresolved:
                errors.append(
                    f"chunk[{start}:{start + len(chunk)}] {backend_label} "
                    f"invalid decision schema or missing decisions for {len(unresolved)} "
                    "packet(s)"
                )
            elif resolved_here == 0:
                errors.append(
                    f"chunk[{start}:{start + len(chunk)}] {backend_label} "
                    "invalid decision schema"
                )

    if not errors:
        return mapped, None
    if len(errors) == 1:
        return mapped, errors[0]
    return mapped, f"{errors[0]}; +{len(errors) - 1} more chunk errors"


def _apply_approve_guardrails(
    rule: AutoDecisionRuleResult,
    packet: dict[str, object],
    *,
    approve_score_min: float,
    approve_net_buy_shares_min: float,
    quant_min_confidence: float,
) -> AutoDecisionRuleResult:
    if rule.decision != "approve":
        return rule

    payload = packet.get("payload")
    payload_dict = payload if isinstance(payload, dict) else {}
    rationale = payload_dict.get("rationale")
    rationale_dict = rationale if isinstance(rationale, dict) else {}

    score = _to_float(payload_dict.get("score"))
    net_buy_shares = _to_float(rationale_dict.get("net_buy_shares"))
    open_market_buy_shares = _to_float(rationale_dict.get("open_market_buy_shares"))
    trade_pct_daily_turnover = _to_float(rationale_dict.get("trade_pct_daily_turnover"))
    regime_earnings_shock_flag = _to_bool(rationale_dict.get("regime_earnings_shock_flag"))
    role_tier_obj = rationale_dict.get("role_tier")
    role_tier = str(role_tier_obj).strip().lower() if isinstance(role_tier_obj, str) else ""
    has_10b5_1_plan = _to_bool(rationale_dict.get("has_10b5_1_plan"))
    has_equity_comp_event = _to_bool(rationale_dict.get("has_equity_comp_event"))
    has_tax_withholding_language = _to_bool(rationale_dict.get("has_tax_withholding_language"))
    owner_is_ten_percent_owner = _to_bool(rationale_dict.get("owner_is_ten_percent_owner"))
    owner_is_exec = _to_bool(rationale_dict.get("owner_is_exec"))
    packet_id = str(packet.get("packet_id", "unknown"))

    if score is None or net_buy_shares is None:
        return AutoDecisionRuleResult(
            decision="escalate",
            reason=f"safety block: packet={packet_id} missing score/net_buy_shares for approve",
            source="safety",
            confidence=None,
            reason_code="safety_missing_core_features",
        )

    if open_market_buy_shares is None or open_market_buy_shares <= 0:
        return AutoDecisionRuleResult(
            decision="escalate",
            reason=(
                "safety block: "
                f"packet={packet_id} approve requires discretionary open-market buying"
            ),
            source="safety",
            confidence=None,
            reason_code="safety_no_open_market_buy",
        )

    if has_10b5_1_plan:
        return AutoDecisionRuleResult(
            decision="escalate",
            reason=f"safety block: packet={packet_id} flagged 10b5-1/planned flow",
            source="safety",
            confidence=None,
            reason_code="safety_planned_flow",
        )

    if has_equity_comp_event and has_tax_withholding_language:
        return AutoDecisionRuleResult(
            decision="escalate",
            reason=(
                f"safety block: packet={packet_id} appears compensation/tax-withholding driven"
            ),
            source="safety",
            confidence=None,
            reason_code="safety_comp_tax_flow",
        )

    if owner_is_ten_percent_owner and not owner_is_exec:
        return AutoDecisionRuleResult(
            decision="escalate",
            reason=f"safety block: packet={packet_id} passive ten-percent owner flow",
            source="safety",
            confidence=None,
            reason_code="safety_passive_owner",
        )

    if score < approve_score_min or net_buy_shares <= approve_net_buy_shares_min:
        return AutoDecisionRuleResult(
            decision="escalate",
            reason=(
                "safety block: "
                f"score={score:.2f}, net_buy_shares={net_buy_shares:.2f} "
                f"(requires score >= {approve_score_min:.2f} and "
                f"net_buy_shares > {approve_net_buy_shares_min:.2f})"
            ),
            source="safety",
            confidence=None,
            reason_code="safety_threshold_block",
        )

    if role_tier == "director" and (
        trade_pct_daily_turnover is None or trade_pct_daily_turnover < 0.1
    ):
        reason_code = (
            "missing_market_context"
            if trade_pct_daily_turnover is None
            else "safety_low_edge_director"
        )
        return AutoDecisionRuleResult(
            decision="escalate",
            reason=(
                "safety block: "
                f"director signal has low liquidity impact "
                f"(trade_pct_daily_turnover={trade_pct_daily_turnover})"
            ),
            source="safety",
            confidence=None,
            reason_code=reason_code,
        )

    if regime_earnings_shock_flag and (
        trade_pct_daily_turnover is None or trade_pct_daily_turnover < 0.25
    ):
        reason_code = (
            "missing_market_context"
            if trade_pct_daily_turnover is None
            else "safety_shock_regime_block"
        )
        return AutoDecisionRuleResult(
            decision="escalate",
            reason=(
                "safety block: "
                "post-shock regime requires stronger liquidity conviction "
                f"(trade_pct_daily_turnover={trade_pct_daily_turnover})"
            ),
            source="safety",
            confidence=None,
            reason_code=reason_code,
        )

    if rule.source.startswith("quant:") and (
        rule.confidence is None or rule.confidence < quant_min_confidence
    ):
        confidence_text = "missing" if rule.confidence is None else f"{rule.confidence:.2f}"
        return AutoDecisionRuleResult(
            decision="escalate",
            reason=(
                f"safety block: quant confidence={confidence_text} below {quant_min_confidence:.2f}"
            ),
            source="safety",
            confidence=None,
            reason_code="safety_low_quant_confidence",
        )

    return rule


def _build_trade_signal_notification(
    packet: dict[str, object],
    decision_payload: dict[str, str],
) -> tuple[str, str, list[str], int]:
    payload = packet.get("payload")
    payload_dict = payload if isinstance(payload, dict) else {}
    rationale = payload_dict.get("rationale")
    rationale_dict = rationale if isinstance(rationale, dict) else {}

    ticker = str(payload_dict.get("issuer_symbol") or "UNKNOWN")
    owner = str(payload_dict.get("owner") or "UNKNOWN")
    score = _to_float(payload_dict.get("score"))
    net_buy = _to_float(rationale_dict.get("net_buy_shares"))
    open_market_buy = _to_float(rationale_dict.get("open_market_buy_shares"))
    gross = _to_float(rationale_dict.get("gross_value"))
    novelty_penalty = _to_float(rationale_dict.get("novelty_penalty"))
    conviction_score = _to_float(decision_payload.get("conviction_score"))
    conviction_holding_pct = _to_float(decision_payload.get("conviction_holding_pct"))
    conviction_value_pct = _to_float(decision_payload.get("conviction_value_pct"))
    conviction_liquidity_pct = _to_float(decision_payload.get("conviction_liquidity_pct"))
    role_tier_obj = rationale_dict.get("role_tier")
    role_tier = str(role_tier_obj) if isinstance(role_tier_obj, str) else "unknown"
    trade_pct_daily_turnover = _to_float(rationale_dict.get("trade_pct_daily_turnover"))
    trade_pct_daily_volume = _to_float(rationale_dict.get("trade_pct_daily_volume"))
    regime_shock = _to_bool(rationale_dict.get("regime_earnings_shock_flag"))
    has_10b5 = _to_bool(rationale_dict.get("has_10b5_1_plan"))
    has_comp_event = _to_bool(rationale_dict.get("has_equity_comp_event"))
    packet_id = str(packet.get("packet_id") or decision_payload["packet_id"])
    why = decision_payload.get("reason", "").strip()
    source = decision_payload.get("decision_source", decision_payload.get("analyst", "quant"))

    title = f"TRADE SIGNAL: {ticker}"
    message = "\n".join(
        [
            f"ticker={ticker}",
            f"packet={packet_id}",
            f"owner={owner}",
            f"score={score:.2f}" if score is not None else "score=NA",
            f"net_buy_shares={net_buy:.2f}" if net_buy is not None else "net_buy_shares=NA",
            (
                f"open_market_buy_shares={open_market_buy:.2f}"
                if open_market_buy is not None
                else "open_market_buy_shares=NA"
            ),
            f"gross_value={gross:.2f}" if gross is not None else "gross_value=NA",
            (
                f"novelty_penalty={novelty_penalty:.2f}"
                if novelty_penalty is not None
                else "novelty_penalty=NA"
            ),
            (
                f"conviction_score={conviction_score:.1f}"
                if conviction_score is not None
                else "conviction_score=NA"
            ),
            (
                f"conviction_holding_pct={conviction_holding_pct:.1f}"
                if conviction_holding_pct is not None
                else "conviction_holding_pct=NA"
            ),
            (
                f"conviction_value_pct={conviction_value_pct:.1f}"
                if conviction_value_pct is not None
                else "conviction_value_pct=NA"
            ),
            (
                f"conviction_liquidity_pct={conviction_liquidity_pct:.1f}"
                if conviction_liquidity_pct is not None
                else "conviction_liquidity_pct=NA"
            ),
            f"role_tier={role_tier}",
            (
                f"trade_pct_daily_turnover={trade_pct_daily_turnover:.4f}"
                if trade_pct_daily_turnover is not None
                else "trade_pct_daily_turnover=NA"
            ),
            (
                f"trade_pct_daily_volume={trade_pct_daily_volume:.4f}"
                if trade_pct_daily_volume is not None
                else "trade_pct_daily_volume=NA"
            ),
            f"regime_earnings_shock_flag={str(regime_shock).lower()}",
            f"has_10b5_1_plan={str(has_10b5).lower()}",
            f"has_equity_comp_event={str(has_comp_event).lower()}",
            f"source={source}",
            f"why={why or 'N/A'}",
        ]
    )
    tags = ["trade-signal", "insider-alerts", ticker.lower().replace(" ", "-")]
    return title, message, tags, 4


def _send_review_notification(
    settings: Settings,
    payload: dict[str, str],
    *,
    packet: dict[str, object] | None = None,
    dry_message: str | None = None,
) -> None:
    notifier = NtfyNotifier(settings)
    observer, observer_error_handler = _notification_transport_observer(settings, payload)
    decision = payload.get("decision", "")
    if decision == "approve" and packet is not None:
        title, message, tags, priority = _build_trade_signal_notification(packet, payload)
        notifier.send(
            title=title,
            message=message,
            tags=tags,
            priority=priority,
            markdown=True,
            observer=observer,
            observer_error_handler=observer_error_handler,
        )
        return

    message = f"packet={payload['packet_id']} decision={decision} analyst={payload['analyst']}"
    if dry_message:
        message = f"{message} note={dry_message}"
    notifier.send(
        title="Insider Review Applied",
        message=message,
        tags=["insider-alerts", "review"],
        priority=3,
        markdown=False,
        observer=observer,
        observer_error_handler=observer_error_handler,
    )


def _notification_transport_database(settings: Settings) -> Path:
    repo_root = Path(__file__).resolve().parents[2]
    database = Path(settings.notification_transport_db)
    if not database.is_absolute():
        database = repo_root / database
    return database


@lru_cache(maxsize=8)
def _notification_runtime_git_commit(repo_root: Path) -> str:
    """Bind events to the source loaded by this process without per-alert subprocesses."""

    return resolve_git_commit(repo_root, timeout_seconds=1)


def _notification_transport_config(settings: Settings) -> NotificationJournalConfig:
    repo_root = Path(__file__).resolve().parents[2]
    database = _notification_transport_database(settings)
    policy = Path(settings.notification_transport_policy_path)
    if not policy.is_absolute():
        policy = repo_root / policy
    return NotificationJournalConfig(
        database=database,
        research_root=repo_root / "data" / "research",
        policy_path=policy,
        policy_root=repo_root / "docs" / "research" / "contracts",
        runtime_git_commit=_notification_runtime_git_commit(repo_root),
    )


def _notification_transport_observer(
    settings: Settings, payload: dict[str, str]
) -> tuple[
    Callable[[NtfyTransportEvent], None] | None,
    Callable[[Exception], None] | None,
]:
    error_log = Path(__file__).resolve().parents[2] / "logs" / "notification-transport.err.log"

    def capture_error(exc: Exception) -> None:
        with contextlib.suppress(OSError):
            error_log.parent.mkdir(parents=True, exist_ok=True)
            with error_log.open("a", encoding="utf-8") as handle:
                handle.write(
                    f"{datetime.now(UTC).isoformat()} notification transport capture "
                    f"isolated: {type(exc).__name__}\n"
                )

    packet_id = payload.get("packet_id", "").strip()
    if not packet_id:
        return None, None
    try:
        if not _notification_transport_database(settings).is_file():
            return None, None
        config = _notification_transport_config(settings)
        journal = NotificationTransportJournal(config)
        transport_id = notification_transport_id(packet_id, secrets.token_hex(32))
    except ProcessTreeCleanupError:
        raise
    except Exception as exc:
        capture_error(exc)
        return None, capture_error

    def observe(event: NtfyTransportEvent) -> None:
        journal.append(packet_id=packet_id, transport_id=transport_id, event=event)

    return observe, capture_error


@notify_app.command("test")
def notify_test() -> None:
    """Send a test notification via NTFY."""
    settings = get_settings()
    notifier = NtfyNotifier(settings)

    try:
        notifier.send(
            title="Insider Alerts Test",
            message="Test notification from insider-alerts CLI.",
            tags=["test", "insider-alerts"],
            priority=3,
            markdown=True,
        )
    except NtfyNotificationError as exc:
        typer.secho(f"Notification failed: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from exc

    typer.secho("Notification sent.", fg=typer.colors.GREEN)


@sec_app.command("poll")
def sec_poll(
    once: bool = typer.Option(
        True,
        "--once/--loop",
        help="Run a single poll cycle or keep polling.",
    ),
    interval: int = typer.Option(
        600,
        "--interval",
        min=1,
        help="Seconds between polls when looping.",
    ),
    max_items: int = typer.Option(40, "--max-items", min=1, max=200, help="Max parsed items."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Parse only, no DB writes."),
) -> None:
    """Poll SEC Form 4 RSS and persist new filing references."""
    settings = get_settings()

    def _run_once() -> None:
        result = run_sec_poll_once(settings, max_items=max_items, dry_run=dry_run)
        summary = (
            "sec poll completed "
            f"(fetched={result.fetched}, "
            f"inserted={result.inserted}, "
            f"skipped_existing={result.skipped_existing}, "
            f"source_items_seen={result.source_items_seen}, "
            f"source_boundary_rejected={result.source_boundary_rejected}, "
            f"source_invalid_items={result.source_invalid_items}, "
            f"dry_run={dry_run})"
        )
        typer.echo(summary)

    _run_once()
    if not once:
        while True:
            time.sleep(interval)
            _run_once()


@sec_app.command("enrich")
def sec_enrich(
    limit: int = typer.Option(40, "--limit", min=1, max=500, help="Max filings to enrich."),
) -> None:
    """Fetch filing index pages and store discovered Form 4 XML URLs."""
    settings = get_settings()
    result = enrich_filings_with_xml_url(settings, limit=limit)
    typer.echo(f"sec enrich completed (scanned={result.scanned}, updated={result.updated})")


@sec_app.command("backfill")
def sec_backfill(
    start_date_text: str = typer.Option(..., "--start-date", help="Inclusive YYYY-MM-DD start."),
    end_date_text: str = typer.Option(..., "--end-date", help="Inclusive YYYY-MM-DD end."),
) -> None:
    """Backfill historical Form 4 filing references from SEC master index files."""
    settings = get_settings()
    try:
        start_date = date.fromisoformat(start_date_text)
        end_date = date.fromisoformat(end_date_text)
    except ValueError as exc:
        typer.secho(
            "invalid date format; expected YYYY-MM-DD",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2) from exc

    if start_date > end_date:
        typer.secho("start-date cannot be after end-date", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2)

    result = backfill_form4_filings(
        settings,
        start_date=start_date,
        end_date=end_date,
    )
    typer.echo(
        "sec backfill completed "
        f"(requested_quarters={result.requested_quarters}, "
        f"fetched_quarters={result.fetched_quarters}, "
        f"matched_filings={result.matched_filings}, "
        f"inserted={result.inserted}, "
        f"skipped_existing={result.skipped_existing})"
    )


@review_app.command("enqueue")
def review_enqueue(
    limit: int = typer.Option(50, "--limit", min=1, max=1000, help="Max filings to process."),
    oldest_first: bool = typer.Option(
        False,
        "--oldest-first/--newest-first",
        help="Process oldest filings first (useful for historical backfills).",
    ),
    start_date_text: str = typer.Option(
        "",
        "--start-date",
        help="Optional inclusive filing-date lower bound (YYYY-MM-DD).",
    ),
    end_date_text: str = typer.Option(
        "",
        "--end-date",
        help="Optional inclusive filing-date upper bound (YYYY-MM-DD).",
    ),
) -> None:
    """Build scored review packets from filings that have Form 4 XML URLs."""
    settings = get_settings()
    start_supplied = bool(start_date_text.strip())
    end_supplied = bool(end_date_text.strip())
    if start_supplied != end_supplied:
        typer.secho(
            "must provide both --start-date and --end-date, or neither",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2)
    start_date: date | None = None
    end_date: date | None = None
    if start_supplied and end_supplied:
        try:
            start_date = date.fromisoformat(start_date_text)
            end_date = date.fromisoformat(end_date_text)
        except ValueError as exc:
            typer.secho(
                "invalid date format; expected YYYY-MM-DD",
                fg=typer.colors.RED,
                err=True,
            )
            raise typer.Exit(code=2) from exc
        if start_date > end_date:
            typer.secho("start-date cannot be after end-date", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=2)

    result = enqueue_review_packets(
        settings,
        limit=limit,
        oldest_first=oldest_first,
        start_date=start_date,
        end_date=end_date,
    )
    typer.echo(
        "review enqueue completed "
        f"(processed={result.processed}, enqueued={result.enqueued}, "
        f"skipped_existing={result.skipped_existing}, "
        f"http_failed={result.http_failed}, parse_failed={result.parse_failed})"
    )


@review_app.command("pending")
def review_pending(
    limit: int = typer.Option(50, "--limit", min=1, max=1000, help="Max packets to list."),
) -> None:
    """List pending review packets in JSON for analyst/agent decisioning."""
    settings = get_settings()
    rows = list_pending_review_packets(settings.database_path, limit=limit)
    typer.echo(json.dumps(rows, indent=2, sort_keys=True))


@review_app.command("decide")
def review_decide(
    packet_id: str = typer.Option(..., "--packet-id"),
    decision: str = typer.Option(..., "--decision", help="approve|reject|escalate|deadletter"),
    reason: str = typer.Option(..., "--reason"),
    analyst: str = typer.Option("quant", "--analyst"),
    notify: bool = typer.Option(False, "--notify", help="Send NTFY notification when applied."),
) -> None:
    """Apply a single decision directly (automation-friendly, no decision-file needed)."""
    settings = get_settings()
    payload: dict[str, object] = {
        "packet_id": packet_id,
        "decision": decision,
        "analyst": analyst,
        "reason": reason,
        "decision_source": analyst,
    }
    packet = get_review_packet(settings.database_path, packet_id)

    try:
        updated = apply_decision(
            settings.database_path,
            payload,
            notification_required=notify,
        )
    except DecisionValidationError as exc:
        typer.secho(f"decision validation failed: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc

    if updated != 1:
        typer.secho(
            "review decide failed: packet not found or not pending",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=3)

    typer.echo(f"review decide completed (updated={updated})")
    if notify:
        notify_payload = {k: str(v) for k, v in payload.items()}
        _send_review_notification(settings, notify_payload, packet=packet)
        mark_notification_delivered(settings.database_path, packet_id, payload)


@review_app.command("apply")
def review_apply(
    decision_file: Path = typer.Option(  # noqa: B008
        ..., "--decision-file", exists=True, readable=True
    ),
    notify: bool = typer.Option(False, "--notify", help="Send NTFY notification when applied."),
) -> None:
    """Apply review decision JSON payload to pending queue packet."""
    settings = get_settings()
    try:
        payload = json.loads(decision_file.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        typer.secho(
            f"decision validation failed: invalid JSON ({exc})",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2) from exc

    try:
        packet_id_obj = payload.get("packet_id")
        packet = (
            get_review_packet(settings.database_path, packet_id_obj)
            if isinstance(packet_id_obj, str)
            else None
        )
        updated = apply_decision(
            settings.database_path,
            payload,
            notification_required=notify,
        )
    except DecisionValidationError as exc:
        typer.secho(f"decision validation failed: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc

    if updated != 1:
        typer.secho(
            "review apply failed: packet not found or not pending",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=3)

    typer.echo(f"review apply completed (updated={updated})")
    if notify:
        notify_payload = {k: str(v) for k, v in payload.items() if isinstance(k, str)}
        _send_review_notification(settings, notify_payload, packet=packet)
        if isinstance(packet_id_obj, str):
            mark_notification_delivered(settings.database_path, packet_id_obj, payload)


@ops_app.command("deadletter-list")
def deadletter_list() -> None:
    """List deadletter records for failed packets."""
    settings = get_settings()
    rows = list_deadletters(settings.database_path)
    typer.echo(json.dumps(rows, indent=2, sort_keys=True))


@ops_app.command("deadletter-replay")
def deadletter_replay(packet_id: str = typer.Option(..., "--packet-id")) -> None:
    """Replay a deadletter packet by resetting its status to pending."""
    settings = get_settings()
    updated = replay_deadletter(settings.database_path, packet_id)
    typer.echo(f"deadletter replay completed (updated={updated})")


@ops_app.command("backtest")
def ops_backtest(
    start_date_text: str = typer.Option(
        "",
        "--start-date",
        help="Inclusive YYYY-MM-DD start. Must be provided with --end-date.",
    ),
    end_date_text: str = typer.Option(
        "",
        "--end-date",
        help="Inclusive YYYY-MM-DD end. Must be provided with --start-date.",
    ),
    min_score_grid_text: str = typer.Option(
        "70,80,90",
        "--min-score-grid",
        help="Comma-separated score thresholds.",
    ),
    hold_days_grid_text: str = typer.Option(
        "3,5,10,20",
        "--hold-days-grid",
        help="Comma-separated max hold days (trading days).",
    ),
    stop_loss_grid_text: str = typer.Option(
        "0.03,0.05",
        "--stop-loss-grid",
        help="Comma-separated stop-loss fractions (0.03=3%).",
    ),
    take_profit_rr_grid_text: str = typer.Option(
        "1.5,2.0,3.0",
        "--take-profit-rr-grid",
        help="Comma-separated take-profit multiples of stop.",
    ),
    benchmark_symbol: str = typer.Option("SPY", "--benchmark-symbol"),
    transaction_cost_bps: float = typer.Option(
        5.0,
        "--transaction-cost-bps",
        min=0.0,
        help="One-way transaction cost in basis points.",
    ),
    slippage_bps: float = typer.Option(
        5.0,
        "--slippage-bps",
        min=0.0,
        help="One-way slippage in basis points.",
    ),
    train_window_days: int = typer.Option(
        365,
        "--train-window-days",
        min=60,
        help="Walk-forward training window in calendar days.",
    ),
    test_window_days: int = typer.Option(
        90,
        "--test-window-days",
        min=20,
        help="Walk-forward test window in calendar days.",
    ),
    min_train_trades: int = typer.Option(
        15,
        "--min-train-trades",
        min=1,
        help="Minimum training trades per fold to select params.",
    ),
    max_signals: int = typer.Option(
        0,
        "--max-signals",
        min=0,
        help="Optional cap for debug runs (0=all).",
    ),
    refresh_prices_enabled: bool = typer.Option(
        True,
        "--refresh-prices/--no-refresh-prices",
        help="Request missing/stale symbol price history from data provider.",
    ),
    output_json_path: Path | None = typer.Option(  # noqa: B008
        None,
        "--output-json",
        help="Optional file path to write JSON report.",
    ),
) -> None:
    """
    Backtest pre-LLM score-driven insider signals with walk-forward validation.
    """
    settings = get_settings()

    start_supplied = bool(start_date_text.strip())
    end_supplied = bool(end_date_text.strip())
    if start_supplied != end_supplied:
        typer.secho(
            "must provide both --start-date and --end-date, or neither",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2)

    if start_supplied and end_supplied:
        try:
            start_date = date.fromisoformat(start_date_text)
            end_date = date.fromisoformat(end_date_text)
        except ValueError as exc:
            typer.secho(
                "invalid date format; expected YYYY-MM-DD",
                fg=typer.colors.RED,
                err=True,
            )
            raise typer.Exit(code=2) from exc
        date_window_mode = "explicit"
    else:
        end_date = date.today()
        start_date = end_date - timedelta(days=365)
        date_window_mode = "default_last_year"

    if start_date > end_date:
        typer.secho("start-date cannot be after end-date", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2)

    bootstrap_refresh: dict[str, int] | None = None
    signals = load_scored_signals(settings.database_path, start_date=start_date, end_date=end_date)
    filing_min_date, filing_max_date = get_filing_date_bounds(settings.database_path)
    effective_coverage_end = min(end_date, date.today())
    coverage_gap_detected = (
        filing_min_date is None
        or filing_max_date is None
        or filing_min_date > start_date
        or filing_max_date < effective_coverage_end
    )
    should_bootstrap = coverage_gap_detected or not signals
    if should_bootstrap:
        try:
            backfill_result: BackfillResult = backfill_form4_filings(
                settings,
                start_date=start_date,
                end_date=end_date,
            )

            bootstrap_batch_limit = 1000

            enrich_scanned_total = 0
            enrich_updated_total = 0
            enrich_batches = 0
            while True:
                enrich_result = enrich_filings_with_xml_url(settings, limit=bootstrap_batch_limit)
                enrich_batches += 1
                enrich_scanned_total += enrich_result.scanned
                enrich_updated_total += enrich_result.updated
                if enrich_result.scanned < bootstrap_batch_limit:
                    break
                if enrich_result.updated == 0:
                    break

            enqueue_processed_total = 0
            enqueue_enqueued_total = 0
            enqueue_skipped_existing_total = 0
            enqueue_http_failed_total = 0
            enqueue_parse_failed_total = 0
            enqueue_batches = 0
            enqueue_stalled_batches = 0
            while True:
                try:
                    enqueue_result = enqueue_review_packets(
                        settings,
                        limit=bootstrap_batch_limit,
                        oldest_first=True,
                        start_date=start_date,
                        end_date=end_date,
                    )
                except TypeError:
                    # Backward-compatibility for monkeypatched test doubles.
                    enqueue_result = enqueue_review_packets(settings, limit=bootstrap_batch_limit)
                enqueue_batches += 1
                enqueue_processed_total += enqueue_result.processed
                enqueue_enqueued_total += enqueue_result.enqueued
                enqueue_skipped_existing_total += enqueue_result.skipped_existing
                enqueue_http_failed_total += enqueue_result.http_failed
                enqueue_parse_failed_total += enqueue_result.parse_failed
                if enqueue_result.processed == 0:
                    break
                if enqueue_result.processed < bootstrap_batch_limit:
                    break
                if enqueue_result.enqueued == 0:
                    enqueue_stalled_batches += 1
                else:
                    enqueue_stalled_batches = 0
                if enqueue_stalled_batches >= 3:
                    typer.secho(
                        "warning: enqueue made no progress for 3 consecutive batches; "
                        "continuing with currently available signal packets",
                        fg=typer.colors.YELLOW,
                        err=True,
                    )
                    break

            bootstrap_refresh = {
                "coverage_gap_detected": int(coverage_gap_detected),
                "backfill_requested_quarters": backfill_result.requested_quarters,
                "backfill_fetched_quarters": backfill_result.fetched_quarters,
                "backfill_matched_filings": backfill_result.matched_filings,
                "backfill_inserted": backfill_result.inserted,
                "backfill_skipped_existing": backfill_result.skipped_existing,
                "enrich_batches": enrich_batches,
                "enrich_scanned": enrich_scanned_total,
                "enrich_updated": enrich_updated_total,
                "enqueue_batches": enqueue_batches,
                "enqueue_processed": enqueue_processed_total,
                "enqueue_enqueued": enqueue_enqueued_total,
                "enqueue_skipped_existing": enqueue_skipped_existing_total,
                "enqueue_http_failed": enqueue_http_failed_total,
                "enqueue_parse_failed": enqueue_parse_failed_total,
            }
            signals = load_scored_signals(
                settings.database_path,
                start_date=start_date,
                end_date=end_date,
            )
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower():
                raise
            typer.secho(
                "warning: sqlite database is locked during bootstrap refresh; "
                "continuing with currently available signals",
                fg=typer.colors.YELLOW,
                err=True,
            )
        except SecHttpError as exc:
            typer.secho(
                f"warning: unable to refresh missing signal data before backtest: {exc}",
                fg=typer.colors.YELLOW,
                err=True,
            )
    if max_signals > 0:
        signals = signals[:max_signals]
    if not signals:
        typer.secho(
            "no signals found for requested window; ingest more filings before backtesting",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=3)

    min_score_grid = _parse_float_grid(min_score_grid_text, min_value=0.0)
    hold_days_grid = _parse_int_grid(hold_days_grid_text, min_value=1)
    stop_loss_grid = _parse_float_grid(stop_loss_grid_text, min_value=0.0)
    take_profit_rr_grid = _parse_float_grid(take_profit_rr_grid_text, min_value=0.0)

    parameter_grid: list[BacktestParams] = []
    for min_score in min_score_grid:
        for hold_days in hold_days_grid:
            for stop_loss_pct in stop_loss_grid:
                for take_profit_rr in take_profit_rr_grid:
                    parameter_grid.append(
                        BacktestParams(
                            min_score=min_score,
                            hold_days=hold_days,
                            stop_loss_pct=stop_loss_pct,
                            take_profit_rr=take_profit_rr,
                        )
                    )

    min_grid_score = min(param.min_score for param in parameter_grid)
    price_candidate_signals = [
        signal
        for signal in signals
        if signal.score >= min_grid_score
        and signal.open_market_buy_shares > 0
        and signal.open_market_net_shares > 0
        and not signal.has_10b5_1_plan
        and not (signal.has_equity_comp_event and signal.has_tax_withholding_language)
    ]
    unique_symbols = sorted({signal.symbol for signal in price_candidate_signals})
    benchmark = benchmark_symbol.strip().upper()
    if benchmark:
        unique_symbols.append(benchmark)
    unique_symbols = sorted(set(unique_symbols))

    effective_start = min(signal.filed_at.date() for signal in signals)
    effective_end = max(signal.filed_at.date() for signal in signals)
    max_hold_days = max(param.hold_days for param in parameter_grid)
    price_start = effective_start - timedelta(days=10)
    price_end = effective_end + timedelta(days=max_hold_days + 10)

    price_client = StooqPriceClient(
        user_agent=settings.sec_user_agent,
        timeout_seconds=settings.market_data_timeout_seconds,
        rate_limit_per_second=settings.market_data_rate_limit_per_second,
        retry_attempts=settings.market_data_retry_attempts,
        retry_min_seconds=settings.market_data_retry_min_seconds,
        retry_max_seconds=settings.market_data_retry_max_seconds,
        prefer_yahoo=True,
    )
    bars_by_symbol: dict[str, list[DailyBar]] = {}
    price_errors: list[str] = []
    for symbol in unique_symbols:
        fetch_error: str | None = None
        cache_start, cache_end = get_price_bar_bounds(settings.database_path, symbol=symbol)
        needs_refresh = refresh_prices_enabled and (
            cache_start is None
            or cache_end is None
            or cache_start > price_start
            or cache_end < price_end
        )
        try:
            if needs_refresh:
                fetched = price_client.fetch_history(symbol)
                refresh_price_bars(settings.database_path, symbol=symbol, bars=fetched)
        except PriceDataError as exc:
            fetch_error = f"{symbol}: {exc}"
        except Exception as exc:  # Defensive: keep one bad symbol from aborting entire run.
            fetch_error = f"{symbol}: unexpected price refresh error: {exc}"

        bars = get_price_bars(
            settings.database_path,
            symbol=symbol,
            start_date=price_start,
            end_date=price_end,
        )
        if bars:
            bars_by_symbol[symbol] = bars
        elif fetch_error is None:
            if needs_refresh:
                price_errors.append(f"{symbol}: no valid price bars after refresh")
            else:
                price_errors.append(f"{symbol}: no cached bars in requested range")
        if fetch_error is not None:
            price_errors.append(fetch_error)

    if not bars_by_symbol:
        typer.secho("no price bars available for backtest", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=4)

    grid_results = evaluate_parameter_grid(
        signals,
        bars_by_symbol=bars_by_symbol,
        parameter_grid=parameter_grid,
        benchmark_symbol=benchmark,
        transaction_cost_bps=transaction_cost_bps,
        slippage_bps=slippage_bps,
    )
    best_grid = grid_results[0]
    best_metrics, _ = run_backtest(
        signals,
        bars_by_symbol=bars_by_symbol,
        params=best_grid.params,
        benchmark_symbol=benchmark,
        transaction_cost_bps=transaction_cost_bps,
        slippage_bps=slippage_bps,
    )
    walk_forward = run_walk_forward(
        signals,
        bars_by_symbol=bars_by_symbol,
        parameter_grid=parameter_grid,
        train_window_days=train_window_days,
        test_window_days=test_window_days,
        min_train_trades=min_train_trades,
        benchmark_symbol=benchmark,
        transaction_cost_bps=transaction_cost_bps,
        slippage_bps=slippage_bps,
    )

    report: dict[str, object] = {
        "requested_start_date": start_date.isoformat(),
        "requested_end_date": end_date.isoformat(),
        "date_window_mode": date_window_mode,
        "bootstrap_refresh": bootstrap_refresh,
        "signals_total": len(signals),
        "symbols_total": len({signal.symbol for signal in signals}),
        "parameter_grid_size": len(parameter_grid),
        "benchmark_symbol": benchmark,
        "best_in_sample_params": _params_to_dict(best_grid.params),
        "best_in_sample_metrics": _metrics_to_dict(best_metrics),
        "walk_forward_folds": len(walk_forward.folds),
        "walk_forward_aggregate_metrics": _metrics_to_dict(walk_forward.aggregate_test_metrics),
        "walk_forward_recommended_params": (
            _params_to_dict(walk_forward.recommended_params)
            if walk_forward.recommended_params is not None
            else None
        ),
        "top_grid_results": [
            {
                "params": _params_to_dict(result.params),
                "metrics": _metrics_to_dict(result.metrics),
            }
            for result in grid_results[:10]
        ],
        "walk_forward_fold_results": [
            {
                "train_start": fold.train_start.isoformat(),
                "train_end": fold.train_end.isoformat(),
                "test_start": fold.test_start.isoformat(),
                "test_end": fold.test_end.isoformat(),
                "selected_params": _params_to_dict(fold.selected_params),
                "train_metrics": _metrics_to_dict(fold.train_metrics),
                "test_metrics": _metrics_to_dict(fold.test_metrics),
            }
            for fold in walk_forward.folds
        ],
        "price_errors": price_errors,
    }

    if output_json_path is not None:
        output_json_path.parent.mkdir(parents=True, exist_ok=True)
        output_json_path.write_text(
            json.dumps(report, indent=2, sort_keys=True, default=_json_default),
            encoding="utf-8",
        )

    typer.echo(json.dumps(report, indent=2, sort_keys=True, default=_json_default))


@ops_app.command("event-study")
def ops_event_study(
    database_path: Path | None = typer.Option(  # noqa: B008
        None,
        "--database-path",
        help="Database snapshot to evaluate and bind to the confirmatory report.",
    ),
    start_date_text: str = typer.Option(
        "",
        "--start-date",
        help="Inclusive YYYY-MM-DD start. Must be provided with --end-date.",
    ),
    end_date_text: str = typer.Option(
        "",
        "--end-date",
        help="Inclusive YYYY-MM-DD end. Must be provided with --start-date.",
    ),
    horizons_text: str = typer.Option(
        "1,3,5,10,20",
        "--horizons",
        help="Comma-separated forward-return horizons in trading days.",
    ),
    bucket_count: int = typer.Option(
        5,
        "--bucket-count",
        min=2,
        help="Number of score quantile buckets per fold.",
    ),
    train_window_days: int = typer.Option(
        365,
        "--train-window-days",
        min=30,
        help="Train window size in calendar days.",
    ),
    test_window_days: int = typer.Option(
        90,
        "--test-window-days",
        min=10,
        help="Test window size in calendar days.",
    ),
    min_train_events: int = typer.Option(
        100,
        "--min-train-events",
        min=1,
        help="Minimum canonical events required in each train fold.",
    ),
    min_test_events: int = typer.Option(
        25,
        "--min-test-events",
        min=1,
        help="Minimum canonical events required in each test fold.",
    ),
    min_total_canonical_events: int = typer.Option(
        500,
        "--min-total-canonical-events",
        min=1,
    ),
    min_monthly_canonical_events: int = typer.Option(
        20,
        "--min-monthly-canonical-events",
        min=1,
    ),
    benchmark_symbol: str = typer.Option("SPY", "--benchmark-symbol"),
    transaction_cost_bps: float = typer.Option(
        5.0,
        "--transaction-cost-bps",
        min=0.0,
    ),
    slippage_bps: float = typer.Option(
        5.0,
        "--slippage-bps",
        min=0.0,
    ),
    min_price: float = typer.Option(
        2.0,
        "--min-price",
        min=0.0,
        help="Minimum entry price for tradability filter.",
    ),
    min_median_dollar_volume_20d: float = typer.Option(
        500_000.0,
        "--min-median-dollar-volume-20d",
        min=0.0,
    ),
    conviction_feature_coverage_min: float = typer.Option(
        0.80,
        "--conviction-feature-coverage-min",
        min=0.0,
        max=1.0,
    ),
    random_seed: int = typer.Option(7, "--random-seed"),
    bootstrap_iterations: int = typer.Option(
        1000,
        "--bootstrap-iterations",
        min=100,
    ),
    monotonicity_iterations: int = typer.Option(
        1000,
        "--monotonicity-iterations",
        min=100,
    ),
    negative_control_iterations: int = typer.Option(
        500,
        "--negative-control-iterations",
        min=100,
    ),
    min_fold_count: int = typer.Option(
        3,
        "--min-fold-count",
        min=1,
        help="Minimum folds required for decision-grade output.",
    ),
    max_missing_price_skip_rate: float = typer.Option(
        0.25,
        "--max-missing-price-skip-rate",
        min=0.0,
        max=1.0,
    ),
    ci_lower_bound_bps: float = typer.Option(
        -25.0,
        "--ci-lower-bound-bps",
        help="Minimum acceptable 95% CI lower bound (basis points) for top bucket.",
    ),
    fdr_q_threshold: float = typer.Option(
        0.10,
        "--fdr-q-threshold",
        min=0.0,
        max=1.0,
    ),
    confirmatory_report_path: Path | None = typer.Option(  # noqa: B008
        None,
        "--confirmatory-report",
        help="Locked ops signal-study JSON required for any decision-grade result.",
    ),
    candidate_hypothesis: str = typer.Option(
        "E07|F00",
        "--candidate-hypothesis",
        help="Frozen signal-study hypothesis that this diagnostic is intended to support.",
    ),
    refresh_prices_enabled: bool = typer.Option(
        True,
        "--refresh-prices/--no-refresh-prices",
    ),
    output_json_path: Path | None = typer.Option(  # noqa: B008
        None,
        "--output-json",
        help="Optional file path to write JSON report.",
    ),
    output_csv_path: Path | None = typer.Option(  # noqa: B008
        None,
        "--output-csv",
        help="Optional file path to write aggregate CSV report.",
    ),
) -> None:
    """
    Run OOS event-study alpha validation for Form 4 signals.
    """
    settings = get_settings()
    selected_db = database_path or Path(settings.database_path)
    start_supplied = bool(start_date_text.strip())
    end_supplied = bool(end_date_text.strip())
    if start_supplied != end_supplied:
        typer.secho(
            "must provide both --start-date and --end-date, or neither",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2)

    if start_supplied and end_supplied:
        try:
            start_date = date.fromisoformat(start_date_text)
            end_date = date.fromisoformat(end_date_text)
        except ValueError as exc:
            typer.secho(
                "invalid date format; expected YYYY-MM-DD",
                fg=typer.colors.RED,
                err=True,
            )
            raise typer.Exit(code=2) from exc
        date_window_mode = "explicit"
    else:
        end_date = date.today()
        start_date = end_date - timedelta(days=365)
        date_window_mode = "default_last_year"

    if start_date > end_date:
        typer.secho("start-date cannot be after end-date", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2)

    horizons = _parse_int_grid(horizons_text, min_value=1)
    benchmark = benchmark_symbol.strip().upper() or "SPY"
    canonical_events = load_canonical_events(
        str(selected_db),
        start_date=start_date,
        end_date=end_date,
    )
    raw_signals = load_scored_signals(
        str(selected_db),
        start_date=start_date,
        end_date=end_date,
    )

    unique_symbols = sorted({event.symbol for event in canonical_events})
    if benchmark:
        unique_symbols.append(benchmark)
    unique_symbols = sorted(set(unique_symbols))

    if canonical_events:
        signal_min_date = min(event.filed_at.date() for event in canonical_events)
        signal_max_date = max(event.filed_at.date() for event in canonical_events)
    else:
        signal_min_date = start_date
        signal_max_date = end_date

    max_horizon = max(horizons)
    price_start = signal_min_date - timedelta(days=40)
    price_end = signal_max_date + timedelta(days=max_horizon + 10)
    price_client = StooqPriceClient(
        user_agent=settings.sec_user_agent,
        timeout_seconds=settings.market_data_timeout_seconds,
        rate_limit_per_second=settings.market_data_rate_limit_per_second,
        retry_attempts=settings.market_data_retry_attempts,
        retry_min_seconds=settings.market_data_retry_min_seconds,
        retry_max_seconds=settings.market_data_retry_max_seconds,
        prefer_yahoo=True,
    )
    bars_by_symbol: dict[str, list[DailyBar]] = {}
    price_errors: list[str] = []
    for symbol in unique_symbols:
        fetch_error: str | None = None
        cache_start, cache_end = get_price_bar_bounds(str(selected_db), symbol=symbol)
        needs_refresh = refresh_prices_enabled and (
            cache_start is None
            or cache_end is None
            or cache_start > price_start
            or cache_end < price_end
        )
        try:
            if needs_refresh:
                fetched = price_client.fetch_history(symbol)
                refresh_price_bars(str(selected_db), symbol=symbol, bars=fetched)
        except PriceDataError as exc:
            fetch_error = f"{symbol}: {exc}"
        except Exception as exc:  # Defensive: keep one symbol error from aborting.
            fetch_error = f"{symbol}: unexpected price refresh error: {exc}"

        bars = get_price_bars(
            str(selected_db),
            symbol=symbol,
            start_date=price_start,
            end_date=price_end,
        )
        if bars:
            bars_by_symbol[symbol] = bars
        elif fetch_error is None:
            if needs_refresh:
                price_errors.append(f"{symbol}: no valid price bars after refresh")
            else:
                price_errors.append(f"{symbol}: no cached bars in requested range")
        if fetch_error is not None:
            price_errors.append(fetch_error)

    # Price coverage is part of readiness. Audit only after the optional refresh so a
    # successful first run cannot report the stale pre-refresh state.
    readiness = audit_event_study_readiness(
        str(selected_db),
        start_date=start_date,
        end_date=end_date,
        canonical_events=[asdict(event) for event in canonical_events],
        config=EventStudyReadinessConfig(
            min_total_canonical_events=min_total_canonical_events,
            min_monthly_canonical_events=min_monthly_canonical_events,
            conviction_feature_coverage_min=conviction_feature_coverage_min,
        ),
    )

    event_study = run_oos_event_study(
        canonical_events,
        bars_by_symbol=bars_by_symbol,
        horizons=horizons,
        bucket_count=bucket_count,
        train_window_days=train_window_days,
        test_window_days=test_window_days,
        min_train_events=min_train_events,
        min_test_events=min_test_events,
        benchmark_symbol=benchmark,
        transaction_cost_bps=transaction_cost_bps,
        slippage_bps=slippage_bps,
        tradability=TradabilityConfig(
            min_price=min_price,
            min_median_dollar_volume_20d=min_median_dollar_volume_20d,
        ),
        random_seed=random_seed,
        bootstrap_iterations=bootstrap_iterations,
        monotonicity_iterations=monotonicity_iterations,
        negative_control_iterations=negative_control_iterations,
    )
    conviction_event_study = (
        run_oos_event_study(
            canonical_events,
            bars_by_symbol=bars_by_symbol,
            horizons=horizons,
            bucket_count=bucket_count,
            train_window_days=train_window_days,
            test_window_days=test_window_days,
            min_train_events=min_train_events,
            min_test_events=min_test_events,
            benchmark_symbol=benchmark,
            transaction_cost_bps=transaction_cost_bps,
            slippage_bps=slippage_bps,
            tradability=TradabilityConfig(
                min_price=min_price,
                min_median_dollar_volume_20d=min_median_dollar_volume_20d,
            ),
            random_seed=random_seed,
            bootstrap_iterations=bootstrap_iterations,
            monotonicity_iterations=monotonicity_iterations,
            negative_control_iterations=negative_control_iterations,
            bucket_dimension="conviction",
        )
        if readiness.conviction_feature_coverage_ready
        else None
    )

    confirmatory_gate = _load_confirmatory_gate(
        confirmatory_report_path,
        candidate_hypothesis=candidate_hypothesis,
        expected_database_path=selected_db,
    )
    go_no_go = _evaluate_event_study_gates(
        readiness=readiness,
        event_study=event_study,
        bucket_count=bucket_count,
        min_fold_count=min_fold_count,
        min_test_events=min_test_events,
        max_missing_price_skip_rate=max_missing_price_skip_rate,
        core_horizons=(5, 10),
        ci_lower_bound_bps=ci_lower_bound_bps,
        fdr_q_threshold=fdr_q_threshold,
        confirmatory_gate=confirmatory_gate,
    )

    total_cluster_packets = sum(event.cluster_packet_count for event in canonical_events)
    canonical_count = len(canonical_events)
    db_path = selected_db
    db_hash = _file_sha256(db_path) if db_path.exists() else None
    db_size_bytes = db_path.stat().st_size if db_path.exists() else None
    report: dict[str, object] = {
        "analysis_class": "exploratory_oos_diagnostic",
        "confirmatory_eligible": False,
        "run_timestamp_utc": datetime.now(tz=UTC).isoformat(),
        "database_metadata": {
            "database_path": str(selected_db.resolve()),
            "database_sha256": db_hash,
            "database_size_bytes": db_size_bytes,
        },
        "requested_start_date": start_date.isoformat(),
        "requested_end_date": end_date.isoformat(),
        "date_window_mode": date_window_mode,
        "benchmark_symbol": benchmark,
        "horizons": horizons,
        "bucket_count": bucket_count,
        "dedupe_diagnostics": {
            "raw_scored_signals": len(raw_signals),
            "canonical_events": canonical_count,
            "collapsed_duplicate_count": max(0, len(raw_signals) - canonical_count),
            "cluster_packet_total": total_cluster_packets,
            "cluster_packet_average": (
                total_cluster_packets / canonical_count if canonical_count > 0 else 0.0
            ),
        },
        "readiness": _metrics_to_dict(readiness),
        "skip_diagnostics": {
            "aggregate": event_study.aggregate_skip_diagnostics,
            "aggregate_by_horizon": event_study.aggregate_skip_diagnostics_by_horizon,
        },
        "folds": [_metrics_to_dict(fold) for fold in event_study.folds],
        "skipped_folds": [_metrics_to_dict(fold) for fold in event_study.skipped_folds],
        "aggregate_bucket_metrics": [
            _metrics_to_dict(metric) for metric in event_study.aggregate_bucket_metrics
        ],
        "monotonicity": [_metrics_to_dict(item) for item in event_study.monotonicity],
        "negative_control": [_metrics_to_dict(item) for item in event_study.negative_control],
        "conviction_bucket_analysis": {
            "available": conviction_event_study is not None,
            "unavailable_reason": (
                None
                if conviction_event_study is not None
                else "conviction_feature_coverage_below_threshold"
            ),
            "folds": (
                [_metrics_to_dict(fold) for fold in conviction_event_study.folds]
                if conviction_event_study is not None
                else []
            ),
            "skipped_folds": (
                [_metrics_to_dict(fold) for fold in conviction_event_study.skipped_folds]
                if conviction_event_study is not None
                else []
            ),
            "aggregate_bucket_metrics": (
                [
                    _metrics_to_dict(metric)
                    for metric in conviction_event_study.aggregate_bucket_metrics
                ]
                if conviction_event_study is not None
                else []
            ),
            "monotonicity": (
                [_metrics_to_dict(item) for item in conviction_event_study.monotonicity]
                if conviction_event_study is not None
                else []
            ),
            "negative_control": (
                [_metrics_to_dict(item) for item in conviction_event_study.negative_control]
                if conviction_event_study is not None
                else []
            ),
        },
        "confirmatory_gate": confirmatory_gate,
        "price_errors": price_errors,
        "go_no_go": go_no_go,
    }

    if output_json_path is not None:
        output_json_path.parent.mkdir(parents=True, exist_ok=True)
        output_json_path.write_text(
            json.dumps(report, indent=2, sort_keys=True, default=_json_default),
            encoding="utf-8",
        )
    if output_csv_path is not None:
        _write_event_study_csv(
            output_csv_path,
            aggregate_bucket_metrics=cast(
                list[dict[str, object]],
                report["aggregate_bucket_metrics"],
            ),
        )

    typer.echo(json.dumps(report, indent=2, sort_keys=True, default=_json_default))
    if not bool(go_no_go.get("decision_grade", False)):
        raise typer.Exit(code=3)


@ops_app.command("sec-ingestion")
def ops_sec_ingestion(
    once: bool = typer.Option(
        False,
        "--once/--loop",
        help="Run one ingestion cycle or keep running in a background loop.",
    ),
    interval: int = typer.Option(60, "--interval", min=10),
    poll_max_items: int = typer.Option(40, "--poll-max-items", min=1, max=200),
    enrich_limit: int = typer.Option(100, "--enrich-limit", min=1, max=1000),
    enqueue_limit: int = typer.Option(100, "--enqueue-limit", min=1, max=2000),
    output_log_path: Path | None = typer.Option(  # noqa: B008
        None, "--output-log", hidden=True
    ),
    error_log_path: Path | None = typer.Option(  # noqa: B008
        None, "--error-log", hidden=True
    ),
    heartbeat_db: Path | None = typer.Option(None, "--heartbeat-db", hidden=True),  # noqa: B008
    heartbeat_stale_seconds: int | None = typer.Option(
        None,
        "--heartbeat-stale-seconds",
        min=1,
        hidden=True,
    ),
) -> None:
    """Continuously acquire and normalize SEC filings without making decisions."""

    settings = get_settings()

    def append_process_log(path: Path | None, message: str) -> None:
        if path is None:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(message.rstrip() + "\n")

    if (heartbeat_db is None) != (heartbeat_stale_seconds is None):
        typer.secho(
            "--heartbeat-db and --heartbeat-stale-seconds must be provided together",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2)
    if heartbeat_stale_seconds is not None:
        try:
            validate_sec_ingestion_stale_threshold(
                stale_seconds=heartbeat_stale_seconds,
                settings=settings,
            )
        except ValueError as exc:
            typer.secho(str(exc), fg=typer.colors.RED, err=True)
            raise typer.Exit(code=2) from exc

    health_store = AutopilotHealthStore(heartbeat_db) if heartbeat_db is not None else None
    runtime_id = secrets.token_hex(16)
    heartbeat_failure_count = 0
    heartbeat_paused = False

    class HeartbeatOwnershipUnavailableError(sqlite3.OperationalError):
        """The worker cannot prove that it still owns the runtime heartbeat."""

    def _heartbeat(
        stage: str,
        *,
        cycle_started: bool = False,
        cycle_succeeded: bool = False,
        error: BaseException | None = None,
        advance_progress: bool = True,
        force: bool = False,
        require_success: bool = False,
    ) -> None:
        nonlocal heartbeat_failure_count
        if health_store is None or (heartbeat_paused and not force):
            return
        try:
            health_store.progress(
                runtime_id=runtime_id,
                stage=stage,
                now=datetime.now(UTC),
                cycle_started=cycle_started,
                cycle_succeeded=cycle_succeeded,
                error=error,
                advance_progress=advance_progress,
            )
        except RuntimeOwnershipError:
            raise
        except sqlite3.OperationalError as exc:
            normalized = str(exc).casefold()
            if "locked" not in normalized and "busy" not in normalized:
                raise
            if require_success:
                raise HeartbeatOwnershipUnavailableError(str(exc)) from exc
            heartbeat_failure_count += 1
            if heartbeat_failure_count == 1:
                append_process_log(
                    error_log_path,
                    f"SEC ingestion heartbeat degraded ({type(exc).__name__}: {exc})",
                )
            if heartbeat_failure_count >= 3:
                raise RuntimeError(
                    "SEC ingestion heartbeat unavailable for three consecutive progress writes"
                ) from exc
        except (OSError, sqlite3.Error):
            raise
        else:
            heartbeat_failure_count = 0

    def _run_cycle() -> SecIngestionCycleResult:
        nonlocal heartbeat_paused
        # Even while forward-progress timestamps are frozen after a retryable failure, prove that
        # this runtime still owns the heartbeat row before the next mutating SEC poll.
        _heartbeat(
            "cycle_started",
            cycle_started=True,
            advance_progress=not heartbeat_paused,
            force=True,
            require_success=True,
        )
        poll_result = run_sec_poll_once(settings, max_items=poll_max_items, dry_run=False)
        # A successful source poll proves forward progress after a retryable failure. Resume
        # normal stage heartbeats before the potentially long per-item enrichment work.
        heartbeat_paused = False
        _heartbeat("sec_poll_completed")
        if health_store is None:
            enrich_result = enrich_filings_with_xml_url(settings, limit=enrich_limit)
        else:
            enrich_result = enrich_filings_with_xml_url(
                settings,
                limit=enrich_limit,
                progress_callback=_heartbeat,
            )
        _heartbeat("enrichment_completed")
        if health_store is None:
            enqueue_result = enqueue_review_packets(settings, limit=enqueue_limit)
        else:
            enqueue_result = enqueue_review_packets(
                settings,
                limit=enqueue_limit,
                progress_callback=_heartbeat,
            )
        _heartbeat("review_enqueue_completed")

        if enrich_result.http_failed or enrich_result.xml_not_found:
            append_process_log(
                error_log_path,
                "SEC ingestion enrichment degraded "
                f"(http_failed={enrich_result.http_failed}, "
                f"xml_not_found={enrich_result.xml_not_found})",
            )
        if (
            enqueue_result.http_failed
            or enqueue_result.parse_failed
            or enqueue_result.market_failed
        ):
            append_process_log(
                error_log_path,
                "SEC ingestion review enrichment degraded "
                f"(http_failed={enqueue_result.http_failed}, "
                f"parse_failed={enqueue_result.parse_failed}, "
                f"market_failed={enqueue_result.market_failed})",
            )

        cycle = SecIngestionCycleResult(
            fetched=poll_result.fetched,
            inserted=poll_result.inserted,
            skipped_existing=poll_result.skipped_existing,
            enriched_scanned=enrich_result.scanned,
            enriched_updated=enrich_result.updated,
            enqueue_processed=enqueue_result.processed,
            enqueue_enqueued=enqueue_result.enqueued,
            enrichment_http_failed=enrich_result.http_failed,
            enrichment_xml_not_found=enrich_result.xml_not_found,
            enqueue_http_failed=enqueue_result.http_failed,
            enqueue_parse_failed=enqueue_result.parse_failed,
            enqueue_market_failed=enqueue_result.market_failed,
            source_items_seen=poll_result.source_items_seen,
            source_boundary_rejected=poll_result.source_boundary_rejected,
            source_invalid_items=poll_result.source_invalid_items,
        )
        cycle_message = (
            "ops SEC ingestion cycle completed "
            f"(fetched={cycle.fetched}, inserted={cycle.inserted}, "
            f"skipped_existing={cycle.skipped_existing}, "
            f"source_items_seen={cycle.source_items_seen}, "
            f"source_boundary_rejected={cycle.source_boundary_rejected}, "
            f"source_invalid_items={cycle.source_invalid_items}, "
            f"enrich_scanned={cycle.enriched_scanned}, "
            f"enrich_updated={cycle.enriched_updated}, "
            f"enqueue_processed={cycle.enqueue_processed}, "
            f"enqueue_enqueued={cycle.enqueue_enqueued}, "
            f"enrichment_http_failed={cycle.enrichment_http_failed}, "
            f"enrichment_xml_not_found={cycle.enrichment_xml_not_found}, "
            f"enqueue_http_failed={cycle.enqueue_http_failed}, "
            f"enqueue_parse_failed={cycle.enqueue_parse_failed}, "
            f"enqueue_market_failed={cycle.enqueue_market_failed})"
        )
        typer.echo(cycle_message)
        append_process_log(output_log_path, cycle_message)
        _heartbeat("cycle_succeeded", cycle_succeeded=True)
        return cycle

    def _record_retryable_cycle_failure(
        exc: BaseException,
        *,
        loop_mode: bool,
    ) -> None:
        nonlocal heartbeat_paused
        heartbeat_paused = True
        # Preserve the diagnostic without making a permanently failing loop look healthy. Wait
        # and retry-stage heartbeats remain paused until a later SEC poll actually succeeds.
        _heartbeat(
            "cycle_retryable_failure",
            error=exc,
            advance_progress=False,
            force=True,
        )
        failure_message = (
            "ops SEC ingestion cycle failed "
            f"(retryable, {type(exc).__name__}: {exc})"
        )
        typer.secho(failure_message, fg=typer.colors.RED, err=True)
        append_process_log(error_log_path, failure_message)
        if not loop_mode:
            raise typer.Exit(code=1) from exc

    def _run_cycle_with_recovery(*, loop_mode: bool) -> SecIngestionCycleResult | None:
        try:
            return _run_cycle()
        except (SecHttpError, SecRssParseError) as exc:
            _record_retryable_cycle_failure(exc, loop_mode=loop_mode)
            return None
        except HeartbeatOwnershipUnavailableError:
            raise
        except sqlite3.OperationalError as exc:
            normalized = str(exc).casefold()
            if "locked" not in normalized and "busy" not in normalized:
                raise
            _record_retryable_cycle_failure(exc, loop_mode=loop_mode)
            return None

    def _source_changed_during_wait(startup_fingerprint: str) -> bool:
        remaining = float(interval)
        while remaining > 0:
            _heartbeat("cycle_wait")
            if runtime_source_fingerprint() != startup_fingerprint:
                return True
            sleep_seconds = min(SOURCE_REVISION_CHECK_INTERVAL_SECONDS, remaining)
            time.sleep(sleep_seconds)
            remaining -= sleep_seconds
        return runtime_source_fingerprint() != startup_fingerprint

    try:
        startup_fingerprint: str | None = None
        if health_store is not None:
            if not once:
                ensure_kill_on_close_process_tree()
            startup_fingerprint = runtime_source_fingerprint()
            health_store.register_runtime(
                runtime_id=runtime_id,
                source_fingerprint=startup_fingerprint,
                now=datetime.now(UTC),
            )
        if once:
            _run_cycle_with_recovery(loop_mode=False)
            return
        if startup_fingerprint is None:
            startup_fingerprint = runtime_source_fingerprint()
        _run_cycle_with_recovery(loop_mode=True)
        while True:
            if _source_changed_during_wait(startup_fingerprint):
                _heartbeat("source_changed")
                source_message = (
                    "SEC ingestion source changed; exiting so the hidden watchdog "
                    "can start a fresh worker"
                )
                typer.secho(source_message, fg=typer.colors.YELLOW, err=True)
                append_process_log(output_log_path, source_message)
                return
            _run_cycle_with_recovery(loop_mode=True)
    except typer.Exit:
        raise
    except RuntimeOwnershipError as exc:
        failure_message = f"SEC ingestion process stopped ({type(exc).__name__}: {exc})"
        append_process_log(error_log_path, failure_message)
        raise
    except Exception as exc:
        try:
            _heartbeat("process_failure", error=exc)
        except Exception as heartbeat_exc:
            append_process_log(
                error_log_path,
                "SEC ingestion terminal heartbeat failed "
                f"({type(heartbeat_exc).__name__}: {heartbeat_exc})",
            )
        failure_message = f"SEC ingestion process failed ({type(exc).__name__}: {exc})"
        append_process_log(error_log_path, failure_message)
        raise


@ops_app.command("autopilot")
def ops_autopilot(
    once: bool = typer.Option(
        False,
        "--once/--loop",
        help="Run one cycle or keep running in background loop.",
    ),
    interval: int = typer.Option(
        300,
        "--interval",
        min=10,
        help="Seconds between cycles when looping.",
    ),
    poll_max_items: int = typer.Option(40, "--poll-max-items", min=1, max=200),
    enrich_limit: int = typer.Option(100, "--enrich-limit", min=1, max=1000),
    enqueue_limit: int = typer.Option(100, "--enqueue-limit", min=1, max=2000),
    sec_ingestion_enabled: bool = typer.Option(
        True,
        "--sec-ingestion/--no-sec-ingestion",
        help="Run legacy in-process SEC ingestion before decisions.",
    ),
    decision_limit: int = typer.Option(200, "--decision-limit", min=1, max=5000),
    decision_engine: str = typer.Option("quant", "--decision-engine", help="quant|rules"),
    approve_score_min: float = typer.Option(90.0, "--approve-score-min"),
    approve_net_buy_shares_min: float = typer.Option(0.0, "--approve-net-buy-shares-min"),
    reject_score_max: float = typer.Option(35.0, "--reject-score-max"),
    conviction_lookback_days: int = typer.Option(
        365,
        "--conviction-lookback-days",
        min=60,
        max=3650,
    ),
    conviction_min_history_samples: int = typer.Option(
        200,
        "--conviction-min-history-samples",
        min=10,
        max=100000,
    ),
    conviction_min_role_samples: int = typer.Option(
        40,
        "--conviction-min-role-samples",
        min=5,
        max=10000,
    ),
    conviction_min_score: float = typer.Option(70.0, "--conviction-min-score"),
    conviction_reject_max: float = typer.Option(30.0, "--conviction-reject-max"),
    conviction_value_pct_min: float = typer.Option(50.0, "--conviction-value-pct-min"),
    conviction_holding_pct_min: float = typer.Option(60.0, "--conviction-holding-pct-min"),
    conviction_liquidity_pct_min: float = typer.Option(65.0, "--conviction-liquidity-pct-min"),
    director_turnover_min: float = typer.Option(0.1, "--director-turnover-min"),
    shock_turnover_min: float = typer.Option(0.25, "--shock-turnover-min"),
    quant_agent_id: str = typer.Option("quant-insider", "--quant-agent-id"),
    quant_thinking: str = typer.Option("low", "--quant-thinking"),
    quant_timeout_seconds: int = typer.Option(120, "--quant-timeout-seconds", min=10, max=900),
    quant_batch_size: int = typer.Option(8, "--quant-batch-size", min=1, max=200),
    quant_min_confidence: float = typer.Option(0.7, "--quant-min-confidence"),
    quant_require_isolated_agent: bool = typer.Option(
        True,
        "--quant-require-isolated-agent/--no-quant-require-isolated-agent",
    ),
    quant_fallback_to_rules: bool = typer.Option(
        False,
        "--quant-fallback-to-rules/--no-quant-fallback-to-rules",
    ),
    analyst: str = typer.Option("quant", "--analyst"),
    notify: bool = typer.Option(True, "--notify/--no-notify"),
    notify_approve_only: bool = typer.Option(
        True,
        "--notify-approve-only/--notify-all-decisions",
    ),
    output_log_path: Path | None = typer.Option(  # noqa: B008
        None, "--output-log", hidden=True
    ),
    error_log_path: Path | None = typer.Option(  # noqa: B008
        None, "--error-log", hidden=True
    ),
    alpha_chain_python: Path | None = typer.Option(  # noqa: B008
        None, "--alpha-chain-python", hidden=True
    ),
    alpha_chain_script: Path | None = typer.Option(  # noqa: B008
        None, "--alpha-chain-script", hidden=True
    ),
    option_chain_store_db: Path | None = typer.Option(  # noqa: B008
        None, "--option-chain-store-db", hidden=True
    ),
    heartbeat_db: Path | None = typer.Option(None, "--heartbeat-db", hidden=True),  # noqa: B008
    heartbeat_stale_seconds: int | None = typer.Option(
        None,
        "--heartbeat-stale-seconds",
        min=1,
        hidden=True,
    ),
) -> None:
    """
    Run the auto-decision loop and, by default, legacy in-process SEC ingestion.
    """
    settings = get_settings()
    repo_root = Path(__file__).resolve().parents[2]

    def append_process_log(path: Path | None, message: str) -> None:
        if path is None:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(message.rstrip() + "\n")

    decision_engine = decision_engine.strip().lower()
    if decision_engine not in {"quant", "rules"}:
        typer.secho(
            "invalid --decision-engine (expected quant|rules)",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2)
    quant_thinking = quant_thinking.strip().lower()
    if quant_thinking not in {"off", "minimal", "low", "medium", "high"}:
        typer.secho(
            "invalid --quant-thinking (expected off|minimal|low|medium|high)",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2)
    if conviction_min_score < 0 or conviction_min_score > 100:
        typer.secho(
            "invalid --conviction-min-score (expected 0..100)",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2)
    if conviction_reject_max < 0 or conviction_reject_max > 100:
        typer.secho(
            "invalid --conviction-reject-max (expected 0..100)",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2)
    if conviction_reject_max > conviction_min_score:
        typer.secho(
            "--conviction-reject-max cannot exceed --conviction-min-score",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2)
    for option_name, threshold in (
        ("--conviction-value-pct-min", conviction_value_pct_min),
        ("--conviction-holding-pct-min", conviction_holding_pct_min),
        ("--conviction-liquidity-pct-min", conviction_liquidity_pct_min),
    ):
        if threshold < 0 or threshold > 100:
            typer.secho(
                f"invalid {option_name} (expected 0..100)",
                fg=typer.colors.RED,
                err=True,
            )
            raise typer.Exit(code=2)
    if director_turnover_min < 0:
        typer.secho(
            "invalid --director-turnover-min (expected >= 0)",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2)
    if shock_turnover_min < 0:
        typer.secho(
            "invalid --shock-turnover-min (expected >= 0)",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2)
    quant_agent_id = quant_agent_id.strip()
    if (
        decision_engine == "quant"
        and quant_require_isolated_agent
        and quant_agent_id.lower() == "main"
    ):
        typer.secho(
            "unsafe quant agent: 'main' is blocked in isolated mode; "
            "use a dedicated agent id (for example, quant-insider) or pass "
            "--no-quant-require-isolated-agent",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2)
    option_chain_values = (
        alpha_chain_python,
        alpha_chain_script,
        option_chain_store_db,
    )
    if any(value is not None for value in option_chain_values) and not all(
        value is not None for value in option_chain_values
    ):
        typer.secho(
            "--alpha-chain-python, --alpha-chain-script, and --option-chain-store-db "
            "must be provided together",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2)
    if (heartbeat_db is None) != (heartbeat_stale_seconds is None):
        typer.secho(
            "--heartbeat-db and --heartbeat-stale-seconds must be provided together",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2)
    if heartbeat_stale_seconds is not None:
        try:
            validate_stale_threshold(
                quant_timeout_seconds=quant_timeout_seconds,
                stale_seconds=heartbeat_stale_seconds,
                settings=settings,
            )
        except ValueError as exc:
            typer.secho(str(exc), fg=typer.colors.RED, err=True)
            raise typer.Exit(code=2) from exc
    option_chain_config = (
        OptionChainAdmissionConfig(
            source_db=Path(settings.database_path),
            repo_root=repo_root,
            chain_store_db=cast(Path, option_chain_store_db),
            alpha_python=cast(Path, alpha_chain_python),
            alpha_script=cast(Path, alpha_chain_script),
        )
        if all(value is not None for value in option_chain_values)
        else None
    )
    health_store = AutopilotHealthStore(heartbeat_db) if heartbeat_db is not None else None
    runtime_id = secrets.token_hex(16)
    heartbeat_failure_count = 0

    def _heartbeat(
        stage: str,
        *,
        cycle_started: bool = False,
        cycle_succeeded: bool = False,
        error: BaseException | None = None,
    ) -> None:
        nonlocal heartbeat_failure_count
        if health_store is None:
            return
        try:
            health_store.progress(
                runtime_id=runtime_id,
                stage=stage,
                now=datetime.now(UTC),
                cycle_started=cycle_started,
                cycle_succeeded=cycle_succeeded,
                error=error,
            )
        except RuntimeOwnershipError:
            raise
        except (OSError, sqlite3.Error) as exc:
            heartbeat_failure_count += 1
            if heartbeat_failure_count == 1:
                append_process_log(
                    error_log_path,
                    f"autopilot heartbeat degraded ({type(exc).__name__}: {exc})",
                )
            if heartbeat_failure_count >= 3:
                raise RuntimeError(
                    "autopilot heartbeat unavailable for three consecutive progress writes"
                ) from exc
        else:
            heartbeat_failure_count = 0

    def _run_cycle() -> AutoPilotCycleResult:
        _heartbeat("cycle_started", cycle_started=True)
        outbox_notified = 0
        notify_suppressed_duplicate = 0
        alerted_event_keys = _recent_alerted_event_keys(settings.database_path)
        if notify and health_store is not None and not once:
            outbox_event_keys_attempted: set[str] = set()
            for outbox_index, outbox_packet in enumerate(
                list_notification_outbox(settings.database_path, limit=decision_limit)
            ):
                _heartbeat(f"notification_outbox_{outbox_index}_started")
                packet_id = str(outbox_packet["packet_id"])
                decision_payload = outbox_packet.get("decision")
                if not isinstance(decision_payload, dict):
                    append_process_log(
                        error_log_path,
                        f"autopilot notification outbox malformed for packet={packet_id}",
                    )
                    continue
                outbox_event_key = _economic_event_key(outbox_packet)
                if (
                    outbox_event_key is not None
                    and outbox_event_key in alerted_event_keys
                ):
                    suppressed = mark_notification_suppressed(
                        settings.database_path,
                        packet_id,
                        decision_payload,
                    )
                    notify_suppressed_duplicate += suppressed
                    log_path = output_log_path if suppressed == 1 else error_log_path
                    outcome = "suppressed duplicate" if suppressed == 1 else "stale suppression"
                    append_process_log(
                        log_path,
                        f"autopilot notification outbox {outcome} "
                        f"packet={packet_id} event={outbox_event_key}",
                    )
                    continue
                if (
                    outbox_event_key is not None
                    and outbox_event_key in outbox_event_keys_attempted
                ):
                    notify_suppressed_duplicate += 1
                    append_process_log(
                        output_log_path,
                        "autopilot notification outbox deferred duplicate behind pending "
                        f"representative packet={packet_id} event={outbox_event_key}",
                    )
                    continue
                if outbox_event_key is not None:
                    outbox_event_keys_attempted.add(outbox_event_key)
                try:
                    _send_review_notification(
                        settings,
                        {key: str(value) for key, value in decision_payload.items()},
                        packet=outbox_packet,
                        dry_message=str(decision_payload.get("reason", "decision alert")),
                    )
                    acknowledged = mark_notification_delivered(
                        settings.database_path,
                        packet_id,
                        decision_payload,
                    )
                    if acknowledged == 1 and outbox_event_key is not None:
                        alerted_event_keys.add(outbox_event_key)
                    outbox_notified += acknowledged
                    log_path = output_log_path if acknowledged == 1 else error_log_path
                    outcome = "delivered" if acknowledged == 1 else "stale acknowledgement"
                    append_process_log(
                        log_path,
                        f"autopilot notification outbox {outcome} packet={packet_id}",
                    )
                except NtfyNotificationError as exc:
                    append_process_log(
                        error_log_path,
                        f"autopilot notification outbox failed for packet={packet_id}: {exc}",
                    )
        if sec_ingestion_enabled:
            poll_result = run_sec_poll_once(settings, max_items=poll_max_items, dry_run=False)
            _heartbeat("sec_poll_completed")
            if health_store is None:
                enrich_result = enrich_filings_with_xml_url(settings, limit=enrich_limit)
            else:
                enrich_result = enrich_filings_with_xml_url(
                    settings,
                    limit=enrich_limit,
                    progress_callback=_heartbeat,
                )
            _heartbeat("enrichment_completed")
            if health_store is None:
                enqueue_result = enqueue_review_packets(settings, limit=enqueue_limit)
            else:
                enqueue_result = enqueue_review_packets(
                    settings,
                    limit=enqueue_limit,
                    progress_callback=_heartbeat,
                )
            _heartbeat("review_enqueue_completed")
        else:
            poll_result = PollResult(fetched=0, inserted=0, skipped_existing=0)
            enrich_result = EnrichResult(scanned=0, updated=0)
            enqueue_result = QueueResult(processed=0, enqueued=0)
            _heartbeat("external_sec_ingestion")
        if enrich_result.http_failed or enrich_result.xml_not_found:
            append_process_log(
                error_log_path,
                "autopilot SEC enrichment degraded "
                f"(http_failed={enrich_result.http_failed}, "
                f"xml_not_found={enrich_result.xml_not_found})",
            )
        if (
            enqueue_result.http_failed
            or enqueue_result.parse_failed
            or enqueue_result.market_failed
        ):
            append_process_log(
                error_log_path,
                "autopilot review enrichment degraded "
                f"(http_failed={enqueue_result.http_failed}, "
                f"parse_failed={enqueue_result.parse_failed}, "
                f"market_failed={enqueue_result.market_failed})",
            )
        pending = list_pending_review_packets(settings.database_path, limit=decision_limit)
        conviction_history = _load_conviction_history(
            settings.database_path,
            as_of_date=date.today(),
            lookback_days=conviction_lookback_days,
        )
        _heartbeat("history_loaded")
        baseline_rules: dict[str, AutoDecisionRuleResult] = {}
        for packet in pending:
            packet_id_obj = packet.get("packet_id")
            if not isinstance(packet_id_obj, str):
                continue
            baseline_rule = _auto_decide_packet(
                packet,
                approve_score_min=approve_score_min,
                approve_net_buy_shares_min=approve_net_buy_shares_min,
                reject_score_max=reject_score_max,
            )
            baseline_rules[packet_id_obj] = _apply_conviction_baseline(
                baseline_rule,
                packet,
                history=conviction_history,
                min_history_samples=conviction_min_history_samples,
                min_role_samples=conviction_min_role_samples,
                conviction_min_score=conviction_min_score,
                conviction_reject_max=conviction_reject_max,
                conviction_value_pct_min=conviction_value_pct_min,
                conviction_holding_pct_min=conviction_holding_pct_min,
                conviction_liquidity_pct_min=conviction_liquidity_pct_min,
                director_turnover_min=director_turnover_min,
                shock_turnover_min=shock_turnover_min,
            )
        _heartbeat("baseline_completed")
        quant_decisions: dict[str, AutoDecisionRuleResult] = {}
        quant_error: str | None = None
        if decision_engine == "quant" and pending:
            quant_candidates = [
                packet
                for packet in pending
                if isinstance(packet.get("packet_id"), str)
                and baseline_rules.get(str(packet.get("packet_id"))) is not None
                # Route BOTH baseline approvals and baseline escalations to the quant judge.
                # "escalate" is the baseline explicitly saying it cannot decide -- that is exactly
                # the case a stronger judge exists for. Before 2026-08-10 only approvals were sent,
                # so when baseline approvals dried up (last one 2026-07-10) the judge was starved to
                # zero candidates and every ambiguous filing was silently dropped: 167 escalations
                # accumulated in Aug 2026 alone, including one scoring 82.67. Hard baseline rejects
                # still never reach the judge, which keeps the LLM cost on the ambiguous minority.
                and baseline_rules[str(packet.get("packet_id"))].decision in ("approve", "escalate")
            ]
            if health_store is None:
                quant_decisions, quant_error = _decide_packets_with_quant(
                    quant_candidates,
                    quant_agent_id=quant_agent_id,
                    quant_timeout_seconds=quant_timeout_seconds,
                    quant_thinking=quant_thinking,
                    quant_batch_size=quant_batch_size,
                )
            else:
                quant_decisions, quant_error = _decide_packets_with_quant(
                    quant_candidates,
                    quant_agent_id=quant_agent_id,
                    quant_timeout_seconds=quant_timeout_seconds,
                    quant_thinking=quant_thinking,
                    quant_batch_size=quant_batch_size,
                    progress_callback=_heartbeat,
                )
            # OBSERVABILITY (2026-08-10): quant_error used to be consumed silently by the
            # fallback-to-baseline branch, so a dead decision engine looked identical to a quiet
            # market. That is how the judge stayed broken from ~May to Aug 2026 with a clean
            # error log. Always surface it; the counters alone cannot distinguish the two.
            if quant_error is not None:
                quant_message = (
                    f"quant decision engine degraded ({len(quant_decisions)}/"
                    f"{len(quant_candidates)} decided): {quant_error}"
                )
                typer.secho(
                    quant_message,
                    fg=typer.colors.YELLOW,
                    err=True,
                )
                append_process_log(error_log_path, quant_message)
            elif quant_candidates:
                quant_message = (
                    f"quant decided {len(quant_decisions)}/{len(quant_candidates)} candidates"
                )
                typer.secho(
                    quant_message,
                    fg=typer.colors.GREEN,
                    err=True,
                )
                append_process_log(output_log_path, quant_message)

        decided = 0
        approved = 0
        rejected = 0
        escalated = 0
        deadlettered = 0
        notified = 0
        # Seeded from recent approvals so co-filings landing in LATER cycles are still caught,
        # then extended in-cycle so co-filings inside a single batch collapse too.
        approved_high_edge = 0
        rejected_low_edge = 0
        escalated_missing_context = 0
        escalated_schema_invalid = 0
        quant_deferred = 0
        seen_decision_keys: set[str] = set()
        option_chain_succeeded = 0
        option_chain_skipped_cadence = 0
        option_chain_failed = 0
        option_chain_timed_out = 0
        option_chain_ambiguous = 0

        for packet_index, packet in enumerate(pending):
            _heartbeat(f"decision_{packet_index}_started")
            packet_id_obj = packet.get("packet_id")
            if not isinstance(packet_id_obj, str):
                continue

            decision_key = _packet_decision_key(packet)
            if decision_key is not None and decision_key in seen_decision_keys:
                rule = AutoDecisionRuleResult(
                    decision="deadletter",
                    reason=f"safety dedupe: duplicate pending packet key={decision_key}",
                    source="safety",
                    confidence=None,
                    reason_code="safety_duplicate_packet",
                )
            else:
                if decision_key is not None:
                    seen_decision_keys.add(decision_key)
                if decision_engine == "quant":
                    packet_baseline_rule = baseline_rules.get(packet_id_obj)
                    # Only hard baseline REJECTS short-circuit the judge; approvals and escalations
                    # are both adjudicated by quant (see the quant_candidates note above).
                    if packet_baseline_rule is not None and packet_baseline_rule.decision not in (
                        "approve",
                        "escalate",
                    ):
                        rule = packet_baseline_rule
                    else:
                        quant_rule = quant_decisions.get(packet_id_obj)
                        if quant_rule is not None:
                            if packet_baseline_rule is not None:
                                quant_rule.conviction_score = packet_baseline_rule.conviction_score
                                quant_rule.conviction_holding_pct = (
                                    packet_baseline_rule.conviction_holding_pct
                                )
                                quant_rule.conviction_value_pct = (
                                    packet_baseline_rule.conviction_value_pct
                                )
                                quant_rule.conviction_liquidity_pct = (
                                    packet_baseline_rule.conviction_liquidity_pct
                                )
                            rule = quant_rule
                        elif quant_error is not None and quant_fallback_to_rules:
                            rule = packet_baseline_rule or _auto_decide_packet(
                                packet,
                                approve_score_min=approve_score_min,
                                approve_net_buy_shares_min=approve_net_buy_shares_min,
                                reject_score_max=reject_score_max,
                            )
                        else:
                            # Infrastructure failure is not a scientific classification. Keep the
                            # packet pending so a later cycle can retry the identical immutable
                            # payload; applying ``escalate`` here used to suppress signals forever.
                            quant_deferred += 1
                            continue
                else:
                    rule = baseline_rules.get(packet_id_obj) or _auto_decide_packet(
                        packet,
                        approve_score_min=approve_score_min,
                        approve_net_buy_shares_min=approve_net_buy_shares_min,
                        reject_score_max=reject_score_max,
                    )
                rule = _apply_approve_guardrails(
                    rule,
                    packet,
                    approve_score_min=approve_score_min,
                    approve_net_buy_shares_min=approve_net_buy_shares_min,
                    quant_min_confidence=quant_min_confidence,
                )
                if rule.conviction_score is None:
                    packet_baseline_rule = baseline_rules.get(packet_id_obj)
                    if packet_baseline_rule is not None:
                        rule.conviction_score = packet_baseline_rule.conviction_score
                        rule.conviction_holding_pct = packet_baseline_rule.conviction_holding_pct
                        rule.conviction_value_pct = packet_baseline_rule.conviction_value_pct
                        rule.conviction_liquidity_pct = (
                            packet_baseline_rule.conviction_liquidity_pct
                        )

            payload: dict[str, object] = {
                "packet_id": packet_id_obj,
                "decision": rule.decision,
                "analyst": analyst,
                "reason": rule.reason,
                "decision_source": rule.source,
                "decision_reason_code": rule.reason_code,
            }
            if rule.confidence is not None:
                payload["confidence"] = round(rule.confidence, 4)
            if rule.conviction_score is not None:
                payload["conviction_score"] = round(rule.conviction_score, 2)
            if rule.conviction_holding_pct is not None:
                payload["conviction_holding_pct"] = round(rule.conviction_holding_pct, 2)
            if rule.conviction_value_pct is not None:
                payload["conviction_value_pct"] = round(rule.conviction_value_pct, 2)
            if rule.conviction_liquidity_pct is not None:
                payload["conviction_liquidity_pct"] = round(
                    rule.conviction_liquidity_pct,
                    2,
                )

            if rule.decision == "approve" and option_chain_config is not None:
                try:
                    packet_payload = packet.get("payload")
                    if not isinstance(packet_payload, dict):
                        raise ValueError("approved packet payload is not an object")
                    symbol_value = packet_payload.get("issuer_symbol")
                    if not isinstance(symbol_value, str):
                        raise ValueError("approved packet has no issuer symbol")
                    chain_result = capture_predecision_option_chain(
                        option_chain_config,
                        packet_id=packet_id_obj,
                        symbol=symbol_value,
                    )
                    if chain_result.status == "succeeded":
                        option_chain_succeeded += 1
                    elif chain_result.status == "skipped_cadence":
                        option_chain_skipped_cadence += 1
                    elif chain_result.status == "timed_out":
                        option_chain_timed_out += 1
                    elif chain_result.status == "failed":
                        option_chain_failed += 1
                    else:
                        option_chain_ambiguous += 1
                    chain_message = (
                        "predecision option-chain capture "
                        f"packet={packet_id_obj} status={chain_result.status} "
                        f"batch={chain_result.batch_id} exit={chain_result.exit_code} "
                        f"error={chain_result.error_kind}"
                    )
                    if chain_result.status in {"succeeded", "skipped_cadence"}:
                        append_process_log(output_log_path, chain_message)
                    else:
                        append_process_log(error_log_path, chain_message)
                except ProcessTreeCleanupError:
                    raise
                except Exception as exc:
                    option_chain_failed += 1
                    append_process_log(
                        error_log_path,
                        "predecision option-chain capture "
                        f"packet={packet_id_obj} status=internal_failure "
                        f"error={type(exc).__name__}: {exc}",
                    )
                _heartbeat(f"decision_{packet_index}_option_chain_completed")

            should_notify = notify and (not notify_approve_only or rule.decision == "approve")
            suppress_notification = False
            event_key: str | None = None
            if should_notify:
                event_key = _economic_event_key(packet)
                if event_key is not None and event_key in alerted_event_keys:
                    notify_suppressed_duplicate += 1
                    duplicate_message = (
                        f"suppressing duplicate alert for packet={packet_id_obj}: "
                        f"same economic event already alerted ({event_key})"
                    )
                    typer.secho(
                        duplicate_message,
                        fg=typer.colors.YELLOW,
                        err=True,
                    )
                    append_process_log(output_log_path, duplicate_message)
                    should_notify = False
                    suppress_notification = True

            try:
                updated = apply_decision(
                    settings.database_path,
                    payload,
                    notification_required=should_notify or suppress_notification,
                    notification_suppressed=suppress_notification,
                )
            except DecisionValidationError as exc:
                failure_message = f"autopilot decision failed for packet={packet_id_obj}: {exc}"
                typer.secho(
                    failure_message,
                    fg=typer.colors.RED,
                    err=True,
                )
                append_process_log(error_log_path, failure_message)
                continue

            _heartbeat(f"decision_{packet_index}_applied")

            if updated != 1:
                continue

            decided += 1
            if rule.decision == "approve":
                approved += 1
            elif rule.decision == "reject":
                rejected += 1
            elif rule.decision == "deadletter":
                deadlettered += 1
            else:
                escalated += 1

            if rule.decision == "approve" and rule.reason_code in {
                "quant_high_edge",
                "rules_high_edge",
            }:
                approved_high_edge += 1
            if rule.decision == "reject" and rule.reason_code.startswith("reject_"):
                rejected_low_edge += 1
            if rule.decision == "escalate" and rule.reason_code == "missing_market_context":
                escalated_missing_context += 1
            if rule.decision == "escalate" and rule.reason_code == "quant_schema_invalid":
                escalated_schema_invalid += 1

            if should_notify:
                try:
                    notify_payload = {k: str(v) for k, v in payload.items()}
                    _send_review_notification(
                        settings,
                        notify_payload,
                        packet=packet,
                        dry_message=rule.reason,
                    )
                    acknowledged = mark_notification_delivered(
                        settings.database_path,
                        packet_id_obj,
                        payload,
                    )
                    if event_key is not None and acknowledged == 1:
                        alerted_event_keys.add(event_key)
                    notified += acknowledged
                except NtfyNotificationError as exc:
                    failure_message = (
                        f"autopilot notification failed for packet={packet_id_obj}: {exc}"
                    )
                    typer.secho(
                        failure_message,
                        fg=typer.colors.RED,
                        err=True,
                    )
                    append_process_log(error_log_path, failure_message)

        cycle = AutoPilotCycleResult(
            fetched=poll_result.fetched,
            inserted=poll_result.inserted,
            skipped_existing=poll_result.skipped_existing,
            enriched_scanned=enrich_result.scanned,
            enriched_updated=enrich_result.updated,
            enqueue_processed=enqueue_result.processed,
            enqueue_enqueued=enqueue_result.enqueued,
            pending_seen=len(pending),
            decided=decided,
            approved=approved,
            rejected=rejected,
            escalated=escalated,
            deadlettered=deadlettered,
            notified=notified,
            approved_high_edge=approved_high_edge,
            rejected_low_edge=rejected_low_edge,
            escalated_missing_context=escalated_missing_context,
            escalated_schema_invalid=escalated_schema_invalid,
            quant_deferred=quant_deferred,
            notify_suppressed_duplicate=notify_suppressed_duplicate,
            option_chain_succeeded=option_chain_succeeded,
            option_chain_skipped_cadence=option_chain_skipped_cadence,
            option_chain_failed=option_chain_failed,
            option_chain_timed_out=option_chain_timed_out,
            option_chain_ambiguous=option_chain_ambiguous,
            enrichment_http_failed=enrich_result.http_failed,
            enrichment_xml_not_found=enrich_result.xml_not_found,
            enqueue_http_failed=enqueue_result.http_failed,
            enqueue_parse_failed=enqueue_result.parse_failed,
            enqueue_market_failed=enqueue_result.market_failed,
            outbox_notified=outbox_notified,
            source_items_seen=poll_result.source_items_seen,
            source_boundary_rejected=poll_result.source_boundary_rejected,
            source_invalid_items=poll_result.source_invalid_items,
        )
        cycle_message = (
            "ops autopilot cycle completed "
            f"(fetched={cycle.fetched}, inserted={cycle.inserted}, "
            f"skipped_existing={cycle.skipped_existing}, "
            f"source_items_seen={cycle.source_items_seen}, "
            f"source_boundary_rejected={cycle.source_boundary_rejected}, "
            f"source_invalid_items={cycle.source_invalid_items}, "
            f"enrich_scanned={cycle.enriched_scanned}, enrich_updated={cycle.enriched_updated}, "
            f"enqueue_processed={cycle.enqueue_processed}, "
            f"enqueue_enqueued={cycle.enqueue_enqueued}, "
            f"pending_seen={cycle.pending_seen}, decided={cycle.decided}, "
            f"approved={cycle.approved}, rejected={cycle.rejected}, "
            f"escalated={cycle.escalated}, deadlettered={cycle.deadlettered}, "
            f"notified={cycle.notified}, approved_high_edge={cycle.approved_high_edge}, "
            f"rejected_low_edge={cycle.rejected_low_edge}, "
            f"escalated_missing_context={cycle.escalated_missing_context}, "
            f"escalated_schema_invalid={cycle.escalated_schema_invalid}, "
            f"quant_deferred={cycle.quant_deferred}, "
            f"notify_suppressed_duplicate={cycle.notify_suppressed_duplicate}, "
            f"option_chain_succeeded={cycle.option_chain_succeeded}, "
            f"option_chain_skipped_cadence={cycle.option_chain_skipped_cadence}, "
            f"option_chain_failed={cycle.option_chain_failed}, "
            f"option_chain_timed_out={cycle.option_chain_timed_out}, "
            f"option_chain_ambiguous={cycle.option_chain_ambiguous}, "
            f"enrichment_http_failed={cycle.enrichment_http_failed}, "
            f"enrichment_xml_not_found={cycle.enrichment_xml_not_found}, "
            f"enqueue_http_failed={cycle.enqueue_http_failed}, "
            f"enqueue_parse_failed={cycle.enqueue_parse_failed}, "
            f"enqueue_market_failed={cycle.enqueue_market_failed}, "
            f"outbox_notified={cycle.outbox_notified})"
        )
        typer.echo(cycle_message)
        append_process_log(output_log_path, cycle_message)
        _heartbeat("cycle_succeeded", cycle_succeeded=True)
        return cycle

    def _run_cycle_with_recovery(*, loop_mode: bool) -> AutoPilotCycleResult | None:
        try:
            return _run_cycle()
        except (SecHttpError, SecRssParseError) as exc:
            _heartbeat("cycle_retryable_failure", error=exc)
            failure_message = f"ops autopilot cycle failed (retryable, {type(exc).__name__}: {exc})"
            typer.secho(
                failure_message,
                fg=typer.colors.RED,
                err=True,
            )
            append_process_log(error_log_path, failure_message)
            if loop_mode:
                return None
            raise typer.Exit(code=1) from exc

    def _source_changed_during_wait(startup_fingerprint: str) -> bool:
        remaining = float(interval)
        while remaining > 0:
            _heartbeat("cycle_wait")
            if runtime_source_fingerprint() != startup_fingerprint:
                return True
            sleep_seconds = min(SOURCE_REVISION_CHECK_INTERVAL_SECONDS, remaining)
            time.sleep(sleep_seconds)
            remaining -= sleep_seconds
        return runtime_source_fingerprint() != startup_fingerprint

    try:
        startup_fingerprint: str | None = None
        if health_store is not None:
            if not once:
                ensure_kill_on_close_process_tree()
            startup_fingerprint = runtime_source_fingerprint()
            health_store.register_runtime(
                runtime_id=runtime_id,
                source_fingerprint=startup_fingerprint,
                now=datetime.now(UTC),
            )
        if once:
            _run_cycle_with_recovery(loop_mode=False)
            return
        if startup_fingerprint is None:
            startup_fingerprint = runtime_source_fingerprint()
        _run_cycle_with_recovery(loop_mode=True)
        while True:
            if _source_changed_during_wait(startup_fingerprint):
                _heartbeat("source_changed")
                source_message = (
                    "autopilot source changed; exiting so the hidden watchdog "
                    "can start a fresh worker"
                )
                typer.secho(source_message, fg=typer.colors.YELLOW, err=True)
                append_process_log(output_log_path, source_message)
                return
            _run_cycle_with_recovery(loop_mode=True)
    except typer.Exit:
        raise
    except RuntimeOwnershipError as exc:
        failure_message = f"autopilot process stopped ({type(exc).__name__}: {exc})"
        append_process_log(error_log_path, failure_message)
        raise
    except Exception as exc:
        try:
            _heartbeat("process_failure", error=exc)
        except Exception as heartbeat_exc:
            append_process_log(
                error_log_path,
                "autopilot terminal heartbeat failed "
                f"({type(heartbeat_exc).__name__}: {heartbeat_exc})",
            )
        failure_message = f"autopilot process failed ({type(exc).__name__}: {exc})"
        append_process_log(error_log_path, failure_message)
        raise


@ops_app.command("signal-study")
def ops_signal_study(
    database_path: Path | None = typer.Option(  # noqa: B008
        None,
        "--database-path",
        help="SQLite snapshot to analyze. Defaults to DATABASE_PATH.",
    ),
    cohort: str = typer.Option(
        "live",
        "--cohort",
        help="live or historical-replay",
    ),
    start_date_text: str = typer.Option("2026-02-11", "--start-date"),
    end_date_text: str = typer.Option("2026-08-17", "--end-date"),
    bootstrap_iterations: int = typer.Option(10_000, "--bootstrap-iterations", min=100),
    refresh_companyfacts_enabled: bool = typer.Option(
        False,
        "--refresh-companyfacts/--no-refresh-companyfacts",
    ),
    refresh_intraday_enabled: bool = typer.Option(
        False,
        "--refresh-intraday/--no-refresh-intraday",
        help="Fetch missing one-minute RTH sessions from the local IB Gateway.",
    ),
    ib_client_id: int = typer.Option(172, "--ib-client-id", min=1),
    matched_control_iterations: int = typer.Option(
        0,
        "--matched-control-iterations",
        min=0,
        help="Optional same-symbol random-date falsification iterations for E07.",
    ),
    output_json_path: Path | None = typer.Option(  # noqa: B008
        None,
        "--output-json",
    ),
) -> None:
    """Evaluate the preregistered strategy family on delivered live approvals."""

    try:
        start_date = date.fromisoformat(start_date_text)
        end_date = date.fromisoformat(end_date_text)
    except ValueError as exc:
        typer.secho("invalid date format; expected YYYY-MM-DD", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc
    if start_date > end_date:
        typer.secho("start-date cannot be after end-date", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2)
    settings = get_settings()
    selected_db = database_path or Path(settings.database_path)
    normalized_cohort = cohort.strip().lower()
    if normalized_cohort == "live":
        signals = load_delivered_signals(
            str(selected_db),
            start_date=start_date,
            end_date=end_date,
        )
    elif normalized_cohort == "historical-replay":
        signals = load_historical_approved_replay(
            str(selected_db),
            start_date=start_date,
            end_date=end_date,
        )
    else:
        typer.secho("cohort must be live or historical-replay", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2)
    price_start = start_date - timedelta(days=120)
    price_end = end_date + timedelta(days=40)
    symbols = sorted({signal.symbol for signal in signals} | {"SPY"})
    bars_by_symbol = {
        symbol: get_price_bars(
            str(selected_db),
            symbol=symbol,
            start_date=price_start,
            end_date=price_end,
        )
        for symbol in symbols
    }
    companyfacts_refresh: dict[str, object] = {
        "requested": 0,
        "fetched": 0,
        "reused": 0,
        "errors": [],
    }
    if refresh_companyfacts_enabled:
        companyfacts_refresh = refresh_companyfacts(
            str(selected_db),
            ciks=[signal.cik for signal in signals],
            client=SecHttpClient(settings=settings),
        )
    cached_companyfacts = load_cached_companyfacts(str(selected_db))
    market_caps: dict[str, float] = {}
    for signal in signals:
        normalized_cik = "".join(char for char in signal.cik if char.isdigit()).zfill(10)
        payload = cached_companyfacts.get(normalized_cik)
        shares = (
            shares_outstanding_as_of(payload, as_of=signal.signal_at.date())
            if payload is not None
            else None
        )
        if shares is None:
            continue
        features = compute_point_in_time_features(
            signal,
            symbol_bars=bars_by_symbol.get(signal.symbol, ()),
            benchmark_bars=bars_by_symbol.get("SPY", ()),
        )
        if features.prior_close is not None:
            market_caps[signal.packet_id] = shares * features.prior_close
    intraday_requests = (
        build_intraday_requests(
            signals,
            benchmark_daily_bars=bars_by_symbol.get("SPY", ()),
        )
        if normalized_cohort == "live"
        else []
    )
    intraday_refresh: dict[str, object] = {
        "requested": len(intraday_requests),
        "fetched": 0,
        "reused": 0,
        "errors": [],
    }
    if refresh_intraday_enabled and normalized_cohort == "live":
        intraday_refresh = refresh_ibkr_minute_bars(
            str(selected_db),
            requests=intraday_requests,
            client_id=ib_client_id,
        )
    minute_bars_by_symbol = (
        {symbol: get_minute_bars(str(selected_db), symbol=symbol) for symbol in symbols}
        if normalized_cohort == "live"
        else {}
    )
    cached_intraday_pairs = completed_minute_bar_sessions(
        str(selected_db),
        requests=intraday_requests,
    )
    verified_minute_bars_by_symbol = filter_completed_minute_bars(
        minute_bars_by_symbol,
        completed_sessions=cached_intraday_pairs,
    )
    minute_bars_payload = (
        verified_minute_bars_by_symbol
        if any(verified_minute_bars_by_symbol.values())
        else None
    )
    report = evaluate_daily_hypothesis_family(
        signals,
        bars_by_symbol=bars_by_symbol,
        bootstrap_iterations=bootstrap_iterations,
        market_caps=market_caps,
        minute_bars_by_symbol=minute_bars_payload,
        robustness_split_date=(
            date(2026, 7, 1)
            if normalized_cohort == "live"
            else start_date + timedelta(days=(end_date - start_date).days // 2)
        ),
    )
    candidate_diagnostics: dict[str, object] = {}
    for candidate_filter in ("F00", "F06"):
        candidate_id = f"E07|{candidate_filter}"
        candidate_observations = collect_daily_strategy_observations(
            signals,
            rule=DAILY_EXECUTION_RULES[6],
            filter_id=candidate_filter,
            bars_by_symbol=bars_by_symbol,
            market_caps=market_caps,
        )
        candidate_block: dict[str, object] = {
            "portfolio_20_slots": fixed_slot_portfolio_summary(
                candidate_observations,
                slots=20,
            )
        }
        if matched_control_iterations > 0:
            candidate_block["same_symbol_random_date_control"] = matched_random_date_control(
                signals,
                rule=DAILY_EXECUTION_RULES[6],
                filter_id=candidate_filter,
                bars_by_symbol=bars_by_symbol,
                study_start=start_date,
                study_end=end_date,
                iterations=matched_control_iterations,
            )
        candidate_diagnostics[candidate_id] = candidate_block
    report.update(
        {
            "run_timestamp_utc": datetime.now(UTC).isoformat(),
            "cohort": normalized_cohort,
            "database_path": str(selected_db.resolve()),
            "database_sha256": _file_sha256(selected_db) if selected_db.is_file() else None,
            "database_size_bytes": selected_db.stat().st_size if selected_db.is_file() else None,
            "requested_start_date": start_date.isoformat(),
            "requested_end_date": end_date.isoformat(),
            "symbols_requested": len(symbols),
            "symbols_with_prices": sum(bool(bars) for bars in bars_by_symbol.values()),
            "companyfacts_refresh": companyfacts_refresh,
            "point_in_time_market_cap_coverage": (
                len(market_caps) / len(signals) if signals else 0.0
            ),
            "intraday_refresh": intraday_refresh,
            "intraday_cached_request_coverage": {
                "requested": len(intraday_requests),
                "covered": sum(request in cached_intraday_pairs for request in intraday_requests),
                "coverage_rate": (
                    sum(request in cached_intraday_pairs for request in intraday_requests)
                    / len(intraday_requests)
                    if intraday_requests
                    else None
                ),
            },
            "candidate_diagnostics": candidate_diagnostics,
            "preregistration": "docs/research/SIGNAL-STUDY-2026-08-17-PREREG.md",
        }
    )
    if output_json_path is not None:
        output_json_path.parent.mkdir(parents=True, exist_ok=True)
        output_json_path.write_text(
            json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n",
            encoding="utf-8",
        )
    typer.echo(
        json.dumps(
            {
                "conclusion": report["conclusion"],
                "signal_count": report["signal_count"],
                "surviving_hypotheses": report["surviving_hypotheses"],
                "daily_execution_coverage": report["daily_execution_coverage"],
                "output_json": str(output_json_path) if output_json_path else None,
            },
            indent=2,
        )
    )


@ops_app.command("live-canary")
def ops_live_canary(
    database_path: Path | None = typer.Option(  # noqa: B008
        None,
        "--database-path",
        help="Live insider-alerts SQLite database. Defaults to DATABASE_PATH.",
    ),
    ledger_path: Path = typer.Option(  # noqa: B008
        Path("data/live_canary.db"),
        "--ledger-path",
        help="Separate append-oriented canary ledger.",
    ),
    host: str = typer.Option("127.0.0.1", "--host"),
    port: int = typer.Option(4001, "--port", min=1, max=65535),
    client_id: int = typer.Option(173, "--client-id", min=1),
    account: str | None = typer.Option(
        None,
        "--account",
        help="Required if the Gateway login exposes more than one account.",
    ),
    live: bool = typer.Option(
        False,
        "--live/--shadow-only",
        help="Permit live orders only when INSIDER_LIVE_ARM also exactly matches the arm phrase.",
    ),
    arm_phrase_option: str | None = typer.Option(
        None,
        "--arm-phrase",
        hidden=True,
    ),
    loop: bool = typer.Option(False, "--loop/--once"),
    interval: int = typer.Option(60, "--interval", min=15),
    notify: bool = typer.Option(False, "--notify/--no-notify"),
    invalid_commission_handling: str = typer.Option(
        "reject",
        "--invalid-commission-handling",
        help=(
            "How to handle previews with neither an exact commission nor a valid IBKR "
            "range: reject (default) or fallback_to_cap."
        ),
    ),
    output_log_path: Path | None = typer.Option(  # noqa: B008
        None, "--output-log", hidden=True
    ),
    error_log_path: Path | None = typer.Option(  # noqa: B008
        None, "--error-log", hidden=True
    ),
) -> None:
    """Run the prospective E07/F00 shadow book and the capped IBKR live canary."""

    settings = get_settings()
    selected_db = database_path or Path(settings.database_path)
    arm_phrase = arm_phrase_option or os.environ.get("INSIDER_LIVE_ARM", "")
    config = CanaryConfig(
        source_db=str(selected_db),
        ledger_db=str(ledger_path),
        host=host,
        port=port,
        client_id=client_id,
        account=account,
        live_requested=live,
        arm_phrase=arm_phrase,
        poll_seconds=interval,
        invalid_commission_handling=cast(
            Literal["fallback_to_cap", "reject"], invalid_commission_handling
        ),
    )
    if live and arm_phrase != ARM_PHRASE:
        typer.secho(
            "live requested but INSIDER_LIVE_ARM is absent or incorrect; running shadow-only",
            fg=typer.colors.YELLOW,
            err=True,
        )
    runner = CanaryRunner(
        config,
        IbkrBroker(host=host, port=port, client_id=client_id, account=account),
    )
    notifier = NtfyNotifier(settings) if notify else None

    def append_process_log(path: Path | None, message: str) -> None:
        if path is None:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(message.rstrip() + "\n")

    async def run() -> None:
        try:
            while True:
                if runner.source_revision_changed():
                    message = (
                        "live canary source changed; exiting so the hidden watchdog "
                        "can restart the worker"
                    )
                    typer.secho(message, fg=typer.colors.YELLOW, err=True)
                    append_process_log(error_log_path, message)
                    return
                try:
                    result = await runner.cycle(disconnect_after=not loop)
                    result_line = json.dumps(asdict(result), sort_keys=True)
                    typer.echo(result_line)
                    append_process_log(output_log_path, result_line)
                    if notifier is not None and any(
                        (
                            result.live_submitted,
                            result.live_opened,
                            result.live_closed,
                        )
                    ):
                        try:
                            notifier.send(
                                "IBKR insider canary activity",
                                (
                                    f"submitted={result.live_submitted}, "
                                    f"opened={result.live_opened}, "
                                    f"closed={result.live_closed}; "
                                    f"gate={result.live_gate}"
                                ),
                                tags=["chart_with_upwards_trend"],
                                priority=4,
                            )
                        except NtfyNotificationError as exc:
                            typer.secho(
                                f"canary notification failed: {exc}",
                                fg=typer.colors.YELLOW,
                                err=True,
                            )
                            append_process_log(
                                error_log_path,
                                f"canary notification failed: {exc}",
                            )
                except (IbkrExecutionError, OSError, sqlite3.Error, ValueError) as exc:
                    typer.secho(
                        f"live canary cycle failed closed: {exc}",
                        fg=typer.colors.RED,
                        err=True,
                    )
                    append_process_log(error_log_path, f"live canary cycle failed closed: {exc}")
                    if not loop:
                        raise typer.Exit(code=1) from exc
                if not loop:
                    return
                await asyncio.sleep(poll_delay_seconds(config, datetime.now(UTC)))
        finally:
            runner.broker.disconnect()

    asyncio.run(run())


@ops_app.command("live-canary-status")
def ops_live_canary_status(
    ledger_path: Path = typer.Option(  # noqa: B008
        Path("data/live_canary.db"),
        "--ledger-path",
    ),
) -> None:
    """Show canary state without connecting to IBKR or changing broker state."""

    typer.echo(json.dumps(live_canary_status_report(str(ledger_path)), indent=2, sort_keys=True))


@ops_app.command("notification-journal-activate")
def ops_notification_journal_activate(
    activation_at_utc: str = typer.Option(..., "--activation-at-utc"),
) -> None:
    """Seal the capture-only ntfy transport boundary without sending a message."""

    try:
        activated_at = datetime.fromisoformat(activation_at_utc.replace("Z", "+00:00"))
    except ValueError as exc:
        raise typer.BadParameter("activation timestamp must be ISO-8601") from exc
    if activated_at.tzinfo is None:
        raise typer.BadParameter("activation timestamp must include a UTC offset")
    settings = get_settings()
    result = activate_notification_journal(
        _notification_transport_config(settings),
        activated_at_utc=activated_at.astimezone(UTC),
    )
    typer.echo(json.dumps(result, indent=2, sort_keys=True))


@ops_app.command("notification-journal-status")
def ops_notification_journal_status() -> None:
    """Validate capture-only ntfy transport custody without sending a message."""

    settings = get_settings()
    typer.echo(
        json.dumps(
            notification_journal_status(_notification_transport_config(settings)),
            indent=2,
            sort_keys=True,
        )
    )


@ops_app.command("research-capture-status")
def ops_research_capture_status(
    database_path: Path | None = typer.Option(None, "--database-path"),  # noqa: B008
    evidence_db: Path = typer.Option(  # noqa: B008
        Path("data/research/evidence.db"), "--evidence-db"
    ),
) -> None:
    """Show blinded capture health and counts; never expose outcomes."""

    settings = get_settings()
    selected_db = database_path or Path(settings.database_path)
    typer.echo(json.dumps(capture_status(selected_db, evidence_db), indent=2, sort_keys=True))


@ops_app.command("research-bar-feed-status")
def ops_research_bar_feed_status(
    feed_db: Path = typer.Option(  # noqa: B008
        Path("data/research/bar_feed.db"), "--feed-db"
    ),
) -> None:
    """Show completed-bar feed health and counts without connecting to IBKR."""

    report = bar_feed_status(feed_db)
    typer.echo(json.dumps(report, indent=2, sort_keys=True))
    if report.get("integrity_status") != "valid":
        raise typer.Exit(code=3)


@ops_app.command("research-session-feed-status")
def ops_research_session_feed_status(
    feed_db: Path = typer.Option(  # noqa: B008
        Path("data/research/session_feed.db"), "--feed-db"
    ),
) -> None:
    """Show exchange-session feed health without connecting to IBKR."""

    report = session_feed_status(feed_db)
    typer.echo(json.dumps(report, indent=2, sort_keys=True))
    if report.get("integrity_status") != "valid":
        raise typer.Exit(code=3)


@ops_app.command("research-trial-status")
def ops_research_trial_status(
    trial_db: Path = typer.Option(  # noqa: B008
        Path("data/research/trial.db"), "--trial-db"
    ),
) -> None:
    """Show blinded prospective-trial state without reading outcome values."""

    report = trial_runtime_status(trial_db)
    typer.echo(json.dumps(report, indent=2, sort_keys=True))
    if report.get("integrity_status") != "valid":
        raise typer.Exit(code=3)


@ops_app.command("research-diagnostics-status")
def ops_research_diagnostics_status(
    diagnostics_db: Path = typer.Option(  # noqa: B008
        Path("data/research/diagnostics.db"), "--diagnostics-db"
    ),
) -> None:
    """Show isolated control-diagnostic health without reading return values."""

    report = diagnostic_status(diagnostics_db)
    typer.echo(json.dumps(report, indent=2, sort_keys=True))
    if (
        report.get("integrity_status") != "valid"
        or report.get("operational_status") != "healthy"
    ):
        raise typer.Exit(code=3)


@ops_app.command("live-canary-watchdog")
def ops_live_canary_watchdog(
    worker_task_name: str = typer.Option(..., "--worker-task-name"),
    ledger_path: Path = typer.Option(Path("data/live_canary.db"), "--ledger-path"),  # noqa: B008
    stale_seconds: int = typer.Option(120, "--stale-seconds", min=30),
    output_log_path: Path = typer.Option(  # noqa: B008
        Path("logs/live-canary-watchdog.log"),
        "--output-log",
    ),
) -> None:
    """Restart the hidden canary worker when its durable heartbeat is stale."""

    result = run_scheduled_task_watchdog(
        ledger_db=str(ledger_path),
        worker_task_name=worker_task_name,
        stale_seconds=stale_seconds,
    )
    append_watchdog_log(output_log_path, result)


@ops_app.command("autopilot-watchdog")
def ops_autopilot_watchdog(
    worker_task_name: str = typer.Option(..., "--worker-task-name"),
    heartbeat_db: Path = typer.Option(  # noqa: B008
        Path("data/autopilot_health.db"),
        "--heartbeat-db",
    ),
    stale_seconds: int | None = typer.Option(None, "--stale-seconds", min=300),
    quant_timeout_seconds: int = typer.Option(
        120,
        "--quant-timeout-seconds",
        min=10,
        max=900,
    ),
    output_log_path: Path = typer.Option(  # noqa: B008
        Path("logs/autopilot-watchdog.log"),
        "--output-log",
    ),
) -> None:
    """Restart the hidden autopilot worker when its durable progress is stale."""

    try:
        settings = get_settings()
        budget = autopilot_runtime_budget(
            settings=settings,
            quant_timeout_seconds=quant_timeout_seconds,
        )
        effective_stale_seconds = (
            stale_seconds
            if stale_seconds is not None
            else int(budget["required_stale_seconds"])
        )
        validate_stale_threshold(
            quant_timeout_seconds=quant_timeout_seconds,
            stale_seconds=effective_stale_seconds,
            settings=settings,
        )
        result = run_autopilot_watchdog(
            heartbeat_db=heartbeat_db,
            worker_task_name=worker_task_name,
            stale_seconds=effective_stale_seconds,
        )
    except Exception as exc:
        failure: dict[str, object] = {
            "checked_at_utc": datetime.now(UTC).isoformat(),
            "worker_task_name": worker_task_name,
            "action": "error",
            "error_kind": type(exc).__name__,
            "error_message": str(exc)[:1000],
        }
        with contextlib.suppress(OSError):
            append_watchdog_log(output_log_path, failure)
        raise
    append_watchdog_log(output_log_path, result)


@ops_app.command("sec-ingestion-watchdog")
def ops_sec_ingestion_watchdog(
    worker_task_name: str = typer.Option(..., "--worker-task-name"),
    heartbeat_db: Path = typer.Option(  # noqa: B008
        Path("data/sec_ingestion_health.db"),
        "--heartbeat-db",
    ),
    stale_seconds: int | None = typer.Option(None, "--stale-seconds", min=300),
    output_log_path: Path = typer.Option(  # noqa: B008
        Path("logs/sec-ingestion-watchdog.log"),
        "--output-log",
    ),
) -> None:
    """Restart the hidden SEC ingestion worker when durable progress is stale."""

    try:
        settings = get_settings()
        budget = sec_ingestion_runtime_budget(settings=settings)
        effective_stale_seconds = (
            stale_seconds
            if stale_seconds is not None
            else int(budget["required_stale_seconds"])
        )
        validate_sec_ingestion_stale_threshold(
            stale_seconds=effective_stale_seconds,
            settings=settings,
        )
        result = run_autopilot_watchdog(
            heartbeat_db=heartbeat_db,
            worker_task_name=worker_task_name,
            stale_seconds=effective_stale_seconds,
        )
    except Exception as exc:
        failure: dict[str, object] = {
            "checked_at_utc": datetime.now(UTC).isoformat(),
            "worker_task_name": worker_task_name,
            "action": "error",
            "error_kind": type(exc).__name__,
            "error_message": str(exc)[:1000],
        }
        with contextlib.suppress(OSError):
            append_watchdog_log(output_log_path, failure)
        raise
    append_watchdog_log(output_log_path, result)


@ops_app.command("sec-ingestion-config-validate")
def ops_sec_ingestion_config_validate(
    heartbeat_stale_seconds: int | None = typer.Option(
        None,
        "--heartbeat-stale-seconds",
        min=300,
    ),
) -> None:
    """Preflight SEC ingestion watchdog budgets and process-tree ownership."""

    settings = get_settings()
    try:
        budget = sec_ingestion_runtime_budget(settings=settings)
        effective_stale_seconds = (
            heartbeat_stale_seconds
            if heartbeat_stale_seconds is not None
            else int(budget["required_stale_seconds"])
        )
        validate_sec_ingestion_stale_threshold(
            stale_seconds=effective_stale_seconds,
            settings=settings,
        )
        ensure_kill_on_close_process_tree()
    except (RuntimeError, ValueError) as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc
    typer.echo(
        json.dumps(
            {**budget, "effective_stale_seconds": effective_stale_seconds},
            sort_keys=True,
        )
    )


@ops_app.command("sec-ingestion-health-status")
def ops_sec_ingestion_health_status(
    heartbeat_db: Path = typer.Option(  # noqa: B008
        Path("data/sec_ingestion_health.db"),
        "--heartbeat-db",
    ),
) -> None:
    """Show bounded SEC ingestion liveness metadata without filing payloads."""

    report = autopilot_health_status(heartbeat_db)
    typer.echo(json.dumps(report, indent=2, sort_keys=True))
    if not bool(report.get("valid", False)):
        raise typer.Exit(code=3)


@ops_app.command("autopilot-config-validate")
def ops_autopilot_config_validate(
    quant_timeout_seconds: int = typer.Option(120, "--quant-timeout-seconds", min=10, max=900),
    heartbeat_stale_seconds: int | None = typer.Option(
        None,
        "--heartbeat-stale-seconds",
        min=300,
    ),
) -> None:
    """Preflight watchdog budgets and Windows descendant-tree ownership."""

    settings = get_settings()
    try:
        budget = autopilot_runtime_budget(
            settings=settings,
            quant_timeout_seconds=quant_timeout_seconds,
        )
        effective_stale_seconds = (
            heartbeat_stale_seconds
            if heartbeat_stale_seconds is not None
            else int(budget["required_stale_seconds"])
        )
        validate_stale_threshold(
            quant_timeout_seconds=quant_timeout_seconds,
            stale_seconds=effective_stale_seconds,
            settings=settings,
        )
        ensure_kill_on_close_process_tree()
    except (RuntimeError, ValueError) as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc
    typer.echo(
        json.dumps(
            {**budget, "effective_stale_seconds": effective_stale_seconds},
            sort_keys=True,
        )
    )


@ops_app.command("autopilot-health-status")
def ops_autopilot_health_status(
    heartbeat_db: Path = typer.Option(  # noqa: B008
        Path("data/autopilot_health.db"),
        "--heartbeat-db",
    ),
) -> None:
    """Show bounded autopilot liveness metadata without signal payloads."""

    report = autopilot_health_status(heartbeat_db)
    typer.echo(json.dumps(report, indent=2, sort_keys=True))
    if not bool(report.get("valid", False)):
        raise typer.Exit(code=3)


if __name__ == "__main__":
    try:
        app()
    except Exception as exc:
        # Typer enters the command only after settings and argument conversion succeed. The
        # scheduled worker uses pythonw, so preserve failures that occur before its inner logging
        # scope as well as unexpected top-level failures.
        if len(sys.argv) >= 3 and sys.argv[1:3] in (
            ["ops", "autopilot"],
            ["ops", "sec-ingestion"],
        ):
            try:
                error_index = sys.argv.index("--error-log") + 1
                error_path = Path(sys.argv[error_index])
                error_path.parent.mkdir(parents=True, exist_ok=True)
                with error_path.open("a", encoding="utf-8") as handle:
                    handle.write(
                        f"{sys.argv[2]} unhandled process failure "
                        f"({type(exc).__name__}: {exc})\n"
                    )
            except (OSError, ValueError, IndexError):
                pass
        raise
