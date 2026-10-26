"""Replayable input-quality report, with frozen dates and explicit missingness."""

from __future__ import annotations

from collections import Counter
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

import rfc8785

from insider_alerts.research import calendar_v2 as calendar
from insider_alerts.research.commissioning_journal import digest
from insider_alerts.research.commissioning_planner import Evidence
from insider_alerts.research.commissioning_receipts import qualified_identity
from insider_alerts.research.commissioning_source import timestamp
from insider_alerts.research.commissioning_window import parse


def frozen_calendar(
    versions: tuple[calendar.Version, ...], frozen: dict[str, Any], now: datetime
) -> calendar.Version:
    bound = next((v for v in versions if v.digest == frozen["calendar_version"]), None)
    if bound is None or bound.observed_at > min(now, parse(frozen["created_at"])):
        raise ValueError("frozen calendar version unavailable at required observation time")
    return bound


def report(
    records: list[dict[str, Any]], versions: tuple[calendar.Version, ...], *, now: datetime
) -> dict[str, Any]:
    frozen = records[0]["payload"]
    bound = frozen_calendar(versions, frozen, now)
    evidence = Evidence(records, bound, now)
    _, result = evidence.jobs_and_report()
    latest = next(v for v in reversed(versions) if v.observed_at <= now)
    start = date.fromisoformat(frozen["earliest_history"])
    end = date.fromisoformat(frozen["sessions"][-1]["date"])
    count = sum(start <= d.day <= end and d.state == "open" for d in bound.days)
    conflicts = calendar.binding_conflicts(
        calendar.bind(bound, start, count), latest, bound_version=bound
    )
    result["calendar"] = {
        "frozen_version": bound.digest,
        "latest_known_version": latest.digest,
        "latest_observed_at": timestamp(latest.observed_at),
        "conflicted_dates": [str(day) for day in conflicts],
        "extraordinary_closure_freshness_verified": False,
    }
    first_custody: dict[str, dict[str, Any]] = {}
    for record in records:
        if record["kind"] == "custody":
            first_custody.setdefault(record["entity"], record["payload"])
    for case in result["cases"]:
        admission = first_custody[case["case_id"]]["admission"]
        span = admission.get("history_dates", []) + admission.get("horizon_dates", [])
        case["calendar_conflicted_dates"] = [
            str(day) for day in conflicts if span and min(span) <= str(day) <= max(span)
        ]
        owner = "case:" + case["case_id"]
        relevant = [
            (attempt, record)
            for attempt, record, _ in evidence.responses
            if any(key.startswith(owner + "/") for key in attempt["payload"]["jobs"])
        ]
        case["pin_source_receipt"] = next(
            (r["sha256"] for a, r in relevant if qualified_identity(a, r) is not None), None
        )
        case["symbol_resolution_events"] = [
            {
                "receipt_sha256": r["sha256"],
                "resolution": r["payload"].get("symbol_resolution"),
                "changed": r["payload"].get("symbol_resolution_changed", False),
            }
            for _, r in relevant
            if r["payload"].get("symbol_resolution") is not None
        ]
    inventory = frozen["preexisting_inventory"]
    poll_times = [parse(inventory["observed_at"])] + [
        parse(r["payload"]["source_observed_at"]) for r in records if r["kind"] == "custody_poll"
    ]
    poll_times.append(min(now, evidence.deadline))
    if poll_times != sorted(poll_times):
        raise ValueError("custody observation clocks reversed")
    result["custody_continuity"] = {
        "recorded_poll_resolution_seconds": 300,
        "maximum_recorded_gap_seconds": max(
            ((b - a).total_seconds() for a, b in zip(poll_times, poll_times[1:], strict=False)),
            default=0,
        ),
        "last_source_observed_at": timestamp(poll_times[-2]),
        "poll_count": len(poll_times) - 2,
    }
    versions_seen: dict[str, dict[str, Any]] = {}
    notices: Counter[str] = Counter()
    for _, receipt, _ in evidence.responses:
        source = receipt["payload"]
        identity = {key: source.get(key) for key in ("sdk_version", "server_version")}
        versions_seen[digest(rfc8785.dumps(identity))] = identity
        notices.update(str(n["code"]) for n in source.get("notices", []))
    result["source_versions"] = [versions_seen[key] for key in sorted(versions_seen)]
    result["source_notice_counts"] = dict(sorted(notices.items()))
    result["broker_error_code_counts"] = dict(
        sorted(
            Counter(
                str(r["payload"]["error_code"])
                for _, r, _ in evidence.responses
                if r["payload"].get("error_code") is not None
            ).items()
        )
    )
    result["receipt_date_issues"] = [
        {
            "receipt_sha256": r["sha256"],
            "dates": parsed["date_issues"],
            "unassigned_rows": parsed["unassigned_row_issues"],
        }
        for _, r, parsed in evidence.responses
        if parsed["date_issues"] or parsed["unassigned_row_issues"]
    ]
    result["overnight_capacity"] = _capacity(evidence)
    result["last_cycle"] = next(
        (
            r["payload"] | {"recorded_at": r["recorded_at"]}
            for r in reversed(records)
            if r["kind"] == "cycle"
        ),
        None,
    )
    result["unassignable_reasons"] = dict(
        sorted(
            Counter(
                payload["admission"]["reason"]
                for payload in first_custody.values()
                if payload["admission"]["membership"] == "unassignable"
            ).items()
        )
    )
    return result


def _capacity(evidence: Evidence) -> list[dict[str, Any]]:
    boundary = parse(evidence.frozen["boundary"])
    end = min(evidence.now, evidence.deadline)
    day = boundary.astimezone(calendar.NY).date() - timedelta(days=1)
    windows = []
    while day <= end.astimezone(calendar.NY).date():
        start_at = max(boundary, datetime.combine(day, time(18), calendar.NY).astimezone(UTC))
        end_at = min(
            end, datetime.combine(day + timedelta(days=1), time(8), calendar.NY).astimezone(UTC)
        )
        if start_at < end_at:
            attempts = [
                a
                for a in evidence.attempts.values()
                if start_at <= parse(a["recorded_at"]) < end_at
            ]
            outcomes = Counter(
                str(evidence.source_responses.get(a["sha256"], {}).get("result", "no_response"))
                for a in attempts
            )
            windows.append(
                {
                    "night": str(day),
                    "elapsed_window_seconds": (end_at - start_at).total_seconds(),
                    "attempts": len(attempts),
                    "response_results": dict(sorted(outcomes.items())),
                    "unused_capacity_cause_not_inferred": True,
                }
            )
        day += timedelta(days=1)
    return windows
