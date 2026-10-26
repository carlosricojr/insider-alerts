"""Deterministic, outcome-blind request planning and descriptive coverage."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from typing import Any

from insider_alerts.research import calendar_v2 as calendar
from insider_alerts.research.commissioning_receipts import (
    covers,
    inspect_receipt,
    missing_dates,
    qualified_identity,
    stable,
)
from insider_alerts.research.commissioning_source import timestamp
from insider_alerts.research.commissioning_window import parse, validate_manifest


def infrastructure_failure(source: dict[str, Any] | None) -> bool:
    if source is None:
        return True  # Includes an in-flight attempt; the network lock prevents a second caller.
    if source.get("cleanup_errors") or source.get("clock_integrity") != "valid":
        return True
    if source.get("result") in {
        "received",
        "empty_response",
        "contract_missing",
        "contract_ambiguous",
        "contract_not_supported",
        "pinned_contract_not_returned",
    }:
        return False
    if source.get("result") == "broker_error":
        if source.get("error_code") in {200, 354}:
            return False
        if source.get("error_category") in {"hmds_162_no_data", "hmds_162_permissions"}:
            return False
    return True


@dataclass(frozen=True)
class Job:
    owner: str
    stage: str
    symbol: str
    dates: tuple[str, ...]
    ready: datetime
    deadline: datetime
    conid: int | None
    schedule: bool = False

    @property
    def key(self) -> str:
        return f"{self.owner}/{self.stage}"


class Evidence:
    def __init__(self, records: list[dict[str, Any]], version: calendar.Version, now: datetime):
        self.records, self.version, self.now = records, version, now
        timestamp(now)
        self.frozen = records[0]["payload"]
        validate_manifest(self.frozen, version)
        if any(parse(record["recorded_at"]) > now for record in records):
            raise ValueError("journal observations exceed current clock")
        self.deadline = parse(self.frozen["recovery_deadline"])
        self.attempts = {r["sha256"]: r for r in records if r["kind"] == "attempt"}
        self.responses: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]] = []
        for record in records:
            if record["kind"] == "response":
                attempt = self.attempts[record["entity"]]
                self.responses.append((attempt, record, inspect_receipt(attempt, record, version)))
        self.source_responses = {
            attempt["sha256"]: record["payload"] for attempt, record, _ in self.responses
        }

    def infrastructure_retry_at(self) -> datetime | None:
        trailing = 0
        attempts = list(self.attempts.values())
        for attempt in reversed(attempts):
            if not infrastructure_failure(self.source_responses.get(attempt["sha256"])):
                break
            trailing += 1
        if not trailing:
            return None
        delay = min(1800, 300 * 2 ** min(trailing - 1, 10))
        return parse(attempts[-1]["recorded_at"]) + timedelta(seconds=delay)

    def pin(self, owner: str) -> int | None:
        for attempt, record, _ in self.responses:
            if any(key.startswith(owner + "/") for key in attempt["payload"]["jobs"]):
                conid = qualified_identity(attempt, record)
                if conid is not None:
                    return conid
        return None

    def attempts_for(self, job: Job) -> list[dict[str, Any]]:
        return [a for a in self.attempts.values() if job.key in a["payload"]["jobs"]]

    def success(self, job: Job) -> dict[str, Any] | None:
        # First complete receipt is frozen. Never search for a later pair that
        # happens to agree after seeing a revision in the stability repeat.
        return next(
            (
                receipt
                for attempt, record, receipt in self.responses
                if (
                    job.key in attempt["payload"]["jobs"]
                    and job.ready <= parse(attempt["recorded_at"]) < self.deadline
                    and receipt["response_at"] is not None
                    and parse(receipt["response_at"]) < self.deadline
                    and parse(record["recorded_at"]) < self.deadline
                    and receipt["conid"] == job.conid
                    and covers(receipt, list(job.dates), schedule=job.schedule)
                )
            ),
            None,
        )

    def repeat_after(self, received: datetime) -> datetime | None:
        day = received.astimezone(calendar.NY).date()
        for offset in range(32):
            current = self.version.day(day + timedelta(days=offset))
            if current.state == "unknown":
                return None
            if current.closes_at and current.closes_at > received:
                return current.closes_at
        return None

    def details(self, job: Job) -> dict[str, Any]:
        complete = self.success(job)
        latest = next(
            (
                receipt
                for attempt, _, receipt in reversed(self.responses)
                if job.key in attempt["payload"]["jobs"]
            ),
            None,
        )
        selected = complete or latest
        lags = {}
        if selected and selected["response_at"]:
            for day in job.dates:
                close = self.version.day(date.fromisoformat(day)).closes_at
                if close and covers(selected, [day], schedule=job.schedule):
                    lags[day] = (parse(selected["response_at"]) - close).total_seconds()
        return {
            "ready_at": timestamp(job.ready),
            "attempts": len(self.attempts_for(job)),
            "complete": complete is not None,
            "missing_dates": missing_dates(selected, list(job.dates), schedule=job.schedule)
            if selected
            else {
                day: ["no_response_yet" if self.attempts_for(job) else "not_requested_yet"]
                for day in job.dates
            },
            "diagnostic_receipt_sha256": selected["receipt_sha256"] if selected else None,
            "receipt_lag_after_session_close_seconds": lags,
        }

    def stages(
        self,
        owner: str,
        symbol: str,
        history: list[str],
        horizon: list[str],
        observed_at: datetime,
        decision_cutoff: datetime,
    ) -> tuple[list[Job], dict[str, Any]]:
        conid = self.pin(owner)
        close = self.version.day(date.fromisoformat(horizon[-1])).closes_at
        if close is None:
            raise ValueError("unknown horizon close")
        initial = Job(owner, "R1", symbol, tuple(history), observed_at, decision_cutoff, conid)
        horizon_job = Job(
            owner, "R2", symbol, tuple(sorted(set(history + horizon))), close, self.deadline, conid
        )
        jobs = [initial, horizon_job]
        first, second = self.success(initial), self.success(horizon_job)
        third = None
        if second:
            repeat_at = self.repeat_after(parse(second["response_at"]))
            if repeat_at:
                repeat = replace(horizon_job, stage="R3", ready=repeat_at)
                jobs.append(repeat)
                third = self.success(repeat)
        state = {
            "R1_complete": first is not None,
            "R1_received_before_decision": bool(
                first and parse(first["response_at"]) < decision_cutoff
            ),
            "R2_complete": second is not None,
            "R3_complete": third is not None,
            "projection_stable": bool(
                second and third and stable(second, third, list(horizon_job.dates))
            ),
            "original_conid": conid,
            "stage_details": {job.stage: self.details(job) for job in jobs},
            "receipt_bindings": {
                key: receipt["receipt_sha256"] if receipt else None
                for key, receipt in (("R1", first), ("R2", second), ("R3", third))
            },
        }
        return jobs, state

    def jobs_and_report(self) -> tuple[list[Job], dict[str, Any]]:
        cases: dict[str, dict[str, Any]] = {}
        constructed_at: dict[str, str] = {}
        revision_counts: dict[str, int] = {}
        for record in self.records:
            if record["kind"] == "custody":
                cases.setdefault(record["entity"], record["payload"])
                constructed_at.setdefault(record["entity"], record["recorded_at"])
                revision_counts[record["entity"]] = revision_counts.get(record["entity"], 0) + 1
        jobs: list[Job] = []
        summaries = []
        unassignable = 0
        for entity, payload in cases.items():
            admission = payload["admission"]
            if admission["membership"] == "unassignable":
                unassignable += 1
                continue
            if admission["membership"] != "admitted":
                continue
            state: dict[str, Any] = {"collection_complete": False}
            if (
                admission["symbol_valid"]
                and "history_dates" in admission
                and "horizon_dates" in admission
            ):
                candidate_jobs, state = self.stages(
                    "case:" + entity,
                    payload["row"]["symbol"],
                    admission["history_dates"],
                    admission["horizon_dates"],
                    parse(payload["first_observed_at"]),
                    parse(admission["decision_cutoff"]),
                )
                jobs.extend(candidate_jobs)
                state["collection_complete"] = all(
                    state[key]
                    for key in ("R1_complete", "R2_complete", "R3_complete", "projection_stable")
                )
            summaries.append(
                {
                    "case_id": entity,
                    "symbol": payload["row"]["symbol"],
                    "admission_reason": admission["reason"],
                    "source_observed_before_decision": admission["observed_before_decision"],
                    "first_journal_record_constructed_at": constructed_at[entity],
                    "journal_record_constructed_before_decision": bool(
                        admission.get("decision_cutoff")
                        and parse(constructed_at[entity]) < parse(admission["decision_cutoff"])
                    ),
                    "precutoff_durable_read_verified": False,
                    "recorded_entry_agrees": admission.get("recorded_entry_agrees", False),
                    "source_fields_revised": revision_counts[entity] > 1,
                    "source_validation_errors": payload["source_validation_errors"],
                    "producer_attribution": payload["producer_attribution"],
                    **state,
                }
            )
        first_session = date.fromisoformat(self.frozen["entry_sessions"][0])
        history = [
            str(day.day)
            for day in self.version.days
            if (
                str(day.day) >= self.frozen["earliest_history"]
                and day.day < first_session
                and day.state == "open"
            )
        ]
        horizon = [item["date"] for item in self.frozen["sessions"][:19]]
        benchmark_jobs, benchmark = self.stages(
            "benchmark:SPY",
            "SPY",
            history,
            horizon,
            parse(self.frozen["boundary"]),
            datetime.combine(
                first_session, datetime.min.time().replace(hour=9, minute=20), calendar.NY
            ),
        )
        jobs.extend(benchmark_jobs)
        checkpoint = Job(
            "benchmark:SPY",
            "CHECKPOINT:W10",
            "SPY",
            tuple(history + horizon[:10]),
            parse(self.frozen["sessions"][9]["close"]),
            self.deadline,
            self.pin("benchmark:SPY"),
        )
        jobs.append(checkpoint)
        benchmark["W10_checkpoint"] = self.details(checkpoint)
        schedules = []
        for item in self.frozen["sessions"]:
            through = item["date"]
            dates = tuple(
                history + [day["date"] for day in self.frozen["sessions"] if day["date"] <= through]
            )
            job = Job(
                "benchmark:SPY",
                "SCHEDULE:" + through,
                "SPY",
                dates,
                parse(item["close"]),
                self.deadline,
                self.pin("benchmark:SPY"),
                True,
            )
            jobs.append(job)
            schedules.append({"through": through, "complete": self.success(job) is not None})
        retry_at = self.infrastructure_retry_at()
        report = {
            "purpose": "OUTCOME_BLIND_ACQUISITION_ONLY",
            "strategy_trial_ready": False,
            "report_state": "sealed" if self.records[-1]["kind"] == "seal" else "interim",
            "as_of": timestamp(self.now),
            "recovery_deadline": timestamp(self.deadline),
            "admitted_cases": len(summaries),
            "preexisting_records": len(self.frozen["preexisting_inventory"]["packet_id_hashes"])
            + sum(
                payload["admission"]["membership"] == "preexisting" for payload in cases.values()
            ),
            "unassignable_records": unassignable,
            "cases": summaries,
            "benchmark": benchmark,
            "schedule_coverage": schedules,
            "attempt_count": len(self.attempts),
            "response_count": len(self.responses),
            "attempts_without_response": len(self.attempts) - len(self.responses),
            "attempts_without_response_include_in_flight": True,
            "infrastructure_retry_at": timestamp(retry_at) if retry_at else None,
            "response_issues": [
                {"receipt_sha256": record["sha256"], "issues": receipt["issues"]}
                for _, record, receipt in self.responses
                if receipt["issues"]
            ],
            "watermark": {
                "sequence": self.records[-1]["sequence"],
                "sha256": self.records[-1]["sha256"],
            },
            "unverified": [
                "historical_issuer_identity",
                "owner_history_point_in_time",
                "auction_execution_and_costs",
                "extraordinary_calendar_freshness",
                "statistical_yield_and_power",
            ],
        }
        report["acquisition_counts"] = {
            key: sum(bool(case.get(key)) for case in summaries)
            for key in (
                "R1_complete",
                "R2_complete",
                "R3_complete",
                "projection_stable",
                "collection_complete",
            )
        }
        report["custody_counts"] = {
            "source_observed_before_decision": sum(
                case["source_observed_before_decision"] for case in summaries
            ),
            "source_observed_after_decision": sum(
                not case["source_observed_before_decision"] for case in summaries
            ),
            "journal_constructed_before_decision": sum(
                case["journal_record_constructed_before_decision"] for case in summaries
            ),
            "precutoff_durable_read_verified": 0,
        }
        report["canary_agreement_counts"] = {
            "entry_agrees": sum(case["recorded_entry_agrees"] for case in summaries),
            "source_fields_revised": sum(case["source_fields_revised"] for case in summaries),
            "invalid_source_fields": sum(
                bool(case["source_validation_errors"]) for case in summaries
            ),
        }
        report["admission_counts"] = {
            "admitted": len(summaries),
            "unassignable": unassignable,
            "invalid_symbol": sum(
                case["admission_reason"] == "invalid_symbol" for case in summaries
            ),
        }
        return jobs, report

    def next_request(self, jobs: list[Job]) -> dict[str, Any] | None:
        if (
            self.now < parse(self.frozen["boundary"])
            or self.now >= self.deadline
            or self.records[-1]["kind"] == "seal"
            or 8 <= self.now.astimezone(calendar.NY).hour < 18
        ):
            return None
        attempts = list(self.attempts.values())
        if attempts and (self.now - parse(attempts[-1]["recorded_at"])).total_seconds() < 300:
            return None
        retry_at = self.infrastructure_retry_at()
        if retry_at is not None and self.now < retry_at:
            return None
        eligible: list[tuple[Job, int]] = []
        for job in jobs:
            previous = self.attempts_for(job)
            if job.ready > self.now or self.success(job):
                continue
            content_attempts = [
                attempt
                for attempt in previous
                if not infrastructure_failure(self.source_responses.get(attempt["sha256"]))
            ]
            if content_attempts:
                backoff = min(21600, 300 * 2 ** min(len(content_attempts) - 1, 10))
                if (
                    self.now - parse(content_attempts[-1]["recorded_at"])
                ).total_seconds() < backoff:
                    continue
            eligible.append((job, len(previous)))
        if not eligible:
            return None
        recent_benchmark = any(
            any(key.startswith("benchmark:") for key in attempt["payload"]["jobs"])
            for attempt in attempts[-4:]
        )
        # Reserve at most one in five attempts for the benchmark while candidate
        # work is eligible. Fresh jobs precede retries; then earliest deadline.
        eligible.sort(
            key=lambda pair: (
                (pair[0].owner.startswith("benchmark:") == recent_benchmark),
                pair[1] > 0,
                pair[0].deadline,
                pair[0].ready,
                pair[0].key,
            )
        )
        chosen = eligible[0][0]
        merged = [
            job
            for job, _ in eligible
            if (job.symbol, job.conid, job.schedule)
            == (chosen.symbol, chosen.conid, chosen.schedule)
        ]
        return {
            "symbol": chosen.symbol,
            "expected_conid": chosen.conid,
            "schedule": chosen.schedule,
            "start_date": min(day for job in merged for day in job.dates),
            "end_at": timestamp(self.now),
            "jobs": sorted(job.key for job in merged),
            "required_dates": sorted({day for job in merged for day in job.dates}),
            "planned_at": timestamp(self.now),
        }
