"""Outcome-blind qualification of archived E07 volatility inputs."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter, defaultdict
from collections.abc import Sequence
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import rfc8785

from e07_volatility_exits.features import FEATURE_CUTOFF, LOOKBACK_CLOSES
from insider_alerts.research.calendar_v2 import NY, CalendarJournal, Version
from insider_alerts.research.commissioning_journal import Journal
from insider_alerts.research.commissioning_receipts import inspect_receipt
from insider_alerts.research.commissioning_window import parse, validate_manifest

CONTRACT = "e07-volatility-commissioning-input-audit-v1"
FINALIZATION_BUFFER = timedelta(minutes=1)


class CalendarCoverageError(ValueError):
    """An admitted case's prior sessions cannot be established."""


def _history(version: Version, entry: date) -> tuple[set[str], datetime]:
    """Derive the exact 21 sessions and latest safe daily-bar observation boundary."""
    dates: set[str] = set()
    finalization = None
    day = entry
    while len(dates) < LOOKBACK_CLOSES:
        day -= timedelta(days=1)
        session = version.day(day)
        if session.state == "unknown":
            raise CalendarCoverageError("unknown calendar during forecast lookback")
        if session.state == "open":
            if session.closes_at is None:
                raise CalendarCoverageError("missing close for an open calendar session")
            if finalization is None:
                finalization = session.closes_at + FINALIZATION_BUFFER
            dates.add(day.isoformat())

    if finalization is None:
        raise ValueError("empty forecast lookback")
    return dates, finalization


def _group_archive(
    records: Sequence[dict[str, Any]], version: Version
) -> tuple[dict[str, dict[str, Any]], dict[str, list[tuple[datetime, str, dict[str, Any]]]]]:
    cases: dict[str, dict[str, Any]] = {}
    attempts: dict[str, dict[str, Any]] = {}
    receipts: dict[str, list[tuple[datetime, str, dict[str, Any]]]] = defaultdict(list)
    for record in records:
        if record["kind"] == "custody":
            cases.setdefault("case:" + record["entity"], record)
        elif record["kind"] == "attempt":
            attempts[record["sha256"]] = record
        elif record["kind"] == "response":
            attempt = attempts[record["entity"]]
            case_jobs = tuple(
                job
                for job in attempt["payload"]["jobs"]
                if job.startswith("case:") and job.endswith("/R1")
            )
            if case_jobs:
                proof = inspect_receipt(attempt, record, version)
                recorded_at = parse(record["recorded_at"])
                for job in case_jobs:
                    receipts[job.removesuffix("/R1")].append(
                        (recorded_at, attempt["payload"]["symbol"], proof)
                    )

    return cases, receipts


def audit_archive(records: Sequence[dict[str, Any]], versions: Sequence[Version]) -> dict[str, Any]:
    """Audit one verified archive head and the calendar version bound at activation."""
    if not records or records[0]["kind"] != "activation":
        raise ValueError("commissioning activation is missing")
    manifest = records[0]["payload"]
    version = next((item for item in versions if item.digest == manifest["calendar_version"]), None)
    if version is None:
        raise ValueError("commissioning activation calendar is absent")
    validate_manifest(manifest, version)

    cases, receipts = _group_archive(records, version)
    membership: Counter[str] = Counter()
    admitted_results: Counter[str] = Counter()
    for key, case in cases.items():
        admission = case["payload"]["admission"]
        state = admission["membership"]
        membership[state] += 1
        if state != "admitted":
            continue

        entry = date.fromisoformat(admission["entry_session"])
        if not admission["symbol_valid"]:
            admitted_results["invalid_symbol"] += 1
            continue
        try:
            expected, finalization = _history(version, entry)
        except CalendarCoverageError:
            admitted_results["unavailable_calendar"] += 1
            continue

        cutoff = datetime.combine(entry, FEATURE_CUTOFF, NY)
        available = receipts.get(key, ())
        case_symbol = case["payload"]["row"]["symbol"]
        complete = None
        missing = "no_response"
        for recorded, symbol, proof in available:
            if symbol != case_symbol:
                missing = "case_symbol_mismatch"
                continue
            if not proof["usable"]:
                missing = "unusable_receipt"
                continue
            if not expected.issubset(proof["bars"]):
                missing = "incomplete_prior_sessions"
                continue
            received = parse(proof["response_at"])
            if not finalization <= received <= recorded:
                missing = "receipt_clock_outside_finalization"
                continue
            complete = recorded
            break

        if complete is None:
            result = missing
        elif complete >= cutoff:
            result = "first_complete_response_recorded_after_cutoff"
        elif parse(case["recorded_at"]) >= cutoff:
            result = "timely_response_case_recorded_after_cutoff"
        else:
            result = "response_and_case_recorded_before_cutoff"
        admitted_results[result] += 1

    body = {
        "contract": CONTRACT,
        "status": "descriptive_only",
        "coverage_rule": {
            "required_sessions": LOOKBACK_CLOSES,
            "feature_cutoff_ny": FEATURE_CUTOFF.isoformat(),
            "seconds_after_close": int(FINALIZATION_BUFFER.total_seconds()),
            "journal_selection": "first_complete_matching_case_receipt",
        },
        "scientific_reuse": "prohibited",
        "activation_sha256": records[0]["sha256"],
        "watermark_sequence": records[-1]["sequence"],
        "watermark_sha256": records[-1]["sha256"],
        "calendar_sha256": version.digest,
        "membership_counts": dict(membership),
        "admitted_receipt_counts": dict(admitted_results),
        "issuer_identity": "unverified_in_this_archive_contract",
        "pre_open_decision_read": "unverified",
    }
    return {**body, "audit_sha256": hashlib.sha256(rfc8785.dumps(body)).hexdigest()}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--journal", required=True, type=Path)
    parser.add_argument("--calendar", required=True, type=Path)
    args = parser.parse_args()
    print(
        json.dumps(
            audit_archive(
                Journal(args.journal).records(), CalendarJournal(args.calendar).versions()
            ),
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
