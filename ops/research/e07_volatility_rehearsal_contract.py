"""Case and source qualification for the volatility input-custody rehearsal."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from typing import Any

from e07_volatility_exits.features import BASE_WIDTH, CloseSnapshot, estimate_volatility
from insider_alerts.research.calendar_v2 import Version
from insider_alerts.research.commissioning_receipts import inspect_receipt
from insider_alerts.research.commissioning_window import parse

DIRECTORY_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
DIRECTORY_AGE = timedelta(hours=48)
# Restrict positive links to exchange names with an unambiguous translation.
VENUES = {"Nasdaq": {"NASDAQ", "ISLAND"}, "NYSE": {"NYSE"}, "NYSE American": {"AMEX"}}


def issuer_directory(text: str) -> dict[str, set[tuple[int, str]]]:
    """Parse the official directory without guessing a missing or changed column."""
    body = json.loads(text)
    if (
        not isinstance(body, dict)
        or body.get("fields") != ["cik", "name", "ticker", "exchange"]
        or not isinstance(body.get("data"), list)
        or not body["data"]
    ):
        raise ValueError("unrecognized SEC ticker directory")
    index: dict[str, set[tuple[int, str]]] = {}
    for row in body["data"]:
        if (
            not isinstance(row, list)
            or len(row) != 4
            or type(row[0]) is not int
            or row[0] <= 0
            or not isinstance(row[2], str)
        ):
            raise ValueError("invalid SEC ticker directory row")
        if not isinstance(row[3], (str, type(None))):
            raise ValueError("invalid SEC ticker directory exchange")
        # Keep unlisted/unsupported rows as ambiguity, even if another row is listed.
        index.setdefault(row[2].upper().replace("-", "."), set()).add((row[0], row[3] or ""))
    return index


def issuer_matches(
    directory: dict[str, set[tuple[int, str]]],
    row: dict[str, Any],
    identity: dict[str, Any],
) -> bool:
    symbol = row["symbol"].replace("-", ".")
    cik = str(row["cik"])
    if not cik.isascii() or not cik.isdigit() or len(cik) > 10:
        return False
    listing = directory.get(symbol, set())
    if len(listing) != 1:
        return False
    issuer, venue = next(iter(listing))
    return issuer == int(cik) and identity["primary_exchange"] in VENUES.get(venue, set())


def bar_proof(
    attempt: dict[str, Any],
    response: dict[str, Any],
    version: Version,
    *,
    dates: tuple[str, ...],
    finalized_at: datetime,
    deadline: datetime,
) -> dict[str, Any] | None:
    request = attempt["payload"]
    if (
        request["job_stage"] == "history"
        and request["expected_conid"] is not None
        and response["payload"].get("symbol_resolution_changed") is not False
    ):
        return None
    proof = inspect_receipt(attempt, response, version)
    if not proof["usable"] or not all(day in proof["bars"] for day in dates):
        return None
    received = parse(proof["response_at"])
    if not finalized_at <= received <= parse(response["recorded_at"]) < deadline:
        return None
    return proof


def decision_material(
    case: dict[str, Any],
    receipts: tuple[tuple[dict[str, Any], dict[str, Any], dict[str, Any]], ...],
    directory_record: dict[str, Any] | None,
    version: Version,
    cutoff: datetime,
) -> dict[str, Any]:
    """Bind the earliest complete input response and report qualification separately."""
    dates = tuple(case["history"])
    last_day = version.day(date.fromisoformat(dates[-1]))
    close = last_day.closes_at
    if close is None:
        raise ValueError("frozen history has no final close")
    for attempt, response, proof in receipts:
        identity = response["payload"]["identity"]
        result = {
            "case_sha256": case["case_sha256"],
            "attempt_sha256": attempt["sha256"],
            "observation_sha256": response["sha256"],
            "conid": proof["conid"],
            "bar_digests": [proof["bars"][day]["content_sha256"] for day in dates],
            "directory_sha256": None,
            "reason": "issuer_directory_unavailable",
            "fallback_width": BASE_WIDTH,
        }
        if directory_record is None:
            return result
        directory = directory_record["payload"]
        observed, recorded = parse(directory["observed_at"]), parse(directory_record["recorded_at"])
        if not cutoff - DIRECTORY_AGE <= observed <= recorded < cutoff:
            return result | {"reason": "issuer_directory_outside_window"}
        directory_sha = directory_record["sha256"]
        if not issuer_matches(issuer_directory(directory["raw"]), case["row"], identity):
            return result | {"reason": "issuer_venue_unverified", "directory_sha256": directory_sha}
        raw = response["payload"]["response"]
        security = f"ibkr:{proof['conid']}"
        estimate = estimate_volatility(
            security,
            cutoff,
            tuple(date.fromisoformat(day) for day in dates),
            CloseSnapshot(
                instrument=security,
                received_at=parse(proof["response_at"]),
                closes=tuple(
                    (date.fromisoformat(day), float(raw[proof["bars"][day]["index"]]["close"]))
                    for day in dates
                ),
            ),
            previous_session_close=close,
        )
        return result | {
            "directory_sha256": directory_sha,
            "reason": estimate.missing,
            "fallback_width": BASE_WIDTH if estimate.missing else None,
            "daily_volatility": estimate.daily,
        }
    return {
        "case_sha256": case["case_sha256"],
        "reason": "no_complete_preopen_bar_receipt",
        "fallback_width": BASE_WIDTH,
    }
