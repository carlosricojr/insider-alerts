"""Case and source qualification for the volatility input-custody rehearsal."""

from __future__ import annotations

import json
from dataclasses import dataclass
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
    symbol: str,
    issuer_cik: int,
    identity: dict[str, Any],
) -> bool:
    listing = directory.get(symbol.replace("-", "."), set())
    if len(listing) != 1:
        return False
    issuer, venue = next(iter(listing))
    return issuer == issuer_cik and identity["primary_exchange"] in VENUES.get(venue, set())


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


@dataclass(frozen=True, slots=True)
class DirectoryReceipt:
    record: dict[str, Any]
    index: dict[str, set[tuple[int, str]]]


def decision_material(
    case: dict[str, Any],
    matched: tuple[dict[str, Any], dict[str, Any], dict[str, Any]] | None,
    directory: DirectoryReceipt | None,
    version: Version,
    cutoff: datetime,
    *,
    approval: dict[str, Any] | None,
) -> dict[str, Any]:
    """Bind the first complete input response and classify identity separately."""
    result = {"case_sha256": case["case_sha256"], "fallback_width": BASE_WIDTH}
    if matched is None:
        return result | {"reason": "no_complete_preopen_bar_receipt"}

    attempt, response, proof = matched
    dates = tuple(case["history"])
    result.update(
        attempt_sha256=attempt["sha256"],
        observation_sha256=response["sha256"],
        conid=proof["conid"],
        bar_digests=[proof["bars"][day]["content_sha256"] for day in dates],
    )
    if directory is None:
        return result | {"reason": "issuer_directory_unavailable"}
    observed = parse(directory.record["payload"]["observed_at"])
    recorded = parse(directory.record["recorded_at"])
    if not cutoff - DIRECTORY_AGE <= observed <= recorded < cutoff:
        return result | {"reason": "issuer_directory_outside_window"}

    result["directory_sha256"] = directory.record["sha256"]
    if approval is None or "reason" in approval:
        reason = "approval_copy_missing" if approval is None else approval["reason"]
        return result | {"reason": reason}
    result["issuer_approval"] = approval
    if not issuer_matches(
        directory.index,
        approval["issuer_symbol"],
        approval["issuer_cik"],
        response["payload"]["identity"],
    ):
        return result | {"reason": "issuer_venue_unverified"}

    close = version.day(date.fromisoformat(dates[-1])).closes_at
    if close is None:
        raise ValueError("frozen history has no final close")
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
        "issuer_linked": True,
        "reason": estimate.missing,
        "fallback_width": BASE_WIDTH if estimate.missing else None,
        "daily_volatility": estimate.daily,
    }
