"""Read issuer identity from an immutable Form 4 approval copy."""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from collections.abc import Callable, Sequence
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import rfc8785

from insider_alerts.backtest.signal_study import normalize_delivered_signal_symbol
from insider_alerts.execution.observer import safe_path
from insider_alerts.research.commissioning_journal import digest
from insider_alerts.research.commissioning_source import timestamp
from insider_alerts.research.commissioning_window import parse

APPROVAL_CONTRACT = "insider-evidence-capture-v1"


def approval_database(path: Path) -> dict[str, str]:
    path = safe_path(path).resolve(strict=True)
    stat = path.stat()
    if not path.is_file():
        raise ValueError("approval source is not a file")
    return {"path": str(path), "device": str(stat.st_dev), "inode": str(stat.st_ino)}


@dataclass(frozen=True, slots=True)
class ApprovalRead:
    started: datetime
    observed: datetime
    rows: dict[str, list[sqlite3.Row]]
    failure: str | None = None

    def evidence(self, candidate: dict[str, Any], cutoff: datetime) -> dict[str, Any]:
        if self.failure is not None:
            return {"reason": self.failure}
        matches = self.rows.get(candidate["packet_id"], [])
        if len(matches) != 1:
            return {"reason": "approval_copy_missing" if not matches else "approval_copy_ambiguous"}
        row = matches[0]
        form = row["form_type"]
        route = candidate["cik"]
        accession = candidate["accession_number"]
        packet = candidate["packet_id"]
        if (
            form not in {"4", "4/A"}
            or row["contract_version"] != APPROVAL_CONTRACT
            or row["packet_id"] != packet
            or row["job_id"] != f"{packet}|{APPROVAL_CONTRACT}"
            or row["accession_number"] != accession
            or row["issuer_cik"] != route
            or packet != f"{accession}|{route}|{form}"
        ):
            return {"reason": "approval_copy_identity_mismatch"}

        try:
            approved_at = parse(row["decision_at_utc"])
            if (
                not parse(row["source_first_observed_at_utc"])
                <= approved_at
                == parse(row["created_at_utc"])
                == parse(candidate["signal_at"])
                <= self.started
                <= self.observed
                < cutoff
            ):
                return {"reason": "approval_copy_clock_mismatch"}
            payload = json.loads(row["payload_json"])
            decision = json.loads(row["decision_json"])
        except (AttributeError, ValueError, TypeError):
            return {"reason": "approval_copy_unreadable"}
        if not (
            isinstance(payload, dict)
            and isinstance(decision, dict)
            and decision.get("packet_id") == packet
            and decision.get("decision") == "approve"
        ):
            return {"reason": "approval_copy_invalid_decision"}
        symbol = payload.get("issuer_symbol")
        if (
            not isinstance(symbol, str)
            or normalize_delivered_signal_symbol(symbol) != candidate["symbol"]
        ):
            return {"reason": "approval_symbol_mismatch"}
        cik = payload.get("issuer_cik")
        if not (
            isinstance(cik, str) and cik.isascii() and cik.isdigit() and len(cik) <= 10 and int(cik)
        ):
            return {"reason": "approval_issuer_cik_invalid"}
        material = {
            "approval_contract": APPROVAL_CONTRACT,
            "job_id": row["job_id"],
            "packet_id": packet,
            "accession_number": accession,
            "form_type": form,
            "routing_cik": route,
            "issuer_cik": int(cik),
            "issuer_symbol": candidate["symbol"],
            "decision_at": row["decision_at_utc"],
            "source_first_observed_at": row["source_first_observed_at_utc"],
            "copy_created_at": row["created_at_utc"],
            "filing_payload_sha256": digest(row["payload_json"].encode("utf-8")),
            "approval_decision_sha256": digest(row["decision_json"].encode("utf-8")),
        }
        return material | {
            "approval_sha256": digest(rfc8785.dumps(material)),
            "read_started_at": timestamp(self.started),
            "read_at": timestamp(self.observed),
        }


def read_approvals(
    path: Path,
    identity: dict[str, str],
    packets: Sequence[str],
    clock: Callable[[], datetime],
) -> ApprovalRead:
    """Use one bounded transaction and close it before any evidence is written."""
    start = clock()
    rows: dict[str, list[sqlite3.Row]] = defaultdict(list)
    try:
        if approval_database(path) != identity:
            return ApprovalRead(start, clock(), {}, "approval_source_replaced")
        uri = Path(identity["path"]).as_uri() + "?mode=ro"
        with closing(sqlite3.connect(uri, uri=True, timeout=0.5)) as db:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA query_only=ON")
            db.execute("BEGIN")
            for index in range(0, len(packets), 200):
                batch = packets[index : index + 200]
                markers = ",".join("?" for _ in batch)
                query = f"""
                    SELECT job_id, packet_id, contract_version, accession_number,
                           issuer_cik, form_type, payload_json, decision_json,
                           source_first_observed_at_utc, decision_at_utc, created_at_utc
                    FROM research_capture_jobs
                    WHERE contract_version=? AND packet_id IN ({markers})
                """
                for row in db.execute(query, (APPROVAL_CONTRACT, *batch)):
                    rows[row["packet_id"]].append(row)
        if approval_database(path) != identity:
            return ApprovalRead(start, clock(), {}, "approval_source_replaced")
    except (OSError, sqlite3.Error, ValueError):
        return ApprovalRead(start, clock(), {}, "approval_source_unavailable")
    return ApprovalRead(start, clock(), rows)
