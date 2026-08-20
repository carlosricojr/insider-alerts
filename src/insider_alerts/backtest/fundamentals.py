from __future__ import annotations

import json
import math
import sqlite3
from collections.abc import Iterable, Mapping
from contextlib import closing
from datetime import UTC, date, datetime

from insider_alerts.sec.client import SecHttpClient, SecHttpError

SHARE_FACTS: tuple[tuple[str, str], ...] = (
    ("dei", "EntityCommonStockSharesOutstanding"),
    ("us-gaap", "CommonStockSharesOutstanding"),
)


def shares_outstanding_as_of(payload: Mapping[str, object], *, as_of: date) -> float | None:
    """Return the latest share fact that was public and period-complete by ``as_of``."""

    facts = payload.get("facts")
    if not isinstance(facts, dict):
        return None
    for namespace, fact_name in SHARE_FACTS:
        namespace_obj = facts.get(namespace)
        if not isinstance(namespace_obj, dict):
            continue
        fact_obj = namespace_obj.get(fact_name)
        if not isinstance(fact_obj, dict):
            continue
        units_obj = fact_obj.get("units")
        if not isinstance(units_obj, dict):
            continue
        observations = units_obj.get("shares")
        if not isinstance(observations, list):
            continue
        eligible: list[tuple[date, date, float]] = []
        for observation in observations:
            if not isinstance(observation, dict):
                continue
            try:
                filed = date.fromisoformat(str(observation["filed"]))
                period_end = date.fromisoformat(str(observation["end"]))
                value = float(observation["val"])
            except (KeyError, TypeError, ValueError):
                continue
            if (
                filed <= as_of
                and period_end <= as_of
                and math.isfinite(value)
                and value > 0
            ):
                eligible.append((filed, period_end, value))
        if eligible:
            return max(eligible, key=lambda item: (item[0], item[1]))[2]
    return None


def ensure_companyfacts_table(db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sec_companyfacts_cache (
                cik TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL,
                fetched_at TEXT NOT NULL
            )
            """
        )
        conn.commit()


def load_cached_companyfacts(db_path: str) -> dict[str, dict[str, object]]:
    ensure_companyfacts_table(db_path)
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("SELECT cik, payload_json FROM sec_companyfacts_cache").fetchall()
    result: dict[str, dict[str, object]] = {}
    for cik, payload_text in rows:
        try:
            payload = json.loads(str(payload_text))
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            result[str(cik)] = payload
    return result


def refresh_companyfacts(
    db_path: str,
    *,
    ciks: Iterable[str],
    client: SecHttpClient,
    force: bool = False,
) -> dict[str, object]:
    ensure_companyfacts_table(db_path)
    requested_ciks = sorted(set(ciks))
    fetched = 0
    reused = 0
    errors: list[str] = []
    with closing(sqlite3.connect(db_path)) as read_conn:
        cached_ciks = {
            str(row[0])
            for row in read_conn.execute("SELECT cik FROM sec_companyfacts_cache").fetchall()
        }
    with closing(sqlite3.connect(db_path)) as conn:
        for raw_cik in requested_ciks:
            digits = "".join(char for char in str(raw_cik) if char.isdigit())
            if not digits:
                errors.append(f"{raw_cik}: invalid CIK")
                continue
            normalized = digits.zfill(10)
            if normalized in cached_ciks and not force:
                reused += 1
                continue
            url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{normalized}.json"
            try:
                payload_text = client.get_text(url)
                payload = json.loads(payload_text)
                if not isinstance(payload, dict):
                    raise ValueError("companyfacts payload is not an object")
            except (SecHttpError, json.JSONDecodeError, ValueError) as exc:
                errors.append(f"{normalized}: {exc}")
                continue
            conn.execute(
                """
                INSERT OR REPLACE INTO sec_companyfacts_cache (cik, payload_json, fetched_at)
                VALUES (?, ?, ?)
                """,
                (
                    normalized,
                    json.dumps(payload, separators=(",", ":")),
                    datetime.now(UTC).isoformat(),
                ),
            )
            conn.commit()
            cached_ciks.add(normalized)
            fetched += 1
    return {
        "requested": len(requested_ciks),
        "fetched": fetched,
        "reused": reused,
        "errors": errors,
    }
