from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from insider_alerts.sec.models import FilingRef

SQLITE_CONNECT_TIMEOUT_SECONDS = 30.0
SQLITE_BUSY_TIMEOUT_MS = 30_000
SQLITE_LOCK_RETRY_ATTEMPTS = 6
SQLITE_LOCK_RETRY_BASE_SLEEP_SECONDS = 0.25
SEC_PROCESSING_STAGES = frozenset({"xml_enrichment", "review_xml"})


def form4_source_boundary_sql(table_alias: str = "") -> str:
    """Return the legacy eligibility predicate for a trusted Form 4 source row.

    Older SEC RSS rows did not persist a separately validated feed form type. Preserve all rows,
    but exclude an RSS row when its available title explicitly identifies a different form.
    Non-RSS and title-missing legacy rows remain eligible for backward compatibility.
    """
    if table_alias and not table_alias.replace("_", "").isalnum():
        raise ValueError("table_alias must be an SQL identifier")
    prefix = f"{table_alias}." if table_alias else ""
    source = f"{prefix}source"
    raw = f"{prefix}raw_rss_entry"
    title = (
        f"CASE WHEN json_valid({raw}) "
        f"THEN trim(json_extract({raw}, '$.title')) ELSE NULL END"
    )
    feed_form_type = (
        f"CASE WHEN json_valid({raw}) "
        f"THEN upper(trim(json_extract({raw}, '$.feed_form_type'))) ELSE NULL END"
    )
    return f"""(
        json_valid({raw})
        AND (
            {source} <> 'sec_rss'
            OR nullif({title}, '') IS NULL
            OR (
                instr({title}, '-') > 0
                AND upper(trim(substr({title}, 1, instr({title}, '-') - 1))) IN ('4', '4/A')
            )
            OR {feed_form_type} IN ('4', '4/A')
        )
    )"""


@dataclass(slots=True)
class StoreResult:
    inserted: int
    skipped_existing: int


def _connect_sqlite(db_path: str | Path, *, write: bool) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=SQLITE_CONNECT_TIMEOUT_SECONDS)
    conn.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
    if write:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def _is_sqlite_locked_error(exc: sqlite3.OperationalError) -> bool:
    msg = str(exc).lower()
    return "database is locked" in msg or "database table is locked" in msg


def init_db(db_path: str) -> None:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with _connect_sqlite(path, write=True) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS filings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                cik TEXT NOT NULL,
                accession_number TEXT NOT NULL,
                form_type TEXT NOT NULL,
                filed_at TEXT NOT NULL,
                filing_detail_url TEXT NOT NULL,
                primary_doc_url TEXT,
                filing_index_fetched_at TEXT,
                form4_xml_url TEXT,
                raw_rss_entry TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(accession_number, cik, form_type)
            )
            """
        )

        columns = {
            row[1] for row in conn.execute("PRAGMA table_info(filings)").fetchall()
        }
        if "filing_index_fetched_at" not in columns:
            conn.execute("ALTER TABLE filings ADD COLUMN filing_index_fetched_at TEXT")
        if "form4_xml_url" not in columns:
            conn.execute("ALTER TABLE filings ADD COLUMN form4_xml_url TEXT")

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_filings_form_type_filed_at_cik
            ON filings (form_type, filed_at, cik)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_filings_accession_form_type
            ON filings (accession_number, form_type)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_filings_form4_xml_filed_at_cik
            ON filings (filed_at, cik, accession_number)
            WHERE form_type = '4' AND form4_xml_url IS NOT NULL AND form4_xml_url <> ''
            """
        )
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS sec_processing_rejections (
                stage TEXT NOT NULL,
                accession_number TEXT NOT NULL,
                cik TEXT NOT NULL,
                form_type TEXT NOT NULL,
                input_url TEXT NOT NULL,
                reason TEXT NOT NULL,
                recorded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (stage, accession_number, cik, form_type, input_url)
            );
            CREATE INDEX IF NOT EXISTS idx_sec_processing_rejections_lookup
            ON sec_processing_rejections (
                stage, accession_number, cik, form_type, input_url
            );
            CREATE TRIGGER IF NOT EXISTS sec_processing_rejections_no_update
            BEFORE UPDATE ON sec_processing_rejections
            BEGIN SELECT RAISE(ABORT, 'SEC processing rejections are append-only'); END;
            CREATE TRIGGER IF NOT EXISTS sec_processing_rejections_no_delete
            BEFORE DELETE ON sec_processing_rejections
            BEGIN SELECT RAISE(ABORT, 'SEC processing rejections are append-only'); END;
            """
        )

        conn.commit()


def upsert_filing_refs(db_path: str, refs: list[FilingRef]) -> StoreResult:
    init_db(db_path)
    inserted = 0
    skipped = 0

    with _connect_sqlite(db_path, write=True) as conn:
        for ref in refs:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO filings (
                    source, cik, accession_number, form_type, filed_at,
                    filing_detail_url, primary_doc_url, raw_rss_entry
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ref.source,
                    ref.cik,
                    ref.accession_number,
                    ref.form_type,
                    ref.filed_at.isoformat(),
                    ref.filing_detail_url,
                    ref.primary_doc_url,
                    json.dumps(ref.raw_rss_entry, separators=(",", ":")),
                ),
            )
            if cursor.rowcount == 1:
                inserted += 1
            else:
                skipped += 1

        conn.commit()

    return StoreResult(inserted=inserted, skipped_existing=skipped)


def list_filings_missing_xml(db_path: str, *, limit: int) -> list[FilingRef]:
    init_db(db_path)
    source_boundary = form4_source_boundary_sql()
    with _connect_sqlite(db_path, write=False) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT source, cik, accession_number, form_type, filed_at,
                   filing_detail_url, primary_doc_url, raw_rss_entry
            FROM filings
            WHERE form4_xml_url IS NULL
              AND form_type IN ('4', '4/A')
              AND {source_boundary}
              AND NOT EXISTS (
                  SELECT 1
                  FROM sec_processing_rejections AS rejection
                  WHERE rejection.stage = 'xml_enrichment'
                    AND rejection.accession_number = filings.accession_number
                    AND rejection.cik = filings.cik
                    AND rejection.form_type = filings.form_type
                    AND rejection.input_url = filings.filing_detail_url
              )
            ORDER BY filed_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    results: list[FilingRef] = []
    for row in rows:
        results.append(
            FilingRef(
                source=str(row["source"]),
                cik=str(row["cik"]),
                accession_number=str(row["accession_number"]),
                form_type=str(row["form_type"]),
                filed_at=datetime.fromisoformat(str(row["filed_at"])),
                filing_detail_url=str(row["filing_detail_url"]),
                primary_doc_url=str(row["primary_doc_url"]) if row["primary_doc_url"] else None,
                raw_rss_entry=json.loads(str(row["raw_rss_entry"])),
            )
        )
    return results


def record_sec_processing_rejections(
    db_path: str,
    *,
    stage: str,
    rejections: Sequence[tuple[str, str, str, str, str]],
) -> int:
    """Append deterministic SEC processing rejections without changing source rows."""

    if stage not in SEC_PROCESSING_STAGES:
        raise ValueError(f"unsupported SEC processing stage: {stage}")
    if not rejections:
        return 0
    init_db(db_path)
    with _connect_sqlite(db_path, write=True) as conn:
        before = conn.total_changes
        conn.executemany(
            """
            INSERT OR IGNORE INTO sec_processing_rejections (
                stage, accession_number, cik, form_type, input_url, reason
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (stage, accession_number, cik, form_type, input_url, reason)
                for accession_number, cik, form_type, input_url, reason in rejections
            ],
        )
        conn.commit()
        return conn.total_changes - before


def _update_form4_xml_urls_once(
    db_path: str,
    updates: Sequence[tuple[str, str, str, str]],
) -> int:
    updated = 0
    with _connect_sqlite(db_path, write=True) as conn:
        for accession_number, cik, form_type, xml_url in updates:
            cursor = conn.execute(
                """
                UPDATE filings
                SET form4_xml_url = ?, filing_index_fetched_at = CURRENT_TIMESTAMP
                WHERE accession_number = ? AND cik = ? AND form_type = ?
                  AND (form4_xml_url IS NULL OR form4_xml_url = '')
                """,
                (xml_url, accession_number, cik, form_type),
            )
            updated += int(cursor.rowcount)
        conn.commit()
    return updated


def update_form4_xml_urls(
    db_path: str,
    *,
    updates: Sequence[tuple[str, str, str, str]],
) -> int:
    init_db(db_path)
    if not updates:
        return 0

    delay_seconds = SQLITE_LOCK_RETRY_BASE_SLEEP_SECONDS
    for attempt in range(SQLITE_LOCK_RETRY_ATTEMPTS):
        try:
            return _update_form4_xml_urls_once(db_path, updates)
        except sqlite3.OperationalError as exc:
            if not _is_sqlite_locked_error(exc) or attempt >= SQLITE_LOCK_RETRY_ATTEMPTS - 1:
                raise
            time.sleep(delay_seconds)
            delay_seconds = min(delay_seconds * 2.0, 2.0)
    return 0


def update_form4_xml_url(
    db_path: str,
    *,
    accession_number: str,
    cik: str,
    form_type: str,
    xml_url: str,
) -> int:
    return update_form4_xml_urls(
        db_path,
        updates=[(accession_number, cik, form_type, xml_url)],
    )


def get_filing_date_bounds(db_path: str) -> tuple[date | None, date | None]:
    init_db(db_path)
    with _connect_sqlite(db_path, write=False) as conn:
        row = conn.execute(
            """
            SELECT MIN(date(filed_at)), MAX(date(filed_at))
            FROM filings
            """
        ).fetchone()

    if row is None:
        return None, None
    min_date_obj, max_date_obj = row
    min_date = date.fromisoformat(str(min_date_obj)) if min_date_obj else None
    max_date = date.fromisoformat(str(max_date_obj)) if max_date_obj else None
    return min_date, max_date
