from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from insider_alerts.sec.models import FilingRef


@dataclass(slots=True)
class StoreResult:
    inserted: int
    skipped_existing: int


def init_db(db_path: str) -> None:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(path) as conn:
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

        conn.commit()


def upsert_filing_refs(db_path: str, refs: list[FilingRef]) -> StoreResult:
    init_db(db_path)
    inserted = 0
    skipped = 0

    with sqlite3.connect(db_path) as conn:
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
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT source, cik, accession_number, form_type, filed_at,
                   filing_detail_url, primary_doc_url, raw_rss_entry
            FROM filings
            WHERE form4_xml_url IS NULL
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


def update_form4_xml_url(
    db_path: str,
    *,
    accession_number: str,
    cik: str,
    form_type: str,
    xml_url: str,
) -> int:
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            UPDATE filings
            SET form4_xml_url = ?, filing_index_fetched_at = CURRENT_TIMESTAMP
            WHERE accession_number = ? AND cik = ? AND form_type = ?
              AND (form4_xml_url IS NULL OR form4_xml_url = '')
            """,
            (xml_url, accession_number, cik, form_type),
        )
        conn.commit()
    return int(cursor.rowcount)
