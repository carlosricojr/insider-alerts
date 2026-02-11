from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
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
                raw_rss_entry TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(accession_number, cik, form_type)
            )
            """
        )
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
