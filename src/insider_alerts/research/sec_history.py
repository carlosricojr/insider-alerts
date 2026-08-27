"""Immutable SEC ownership-history archives and cutoff-safe owner classification.

This module is deliberately order-incapable.  It only downloads public SEC evidence,
normalizes immutable source rows, and classifies reporting-owner histories.
"""

from __future__ import annotations

import csv
import hashlib
import io
import os
import re
import sqlite3
import tempfile
import uuid
import zipfile
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urljoin, urlparse

import rfc8785

from insider_alerts.sec.client import SecHttpClient, SecResource

ARCHIVE_MANIFEST_URL = (
    "https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets"
)
ARCHIVE_RE = re.compile(r"(?P<year>20\d{2})q(?P<quarter>[1-4])_form345\.zip$", re.I)
CLASSIFIER_VERSION = "cmp-owner-calendar-v1"
_MONTHS = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}
_REQUIRED_ARCHIVE_FILES = {"SUBMISSION.TSV", "REPORTINGOWNER.TSV", "NONDERIV_TRANS.TSV"}
_MAX_ARCHIVE_ENTRIES = 32
_MAX_MEMBER_BYTES = 750_000_000
_MAX_TOTAL_UNCOMPRESSED_BYTES = 2_000_000_000

ClassificationState = Literal[
    "routine", "opportunistic", "unpartitionable", "ambiguous_multi_owner"
]


@dataclass(frozen=True, slots=True, order=True)
class ArchiveRef:
    year: int
    quarter: int
    url: str


@dataclass(frozen=True, slots=True)
class RawObject:
    sha256: str
    size_bytes: int
    path: Path


@dataclass(frozen=True, slots=True)
class Coverage:
    complete_from: date
    complete_through: date
    prehistory_complete: bool
    missing_quarters: tuple[str, ...] = ()
    prehistory_evidence_sha256: str | None = None

    def __post_init__(self) -> None:
        if self.complete_from > self.complete_through:
            raise ValueError("coverage start cannot be after coverage end")
        digest = self.prehistory_evidence_sha256
        if self.prehistory_complete and (
            digest is None
            or len(digest) != 64
            or any(char not in "0123456789abcdef" for char in digest)
        ):
            raise ValueError("complete prehistory requires a SHA-256 evidence binding")


@dataclass(frozen=True, slots=True)
class OwnerFiling:
    accession_number: str
    filing_date: date
    form_type: str
    issuer_cik: str
    owner_ciks: tuple[str, ...]
    original_submission_date: date | None
    transaction_dates: tuple[date, ...]
    has_invalid_transaction: bool


@dataclass(frozen=True, slots=True)
class Classification:
    state: ClassificationState
    reason: str
    owner_cik: str
    classification_year: int
    cutoff_date: date
    history_coverage_complete: bool
    left_censored: bool
    routine_since_year: int | None
    history_input_sha256: str


@dataclass(frozen=True, slots=True)
class SyncResult:
    snapshot_sha256: str
    manifest_sha256: str
    archive_count: int
    downloaded_count: int
    reused_count: int
    first_quarter: str
    last_quarter: str


class _ManifestLinks(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        for key, value in attrs:
            if key.lower() == "href" and value:
                self.hrefs.append(value)


def _normalize_cik(value: str) -> str:
    digits = "".join(char for char in value if char.isdigit())
    if not digits or len(digits) > 10:
        raise ValueError(f"invalid CIK: {value!r}")
    return digits.lstrip("0") or "0"


def _parse_sec_date(value: str) -> date | None:
    stripped = value.strip()
    if not stripped:
        return None
    parts = stripped.upper().split("-")
    try:
        if len(parts) != 3:
            raise ValueError
        return date(int(parts[2]), _MONTHS[parts[1]], int(parts[0]))
    except (KeyError, ValueError) as exc:
        raise ValueError(f"invalid SEC date: {value!r}") from exc


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("retrieval timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def parse_archive_manifest(
    html: str, *, base_url: str = "https://www.sec.gov/"
) -> list[ArchiveRef]:
    """Discover archives from SEC-published links without assuming URL namespaces."""

    parser = _ManifestLinks()
    parser.feed(html)
    found: dict[tuple[int, int], ArchiveRef] = {}
    for href in parser.hrefs:
        absolute = urljoin(base_url, href)
        parsed = urlparse(absolute)
        if parsed.scheme != "https" or parsed.hostname not in {"sec.gov", "www.sec.gov"}:
            continue
        match = ARCHIVE_RE.search(parsed.path)
        if match is None:
            continue
        ref = ArchiveRef(int(match["year"]), int(match["quarter"]), absolute)
        key = (ref.year, ref.quarter)
        previous = found.get(key)
        if previous is not None and previous.url != ref.url:
            raise ValueError(f"conflicting URLs for {ref.year} Q{ref.quarter}")
        found[key] = ref
    if not found:
        raise ValueError("SEC archive manifest contained no Form 3/4/5 ZIP links")
    return sorted(found.values())


class RawObjectStore:
    """Content-addressed byte store; an existing digest is always verified."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def publish(self, content: bytes, *, suffix: str = "") -> RawObject:
        digest = hashlib.sha256(content).hexdigest()
        directory = self.root / "sha256" / digest[:2]
        directory.mkdir(parents=True, exist_ok=True)
        del suffix  # Media type belongs in retrieval metadata, not object identity.
        destination = directory / digest
        if destination.exists():
            existing = destination.read_bytes()
            if hashlib.sha256(existing).hexdigest() != digest:
                raise RuntimeError(f"content-address collision or corruption at {destination}")
        else:
            fd, temporary_name = tempfile.mkstemp(prefix=f".{digest}.", dir=directory)
            try:
                with os.fdopen(fd, "wb") as handle:
                    handle.write(content)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary_name, destination)
            finally:
                if os.path.exists(temporary_name):
                    os.unlink(temporary_name)
        return RawObject(digest, len(content), destination)


class HistoryStore:
    """Append-only normalized store keyed to immutable archive releases."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            self._ensure_schema(conn)

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        try:
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def _ensure_schema(conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS raw_objects(
                sha256 TEXT PRIMARY KEY CHECK(length(sha256)=64),
                size_bytes INTEGER NOT NULL CHECK(size_bytes >= 0),
                path TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS raw_retrievals(
                retrieval_id TEXT PRIMARY KEY,
                url TEXT NOT NULL,
                retrieved_at_utc TEXT NOT NULL,
                raw_sha256 TEXT NOT NULL REFERENCES raw_objects(sha256),
                status_code INTEGER NOT NULL,
                etag TEXT,
                last_modified TEXT,
                content_type TEXT,
                final_url TEXT NOT NULL,
                upstream_digest TEXT
            );
            CREATE TABLE IF NOT EXISTS archive_releases(
                archive_sha256 TEXT PRIMARY KEY REFERENCES raw_objects(sha256),
                year INTEGER NOT NULL,
                quarter INTEGER NOT NULL CHECK(quarter BETWEEN 1 AND 4),
                source_url TEXT NOT NULL,
                retrieved_at_utc TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sec_submissions(
                archive_sha256 TEXT NOT NULL REFERENCES archive_releases(archive_sha256),
                accession_number TEXT NOT NULL,
                filing_date TEXT NOT NULL,
                period_of_report TEXT,
                original_submission_date TEXT,
                document_type TEXT NOT NULL,
                issuer_cik TEXT NOT NULL,
                issuer_symbol TEXT,
                PRIMARY KEY(archive_sha256, accession_number)
            );
            CREATE TABLE IF NOT EXISTS sec_reporting_owners(
                archive_sha256 TEXT NOT NULL,
                accession_number TEXT NOT NULL,
                owner_cik TEXT NOT NULL,
                PRIMARY KEY(archive_sha256, accession_number, owner_cik),
                FOREIGN KEY(archive_sha256, accession_number)
                    REFERENCES sec_submissions(archive_sha256, accession_number)
            );
            CREATE INDEX IF NOT EXISTS idx_sec_reporting_owners_cik
                ON sec_reporting_owners(owner_cik, archive_sha256, accession_number);
            CREATE TABLE IF NOT EXISTS sec_nonderiv_transactions(
                archive_sha256 TEXT NOT NULL,
                accession_number TEXT NOT NULL,
                transaction_key TEXT NOT NULL,
                transaction_date TEXT,
                transaction_form_type TEXT,
                transaction_code TEXT NOT NULL,
                acquired_disposed_code TEXT,
                is_valid INTEGER NOT NULL CHECK(is_valid IN (0,1)),
                PRIMARY KEY(archive_sha256, accession_number, transaction_key),
                FOREIGN KEY(archive_sha256, accession_number)
                    REFERENCES sec_submissions(archive_sha256, accession_number)
            );
            CREATE TABLE IF NOT EXISTS archive_snapshots(
                snapshot_sha256 TEXT PRIMARY KEY CHECK(length(snapshot_sha256)=64),
                manifest_sha256 TEXT NOT NULL CHECK(length(manifest_sha256)=64),
                created_at_utc TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS archive_snapshot_members(
                snapshot_sha256 TEXT NOT NULL REFERENCES archive_snapshots(snapshot_sha256),
                year INTEGER NOT NULL,
                quarter INTEGER NOT NULL CHECK(quarter BETWEEN 1 AND 4),
                archive_sha256 TEXT NOT NULL REFERENCES archive_releases(archive_sha256),
                PRIMARY KEY(snapshot_sha256, year, quarter)
            );
            """
        )
        immutable_tables = (
            "raw_objects",
            "raw_retrievals",
            "archive_releases",
            "sec_submissions",
            "sec_reporting_owners",
            "sec_nonderiv_transactions",
            "archive_snapshots",
            "archive_snapshot_members",
        )
        for table in immutable_tables:
            conn.execute(
                f"""
                CREATE TRIGGER IF NOT EXISTS {table}_immutable_update
                BEFORE UPDATE ON {table}
                BEGIN SELECT RAISE(ABORT, '{table} is immutable'); END
                """
            )
            conn.execute(
                f"""
                CREATE TRIGGER IF NOT EXISTS {table}_immutable_delete
                BEFORE DELETE ON {table}
                BEGIN SELECT RAISE(ABORT, '{table} is immutable'); END
                """
            )

    def record_retrieval(
        self,
        *,
        url: str,
        retrieved_at: datetime,
        raw_object: RawObject,
        status_code: int,
        etag: str | None,
        last_modified: str | None,
        content_type: str | None,
        final_url: str | None = None,
        upstream_digest: str | None = None,
    ) -> str:
        retrieval_id = str(uuid.uuid4())
        with self.connect() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO raw_objects(sha256,size_bytes,path) VALUES(?,?,?)",
                (raw_object.sha256, raw_object.size_bytes, str(raw_object.path)),
            )
            conn.execute(
                """
                INSERT INTO raw_retrievals(
                    retrieval_id,url,retrieved_at_utc,raw_sha256,status_code,etag,last_modified,
                    content_type,final_url,upstream_digest
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    retrieval_id,
                    url,
                    _utc_text(retrieved_at),
                    raw_object.sha256,
                    status_code,
                    etag,
                    last_modified,
                    content_type,
                    final_url or url,
                    upstream_digest,
                ),
            )
        return retrieval_id

    @staticmethod
    def _zip_members(archive: zipfile.ZipFile) -> dict[str, zipfile.ZipInfo]:
        infos = archive.infolist()
        if len(infos) > _MAX_ARCHIVE_ENTRIES:
            raise ValueError("SEC archive has too many members")
        total = 0
        members: dict[str, zipfile.ZipInfo] = {}
        for info in infos:
            path = Path(info.filename)
            if path.is_absolute() or ".." in path.parts:
                raise ValueError(f"unsafe SEC archive member: {info.filename}")
            if info.file_size > _MAX_MEMBER_BYTES:
                raise ValueError(f"oversized SEC archive member: {info.filename}")
            total += info.file_size
            members[path.name.upper()] = info
        if total > _MAX_TOTAL_UNCOMPRESSED_BYTES:
            raise ValueError("SEC archive expands beyond safety limit")
        missing = _REQUIRED_ARCHIVE_FILES.difference(members)
        if missing:
            raise ValueError(f"SEC archive missing required files: {sorted(missing)}")
        return members

    @staticmethod
    def _rows(
        archive: zipfile.ZipFile, info: zipfile.ZipInfo
    ) -> Iterator[dict[str, str]]:
        with (
            archive.open(info) as binary,
            io.TextIOWrapper(binary, encoding="utf-8-sig", newline="") as text,
        ):
            yield from csv.DictReader(text, delimiter="\t")

    def ingest_archive(
        self,
        ref: ArchiveRef,
        *,
        raw_object: RawObject,
        retrieved_at: datetime,
    ) -> None:
        with self.connect() as conn:
            existing = conn.execute(
                "SELECT year,quarter,source_url FROM archive_releases WHERE archive_sha256=?",
                (raw_object.sha256,),
            ).fetchone()
        if existing is not None:
            existing_identity = (
                int(existing["year"]),
                int(existing["quarter"]),
                str(existing["source_url"]),
            )
            if existing_identity[:2] != (ref.year, ref.quarter):
                raise ValueError("archive digest is bound to different release metadata")
            return
        if hashlib.sha256(raw_object.path.read_bytes()).hexdigest() != raw_object.sha256:
            raise RuntimeError("SEC archive raw-object digest mismatch")
        with zipfile.ZipFile(raw_object.path) as archive:
            members = self._zip_members(archive)
            submissions: list[tuple[object, ...]] = []
            for row in self._rows(archive, members["SUBMISSION.TSV"]):
                accession = row.get("ACCESSION_NUMBER", "").strip()
                filing_date = _parse_sec_date(row.get("FILING_DATE", ""))
                document_type = row.get("DOCUMENT_TYPE", "").strip().upper()
                if not accession or filing_date is None or not document_type:
                    raise ValueError("malformed required SEC submission fields")
                submissions.append(
                    (
                        raw_object.sha256,
                        accession,
                        filing_date.isoformat(),
                        (_parse_sec_date(row.get("PERIOD_OF_REPORT", "")) or ""),
                        (_parse_sec_date(row.get("DATE_OF_ORIG_SUB", "")) or ""),
                        document_type,
                        _normalize_cik(row.get("ISSUERCIK", "")),
                        row.get("ISSUERTRADINGSYMBOL", "").strip() or None,
                    )
                )
            owners: list[tuple[str, str, str]] = []
            for row in self._rows(archive, members["REPORTINGOWNER.TSV"]):
                owners.append(
                    (
                        raw_object.sha256,
                        row.get("ACCESSION_NUMBER", "").strip(),
                        _normalize_cik(row.get("RPTOWNERCIK", "")),
                    )
                )
            transactions: list[tuple[object, ...]] = []
            for row_number, row in enumerate(
                self._rows(archive, members["NONDERIV_TRANS.TSV"]), start=1
            ):
                code = row.get("TRANS_CODE", "").strip().upper()
                if code not in {"P", "S"}:
                    continue
                transaction_date = _parse_sec_date(row.get("TRANS_DATE", ""))
                acquired_disposed = row.get("TRANS_ACQUIRED_DISP_CD", "").strip().upper()
                is_valid = transaction_date is not None and (
                    (code == "P" and acquired_disposed == "A")
                    or (code == "S" and acquired_disposed == "D")
                )
                transaction_key = row.get("NONDERIV_TRANS_SK", "").strip() or str(row_number)
                transactions.append(
                    (
                        raw_object.sha256,
                        row.get("ACCESSION_NUMBER", "").strip(),
                        transaction_key,
                        transaction_date.isoformat() if transaction_date else None,
                        row.get("TRANS_FORM_TYPE", "").strip().upper() or None,
                        code,
                        acquired_disposed or None,
                        int(is_valid),
                    )
                )
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT OR IGNORE INTO raw_objects(sha256,size_bytes,path) VALUES(?,?,?)",
                (raw_object.sha256, raw_object.size_bytes, str(raw_object.path)),
            )
            conn.execute(
                """
                INSERT INTO archive_releases(
                    archive_sha256,year,quarter,source_url,retrieved_at_utc
                ) VALUES(?,?,?,?,?)
                """,
                (raw_object.sha256, ref.year, ref.quarter, ref.url, _utc_text(retrieved_at)),
            )
            conn.executemany(
                """
                INSERT INTO sec_submissions(
                    archive_sha256,accession_number,filing_date,period_of_report,
                    original_submission_date,document_type,issuer_cik,issuer_symbol
                ) VALUES(?,?,?,?,?,?,?,?)
                """,
                [
                    (
                        *row[:3],
                        row[3].isoformat() if isinstance(row[3], date) else None,
                        row[4].isoformat() if isinstance(row[4], date) else None,
                        *row[5:],
                    )
                    for row in submissions
                ],
            )
            conn.executemany(
                "INSERT INTO sec_reporting_owners VALUES(?,?,?)", owners
            )
            conn.executemany(
                "INSERT INTO sec_nonderiv_transactions VALUES(?,?,?,?,?,?,?,?)",
                transactions,
            )

    def create_snapshot(
        self,
        *,
        manifest_sha256: str,
        members: Sequence[tuple[int, int, str]],
        created_at: datetime,
    ) -> str:
        ordered = sorted(members)
        if len({(year, quarter) for year, quarter, _ in ordered}) != len(ordered):
            raise ValueError("archive snapshot has duplicate quarters")
        with self.connect() as conn:
            if conn.execute(
                "SELECT 1 FROM raw_objects WHERE sha256=?", (manifest_sha256,)
            ).fetchone() is None:
                raise ValueError("archive snapshot manifest raw object is missing")
            for year, quarter, digest in ordered:
                release = conn.execute(
                    "SELECT year,quarter FROM archive_releases WHERE archive_sha256=?",
                    (digest,),
                ).fetchone()
                if release is None or (int(release["year"]), int(release["quarter"])) != (
                    year,
                    quarter,
                ):
                    raise ValueError("archive snapshot member metadata mismatch")
        body: dict[str, Any] = {
            "manifest_sha256": manifest_sha256,
            "members": [
                {"year": year, "quarter": quarter, "archive_sha256": digest}
                for year, quarter, digest in ordered
            ],
        }
        snapshot_sha = hashlib.sha256(rfc8785.dumps(body)).hexdigest()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT OR IGNORE INTO archive_snapshots VALUES(?,?,?)",
                (snapshot_sha, manifest_sha256, _utc_text(created_at)),
            )
            conn.executemany(
                "INSERT OR IGNORE INTO archive_snapshot_members VALUES(?,?,?,?)",
                [(snapshot_sha, year, quarter, digest) for year, quarter, digest in ordered],
            )
        return snapshot_sha

    def archived_object_for_url(
        self, url: str, *, year: int, quarter: int
    ) -> RawObject | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT o.sha256,o.size_bytes,o.path
                FROM raw_retrievals r
                JOIN archive_releases a ON a.archive_sha256=r.raw_sha256
                JOIN raw_objects o ON o.sha256=a.archive_sha256
                WHERE (r.url=? OR r.final_url=?) AND a.year=? AND a.quarter=?
                ORDER BY r.retrieved_at_utc DESC LIMIT 1
                """,
                (url, url, year, quarter),
            ).fetchone()
        if row is None:
            return None
        raw_object = RawObject(
            sha256=str(row["sha256"]),
            size_bytes=int(row["size_bytes"]),
            path=Path(str(row["path"])),
        )
        if not raw_object.path.is_file():
            return None
        if raw_object.path.stat().st_size != raw_object.size_bytes:
            return None
        if hashlib.sha256(raw_object.path.read_bytes()).hexdigest() != raw_object.sha256:
            return None
        return raw_object

    def owner_filings(
        self, snapshot_sha256: str, owner_cik: str, *, cutoff_date: date
    ) -> list[OwnerFiling]:
        normalized_owner = _normalize_cik(owner_cik)
        with self.connect() as conn:
            submission_rows = conn.execute(
                """
                SELECT DISTINCT s.archive_sha256,s.accession_number,s.filing_date,
                       s.document_type,s.issuer_cik,s.original_submission_date
                FROM archive_snapshot_members m
                JOIN sec_reporting_owners target
                  ON target.archive_sha256=m.archive_sha256 AND target.owner_cik=?
                JOIN sec_submissions s
                  ON s.archive_sha256=target.archive_sha256
                 AND s.accession_number=target.accession_number
                WHERE m.snapshot_sha256=? AND s.filing_date < ?
                  AND s.document_type IN ('4','4/A','5','5/A')
                ORDER BY s.filing_date,s.accession_number
                """,
                (normalized_owner, snapshot_sha256, cutoff_date.isoformat()),
            ).fetchall()
            filings: list[OwnerFiling] = []
            for row in submission_rows:
                key = (str(row["archive_sha256"]), str(row["accession_number"]))
                owner_rows = conn.execute(
                    """
                    SELECT owner_cik FROM sec_reporting_owners
                    WHERE archive_sha256=? AND accession_number=? ORDER BY owner_cik
                    """,
                    key,
                ).fetchall()
                transaction_rows = conn.execute(
                    """
                    SELECT transaction_date,is_valid FROM sec_nonderiv_transactions
                    WHERE archive_sha256=? AND accession_number=? ORDER BY transaction_key
                    """,
                    key,
                ).fetchall()
                transactions = tuple(
                    date.fromisoformat(str(transaction["transaction_date"]))
                    for transaction in transaction_rows
                    if int(transaction["is_valid"]) == 1
                )
                filings.append(
                    OwnerFiling(
                        accession_number=str(row["accession_number"]),
                        filing_date=date.fromisoformat(str(row["filing_date"])),
                        form_type=str(row["document_type"]),
                        issuer_cik=str(row["issuer_cik"]),
                        owner_ciks=tuple(str(owner["owner_cik"]) for owner in owner_rows),
                        original_submission_date=(
                            date.fromisoformat(str(row["original_submission_date"]))
                            if row["original_submission_date"]
                            else None
                        ),
                        transaction_dates=transactions,
                        has_invalid_transaction=any(
                            int(transaction["is_valid"]) == 0
                            for transaction in transaction_rows
                        ),
                    )
                )
        return filings


def _filing_fingerprint(filing: OwnerFiling) -> tuple[object, ...]:
    return (
        filing.form_type,
        filing.issuer_cik,
        filing.owner_ciks,
        tuple(value.isoformat() for value in filing.transaction_dates),
        filing.has_invalid_transaction,
    )


def _effective_filings(filings: Sequence[OwnerFiling]) -> tuple[list[OwnerFiling], str | None]:
    originals = [filing for filing in filings if not filing.form_type.endswith("/A")]
    amendments = [filing for filing in filings if filing.form_type.endswith("/A")]
    replacements: dict[str, OwnerFiling] = {}
    for amendment in amendments:
        if amendment.original_submission_date is None:
            return [], "unresolved_amendment"
        base_form = amendment.form_type.removesuffix("/A")
        candidates = [
            filing
            for filing in originals
            if filing.form_type == base_form
            and filing.issuer_cik == amendment.issuer_cik
            and filing.owner_ciks == amendment.owner_ciks
            and filing.filing_date == amendment.original_submission_date
        ]
        if len(candidates) != 1:
            return [], "unresolved_amendment"
        original = candidates[0]
        previous = replacements.get(original.accession_number)
        if previous is None or amendment.filing_date > previous.filing_date:
            replacements[original.accession_number] = amendment
        elif amendment.filing_date == previous.filing_date:
            if _filing_fingerprint(amendment) != _filing_fingerprint(previous):
                return [], "unresolved_amendment_order"
            if amendment.accession_number < previous.accession_number:
                replacements[original.accession_number] = amendment
    effective = [replacements.get(filing.accession_number, filing) for filing in originals]
    return sorted(effective, key=lambda item: (item.filing_date, item.accession_number)), None


def _history_digest(
    owner_cik: str,
    classification_year: int,
    filings: Sequence[OwnerFiling],
    coverage: Coverage,
) -> str:
    body: dict[str, Any] = {
        "classifier_version": CLASSIFIER_VERSION,
        "owner_cik": owner_cik,
        "classification_year": classification_year,
        "coverage": {
            "complete_from": coverage.complete_from.isoformat(),
            "complete_through": coverage.complete_through.isoformat(),
            "prehistory_complete": coverage.prehistory_complete,
            "missing_quarters": list(coverage.missing_quarters),
            "prehistory_evidence_sha256": coverage.prehistory_evidence_sha256,
        },
        "filings": [
            {
                "accession_number": filing.accession_number,
                "filing_date": filing.filing_date.isoformat(),
                "form_type": filing.form_type,
                "issuer_cik": filing.issuer_cik,
                "owner_ciks": list(filing.owner_ciks),
                "original_submission_date": (
                    filing.original_submission_date.isoformat()
                    if filing.original_submission_date
                    else None
                ),
                "transaction_dates": [value.isoformat() for value in filing.transaction_dates],
                "has_invalid_transaction": filing.has_invalid_transaction,
            }
            for filing in sorted(
                filings, key=lambda item: (item.filing_date, item.accession_number)
            )
        ],
    }
    return hashlib.sha256(rfc8785.dumps(body)).hexdigest()


def classify_owner(
    *,
    owner_cik: str,
    classification_year: int,
    filings: Sequence[OwnerFiling],
    coverage: Coverage,
) -> Classification:
    """Apply the frozen trader-calendar rule using information public before Jan 1."""

    normalized_owner = _normalize_cik(owner_cik)
    cutoff = date(classification_year, 1, 1)
    visible = [filing for filing in filings if filing.filing_date < cutoff]
    digest = _history_digest(normalized_owner, classification_year, visible, coverage)
    left_censored = not coverage.prehistory_complete

    def result(
        state: ClassificationState, reason: str, routine_since: int | None = None
    ) -> Classification:
        complete = (
            coverage.prehistory_complete
            and not coverage.missing_quarters
            and coverage.complete_through >= date(classification_year - 1, 12, 31)
        )
        return Classification(
            state=state,
            reason=reason,
            owner_cik=normalized_owner,
            classification_year=classification_year,
            cutoff_date=cutoff,
            history_coverage_complete=complete,
            left_censored=left_censored,
            routine_since_year=routine_since,
            history_input_sha256=digest,
        )

    if any(normalized_owner not in filing.owner_ciks for filing in visible):
        return result("unpartitionable", "owner_identity_mismatch")
    if any(len(filing.owner_ciks) != 1 for filing in visible):
        return result("unpartitionable", "ambiguous_owner_history")
    if any(filing.has_invalid_transaction for filing in visible):
        return result("unpartitionable", "invalid_transaction")

    effective, resolution_error = _effective_filings(visible)
    if resolution_error is not None:
        return result("unpartitionable", resolution_error)

    months_by_year: dict[int, set[int]] = defaultdict(set)
    for filing in effective:
        for transaction_date in filing.transaction_dates:
            if transaction_date < cutoff:
                months_by_year[transaction_date.year].add(transaction_date.month)

    first_complete_year = coverage.complete_from.year
    if coverage.complete_from != date(coverage.complete_from.year, 1, 1):
        first_complete_year += 1
    earliest_window_end = max(first_complete_year + 2, 3)
    for end_year in range(earliest_window_end, classification_year):
        years = (end_year - 2, end_year - 1, end_year)
        month_sets = [months_by_year[year] for year in years]
        if all(month_sets) and set.intersection(*month_sets):
            return result("routine", "absorbing_routine_pattern", end_year + 1)

    if coverage.complete_through < date(classification_year - 1, 12, 31):
        return result("unpartitionable", "coverage_not_current")
    if coverage.missing_quarters:
        return result("unpartitionable", "coverage_gap")

    target_years = (
        classification_year - 3,
        classification_year - 2,
        classification_year - 1,
    )
    target_sets = [months_by_year[year] for year in target_years]
    if not all(target_sets):
        return result("unpartitionable", "incomplete_trade_years")
    if set.intersection(*target_sets):
        return result("routine", "same_month_in_three_prior_years", classification_year)
    if left_censored:
        return result("unpartitionable", "left_censored")
    return result("opportunistic", "disjoint_months_in_three_prior_years")


def quarter_labels(refs: Iterable[ArchiveRef]) -> tuple[str, ...]:
    return tuple(f"{ref.year}Q{ref.quarter}" for ref in sorted(refs))


def _quarter_range(start: tuple[int, int], end: tuple[int, int]) -> list[tuple[int, int]]:
    if start > end:
        return []
    year, quarter = start
    values: list[tuple[int, int]] = []
    while (year, quarter) <= end:
        values.append((year, quarter))
        quarter += 1
        if quarter == 5:
            year += 1
            quarter = 1
    return values


def select_complete_archives(
    refs: Sequence[ArchiveRef], *, through: tuple[int, int] | None = None
) -> list[ArchiveRef]:
    by_quarter = {(ref.year, ref.quarter): ref for ref in refs if ref.year >= 2006}
    if not by_quarter:
        raise ValueError("SEC archive manifest has no coverage from 2006")
    selected_through = through or max(by_quarter)
    expected = _quarter_range((2006, 1), selected_through)
    missing = [
        f"{year}Q{quarter}"
        for year, quarter in expected
        if (year, quarter) not in by_quarter
    ]
    if missing:
        raise ValueError(f"SEC archive manifest has coverage gaps: {missing}")
    return [by_quarter[key] for key in expected]


def _record_resource(
    *,
    store: HistoryStore,
    raw_store: RawObjectStore,
    requested_url: str,
    resource: SecResource,
    retrieved_at: datetime,
    suffix: str,
) -> RawObject:
    raw_object = raw_store.publish(resource.content, suffix=suffix)
    store.record_retrieval(
        url=requested_url,
        retrieved_at=retrieved_at,
        raw_object=raw_object,
        status_code=resource.status_code,
        etag=resource.etag,
        last_modified=resource.last_modified,
        content_type=resource.content_type,
        final_url=resource.final_url,
        upstream_digest=resource.upstream_digest,
    )
    return raw_object


def sync_bulk_archives(
    *,
    client: SecHttpClient,
    store: HistoryStore,
    raw_store: RawObjectStore,
    through: tuple[int, int] | None = None,
    refresh: bool = False,
    now_fn: Callable[[], datetime] | None = None,
) -> SyncResult:
    """Materialize one complete, immutable selection of SEC quarterly releases."""

    clock = now_fn or (lambda: datetime.now(UTC))
    manifest_retrieved_at = clock()
    manifest_resource = client.get_resource(ARCHIVE_MANIFEST_URL)
    manifest_object = _record_resource(
        store=store,
        raw_store=raw_store,
        requested_url=ARCHIVE_MANIFEST_URL,
        resource=manifest_resource,
        retrieved_at=manifest_retrieved_at,
        suffix=".html",
    )
    refs = select_complete_archives(
        parse_archive_manifest(manifest_resource.text()), through=through
    )
    members: list[tuple[int, int, str]] = []
    downloaded = 0
    reused = 0
    for ref in refs:
        raw_object = (
            None
            if refresh
            else store.archived_object_for_url(
                ref.url, year=ref.year, quarter=ref.quarter
            )
        )
        if raw_object is None:
            retrieved_at = clock()
            resource = client.get_resource(ref.url)
            if not resource.content.startswith(b"PK"):
                raise ValueError(f"SEC archive is not a ZIP file: {ref.url}")
            raw_object = _record_resource(
                store=store,
                raw_store=raw_store,
                requested_url=ref.url,
                resource=resource,
                retrieved_at=retrieved_at,
                suffix=".zip",
            )
            store.ingest_archive(ref, raw_object=raw_object, retrieved_at=retrieved_at)
            downloaded += 1
        else:
            reused += 1
        members.append((ref.year, ref.quarter, raw_object.sha256))
    snapshot_sha = store.create_snapshot(
        manifest_sha256=manifest_object.sha256,
        members=members,
        created_at=clock(),
    )
    labels = quarter_labels(refs)
    return SyncResult(
        snapshot_sha256=snapshot_sha,
        manifest_sha256=manifest_object.sha256,
        archive_count=len(refs),
        downloaded_count=downloaded,
        reused_count=reused,
        first_quarter=labels[0],
        last_quarter=labels[-1],
    )
