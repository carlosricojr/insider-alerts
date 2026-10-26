"""Versioned NYSE baseline for acquisition commissioning; never trial-readiness proof.

Live and frozen V1 consumers do not import this module. Annual facts do not prove
operational freshness or the absence of extraordinary closures.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import sqlite3
import tempfile
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import rfc8785

FORMAT = "research-calendar-v2-baseline-1"
APPLICATION_ID = 0x49414332
START, END = date(2026, 1, 1), date(2028, 12, 31)
NY = ZoneInfo("America/New_York")
PDF_SHA256 = "76775c27848d19dc15d7cac8927f6e85f63605aa715fe5401ed486aa722e1d23"
PDF_URL = (
    "https://s2.q4cdn.com/154085107/files/doc_news/"
    "NYSE-Group-Announces-2026-2027-and-2028-Holiday-and-Early-Closings-Calendar-2025.pdf"
)
PUBLISHED_ON = date(2025, 12, 23)
HOLIDAYS = frozenset(
    date.fromisoformat(value)
    for value in (
        "2026-01-01",
        "2026-01-19",
        "2026-02-16",
        "2026-04-03",
        "2026-05-25",
        "2026-06-19",
        "2026-07-03",
        "2026-09-07",
        "2026-11-26",
        "2026-12-25",
        "2027-01-01",
        "2027-01-18",
        "2027-02-15",
        "2027-03-26",
        "2027-05-31",
        "2027-06-18",
        "2027-07-05",
        "2027-09-06",
        "2027-11-25",
        "2027-12-24",
        "2028-01-17",
        "2028-02-21",
        "2028-04-14",
        "2028-05-29",
        "2028-06-19",
        "2028-07-04",
        "2028-09-04",
        "2028-11-23",
        "2028-12-25",
    )
)
EARLY_CLOSES = frozenset(
    date.fromisoformat(value)
    for value in (
        "2026-11-27",
        "2026-12-24",
        "2027-11-26",
        "2028-07-03",
        "2028-11-24",
    )
)
Status = Literal["open", "closed", "unknown"]
NoticeKind = Literal["closed", "unknown"]


class CalendarError(ValueError):
    """Invalid provenance, unsupported coverage, or inconsistent journal."""


def _hash(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise CalendarError("naive observation time")
    return value.astimezone(UTC)


def _text(value: datetime) -> str:
    return _utc(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _now() -> datetime:
    return datetime.now(UTC)


def _observation(value: datetime) -> datetime:
    observed = _utc(value)
    if observed > _now() + timedelta(seconds=5):
        raise CalendarError("observation exceeds current clock allowance")
    return observed


@dataclass(frozen=True)
class Day:
    day: date
    state: Status
    opens_at: datetime | None = None
    closes_at: datetime | None = None

    def record(self) -> dict[str, Any]:
        return {
            "date": str(self.day),
            "state": self.state,
            "open": _text(self.opens_at) if self.opens_at else None,
            "close": _text(self.closes_at) if self.closes_at else None,
        }


def _baseline_days() -> tuple[Day, ...]:
    result = []
    for offset in range((END - START).days + 1):
        day = START + timedelta(days=offset)
        if day.weekday() >= 5 or day in HOLIDAYS:
            result.append(Day(day, "closed"))
        else:
            result.append(
                Day(
                    day,
                    "open",
                    datetime.combine(day, time(9, 30), NY),
                    datetime.combine(day, time(13 if day in EARLY_CLOSES else 16), NY),
                )
            )
    return tuple(result)


@dataclass(frozen=True)
class Version:
    digest: str
    parent: str | None
    observed_at: datetime
    days: tuple[Day, ...]
    ancestors: tuple[str, ...]

    def day(self, value: date) -> Day:
        if not START <= value <= END:
            return Day(value, "unknown")
        return self.days[(value - START).days]


@dataclass(frozen=True)
class Binding:
    digest: str
    version_digest: str
    anchor: date
    sessions: tuple[Day, ...]


def bind(version: Version, anchor: date, count: int = 10) -> Binding:
    """Count the open anchor as session one. Unknown is never skipped."""
    if type(anchor) is not date:
        raise CalendarError("anchor must be a date")
    if type(count) is not int or not 1 <= count <= 252:
        raise CalendarError("invalid session count")
    if version.day(anchor).state != "open":
        raise CalendarError("entry is not a known open session")
    sessions: list[Day] = []
    day = anchor
    while len(sessions) < count:
        current = version.day(day)
        if current.state == "unknown":
            raise CalendarError(f"unknown calendar date: {day}")
        if current.state == "open":
            sessions.append(current)
        day += timedelta(days=1)
    material: dict[str, Any] = {
        "version": version.digest,
        "anchor": str(anchor),
        "convention": "open_anchor_is_session_one",
        "sessions": [item.record() for item in sessions],
    }
    return Binding(_hash(rfc8785.dumps(material)), version.digest, anchor, tuple(sessions))


def binding_conflicts(
    binding: Binding, version: Version, *, bound_version: Version
) -> tuple[date, ...]:
    """Do not repair a frozen binding; expose subsequent schedule contradictions."""
    if binding != bind(bound_version, binding.anchor, len(binding.sessions)):
        raise CalendarError("binding differs from original validated version")
    if bound_version.digest not in (*version.ancestors, version.digest):
        raise CalendarError("comparison version is not a descendant")
    expected = {item.day: item for item in binding.sessions}
    # Skipped dates were known closed when bound. Unknown on one of those dates
    # also invalidates the frozen session count, even if every open day agrees.
    span = (binding.sessions[-1].day - binding.anchor).days + 1
    dates = (binding.anchor + timedelta(days=offset) for offset in range(span))
    return tuple(day for day in dates if version.day(day) != expected.get(day, Day(day, "closed")))


def missing_bar_dates(binding: Binding, observed_dates: frozenset[date]) -> tuple[date, ...]:
    """Presence only; not final-bar, receipt, price, or scientific-readiness proof."""
    return tuple(item.day for item in binding.sessions if item.day not in observed_dates)


@dataclass(frozen=True)
class Notice:
    kind: NoticeKind
    dates: tuple[date, ...]
    reason: str
    source_bytes: bytes
    source_url: str
    published_on: date
    review_reference: str

    def material(self) -> dict[str, Any]:
        if self.kind not in {"closed", "unknown"} or not self.dates:
            raise CalendarError("notice must explicitly close or mark dates unknown")
        if type(self.published_on) is not date or any(type(day) is not date for day in self.dates):
            raise CalendarError("notice dates must be dates")
        if len(set(self.dates)) != len(self.dates) or any(
            not START <= day <= END for day in self.dates
        ):
            raise CalendarError("duplicate or unsupported notice dates")
        _source(self.source_bytes, self.source_url, self.review_reference)
        if not self.reason.strip():
            raise CalendarError("notice reason required")
        return {
            "kind": self.kind,
            "dates": sorted(map(str, self.dates)),
            "reason": self.reason,
            "source_sha256": _hash(self.source_bytes),
            "source_url": self.source_url,
            "published_on": str(self.published_on),
            "review_reference": self.review_reference,
        }


def _source(raw: bytes, url: str, review: str) -> None:
    parsed = urlsplit(url)
    if (
        not 0 < len(raw) <= 8 * 1024 * 1024
        or not review.strip()
        or parsed.scheme != "https"
        or parsed.hostname
        not in {
            "nyse.com",
            "www.nyse.com",
            "ir.theice.com",
            "s2.q4cdn.com",
        }
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or parsed.port is not None
        or (parsed.hostname == "s2.q4cdn.com" and not parsed.path.startswith("/154085107/"))
    ):
        raise CalendarError("invalid source custody metadata")


TABLES = (
    "CREATE TABLE calendar_sources(sha256 TEXT PRIMARY KEY, payload BLOB NOT NULL)",
    "CREATE TABLE calendar_versions(sequence INTEGER PRIMARY KEY, sha256 TEXT UNIQUE NOT NULL, "
    "payload BLOB NOT NULL)",
)
TRIGGERS = tuple(
    f"CREATE TRIGGER {table}_no_{operation.lower()} BEFORE {operation} ON {table} "
    "BEGIN SELECT RAISE(ABORT,'append-only calendar journal'); END"
    for table in ("calendar_sources", "calendar_versions")
    for operation in ("UPDATE", "DELETE")
)


def _schema(conn: sqlite3.Connection) -> list[tuple[str, str, str]]:
    return conn.execute(
        "SELECT type,name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
    ).fetchall()


@lru_cache(maxsize=1)
def _expected_schema() -> list[tuple[str, str, str]]:
    with contextlib.closing(sqlite3.connect(":memory:")) as conn:
        for sql in (*TABLES, *TRIGGERS):
            conn.execute(sql)
        return _schema(conn)


def _connect(path: Path, *, write: bool = False) -> sqlite3.Connection:
    resolved = path.resolve(strict=True)
    conn = sqlite3.connect(
        resolved.as_uri() + ("?mode=rw" if write else "?mode=ro"), uri=True, timeout=5
    )
    try:
        if (
            conn.execute("PRAGMA application_id").fetchone()[0] != APPLICATION_ID
            or conn.execute("PRAGMA user_version").fetchone()[0] != 1
            or conn.execute("PRAGMA journal_mode").fetchone()[0] != "delete"
            or _schema(conn) != _expected_schema()
        ):
            raise CalendarError("foreign or modified calendar schema")
        if conn.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
            raise CalendarError("calendar database integrity check failed")
        conn.execute("PRAGMA recursive_triggers=ON")
        if write:
            conn.execute("PRAGMA synchronous=FULL")
        else:
            conn.execute("PRAGMA query_only=ON")
        return conn
    except BaseException:
        conn.close()
        raise


def _changed(days: tuple[Day, ...], material: dict[str, Any]) -> tuple[Day, ...]:
    selected = set(material["dates"])
    return tuple(
        Day(item.day, material["kind"]) if str(item.day) in selected else item for item in days
    )


def _load(conn: sqlite3.Connection) -> tuple[list[Version], list[dict[str, Any]]]:
    try:
        return _load_records(conn)
    except (
        TypeError,
        KeyError,
        ValueError,
        AttributeError,
        ArithmeticError,
        RecursionError,
    ) as exc:
        if isinstance(exc, CalendarError):
            raise
        raise CalendarError("malformed calendar journal record") from exc


def _load_records(conn: sqlite3.Connection) -> tuple[list[Version], list[dict[str, Any]]]:
    sources = dict(conn.execute("SELECT sha256,payload FROM calendar_sources"))
    if any(_hash(raw) != sha for sha, raw in sources.items()):
        raise CalendarError("source digest mismatch")
    versions: list[Version] = []
    records = []
    used = set()
    for sequence, sha, raw in conn.execute(
        "SELECT sequence,sha256,payload FROM calendar_versions ORDER BY sequence"
    ):
        if sequence != len(versions) + 1 or _hash(raw) != sha:
            raise CalendarError("version sequence/digest mismatch")
        record = json.loads(raw)
        if (
            rfc8785.dumps(record) != raw
            or set(record)
            != {
                "format",
                "observed_at_utc",
                "parent",
                "material",
                "days",
            }
            or record["format"] != FORMAT
        ):
            raise CalendarError("noncanonical/unsupported calendar version")
        observed = _utc(datetime.fromisoformat(record["observed_at_utc"]))
        if _text(observed) != record["observed_at_utc"]:
            raise CalendarError("noncanonical observation timestamp")
        material = record["material"]
        if set(material) != {
            "kind",
            "dates",
            "reason",
            "source_sha256",
            "source_url",
            "published_on",
            "review_reference",
        }:
            raise CalendarError("invalid source material")
        source_sha = material["source_sha256"]
        if source_sha not in sources:
            raise CalendarError("missing source bytes")
        _source(sources[source_sha], material["source_url"], material["review_reference"])
        used.add(source_sha)
        if date.fromisoformat(material["published_on"]) > observed.astimezone(NY).date():
            raise CalendarError("source publication follows observation")
        if not versions:
            if (
                record["parent"] is not None
                or material["kind"] != "baseline"
                or source_sha != PDF_SHA256
                or material["source_url"] != PDF_URL
                or material["published_on"] != str(PUBLISHED_ON)
                or material["dates"] != []
                or material["reason"] != "published annual baseline"
            ):
                raise CalendarError("invalid baseline")
            days = _baseline_days()
        else:
            prior = versions[-1]
            if record["parent"] != prior.digest or observed <= prior.observed_at:
                raise CalendarError("calendar parent/clock mismatch")
            notice = Notice(
                material["kind"],
                tuple(map(date.fromisoformat, material["dates"])),
                material["reason"],
                sources[source_sha],
                material["source_url"],
                date.fromisoformat(material["published_on"]),
                material["review_reference"],
            )
            if notice.material() != material:
                raise CalendarError("notice material mismatch")
            days = _changed(prior.days, material)
            if days == prior.days:
                raise CalendarError("notice changes nothing")
        if record["days"] != [item.record() for item in days]:
            raise CalendarError("calendar facts mismatch")
        ancestors = tuple(prior.digest for prior in versions)
        versions.append(Version(sha, record["parent"], observed, days, ancestors))
        records.append(record)
    if not versions or used != set(sources):
        raise CalendarError("empty journal or unbound sources")
    return versions, records


def _insert(
    conn: sqlite3.Connection,
    raw_source: bytes,
    material: dict[str, Any],
    days: tuple[Day, ...],
    observed: datetime,
    parent: str | None,
    sequence: int,
) -> None:
    record = {
        "format": FORMAT,
        "observed_at_utc": _text(observed),
        "parent": parent,
        "material": material,
        "days": [item.record() for item in days],
    }
    raw = rfc8785.dumps(record)
    conn.execute(
        "INSERT OR IGNORE INTO calendar_sources VALUES(?,?)", (_hash(raw_source), raw_source)
    )
    conn.execute("INSERT INTO calendar_versions VALUES(?,?,?)", (sequence, _hash(raw), raw))


def create_journal(
    path: Path, *, baseline_pdf: bytes, review_reference: str, observed_at: datetime
) -> None:
    """Explicit exclusive publication; review_reference records, not authenticates, approval."""
    observed = _observation(observed_at)
    if _hash(baseline_pdf) != PDF_SHA256:
        raise CalendarError("unreviewed baseline PDF bytes")
    _source(baseline_pdf, PDF_URL, review_reference)
    if path.exists():
        raise FileExistsError(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(prefix=".calendar-", suffix=".sqlite", dir=path.parent)
    os.close(descriptor)
    staging = Path(name)
    try:
        with contextlib.closing(sqlite3.connect(staging)) as conn:
            conn.execute("PRAGMA journal_mode=DELETE")
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute(f"PRAGMA application_id={APPLICATION_ID}")
            conn.execute("PRAGMA user_version=1")
            with conn:
                for sql in (*TABLES, *TRIGGERS):
                    conn.execute(sql)
                material = {
                    "kind": "baseline",
                    "dates": [],
                    "reason": "published annual baseline",
                    "source_sha256": PDF_SHA256,
                    "source_url": PDF_URL,
                    "published_on": str(PUBLISHED_ON),
                    "review_reference": review_reference,
                }
                _insert(conn, baseline_pdf, material, _baseline_days(), observed, None, 1)
                _load(conn)
        os.link(staging, path)  # Atomic non-replacing publication on the same filesystem.
    finally:
        staging.unlink(missing_ok=True)


class CalendarJournal:
    def __init__(self, path: Path) -> None:
        self.path = path

    def versions(self) -> tuple[Version, ...]:
        with contextlib.closing(_connect(self.path)) as conn:
            conn.execute("BEGIN")
            return tuple(_load(conn)[0])

    def as_of(self, observed_at: datetime) -> Version | None:
        cutoff = _utc(observed_at)
        return next((v for v in reversed(self.versions()) if v.observed_at <= cutoff), None)

    def append_notice(
        self, notice: Notice, *, expected_parent: str, observed_at: datetime
    ) -> Version:
        observed = _observation(observed_at)
        material = notice.material()
        with contextlib.closing(_connect(self.path, write=True)) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            versions, records = _load(conn)
            prior = versions[-1]
            if records[-1]["material"] == material and prior.parent == expected_parent:
                return prior  # Exact retry after committed success; original clock is preserved.
            if prior.digest != expected_parent:
                raise CalendarError("expected parent changed")
            if observed <= prior.observed_at:
                raise CalendarError("observation clock must increase")
            days = _changed(prior.days, material)
            if days == prior.days:
                raise CalendarError("notice changes nothing")
            _insert(
                conn, notice.source_bytes, material, days, observed, prior.digest, len(versions) + 1
            )
            return _load(conn)[0][-1]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--journal", type=Path, required=True)
    parser.add_argument("--create-from-pdf", type=Path)
    parser.add_argument("--review-reference")
    args = parser.parse_args()
    if args.review_reference and not args.create_from_pdf:
        parser.error("--review-reference requires --create-from-pdf")
    if args.create_from_pdf:
        if not args.review_reference:
            parser.error("--review-reference is required for explicit creation")
        create_journal(
            args.journal,
            baseline_pdf=args.create_from_pdf.read_bytes(),
            review_reference=args.review_reference,
            observed_at=datetime.now(UTC),
        )
    versions = CalendarJournal(args.journal).versions()
    print(
        json.dumps(
            {
                "readiness": "BASELINE_ONLY",
                "versions": len(versions),
                "head": versions[-1].digest,
                "observed_at": _text(versions[-1].observed_at),
                "coverage": [str(START), str(END)],
                "trial_activated": False,
            }
        )
    )


if __name__ == "__main__":
    main()
