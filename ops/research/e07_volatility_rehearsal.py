"""Hidden, order-incapable acquisition rehearsal for E07 volatility inputs."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import subprocess
import sys
from collections import Counter
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from time import monotonic
from typing import Any

import rfc8785

from e07_volatility_exits.features import BASE_WIDTH, FEATURE_CUTOFF
from insider_alerts.config import Settings
from insider_alerts.execution.observer import COLUMNS, safe_path, valid_symbol
from insider_alerts.execution.windows_job import ensure_kill_on_close_process_tree
from insider_alerts.research import calendar_v2 as calendar
from insider_alerts.research.commissioning_custody import inventory, snapshot, source_value
from insider_alerts.research.commissioning_journal import Journal, digest
from insider_alerts.research.commissioning_receipts import qualified_identity
from insider_alerts.research.commissioning_source import CommissioningSource, timestamp
from insider_alerts.research.commissioning_window import parse, session_for
from insider_alerts.research.commissioning_worker import (
    LockBusy,
    clean_main,
    exclusive_lock,
    implementation_digest,
)
from insider_alerts.sec.client import SecHttpClient, SecHttpError
from ops.research.e07_volatility_input_audit import _history
from ops.research.e07_volatility_rehearsal_contract import (
    DIRECTORY_URL,
    DirectoryReceipt,
    bar_proof,
    decision_material,
    issuer_directory,
)

CONTRACT = "e07-volatility-custody-rehearsal-v1"
FOLDER = "data/research-e07-volatility-rehearsal"
REHEARSAL_SESSIONS = 2
HOLD_SESSIONS = 10
PACE = timedelta(minutes=5)
MAX_ATTEMPTS = 3
DIRECTORY_RETRY = timedelta(minutes=15)
READ_WINDOW = timedelta(minutes=2)


def local(day: date, hour: time) -> datetime:
    return datetime.combine(day, hour, calendar.NY).astimezone(UTC)


def last_rth_close(version: calendar.Version, day: date) -> datetime:
    close = version.day(day).closes_at
    if close is None:
        raise ValueError("unavailable session close")
    return close.astimezone(UTC)


def utc_now() -> datetime:
    return datetime.now(UTC)


class ClockDiscontinuity(RuntimeError):
    """The wall clock disagrees with the current process's monotonic clock."""


def checked_clock(
    clock: Callable[[], datetime], ticks: Callable[[], float]
) -> Callable[[], datetime]:
    started, initial = clock(), ticks()
    timestamp(started)
    latest = started

    def read() -> datetime:
        nonlocal latest
        value = clock()
        timestamp(value)
        elapsed = ticks() - initial
        if value < latest or elapsed < 0 or abs((value - started).total_seconds() - elapsed) > 2:
            raise ClockDiscontinuity("rehearsal clock discontinuity")
        latest = value
        return value

    return read


def fingerprint(root: Path) -> str:
    paths = (
        Path("ops/research/e07_volatility_rehearsal.py"),
        Path("ops/research/e07_volatility_rehearsal_contract.py"),
        Path("ops/research/e07_volatility_input_audit.py"),
        Path("src/e07_volatility_exits/features.py"),
        Path("src/e07_volatility_exits/__init__.py"),
        Path("ops/windows/install-volatility-rehearsal-task.ps1"),
    )
    return digest(
        rfc8785.dumps(
            {
                "source": implementation_digest(root),
                "owned": {p.as_posix(): digest(safe_path(root / p).read_bytes()) for p in paths},
            }
        )
    )


def legacy_idle() -> bool:
    if os.name != "nt":
        return False
    try:
        result = subprocess.run(
            [
                "powershell.exe",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "$ErrorActionPreference='Stop'; "
                "$t=Get-ScheduledTask -TaskName 'Insider Alerts Research Commissioning V2'; "
                "if($t.State -ne 'Disabled'){exit 1}; "
                "$filter=\"Name='pythonw.exe' OR Name='python.exe'\"; "
                "$p=@(Get-CimInstance Win32_Process -Filter $filter | "
                "Where-Object {$_.CommandLine -like "
                "'*insider_alerts.research.commissioning_worker*'}); "
                "if($p.Count){exit 1}; exit 0",
            ],
            timeout=10,
            capture_output=True,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            check=False,
        )
    except (subprocess.TimeoutExpired, OSError):
        return False
    return result.returncode == 0


def sec_user_agent(contact: str) -> str:
    if contact:
        if (
            len(contact) > 254
            or not contact.isascii()
            or not re.fullmatch(r"[A-Za-z0-9.!_+~-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", contact)
        ):
            raise ValueError("invalid rehearsal SEC contact")
        return f"Insider Alerts research ({contact})"
    configured = Settings().sec_user_agent
    if "@example.com" in configured or "@" not in configured:
        raise ValueError("SEC contact not configured")
    return configured


def get_directory(user_agent: str) -> str:
    settings = Settings().model_copy(
        update={
            "sec_user_agent": user_agent,
            "sec_timeout_seconds": 6.0,
            "sec_retry_attempts": 1,
        }
    )
    response = SecHttpClient(settings).get_resource(DIRECTORY_URL)
    if (
        response.status_code != 200
        or response.final_url != DIRECTORY_URL
        or len(response.content) > 1_200_000
    ):
        raise ValueError("unexpected SEC directory response")
    raw = response.content.decode("utf-8")
    issuer_directory(raw)
    return raw


@dataclass(frozen=True, slots=True)
class Job:
    case: dict[str, Any]
    stage: str
    dates: tuple[str, ...]
    finalized_at: datetime
    deadline: datetime
    conid: int | None = None

    @property
    def key(self) -> str:
        row = self.case["payload"]["row"]
        return digest(
            rfc8785.dumps(
                {
                    "stage": self.stage,
                    "symbol": row["symbol"],
                    "cik": row["cik"],
                    "dates": list(self.dates),
                    "deadline": timestamp(self.deadline),
                    "conid": self.conid if self.stage == "daily" else None,
                }
            )
        )


class Evidence:
    def __init__(self, records: list[dict[str, Any]], version: calendar.Version) -> None:
        self.records, self.version = records, version
        self.frozen = records[0]["payload"]
        self.cases: dict[str, dict[str, Any]] = {}
        self.attempts: dict[str, list[dict[str, Any]]] = {}
        self.responses: dict[str, dict[str, Any]] = {}
        self.cycle: dict[str, dict[str, Any]] = {}
        self.directories: list[dict[str, Any]] = []
        self.directory_attempts: dict[str, list[datetime]] = {}
        self.last_attempt: datetime | None = None
        for record in records:
            self.add(record, keep=False)

    def add(self, record: dict[str, Any], *, keep: bool = True) -> None:
        if keep:
            self.records.append(record)
        entity, kind = record["entity"], record["kind"]
        if kind == "custody":
            self.cases.setdefault(entity, record)
        elif kind == "attempt":
            self.attempts.setdefault(entity, []).append(record)
            self.last_attempt = parse(record["recorded_at"])
        elif kind == "response":
            self.responses[entity] = record
        elif kind == "cycle":
            self.cycle[entity] = record
            if entity == "directory":
                self.directories.append(record)
            elif entity.startswith("directory-attempt:"):
                self.directory_attempts.setdefault(entity, []).append(parse(record["recorded_at"]))

    def prior(self, case: dict[str, Any]) -> Job:
        payload = case["payload"]
        dates = tuple(payload["history"])
        done = last_rth_close(self.version, date.fromisoformat(dates[-1])) + timedelta(minutes=1)
        return Job(case, "history", dates, done, parse(payload["cutoff"]))

    def matching_attempts(self, job: Job) -> list[dict[str, Any]]:
        matches = []
        for attempt in self.attempts.get(job.key, ()):
            request = attempt["payload"]
            producer = self.cases.get(request["source_case"])
            if (
                producer is None
                or producer["sha256"] != request["case_sha256"]
                or request["job_stage"] != job.stage
                or tuple(request["required_dates"]) != job.dates
                or request["symbol"] != job.case["payload"]["row"]["symbol"]
                or producer["payload"]["row"]["cik"] != job.case["payload"]["row"]["cik"]
                or (job.stage == "daily" and request["expected_conid"] != job.conid)
            ):
                continue
            matches.append(attempt)
        return matches

    def proof(self, job: Job) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]] | None:
        for attempt in self.matching_attempts(job):
            response = self.responses.get(attempt["sha256"])
            if response is None:
                continue
            proof = bar_proof(
                attempt,
                response,
                self.version,
                dates=job.dates,
                finalized_at=job.finalized_at,
                deadline=job.deadline,
            )
            if proof is not None and (job.conid is None or proof["conid"] == job.conid):
                return attempt, response, proof
        return None

    def pin(self, job: Job) -> int | None:
        for attempt in self.matching_attempts(job):
            response = self.responses.get(attempt["sha256"])
            if response is not None:
                conid = qualified_identity(attempt, response)
                if conid is not None:
                    return conid
        return None

    def confirmed(self, case: dict[str, Any]) -> dict[str, Any] | None:
        entity = case["entity"]
        decision = self.cycle.get("read:" + entity)
        ack = self.cycle.get("commit:" + entity)
        if decision is None or ack is None:
            return None
        observed = parse(ack["payload"]["commit_observed_at"])
        material = decision["payload"]
        if (
            ack["payload"]["decision_sha256"] == decision["sha256"]
            and material["case_sha256"] == case["sha256"]
            and parse(case["recorded_at"])
            <= parse(material["first_read_at"])
            <= parse(material["completed_read_at"])
            <= parse(decision["recorded_at"])
            <= observed
            < parse(case["payload"]["cutoff"])
            and observed <= parse(ack["recorded_at"])
        ):
            return decision
        return None

    def daily(self, case: dict[str, Any], conid: int | None) -> tuple[Job, ...]:
        days = [date.fromisoformat(day) for day in case["payload"]["horizon"]]
        return tuple(
            Job(
                case,
                "daily",
                (str(day),),
                last_rth_close(self.version, day) + timedelta(minutes=1),
                local(day + timedelta(days=1), time(8)),
                conid,
            )
            for day in days
        )

    def admitted(self) -> list[dict[str, Any]]:
        return [case for case in self.cases.values() if case["payload"]["reason"] is None]

    def report(self, now: datetime) -> dict[str, Any]:
        custody = Counter(case["payload"]["reason"] or "admitted" for case in self.cases.values())
        preopen: Counter[str] = Counter()
        daily: Counter[str] = Counter()
        for case in self.admitted():
            entity = case["entity"]
            boundary = parse(case["payload"]["cutoff"])
            decision = self.confirmed(case)
            if decision is not None:
                preopen[decision["payload"]["reason"] or "features_observed"] += 1
            elif now >= boundary:
                if "read:" + entity in self.cycle:
                    preopen["durability_not_proven_before_cutoff"] += 1
                else:
                    preopen["preopen_read_missed"] += 1
            else:
                preopen["waiting_for_cutoff"] += 1
            for job in self.daily(case, self.pin(self.prior(case))):
                if now < job.finalized_at:
                    daily["not_due"] += 1
                elif job.conid is None:
                    daily["contract_pin_unavailable"] += 1
                elif self.proof(job):
                    if decision is not None and decision["payload"].get("issuer_linked"):
                        daily["issuer_linked_projection_observed"] += 1
                    else:
                        daily["issuer_unverified_projection_observed"] += 1
                elif now >= job.deadline:
                    daily["daily_window_missed"] += 1
                else:
                    daily["pending"] += 1
        return {
            "contract": CONTRACT,
            "research_reuse": "rehearsal_only",
            "trading_enabled": False,
            "calendar_sha256": self.version.digest,
            "journal_head": self.records[-1]["sha256"],
            "journal_sequence": self.records[-1]["sequence"],
            "custody": dict(custody),
            "preopen": dict(preopen),
            "read_windows_seen": [
                day for day in self.frozen["entries"] if f"read-window:{day}" in self.cycle
            ],
            "daily_projections": dict(daily),
            "outcome_price_basis": "as_traded_unverified",
            "provider_result_counts": dict(
                Counter(response["payload"]["result"] for response in self.responses.values())
            ),
            "unanswered_requests": sum(
                attempt["sha256"] not in self.responses
                for requests in self.attempts.values()
                for attempt in requests
            ),
            "sealed": self.records[-1]["kind"] == "seal",
        }


class Rehearsal:
    def __init__(
        self,
        root: Path,
        *,
        clock: Callable[[], datetime] = utc_now,
        ticks: Callable[[], float] = monotonic,
        digest_for: Callable[[Path], str] = fingerprint,
        source: Callable[..., CommissioningSource] = CommissioningSource,
        directory: Callable[[str], str] = get_directory,
        network_idle: Callable[[], bool] = legacy_idle,
    ) -> None:
        self.root, self.now, self.ticks = safe_path(root), checked_clock(clock, ticks), ticks
        self.folder = safe_path(self.root / FOLDER)
        self.ledger = safe_path(self.root / "data/live_canary.db")
        self.calendar = calendar.CalendarJournal(
            safe_path(self.root / "data/research-commissioning-v2/calendar.db")
        )
        self.journal = Journal(self.folder / "evidence.db", clock=self.now)
        self.digest_for, self.source = digest_for, source
        self.directory, self.network_idle = directory, network_idle

    def activate(
        self, first_session: date, review_reference: str, *, sec_contact: str = ""
    ) -> None:
        head = clean_main(self.root, fetch=True)
        versions = self.calendar.versions()
        version = versions[-1]
        now = self.now()
        if version.observed_at > now or not review_reference.strip():
            raise ValueError("reviewed calendar and release reference are required")
        agent = sec_user_agent(sec_contact)
        opening = local(first_session, FEATURE_CUTOFF)
        if opening < now + timedelta(hours=2):
            raise ValueError("first pre-open cutoff must be at least two hours away")
        entry = calendar.bind(version, first_session, count=REHEARSAL_SESSIONS).sessions
        for session in entry:
            _history(version, session.day)
        final = calendar.bind(version, entry[-1].day, count=HOLD_SESSIONS).sessions[-1].day
        observed = snapshot(self.ledger, clock=self.now)
        self.journal = Journal.create(
            self.journal.path,
            {
                "contract": CONTRACT,
                "implementation_sha256": self.digest_for(self.root),
                "base_commit": head,
                "sec_user_agent": agent,
                "review_reference": review_reference,
                "calendar_sha256": version.digest,
                "source_inventory": inventory(observed),
                "activated_at": timestamp(self.now()),
                "entries": [str(s.day) for s in entry],
                "expiry": timestamp(local(final + timedelta(days=1), time(8))),
            },
            clock=self.now,
        )

    def load(self) -> Evidence:
        records = self.journal.records()
        frozen = records[0]["payload"]
        if frozen.get("contract") != CONTRACT:
            raise ValueError("foreign rehearsal journal")
        if parse(records[-1]["recorded_at"]) > self.now():
            raise ValueError("journal clock ahead of local clock")
        if frozen["implementation_sha256"] != self.digest_for(self.root):
            raise ValueError("rehearsal implementation changed")
        version = self.calendar.versions()[-1]
        if version.digest != frozen["calendar_sha256"]:
            raise ValueError("rehearsal calendar changed")
        checkpoint = safe_path(self.folder / "heartbeat.json")
        if checkpoint.exists():
            body = json.loads(checkpoint.read_bytes())
            # A quiet cycle can make the most recent known clock newer
            # than the last journal entry.
            if parse(body["observed_at"]) > self.now():
                raise ValueError("heartbeat clock ahead of local clock")
            sequence = body["journal_sequence"]
            if (
                type(sequence) is not int
                or not 1 <= sequence <= len(records)
                or body["journal_head"] != records[sequence - 1]["sha256"]
            ):
                raise ValueError("journal does not contain heartbeat checkpoint")
        return Evidence(records, version)

    def append(
        self, evidence: Evidence, kind: str, entity: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        result = self.journal.append(kind, entity, payload)
        evidence.add(result)
        return result

    def capture(self, evidence: Evidence) -> None:
        seen = snapshot(self.ledger, clock=self.now)
        base = evidence.frozen["source_inventory"]
        for key in ("source_path", "source_device", "source_inode"):
            if seen[key] != base[key]:
                raise ValueError("candidate source inventory identity changed")
        old = set(base["packet_id_hashes"])
        activated = parse(evidence.frozen["activated_at"])
        observed = parse(seen["observed_at"])
        for source in seen["rows"]:
            row = {key: source_value(source[key]) for key in COLUMNS}
            entity = digest(rfc8785.dumps({"packet_id": row["packet_id"]}))
            if entity in old or entity in evidence.cases:
                continue
            try:
                signal = parse(row["signal_at"])
                created = parse(row["created_at"])
                if observed < max(signal, created) <= observed + timedelta(seconds=2):
                    continue
                if max(signal, created) > observed:
                    raise ValueError("candidate source clock is in the future")
                if min(signal, created) < activated:
                    if row["entry_session"] not in evidence.frozen["entries"]:
                        continue
                    entry = date.fromisoformat(row["entry_session"])
                else:
                    entry = session_for(evidence.version, signal)
            except (ValueError, TypeError, AttributeError):
                self.append(
                    evidence,
                    "custody",
                    entity,
                    {
                        "row": row,
                        "observed_at": seen["observed_at"],
                        "reason": "candidate_time_or_session_unverified",
                    },
                )
                continue
            if str(entry) not in evidence.frozen["entries"]:
                continue
            reason = None
            if min(signal, created) < activated:
                reason = "candidate_predates_activation"
            elif row["eligible"] != 1:
                reason = "ineligible_under_e07"
            elif row["entry_session"] != str(entry):
                reason = "candidate_entry_session_mismatch"
            elif not valid_symbol(row["symbol"]):
                reason = "candidate_symbol_unavailable"
            expected, _ = _history(evidence.version, entry)
            horizon = calendar.bind(evidence.version, entry, count=HOLD_SESSIONS)
            self.append(
                evidence,
                "custody",
                entity,
                {
                    "row": row,
                    "entry": str(entry),
                    "observed_at": seen["observed_at"],
                    "source_read_started_at": seen["read_started_at"],
                    "reason": reason,
                    "cutoff": timestamp(local(entry, FEATURE_CUTOFF)),
                    "history": sorted(expected),
                    "horizon": [str(day.day) for day in horizon.sessions],
                    "horizon_sha256": horizon.digest,
                },
            )

    def refresh_directory(self, evidence: Evidence, now: datetime) -> None:
        today = now.astimezone(calendar.NY).date()
        if not any(
            now < parse(case["payload"]["cutoff"])
            and date.fromisoformat(case["payload"]["entry"]) <= today + timedelta(days=4)
            for case in evidence.admitted()
        ):
            return
        day_key = f"directory-attempt:{today}"
        recent = evidence.directory_attempts.get(day_key, ())
        current = evidence.directories[-1] if evidence.directories else None
        if (
            8 <= now.astimezone(calendar.NY).hour < 18
            or (current and parse(current["recorded_at"]).astimezone(calendar.NY).date() == today)
            or len(recent) >= MAX_ATTEMPTS
            or (recent and now < recent[-1] + DIRECTORY_RETRY)
        ):
            return
        self.append(
            evidence, "cycle", day_key, {"started_at": timestamp(now), "url": DIRECTORY_URL}
        )
        try:
            raw = self.directory(evidence.frozen["sec_user_agent"])
        except (SecHttpError, ValueError, OSError) as exc:
            self.append(
                evidence, "cycle", f"directory-failure:{today}", {"reason": type(exc).__name__}
            )
            return
        observed = self.now()
        self.append(
            evidence,
            "cycle",
            "directory",
            {
                "url": DIRECTORY_URL,
                "observed_at": timestamp(observed),
                "content_sha256": digest(raw.encode("utf-8")),
                "raw": raw,
            },
        )

    def preopen(self, evidence: Evidence) -> None:
        read_at = self.now()
        due = [
            case
            for case in evidence.admitted()
            if "read:" + case["entity"] not in evidence.cycle
            and parse(case["payload"]["cutoff"]) - READ_WINDOW
            <= read_at
            < parse(case["payload"]["cutoff"])
        ]
        if not due:
            return
        # One durable prefix feeds this pass. A separate pass after candidate
        # capture only reads any new cases that still have time before cutoff.
        durable = Evidence(self.journal.records(), evidence.version)
        directory = next(
            (
                record
                for record in reversed(durable.directories)
                if parse(record["recorded_at"]) <= read_at
            ),
            None,
        )
        parsed_directory = None
        if directory is not None:
            with suppress(TypeError, KeyError, ValueError):
                parsed_directory = DirectoryReceipt(
                    directory, issuer_directory(directory["payload"]["raw"])
                )
        for case in due:
            entity = case["entity"]
            cutoff = parse(case["payload"]["cutoff"])
            if self.now() >= cutoff:
                break
            try:
                prior = durable.prior(case)
                proof = durable.proof(replace(prior, conid=durable.pin(prior)))
                material = decision_material(
                    case["payload"] | {"case_sha256": case["sha256"]},
                    proof,
                    parsed_directory,
                    evidence.version,
                    cutoff,
                )
            except (
                ValueError,
                AttributeError,
                TypeError,
                KeyError,
                IndexError,
                ArithmeticError,
            ) as exc:
                material = {
                    "case_sha256": case["sha256"],
                    "reason": "candidate_input_invalid",
                    "error_type": type(exc).__name__,
                    "fallback_width": BASE_WIDTH,
                }
            completed = self.now()
            if completed >= cutoff:
                break
            decision = self.append(
                evidence,
                "cycle",
                "read:" + entity,
                material
                | {
                    "first_read_at": timestamp(read_at),
                    "completed_read_at": timestamp(completed),
                    "input_watermark": durable.records[-1]["sha256"],
                },
            )
            # Journal.append returns after SQLite finishes the durable write.
            # Lack of confirmation remains unverified across restart.
            confirmed = self.now()
            if confirmed < cutoff:
                self.append(
                    evidence,
                    "cycle",
                    "commit:" + entity,
                    {
                        "decision_sha256": decision["sha256"],
                        "commit_observed_at": timestamp(confirmed),
                    },
                )

    async def acquire(self, evidence: Evidence, now: datetime) -> str | None:
        wall = now.astimezone(calendar.NY)
        if 8 <= wall.hour < 18 or (evidence.last_attempt and now < evidence.last_attempt + PACE):
            return None
        pending: dict[str, Job] = {}
        for case in evidence.admitted():
            prior = evidence.prior(case)
            pin = evidence.pin(prior)
            pending.setdefault(prior.key, replace(prior, conid=pin))
            if pin is not None:
                for job in evidence.daily(case, pin):
                    pending.setdefault(job.key, job)
        ready = [
            job
            for job in pending.values()
            if job.finalized_at <= now < job.deadline
            and len(evidence.attempts.get(job.key, ())) < MAX_ATTEMPTS
            and not evidence.proof(job)
        ]
        if not ready:
            return None
        # Share the empty coordination lock for client 179 with the legacy
        # program. Its archived source journal is never opened for write.
        lock = safe_path(self.calendar.path.parent / "network-lock.db")
        if not lock.is_file():
            return "legacy_network_lock_unavailable"
        try:
            with exclusive_lock(lock):
                if not self.network_idle():
                    return "legacy_client_not_idle"
                await self.request(
                    evidence,
                    min(
                        ready,
                        key=lambda item: (
                            item.stage != "history",
                            item.deadline,
                            item.key,
                        ),
                    ),
                )
        except LockBusy:
            return "legacy_network_lock_busy"
        return None

    async def request(self, evidence: Evidence, job: Job) -> None:
        now = self.now()
        wall = now.astimezone(calendar.NY)
        pause_date = wall.date() + timedelta(days=wall.hour >= 18)
        pause = local(pause_date, time(8))
        budget = min(50.0, (min(job.deadline, pause) - now).total_seconds())
        if 8 <= wall.hour < 18 or budget <= 0:
            return
        end = job.finalized_at
        intent = self.append(
            evidence,
            "attempt",
            job.key,
            {
                "symbol": job.case["payload"]["row"]["symbol"],
                "schedule": False,
                "expected_conid": job.conid,
                "start_date": job.dates[0],
                "end_at": timestamp(end),
                "case_sha256": job.case["sha256"],
                "source_case": job.case["entity"],
                "job_stage": job.stage,
                "required_dates": list(job.dates),
            },
        )
        expiry = self.ticks() + budget

        def remaining() -> float:
            return min(
                expiry - self.ticks(), (min(job.deadline, pause) - self.now()).total_seconds()
            )

        try:
            response = await self.source(clock=self.now, budget=remaining).acquire(
                intent["payload"]["symbol"],
                start=date.fromisoformat(job.dates[0]),
                end=end,
                expected_conid=job.conid,
            )
        except ClockDiscontinuity:
            raise
        except Exception as exc:
            response = {"result": "source_exception", "error_type": type(exc).__name__}
        self.append(evidence, "response", intent["sha256"], response)

    async def cycle(self) -> None:
        if not self.journal.path.is_file():
            raise ValueError("rehearsal explicitly requires activation")
        with exclusive_lock(self.folder / "worker-lock.db"):
            evidence = self.load()
            acquisition_issue = None
            if evidence.records[-1]["kind"] != "seal":
                now = self.now()
                if now < parse(evidence.frozen["expiry"]):
                    day = now.astimezone(calendar.NY).date()
                    cutoff = local(day, FEATURE_CUTOFF)
                    marker = f"read-window:{day}"
                    if (
                        str(day) in evidence.frozen["entries"]
                        and cutoff - READ_WINDOW <= now < cutoff
                        and marker not in evidence.cycle
                    ):
                        self.append(evidence, "cycle", marker, {"observed_at": timestamp(now)})
                    self.preopen(evidence)
                    self.capture(evidence)
                    self.preopen(evidence)
                    self.refresh_directory(evidence, self.now())
                    acquisition_issue = await self.acquire(evidence, self.now())
                else:
                    self.append(
                        evidence,
                        "seal",
                        "singleton",
                        {
                            "coverage": evidence.report(now),
                            "reason": "fixed_rehearsal_end",
                        },
                    )
            self.heartbeat(
                evidence.report(self.now())
                | {
                    "acquisition_issue": acquisition_issue,
                }
            )

    def heartbeat(self, status: dict[str, Any]) -> None:
        temporary = safe_path(self.folder / "heartbeat.pending.json")
        with temporary.open("wb") as file:
            file.write(rfc8785.dumps(status | {"observed_at": timestamp(self.now())}))
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, safe_path(self.folder / "heartbeat.json"))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--activate", type=date.fromisoformat)
    mode.add_argument("--status", action="store_true")
    mode.add_argument("--verify-installation", action="store_true")
    mode.add_argument("--cycle", action="store_true")
    parser.add_argument("--review-reference", default="")
    parser.add_argument("--sec-contact", default="")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    if args.cycle:
        try:
            ensure_kill_on_close_process_tree()
            asyncio.run(Rehearsal(root).cycle())
        except LockBusy:
            return
        except Exception as exc:
            # Error summaries remain independent of the evidence journal.
            with suppress(OSError, ValueError):
                folder = safe_path(root / FOLDER)
                if folder.is_dir():
                    path = safe_path(folder / "worker-errors.log")
                    with path.open("a", encoding="utf-8") as file:
                        event = {"error": type(exc).__name__, "at": timestamp(utc_now())}
                        file.write(json.dumps(event) + "\n")
            if sys.stderr is not None:
                print(f"Rehearsal error: {type(exc).__name__}", file=sys.stderr)
            raise SystemExit(1) from exc
        return

    rehearsal = Rehearsal(root)
    if args.activate:
        rehearsal.activate(args.activate, args.review_reference, sec_contact=args.sec_contact)
    elif args.verify_installation:
        rehearsal.load()
        if not rehearsal.network_idle():
            raise SystemExit("Legacy broker client is not idle")
    elif args.status:
        report = rehearsal.load().report(rehearsal.now())
        heartbeat = safe_path(rehearsal.folder / "heartbeat.json")
        health = json.loads(heartbeat.read_bytes()) if heartbeat.exists() else {}
        report["latest_heartbeat"] = health.get("observed_at")
        report["acquisition_issue"] = health.get("acquisition_issue")
        print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
