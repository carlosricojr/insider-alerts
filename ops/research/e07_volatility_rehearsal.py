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
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from time import monotonic
from typing import Any

import rfc8785

from e07_volatility_exits.features import FEATURE_CUTOFF
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
            raise ValueError("rehearsal clock discontinuity")
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
        return f"{self.case['entity']}/{self.stage}/{self.dates[-1]}"


class Evidence:
    def __init__(self, records: list[dict[str, Any]], version: calendar.Version) -> None:
        self.records, self.version = records, version
        self.frozen = records[0]["payload"]
        self.cases: dict[str, dict[str, Any]] = {}
        self.attempts: dict[str, list[dict[str, Any]]] = {}
        self.responses: dict[str, dict[str, Any]] = {}
        self.cycle: dict[str, dict[str, Any]] = {}
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

    def prior(self, case: dict[str, Any]) -> Job:
        payload = case["payload"]
        dates = tuple(payload["history"])
        done = last_rth_close(self.version, date.fromisoformat(dates[-1])) + timedelta(minutes=1)
        return Job(case, "history", dates, done, parse(payload["cutoff"]))

    def proof(self, job: Job) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]] | None:
        for attempt in self.attempts.get(job.key, ()):
            request = attempt["payload"]
            if (
                request["case_sha256"] != job.case["sha256"]
                or request["job_stage"] != job.stage
                or tuple(request["required_dates"]) != job.dates
            ):
                continue
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
            if proof is not None:
                return attempt, response, proof
        return None

    def pin(self, job: Job) -> int | None:
        for attempt in self.attempts.get(job.key, ()):
            response = self.responses.get(attempt["sha256"])
            if response is not None:
                conid = qualified_identity(attempt, response)
                if conid is not None:
                    return conid
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
            decision = self.cycle.get("read:" + entity)
            ack = self.cycle.get("commit:" + entity)
            if decision is not None and ack is not None:
                body = ack["payload"]
                if (
                    body["decision_sha256"] == decision["sha256"]
                    and parse(decision["recorded_at"])
                    <= parse(body["commit_observed_at"])
                    < boundary
                    and parse(body["commit_observed_at"]) <= parse(ack["recorded_at"])
                ):
                    preopen[decision["payload"]["reason"] or "features_observed"] += 1
                else:
                    preopen["durability_not_proven_before_cutoff"] += 1
            elif now >= boundary:
                preopen[
                    "durability_not_proven_before_cutoff" if decision else "preopen_read_missed"
                ] += 1
            else:
                preopen["waiting_for_cutoff"] += 1
            for job in self.daily(case, self.pin(self.prior(case))):
                if now < job.finalized_at:
                    daily["not_due"] += 1
                elif job.conid is None:
                    daily["contract_pin_unavailable"] += 1
                elif self.proof(job):
                    daily["provider_projection_observed"] += 1
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
            "daily_projections": dict(daily),
            "outcome_price_basis": "as_traded_unverified",
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
                if max(signal, created) > observed:
                    raise ValueError("candidate source clock is in the future")
                if min(signal, created) < activated:
                    continue
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
            if row["eligible"] != 1:
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
        if day_key in evidence.cycle or now.astimezone(calendar.NY).hour in range(8, 18):
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
        for case in evidence.admitted():
            now = self.now()
            entity = case["entity"]
            cutoff = parse(case["payload"]["cutoff"])
            read_key = "read:" + entity
            if read_key in evidence.cycle or not cutoff - READ_WINDOW <= now < cutoff:
                continue
            # Reopen the actual durable journal. A result built from an old in-memory
            # response or from a post-cutoff read cannot claim a timely decision.
            read_at = self.now()
            durable = Evidence(self.journal.records(), evidence.version)
            proof = durable.proof(durable.prior(case))
            material = decision_material(
                case["payload"] | {"case_sha256": case["sha256"]},
                (proof,) if proof else (),
                durable.cycle.get("directory"),
                evidence.version,
                cutoff,
            )
            completed = self.now()
            if completed >= cutoff:
                continue
            decision = self.append(
                evidence,
                "cycle",
                read_key,
                material
                | {
                    "first_read_at": timestamp(read_at),
                    "completed_read_at": timestamp(completed),
                    "input_watermark": durable.records[-1]["sha256"],
                },
            )
            # A return from Journal.append is after SQLite's FULL-synchronous commit.
            # On restart a missing confirmation remains unverified.
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

    async def acquire(self, evidence: Evidence, now: datetime) -> None:
        wall = now.astimezone(calendar.NY)
        if 8 <= wall.hour < 18 or (evidence.last_attempt and now < evidence.last_attempt + PACE):
            return
        pending: list[Job] = []
        for case in evidence.admitted():
            prior = evidence.prior(case)
            pin = evidence.pin(prior)
            pending.append(
                Job(case, prior.stage, prior.dates, prior.finalized_at, prior.deadline, pin)
            )
            if pin is not None:
                pending.extend(evidence.daily(case, pin))
        ready = [
            job
            for job in pending
            if job.finalized_at <= now < job.deadline
            and len(evidence.attempts.get(job.key, ())) < MAX_ATTEMPTS
            and not evidence.proof(job)
        ]
        if not ready:
            return
        # Share the existing coordination lock for client 179. This opens
        # only its empty lock database, never the archived source journal for write.
        lock = safe_path(self.calendar.path.parent / "network-lock.db")
        if not lock.is_file():
            raise ValueError("legacy network coordination lock unavailable")
        with exclusive_lock(lock):
            await self.request(
                evidence,
                min(ready, key=lambda item: (item.stage != "history", item.deadline, item.key)),
            )

    async def request(self, evidence: Evidence, job: Job) -> None:
        if not self.network_idle():
            raise ValueError("legacy acquisition client is not verified idle")
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
                "job_stage": job.stage,
                "required_dates": list(job.dates),
            },
        )
        expiry = self.ticks() + budget

        def remaining() -> float:
            return min(
                expiry - self.ticks(), (min(job.deadline, pause) - self.now()).total_seconds()
            )

        response = await self.source(clock=self.now, budget=remaining).acquire(
            intent["payload"]["symbol"],
            start=date.fromisoformat(job.dates[0]),
            end=end,
            expected_conid=job.conid,
        )
        self.append(evidence, "response", intent["sha256"], response)

    async def cycle(self) -> None:
        if not self.journal.path.is_file():
            raise ValueError("rehearsal explicitly requires activation")
        with exclusive_lock(self.folder / "worker-lock.db"):
            evidence = self.load()
            if evidence.records[-1]["kind"] != "seal":
                now = self.now()
                if now < parse(evidence.frozen["expiry"]):
                    self.capture(evidence)
                    self.preopen(evidence)
                    self.refresh_directory(evidence, self.now())
                    await self.acquire(evidence, self.now())
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
            self.heartbeat(evidence.report(self.now()))

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
    rehearsal = Rehearsal(Path(__file__).resolve().parents[2])
    if args.activate:
        rehearsal.activate(args.activate, args.review_reference, sec_contact=args.sec_contact)
    elif args.verify_installation:
        rehearsal.load()
        if not rehearsal.network_idle():
            raise SystemExit("Legacy broker client is not idle")
    elif args.status:
        report = rehearsal.load().report(rehearsal.now())
        heartbeat = safe_path(rehearsal.folder / "heartbeat.json")
        report["latest_heartbeat"] = (
            json.loads(heartbeat.read_bytes()).get("observed_at") if heartbeat.exists() else None
        )
        print(json.dumps(report, sort_keys=True))
    else:
        ensure_kill_on_close_process_tree()
        try:
            asyncio.run(rehearsal.cycle())
        except LockBusy:
            return
        except Exception as exc:
            # Error summaries remain independent of the evidence journal.
            if rehearsal.folder.is_dir():
                path = safe_path(rehearsal.folder / "worker-errors.log")
                with path.open("a", encoding="utf-8") as file:
                    file.write(
                        json.dumps({"error": type(exc).__name__, "at": timestamp(utc_now())}) + "\n"
                    )
            if sys.stderr is not None:
                print(f"Rehearsal error: {type(exc).__name__}", file=sys.stderr)
            raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
