"""Hidden, bounded, read-only acquisition worker. Activation is always explicit."""

from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
import subprocess
import sys
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from datetime import UTC, date, datetime, time, timedelta
from importlib.metadata import version
from pathlib import Path
from time import monotonic
from typing import Any

import rfc8785

from insider_alerts.execution.observer import safe_path, worker_lock
from insider_alerts.execution.windows_job import ensure_kill_on_close_process_tree
from insider_alerts.research import calendar_v2 as calendar
from insider_alerts.research.commissioning_custody import capture, inventory, snapshot
from insider_alerts.research.commissioning_journal import Journal, digest
from insider_alerts.research.commissioning_planner import Evidence
from insider_alerts.research.commissioning_report import frozen_calendar, report
from insider_alerts.research.commissioning_source import (
    CommissioningSource,
    error_message,
    timestamp,
)
from insider_alerts.research.commissioning_window import HOURS_SHA256, manifest, parse


class BoundaryFailure(ValueError):
    pass


class LockBusy(RuntimeError):
    pass


def implementation_digest(root: Path) -> str:
    package = root / "src" / "insider_alerts"
    files = set((package / "research").glob("commissioning*.py"))
    files.update(
        package / name
        for name in (
            "__init__.py",
            "execution/__init__.py",
            "research/__init__.py",
            "strategy/__init__.py",
            "backtest/__init__.py",
        )
    )
    files.update(
        package / name
        for name in (
            "research/calendar_v2.py",
            "execution/observer.py",
            "research/bar_feed.py",
            "strategy/e07.py",
            "backtest/models.py",
            "execution/windows_job.py",
            "execution/canary.py",
        )
    )
    files.update(root / name for name in ("uv.lock", "pyproject.toml"))
    files.update((root / "docs/research/calendar-sources").glob("nyse-*"))
    if (
        digest((root / "docs/research/calendar-sources/nyse-hours-2026-10-26.html").read_bytes())
        != HOURS_SHA256
    ):
        raise BoundaryFailure("hours_archive_changed")
    material = {
        "files": {
            str(path.relative_to(root)).replace("\\", "/"): digest(safe_path(path).read_bytes())
            for path in sorted(files)
        },
        "runtime": {
            "python": sys.version,
            "sqlite": sqlite3.sqlite_version,
            **{name: version(name) for name in ("ib_async", "rfc8785", "tzdata")},
        },
    }
    return digest(rfc8785.dumps(material))


def git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    ).stdout.strip()


def clean_main(root: Path, *, fetch: bool) -> str:
    if fetch:
        git(root, "fetch", "--quiet", "origin", "main")
    head = git(root, "rev-parse", "HEAD")
    if (
        git(root, "branch", "--show-current") != "main"
        or head != git(root, "rev-parse", "origin/main")
        or git(root, "status", "--porcelain")
    ):
        raise BoundaryFailure("deployment_not_clean_synced_main")
    return head


def busy(exc: sqlite3.OperationalError) -> bool:
    return getattr(exc, "sqlite_errorcode", None) in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}


@contextmanager
def exclusive_lock(path: Path) -> Iterator[None]:
    with ExitStack() as stack:
        try:
            stack.enter_context(worker_lock(path))
        except sqlite3.OperationalError as exc:
            if not busy(exc):
                raise
            raise LockBusy(str(path)) from exc
        yield


class Runtime:
    def __init__(
        self,
        root: Path,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        ticks: Callable[[], float] = monotonic,
        fingerprint: Callable[[], str] | None = None,
        source_factory: Callable[..., Any] = CommissioningSource,
    ):
        self.root, self.clock, self.ticks = safe_path(root), clock, ticks
        self.folder = safe_path(self.root / "data/research-commissioning-v2")
        self.source = safe_path(self.root / "data/live_canary.db")
        self.journal = Journal(self.folder / "evidence.db", clock=clock)
        self.calendar = calendar.CalendarJournal(self.folder / "calendar.db")
        self.fingerprint = fingerprint or (lambda: implementation_digest(self.root))
        self.source_factory = source_factory

    def load(self) -> tuple[list[dict[str, Any]], tuple[calendar.Version, ...]]:
        records = self.journal.records()
        if parse(records[-1]["recorded_at"]) > self.clock():
            raise BoundaryFailure("clock_regression")
        self.verify_implementation(records[0]["payload"])
        return records, self.calendar.versions()

    def verify_implementation(self, frozen: dict[str, Any]) -> None:
        if self.fingerprint() != frozen["implementation_sha256"]:
            raise BoundaryFailure("implementation_changed")

    def verify_source(self, frozen: dict[str, Any]) -> None:
        source = safe_path(self.source).resolve(strict=True)
        info = source.stat()
        expected = frozen["preexisting_inventory"]
        if (str(source), str(info.st_dev), str(info.st_ino)) != tuple(
            expected[key] for key in ("source_path", "source_device", "source_inode")
        ):
            raise BoundaryFailure("source_identity_changed")

    def activate(self, boundary: datetime, review_reference: str) -> dict[str, Any]:
        if self.journal.path.exists():
            raise FileExistsError("commissioning already activated; never overwrite or reset")
        head = clean_main(self.root, fetch=True)
        fingerprint = self.fingerprint()
        observed = snapshot(self.source, clock=self.clock)
        created = self.clock()
        if boundary < created + timedelta(hours=2):
            raise BoundaryFailure("activation_boundary_less_than_two_hours_ahead")
        self.folder.mkdir(parents=True, exist_ok=True)
        # Explicit activation may create this separate annual-baseline journal;
        # the default worker and status command never initialize either store.
        if not self.calendar.path.exists():
            calendar.create_journal(
                self.calendar.path,
                baseline_pdf=(
                    self.root / "docs/research/calendar-sources/nyse-holidays-2026-2028.pdf"
                ).read_bytes(),
                review_reference=review_reference,
                observed_at=created,
            )
        bound = self.calendar.as_of(created)
        if bound is None:
            raise BoundaryFailure("calendar_not_observed_at_activation")
        frozen = manifest(
            bound,
            boundary=boundary,
            created_at=created,
            revision=head,
            review_reference=review_reference,
            implementation_sha256=fingerprint,
            inventory=inventory(observed),
        )
        if clean_main(self.root, fetch=False) != head:
            raise BoundaryFailure("deployment_changed_during_activation")
        self.verify_implementation(frozen)
        self.verify_source(frozen)
        if boundary < self.clock() + timedelta(hours=2):
            raise BoundaryFailure("activation_boundary_less_than_two_hours_ahead")
        self.journal = Journal.create(self.journal.path, frozen, clock=self.clock)
        return {
            "result": "activated",
            "boundary": frozen["boundary"],
            "recovery_deadline": frozen["recovery_deadline"],
            "strategy_trial_ready": False,
        }

    def status(self) -> dict[str, Any]:
        records, versions = self.load()
        if records[-1]["kind"] != "seal":
            return report(records, versions, now=self.clock())
        seal = records[-1]
        material = seal["payload"]
        prior = records[:-1]
        if material["watermark"] != {
            "sequence": prior[-1]["sequence"],
            "sha256": prior[-1]["sha256"],
        }:
            raise BoundaryFailure("seal_watermark_mismatch")
        result = report(prior, versions, now=parse(material["report_as_of"]))
        if digest(rfc8785.dumps(result)) != material["report_sha256"]:
            raise BoundaryFailure("seal_report_digest_mismatch")
        return result | {
            "report_state": "sealed",
            "seal": {
                "sha256": seal["sha256"],
                "record_constructed_at": seal["recorded_at"],
                **material,
            },
        }

    def heartbeat(self, result: str, **extra: Any) -> None:
        records = self.journal.records()
        previous = next((r for r in reversed(records) if r["kind"] == "cycle"), None)
        if (
            previous is None
            or result != previous["payload"]["result"]
            or self.clock() - parse(previous["recorded_at"]) >= timedelta(minutes=5)
        ):
            self.journal.append("cycle", "singleton", {"result": result, **extra})

    def seal_locked(self) -> dict[str, Any]:
        # Caller holds network. Normal custody never nests acquisition of network.
        try:
            with exclusive_lock(self.folder / "custody-lock.db"):
                records, versions = self.load()
                if records[-1]["kind"] == "seal":
                    return {"result": "sealed", "seal_sha256": records[-1]["sha256"]}
                now = self.clock()
                if now < parse(records[0]["payload"]["recovery_deadline"]):
                    raise BoundaryFailure("seal_before_recovery_deadline")
                result = report(records, versions, now=now)
                self.verify_implementation(records[0]["payload"])
                seal = self.journal.append(
                    "seal",
                    "singleton",
                    {
                        "report_as_of": timestamp(now),
                        "watermark": result["watermark"],
                        "report_sha256": digest(rfc8785.dumps(result)),
                    },
                )
                return {"result": "sealed", "seal_sha256": seal["sha256"]}
        except LockBusy:
            return {"result": "seal_deferred_custody_busy"}

    async def run_once(self) -> dict[str, Any]:
        records, versions = self.load()
        frozen = records[0]["payload"]
        if records[-1]["kind"] == "seal":
            return {"result": "sealed", "seal_sha256": records[-1]["sha256"]}
        if self.clock() < parse(frozen["recovery_deadline"]):
            self.verify_source(frozen)
            try:
                with exclusive_lock(self.folder / "custody-lock.db"):
                    records, versions = self.load()
                    bound = frozen_calendar(versions, frozen, self.clock())
                    if self.clock() < parse(frozen["recovery_deadline"]):
                        try:
                            observed = snapshot(self.source, clock=self.clock)
                            self.verify_implementation(frozen)
                            added = capture(self.journal, bound, observed)
                        except (OSError, sqlite3.Error) as exc:
                            self.heartbeat("source_unavailable", error_type=type(exc).__name__)
                            return {"result": "source_unavailable"}
                        if self.clock() < parse(frozen["recovery_deadline"]):
                            self.heartbeat("custody_captured", new_revisions=added)
            except LockBusy:
                pass
        try:
            with exclusive_lock(self.folder / "network-lock.db"):
                records, versions = self.load()
                if self.clock() >= parse(frozen["recovery_deadline"]):
                    return self.seal_locked()
                if records[-1]["kind"] == "seal":
                    return {"result": "sealed", "seal_sha256": records[-1]["sha256"]}
                self.verify_source(frozen)
                evidence = Evidence(
                    records, frozen_calendar(versions, frozen, self.clock()), self.clock()
                )
                jobs, _ = evidence.jobs_and_report()
                request = evidence.next_request(jobs)
                if request is None:
                    return {"result": "idle"}
                return await self.acquire_locked(frozen, request)
        except LockBusy:
            return {"result": "network_busy"}

    async def acquire_locked(
        self, frozen: dict[str, Any], request: dict[str, Any]
    ) -> dict[str, Any]:
        local = self.clock().astimezone(calendar.NY)
        if 8 <= local.hour < 18:
            return {"result": "idle"}
        cutoff_day = local.date() + timedelta(days=local.hour >= 18)
        cutoff = min(
            datetime.combine(cutoff_day, time(8), calendar.NY), parse(frozen["recovery_deadline"])
        )
        ticks_end = self.ticks() + min(50, (cutoff - self.clock()).total_seconds())

        def budget() -> float:
            self.verify_implementation(frozen)
            return max(0.0, min(ticks_end - self.ticks(), (cutoff - self.clock()).total_seconds()))

        if budget() <= 0:
            return {"result": "idle"}
        attempt = self.journal.append("attempt", "network", request)
        try:
            remaining = budget()
            if remaining <= 0:
                raise TimeoutError("window ended after durable attempt")
            source = self.source_factory(clock=self.clock, budget=budget)
            response = await asyncio.wait_for(
                source.acquire(
                    request["symbol"],
                    start=date.fromisoformat(request["start_date"]),
                    end=parse(request["end_at"]),
                    expected_conid=request["expected_conid"],
                    schedule=request["schedule"],
                ),
                timeout=remaining,
            )
        except Exception as exc:
            response = {
                "result": "worker_source_exception",
                "error_type": type(exc).__name__,
                "error_message": error_message(str(exc)),
                "client_dispatch_at": None,
                "response_at": None,
                "finished_at": timestamp(self.clock()),
            }
        self.verify_implementation(frozen)
        receipt = self.journal.append("response", attempt["sha256"], response)
        self.heartbeat(
            "network_completed", source_result=response["result"], receipt_sha256=receipt["sha256"]
        )
        return {"result": response["result"], "receipt_sha256": receipt["sha256"]}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--activate-at", type=parse)
    mode.add_argument("--status", action="store_true")
    mode.add_argument("--verify-installation", action="store_true")
    parser.add_argument("--review-reference")
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[3]
    try:
        ensure_kill_on_close_process_tree()
        if safe_path(Path.cwd()) != safe_path(root):
            raise BoundaryFailure("worker_checkout_differs_from_working_directory")
        runtime = Runtime(root)
        if args.activate_at:
            if not args.review_reference:
                raise BoundaryFailure("activation_requires_review_reference")
            result = runtime.activate(args.activate_at, args.review_reference)
        elif args.status:
            result = runtime.status()
        elif args.verify_installation:
            clean_main(root, fetch=False)
            records, _ = runtime.load()
            runtime.verify_source(records[0]["payload"])
            git(
                root,
                "merge-base",
                "--is-ancestor",
                records[0]["payload"]["reviewed_revision"],
                "HEAD",
            )
            if records[-1]["kind"] == "seal" or runtime.clock() >= parse(
                records[0]["payload"]["recovery_deadline"]
            ):
                raise BoundaryFailure("commissioning_window_ended")
            result = {"result": "installation_verified"}
        else:
            result = asyncio.run(runtime.run_once())
        if sys.stdout is not None:
            print(json.dumps(result))
        return (
            0
            if result.get("result")
            in {
                None,
                "activated",
                "installation_verified",
                "received",
                "empty_response",
                "idle",
                "network_busy",
                "sealed",
                "seal_deferred_custody_busy",
            }
            else 2
        )
    except Exception as exc:
        result = {
            "result": str(exc) if isinstance(exc, BoundaryFailure) else "worker_error",
            "error_type": type(exc).__name__,
            "strategy_trial_ready": False,
        }
        if sys.stdout is not None:
            print(json.dumps(result))
        log = safe_path(root / "logs/commissioning-v2.err.log")
        log.parent.mkdir(parents=True, exist_ok=True)
        with log.open("a", encoding="utf-8") as stream:
            stream.write(timestamp(datetime.now(UTC)) + " " + json.dumps(result) + "\n")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
