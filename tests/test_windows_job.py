from __future__ import annotations

import ctypes
import os
import sys
import time
from pathlib import Path

import pytest

from insider_alerts.execution import windows_job
from insider_alerts.research.capture import run_hidden_process


class FakeJobApi:
    def __init__(self, *, fail_assignment: bool = False) -> None:
        self.fail_assignment = fail_assignment
        self.events: list[tuple[str, int]] = []

    def create_job(self) -> int:
        self.events.append(("create", 42))
        return 42

    def configure_kill_on_close(self, handle: int) -> None:
        self.events.append(("configure", handle))

    def assign_current_process(self, handle: int) -> None:
        self.events.append(("assign", handle))
        if self.fail_assignment:
            raise windows_job.WindowsJobError("assignment failed")

    def close(self, handle: int) -> None:
        self.events.append(("close", handle))


@pytest.fixture(autouse=True)
def reset_job_state():  # type: ignore[no-untyped-def]
    windows_job._close_process_job()
    yield
    windows_job._close_process_job()


def test_non_windows_process_tree_ownership_is_a_noop() -> None:
    api = FakeJobApi()

    windows_job.ensure_kill_on_close_process_tree(platform="posix", api=api)

    assert api.events == []


def test_windows_process_tree_is_assigned_to_kill_on_close_job() -> None:
    api = FakeJobApi()

    windows_job.ensure_kill_on_close_process_tree(platform="nt", api=api)
    windows_job.ensure_kill_on_close_process_tree(platform="nt", api=api)

    assert api.events == [("create", 42), ("configure", 42), ("assign", 42)]
    windows_job._close_process_job()
    assert api.events[-1] == ("close", 42)


def test_failed_assignment_closes_job_and_allows_retry() -> None:
    failed_api = FakeJobApi(fail_assignment=True)

    with pytest.raises(windows_job.WindowsJobError, match="assignment failed"):
        windows_job.ensure_kill_on_close_process_tree(platform="nt", api=failed_api)

    assert failed_api.events[-1] == ("close", 42)
    retry_api = FakeJobApi()
    windows_job.ensure_kill_on_close_process_tree(platform="nt", api=retry_api)
    assert retry_api.events[-1] == ("assign", 42)


@pytest.mark.skipif(os.name != "nt", reason="Windows process-tree contract")
def test_hidden_timeout_kills_pipe_inheriting_grandchild() -> None:
    child_code = (
        "import subprocess,sys,time; "
        "child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)']); "
        "print(child.pid,flush=True); time.sleep(60)"
    )
    started = time.monotonic()

    result = run_hidden_process(
        [sys.executable, "-c", child_code],
        cwd=Path.cwd(),
        timeout_seconds=1,
    )

    assert result.timed_out is True
    assert time.monotonic() - started < 12
    grandchild_pid = int(result.stdout.strip())
    process_query_limited_information = 0x1000
    handle = ctypes.windll.kernel32.OpenProcess(  # type: ignore[attr-defined]
        process_query_limited_information,
        False,
        grandchild_pid,
    )
    if handle:
        ctypes.windll.kernel32.CloseHandle(handle)  # type: ignore[attr-defined]
    assert not handle
