from __future__ import annotations

import ctypes
import os
from typing import Any, Protocol

_ctypes_windows: Any = ctypes


class WindowsJobError(RuntimeError):
    """The worker could not establish kill-on-close ownership of its process tree."""


class WindowsJobApi(Protocol):
    def create_job(self) -> int: ...

    def configure_kill_on_close(self, handle: int) -> None: ...

    def assign_current_process(self, handle: int) -> None: ...

    def close(self, handle: int) -> None: ...


class _CtypesWindowsJobApi:
    _JOB_OBJECT_EXTENDED_LIMIT_INFORMATION = 9
    _JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000

    def __init__(self) -> None:
        from ctypes import wintypes

        class IoCounters(ctypes.Structure):
            _fields_ = [
                ("ReadOperationCount", ctypes.c_ulonglong),
                ("WriteOperationCount", ctypes.c_ulonglong),
                ("OtherOperationCount", ctypes.c_ulonglong),
                ("ReadTransferCount", ctypes.c_ulonglong),
                ("WriteTransferCount", ctypes.c_ulonglong),
                ("OtherTransferCount", ctypes.c_ulonglong),
            ]

        class BasicLimitInformation(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", ctypes.c_longlong),
                ("PerJobUserTimeLimit", ctypes.c_longlong),
                ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD),
            ]

        class ExtendedLimitInformation(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", BasicLimitInformation),
                ("IoInfo", IoCounters),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        kernel32 = _ctypes_windows.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateJobObjectW.argtypes = (ctypes.c_void_p, wintypes.LPCWSTR)
        kernel32.CreateJobObjectW.restype = wintypes.HANDLE
        kernel32.SetInformationJobObject.argtypes = (
            wintypes.HANDLE,
            ctypes.c_int,
            ctypes.c_void_p,
            wintypes.DWORD,
        )
        kernel32.SetInformationJobObject.restype = wintypes.BOOL
        kernel32.AssignProcessToJobObject.argtypes = (wintypes.HANDLE, wintypes.HANDLE)
        kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
        kernel32.GetCurrentProcess.restype = wintypes.HANDLE
        kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
        kernel32.CloseHandle.restype = wintypes.BOOL
        self._kernel32 = kernel32
        self._extended_type = ExtendedLimitInformation

    @staticmethod
    def _raise_last_error(operation: str) -> None:
        error_code = int(_ctypes_windows.get_last_error())
        raise WindowsJobError(f"{operation} failed with Windows error {error_code}")

    def create_job(self) -> int:
        handle = self._kernel32.CreateJobObjectW(None, None)
        if not handle:
            self._raise_last_error("CreateJobObjectW")
        return int(handle)

    def configure_kill_on_close(self, handle: int) -> None:
        information = self._extended_type()
        information.BasicLimitInformation.LimitFlags = self._JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        configured = self._kernel32.SetInformationJobObject(
            handle,
            self._JOB_OBJECT_EXTENDED_LIMIT_INFORMATION,
            ctypes.byref(information),
            ctypes.sizeof(information),
        )
        if not configured:
            self._raise_last_error("SetInformationJobObject")

    def assign_current_process(self, handle: int) -> None:
        assigned = self._kernel32.AssignProcessToJobObject(
            handle,
            self._kernel32.GetCurrentProcess(),
        )
        if not assigned:
            self._raise_last_error("AssignProcessToJobObject")

    def close(self, handle: int) -> None:
        self._kernel32.CloseHandle(handle)


_PROCESS_JOB_HANDLE: int | None = None
_PROCESS_JOB_API: WindowsJobApi | None = None


def _close_process_job() -> None:
    global _PROCESS_JOB_API, _PROCESS_JOB_HANDLE
    if _PROCESS_JOB_HANDLE is not None and _PROCESS_JOB_API is not None:
        _PROCESS_JOB_API.close(_PROCESS_JOB_HANDLE)
    _PROCESS_JOB_HANDLE = None
    _PROCESS_JOB_API = None


def ensure_kill_on_close_process_tree(
    *,
    platform: str = os.name,
    api: WindowsJobApi | None = None,
) -> None:
    """Fence all worker descendants in a Windows kill-on-close Job Object.

    The process deliberately retains the final handle until Windows tears the process down. An
    explicit ``atexit`` close would activate KILL_ON_JOB_CLOSE while Python is still flushing
    buffers and running other shutdown handlers.
    """

    global _PROCESS_JOB_API, _PROCESS_JOB_HANDLE
    if platform != "nt":
        return
    if _PROCESS_JOB_HANDLE is not None:
        return
    selected_api = api or _CtypesWindowsJobApi()
    handle = selected_api.create_job()
    try:
        selected_api.configure_kill_on_close(handle)
        selected_api.assign_current_process(handle)
    except Exception:
        selected_api.close(handle)
        raise
    _PROCESS_JOB_API = selected_api
    _PROCESS_JOB_HANDLE = handle


__all__ = ["WindowsJobApi", "WindowsJobError", "ensure_kill_on_close_process_tree"]
