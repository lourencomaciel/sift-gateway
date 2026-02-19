"""Classify upstream/runtime exceptions into stable gateway error codes."""

from __future__ import annotations

import asyncio
import errno
import socket


def classify_upstream_exception(exc: Exception) -> str:
    """Map a runtime exception to a stable upstream error code.

    Args:
        exc: Raised exception from upstream transport, process
            launch, DNS, or runtime execution.

    Returns:
        Stable machine-readable error code.
    """
    if isinstance(exc, socket.gaierror):
        return "UPSTREAM_DNS_FAILURE"
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        return "UPSTREAM_TIMEOUT"
    if isinstance(exc, (FileNotFoundError, PermissionError)):
        return "UPSTREAM_LAUNCH_FAILURE"
    if isinstance(exc, OSError):
        if exc.errno in {errno.ENOENT, errno.ENOTDIR, errno.EACCES}:
            return "UPSTREAM_LAUNCH_FAILURE"
        if exc.errno in {errno.EHOSTUNREACH, errno.ENETUNREACH}:
            return "UPSTREAM_NETWORK_FAILURE"
        return "UPSTREAM_TRANSPORT_FAILURE"
    return "UPSTREAM_RUNTIME_FAILURE"
