"""Classify upstream/runtime exceptions into stable gateway error codes."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
import errno
from http import HTTPStatus
import socket

from mcp import McpError


def _iter_exception_chain(exc: BaseException) -> Iterator[BaseException]:
    """Yield *exc* and its cause/context chain without revisiting nodes."""
    pending: list[BaseException] = [exc]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        marker = id(current)
        if marker in seen:
            continue
        seen.add(marker)
        yield current
        cause = getattr(current, "__cause__", None)
        if isinstance(cause, BaseException):
            pending.append(cause)
            continue
        if getattr(current, "__suppress_context__", False):
            continue
        context = getattr(current, "__context__", None)
        if isinstance(context, BaseException):
            pending.append(context)


def _classify_single_exception(exc: BaseException) -> str | None:
    """Map one exception object to an upstream error code when possible."""
    if isinstance(exc, socket.gaierror):
        return "UPSTREAM_DNS_FAILURE"
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        return "UPSTREAM_TIMEOUT"
    if (
        isinstance(exc, McpError)
        and getattr(exc.error, "code", None) == HTTPStatus.REQUEST_TIMEOUT
    ):
        return "UPSTREAM_TIMEOUT"
    if isinstance(exc, (FileNotFoundError, PermissionError)):
        return "UPSTREAM_LAUNCH_FAILURE"
    if isinstance(exc, OSError):
        if exc.errno in {errno.ENOENT, errno.ENOTDIR, errno.EACCES}:
            return "UPSTREAM_LAUNCH_FAILURE"
        if exc.errno in {errno.EHOSTUNREACH, errno.ENETUNREACH}:
            return "UPSTREAM_NETWORK_FAILURE"
        return "UPSTREAM_TRANSPORT_FAILURE"
    return None


def classify_upstream_exception(exc: Exception) -> str:
    """Map a runtime exception to a stable upstream error code.

    Args:
        exc: Raised exception from upstream transport, process
            launch, DNS, or runtime execution.

    Returns:
        Stable machine-readable error code.
    """
    for candidate in _iter_exception_chain(exc):
        code = _classify_single_exception(candidate)
        if code is not None:
            return code
    return "UPSTREAM_RUNTIME_FAILURE"
