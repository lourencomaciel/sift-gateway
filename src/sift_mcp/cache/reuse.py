"""Provide advisory lock stampede control and artifact reuse.

Implement cache-hit detection by request key or dedupe alias,
and advisory-lock acquisition (Postgres native or SQLite
emulated) to prevent concurrent duplicate artifact creation.
Key exports are ``ReuseResult``, ``check_reuse_candidate``,
and ``acquire_advisory_lock``.
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
import threading as _threading
import time
from typing import Any

from sift_mcp.db.protocols import increment_metric
from sift_mcp.obs.logging import LogEvents, get_logger
from sift_mcp.util.hashing import advisory_lock_keys

# Per-key locks for SQLite advisory lock emulation.
# Postgres uses pg_try_advisory_xact_lock (transaction-scoped).
# SQLite has no equivalent, so we emulate with threading.Lock per request_key.
_sqlite_key_locks: dict[str, _threading.Lock] = {}
_sqlite_guard = _threading.Lock()


@dataclass(frozen=True)
class ReuseResult:
    """Outcome of an artifact reuse check.

    Indicates whether a previous artifact can satisfy the
    current request and, if so, which artifact matched.

    Attributes:
        reused: True if a reusable artifact was found.
        artifact_id: Matching artifact ID, or None on miss.
        reason: Match strategy, e.g. "request_key_match"
            or "dedupe_alias_match", or None on miss.
    """

    reused: bool
    artifact_id: str | None = None
    reason: str | None = None  # "request_key_match" | "dedupe_alias_match"


# SQL for advisory lock
ACQUIRE_ADVISORY_LOCK_SQL = """
SELECT pg_try_advisory_xact_lock(%s, %s)
"""

# SQL for finding reusable artifact by request_key
FIND_REUSABLE_BY_REQUEST_KEY_SQL = """
SELECT artifact_id, payload_hash_full,
       upstream_tool_schema_hash, map_status, generation
FROM artifacts
WHERE workspace_id = %s
  AND request_key = %s
  AND deleted_at IS NULL
  AND (expires_at IS NULL OR expires_at > NOW())
ORDER BY created_seq DESC
LIMIT 1
"""

# SQL for finding reusable artifact by dedupe alias
FIND_REUSABLE_BY_DEDUPE_SQL = """
SELECT a.artifact_id, a.payload_hash_full, a.upstream_tool_schema_hash,
       a.map_status, a.generation
FROM payload_hash_aliases pha
JOIN artifacts a ON a.workspace_id = pha.workspace_id
    AND a.payload_hash_full = pha.payload_hash_full
WHERE pha.workspace_id = %s
  AND pha.payload_hash_dedupe = %s
  AND pha.upstream_instance_id = %s
  AND pha.tool = %s
  AND a.deleted_at IS NULL
  AND (a.expires_at IS NULL OR a.expires_at > NOW())
ORDER BY a.created_seq DESC
LIMIT 1
"""


def check_reuse_candidate(
    candidate_row: dict[str, Any] | None,
    *,
    expected_schema_hash: str | None,
    strict_schema_reuse: bool = True,
    metrics: Any | None = None,
    logger: Any | None = None,
    request_key: str | None = None,
) -> ReuseResult:
    """Evaluate whether a candidate artifact can satisfy a new request.

    Reuse requires the candidate to exist and, when strict mode
    is enabled, the upstream tool schema hash to match.

    Args:
        candidate_row: DB row dict with artifact_id and
            upstream_tool_schema_hash, or None on cache miss.
        expected_schema_hash: Schema hash the caller expects,
            or None to skip schema comparison.
        strict_schema_reuse: Reject candidates whose schema
            hash differs from expected_schema_hash.
        metrics: Optional metrics collector for hit/miss
            counters.
        logger: Optional structured logger override.
        request_key: Request fingerprint for log context.

    Returns:
        A ReuseResult indicating hit or miss with reason.
    """
    log = logger or get_logger(component="cache.reuse")

    if candidate_row is None:
        increment_metric(metrics, "cache_misses")
        log.info(LogEvents.REUSE_MISS, request_key=request_key)
        return ReuseResult(reused=False)

    if strict_schema_reuse and expected_schema_hash is not None:
        stored_hash = candidate_row.get("upstream_tool_schema_hash")
        if stored_hash != expected_schema_hash:
            increment_metric(metrics, "cache_misses")
            log.info(
                LogEvents.REUSE_MISS,
                request_key=request_key,
                reason="schema_hash_mismatch",
            )
            return ReuseResult(reused=False, reason="schema_hash_mismatch")

    increment_metric(metrics, "cache_hits")
    log.info(
        LogEvents.REUSE_HIT,
        request_key=request_key,
        artifact_id=candidate_row["artifact_id"],
    )
    return ReuseResult(
        reused=True,
        artifact_id=candidate_row["artifact_id"],
        reason="request_key_match",
    )


# SQL for inserting payload_hash_aliases
INSERT_DEDUPE_ALIAS_SQL = """
INSERT INTO payload_hash_aliases (
    workspace_id, payload_hash_dedupe, payload_hash_full,
    upstream_instance_id, tool
) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (workspace_id, payload_hash_dedupe, payload_hash_full) DO NOTHING
"""


def _lock_result(row: tuple[object, ...] | None) -> bool:
    """Interpret a pg_try_advisory_xact_lock result row as bool.

    Args:
        row: Single-column row from the lock query, or None.

    Returns:
        True if the lock was acquired, False otherwise.
    """
    if row is None or not row:
        return False
    return bool(row[0])


def try_acquire_advisory_lock(connection: Any, *, request_key: str) -> bool:
    """Try to acquire an advisory lock for request_key.

    For SQLite connections, acquire a per-key threading.Lock
    (non-blocking).  For Postgres, use
    ``pg_try_advisory_xact_lock``.

    Args:
        connection: Database connection (sqlite3 or psycopg).
        request_key: Content-addressed request fingerprint.

    Returns:
        True if the lock was acquired, False if contended.
    """
    import sqlite3

    from sift_mcp.db.backend import _SqliteConnectionProxy

    if isinstance(connection, (sqlite3.Connection, _SqliteConnectionProxy)):
        with _sqlite_guard:
            lock = _sqlite_key_locks.setdefault(request_key, _threading.Lock())
        return lock.acquire(blocking=False)

    key_a, key_b = advisory_lock_keys(request_key)
    row = connection.execute(
        ACQUIRE_ADVISORY_LOCK_SQL, (key_a, key_b)
    ).fetchone()
    return _lock_result(row)


def release_advisory_lock(connection: Any, *, request_key: str) -> None:
    """Release the advisory lock for request_key.

    For SQLite, release and remove the per-key threading.Lock
    so the dict does not grow unbounded.  For Postgres this is
    a no-op because advisory locks are transaction-scoped.

    Args:
        connection: Database connection (sqlite3 or psycopg).
        request_key: Content-addressed request fingerprint.
    """
    import sqlite3

    from sift_mcp.db.backend import _SqliteConnectionProxy

    if not isinstance(connection, (sqlite3.Connection, _SqliteConnectionProxy)):
        return
    with _sqlite_guard:
        lock = _sqlite_key_locks.pop(request_key, None)
    if lock is not None:
        with contextlib.suppress(RuntimeError):
            lock.release()


def acquire_advisory_lock(
    connection: Any,
    *,
    request_key: str,
    timeout_ms: int,
    poll_interval_ms: int = 50,
    metrics: Any | None = None,
    logger: Any | None = None,
) -> bool:
    """Acquire an advisory lock with polling timeout.

    Repeatedly attempt ``try_acquire_advisory_lock`` until the
    lock is obtained or the deadline expires.

    Args:
        connection: Database connection (sqlite3 or psycopg).
        request_key: Content-addressed request fingerprint.
        timeout_ms: Maximum milliseconds to wait for the lock.
        poll_interval_ms: Milliseconds between retry attempts.
        metrics: Optional metrics collector for lock counters.
        logger: Optional structured logger override.

    Returns:
        True if the lock was acquired before timeout.
    """
    log = logger or get_logger(component="cache.reuse")
    deadline = time.monotonic() + (max(timeout_ms, 0) / 1000.0)
    sleep_seconds = max(poll_interval_ms, 1) / 1000.0

    while True:
        if try_acquire_advisory_lock(connection, request_key=request_key):
            increment_metric(metrics, "advisory_lock_acquired")
            log.info(
                LogEvents.ADVISORY_LOCK_ACQUIRED,
                request_key=request_key,
            )
            return True
        if time.monotonic() >= deadline:
            increment_metric(metrics, "advisory_lock_timeouts")
            log.warning(
                LogEvents.ADVISORY_LOCK_TIMEOUT,
                request_key=request_key,
                timeout_ms=timeout_ms,
            )
            return False
        time.sleep(sleep_seconds)


async def acquire_advisory_lock_async(
    connection: Any,
    *,
    request_key: str,
    timeout_ms: int,
    poll_interval_ms: int = 50,
    metrics: Any | None = None,
    logger: Any | None = None,
) -> bool:
    """Acquire an advisory lock with async polling timeout.

    Same semantics as ``acquire_advisory_lock`` but uses
    ``asyncio.sleep`` to avoid blocking the event loop.

    Args:
        connection: Database connection (sqlite3 or psycopg).
        request_key: Content-addressed request fingerprint.
        timeout_ms: Maximum milliseconds to wait for the lock.
        poll_interval_ms: Milliseconds between retry attempts.
        metrics: Optional metrics collector for lock counters.
        logger: Optional structured logger override.

    Returns:
        True if the lock was acquired before timeout.
    """
    log = logger or get_logger(component="cache.reuse")
    deadline = time.monotonic() + (max(timeout_ms, 0) / 1000.0)
    sleep_seconds = max(poll_interval_ms, 1) / 1000.0

    while True:
        if try_acquire_advisory_lock(connection, request_key=request_key):
            increment_metric(metrics, "advisory_lock_acquired")
            log.info(
                LogEvents.ADVISORY_LOCK_ACQUIRED,
                request_key=request_key,
            )
            return True
        if time.monotonic() >= deadline:
            increment_metric(metrics, "advisory_lock_timeouts")
            log.warning(
                LogEvents.ADVISORY_LOCK_TIMEOUT,
                request_key=request_key,
                timeout_ms=timeout_ms,
            )
            return False
        await asyncio.sleep(sleep_seconds)
