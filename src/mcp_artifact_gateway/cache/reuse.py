"""Advisory lock stampede control and artifact reuse logic."""

from __future__ import annotations

import asyncio
import threading as _threading
import time
from dataclasses import dataclass
from typing import Any

from mcp_artifact_gateway.db.protocols import increment_metric
from mcp_artifact_gateway.obs.logging import LogEvents, get_logger
from mcp_artifact_gateway.util.hashing import advisory_lock_keys

# Per-key locks for SQLite advisory lock emulation.
# Postgres uses pg_try_advisory_xact_lock (transaction-scoped).
# SQLite has no equivalent, so we emulate with threading.Lock per request_key.
_sqlite_key_locks: dict[str, _threading.Lock] = {}
_sqlite_guard = _threading.Lock()


@dataclass(frozen=True)
class ReuseResult:
    """Result of checking for artifact reuse."""

    reused: bool
    artifact_id: str | None = None
    reason: str | None = None  # "request_key_match" | "dedupe_alias_match"


# SQL for advisory lock
ACQUIRE_ADVISORY_LOCK_SQL = """
SELECT pg_try_advisory_xact_lock(%s, %s)
"""

# SQL for finding reusable artifact by request_key
FIND_REUSABLE_BY_REQUEST_KEY_SQL = """
SELECT artifact_id, payload_hash_full, upstream_tool_schema_hash, map_status, generation
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
    """Check if a candidate artifact can be reused.

    Reuse requires:
    - candidate exists and is not deleted/expired
    - schema hash matches if strict reuse enabled
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
    if row is None or not row:
        return False
    return bool(row[0])


def try_acquire_advisory_lock(connection: Any, *, request_key: str) -> bool:
    """Try to acquire advisory xact lock for request_key.

    For SQLite: acquires a per-key threading.Lock (non-blocking).
    For Postgres: uses pg_try_advisory_xact_lock.
    """
    import sqlite3

    if isinstance(connection, sqlite3.Connection):
        with _sqlite_guard:
            lock = _sqlite_key_locks.setdefault(request_key, _threading.Lock())
        return lock.acquire(blocking=False)

    key_a, key_b = advisory_lock_keys(request_key)
    row = connection.execute(ACQUIRE_ADVISORY_LOCK_SQL, (key_a, key_b)).fetchone()
    return _lock_result(row)


def release_advisory_lock(connection: Any, *, request_key: str) -> None:
    """Release advisory lock for request_key.

    For SQLite: releases and removes the per-key threading.Lock so the
    dict does not grow unbounded over the process lifetime.
    For Postgres: no-op (advisory locks are transaction-scoped).
    """
    import sqlite3

    if not isinstance(connection, sqlite3.Connection):
        return
    with _sqlite_guard:
        lock = _sqlite_key_locks.pop(request_key, None)
    if lock is not None:
        try:
            lock.release()
        except RuntimeError:
            pass  # lock was not held


def acquire_advisory_lock(
    connection: Any,
    *,
    request_key: str,
    timeout_ms: int,
    poll_interval_ms: int = 50,
    metrics: Any | None = None,
    logger: Any | None = None,
) -> bool:
    """Acquire advisory lock with timeout and optional metrics hooks."""
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
    """Acquire advisory lock with timeout, using asyncio.sleep to avoid blocking the event loop."""
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
