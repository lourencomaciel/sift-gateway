"""Quota enforcement: check storage caps and prune if breached."""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any

from mcp_artifact_gateway.constants import WORKSPACE_ID
from mcp_artifact_gateway.db.protocols import ConnectionLike, increment_metric, safe_rollback
from mcp_artifact_gateway.jobs.hard_delete import run_hard_delete_batch
from mcp_artifact_gateway.obs.logging import LogEvents, get_logger


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class StorageUsage:
    """Current storage usage for a workspace."""
    binary_blob_bytes: int
    payload_total_bytes: int
    total_storage_bytes: int  # payload_json_bytes + binary_blob_bytes


@dataclass(frozen=True)
class QuotaBreaches:
    """Which storage caps are exceeded."""
    binary_blob_exceeded: bool
    payload_total_exceeded: bool
    total_storage_exceeded: bool

    @property
    def any_exceeded(self) -> bool:
        return (
            self.binary_blob_exceeded
            or self.payload_total_exceeded
            or self.total_storage_exceeded
        )


@dataclass(frozen=True)
class QuotaEnforcementResult:
    """Result of a quota enforcement pass."""
    usage_before: StorageUsage
    usage_after: StorageUsage | None
    breaches_before: QuotaBreaches
    breaches_after: QuotaBreaches | None
    pruned: bool
    soft_deleted_count: int
    hard_deleted_count: int
    bytes_reclaimed: int
    space_cleared: bool


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------
STORAGE_USAGE_SQL = """
SELECT
    COALESCE((SELECT SUM(byte_count) FROM binary_blobs WHERE workspace_id = %s), 0),
    COALESCE((SELECT SUM(payload_total_bytes) FROM payload_blobs WHERE workspace_id = %s), 0),
    COALESCE((SELECT SUM(payload_json_bytes) FROM payload_blobs WHERE workspace_id = %s), 0)
"""

SOFT_DELETE_LRU_FOR_QUOTA_SQL = """
WITH candidates AS (
    SELECT artifact_id, generation, payload_total_bytes
    FROM artifacts
    WHERE workspace_id = %s
      AND deleted_at IS NULL
    ORDER BY last_referenced_at ASC
    LIMIT %s
    FOR UPDATE SKIP LOCKED
)
UPDATE artifacts a
SET deleted_at = NOW(),
    generation = a.generation + 1
FROM candidates c
WHERE a.workspace_id = %s
  AND a.artifact_id = c.artifact_id
  AND a.generation = c.generation
  AND a.deleted_at IS NULL
RETURNING a.artifact_id, c.payload_total_bytes
"""


# ---------------------------------------------------------------------------
# Param helpers
# ---------------------------------------------------------------------------
def storage_usage_params() -> tuple[object, ...]:
    """Params for STORAGE_USAGE_SQL."""
    return (WORKSPACE_ID, WORKSPACE_ID, WORKSPACE_ID)


def soft_delete_lru_params(batch_size: int = 100) -> tuple[object, ...]:
    """Params for SOFT_DELETE_LRU_FOR_QUOTA_SQL."""
    return (WORKSPACE_ID, batch_size, WORKSPACE_ID)


# ---------------------------------------------------------------------------
# Pure functions
# ---------------------------------------------------------------------------
def _parse_storage_usage(row: tuple[object, ...] | None) -> StorageUsage:
    """Parse a row from STORAGE_USAGE_SQL into StorageUsage."""
    if row is None or len(row) < 3:
        return StorageUsage(
            binary_blob_bytes=0,
            payload_total_bytes=0,
            total_storage_bytes=0,
        )
    binary_blob_bytes = int(row[0]) if isinstance(row[0], (int, float)) else 0
    payload_total_bytes = int(row[1]) if isinstance(row[1], (int, float)) else 0
    payload_json_bytes_sum = int(row[2]) if isinstance(row[2], (int, float)) else 0
    return StorageUsage(
        binary_blob_bytes=binary_blob_bytes,
        payload_total_bytes=payload_total_bytes,
        total_storage_bytes=payload_json_bytes_sum + binary_blob_bytes,
    )


def check_breaches(
    usage: StorageUsage,
    *,
    max_binary_blob_bytes: int,
    max_payload_total_bytes: int,
    max_total_storage_bytes: int,
) -> QuotaBreaches:
    """Compare usage against caps."""
    return QuotaBreaches(
        binary_blob_exceeded=usage.binary_blob_bytes > max_binary_blob_bytes,
        payload_total_exceeded=usage.payload_total_bytes > max_payload_total_bytes,
        total_storage_exceeded=usage.total_storage_bytes > max_total_storage_bytes,
    )


# ---------------------------------------------------------------------------
# DB functions
# ---------------------------------------------------------------------------
def query_storage_usage(connection: ConnectionLike) -> StorageUsage:
    """Execute STORAGE_USAGE_SQL and return parsed result."""
    row = connection.execute(STORAGE_USAGE_SQL, storage_usage_params()).fetchone()
    return _parse_storage_usage(row)


def soft_delete_lru_batch(
    connection: ConnectionLike,
    *,
    batch_size: int = 100,
    metrics: Any | None = None,
    logger: Any | None = None,
) -> tuple[int, int]:
    """Soft-delete oldest artifacts by LRU for quota enforcement.

    Returns (count, estimated_bytes_freed).
    """
    log = logger or get_logger(component="jobs.quota")
    rows = connection.execute(
        SOFT_DELETE_LRU_FOR_QUOTA_SQL,
        soft_delete_lru_params(batch_size=batch_size),
    ).fetchall()

    count = 0
    estimated_bytes = 0
    for row in rows:
        if not row or len(row) < 2:
            continue
        artifact_id = row[0]
        payload_bytes = row[1]
        if isinstance(artifact_id, str):
            count += 1
        if isinstance(payload_bytes, (int, float)) and payload_bytes > 0:
            estimated_bytes += int(payload_bytes)

    connection.commit()
    increment_metric(metrics, "prune_soft_deletes", count)
    if count > 0:
        log.info(
            LogEvents.PRUNE_SOFT_DELETE,
            deleted_count=count,
            batch_size=batch_size,
            reason="quota_enforcement",
        )
    return count, estimated_bytes


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
def enforce_quota(
    connection: ConnectionLike,
    *,
    max_binary_blob_bytes: int,
    max_payload_total_bytes: int,
    max_total_storage_bytes: int,
    prune_batch_size: int = 100,
    max_prune_rounds: int = 5,
    hard_delete_grace_seconds: int = 0,
    remove_fs_blobs: bool = True,
    metrics: Any | None = None,
    logger: Any | None = None,
) -> QuotaEnforcementResult:
    """Check storage caps and prune if breached.

    Strategy:
    1. Query current usage and check breaches
    2. If no breach, return immediately (space_cleared=True)
    3. Loop up to max_prune_rounds:
       a. Soft-delete oldest LRU artifacts
       b. Hard-delete to reclaim space
       c. Re-check usage
       d. Break if cleared
    4. Return result with space_cleared reflecting final state
    """
    log = logger or get_logger(component="jobs.quota")
    increment_metric(metrics, "quota_checks")

    try:
        usage_before = query_storage_usage(connection)
    except Exception:
        safe_rollback(connection)
        raise

    breaches_before = check_breaches(
        usage_before,
        max_binary_blob_bytes=max_binary_blob_bytes,
        max_payload_total_bytes=max_payload_total_bytes,
        max_total_storage_bytes=max_total_storage_bytes,
    )

    log.info(
        LogEvents.QUOTA_CHECK,
        binary_blob_bytes=usage_before.binary_blob_bytes,
        payload_total_bytes=usage_before.payload_total_bytes,
        total_storage_bytes=usage_before.total_storage_bytes,
        any_exceeded=breaches_before.any_exceeded,
    )

    if not breaches_before.any_exceeded:
        return QuotaEnforcementResult(
            usage_before=usage_before,
            usage_after=None,
            breaches_before=breaches_before,
            breaches_after=None,
            pruned=False,
            soft_deleted_count=0,
            hard_deleted_count=0,
            bytes_reclaimed=0,
            space_cleared=True,
        )

    increment_metric(metrics, "quota_breaches")
    increment_metric(metrics, "quota_prune_triggered")
    log.info(
        LogEvents.QUOTA_BREACH,
        binary_blob_exceeded=breaches_before.binary_blob_exceeded,
        payload_total_exceeded=breaches_before.payload_total_exceeded,
        total_storage_exceeded=breaches_before.total_storage_exceeded,
    )

    total_soft_deleted = 0
    total_hard_deleted = 0
    total_bytes_reclaimed = 0
    usage_after = usage_before
    breaches_after = breaches_before

    # Hard-delete query uses deleted_at < cutoff; grace must move cutoff
    # into the past so recently soft-deleted artifacts are retained.
    grace_cutoff_timestamp = (
        dt.datetime.now(dt.timezone.utc)
        - dt.timedelta(seconds=hard_delete_grace_seconds)
    ).isoformat()

    try:
        for _round in range(max_prune_rounds):
            soft_count, _est_bytes = soft_delete_lru_batch(
                connection,
                batch_size=prune_batch_size,
                metrics=metrics,
                logger=log,
            )
            total_soft_deleted += soft_count

            hard_result = run_hard_delete_batch(
                connection,
                grace_period_timestamp=grace_cutoff_timestamp,
                batch_size=prune_batch_size,
                remove_fs_blobs=remove_fs_blobs,
                metrics=metrics,
                logger=log,
            )
            total_hard_deleted += hard_result.artifacts_deleted
            total_bytes_reclaimed += hard_result.bytes_reclaimed

            usage_after = query_storage_usage(connection)
            breaches_after = check_breaches(
                usage_after,
                max_binary_blob_bytes=max_binary_blob_bytes,
                max_payload_total_bytes=max_payload_total_bytes,
                max_total_storage_bytes=max_total_storage_bytes,
            )

            if not breaches_after.any_exceeded:
                break

            if (
                soft_count == 0
                and hard_result.artifacts_deleted == 0
                and hard_result.payloads_deleted == 0
                and hard_result.binary_blobs_deleted == 0
            ):
                break
    except Exception:
        safe_rollback(connection)
        raise

    space_cleared = not breaches_after.any_exceeded

    log.info(
        LogEvents.QUOTA_PRUNE_COMPLETE,
        soft_deleted_count=total_soft_deleted,
        hard_deleted_count=total_hard_deleted,
        bytes_reclaimed=total_bytes_reclaimed,
        space_cleared=space_cleared,
    )

    if not space_cleared:
        log.warning(
            LogEvents.QUOTA_EXCEEDED,
            total_storage_bytes=usage_after.total_storage_bytes,
            max_total_storage_bytes=max_total_storage_bytes,
        )

    return QuotaEnforcementResult(
        usage_before=usage_before,
        usage_after=usage_after,
        breaches_before=breaches_before,
        breaches_after=breaches_after,
        pruned=True,
        soft_deleted_count=total_soft_deleted,
        hard_deleted_count=total_hard_deleted,
        bytes_reclaimed=total_bytes_reclaimed,
        space_cleared=space_cleared,
    )
