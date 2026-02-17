"""Enforce workspace storage quotas by pruning excess artifacts.

Queries current storage usage (binary blobs, payload totals),
compares against configured caps, and iteratively soft- and
hard-deletes the least-recently-used artifacts until usage is
within bounds.  Exports ``StorageUsage``, ``QuotaBreaches``,
``QuotaEnforcementResult``, and the ``enforce_quota``
orchestrator function.
"""

from __future__ import annotations

from dataclasses import dataclass
import datetime as dt
from decimal import Decimal
from pathlib import Path
from typing import Any

from sift_mcp.constants import WORKSPACE_ID
from sift_mcp.db.protocols import (
    ConnectionLike,
    increment_metric,
    safe_rollback,
)
from sift_mcp.jobs.hard_delete import run_hard_delete_batch
from sift_mcp.obs.logging import LogEvents, get_logger


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class StorageUsage:
    """Current storage usage for a workspace.

    Attributes:
        binary_blob_bytes: Total bytes stored in binary blobs.
        payload_total_bytes: Total bytes across all payload
            blobs.
        total_storage_bytes: Combined payload JSON bytes plus
            binary blob bytes.
    """

    binary_blob_bytes: int
    payload_total_bytes: int
    total_storage_bytes: int  # payload_json_bytes + binary_blob_bytes


@dataclass(frozen=True)
class QuotaBreaches:
    """Which storage caps are exceeded.

    Attributes:
        binary_blob_exceeded: True if binary blob bytes exceed
            the configured cap.
        payload_total_exceeded: True if total payload bytes
            exceed the configured cap.
        total_storage_exceeded: True if combined storage bytes
            exceed the configured cap.
    """

    binary_blob_exceeded: bool
    payload_total_exceeded: bool
    total_storage_exceeded: bool

    @property
    def any_exceeded(self) -> bool:
        """Return whether any storage quota is exceeded.

        Returns:
            True if any cap is breached.
        """
        return (
            self.binary_blob_exceeded
            or self.payload_total_exceeded
            or self.total_storage_exceeded
        )


@dataclass(frozen=True)
class QuotaEnforcementResult:
    """Result of a quota enforcement pass.

    Attributes:
        usage_before: Storage usage snapshot before enforcement.
        usage_after: Storage usage after enforcement, or None if
            no pruning was needed.
        breaches_before: Quota breach flags before enforcement.
        breaches_after: Quota breach flags after enforcement,
            or None if no pruning was needed.
        pruned: True if any prune rounds were executed.
        soft_deleted_count: Total artifacts soft-deleted.
        hard_deleted_count: Total artifacts hard-deleted.
        bytes_reclaimed: Total bytes freed by hard deletion.
        space_cleared: True if all quotas are satisfied after
            enforcement.
    """

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
    COALESCE((SELECT SUM(byte_count)
        FROM binary_blobs WHERE workspace_id = %s), 0),
    COALESCE((SELECT SUM(payload_total_bytes)
        FROM payload_blobs WHERE workspace_id = %s), 0),
    COALESCE((SELECT SUM(payload_json_bytes)
        FROM payload_blobs WHERE workspace_id = %s), 0)
"""

SOFT_DELETE_LRU_FOR_QUOTA_SQL_PG = """
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

SOFT_DELETE_LRU_FOR_QUOTA_SQL_SQLITE = """
UPDATE artifacts
SET deleted_at = datetime('now'),
    generation = generation + 1
WHERE workspace_id = ?
  AND deleted_at IS NULL
  AND artifact_id IN (
      SELECT artifact_id FROM artifacts
      WHERE workspace_id = ?
        AND deleted_at IS NULL
      ORDER BY last_referenced_at ASC
      LIMIT ?
  )
RETURNING artifact_id, payload_total_bytes
"""


# ---------------------------------------------------------------------------
# Param helpers
# ---------------------------------------------------------------------------
def storage_usage_params() -> tuple[object, ...]:
    """Build parameter tuple for STORAGE_USAGE_SQL.

    Returns:
        Tuple of workspace ID parameters for the usage query.
    """
    return (WORKSPACE_ID, WORKSPACE_ID, WORKSPACE_ID)


def soft_delete_lru_params_pg(
    batch_size: int = 100,
) -> tuple[object, ...]:
    """Build parameter tuple for the Postgres LRU soft-delete SQL.

    Args:
        batch_size: Maximum artifacts to soft-delete.

    Returns:
        Parameter tuple for the Postgres variant.
    """
    return (WORKSPACE_ID, batch_size, WORKSPACE_ID)


def soft_delete_lru_params_sqlite(
    batch_size: int = 100,
) -> tuple[object, ...]:
    """Build parameter tuple for the SQLite LRU soft-delete SQL.

    Args:
        batch_size: Maximum artifacts to soft-delete.

    Returns:
        Parameter tuple for the SQLite variant.
    """
    return (WORKSPACE_ID, WORKSPACE_ID, batch_size)


# ---------------------------------------------------------------------------
# Pure functions
# ---------------------------------------------------------------------------
def _parse_storage_usage(row: tuple[object, ...] | None) -> StorageUsage:
    """Parse a row from STORAGE_USAGE_SQL into StorageUsage.

    Args:
        row: Single result row, or None if query returned
            nothing.

    Returns:
        A StorageUsage with parsed byte counts.
    """
    if row is None or len(row) < 3:
        return StorageUsage(
            binary_blob_bytes=0,
            payload_total_bytes=0,
            total_storage_bytes=0,
        )
    # psycopg may return Decimal for SUM(BIGINT).
    binary_blob_bytes = _coerce_numeric_to_int(row[0])
    payload_total_bytes = _coerce_numeric_to_int(row[1])
    payload_json_bytes_sum = _coerce_numeric_to_int(row[2])
    return StorageUsage(
        binary_blob_bytes=binary_blob_bytes,
        payload_total_bytes=payload_total_bytes,
        total_storage_bytes=payload_json_bytes_sum + binary_blob_bytes,
    )


def _coerce_numeric_to_int(value: object) -> int:
    """Convert supported numeric DB return types to int.

    Args:
        value: Database value from aggregate queries.

    Returns:
        Integer representation for int/float/Decimal values;
        ``0`` for unsupported types.
    """
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float, Decimal)):
        return int(value)
    return 0


def check_breaches(
    usage: StorageUsage,
    *,
    max_binary_blob_bytes: int,
    max_payload_total_bytes: int,
    max_total_storage_bytes: int,
) -> QuotaBreaches:
    """Compare storage usage against configured caps.

    Args:
        usage: Current storage usage snapshot.
        max_binary_blob_bytes: Cap for binary blob storage.
        max_payload_total_bytes: Cap for total payload storage.
        max_total_storage_bytes: Cap for combined storage.

    Returns:
        A QuotaBreaches indicating which caps are exceeded.
    """
    return QuotaBreaches(
        binary_blob_exceeded=usage.binary_blob_bytes > max_binary_blob_bytes,
        payload_total_exceeded=usage.payload_total_bytes
        > max_payload_total_bytes,
        total_storage_exceeded=usage.total_storage_bytes
        > max_total_storage_bytes,
    )


def _hard_delete_cutoff_timestamp(hard_delete_grace_seconds: int) -> str:
    """Compute hard-delete cutoff timestamp for the current round.

    Args:
        hard_delete_grace_seconds: Grace period in seconds after
            soft-delete before hard-delete is allowed.

    Returns:
        ISO-formatted UTC timestamp string.
    """
    return (
        dt.datetime.now(dt.UTC)
        - dt.timedelta(seconds=hard_delete_grace_seconds)
    ).isoformat()


# ---------------------------------------------------------------------------
# DB functions
# ---------------------------------------------------------------------------
def query_storage_usage(connection: ConnectionLike) -> StorageUsage:
    """Execute STORAGE_USAGE_SQL and return parsed result.

    Args:
        connection: Database connection to query.

    Returns:
        A StorageUsage with current byte counts.
    """
    row = connection.execute(
        STORAGE_USAGE_SQL, storage_usage_params()
    ).fetchone()
    return _parse_storage_usage(row)


def soft_delete_lru_batch(
    connection: ConnectionLike,
    *,
    batch_size: int = 100,
    metrics: Any | None = None,
    logger: Any | None = None,
) -> tuple[int, int]:
    """Soft-delete oldest artifacts by LRU for quota enforcement.

    Does not commit; caller controls the transaction boundary.

    Args:
        connection: Database connection for the transaction.
        batch_size: Maximum artifacts to soft-delete per batch.
        metrics: Optional GatewayMetrics for counter updates.
        logger: Optional structured logger override.

    Returns:
        Tuple of (count, estimated_bytes_freed).
    """
    log = logger or get_logger(component="jobs.quota")

    from sift_mcp.db.backend import _SqliteConnectionProxy

    if isinstance(connection, _SqliteConnectionProxy):
        sql = SOFT_DELETE_LRU_FOR_QUOTA_SQL_SQLITE
        params = soft_delete_lru_params_sqlite(batch_size=batch_size)
    else:
        sql = SOFT_DELETE_LRU_FOR_QUOTA_SQL_PG
        params = soft_delete_lru_params_pg(batch_size=batch_size)
    rows = connection.execute(sql, params).fetchall()

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
    blobs_root: Path | None = None,
    metrics: Any | None = None,
    logger: Any | None = None,
) -> QuotaEnforcementResult:
    """Check storage caps and prune if breached.

    Iteratively soft- and hard-deletes LRU artifacts until all
    quotas are satisfied or max rounds are exhausted.

    Args:
        connection: Database connection for the transaction.
        max_binary_blob_bytes: Cap for binary blob storage.
        max_payload_total_bytes: Cap for total payload storage.
        max_total_storage_bytes: Cap for combined storage.
        prune_batch_size: Artifacts per prune batch.
        max_prune_rounds: Maximum prune iterations.
        hard_delete_grace_seconds: Grace period in seconds
            after soft-delete before hard-delete.
        remove_fs_blobs: If True, unlink orphaned blob files.
        blobs_root: Optional root directory used to constrain
            filesystem blob deletion paths.
        metrics: Optional GatewayMetrics for counter updates.
        logger: Optional structured logger override.

    Returns:
        A QuotaEnforcementResult with before/after usage and
        prune statistics.
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

    try:
        for _round in range(max_prune_rounds):
            soft_count, _est_bytes = soft_delete_lru_batch(
                connection,
                batch_size=prune_batch_size,
                metrics=metrics,
                logger=log,
            )
            total_soft_deleted += soft_count

            # Recompute cutoff per round so hard-delete can consider artifacts
            # soft-deleted in this round when grace is 0.
            grace_cutoff_timestamp = _hard_delete_cutoff_timestamp(
                hard_delete_grace_seconds
            )
            hard_result = run_hard_delete_batch(
                connection,
                grace_period_timestamp=grace_cutoff_timestamp,
                batch_size=prune_batch_size,
                remove_fs_blobs=remove_fs_blobs,
                blobs_root=blobs_root,
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
