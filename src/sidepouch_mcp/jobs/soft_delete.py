"""Mark expired or unreferenced artifacts as soft-deleted.

Provides batched SQL operations that set ``deleted_at`` on
artifacts whose TTL has expired or whose last reference is
older than a threshold.  Supports both Postgres (``FOR UPDATE
SKIP LOCKED``) and SQLite (subquery) concurrency strategies.
Exports ``SoftDeleteResult`` and the two runner functions.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sidepouch_mcp.constants import WORKSPACE_ID
from sidepouch_mcp.db.backend import Dialect
from sidepouch_mcp.db.protocols import (
    ConnectionLike,
    increment_metric,
    safe_rollback,
)
from sidepouch_mcp.obs.logging import LogEvents, get_logger


@dataclass(frozen=True)
class SoftDeleteResult:
    """Result of a soft delete batch.

    Attributes:
        deleted_count: Number of artifacts soft-deleted.
        artifact_ids: IDs of the artifacts that were deleted.
    """

    deleted_count: int
    artifact_ids: list[str]


# Use SKIP LOCKED for concurrent safety, recheck predicates
SOFT_DELETE_BATCH_SQL = """
WITH candidates AS (
    SELECT artifact_id, generation
    FROM artifacts
    WHERE workspace_id = %s
      AND deleted_at IS NULL
      AND expires_at IS NOT NULL
      AND expires_at <= NOW()
    ORDER BY expires_at ASC
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
  AND a.expires_at IS NOT NULL
  AND a.expires_at <= NOW()
RETURNING a.artifact_id
"""

# SQLite version: no table alias on UPDATE, no FROM clause, subquery instead
SOFT_DELETE_BATCH_SQLITE_SQL = """
UPDATE artifacts
SET deleted_at = datetime('now'),
    generation = generation + 1
WHERE workspace_id = ?
  AND artifact_id IN (
      SELECT artifact_id FROM artifacts
      WHERE workspace_id = ?
        AND deleted_at IS NULL
        AND expires_at IS NOT NULL
        AND expires_at <= datetime('now')
      ORDER BY expires_at ASC
      LIMIT ?
  )
  AND deleted_at IS NULL
  AND expires_at IS NOT NULL
  AND expires_at <= datetime('now')
RETURNING artifact_id
"""

# Soft delete by last_referenced_at threshold
SOFT_DELETE_UNREFERENCED_SQL = """
WITH candidates AS (
    SELECT artifact_id, generation
    FROM artifacts
    WHERE workspace_id = %s
      AND deleted_at IS NULL
      AND last_referenced_at < %s
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
RETURNING a.artifact_id
"""

SOFT_DELETE_UNREFERENCED_SQLITE_SQL = """
UPDATE artifacts
SET deleted_at = datetime('now'),
    generation = generation + 1
WHERE workspace_id = ?
  AND artifact_id IN (
      SELECT artifact_id FROM artifacts
      WHERE workspace_id = ?
        AND deleted_at IS NULL
        AND last_referenced_at < ?
      ORDER BY last_referenced_at ASC
      LIMIT ?
  )
  AND deleted_at IS NULL
RETURNING artifact_id
"""


def soft_delete_expired_params(batch_size: int = 100) -> tuple[object, ...]:
    """Build parameter tuple for SOFT_DELETE_BATCH_SQL.

    Args:
        batch_size: Maximum number of artifacts to soft-delete.

    Returns:
        Parameter tuple for the expired soft-delete query.
    """
    return (WORKSPACE_ID, batch_size, WORKSPACE_ID)


def soft_delete_unreferenced_params(
    threshold_timestamp: str,
    batch_size: int = 100,
) -> tuple[object, ...]:
    """Build parameter tuple for SOFT_DELETE_UNREFERENCED_SQL.

    Args:
        threshold_timestamp: ISO timestamp cutoff for
            last_referenced_at comparison.
        batch_size: Maximum number of artifacts to soft-delete.

    Returns:
        Parameter tuple for the unreferenced soft-delete query.
    """
    return (WORKSPACE_ID, threshold_timestamp, batch_size, WORKSPACE_ID)


def _extract_artifact_ids(rows: list[tuple[object, ...]]) -> list[str]:
    """Extract valid artifact ID strings from result rows.

    Args:
        rows: List of row tuples from a RETURNING clause.

    Returns:
        List of non-empty artifact ID strings.
    """
    artifact_ids: list[str] = []
    for row in rows:
        if not row:
            continue
        raw = row[0]
        if isinstance(raw, str) and raw:
            artifact_ids.append(raw)
    return artifact_ids


def run_soft_delete_expired(
    connection: ConnectionLike,
    *,
    batch_size: int = 100,
    dialect: Dialect = Dialect.POSTGRES,
    metrics: Any | None = None,
    logger: Any | None = None,
) -> SoftDeleteResult:
    """Execute one soft-delete batch for expired artifacts.

    Args:
        connection: Database connection for the transaction.
        batch_size: Maximum artifacts to process per batch.
        dialect: SQL dialect (Postgres or SQLite).
        metrics: Optional GatewayMetrics for counter updates.
        logger: Optional structured logger override.

    Returns:
        A SoftDeleteResult with count and affected IDs.
    """
    log = logger or get_logger(component="jobs.soft_delete")
    try:
        if dialect is Dialect.SQLITE:
            sql = SOFT_DELETE_BATCH_SQLITE_SQL
            params: tuple[object, ...] = (
                WORKSPACE_ID,
                WORKSPACE_ID,
                batch_size,
            )
        else:
            sql = SOFT_DELETE_BATCH_SQL
            params = soft_delete_expired_params(batch_size=batch_size)
        rows = connection.execute(sql, params).fetchall()
        artifact_ids = _extract_artifact_ids(rows)
        connection.commit()
        increment_metric(metrics, "prune_soft_deletes", len(artifact_ids))
        if artifact_ids:
            log.info(
                LogEvents.PRUNE_SOFT_DELETE,
                deleted_count=len(artifact_ids),
                batch_size=batch_size,
                reason="expired",
            )
        return SoftDeleteResult(
            deleted_count=len(artifact_ids),
            artifact_ids=artifact_ids,
        )
    except Exception:
        safe_rollback(connection)
        raise


def run_soft_delete_unreferenced(
    connection: ConnectionLike,
    *,
    threshold_timestamp: str,
    batch_size: int = 100,
    dialect: Dialect = Dialect.POSTGRES,
    metrics: Any | None = None,
    logger: Any | None = None,
) -> SoftDeleteResult:
    """Execute one soft-delete batch for old unreferenced artifacts.

    Args:
        connection: Database connection for the transaction.
        threshold_timestamp: ISO timestamp cutoff for
            last_referenced_at.
        batch_size: Maximum artifacts to process per batch.
        dialect: SQL dialect (Postgres or SQLite).
        metrics: Optional GatewayMetrics for counter updates.
        logger: Optional structured logger override.

    Returns:
        A SoftDeleteResult with count and affected IDs.
    """
    log = logger or get_logger(component="jobs.soft_delete")
    try:
        if dialect is Dialect.SQLITE:
            sql = SOFT_DELETE_UNREFERENCED_SQLITE_SQL
            params: tuple[object, ...] = (
                WORKSPACE_ID,
                WORKSPACE_ID,
                threshold_timestamp,
                batch_size,
            )
        else:
            sql = SOFT_DELETE_UNREFERENCED_SQL
            params = soft_delete_unreferenced_params(
                threshold_timestamp=threshold_timestamp,
                batch_size=batch_size,
            )
        rows = connection.execute(sql, params).fetchall()
        artifact_ids = _extract_artifact_ids(rows)
        connection.commit()
        increment_metric(metrics, "prune_soft_deletes", len(artifact_ids))
        if artifact_ids:
            log.info(
                LogEvents.PRUNE_SOFT_DELETE,
                deleted_count=len(artifact_ids),
                batch_size=batch_size,
                threshold_timestamp=threshold_timestamp,
                reason="unreferenced",
            )
        return SoftDeleteResult(
            deleted_count=len(artifact_ids),
            artifact_ids=artifact_ids,
        )
    except Exception:
        safe_rollback(connection)
        raise
