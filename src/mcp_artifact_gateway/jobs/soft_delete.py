"""Soft delete job: marks expired/eligible artifacts as deleted."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from mcp_artifact_gateway.constants import WORKSPACE_ID
from mcp_artifact_gateway.obs.logging import LogEvents, get_logger


@dataclass(frozen=True)
class SoftDeleteResult:
    """Result of a soft delete batch."""
    deleted_count: int
    artifact_ids: list[str]


class CursorLike(Protocol):
    def fetchall(self) -> list[tuple[object, ...]]: ...


class SoftDeleteConnectionLike(Protocol):
    def execute(self, query: str, params: tuple[object, ...] | None = None) -> CursorLike: ...

    def commit(self) -> None: ...


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


def soft_delete_expired_params(batch_size: int = 100) -> tuple[object, ...]:
    """Params for SOFT_DELETE_BATCH_SQL."""
    return (WORKSPACE_ID, batch_size, WORKSPACE_ID)


def soft_delete_unreferenced_params(
    threshold_timestamp: str,
    batch_size: int = 100,
) -> tuple[object, ...]:
    """Params for SOFT_DELETE_UNREFERENCED_SQL."""
    return (WORKSPACE_ID, threshold_timestamp, batch_size, WORKSPACE_ID)


def _extract_artifact_ids(rows: list[tuple[object, ...]]) -> list[str]:
    artifact_ids: list[str] = []
    for row in rows:
        if not row:
            continue
        raw = row[0]
        if isinstance(raw, str) and raw:
            artifact_ids.append(raw)
    return artifact_ids


def _safe_rollback(connection: object) -> None:
    rollback = getattr(connection, "rollback", None)
    if callable(rollback):
        rollback()


def _increment_metric(metrics: Any | None, attr: str, amount: int = 1) -> None:
    if metrics is None:
        return
    counter = getattr(metrics, attr, None)
    increment = getattr(counter, "increment", None)
    if callable(increment):
        increment(amount)


def run_soft_delete_expired(
    connection: SoftDeleteConnectionLike,
    *,
    batch_size: int = 100,
    metrics: Any | None = None,
    logger: Any | None = None,
) -> SoftDeleteResult:
    """Execute one soft-delete batch for expired artifacts."""
    log = logger or get_logger(component="jobs.soft_delete")
    try:
        rows = connection.execute(
            SOFT_DELETE_BATCH_SQL,
            soft_delete_expired_params(batch_size=batch_size),
        ).fetchall()
        artifact_ids = _extract_artifact_ids(rows)
        connection.commit()
        _increment_metric(metrics, "prune_soft_deletes", len(artifact_ids))
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
        _safe_rollback(connection)
        raise


def run_soft_delete_unreferenced(
    connection: SoftDeleteConnectionLike,
    *,
    threshold_timestamp: str,
    batch_size: int = 100,
    metrics: Any | None = None,
    logger: Any | None = None,
) -> SoftDeleteResult:
    """Execute one soft-delete batch for old unreferenced artifacts."""
    log = logger or get_logger(component="jobs.soft_delete")
    try:
        rows = connection.execute(
            SOFT_DELETE_UNREFERENCED_SQL,
            soft_delete_unreferenced_params(
                threshold_timestamp=threshold_timestamp,
                batch_size=batch_size,
            ),
        ).fetchall()
        artifact_ids = _extract_artifact_ids(rows)
        connection.commit()
        _increment_metric(metrics, "prune_soft_deletes", len(artifact_ids))
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
        _safe_rollback(connection)
        raise
