"""Pruning repository SQL helpers."""

from __future__ import annotations

from sift_gateway.constants import WORKSPACE_ID

SOFT_DELETE_EXPIRED_SQL = """
UPDATE artifacts
SET deleted_at = datetime('now'),
    generation = generation + 1
WHERE workspace_id = ?
  AND deleted_at IS NULL
  AND expires_at IS NOT NULL
  AND expires_at <= datetime('now')
"""


def soft_delete_expired_params() -> tuple[str]:
    """Build parameter tuple for expired artifact soft-delete.

    Returns:
        Single-element tuple with the workspace ID.
    """
    return (WORKSPACE_ID,)
