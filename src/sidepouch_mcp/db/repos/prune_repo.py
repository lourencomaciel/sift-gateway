"""Pruning repository SQL helpers."""

from __future__ import annotations

from sidepouch_mcp.constants import WORKSPACE_ID

SOFT_DELETE_EXPIRED_SQL = """
UPDATE artifacts
SET deleted_at = NOW(),
    generation = generation + 1
WHERE workspace_id = %s
  AND deleted_at IS NULL
  AND expires_at IS NOT NULL
  AND expires_at <= NOW()
"""


def soft_delete_expired_params() -> tuple[str]:
    """Build parameter tuple for expired artifact soft-delete.

    Returns:
        Single-element tuple with the workspace ID.
    """
    return (WORKSPACE_ID,)
