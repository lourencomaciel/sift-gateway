"""Session repository SQL helpers."""

from __future__ import annotations

from sidepouch_mcp.constants import WORKSPACE_ID

UPSERT_SESSION_SQL = """
INSERT INTO sessions (workspace_id, session_id, created_at, last_seen_at)
VALUES (%s, %s, NOW(), NOW())
ON CONFLICT (workspace_id, session_id)
DO UPDATE SET last_seen_at = EXCLUDED.last_seen_at
"""


def upsert_session_params(session_id: str) -> tuple[str, str]:
    """Build parameter tuple for the session upsert.

    Args:
        session_id: Client session identifier.

    Returns:
        Tuple of (workspace_id, session_id).
    """
    return (WORKSPACE_ID, session_id)
