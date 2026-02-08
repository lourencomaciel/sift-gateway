"""Repository functions for the ``sessions`` and ``artifact_refs`` tables."""

from __future__ import annotations

from typing import Any

import psycopg
from psycopg.rows import dict_row

from mcp_artifact_gateway.constants import WORKSPACE_ID

# ---------------------------------------------------------------------------
# sessions
# ---------------------------------------------------------------------------

_UPSERT_SESSION = """\
INSERT INTO sessions (workspace_id, session_id)
VALUES (%s, %s)
ON CONFLICT (workspace_id, session_id)
DO UPDATE SET last_seen_at = now()
RETURNING workspace_id, session_id, created_at, last_seen_at;
"""

_UPDATE_SESSION_LAST_SEEN = """\
UPDATE sessions
   SET last_seen_at = now()
 WHERE workspace_id = %s
   AND session_id   = %s;
"""


async def upsert_session(
    conn: psycopg.AsyncConnection[dict[str, Any]],
    session_id: str,
) -> dict[str, Any]:
    """Insert a new session or bump its ``last_seen_at`` timestamp.

    Returns the full session row.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(_UPSERT_SESSION, (WORKSPACE_ID, session_id))
        row = await cur.fetchone()
    assert row is not None
    return row


async def update_session_last_seen(
    conn: psycopg.AsyncConnection[dict[str, Any]],
    session_id: str,
) -> None:
    """Touch ``last_seen_at`` on an existing session."""
    await conn.execute(
        _UPDATE_SESSION_LAST_SEEN,
        (WORKSPACE_ID, session_id),
    )


# ---------------------------------------------------------------------------
# artifact_refs
# ---------------------------------------------------------------------------

_UPSERT_ARTIFACT_REF = """\
INSERT INTO artifact_refs (workspace_id, session_id, artifact_id)
VALUES (%s, %s, %s)
ON CONFLICT (workspace_id, session_id, artifact_id)
DO UPDATE SET last_seen_at = now()
RETURNING workspace_id, session_id, artifact_id, first_seen_at, last_seen_at;
"""

_UPDATE_ARTIFACT_REF_LAST_SEEN = """\
UPDATE artifact_refs
   SET last_seen_at = now()
 WHERE workspace_id = %s
   AND session_id   = %s
   AND artifact_id  = %s;
"""


async def upsert_artifact_ref(
    conn: psycopg.AsyncConnection[dict[str, Any]],
    session_id: str,
    artifact_id: str,
) -> dict[str, Any]:
    """Insert or update an artifact reference for the given session.

    Returns the full ``artifact_refs`` row.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            _UPSERT_ARTIFACT_REF,
            (WORKSPACE_ID, session_id, artifact_id),
        )
        row = await cur.fetchone()
    assert row is not None
    return row


async def update_artifact_ref_last_seen(
    conn: psycopg.AsyncConnection[dict[str, Any]],
    session_id: str,
    artifact_id: str,
) -> None:
    """Touch ``last_seen_at`` on an existing artifact reference."""
    await conn.execute(
        _UPDATE_ARTIFACT_REF_LAST_SEEN,
        (WORKSPACE_ID, session_id, artifact_id),
    )
