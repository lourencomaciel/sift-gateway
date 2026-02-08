"""Session tracking and touch policy enforcement."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from mcp_artifact_gateway.constants import WORKSPACE_ID

# ---------------------------------------------------------------------------
# Result DTO
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TouchResult:
    """Outcome of a touch operation."""

    session_updated: bool
    artifact_ref_updated: bool
    artifact_touched: bool


# ---------------------------------------------------------------------------
# SQL statements
# ---------------------------------------------------------------------------

_UPSERT_SESSION_SQL = """\
INSERT INTO sessions (workspace_id, session_id, created_at, last_seen_at)
VALUES (%s, %s, NOW(), NOW())
ON CONFLICT (workspace_id, session_id)
DO UPDATE SET last_seen_at = EXCLUDED.last_seen_at;
"""

_UPSERT_ARTIFACT_REF_SQL = """\
INSERT INTO artifact_refs (workspace_id, session_id, artifact_id, first_seen_at, last_seen_at)
VALUES (%s, %s, %s, NOW(), NOW())
ON CONFLICT (workspace_id, session_id, artifact_id)
DO UPDATE SET last_seen_at = EXCLUDED.last_seen_at;
"""

_BATCH_UPSERT_ARTIFACT_REFS_SQL = """\
INSERT INTO artifact_refs (workspace_id, session_id, artifact_id, first_seen_at, last_seen_at)
SELECT %s, %s, unnest(%s::text[]), NOW(), NOW()
ON CONFLICT (workspace_id, session_id, artifact_id)
DO UPDATE SET last_seen_at = EXCLUDED.last_seen_at;
"""

_TOUCH_ARTIFACT_SQL = """\
UPDATE artifacts
SET last_referenced_at = NOW()
WHERE workspace_id = %s
  AND artifact_id = %s
  AND deleted_at IS NULL;
"""


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def upsert_session(conn: Any, session_id: str) -> bool:
    """Insert or update session last_seen_at. Returns True on success."""
    with conn.cursor() as cur:
        cur.execute(_UPSERT_SESSION_SQL, (WORKSPACE_ID, session_id))
    return True


def upsert_artifact_ref(conn: Any, session_id: str, artifact_id: str) -> bool:
    """Create or update artifact_refs row. Returns True on success."""
    with conn.cursor() as cur:
        cur.execute(
            _UPSERT_ARTIFACT_REF_SQL,
            (WORKSPACE_ID, session_id, artifact_id),
        )
    return True


def touch_artifact(conn: Any, artifact_id: str) -> bool:
    """Update artifacts.last_referenced_at if not deleted. Returns True if row updated."""
    with conn.cursor() as cur:
        cur.execute(_TOUCH_ARTIFACT_SQL, (WORKSPACE_ID, artifact_id))
        return cur.rowcount > 0


# ---------------------------------------------------------------------------
# High-level touch policies
# ---------------------------------------------------------------------------


def touch_for_creation(conn: Any, session_id: str, artifact_id: str) -> TouchResult:
    """Touch all three tables on artifact creation.

    Creation always touches artifacts.last_referenced_at.
    """
    session_ok = upsert_session(conn, session_id)
    ref_ok = upsert_artifact_ref(conn, session_id, artifact_id)
    artifact_ok = touch_artifact(conn, artifact_id)
    return TouchResult(
        session_updated=session_ok,
        artifact_ref_updated=ref_ok,
        artifact_touched=artifact_ok,
    )


def touch_for_retrieval(conn: Any, session_id: str, artifact_id: str) -> TouchResult:
    """Touch session + refs + artifact (if not deleted) on retrieval/describe.

    Retrieval touches artifacts.last_referenced_at only if not deleted.
    """
    session_ok = upsert_session(conn, session_id)
    ref_ok = upsert_artifact_ref(conn, session_id, artifact_id)
    artifact_ok = touch_artifact(conn, artifact_id)
    return TouchResult(
        session_updated=session_ok,
        artifact_ref_updated=ref_ok,
        artifact_touched=artifact_ok,
    )


def batch_upsert_artifact_refs(conn: Any, session_id: str, artifact_ids: list[str]) -> bool:
    """Upsert multiple artifact_refs rows in a single query. Returns True on success."""
    if not artifact_ids:
        return False
    with conn.cursor() as cur:
        cur.execute(
            _BATCH_UPSERT_ARTIFACT_REFS_SQL,
            (WORKSPACE_ID, session_id, artifact_ids),
        )
    return True


def touch_for_search(conn: Any, session_id: str, artifact_ids: list[str]) -> TouchResult:
    """Touch session + artifact_refs only on search.

    Search does NOT touch artifacts.last_referenced_at.
    """
    session_ok = upsert_session(conn, session_id)
    ref_ok = batch_upsert_artifact_refs(conn, session_id, artifact_ids)
    return TouchResult(
        session_updated=session_ok,
        artifact_ref_updated=ref_ok,
        artifact_touched=False,
    )
