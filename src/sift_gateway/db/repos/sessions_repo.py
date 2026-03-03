"""Session repository SQL helpers."""

from __future__ import annotations

from typing import Any

from sift_gateway.constants import WORKSPACE_ID

UPSERT_SESSION_SQL = """
INSERT INTO sessions (
    workspace_id, session_id, created_at, last_seen_at,
    last_runtime_pid, last_runtime_instance_uuid
)
VALUES (%s, %s, NOW(), NOW(), %s, %s)
ON CONFLICT (workspace_id, session_id)
DO UPDATE SET
    last_seen_at = EXCLUDED.last_seen_at,
    last_runtime_pid = COALESCE(
        EXCLUDED.last_runtime_pid, sessions.last_runtime_pid
    ),
    last_runtime_instance_uuid = COALESCE(
        EXCLUDED.last_runtime_instance_uuid,
        sessions.last_runtime_instance_uuid
    )
"""


def upsert_session_params(
    session_id: str,
    *,
    runtime_provenance: dict[str, Any] | None = None,
) -> tuple[str, str, int | None, str | None]:
    """Build parameter tuple for the session upsert.

    Args:
        session_id: Client session identifier.
        runtime_provenance: Optional process identity fields to
            persist with the session heartbeat.

    Returns:
        Tuple of (workspace_id, session_id, runtime_pid, runtime_instance_uuid).
    """
    runtime_pid: int | None = None
    runtime_instance_uuid: str | None = None
    if isinstance(runtime_provenance, dict):
        raw_pid = runtime_provenance.get("gateway_pid")
        if isinstance(raw_pid, int):
            runtime_pid = raw_pid
        raw_instance_uuid = runtime_provenance.get("gateway_instance_uuid")
        if isinstance(raw_instance_uuid, str) and raw_instance_uuid:
            runtime_instance_uuid = raw_instance_uuid
    return (
        WORKSPACE_ID,
        session_id,
        runtime_pid,
        runtime_instance_uuid,
    )
