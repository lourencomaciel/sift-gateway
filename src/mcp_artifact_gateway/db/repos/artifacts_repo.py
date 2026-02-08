"""Repository functions for the ``artifacts`` table."""

from __future__ import annotations

import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row

from mcp_artifact_gateway.canon.decimal_json import dumps_safe
from mcp_artifact_gateway.constants import WORKSPACE_ID

# ---------------------------------------------------------------------------
# INSERT
# ---------------------------------------------------------------------------

_INSERT_ARTIFACT = """\
INSERT INTO artifacts (
    workspace_id,
    artifact_id,
    session_id,
    source_tool,
    upstream_instance_id,
    upstream_tool_schema_hash,
    request_key,
    request_args_hash,
    request_args_prefix,
    payload_hash_full,
    canonicalizer_version,
    payload_json_bytes,
    payload_binary_bytes_total,
    payload_total_bytes,
    expires_at,
    parent_artifact_id,
    chain_seq,
    map_kind,
    map_status,
    mapped_part_index,
    mapper_version,
    map_budget_fingerprint,
    map_backend_id,
    prng_version,
    map_error,
    index_status,
    error_summary
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s
)
RETURNING *;
"""


async def insert_artifact(
    conn: psycopg.AsyncConnection[dict[str, Any]],
    *,
    artifact_id: str,
    session_id: str,
    source_tool: str,
    upstream_instance_id: str,
    upstream_tool_schema_hash: str | None,
    request_key: str,
    request_args_hash: str,
    request_args_prefix: str,
    payload_hash_full: str,
    canonicalizer_version: str,
    payload_json_bytes: int,
    payload_binary_bytes_total: int,
    payload_total_bytes: int,
    expires_at: datetime.datetime | None = None,
    parent_artifact_id: str | None = None,
    chain_seq: int | None = None,
    map_kind: str = "none",
    map_status: str = "pending",
    mapped_part_index: int | None = None,
    mapper_version: str = "",
    map_budget_fingerprint: str | None = None,
    map_backend_id: str | None = None,
    prng_version: str | None = None,
    map_error: Any | None = None,
    index_status: str = "off",
    error_summary: str | None = None,
) -> dict[str, Any]:
    """Insert a new artifact row and return the complete row (including ``created_seq``)."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            _INSERT_ARTIFACT,
            (
                WORKSPACE_ID,
                artifact_id,
                session_id,
                source_tool,
                upstream_instance_id,
                upstream_tool_schema_hash,
                request_key,
                request_args_hash,
                request_args_prefix,
                payload_hash_full,
                canonicalizer_version,
                payload_json_bytes,
                payload_binary_bytes_total,
                payload_total_bytes,
                expires_at,
                parent_artifact_id,
                chain_seq,
                map_kind,
                map_status,
                mapped_part_index,
                mapper_version,
                map_budget_fingerprint,
                map_backend_id,
                prng_version,
                psycopg.types.json.Jsonb(map_error, dumps=dumps_safe) if map_error is not None else None,
                index_status,
                error_summary,
            ),
        )
        row = await cur.fetchone()
    assert row is not None
    return row


# ---------------------------------------------------------------------------
# GET / FIND
# ---------------------------------------------------------------------------

_GET_ARTIFACT = """\
SELECT *
  FROM artifacts
 WHERE workspace_id = %s
   AND artifact_id  = %s;
"""

_FIND_LATEST_BY_REQUEST_KEY = """\
SELECT *
  FROM artifacts
 WHERE workspace_id = %s
   AND request_key  = %s
   AND deleted_at IS NULL
   AND (expires_at IS NULL OR expires_at > now())
 ORDER BY created_seq DESC
 LIMIT 1;
"""


async def get_artifact(
    conn: psycopg.AsyncConnection[dict[str, Any]],
    artifact_id: str,
) -> dict[str, Any] | None:
    """Fetch a single artifact by its ID."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(_GET_ARTIFACT, (WORKSPACE_ID, artifact_id))
        return await cur.fetchone()


async def find_latest_by_request_key(
    conn: psycopg.AsyncConnection[dict[str, Any]],
    request_key: str,
) -> dict[str, Any] | None:
    """Find the latest non-deleted, non-expired artifact for a request key."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(_FIND_LATEST_BY_REQUEST_KEY, (WORKSPACE_ID, request_key))
        return await cur.fetchone()


# ---------------------------------------------------------------------------
# UPDATE
# ---------------------------------------------------------------------------

_TOUCH_LAST_REFERENCED = """\
UPDATE artifacts
   SET last_referenced_at = now()
 WHERE workspace_id = %s
   AND artifact_id  = %s
   AND deleted_at IS NULL;
"""

_SOFT_DELETE = """\
UPDATE artifacts
   SET deleted_at  = now(),
       generation  = generation + 1
 WHERE workspace_id = %s
   AND artifact_id  = %s
   AND deleted_at IS NULL
   AND generation   = %s
RETURNING *;
"""


async def touch_last_referenced(
    conn: psycopg.AsyncConnection[dict[str, Any]],
    artifact_id: str,
) -> None:
    """Bump ``last_referenced_at`` on a non-deleted artifact."""
    await conn.execute(_TOUCH_LAST_REFERENCED, (WORKSPACE_ID, artifact_id))


async def soft_delete(
    conn: psycopg.AsyncConnection[dict[str, Any]],
    artifact_id: str,
    generation: int,
) -> dict[str, Any] | None:
    """Soft-delete an artifact if it matches the expected generation.

    Returns the updated row on success, or ``None`` if the optimistic
    concurrency check failed (wrong generation or already deleted).
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(_SOFT_DELETE, (WORKSPACE_ID, artifact_id, generation))
        return await cur.fetchone()


# ---------------------------------------------------------------------------
# SEARCH (session-scoped, with optional filters)
# ---------------------------------------------------------------------------


async def search_by_session(
    conn: psycopg.AsyncConnection[dict[str, Any]],
    session_id: str,
    *,
    filters: dict[str, Any] | None = None,
    order_by: str = "created_seq DESC",
    limit: int = 50,
    cursor_pos: int | None = None,
) -> list[dict[str, Any]]:
    """Search artifacts visible to a session via ``artifact_refs``.

    Parameters
    ----------
    session_id:
        The session whose artifact refs will be joined.
    filters:
        Optional column-level equality filters applied to the ``artifacts``
        table (e.g. ``{"source_tool": "github_search", "deleted_at": None}``).
    order_by:
        SQL ``ORDER BY`` clause fragment.  Only ``created_seq DESC`` and
        ``created_seq ASC`` are accepted to prevent injection.
    limit:
        Maximum number of rows to return.
    cursor_pos:
        If provided, a ``created_seq`` value used for keyset pagination.
        Rows whose ``created_seq`` is strictly less than (for DESC) or
        greater than (for ASC) this value are returned.
    """
    allowed_orders = {"created_seq DESC", "created_seq ASC"}
    if order_by not in allowed_orders:
        raise ValueError(f"order_by must be one of {allowed_orders}")

    descending = order_by == "created_seq DESC"

    parts: list[str] = [
        "SELECT a.*",
        "  FROM artifacts a",
        "  JOIN artifact_refs ar",
        "    ON ar.workspace_id = a.workspace_id",
        "   AND ar.artifact_id  = a.artifact_id",
        " WHERE a.workspace_id = %s",
        "   AND ar.session_id  = %s",
    ]
    params: list[Any] = [WORKSPACE_ID, session_id]

    # Keyset cursor
    if cursor_pos is not None:
        if descending:
            parts.append("   AND a.created_seq < %s")
        else:
            parts.append("   AND a.created_seq > %s")
        params.append(cursor_pos)

    # Dynamic equality filters (safe: values are parameterized, column names
    # are validated against an allow-list).
    _ALLOWED_FILTER_COLUMNS = frozenset({
        "source_tool",
        "upstream_instance_id",
        "request_key",
        "map_kind",
        "map_status",
        "index_status",
        "deleted_at",
        "parent_artifact_id",
    })

    if filters:
        for col, val in filters.items():
            if col not in _ALLOWED_FILTER_COLUMNS:
                raise ValueError(f"Filter column {col!r} is not allowed")
            if val is None:
                parts.append(f"   AND a.{col} IS NULL")
            else:
                parts.append(f"   AND a.{col} = %s")
                params.append(val)

    parts.append(f" ORDER BY a.{order_by}")
    parts.append(" LIMIT %s")
    params.append(limit)

    query = "\n".join(parts)

    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, tuple(params))
        return await cur.fetchall()
