"""Repository functions for garbage-collection / pruning operations.

Provides helpers to discover soft-delete candidates, hard-delete artifacts,
and clean up unreferenced payloads and binary blobs.
"""

from __future__ import annotations

import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row

from mcp_artifact_gateway.constants import WORKSPACE_ID

# ---------------------------------------------------------------------------
# Soft-delete candidate discovery
# ---------------------------------------------------------------------------

_FIND_SOFT_DELETE_CANDIDATES = """\
SELECT *
  FROM artifacts
 WHERE workspace_id = %s
   AND deleted_at IS NULL
   AND (
       (expires_at IS NOT NULL AND expires_at < %s)
       OR
       (last_referenced_at < %s)
   )
 ORDER BY created_seq
 LIMIT %s
 FOR UPDATE SKIP LOCKED;
"""


async def find_soft_delete_candidates(
    conn: psycopg.AsyncConnection[dict[str, Any]],
    expired_before: datetime.datetime,
    idle_before: datetime.datetime,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Find artifacts eligible for soft-deletion.

    Returns artifacts that are either expired (``expires_at < expired_before``)
    or idle (``last_referenced_at < idle_before``).

    Uses ``FOR UPDATE SKIP LOCKED`` so concurrent pruners do not contend.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            _FIND_SOFT_DELETE_CANDIDATES,
            (WORKSPACE_ID, expired_before, idle_before, limit),
        )
        return await cur.fetchall()


# ---------------------------------------------------------------------------
# Hard-delete artifact
# ---------------------------------------------------------------------------

_HARD_DELETE_ARTIFACT = """\
DELETE FROM artifacts
 WHERE workspace_id = %s
   AND artifact_id  = %s
   AND deleted_at IS NOT NULL;
"""


async def hard_delete_artifact(
    conn: psycopg.AsyncConnection[dict[str, Any]],
    artifact_id: str,
) -> int:
    """Hard-delete a soft-deleted artifact.

    The ``ON DELETE CASCADE`` constraints on ``artifact_roots``,
    ``artifact_refs``, and ``artifact_samples`` will clean up child rows
    automatically.

    Returns the number of rows deleted (0 or 1).
    """
    async with conn.cursor() as cur:
        await cur.execute(
            _HARD_DELETE_ARTIFACT,
            (WORKSPACE_ID, artifact_id),
        )
        return cur.rowcount if cur.rowcount and cur.rowcount >= 0 else 0


# ---------------------------------------------------------------------------
# Unreferenced payloads
# ---------------------------------------------------------------------------

_FIND_UNREFERENCED_PAYLOADS = """\
SELECT pb.*
  FROM payload_blobs pb
 WHERE pb.workspace_id = %s
   AND NOT EXISTS (
       SELECT 1
         FROM artifacts a
        WHERE a.workspace_id     = pb.workspace_id
          AND a.payload_hash_full = pb.payload_hash_full
   );
"""

_DELETE_PAYLOAD_BLOB = """\
DELETE FROM payload_blobs
 WHERE workspace_id     = %s
   AND payload_hash_full = %s;
"""


async def find_unreferenced_payloads(
    conn: psycopg.AsyncConnection[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Find payload blobs that are not referenced by any artifact."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(_FIND_UNREFERENCED_PAYLOADS, (WORKSPACE_ID,))
        return await cur.fetchall()


async def delete_payload_blob(
    conn: psycopg.AsyncConnection[dict[str, Any]],
    payload_hash_full: str,
) -> int:
    """Delete a payload blob.

    ``ON DELETE CASCADE`` on ``payload_binary_refs`` and
    ``payload_hash_aliases`` will remove dependent rows.

    Returns the number of rows deleted (0 or 1).
    """
    async with conn.cursor() as cur:
        await cur.execute(
            _DELETE_PAYLOAD_BLOB,
            (WORKSPACE_ID, payload_hash_full),
        )
        return cur.rowcount if cur.rowcount and cur.rowcount >= 0 else 0


# ---------------------------------------------------------------------------
# Unreferenced binaries
# ---------------------------------------------------------------------------

_FIND_UNREFERENCED_BINARIES = """\
SELECT bb.*
  FROM binary_blobs bb
 WHERE bb.workspace_id = %s
   AND NOT EXISTS (
       SELECT 1
         FROM payload_binary_refs pbr
        WHERE pbr.workspace_id = bb.workspace_id
          AND pbr.binary_hash  = bb.binary_hash
   );
"""

_DELETE_BINARY_BLOB = """\
DELETE FROM binary_blobs
 WHERE workspace_id = %s
   AND binary_hash  = %s
RETURNING fs_path;
"""


async def find_unreferenced_binaries(
    conn: psycopg.AsyncConnection[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Find binary blobs not referenced by any ``payload_binary_refs`` row."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(_FIND_UNREFERENCED_BINARIES, (WORKSPACE_ID,))
        return await cur.fetchall()


async def delete_binary_blob(
    conn: psycopg.AsyncConnection[dict[str, Any]],
    binary_hash: str,
) -> str | None:
    """Delete a binary blob and return its ``fs_path`` for filesystem cleanup.

    Returns ``None`` if the row did not exist.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            _DELETE_BINARY_BLOB,
            (WORKSPACE_ID, binary_hash),
        )
        row = await cur.fetchone()
    return row["fs_path"] if row else None
