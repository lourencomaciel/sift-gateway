"""Repository functions for ``payload_blobs``, ``binary_blobs``,
``payload_binary_refs``, and ``payload_hash_aliases``.
"""

from __future__ import annotations

from typing import Any

import psycopg
from psycopg.rows import dict_row

from mcp_artifact_gateway.canon.decimal_json import dumps_safe
from mcp_artifact_gateway.constants import WORKSPACE_ID

# ---------------------------------------------------------------------------
# payload_blobs
# ---------------------------------------------------------------------------

_UPSERT_PAYLOAD_BLOB = """\
INSERT INTO payload_blobs (
    workspace_id,
    payload_hash_full,
    envelope,
    envelope_canonical_encoding,
    envelope_canonical_bytes,
    envelope_canonical_bytes_len,
    canonicalizer_version,
    payload_json_bytes,
    payload_binary_bytes_total,
    payload_total_bytes,
    contains_binary_refs
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (workspace_id, payload_hash_full) DO NOTHING
RETURNING *;
"""

_GET_PAYLOAD_BLOB = """\
SELECT *
  FROM payload_blobs
 WHERE workspace_id     = %s
   AND payload_hash_full = %s;
"""


async def upsert_payload_blob(
    conn: psycopg.AsyncConnection[dict[str, Any]],
    payload_hash_full: str,
    envelope_jsonb: Any,
    encoding: str,
    canonical_bytes: bytes,
    canonical_bytes_len: int,
    canonicalizer_version: str,
    json_bytes: int,
    binary_bytes_total: int,
    total_bytes: int,
    contains_binary_refs: bool,
) -> dict[str, Any]:
    """Insert a payload blob (no-op on duplicate hash).

    Returns the row -- either newly inserted or the pre-existing one.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            _UPSERT_PAYLOAD_BLOB,
            (
                WORKSPACE_ID,
                payload_hash_full,
                psycopg.types.json.Jsonb(envelope_jsonb, dumps=dumps_safe),
                encoding,
                canonical_bytes,
                canonical_bytes_len,
                canonicalizer_version,
                json_bytes,
                binary_bytes_total,
                total_bytes,
                contains_binary_refs,
            ),
        )
        row = await cur.fetchone()

    # If the row already existed, the INSERT returned nothing; fetch it.
    if row is None:
        row = await get_payload_blob(conn, payload_hash_full)
    assert row is not None
    return row


async def get_payload_blob(
    conn: psycopg.AsyncConnection[dict[str, Any]],
    payload_hash_full: str,
) -> dict[str, Any] | None:
    """Fetch a single payload blob by its full hash."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(_GET_PAYLOAD_BLOB, (WORKSPACE_ID, payload_hash_full))
        return await cur.fetchone()


# ---------------------------------------------------------------------------
# binary_blobs
# ---------------------------------------------------------------------------

_UPSERT_BINARY_BLOB = """\
INSERT INTO binary_blobs (
    workspace_id,
    binary_hash,
    blob_id,
    byte_count,
    mime,
    fs_path,
    probe_head_hash,
    probe_tail_hash,
    probe_bytes
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (workspace_id, binary_hash) DO NOTHING
RETURNING *;
"""


async def upsert_binary_blob(
    conn: psycopg.AsyncConnection[dict[str, Any]],
    binary_hash: str,
    blob_id: str,
    byte_count: int,
    mime: str | None,
    fs_path: str,
    probe_head_hash: str | None,
    probe_tail_hash: str | None,
    probe_bytes: int | None,
) -> dict[str, Any]:
    """Insert a binary blob (no-op on duplicate hash).

    Returns the row -- either newly inserted or the pre-existing one.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            _UPSERT_BINARY_BLOB,
            (
                WORKSPACE_ID,
                binary_hash,
                blob_id,
                byte_count,
                mime,
                fs_path,
                probe_head_hash,
                probe_tail_hash,
                probe_bytes,
            ),
        )
        row = await cur.fetchone()

    if row is None:
        # Already existed -- fetch it.
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT * FROM binary_blobs WHERE workspace_id = %s AND binary_hash = %s;",
                (WORKSPACE_ID, binary_hash),
            )
            row = await cur.fetchone()
    assert row is not None
    return row


# ---------------------------------------------------------------------------
# payload_binary_refs
# ---------------------------------------------------------------------------

_UPSERT_PAYLOAD_BINARY_REF = """\
INSERT INTO payload_binary_refs (workspace_id, payload_hash_full, binary_hash)
VALUES (%s, %s, %s)
ON CONFLICT (workspace_id, payload_hash_full, binary_hash) DO NOTHING;
"""


async def upsert_payload_binary_ref(
    conn: psycopg.AsyncConnection[dict[str, Any]],
    payload_hash_full: str,
    binary_hash: str,
) -> None:
    """Link a payload to a binary blob (no-op on duplicate)."""
    await conn.execute(
        _UPSERT_PAYLOAD_BINARY_REF,
        (WORKSPACE_ID, payload_hash_full, binary_hash),
    )


# ---------------------------------------------------------------------------
# payload_hash_aliases
# ---------------------------------------------------------------------------

_UPSERT_PAYLOAD_HASH_ALIAS = """\
INSERT INTO payload_hash_aliases (
    workspace_id,
    payload_hash_dedupe,
    payload_hash_full,
    upstream_instance_id,
    tool
) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (workspace_id, payload_hash_dedupe, payload_hash_full) DO NOTHING;
"""

_FIND_BY_DEDUPE_HASH = """\
SELECT pha.*, pb.*
  FROM payload_hash_aliases pha
  JOIN payload_blobs pb
    ON pb.workspace_id     = pha.workspace_id
   AND pb.payload_hash_full = pha.payload_hash_full
 WHERE pha.workspace_id          = %s
   AND pha.payload_hash_dedupe   = %s
   AND pha.upstream_instance_id  = %s
   AND pha.tool                  = %s
 ORDER BY pha.created_at DESC
 LIMIT 1;
"""


async def upsert_payload_hash_alias(
    conn: psycopg.AsyncConnection[dict[str, Any]],
    payload_hash_dedupe: str,
    payload_hash_full: str,
    upstream_instance_id: str,
    tool: str,
) -> None:
    """Record a dedupe-hash to full-hash alias."""
    await conn.execute(
        _UPSERT_PAYLOAD_HASH_ALIAS,
        (WORKSPACE_ID, payload_hash_dedupe, payload_hash_full, upstream_instance_id, tool),
    )


async def find_by_dedupe_hash(
    conn: psycopg.AsyncConnection[dict[str, Any]],
    payload_hash_dedupe: str,
    upstream_instance_id: str,
    tool: str,
) -> dict[str, Any] | None:
    """Find the latest payload blob matching a dedupe hash, instance, and tool.

    Returns the joined alias + payload blob row, or ``None``.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            _FIND_BY_DEDUPE_HASH,
            (WORKSPACE_ID, payload_hash_dedupe, upstream_instance_id, tool),
        )
        return await cur.fetchone()
