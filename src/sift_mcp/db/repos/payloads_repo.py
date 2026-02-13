"""Payload repository SQL helpers."""

from __future__ import annotations

try:
    from psycopg.types.json import Jsonb
except ImportError:  # SQLite-only install — no psycopg
    Jsonb = lambda v: v  # type: ignore[assignment,misc]  # noqa: E731

from sift_mcp.constants import WORKSPACE_ID

INSERT_PAYLOAD_BLOB_SQL = """
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
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT (workspace_id, payload_hash_full) DO NOTHING
"""


def payload_blob_params(
    *,
    payload_hash_full: str,
    envelope: dict[str, object] | None,
    encoding: str,
    canonical_bytes: bytes,
    canonical_len: int,
    canonicalizer_version: str,
    payload_json_bytes: int,
    payload_binary_bytes_total: int,
    payload_total_bytes: int,
    contains_binary_refs: bool,
) -> tuple[object, ...]:
    """Build parameter tuple for the payload blob INSERT.

    Args:
        payload_hash_full: Full SHA-256 payload hash.
        envelope: Envelope dict or None.
        encoding: Canonical encoding name.
        canonical_bytes: Canonical byte representation.
        canonical_len: Length of canonical bytes.
        canonicalizer_version: Canonicalizer version string.
        payload_json_bytes: Size of JSON content in bytes.
        payload_binary_bytes_total: Total binary ref bytes.
        payload_total_bytes: Total payload bytes.
        contains_binary_refs: Whether binary refs exist.

    Returns:
        Positional parameter tuple for the SQL statement.
    """
    return (
        WORKSPACE_ID,
        payload_hash_full,
        Jsonb(envelope) if envelope is not None else None,
        encoding,
        canonical_bytes,
        canonical_len,
        canonicalizer_version,
        payload_json_bytes,
        payload_binary_bytes_total,
        payload_total_bytes,
        contains_binary_refs,
    )
