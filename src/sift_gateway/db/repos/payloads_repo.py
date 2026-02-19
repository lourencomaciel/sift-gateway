"""Payload repository SQL helpers."""

from __future__ import annotations

from sift_gateway.constants import WORKSPACE_ID

INSERT_PAYLOAD_BLOB_SQL = """
INSERT INTO payload_blobs (
    workspace_id,
    payload_hash_full,
    envelope,
    envelope_canonical_encoding,
    payload_fs_path,
    canonicalizer_version,
    payload_json_bytes,
    payload_binary_bytes_total,
    payload_total_bytes,
    contains_binary_refs
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT (workspace_id, payload_hash_full) DO NOTHING
"""


def payload_blob_params(
    *,
    payload_hash_full: str,
    envelope: dict[str, object] | None,
    encoding: str,
    payload_fs_path: str,
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
        payload_fs_path: Relative filesystem path to payload bytes.
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
        envelope,
        encoding,
        payload_fs_path,
        canonicalizer_version,
        payload_json_bytes,
        payload_binary_bytes_total,
        payload_total_bytes,
        contains_binary_refs,
    )
