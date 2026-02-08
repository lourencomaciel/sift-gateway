"""Hashing utilities used throughout the MCP Artifact Gateway.

All hashing uses SHA-256. Functions in this module build on the canonical
JSON layer (:mod:`mcp_artifact_gateway.canon.rfc8785`) to produce
deterministic, content-addressable identifiers for artifacts, requests,
upstream instances, binary blobs, map budgets, and sample sets.

Spec references: SS4.3, SS4.4, SS6.1, SS9.1, SS13.5.3.
"""

from __future__ import annotations

import hashlib
import struct
from typing import Any

from mcp_artifact_gateway.canon.rfc8785 import canonical_bytes as _canonical_bytes


# ---------------------------------------------------------------------------
# Primitive hashing
# ---------------------------------------------------------------------------

def sha256_hex(data: bytes) -> str:
    """Return the full SHA-256 hex digest of *data* (64 hex characters)."""
    return hashlib.sha256(data).hexdigest()


def sha256_trunc(data: bytes, n: int) -> str:
    """Return the SHA-256 hex digest of *data*, truncated to *n* characters.

    Args:
        data: Raw bytes to hash.
        n: Number of leading hex characters to keep (1..64).

    Raises:
        ValueError: If *n* is out of range.
    """
    if not 1 <= n <= 64:
        raise ValueError(f"Truncation length must be 1..64, got {n}")
    return hashlib.sha256(data).hexdigest()[:n]


# ---------------------------------------------------------------------------
# Payload / envelope hashing
# ---------------------------------------------------------------------------

def payload_hash_full(canonical_bytes_data: bytes) -> str:
    """SHA-256 hex digest of canonical envelope bytes.

    This is the primary content-addressable identifier for an artifact's
    payload. The input MUST already be RFC 8785 canonical JSON bytes.
    """
    return sha256_hex(canonical_bytes_data)


# ---------------------------------------------------------------------------
# Request deduplication key (SS4.4)
# ---------------------------------------------------------------------------

def request_key(
    upstream_instance_id: str,
    prefix: str,
    tool_name: str,
    canonical_args_bytes: bytes,
) -> str:
    """Derive the request deduplication key per spec SS4.4.

    The key is ``sha256(upstream_instance_id | "|" | prefix | "|" | tool_name | "|" | canonical_args_bytes)``.

    Args:
        upstream_instance_id: The upstream's stable identity hash.
        prefix: The tool namespace prefix (e.g. ``"github"``).
        tool_name: The tool name within the upstream.
        canonical_args_bytes: RFC 8785 canonical JSON bytes of the forwarded args.

    Returns:
        A 64-character hex string.
    """
    preimage = (
        upstream_instance_id.encode("utf-8")
        + b"|"
        + prefix.encode("utf-8")
        + b"|"
        + tool_name.encode("utf-8")
        + b"|"
        + canonical_args_bytes
    )
    return sha256_hex(preimage)


# ---------------------------------------------------------------------------
# Upstream instance identity (SS4.3)
# ---------------------------------------------------------------------------

def upstream_instance_id(canonical_semantic_bytes: bytes) -> str:
    """Derive the upstream instance identity from its canonical semantic descriptor.

    Returns a 32-character truncated SHA-256 hex string.
    """
    return sha256_trunc(canonical_semantic_bytes, 32)


# ---------------------------------------------------------------------------
# Binary blob hashing (SS6.1)
# ---------------------------------------------------------------------------

def binary_hash(raw_bytes: bytes) -> str:
    """SHA-256 hex digest of raw binary content."""
    return sha256_hex(raw_bytes)


def blob_id(binary_hash_hex: str) -> str:
    """Derive a blob storage ID from a binary hash.

    Format: ``"bin_" + binary_hash_hex[:32]``.
    """
    return "bin_" + binary_hash_hex[:32]


# ---------------------------------------------------------------------------
# Mapping budget fingerprint (SS13.5.3)
# ---------------------------------------------------------------------------

def map_budget_fingerprint(budget_obj: dict[str, Any]) -> str:
    """SHA-256 of the canonical JSON of a map budget object, truncated to 32 chars.

    This fingerprint is used to determine whether a cached partial-map result
    is still valid under the current budget constraints.
    """
    return sha256_trunc(_canonical_bytes(budget_obj), 32)


# ---------------------------------------------------------------------------
# Sample set hash (SS13.5.3)
# ---------------------------------------------------------------------------

def sample_set_hash(
    root_path: str,
    sample_indices: list[int],
    map_budget_fp: str,
    mapper_version: str,
) -> str:
    """Hash identifying a specific sample set for partial mapping.

    Incorporates the root path, selected sample indices, budget fingerprint,
    and mapper version into a single 32-char truncated SHA-256.
    """
    if any((not isinstance(i, int)) or i < 0 for i in sample_indices):
        raise ValueError("sample_indices must be a list of non-negative integers")
    if sample_indices != sorted(sample_indices):
        raise ValueError("sample_indices must be sorted ascending for stable hashing")
    if len(sample_indices) != len(set(sample_indices)):
        raise ValueError("sample_indices must not contain duplicates")

    obj = {
        "root_path": root_path,
        "sample_indices": sample_indices,
        "map_budget_fingerprint": map_budget_fp,
        "mapper_version": mapper_version,
    }
    return sha256_trunc(_canonical_bytes(obj), 32)


# ---------------------------------------------------------------------------
# PostgreSQL advisory lock keys (SS9.1)
# ---------------------------------------------------------------------------

def advisory_lock_keys(request_key_hex: str) -> tuple[int, int]:
    """Derive two 32-bit signed integers from a request key for ``pg_advisory_lock``.

    PostgreSQL's ``pg_advisory_lock(key1, key2)`` takes two ``int4`` values.
    We derive them by hashing the request key and splitting the first 8 bytes
    into two signed 32-bit integers.

    Args:
        request_key_hex: The 64-character hex request key.

    Returns:
        A tuple of two signed 32-bit integers ``(key1, key2)``.
    """
    digest = hashlib.sha256(request_key_hex.encode("utf-8")).digest()
    # Unpack first 8 bytes as two signed 32-bit big-endian integers.
    key1, key2 = struct.unpack(">ii", digest[:8])
    return key1, key2
