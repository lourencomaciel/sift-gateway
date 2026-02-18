"""Provide SHA-256-based hashing for identity and integrity.

Centralizes all hash derivations used by the gateway: raw
digests, truncated digests, blob IDs, request keys, advisory
lock keys, payload hashes, upstream instance IDs, map budget
fingerprints, and sample set hashes.  Every function is
deterministic and side-effect-free.
"""

from __future__ import annotations

import hashlib
from typing import Any

from sift_mcp.canon.rfc8785 import canonical_bytes
from sift_mcp.constants import BLOB_ID_PREFIX


def sha256_hex(data: bytes) -> str:
    """Return the full SHA-256 hex digest of raw bytes.

    Args:
        data: Bytes to hash.

    Returns:
        64-character lowercase hex digest string.
    """
    return hashlib.sha256(data).hexdigest()


def sha256_trunc(data: bytes, chars: int) -> str:
    """Return truncated SHA-256 hex digest.

    Args:
        data: Bytes to hash.
        chars: Number of leading hex characters to keep.

    Returns:
        Truncated hex digest string of length *chars*.

    Raises:
        ValueError: If *chars* is not positive.
    """
    if chars <= 0:
        msg = "chars must be positive"
        raise ValueError(msg)
    return sha256_hex(data)[:chars]


def binary_hash(data: bytes) -> str:
    """Return SHA-256 hex digest of raw binary data.

    Semantic alias for ``sha256_hex`` used when hashing
    binary blob content.

    Args:
        data: Raw bytes to hash.

    Returns:
        64-character lowercase hex digest string.
    """
    return sha256_hex(data)


def blob_id(binary_hash_hex: str) -> str:
    """Derive a blob ID from a binary hash hex string.

    Concatenate the ``bin_`` prefix with the first 32 hex
    characters of the hash for a compact identifier.

    Args:
        binary_hash_hex: Full SHA-256 hex digest of the
            binary content.

    Returns:
        Blob ID string (e.g. ``bin_<32 hex chars>``).
    """
    return BLOB_ID_PREFIX + binary_hash_hex[:32]


def payload_hash_full(canonical_bytes_uncompressed: bytes) -> str:
    """Return SHA-256 hex digest of uncompressed canonical bytes.

    Used to fingerprint the full envelope payload before
    compression for integrity verification.

    Args:
        canonical_bytes_uncompressed: Uncompressed
            canonical JSON bytes.

    Returns:
        64-character lowercase hex digest string.
    """
    return sha256_hex(canonical_bytes_uncompressed)


def upstream_instance_id(canonical_semantic_identity_bytes: bytes) -> str:
    """Derive a truncated upstream instance identifier.

    Hash the canonical semantic identity bytes with SHA-256
    and return the first 32 hex characters (128-bit).

    Args:
        canonical_semantic_identity_bytes: Canonical JSON
            bytes encoding the upstream's stable identity.

    Returns:
        32-character hex string identifying the upstream.
    """
    return hashlib.sha256(canonical_semantic_identity_bytes).hexdigest()[:32]


def request_key(
    upstream_instance_id: str,
    prefix: str,
    tool: str,
    canonical_args_bytes: bytes,
) -> str:
    """Compute a request deduplication key.

    Concatenate the upstream instance ID, prefix, tool
    name, and canonical argument bytes with pipe delimiters
    and return the SHA-256 hex digest.

    Args:
        upstream_instance_id: Upstream identity hash.
        prefix: Tool namespace prefix.
        tool: Tool name.
        canonical_args_bytes: RFC 8785 canonical JSON
            bytes of the tool arguments.

    Returns:
        64-character hex digest uniquely identifying the
        request.
    """
    digest = hashlib.sha256()
    digest.update(upstream_instance_id.encode("utf-8"))
    digest.update(b"|")
    digest.update(prefix.encode("utf-8"))
    digest.update(b"|")
    digest.update(tool.encode("utf-8"))
    digest.update(b"|")
    digest.update(canonical_args_bytes)
    return digest.hexdigest()


def map_budget_fingerprint(
    *,
    mapper_version: str,
    traversal_contract_version: str,
    map_backend_id: str,
    prng_version: str,
    budgets: dict[str, Any],
) -> str:
    """Compute a truncated fingerprint of mapping budgets.

    Serialize the version strings and budget parameters
    into RFC 8785 canonical JSON and return the first 32
    hex characters of the SHA-256 digest.

    Args:
        mapper_version: Mapper algorithm version string.
        traversal_contract_version: Traversal contract
            version string.
        map_backend_id: Backend identifier string.
        prng_version: PRNG algorithm version string.
        budgets: Dict of budget parameter key-value pairs.

    Returns:
        32-character hex fingerprint string.
    """
    payload: dict[str, Any] = {
        "map_backend_id": map_backend_id,
        "mapper_version": mapper_version,
        "prng_version": prng_version,
        "traversal_contract_version": traversal_contract_version,
        **budgets,
    }
    return hashlib.sha256(canonical_bytes(payload)).hexdigest()[:32]


def sample_set_hash(
    *,
    root_path: str,
    sample_indices: list[int],
    map_budget_fingerprint: str,
    mapper_version: str,
) -> str:
    """Compute a truncated hash identifying a sample set.

    Serialize the root path, sample indices, budget
    fingerprint, and mapper version into RFC 8785 canonical
    JSON and return the first 32 hex characters of the
    SHA-256 digest (128-bit).

    Args:
        root_path: JSONPath to the root array.
        sample_indices: Sorted list of sampled indices.
        map_budget_fingerprint: Budget fingerprint string.
        mapper_version: Mapper algorithm version string.

    Returns:
        32-character hex sample set identifier.
    """
    payload = {
        "root_path": root_path,
        "sample_indices": sample_indices,
        "map_budget_fingerprint": map_budget_fingerprint,
        "mapper_version": mapper_version,
    }
    return hashlib.sha256(canonical_bytes(payload)).hexdigest()[:32]


