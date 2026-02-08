"""Hashing helpers used across gateway identity and integrity paths."""

from __future__ import annotations

import hashlib
import struct

from mcp_artifact_gateway.canon.rfc8785 import canonical_bytes
from mcp_artifact_gateway.constants import BLOB_ID_PREFIX


def sha256_hex(data: bytes) -> str:
    """Return SHA-256 hex digest."""
    return hashlib.sha256(data).hexdigest()


def sha256_trunc(data: bytes, chars: int) -> str:
    """Return first `chars` chars of SHA-256 hex digest."""
    if chars <= 0:
        msg = "chars must be positive"
        raise ValueError(msg)
    return sha256_hex(data)[:chars]


def binary_hash(data: bytes) -> str:
    """sha256(raw_bytes).hexdigest()"""
    return sha256_hex(data)


def blob_id(binary_hash_hex: str) -> str:
    """'bin_' + binary_hash[:32]"""
    return BLOB_ID_PREFIX + binary_hash_hex[:32]


def advisory_lock_keys(request_key_str: str) -> tuple[int, int]:
    """Two signed int32 keys derived from sha256(request_key) for pg_advisory_lock."""
    digest = hashlib.sha256(request_key_str.encode("utf-8")).digest()
    key1_u32, key2_u32 = struct.unpack(">II", digest[:8])
    return _to_signed_int32(key1_u32), _to_signed_int32(key2_u32)


def payload_hash_full(canonical_bytes_uncompressed: bytes) -> str:
    """sha256(envelope_canonical_bytes_uncompressed).hexdigest()"""
    return sha256_hex(canonical_bytes_uncompressed)


def upstream_instance_id(canonical_semantic_identity_bytes: bytes) -> str:
    """sha256(canonical_semantic_identity_bytes)[:32] hex"""
    return hashlib.sha256(canonical_semantic_identity_bytes).hexdigest()[:32]


def request_key(
    upstream_instance_id: str,
    prefix: str,
    tool: str,
    canonical_args_bytes: bytes,
) -> str:
    """sha256(upstream_instance_id|prefix|tool|canonical_args_bytes).hexdigest()"""
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
    budgets: dict,
) -> str:
    """sha256(canonical_json({mapper_version, ...+ all budget keys}))[:32]

    Uses RFC 8785 canonical JSON for determinism.
    """
    payload: dict = {
        "map_backend_id": map_backend_id,
        "mapper_version": mapper_version,
        "prng_version": prng_version,
        "traversal_contract_version": traversal_contract_version,
    }
    for key, value in budgets.items():
        payload[key] = value
    return hashlib.sha256(canonical_bytes(payload)).hexdigest()[:32]


def sample_set_hash(
    *,
    root_path: str,
    sample_indices: list[int],
    map_budget_fingerprint: str,
    mapper_version: str,
) -> str:
    """sha256(canonical_json({root_path, sample_indices, ...}))[:32].

    This intentionally truncates to 32 hex chars (128-bit) for compactness.
    """
    payload = {
        "root_path": root_path,
        "sample_indices": sample_indices,
        "map_budget_fingerprint": map_budget_fingerprint,
        "mapper_version": mapper_version,
    }
    return hashlib.sha256(canonical_bytes(payload)).hexdigest()[:32]


def _to_signed_int32(value: int) -> int:
    """Convert an unsigned 32-bit integer to signed int32."""
    if value >= 0x80000000:
        return value - 0x100000000
    return value
