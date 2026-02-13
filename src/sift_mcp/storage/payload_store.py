"""Prepare and reconstruct envelope payloads for database storage.

Canonicalizes envelope dicts to RFC 8785 bytes, compresses them
with zstd (or stores raw), hashes for integrity, and computes
JSON/binary size metrics.  ``prepare_payload`` produces a
``PreparedPayload`` ready for insertion into ``payload_blobs``.
``reconstruct_envelope`` reverses the process with integrity
verification.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any

from sift_mcp.canon import (
    canonical_bytes,
    compress_bytes,
    decompress_bytes,
)
from sift_mcp.canon.decimal_json import loads_decimal
from sift_mcp.config.settings import (
    CanonicalEncoding,
    EnvelopeJsonbMode,
)
from sift_mcp.constants import CANONICALIZER_VERSION
from sift_mcp.util.hashing import payload_hash_full, sha256_hex


@dataclass(frozen=True)
class PreparedPayload:
    """All data needed to insert a payload_blobs row.

    Produced by ``prepare_payload`` after canonicalization,
    compression, and integrity verification of an envelope dict.

    Attributes:
        payload_hash: SHA-256 hex of uncompressed canonical
            bytes (storage identity).
        encoding: Compression encoding name (e.g. ``zstd``).
        compressed_bytes: The compressed payload data.
        uncompressed_len: Byte length before compression.
        canonicalizer_version: Version string of the
            canonicalizer used.
        payload_json_bytes: Total JSON content size in bytes.
        payload_binary_bytes_total: Total binary ref size in
            bytes.
        payload_total_bytes: Sum of JSON and binary byte counts.
        contains_binary_refs: True if any content part is a
            binary or image reference.
        envelope_jsonb: Optional JSONB representation for
            database indexing (None if disabled).
    """

    payload_hash: str
    encoding: str
    compressed_bytes: bytes
    uncompressed_len: int
    canonicalizer_version: str
    payload_json_bytes: int
    payload_binary_bytes_total: int
    payload_total_bytes: int
    contains_binary_refs: bool
    envelope_jsonb: dict[str, Any] | None


def _compute_content_sizes(
    envelope_dict: dict[str, Any],
) -> tuple[int, int, bool]:
    """Compute content size metrics from envelope parts.

    Args:
        envelope_dict: Envelope dictionary with a ``content``
            list of typed parts.

    Returns:
        Tuple of (json_bytes, binary_bytes_total,
        has_binary_refs).
    """
    json_bytes = 0
    binary_bytes_total = 0
    has_binary_refs = False

    for part in envelope_dict.get("content", []):
        part_type = part.get("type")
        if part_type in ("binary_ref", "image_ref"):
            has_binary_refs = True
            binary_bytes_total += part.get("byte_count", 0)
        else:
            # Use canonical_bytes to handle Decimal values safely
            json_bytes += len(canonical_bytes(part))

    if json_bytes == 0 and not has_binary_refs:
        # Use canonical_bytes to handle Decimal values safely
        json_bytes = len(canonical_bytes(envelope_dict))

    return json_bytes, binary_bytes_total, has_binary_refs


def _build_jsonb(
    envelope_dict: dict[str, Any],
    mode: EnvelopeJsonbMode,
    threshold: int,
    uncompressed_len: int,
) -> dict[str, Any] | None:
    """Determine JSONB representation based on mode and size.

    Args:
        envelope_dict: Full envelope dictionary.
        mode: JSONB storage mode (none, full, or
            minimal_for_large).
        threshold: Byte threshold for minimal_for_large mode.
        uncompressed_len: Uncompressed canonical byte length.

    Returns:
        Full or minimal envelope dict for JSONB storage, or
        None if mode is ``none``.
    """
    if mode == EnvelopeJsonbMode.none:
        return None
    if mode == EnvelopeJsonbMode.full:
        return envelope_dict
    # minimal_for_large: store full if small, minimal if large
    if uncompressed_len <= threshold:
        return envelope_dict
    return {
        "type": envelope_dict.get("type"),
        "upstream_instance_id": envelope_dict.get("upstream_instance_id"),
        "upstream_prefix": envelope_dict.get("upstream_prefix"),
        "tool": envelope_dict.get("tool"),
        "status": envelope_dict.get("status"),
    }


def prepare_payload(
    envelope_dict: dict[str, Any],
    *,
    encoding: CanonicalEncoding = CanonicalEncoding.zstd,
    jsonb_mode: EnvelopeJsonbMode = EnvelopeJsonbMode.full,
    jsonb_minimize_threshold: int = 1_000_000,
) -> PreparedPayload:
    """Canonicalize, hash, compress, and verify an envelope.

    Args:
        envelope_dict: Envelope dictionary to prepare.
        encoding: Compression encoding to apply.
        jsonb_mode: JSONB storage mode for database indexing.
        jsonb_minimize_threshold: Byte threshold for the
            minimal_for_large JSONB mode.

    Returns:
        A PreparedPayload ready for database insertion.

    Raises:
        ValueError: If compression integrity check fails.
    """
    # 1. Canonicalize
    uncompressed = canonical_bytes(envelope_dict)

    # 2. Hash uncompressed canonical bytes
    p_hash = payload_hash_full(uncompressed)

    # 3. Compress
    compressed = compress_bytes(uncompressed, encoding.value)

    # 4. Verify integrity: decompress and check hash matches
    roundtrip = decompress_bytes(compressed.data, compressed.encoding)
    roundtrip_hash = sha256_hex(roundtrip)
    if roundtrip_hash != p_hash:
        msg = (
            f"compression integrity check failed: "
            f"expected {p_hash}, got {roundtrip_hash}"
        )
        raise ValueError(msg)

    # 5. Compute sizes from envelope content parts
    json_bytes, binary_bytes_total, has_binary_refs = _compute_content_sizes(
        envelope_dict
    )
    total_bytes = json_bytes + binary_bytes_total

    # 6. Determine JSONB storage based on mode/threshold
    envelope_jsonb = _build_jsonb(
        envelope_dict, jsonb_mode, jsonb_minimize_threshold, len(uncompressed)
    )

    return PreparedPayload(
        payload_hash=p_hash,
        encoding=compressed.encoding,
        compressed_bytes=compressed.data,
        uncompressed_len=compressed.uncompressed_len,
        canonicalizer_version=CANONICALIZER_VERSION,
        payload_json_bytes=json_bytes,
        payload_binary_bytes_total=binary_bytes_total,
        payload_total_bytes=total_bytes,
        contains_binary_refs=has_binary_refs,
        envelope_jsonb=envelope_jsonb,
    )


def reconstruct_envelope(
    compressed_bytes: bytes,
    encoding: str,
    expected_hash: str,
) -> dict[str, Any]:
    """Decompress canonical bytes and verify integrity.

    Args:
        compressed_bytes: Compressed payload data.
        encoding: Compression encoding name (e.g. ``zstd``).
        expected_hash: SHA-256 hex digest of the uncompressed
            canonical bytes.

    Returns:
        Reconstructed envelope dictionary.

    Raises:
        ValueError: If integrity check fails or payload is not
            valid JSON.
    """
    decompressed = decompress_bytes(compressed_bytes, encoding)
    actual_hash = sha256_hex(decompressed)
    if actual_hash != expected_hash:
        msg = (
            f"envelope integrity check failed: "
            f"expected {expected_hash}, got {actual_hash}"
        )
        raise ValueError(msg)
    try:
        # Use loads_decimal to preserve Decimal values (no Python float drift)
        payload = loads_decimal(decompressed)
    except (json.JSONDecodeError, ValueError) as exc:
        msg = "envelope payload is not valid JSON"
        raise ValueError(msg) from exc
    if not isinstance(payload, dict):
        msg = "envelope payload must be a JSON object"
        raise ValueError(msg)
    return payload
