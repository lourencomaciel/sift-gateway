"""Tests for payload canonical integrity: prepare, compress, reconstruct."""

from __future__ import annotations

from decimal import Decimal
import hashlib

from sift_mcp.canon import (
    canonical_bytes,
    compress_bytes,
    decompress_bytes,
)
from sift_mcp.config.settings import (
    CanonicalEncoding,
    EnvelopeJsonbMode,
)
from sift_mcp.constants import CANONICALIZER_VERSION
from sift_mcp.storage.payload_store import (
    PreparedPayload,
    prepare_payload,
    reconstruct_envelope,
)
from sift_mcp.util.hashing import payload_hash_full


def _sample_envelope() -> dict:
    return {
        "type": "mcp_envelope",
        "upstream_instance_id": "abc123",
        "upstream_prefix": "github",
        "tool": "list_repos",
        "status": "ok",
        "content": [
            {"type": "json", "value": {"repos": ["a", "b"]}},
        ],
        "error": None,
        "meta": {"warnings": []},
    }


def _envelope_with_binary_ref() -> dict:
    return {
        "type": "mcp_envelope",
        "upstream_instance_id": "abc123",
        "upstream_prefix": "github",
        "tool": "get_file",
        "status": "ok",
        "content": [
            {"type": "json", "value": {"name": "file.bin"}},
            {
                "type": "binary_ref",
                "blob_id": "bin_aaaa",
                "binary_hash": "a" * 64,
                "mime": "application/octet-stream",
                "byte_count": 1024,
            },
        ],
        "error": None,
        "meta": {"warnings": []},
    }


# ---- PreparedPayload creation ----


def test_prepare_payload_creates_valid_result() -> None:
    env = _sample_envelope()
    result = prepare_payload(env)
    assert isinstance(result, PreparedPayload)
    assert len(result.payload_hash) == 64
    assert result.encoding in ("zstd", "gzip", "none")
    assert result.uncompressed_len > 0
    assert result.canonicalizer_version == CANONICALIZER_VERSION
    assert result.contains_binary_refs is False
    assert result.payload_binary_bytes_total == 0
    assert result.payload_json_bytes > 0
    assert result.payload_total_bytes == result.payload_json_bytes


def test_prepare_payload_with_binary_refs() -> None:
    env = _envelope_with_binary_ref()
    result = prepare_payload(env)
    assert result.contains_binary_refs is True
    assert result.payload_binary_bytes_total == 1024
    assert result.payload_total_bytes == result.payload_json_bytes + 1024


# ---- Compression roundtrip ----


def test_compression_roundtrip_zstd() -> None:
    env = _sample_envelope()
    uncompressed = canonical_bytes(env)
    compressed = compress_bytes(uncompressed, "zstd")
    roundtrip = decompress_bytes(compressed.data, compressed.encoding)
    assert roundtrip == uncompressed


def test_compression_roundtrip_gzip() -> None:
    env = _sample_envelope()
    uncompressed = canonical_bytes(env)
    compressed = compress_bytes(uncompressed, "gzip")
    roundtrip = decompress_bytes(compressed.data, compressed.encoding)
    assert roundtrip == uncompressed


def test_compression_roundtrip_none() -> None:
    env = _sample_envelope()
    uncompressed = canonical_bytes(env)
    compressed = compress_bytes(uncompressed, "none")
    assert compressed.data == uncompressed


# ---- Hash integrity ----


def test_payload_hash_matches_sha256_of_uncompressed() -> None:
    env = _sample_envelope()
    uncompressed = canonical_bytes(env)
    expected = hashlib.sha256(uncompressed).hexdigest()
    assert payload_hash_full(uncompressed) == expected


def test_prepare_payload_hash_integrity() -> None:
    env = _sample_envelope()
    result = prepare_payload(env)
    uncompressed = canonical_bytes(env)
    assert result.payload_hash == hashlib.sha256(uncompressed).hexdigest()


# ---- JSONB modes ----


def test_jsonb_mode_full() -> None:
    env = _sample_envelope()
    result = prepare_payload(env, jsonb_mode=EnvelopeJsonbMode.full)
    assert result.envelope_jsonb is not None
    assert result.envelope_jsonb["type"] == "mcp_envelope"
    assert "content" in result.envelope_jsonb


def test_jsonb_mode_none() -> None:
    env = _sample_envelope()
    result = prepare_payload(env, jsonb_mode=EnvelopeJsonbMode.none)
    assert result.envelope_jsonb is None


def test_jsonb_mode_minimal_for_large_small_envelope() -> None:
    """Small envelopes get full JSONB even in minimal_for_large mode."""
    env = _sample_envelope()
    result = prepare_payload(
        env,
        jsonb_mode=EnvelopeJsonbMode.minimal_for_large,
        jsonb_minimize_threshold=1_000_000,
    )
    assert result.envelope_jsonb is not None
    assert "content" in result.envelope_jsonb


def test_jsonb_mode_minimal_for_large_big_envelope() -> None:
    """Large envelopes get minimal JSONB in minimal_for_large mode."""
    env = _sample_envelope()
    result = prepare_payload(
        env,
        jsonb_mode=EnvelopeJsonbMode.minimal_for_large,
        jsonb_minimize_threshold=1,  # Force "large" behavior
    )
    assert result.envelope_jsonb is not None
    assert "content" not in result.envelope_jsonb
    assert result.envelope_jsonb["type"] == "mcp_envelope"
    assert result.envelope_jsonb["tool"] == "list_repos"


# ---- Encoding options ----


def test_prepare_payload_gzip_encoding() -> None:
    env = _sample_envelope()
    result = prepare_payload(env, encoding=CanonicalEncoding.gzip)
    assert result.encoding == "gzip"


def test_prepare_payload_none_encoding() -> None:
    env = _sample_envelope()
    result = prepare_payload(env, encoding=CanonicalEncoding.none)
    assert result.encoding == "none"


# ---- reconstruct_envelope ----


def test_reconstruct_envelope_roundtrip() -> None:
    env = _sample_envelope()
    prepared = prepare_payload(env)
    reconstructed = reconstruct_envelope(
        prepared.compressed_bytes,
        prepared.encoding,
        prepared.payload_hash,
    )
    assert reconstructed["type"] == "mcp_envelope"
    assert reconstructed["tool"] == "list_repos"
    assert reconstructed["status"] == "ok"


def test_reconstruct_envelope_verifies_hash() -> None:
    env = _sample_envelope()
    prepared = prepare_payload(env)
    try:
        reconstruct_envelope(
            prepared.compressed_bytes,
            prepared.encoding,
            "0000000000000000000000000000000000000000000000000000000000000000",
        )
    except ValueError as exc:
        assert "integrity" in str(exc)
    else:
        raise AssertionError("expected ValueError for hash mismatch")


def test_reconstruct_envelope_rejects_invalid_json_payload() -> None:
    raw = b"this is not json"
    compressed = compress_bytes(raw, "none")
    expected_hash = hashlib.sha256(raw).hexdigest()
    try:
        reconstruct_envelope(
            compressed.data, compressed.encoding, expected_hash
        )
    except ValueError as exc:
        assert "valid JSON" in str(exc)
    else:
        raise AssertionError("expected ValueError for invalid JSON payload")


def test_reconstruct_envelope_rejects_non_object_json() -> None:
    raw = b'["not","an","object"]'
    compressed = compress_bytes(raw, "none")
    expected_hash = hashlib.sha256(raw).hexdigest()
    try:
        reconstruct_envelope(
            compressed.data, compressed.encoding, expected_hash
        )
    except ValueError as exc:
        assert "JSON object" in str(exc)
    else:
        raise AssertionError(
            "expected ValueError for non-object envelope payload"
        )


def test_reconstruct_envelope_all_encodings() -> None:
    """Verify reconstruct works for all three encoding modes."""
    env = _sample_envelope()
    for enc in (
        CanonicalEncoding.zstd,
        CanonicalEncoding.gzip,
        CanonicalEncoding.none,
    ):
        prepared = prepare_payload(env, encoding=enc)
        reconstructed = reconstruct_envelope(
            prepared.compressed_bytes,
            prepared.encoding,
            prepared.payload_hash,
        )
        assert reconstructed["tool"] == "list_repos"


# ---- Decimal safety: reconstruct_envelope uses loads_decimal ----


def _envelope_with_decimal() -> dict:
    return {
        "type": "mcp_envelope",
        "upstream_instance_id": "abc123",
        "upstream_prefix": "github",
        "tool": "get_price",
        "status": "ok",
        "content": [
            {
                "type": "json",
                "value": {"price": Decimal("19.99"), "quantity": 5},
            },
        ],
        "error": None,
        "meta": {"warnings": []},
    }


def test_prepare_and_reconstruct_preserves_decimal() -> None:
    """Decimal values survive prepare -> reconstruct without float drift."""
    env = _envelope_with_decimal()
    prepared = prepare_payload(env)
    reconstructed = reconstruct_envelope(
        prepared.compressed_bytes,
        prepared.encoding,
        prepared.payload_hash,
    )
    # The reconstructed value should be Decimal, not float
    price = reconstructed["content"][0]["value"]["price"]
    assert isinstance(price, Decimal), (
        f"expected Decimal, got {type(price).__name__}"
    )
    assert price == Decimal("19.99")


def test_payload_hash_is_sha256_of_canonical_uncompressed() -> None:
    """payload_hash_full == sha256(canonical_bytes(envelope_dict))."""
    env = _envelope_with_decimal()
    uncompressed = canonical_bytes(env)
    result = prepare_payload(env)
    assert result.payload_hash == hashlib.sha256(uncompressed).hexdigest()
    assert result.payload_hash == payload_hash_full(uncompressed)


def test_compression_integrity_verified_in_prepare() -> None:
    """prepare_payload verifies decompress(compress(x)) == x."""
    env = _sample_envelope()
    # This implicitly tests the integrity check inside prepare_payload
    for enc in (
        CanonicalEncoding.zstd,
        CanonicalEncoding.gzip,
        CanonicalEncoding.none,
    ):
        result = prepare_payload(env, encoding=enc)
        # Verify we can reconstruct successfully
        reconstructed = reconstruct_envelope(
            result.compressed_bytes,
            result.encoding,
            result.payload_hash,
        )
        assert reconstructed["tool"] == "list_repos"


def test_reconstruct_roundtrip_with_decimal_preserves_hash() -> None:
    """Full cycle: canonical -> compress -> decompress -> hash check with Decimal data."""
    env = _envelope_with_decimal()
    uncompressed = canonical_bytes(env)
    expected_hash = hashlib.sha256(uncompressed).hexdigest()

    compressed = compress_bytes(uncompressed, "zstd")
    decompressed = decompress_bytes(compressed.data, compressed.encoding)
    assert hashlib.sha256(decompressed).hexdigest() == expected_hash
    assert decompressed == uncompressed
