"""End-to-end tests for the oversize JSON pipeline.

Verifies the full cycle:
  detect oversize -> store as binary_ref -> replace part -> add meta.warnings
and integration with normalize_envelope.
"""

from __future__ import annotations

import hashlib
from decimal import Decimal

from mcp_artifact_gateway.canon.rfc8785 import canonical_bytes
from mcp_artifact_gateway.envelope.model import (
    BinaryRefContentPart,
    Envelope,
    JsonContentPart,
    TextContentPart,
)
from mcp_artifact_gateway.envelope.normalize import normalize_envelope
from mcp_artifact_gateway.envelope.oversize import replace_oversized_json_parts
from mcp_artifact_gateway.fs.blob_store import BlobStore
from mcp_artifact_gateway.util.hashing import sha256_hex


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_envelope(content_parts, meta=None):
    """Build a minimal Envelope with the given content parts."""
    return Envelope(
        upstream_instance_id="inst_test",
        upstream_prefix="test",
        tool="run",
        status="ok",
        content=content_parts,
        meta=meta or {"warnings": []},
    )


# ---------------------------------------------------------------------------
# Basic oversize replacement
# ---------------------------------------------------------------------------

def test_oversize_json_part_replaced_with_binary_ref(tmp_path) -> None:
    """A JSON part exceeding the threshold becomes a BinaryRefContentPart."""
    store = BlobStore(tmp_path / "blobs" / "bin")
    big_value = {"data": "x" * 5000}
    envelope = _make_envelope([JsonContentPart(value=big_value)])

    result = replace_oversized_json_parts(
        envelope,
        max_json_part_parse_bytes=100,
        blob_store=store,
    )

    assert len(result.content) == 1
    part = result.content[0]
    assert isinstance(part, BinaryRefContentPart)
    assert part.mime == "application/json"
    assert part.byte_count > 0


def test_oversize_adds_warning_with_correct_fields(tmp_path) -> None:
    """Warning entry must contain code, part_index, encoding, byte_count."""
    store = BlobStore(tmp_path / "blobs" / "bin")
    big_value = {"payload": "y" * 3000}
    envelope = _make_envelope([JsonContentPart(value=big_value)])

    result = replace_oversized_json_parts(
        envelope,
        max_json_part_parse_bytes=50,
        blob_store=store,
    )

    warnings = result.meta["warnings"]
    assert len(warnings) == 1
    w = warnings[0]
    assert w["code"] == "oversized_json_part"
    assert w["part_index"] == 0
    assert w["encoding"] == "utf-8"
    assert isinstance(w["byte_count"], int)
    assert w["byte_count"] > 0


def test_oversize_preserves_blob_content_integrity(tmp_path) -> None:
    """The stored blob must match the canonical bytes of the original JSON value."""
    store = BlobStore(tmp_path / "blobs" / "bin")
    big_value = {"key": "z" * 2000}
    envelope = _make_envelope([JsonContentPart(value=big_value)])

    expected_canonical = canonical_bytes(big_value)
    expected_hash = sha256_hex(expected_canonical)

    result = replace_oversized_json_parts(
        envelope,
        max_json_part_parse_bytes=50,
        blob_store=store,
    )

    ref_part = result.content[0]
    assert isinstance(ref_part, BinaryRefContentPart)
    assert ref_part.binary_hash == expected_hash
    assert ref_part.byte_count == len(expected_canonical)

    # Read stored blob and verify integrity
    with store.open_stream(ref_part.binary_hash) as f:
        stored_bytes = f.read()
    assert stored_bytes == expected_canonical
    assert sha256_hex(stored_bytes) == expected_hash


def test_under_threshold_json_part_kept(tmp_path) -> None:
    """A JSON part under the threshold remains untouched."""
    store = BlobStore(tmp_path / "blobs" / "bin")
    small_value = {"ok": True}
    envelope = _make_envelope([JsonContentPart(value=small_value)])

    result = replace_oversized_json_parts(
        envelope,
        max_json_part_parse_bytes=1_000_000,
        blob_store=store,
    )

    assert len(result.content) == 1
    assert isinstance(result.content[0], JsonContentPart)
    assert result.content[0].value == small_value
    assert result.meta["warnings"] == []


# ---------------------------------------------------------------------------
# Mixed content handling
# ---------------------------------------------------------------------------

def test_mixed_parts_only_oversized_replaced(tmp_path) -> None:
    """Only JSON parts exceeding threshold are replaced; others preserved."""
    store = BlobStore(tmp_path / "blobs" / "bin")
    small_json = JsonContentPart(value={"a": 1})
    big_json = JsonContentPart(value={"b": "x" * 5000})
    text_part = TextContentPart(text="hello world")

    envelope = _make_envelope([small_json, big_json, text_part])

    result = replace_oversized_json_parts(
        envelope,
        max_json_part_parse_bytes=200,
        blob_store=store,
    )

    assert len(result.content) == 3
    assert isinstance(result.content[0], JsonContentPart)
    assert isinstance(result.content[1], BinaryRefContentPart)
    assert isinstance(result.content[2], TextContentPart)

    # Warning should reference index 1
    assert len(result.meta["warnings"]) == 1
    assert result.meta["warnings"][0]["part_index"] == 1


def test_multiple_oversized_parts_all_replaced(tmp_path) -> None:
    """Multiple oversized JSON parts each get their own binary ref and warning."""
    store = BlobStore(tmp_path / "blobs" / "bin")
    parts = [JsonContentPart(value={"idx": i, "data": "x" * 2000}) for i in range(3)]
    envelope = _make_envelope(parts)

    result = replace_oversized_json_parts(
        envelope,
        max_json_part_parse_bytes=100,
        blob_store=store,
    )

    assert all(isinstance(p, BinaryRefContentPart) for p in result.content)
    assert len(result.meta["warnings"]) == 3
    indices = {w["part_index"] for w in result.meta["warnings"]}
    assert indices == {0, 1, 2}


# ---------------------------------------------------------------------------
# Existing warnings preserved
# ---------------------------------------------------------------------------

def test_oversize_preserves_existing_warnings(tmp_path) -> None:
    """Pre-existing warnings are not lost when oversize warnings are appended."""
    store = BlobStore(tmp_path / "blobs" / "bin")
    existing_warning = {"code": "something_else", "detail": "test"}
    envelope = _make_envelope(
        [JsonContentPart(value={"data": "x" * 3000})],
        meta={"warnings": [existing_warning]},
    )

    result = replace_oversized_json_parts(
        envelope,
        max_json_part_parse_bytes=50,
        blob_store=store,
    )

    assert len(result.meta["warnings"]) == 2
    assert result.meta["warnings"][0] == existing_warning
    assert result.meta["warnings"][1]["code"] == "oversized_json_part"


# ---------------------------------------------------------------------------
# Integration with normalize_envelope
# ---------------------------------------------------------------------------

def test_normalize_envelope_with_oversize_handling(tmp_path) -> None:
    """normalize_envelope replaces oversized JSON parts when configured."""
    store = BlobStore(tmp_path / "blobs" / "bin")
    big_content = [{"type": "json", "value": {"data": "x" * 5000}}]

    envelope = normalize_envelope(
        upstream_instance_id="inst_test",
        upstream_prefix="test",
        tool="run",
        content=big_content,
        max_json_part_parse_bytes=100,
        blob_store=store,
    )

    assert len(envelope.content) == 1
    assert isinstance(envelope.content[0], BinaryRefContentPart)
    assert envelope.content[0].mime == "application/json"
    assert len(envelope.meta["warnings"]) == 1
    assert envelope.meta["warnings"][0]["code"] == "oversized_json_part"


def test_normalize_envelope_without_oversize_keeps_json(tmp_path) -> None:
    """Without oversize params, normalize_envelope does not convert JSON parts."""
    content = [{"type": "json", "value": {"data": "x" * 5000}}]

    envelope = normalize_envelope(
        upstream_instance_id="inst_test",
        upstream_prefix="test",
        tool="run",
        content=content,
    )

    assert len(envelope.content) == 1
    assert isinstance(envelope.content[0], JsonContentPart)
    assert envelope.meta["warnings"] == []


def test_normalize_envelope_under_threshold_preserves_json(tmp_path) -> None:
    """With oversize params but small content, JSON parts remain untouched."""
    store = BlobStore(tmp_path / "blobs" / "bin")
    content = [{"type": "json", "value": {"ok": True}}]

    envelope = normalize_envelope(
        upstream_instance_id="inst_test",
        upstream_prefix="test",
        tool="run",
        content=content,
        max_json_part_parse_bytes=1_000_000,
        blob_store=store,
    )

    assert isinstance(envelope.content[0], JsonContentPart)
    assert envelope.meta["warnings"] == []


# ---------------------------------------------------------------------------
# Canonicalization correctness in oversize path
# ---------------------------------------------------------------------------

def test_oversize_blob_uses_canonical_bytes(tmp_path) -> None:
    """The blob stored for an oversized part must be RFC 8785 canonical JSON."""
    store = BlobStore(tmp_path / "blobs" / "bin")
    # Dict with keys in non-sorted order to verify canonical key ordering
    value = {"zebra": 1, "alpha": 2, "middle": 3}
    envelope = _make_envelope([JsonContentPart(value=value)])

    result = replace_oversized_json_parts(
        envelope,
        max_json_part_parse_bytes=1,  # force everything oversize
        blob_store=store,
    )

    ref_part = result.content[0]
    assert isinstance(ref_part, BinaryRefContentPart)

    with store.open_stream(ref_part.binary_hash) as f:
        stored = f.read()

    expected = canonical_bytes(value)
    assert stored == expected
    # Verify keys are sorted (alpha, middle, zebra)
    assert b'"alpha"' in stored
    text = stored.decode("utf-8")
    assert text.index('"alpha"') < text.index('"middle"') < text.index('"zebra"')


def test_oversize_blob_with_decimal_value(tmp_path) -> None:
    """Oversized JSON containing Decimal values is stored correctly."""
    store = BlobStore(tmp_path / "blobs" / "bin")
    value = {"amount": Decimal("123.456"), "data": "x" * 2000}
    envelope = _make_envelope([JsonContentPart(value=value)])

    result = replace_oversized_json_parts(
        envelope,
        max_json_part_parse_bytes=50,
        blob_store=store,
    )

    ref_part = result.content[0]
    assert isinstance(ref_part, BinaryRefContentPart)

    with store.open_stream(ref_part.binary_hash) as f:
        stored = f.read()

    assert stored == canonical_bytes(value)
    # Verify the decimal is rendered correctly (no float drift)
    assert b"123.456" in stored


# ---------------------------------------------------------------------------
# Full pipeline: normalize -> oversize -> payload_store -> reconstruct
# ---------------------------------------------------------------------------

def test_full_pipeline_normalize_oversize_store_reconstruct(tmp_path) -> None:
    """Full pipeline: normalize with oversize -> prepare_payload -> reconstruct."""
    from mcp_artifact_gateway.storage.payload_store import (
        prepare_payload,
        reconstruct_envelope,
    )

    store = BlobStore(tmp_path / "blobs" / "bin")
    big_content = [{"type": "json", "value": {"data": "x" * 5000}}]

    # Step 1: Normalize with oversize handling
    envelope = normalize_envelope(
        upstream_instance_id="inst_test",
        upstream_prefix="test",
        tool="run",
        content=big_content,
        max_json_part_parse_bytes=100,
        blob_store=store,
    )

    # Envelope should now have a binary_ref
    assert isinstance(envelope.content[0], BinaryRefContentPart)

    # Step 2: Prepare payload for storage
    envelope_dict = envelope.to_dict()
    prepared = prepare_payload(envelope_dict)

    # Step 3: Verify integrity - hash matches canonical bytes of the envelope dict
    expected_hash = sha256_hex(canonical_bytes(envelope_dict))
    assert prepared.payload_hash == expected_hash

    # Step 4: Reconstruct and verify
    reconstructed = reconstruct_envelope(
        prepared.compressed_bytes,
        prepared.encoding,
        prepared.payload_hash,
    )
    assert reconstructed["tool"] == "run"
    assert reconstructed["status"] == "ok"
    assert len(reconstructed["content"]) == 1
    assert reconstructed["content"][0]["type"] == "binary_ref"
    assert reconstructed["content"][0]["mime"] == "application/json"
    assert len(reconstructed["meta"]["warnings"]) == 1


def test_full_pipeline_small_content_no_oversize(tmp_path) -> None:
    """Small content goes through normalize and payload_store without oversize."""
    from mcp_artifact_gateway.storage.payload_store import (
        prepare_payload,
        reconstruct_envelope,
    )

    store = BlobStore(tmp_path / "blobs" / "bin")
    content = [{"type": "json", "value": {"ok": True}}]

    envelope = normalize_envelope(
        upstream_instance_id="inst_test",
        upstream_prefix="test",
        tool="run",
        content=content,
        max_json_part_parse_bytes=1_000_000,
        blob_store=store,
    )

    assert isinstance(envelope.content[0], JsonContentPart)

    envelope_dict = envelope.to_dict()
    prepared = prepare_payload(envelope_dict)
    reconstructed = reconstruct_envelope(
        prepared.compressed_bytes,
        prepared.encoding,
        prepared.payload_hash,
    )

    assert reconstructed["content"][0]["type"] == "json"
    assert reconstructed["content"][0]["value"] == {"ok": True}
    assert reconstructed["meta"]["warnings"] == []
