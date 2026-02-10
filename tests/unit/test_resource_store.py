from __future__ import annotations

from pathlib import Path

from mcp_artifact_gateway.fs.resource_store import ResourceStore
from mcp_artifact_gateway.util.hashing import sha256_hex


def test_resource_store_internal_writes_file(tmp_path: Path) -> None:
    store = ResourceStore(tmp_path / "resources")
    payload = b"hello-resource"

    ref = store.put_bytes(
        payload, mime="text/plain", name="greeting.txt", durability="internal"
    )
    assert ref.durability == "internal"
    assert ref.content_hash is not None
    assert ref.fs_path is not None
    assert Path(ref.fs_path).exists()
    assert Path(ref.fs_path).read_bytes() == payload


def test_resource_store_external_ref_does_not_write(tmp_path: Path) -> None:
    store = ResourceStore(tmp_path / "resources")
    ref = store.put_bytes(
        b"opaque",
        durability="external_ref",
        source_uri="https://example.com/file.pdf",
    )
    assert ref.durability == "external_ref"
    assert ref.uri == "https://example.com/file.pdf"
    assert ref.content_hash == f"sha256:{sha256_hex(b'opaque')}"
    assert ref.fs_path is None


def test_resource_store_external_ref_empty_payload_has_sha256_hash(
    tmp_path: Path,
) -> None:
    store = ResourceStore(tmp_path / "resources")
    ref = store.put_bytes(
        b"",
        durability="external_ref",
        source_uri="https://example.com/empty.bin",
    )
    assert ref.content_hash == f"sha256:{sha256_hex(b'')}"


def test_resource_store_external_ref_requires_source_uri(
    tmp_path: Path,
) -> None:
    store = ResourceStore(tmp_path / "resources")
    try:
        store.put_bytes(b"opaque", durability="external_ref")
    except ValueError as exc:
        assert "requires non-empty source_uri" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_resource_store_rejects_existing_content_mismatch_same_size(
    tmp_path: Path,
) -> None:
    store = ResourceStore(tmp_path / "resources")
    payload = b"same-size-bytes"
    ref = store.put_bytes(payload, durability="internal")

    # Corrupt content while preserving file size.
    Path(ref.fs_path).write_bytes(b"X" * len(payload))
    try:
        store.put_bytes(payload, durability="internal")
    except ValueError as exc:
        assert "content hash mismatch" in str(exc)
    else:
        raise AssertionError("expected ValueError")
