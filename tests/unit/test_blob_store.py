from __future__ import annotations

from pathlib import Path

import pytest

from sift_gateway.fs.blob_store import BlobStore


def test_blob_store_put_and_open(tmp_path: Path) -> None:
    store = BlobStore(tmp_path / "blobs" / "bin", probe_bytes=4)
    payload = b"abcdef123456"

    ref = store.put_bytes(payload, mime="IMAGE/JPG; charset=utf-8")
    assert ref.mime == "image/jpeg"
    assert ref.byte_count == len(payload)
    assert Path(ref.fs_path).exists()
    assert Path(ref.fs_path).read_bytes() == payload

    with store.open_stream(ref.binary_hash) as handle:
        assert handle.read() == payload


def test_blob_store_rejects_existing_size_mismatch(tmp_path: Path) -> None:
    store = BlobStore(tmp_path / "blobs" / "bin")
    payload = b"same-hash-source"
    ref = store.put_bytes(payload)

    # Corrupt file size manually and force re-put same payload hash path.
    Path(ref.fs_path).write_bytes(b"x")
    with pytest.raises(ValueError, match="size mismatch"):
        store.put_bytes(payload)


def test_blob_store_rejects_existing_content_mismatch_same_size(
    tmp_path: Path,
) -> None:
    store = BlobStore(tmp_path / "blobs" / "bin")
    payload = b"same-size-bytes"
    ref = store.put_bytes(payload)

    # Corrupt content while preserving file size.
    Path(ref.fs_path).write_bytes(b"X" * len(payload))
    with pytest.raises(ValueError, match="content hash mismatch"):
        store.put_bytes(payload)
