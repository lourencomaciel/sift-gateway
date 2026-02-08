import hashlib

import pytest

from mcp_artifact_gateway.fs.blob_store import BlobStore


@pytest.mark.asyncio
async def test_put_bytes_and_dedupe(tmp_path) -> None:
    store = BlobStore(tmp_path, probe_bytes=2)
    data = b"abcdef"
    ref = await store.put_bytes(data, mime="text/json")

    assert ref.byte_count == len(data)
    assert ref.mime == "application/json"
    assert ref.blob_id.startswith("bin_")
    assert store.blob_path(ref.binary_hash).exists()

    # Second put should dedupe
    ref2 = await store.put_bytes(data, mime="application/json")
    assert ref2.binary_hash == ref.binary_hash


@pytest.mark.asyncio
async def test_existing_blob_size_mismatch(tmp_path) -> None:
    store = BlobStore(tmp_path)
    data = b"abc"
    binary_hash = hashlib.sha256(data).hexdigest()
    path = store.blob_path(binary_hash)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"ab")  # wrong size

    with pytest.raises(ValueError):
        await store.put_bytes(data)
