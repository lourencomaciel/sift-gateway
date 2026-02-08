from pathlib import Path

import pytest

from mcp_artifact_gateway.fs.resource_store import ResourceStore


@pytest.mark.asyncio
async def test_store_internal_and_resolve(tmp_path) -> None:
    store = ResourceStore(tmp_path)
    rel_path, content_hash = await store.store_internal(
        "https://example.com/path/file.txt", b"hello"
    )
    full_path = store.resolve_path(rel_path)
    assert full_path.exists()
    assert full_path.read_bytes() == b"hello"
    assert content_hash


def test_resolve_path_rejects_escape(tmp_path) -> None:
    store = ResourceStore(tmp_path)
    with pytest.raises(ValueError):
        store.resolve_path("../evil.txt")
