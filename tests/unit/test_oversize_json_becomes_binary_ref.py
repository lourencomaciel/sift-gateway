from pathlib import Path

import pytest

from mcp_artifact_gateway.envelope.oversize import check_and_offload_oversize_json
from mcp_artifact_gateway.fs.blob_store import BinaryRef


@pytest.mark.asyncio
async def test_oversize_json_offloaded(tmp_path) -> None:
    async def fake_put(raw_bytes: bytes, mime: str | None) -> BinaryRef:
        return BinaryRef(
            binary_hash="hash",
            blob_id="bin_hash",
            byte_count=len(raw_bytes),
            mime=mime,
            fs_path=Path(tmp_path / "bin"),
        )

    parts = [
        {"type": "json", "value": {"a": "b"}},
        {"type": "text", "text": "ok"},
    ]

    converted, warnings = await check_and_offload_oversize_json(
        parts, max_json_part_parse_bytes=1, blob_store_put_fn=fake_put
    )

    assert converted[0].type == "binary_ref"
    assert converted[1].type == "text"
    assert warnings
    assert warnings[0]["original_part_index"] == 0
