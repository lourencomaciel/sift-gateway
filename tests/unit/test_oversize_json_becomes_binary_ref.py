from __future__ import annotations

from sift_gateway.envelope.model import (
    BinaryRefContentPart,
    Envelope,
    JsonContentPart,
)
from sift_gateway.envelope.oversize import replace_oversized_json_parts
from sift_gateway.fs.blob_store import BlobStore


def test_oversized_json_part_becomes_binary_ref(tmp_path) -> None:
    store = BlobStore(tmp_path / "blobs" / "bin")
    envelope = Envelope(
        upstream_instance_id="up_1",
        upstream_prefix="github",
        tool="search_issues",
        status="ok",
        content=[JsonContentPart(value={"big": "x" * 1000})],
        meta={"warnings": []},
    )

    rewritten = replace_oversized_json_parts(
        envelope,
        max_json_part_parse_bytes=200,
        blob_store=store,
    )
    assert isinstance(rewritten.content[0], BinaryRefContentPart)
    assert rewritten.meta["warnings"]
    assert rewritten.meta["warnings"][0]["code"] == "oversized_json_part"
