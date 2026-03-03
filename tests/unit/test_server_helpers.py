from __future__ import annotations

import base64

from sift_gateway.fs.blob_store import BlobStore
from sift_gateway.mcp.server_helpers import normalize_upstream_content


def test_normalize_upstream_content_converts_inline_image_block_to_blob_ref(
    tmp_path,
) -> None:
    blob_store = BlobStore(tmp_path / "blobs" / "bin")
    png_bytes = (
        b"\x89PNG\r\n\x1a\n"
        b"\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
        b"\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
    )
    content = [
        {
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": "image/png",
                "data": base64.b64encode(png_bytes).decode("ascii"),
            },
        }
    ]
    binary_refs = []

    normalized = normalize_upstream_content(
        content=content,
        structured_content=None,
        blob_store=blob_store,
        binary_refs_out=binary_refs,
    )

    assert len(normalized) == 1
    part = normalized[0]
    assert part["type"] == "image_ref"
    assert part["mime"] == "image/png"
    assert part["byte_count"] == len(png_bytes)
    assert part["uri"].startswith("sift://blob/bin_")
    assert len(binary_refs) == 1
    assert binary_refs[0].blob_id == part["blob_id"]


def test_normalize_upstream_content_converts_inline_video_block_to_blob_ref(
    tmp_path,
) -> None:
    blob_store = BlobStore(tmp_path / "blobs" / "bin")
    video_bytes = b"\x00\x00\x00\x18ftypmp42"
    content = [
        {
            "type": "video",
            "mimeType": "video/mp4",
            "data": base64.b64encode(video_bytes).decode("ascii"),
        }
    ]
    binary_refs = []

    normalized = normalize_upstream_content(
        content=content,
        structured_content=None,
        blob_store=blob_store,
        binary_refs_out=binary_refs,
    )

    assert len(normalized) == 1
    part = normalized[0]
    assert part["type"] == "binary_ref"
    assert part["mime"] == "video/mp4"
    assert part["byte_count"] == len(video_bytes)
    assert part["uri"].startswith("sift://blob/bin_")
    assert len(binary_refs) == 1
    assert binary_refs[0].binary_hash == part["binary_hash"]


def test_normalize_upstream_content_without_blob_store_falls_back_to_text() -> (
    None
):
    content = [
        {
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": "image/png",
                "data": base64.b64encode(b"abc").decode("ascii"),
            },
        }
    ]

    normalized = normalize_upstream_content(
        content=content,
        structured_content=None,
    )

    assert len(normalized) == 1
    assert normalized[0]["type"] == "text"
    assert "image" in normalized[0]["text"]
