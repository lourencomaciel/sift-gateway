from __future__ import annotations

import pytest

from sift_gateway.envelope.model import BinaryRefContentPart
from sift_gateway.envelope.normalize import (
    normalize_envelope,
    strip_reserved_args,
)


def test_strip_reserved_args_keeps_non_reserved_gateway_word() -> None:
    forwarded = strip_reserved_args(
        {
            "_gateway_context": {"session_id": "s1"},
            "_gateway_parent_artifact_id": "art_1",
            "_gateway_custom": 1,
            "gateway_url": "keep-me",
            "query": "open issues",
        }
    )
    assert forwarded == {"gateway_url": "keep-me", "query": "open issues"}


def test_normalize_image_ref_aliases_to_binary_ref() -> None:
    envelope = normalize_envelope(
        upstream_instance_id="up_1",
        upstream_prefix="github",
        tool="search_issues",
        content=[
            {
                "type": "image_ref",
                "blob_id": "bin_1",
                "binary_hash": "abc",
                "mime": "image/png",
                "byte_count": 42,
            }
        ],
    )
    assert isinstance(envelope.content[0], BinaryRefContentPart)


def test_normalize_infers_error_when_error_object_is_present() -> None:
    envelope = normalize_envelope(
        upstream_instance_id="up_1",
        upstream_prefix="github",
        tool="search_issues",
        error={},
    )
    assert envelope.status == "error"
    assert envelope.error is not None
    assert envelope.error.code == "UPSTREAM_ERROR"


def test_normalize_rejects_inline_binary_bytes() -> None:
    with pytest.raises(ValueError, match="not allowed inline"):
        normalize_envelope(
            upstream_instance_id="up_1",
            upstream_prefix="github",
            tool="search_issues",
            content=[
                {
                    "type": "binary_ref",
                    "blob_id": "bin_1",
                    "binary_hash": "abc",
                    "mime": "application/octet-stream",
                    "byte_count": 2,
                    "bytes": "AAAA",
                }
            ],
        )


def test_normalize_rejects_binary_ref_missing_identifiers() -> None:
    with pytest.raises(ValueError, match="blob_id"):
        normalize_envelope(
            upstream_instance_id="up_1",
            upstream_prefix="github",
            tool="search_issues",
            content=[
                {
                    "type": "binary_ref",
                    "byte_count": 1,
                    "mime": "application/octet-stream",
                }
            ],
        )
