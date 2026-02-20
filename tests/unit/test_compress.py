from __future__ import annotations

import pytest

from sift_gateway.canon.compress import compress_bytes, decompress_bytes


def test_gzip_roundtrip() -> None:
    data = b"hello" * 100
    compressed = compress_bytes(data, "gzip")
    assert compressed.encoding == "gzip"
    assert compressed.uncompressed_len == len(data)
    assert decompress_bytes(compressed.data, compressed.encoding) == data


def test_none_roundtrip() -> None:
    data = b"raw"
    compressed = compress_bytes(data, "none")
    assert compressed.data == data
    assert decompress_bytes(compressed.data, compressed.encoding) == data


def test_unsupported_compress_encoding_raises() -> None:
    with pytest.raises(ValueError, match="unsupported"):
        compress_bytes(b"data", "brotli")


def test_unsupported_decompress_encoding_raises() -> None:
    with pytest.raises(ValueError, match="unsupported"):
        decompress_bytes(b"data", "zstd")
