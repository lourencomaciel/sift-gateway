from __future__ import annotations

from sift_mcp.canon.compress import compress_bytes, decompress_bytes


def test_gzip_roundtrip() -> None:
    data = b"hello" * 100
    compressed = compress_bytes(data, "gzip")
    assert compressed.encoding == "gzip"
    assert compressed.uncompressed_len == len(data)
    assert decompress_bytes(compressed.data, compressed.encoding) == data


def test_zstd_roundtrip() -> None:
    data = b"world" * 100
    compressed = compress_bytes(data, "zstd")
    assert compressed.encoding == "zstd"
    assert decompress_bytes(compressed.data, compressed.encoding) == data


def test_none_roundtrip() -> None:
    data = b"raw"
    compressed = compress_bytes(data, "none")
    assert compressed.data == data
    assert decompress_bytes(compressed.data, compressed.encoding) == data
