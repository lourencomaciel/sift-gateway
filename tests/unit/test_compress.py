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


def test_zstd_compress_raises() -> None:
    with pytest.raises(ValueError, match="unsupported"):
        compress_bytes(b"data", "zstd")


def test_zstd_decompress_legacy_payload() -> None:
    zstandard = pytest.importorskip("zstandard")
    raw = b"hello" * 100
    compressed = zstandard.ZstdCompressor().compress(raw)
    assert decompress_bytes(compressed, "zstd") == raw


def test_zstd_decompress_missing_package(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import builtins

    real_import = builtins.__import__

    def _block_zstandard(name: str, *args: object, **kwargs: object) -> object:
        if name == "zstandard":
            raise ModuleNotFoundError(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _block_zstandard)
    with pytest.raises(ValueError, match="zstandard"):
        decompress_bytes(b"data", "zstd")
