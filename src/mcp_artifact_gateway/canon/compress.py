"""Compression helpers for canonical envelope bytes."""

from __future__ import annotations

import gzip
from dataclasses import dataclass

import zstandard as zstd


@dataclass(frozen=True)
class CompressedBytes:
    """Compressed payload plus metadata."""

    encoding: str
    data: bytes
    uncompressed_len: int


def compress_bytes(data: bytes, encoding: str) -> CompressedBytes:
    """Compress bytes with configured encoding."""
    if encoding == "none":
        return CompressedBytes(encoding="none", data=data, uncompressed_len=len(data))
    if encoding == "gzip":
        return CompressedBytes(
            encoding="gzip",
            data=gzip.compress(data),
            uncompressed_len=len(data),
        )
    if encoding == "zstd":
        compressor = zstd.ZstdCompressor(level=3)
        return CompressedBytes(
            encoding="zstd",
            data=compressor.compress(data),
            uncompressed_len=len(data),
        )

    msg = f"unsupported encoding: {encoding}"
    raise ValueError(msg)


def decompress_bytes(data: bytes, encoding: str) -> bytes:
    """Decompress bytes with configured encoding."""
    if encoding == "none":
        return data
    if encoding == "gzip":
        return gzip.decompress(data)
    if encoding == "zstd":
        decompressor = zstd.ZstdDecompressor()
        return decompressor.decompress(data)

    msg = f"unsupported encoding: {encoding}"
    raise ValueError(msg)

