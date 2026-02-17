"""Compression helpers for canonical envelope bytes."""

from __future__ import annotations

from dataclasses import dataclass
import gzip

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
        return CompressedBytes(
            encoding="none", data=data, uncompressed_len=len(data)
        )
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


def decompress_bytes(
    data: bytes,
    encoding: str,
    *,
    max_output_size: int = 0,
) -> bytes:
    """Decompress bytes with configured encoding.

    Args:
        data: Compressed byte payload.
        encoding: Compression encoding (``"none"``, ``"gzip"``,
            or ``"zstd"``).
        max_output_size: Upper bound on decompressed output in
            bytes.  ``0`` means unlimited (default for backward
            compatibility).

    Returns:
        The decompressed byte string.

    Raises:
        ValueError: If *encoding* is unsupported or the
            decompressed output exceeds *max_output_size*.
    """
    if encoding == "none":
        return data
    if encoding == "gzip":
        out = gzip.decompress(data)
    elif encoding == "zstd":
        out = zstd.ZstdDecompressor().decompress(data)
    else:
        msg = f"unsupported encoding: {encoding}"
        raise ValueError(msg)

    if max_output_size and len(out) > max_output_size:
        msg = (
            f"decompressed size {len(out)} exceeds"
            f" limit {max_output_size}"
        )
        raise ValueError(msg)
    return out
