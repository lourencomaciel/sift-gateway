"""Compression utilities for canonical envelope bytes.

Supports zstd (via the ``zstandard`` library), gzip, and passthrough ("none").
The encoding names align with :class:`~mcp_artifact_gateway.config.settings.CanonicalEncoding`.
"""

from __future__ import annotations

import gzip as _gzip

import zstandard as _zstd

# ---------------------------------------------------------------------------
# Module-level compressor / decompressor (thread-safe, reusable)
# ---------------------------------------------------------------------------
_ZSTD_COMPRESSOR = _zstd.ZstdCompressor(level=3)
_ZSTD_DECOMPRESSOR = _zstd.ZstdDecompressor()

_VALID_ENCODINGS = frozenset({"zstd", "gzip", "none"})


def compress(data: bytes, encoding: str) -> tuple[bytes, int]:
    """Compress *data* using the specified encoding.

    Args:
        data: Raw bytes to compress.
        encoding: One of ``"zstd"``, ``"gzip"``, or ``"none"``.

    Returns:
        A 2-tuple of ``(compressed_bytes, uncompressed_length)``.

    Raises:
        ValueError: If *encoding* is not a recognized value.
    """
    if encoding not in _VALID_ENCODINGS:
        raise ValueError(
            f"Unknown compression encoding {encoding!r}. "
            f"Must be one of: {', '.join(sorted(_VALID_ENCODINGS))}"
        )

    uncompressed_len = len(data)

    if encoding == "zstd":
        compressed = _ZSTD_COMPRESSOR.compress(data)
        return compressed, uncompressed_len

    if encoding == "gzip":
        compressed = _gzip.compress(data)
        return compressed, uncompressed_len

    # encoding == "none"
    return data, uncompressed_len


def decompress(data: bytes, encoding: str, expected_len: int | None = None) -> bytes:
    """Decompress *data* using the specified encoding.

    Args:
        data: Compressed (or raw) bytes.
        encoding: One of ``"zstd"``, ``"gzip"``, or ``"none"``.
        expected_len: If provided, the decompressed result's length is verified
            to match this value exactly.

    Returns:
        The decompressed bytes.

    Raises:
        ValueError: If *encoding* is unrecognized, or if *expected_len* is
            provided and the decompressed length does not match.
    """
    if encoding not in _VALID_ENCODINGS:
        raise ValueError(
            f"Unknown compression encoding {encoding!r}. "
            f"Must be one of: {', '.join(sorted(_VALID_ENCODINGS))}"
        )

    if encoding == "zstd":
        result = _ZSTD_DECOMPRESSOR.decompress(data)
    elif encoding == "gzip":
        result = _gzip.decompress(data)
    else:
        # encoding == "none"
        result = data

    if expected_len is not None and len(result) != expected_len:
        raise ValueError(
            f"Decompressed length mismatch: expected {expected_len} bytes, "
            f"got {len(result)} bytes (encoding={encoding!r})"
        )

    return result
