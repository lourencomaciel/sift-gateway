"""Hashing helpers used across gateway identity and integrity paths."""

from __future__ import annotations

import hashlib


def sha256_hex(data: bytes) -> str:
    """Return SHA-256 hex digest."""
    return hashlib.sha256(data).hexdigest()


def sha256_trunc(data: bytes, chars: int) -> str:
    """Return first `chars` chars of SHA-256 hex digest."""
    if chars <= 0:
        msg = "chars must be positive"
        raise ValueError(msg)
    return sha256_hex(data)[:chars]

