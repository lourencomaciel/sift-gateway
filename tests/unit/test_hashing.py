from __future__ import annotations

from mcp_artifact_gateway.util.hashing import sha256_hex, sha256_trunc


def test_sha256_hex_is_stable() -> None:
    assert sha256_hex(b"abc") == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"


def test_sha256_trunc_respects_length() -> None:
    assert sha256_trunc(b"abc", 8) == "ba7816bf"


def test_sha256_trunc_requires_positive_length() -> None:
    try:
        sha256_trunc(b"abc", 0)
    except ValueError as exc:
        assert "positive" in str(exc)
    else:
        raise AssertionError("expected ValueError")

