import pytest

from mcp_artifact_gateway.util.hashing import (
    advisory_lock_keys,
    request_key,
    sha256_trunc,
)


def test_request_key_deterministic() -> None:
    key1 = request_key("up", "pref", "tool", b"{}")
    key2 = request_key("up", "pref", "tool", b"{}")
    assert key1 == key2
    assert len(key1) == 64


def test_sha256_trunc_bounds() -> None:
    with pytest.raises(ValueError):
        sha256_trunc(b"x", 0)
    with pytest.raises(ValueError):
        sha256_trunc(b"x", 65)


def test_advisory_lock_keys() -> None:
    k1, k2 = advisory_lock_keys("a" * 64)
    assert isinstance(k1, int)
    assert isinstance(k2, int)
