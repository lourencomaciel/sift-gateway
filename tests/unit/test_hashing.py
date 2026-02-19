from __future__ import annotations

import hashlib

import pytest

from sift_gateway.util.hashing import (
    binary_hash,
    blob_id,
    map_budget_fingerprint,
    payload_hash_full,
    request_key,
    sample_set_hash,
    sha256_hex,
    sha256_trunc,
    upstream_instance_id,
)


def test_sha256_hex_is_stable() -> None:
    assert (
        sha256_hex(b"abc")
        == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    )


def test_sha256_trunc_respects_length() -> None:
    assert sha256_trunc(b"abc", 8) == "ba7816bf"


def test_sha256_trunc_requires_positive_length() -> None:
    with pytest.raises(ValueError, match="positive"):
        sha256_trunc(b"abc", 0)


# ---- binary_hash ----


def test_binary_hash_returns_sha256_hex() -> None:
    data = b"hello world"
    expected = hashlib.sha256(data).hexdigest()
    assert binary_hash(data) == expected


def test_binary_hash_empty_bytes() -> None:
    expected = hashlib.sha256(b"").hexdigest()
    assert binary_hash(b"") == expected


# ---- blob_id ----


def test_blob_id_prefix_and_truncation() -> None:
    hex_str = "a" * 64
    result = blob_id(hex_str)
    assert result == "bin_" + "a" * 32
    assert result.startswith("bin_")
    assert len(result) == 36


def test_blob_id_short_hash() -> None:
    result = blob_id("abc")
    assert result == "bin_abc"


# ---- payload_hash_full ----


def test_payload_hash_full_is_sha256() -> None:
    data = b'{"type":"mcp_envelope"}'
    expected = hashlib.sha256(data).hexdigest()
    assert payload_hash_full(data) == expected
    assert len(payload_hash_full(data)) == 64


# ---- upstream_instance_id ----


def test_upstream_instance_id_is_32_chars() -> None:
    data = b"prefix:github|transport:stdio|command:/usr/bin/gh"
    result = upstream_instance_id(data)
    assert len(result) == 32
    full_hex = hashlib.sha256(data).hexdigest()
    assert result == full_hex[:32]


# ---- request_key ----


def test_request_key_deterministic() -> None:
    result1 = request_key("uid123", "github", "list_repos", b'{"org":"test"}')
    result2 = request_key("uid123", "github", "list_repos", b'{"org":"test"}')
    assert result1 == result2
    assert len(result1) == 64


def test_request_key_differs_with_different_args() -> None:
    r1 = request_key("uid", "prefix", "tool", b'{"a":1}')
    r2 = request_key("uid", "prefix", "tool", b'{"a":2}')
    assert r1 != r2


def test_request_key_differs_with_different_tool() -> None:
    r1 = request_key("uid", "prefix", "tool_a", b"{}")
    r2 = request_key("uid", "prefix", "tool_b", b"{}")
    assert r1 != r2


# ---- map_budget_fingerprint ----


def test_map_budget_fingerprint_deterministic() -> None:
    kwargs = {
        "mapper_version": "mapper_v1",
        "traversal_contract_version": "traversal_v1",
        "map_backend_id": "backend_1",
        "prng_version": "prng_xoshiro256ss_v1",
        "budgets": {"max_bytes": 1000, "max_depth": 10},
    }
    fp1 = map_budget_fingerprint(**kwargs)
    fp2 = map_budget_fingerprint(**kwargs)
    assert fp1 == fp2
    assert len(fp1) == 32


def test_map_budget_fingerprint_changes_with_budgets() -> None:
    base = {
        "mapper_version": "mapper_v1",
        "traversal_contract_version": "traversal_v1",
        "map_backend_id": "backend_1",
        "prng_version": "prng_xoshiro256ss_v1",
    }
    fp1 = map_budget_fingerprint(**base, budgets={"max_bytes": 1000})
    fp2 = map_budget_fingerprint(**base, budgets={"max_bytes": 2000})
    assert fp1 != fp2


def test_map_budget_fingerprint_canonical_json_determinism() -> None:
    """Verify that key insertion order does not affect the fingerprint."""
    base = {
        "mapper_version": "mapper_v1",
        "traversal_contract_version": "traversal_v1",
        "map_backend_id": "backend_1",
        "prng_version": "prng_xoshiro256ss_v1",
    }
    budgets_a = {"z_key": 1, "a_key": 2}
    budgets_b = {"a_key": 2, "z_key": 1}
    fp1 = map_budget_fingerprint(**base, budgets=budgets_a)
    fp2 = map_budget_fingerprint(**base, budgets=budgets_b)
    assert fp1 == fp2


# ---- sample_set_hash ----


def test_sample_set_hash_deterministic() -> None:
    kwargs = {
        "root_path": "$.items",
        "sample_indices": [0, 5, 10],
        "map_budget_fingerprint": "abc123",
        "mapper_version": "mapper_v1",
    }
    h1 = sample_set_hash(**kwargs)
    h2 = sample_set_hash(**kwargs)
    assert h1 == h2
    assert len(h1) == 32


def test_sample_set_hash_changes_with_indices() -> None:
    base = {
        "root_path": "$.items",
        "map_budget_fingerprint": "abc123",
        "mapper_version": "mapper_v1",
    }
    h1 = sample_set_hash(sample_indices=[0, 5, 10], **base)
    h2 = sample_set_hash(sample_indices=[0, 5, 11], **base)
    assert h1 != h2


def test_sample_set_hash_changes_with_root_path() -> None:
    base = {
        "sample_indices": [1, 2, 3],
        "map_budget_fingerprint": "abc123",
        "mapper_version": "mapper_v1",
    }
    h1 = sample_set_hash(root_path="$.items", **base)
    h2 = sample_set_hash(root_path="$.records", **base)
    assert h1 != h2
