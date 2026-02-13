from __future__ import annotations

from decimal import Decimal

from sift_mcp.canon.rfc8785 import canonical_bytes
from sift_mcp.request_identity import (
    RequestIdentity,
    compute_dedupe_hash,
    compute_request_identity,
)
from sift_mcp.util.hashing import sha256_hex

# ---- compute_request_identity ----


def test_request_identity_deterministic() -> None:
    ri1 = compute_request_identity(
        upstream_instance_id="inst_abc",
        prefix="github",
        tool_name="search_issues",
        forwarded_args={"query": "open bugs", "repo": "acme/app"},
    )
    ri2 = compute_request_identity(
        upstream_instance_id="inst_abc",
        prefix="github",
        tool_name="search_issues",
        forwarded_args={"query": "open bugs", "repo": "acme/app"},
    )
    assert ri1.request_key == ri2.request_key
    assert ri1.request_args_hash == ri2.request_args_hash
    assert len(ri1.request_key) == 64  # full sha256 hex


def test_different_args_different_request_key() -> None:
    ri1 = compute_request_identity(
        upstream_instance_id="inst_abc",
        prefix="github",
        tool_name="search_issues",
        forwarded_args={"query": "open bugs"},
    )
    ri2 = compute_request_identity(
        upstream_instance_id="inst_abc",
        prefix="github",
        tool_name="search_issues",
        forwarded_args={"query": "closed bugs"},
    )
    assert ri1.request_key != ri2.request_key
    assert ri1.request_args_hash != ri2.request_args_hash


def test_different_tool_different_request_key() -> None:
    ri1 = compute_request_identity(
        upstream_instance_id="inst_abc",
        prefix="github",
        tool_name="search_issues",
        forwarded_args={"query": "open bugs"},
    )
    ri2 = compute_request_identity(
        upstream_instance_id="inst_abc",
        prefix="github",
        tool_name="list_prs",
        forwarded_args={"query": "open bugs"},
    )
    assert ri1.request_key != ri2.request_key


def test_different_upstream_instance_different_key() -> None:
    ri1 = compute_request_identity(
        upstream_instance_id="inst_abc",
        prefix="github",
        tool_name="search",
        forwarded_args={"query": "test"},
    )
    ri2 = compute_request_identity(
        upstream_instance_id="inst_xyz",
        prefix="github",
        tool_name="search",
        forwarded_args={"query": "test"},
    )
    assert ri1.request_key != ri2.request_key


def test_request_args_prefix_capped() -> None:
    # Create args whose canonical form is longer than 200 chars
    long_value = "x" * 300
    ri = compute_request_identity(
        upstream_instance_id="inst_abc",
        prefix="github",
        tool_name="search_issues",
        forwarded_args={"data": long_value},
    )
    assert (
        len(ri.request_args_prefix) <= RequestIdentity.REQUEST_ARGS_PREFIX_CAP
    )


def test_request_args_prefix_short_args_not_truncated() -> None:
    ri = compute_request_identity(
        upstream_instance_id="inst_abc",
        prefix="github",
        tool_name="search",
        forwarded_args={"q": "hi"},
    )
    # Canonical form of {"q":"hi"} is short, should not be truncated
    assert ri.request_args_prefix == ri.canonical_args.decode("utf-8")


def test_canonical_args_stored_correctly() -> None:
    args = {"query": "test", "repo": "acme/app"}
    ri = compute_request_identity(
        upstream_instance_id="inst_abc",
        prefix="github",
        tool_name="search",
        forwarded_args=args,
    )
    assert ri.canonical_args == canonical_bytes(args)


def test_request_identity_preserves_metadata() -> None:
    ri = compute_request_identity(
        upstream_instance_id="inst_abc",
        prefix="github",
        tool_name="search",
        forwarded_args={"q": "test"},
    )
    assert ri.upstream_instance_id == "inst_abc"
    assert ri.prefix == "github"
    assert ri.tool_name == "search"


# ---- compute_dedupe_hash ----


def test_dedupe_hash_no_exclusions() -> None:
    args_bytes = canonical_bytes({"query": "test", "repo": "acme/app"})
    h = compute_dedupe_hash(args_bytes)
    assert h == sha256_hex(args_bytes)


def test_dedupe_hash_with_exclusion() -> None:
    args_bytes = canonical_bytes({"query": "test", "timestamp": "2025-01-01"})
    h_with = compute_dedupe_hash(args_bytes, exclusion_paths=["timestamp"])
    h_without = compute_dedupe_hash(args_bytes)
    assert h_with != h_without

    # Hash with exclusion should equal hash of args without the excluded key
    expected_bytes = canonical_bytes({"query": "test"})
    assert h_with == sha256_hex(expected_bytes)


def test_dedupe_hash_jsonpath_dollar_prefix() -> None:
    args_bytes = canonical_bytes({"query": "test", "ts": "now"})
    h1 = compute_dedupe_hash(args_bytes, exclusion_paths=["$.ts"])
    h2 = compute_dedupe_hash(args_bytes, exclusion_paths=["ts"])
    assert h1 == h2


def test_dedupe_hash_missing_exclusion_key_is_noop() -> None:
    args_bytes = canonical_bytes({"query": "test"})
    h1 = compute_dedupe_hash(args_bytes, exclusion_paths=["nonexistent"])
    h2 = compute_dedupe_hash(args_bytes)
    assert h1 == h2


# ---- Decimal safety in dedupe hash ----


def test_dedupe_hash_preserves_decimal_through_exclusion() -> None:
    """Dedupe hash with exclusions must preserve Decimal values (no float drift).

    canonical_args might contain decimal numbers. When dedupe_hash parses
    them via loads_decimal and re-canonicalizes, the result must be
    identical to canonicalizing the original data without the excluded key.
    """
    args = {"price": Decimal("19.99"), "timestamp": "2025-01-01"}
    args_bytes = canonical_bytes(args)

    h = compute_dedupe_hash(args_bytes, exclusion_paths=["timestamp"])

    # Expected: canonical hash of {"price": Decimal("19.99")} only
    expected = sha256_hex(canonical_bytes({"price": Decimal("19.99")}))
    assert h == expected


def test_dedupe_hash_with_decimal_no_exclusion_is_identity() -> None:
    """Without exclusions, dedupe hash is just sha256 of canonical bytes, even with Decimal."""
    args = {"amount": Decimal("100.50"), "query": "test"}
    args_bytes = canonical_bytes(args)
    h = compute_dedupe_hash(args_bytes)
    assert h == sha256_hex(args_bytes)


def test_request_identity_uses_canonical_bytes_for_args() -> None:
    """Verify that request identity computation uses RFC 8785 canonical_bytes for args."""
    args = {"b_key": "second", "a_key": "first"}
    ri = compute_request_identity(
        upstream_instance_id="inst_abc",
        prefix="github",
        tool_name="search",
        forwarded_args=args,
    )
    # Canonical bytes sort keys: a_key before b_key
    expected_canonical = canonical_bytes(args)
    assert ri.canonical_args == expected_canonical
    text = expected_canonical.decode("utf-8")
    assert text.index('"a_key"') < text.index('"b_key"')


def test_request_identity_with_decimal_args() -> None:
    """Decimal values in forwarded_args are handled correctly through canonicalization."""
    args = {"price": Decimal("42.00"), "name": "test"}
    ri = compute_request_identity(
        upstream_instance_id="inst_abc",
        prefix="github",
        tool_name="search",
        forwarded_args=args,
    )
    assert ri.canonical_args == canonical_bytes(args)
    assert (
        b"42" in ri.canonical_args
    )  # Canonical form of Decimal("42.00") is 42
