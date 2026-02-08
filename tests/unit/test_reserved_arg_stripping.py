"""Tests for reserved gateway argument stripping."""

from __future__ import annotations

from mcp_artifact_gateway.envelope.normalize import strip_reserved_args


def test_strips_gateway_context() -> None:
    args = {"_gateway_context": "abc", "query": "hello"}
    result = strip_reserved_args(args)
    assert "_gateway_context" not in result
    assert result == {"query": "hello"}


def test_strips_gateway_parent_artifact_id() -> None:
    args = {"_gateway_parent_artifact_id": "art_123", "limit": 10}
    result = strip_reserved_args(args)
    assert "_gateway_parent_artifact_id" not in result
    assert result == {"limit": 10}


def test_strips_gateway_chain_seq() -> None:
    args = {"_gateway_chain_seq": 5, "foo": "bar"}
    result = strip_reserved_args(args)
    assert "_gateway_chain_seq" not in result
    assert result == {"foo": "bar"}


def test_strips_all_reserved_exact_keys_together() -> None:
    args = {
        "_gateway_context": "ctx",
        "_gateway_parent_artifact_id": "art_1",
        "_gateway_chain_seq": 0,
        "real_arg": "keep",
    }
    result = strip_reserved_args(args)
    assert result == {"real_arg": "keep"}


def test_strips_any_key_with_gateway_prefix() -> None:
    """Any key starting with '_gateway_' should be removed."""
    args = {
        "_gateway_custom_key": "value",
        "_gateway_foo": 42,
        "_gateway_": "empty suffix",
        "normal": "kept",
    }
    result = strip_reserved_args(args)
    assert result == {"normal": "kept"}


def test_does_not_strip_gateway_url() -> None:
    """'gateway_url' does not start with '_gateway_' prefix."""
    args = {"gateway_url": "https://example.com", "data": 1}
    result = strip_reserved_args(args)
    assert result == {"gateway_url": "https://example.com", "data": 1}


def test_does_not_strip_gateway_without_underscore_prefix() -> None:
    """'gateway' alone is not reserved."""
    args = {"gateway": "value", "other": "kept"}
    result = strip_reserved_args(args)
    assert result == {"gateway": "value", "other": "kept"}


def test_does_not_strip_gatewa_partial_prefix() -> None:
    """'_gatewa' is not a full match of '_gateway_' prefix."""
    args = {"_gatewa": "value", "keep": True}
    result = strip_reserved_args(args)
    assert result == {"_gatewa": "value", "keep": True}


def test_does_not_strip_gateway_without_trailing_underscore() -> None:
    """'_gateway' (no trailing underscore) is not the reserved prefix."""
    args = {"_gateway": "value"}
    result = strip_reserved_args(args)
    assert result == {"_gateway": "value"}


def test_empty_args_returns_empty() -> None:
    assert strip_reserved_args({}) == {}


def test_all_reserved_returns_empty() -> None:
    args = {
        "_gateway_context": "a",
        "_gateway_parent_artifact_id": "b",
        "_gateway_chain_seq": 1,
    }
    result = strip_reserved_args(args)
    assert result == {}
