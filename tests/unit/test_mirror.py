from __future__ import annotations

import pytest

from sift_mcp.config.settings import UpstreamConfig
from sift_mcp.mcp.mirror import (
    build_mirrored_tools,
    extract_gateway_context,
    strip_reserved_gateway_args,
    validate_against_schema,
)
from sift_mcp.mcp.upstream import (
    UpstreamInstance,
    UpstreamToolSchema,
)


def _make_upstream(
    prefix: str,
    tool_names: list[str],
) -> UpstreamInstance:
    config = UpstreamConfig(
        prefix=prefix,
        transport="stdio",
        command="/usr/bin/test-mcp",
    )
    tools = [
        UpstreamToolSchema(
            name=name,
            description=f"Tool {name}",
            input_schema={"type": "object", "properties": {}},
            schema_hash=f"hash_{name}",
        )
        for name in tool_names
    ]
    return UpstreamInstance(
        config=config,
        instance_id=f"inst_{prefix}",
        tools=tools,
    )


# ---- build_mirrored_tools ----


def test_build_mirrored_tools_qualified_names() -> None:
    upstream = _make_upstream("github", ["search_issues", "create_pr"])
    result = build_mirrored_tools([upstream])
    assert "github.search_issues" in result
    assert "github.create_pr" in result
    assert result["github.search_issues"].original_name == "search_issues"
    assert result["github.search_issues"].prefix == "github"


def test_build_mirrored_tools_multiple_upstreams() -> None:
    gh = _make_upstream("github", ["search"])
    jira = _make_upstream("jira", ["list_tickets"])
    result = build_mirrored_tools([gh, jira])
    assert len(result) == 2
    assert "github.search" in result
    assert "jira.list_tickets" in result


def test_build_mirrored_tools_duplicate_raises() -> None:
    up1 = _make_upstream("github", ["search"])
    up2 = _make_upstream("github", ["search"])
    with pytest.raises(ValueError, match=r"(?i)duplicate"):
        build_mirrored_tools([up1, up2])


def test_build_mirrored_tools_empty() -> None:
    upstream = _make_upstream("github", [])
    result = build_mirrored_tools([upstream])
    assert result == {}


# ---- strip_reserved_gateway_args ----


def test_strip_removes_gateway_context() -> None:
    result = strip_reserved_gateway_args(
        {
            "_gateway_context": {"session": "s1"},
            "query": "test",
        }
    )
    assert result == {"query": "test"}


def test_strip_removes_gateway_parent_artifact_id() -> None:
    result = strip_reserved_gateway_args(
        {
            "_gateway_parent_artifact_id": "art_1",
            "query": "test",
        }
    )
    assert result == {"query": "test"}


def test_strip_removes_gateway_chain_seq() -> None:
    result = strip_reserved_gateway_args(
        {
            "_gateway_chain_seq": 3,
            "query": "test",
        }
    )
    assert result == {"query": "test"}


def test_strip_removes_any_gateway_prefix_key() -> None:
    result = strip_reserved_gateway_args(
        {
            "_gateway_anything": "val",
            "_gateway_custom_field": 42,
            "query": "test",
        }
    )
    assert result == {"query": "test"}


def test_strip_keeps_gateway_url() -> None:
    """gateway_url does NOT start with _gateway_ prefix - must be kept."""
    result = strip_reserved_gateway_args(
        {
            "gateway_url": "https://example.com",
            "query": "test",
        }
    )
    assert result == {"gateway_url": "https://example.com", "query": "test"}


def test_strip_keeps_gatewa_without_y() -> None:
    """_gatewa (missing final 'y_') does NOT match prefix - must be kept."""
    result = strip_reserved_gateway_args(
        {
            "_gatewa": "val",
            "query": "test",
        }
    )
    assert result == {"_gatewa": "val", "query": "test"}


def test_strip_combined() -> None:
    """Full combined test matching spec."""
    forwarded = strip_reserved_gateway_args(
        {
            "_gateway_context": {"session_id": "s1"},
            "_gateway_parent_artifact_id": "art_1",
            "_gateway_custom": 1,
            "gateway_url": "keep-me",
            "_gatewa": "keep-me-too",
            "query": "open issues",
        }
    )
    assert forwarded == {
        "gateway_url": "keep-me",
        "_gatewa": "keep-me-too",
        "query": "open issues",
    }


# ---- extract_gateway_context ----


def test_extract_gateway_context_present() -> None:
    ctx = extract_gateway_context(
        {"_gateway_context": {"session": "s1"}, "q": "test"}
    )
    assert ctx == {"session": "s1"}


def test_extract_gateway_context_missing() -> None:
    ctx = extract_gateway_context({"q": "test"})
    assert ctx is None


def test_extract_gateway_context_non_dict_ignored() -> None:
    ctx = extract_gateway_context({"_gateway_context": "not a dict"})
    assert ctx is None


# ---- validate_against_schema ----


def test_validate_missing_required() -> None:
    schema = {
        "type": "object",
        "properties": {"repo": {"type": "string"}, "query": {"type": "string"}},
        "required": ["repo", "query"],
    }
    warnings = validate_against_schema({"repo": "test"}, schema)
    assert any("missing required argument: query" in w for w in warnings)


def test_validate_unknown_arg_rejected_when_additional_false() -> None:
    schema = {
        "type": "object",
        "properties": {"repo": {"type": "string"}},
        "additionalProperties": False,
    }
    warnings = validate_against_schema({"repo": "test", "extra": 1}, schema)
    assert any("unknown argument: extra" in w for w in warnings)


def test_validate_unknown_arg_allowed_by_default() -> None:
    schema = {
        "type": "object",
        "properties": {"repo": {"type": "string"}},
    }
    warnings = validate_against_schema({"repo": "test", "extra": 1}, schema)
    assert warnings == []


def test_validate_no_warnings_when_valid() -> None:
    schema = {
        "type": "object",
        "properties": {"repo": {"type": "string"}},
        "required": ["repo"],
    }
    warnings = validate_against_schema({"repo": "test"}, schema)
    assert warnings == []


def test_validate_rejects_type_mismatch() -> None:
    schema = {
        "type": "object",
        "properties": {"after": {"type": "string"}},
    }
    warnings = validate_against_schema({"after": 100}, schema)
    assert any("argument after must match type: string" in w for w in warnings)


def test_validate_accepts_union_types() -> None:
    schema = {
        "type": "object",
        "properties": {"value": {"type": ["string", "integer"]}},
    }
    warnings_int = validate_against_schema({"value": 7}, schema)
    warnings_str = validate_against_schema({"value": "7"}, schema)
    assert warnings_int == []
    assert warnings_str == []
