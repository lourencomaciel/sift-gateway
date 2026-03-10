"""Tests for tool inspection helpers."""

from __future__ import annotations

from sift_gateway.tools.tool_inspect import (
    build_tool_inspect_response,
    compact_tool_description_for_list,
    trim_description_for_inspect,
)


def test_compact_tool_description_for_list_keeps_short_text() -> None:
    description, compacted = compact_tool_description_for_list(
        "Short description.",
        inspect_tool_name="gateway_inspect_tool",
        safe_tool_name="demo_echo",
        max_chars=80,
    )

    assert description == "Short description."
    assert compacted is False


def test_compact_tool_description_for_list_truncates_and_adds_hint() -> None:
    description, compacted = compact_tool_description_for_list(
        " ".join(f"field_{idx}" for idx in range(120)),
        inspect_tool_name="gateway_inspect_tool",
        safe_tool_name="demo_echo",
        max_chars=120,
    )

    assert compacted is True
    assert '`tool_name="demo_echo"`' in description
    assert "gateway_inspect_tool" in description
    assert len(description) <= 120


def test_trim_description_for_inspect_respects_limit() -> None:
    trimmed, truncated = trim_description_for_inspect(
        "Alpha beta gamma delta epsilon",
        max_chars=14,
    )

    assert truncated is True
    assert trimmed.endswith("...")
    assert len(trimmed) <= 14


def test_build_tool_inspect_response_includes_metadata() -> None:
    payload = build_tool_inspect_response(
        safe_name="demo_echo",
        qualified_name="demo.echo",
        source_kind="mirrored",
        description="Full description",
        tools_list_description="Short description",
        description_compacted_in_tools_list=True,
        input_schema={"type": "object"},
        max_description_chars=None,
        metadata={"schema_hash": "schema_1"},
    )

    assert payload["type"] == "gateway_tool_inspect"
    assert payload["name"] == "demo_echo"
    assert payload["metadata"]["description_compacted_in_tools_list"] is True
    assert payload["metadata"]["schema_hash"] == "schema_1"
    assert payload["input_schema"] == {"type": "object"}
