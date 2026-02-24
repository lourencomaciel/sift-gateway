"""Unit tests for Tier 2 tool bridge."""

from __future__ import annotations

from benchmarks.tier2.tool_bridge import (
    classify_tool_call,
    inject_gateway_context,
    mcp_tools_to_definitions,
)


class TestMcpToolsToDefinitions:
    def test_strips_gateway_context_from_properties(self) -> None:
        mcp_tools = [
            {
                "name": "bench_get_earthquakes",
                "description": "Get earthquake data",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "_gateway_context": {
                            "type": "object",
                        },
                        "limit": {"type": "integer"},
                    },
                    "required": ["_gateway_context"],
                },
            },
        ]
        defs = mcp_tools_to_definitions(mcp_tools)
        assert len(defs) == 1
        assert defs[0].name == "bench_get_earthquakes"
        assert "_gateway_context" not in defs[0].input_schema["properties"]
        assert "_gateway_context" not in defs[0].input_schema.get(
            "required", []
        )

    def test_preserves_other_properties(self) -> None:
        mcp_tools = [
            {
                "name": "artifact",
                "description": "Artifact tool",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "action": {"type": "string"},
                        "artifact_id": {"type": "string"},
                    },
                    "required": ["action"],
                },
            },
        ]
        defs = mcp_tools_to_definitions(mcp_tools)
        assert "action" in defs[0].input_schema["properties"]
        assert "artifact_id" in defs[0].input_schema["properties"]
        assert defs[0].input_schema["required"] == ["action"]

    def test_does_not_mutate_original(self) -> None:
        original_schema = {
            "type": "object",
            "properties": {
                "_gateway_context": {"type": "object"},
                "limit": {"type": "integer"},
            },
            "required": ["_gateway_context", "limit"],
        }
        mcp_tools = [
            {
                "name": "tool",
                "description": "test",
                "input_schema": original_schema,
            },
        ]
        mcp_tools_to_definitions(mcp_tools)
        assert "_gateway_context" in original_schema["properties"]
        assert "_gateway_context" in original_schema["required"]

    def test_empty_tool_list(self) -> None:
        assert mcp_tools_to_definitions([]) == []

    def test_missing_optional_fields(self) -> None:
        mcp_tools = [
            {
                "name": "simple_tool",
                "description": "",
                "input_schema": {},
            },
        ]
        defs = mcp_tools_to_definitions(mcp_tools)
        assert len(defs) == 1
        assert defs[0].name == "simple_tool"


class TestInjectGatewayContext:
    def test_adds_context(self) -> None:
        args = {"action": "query", "artifact_id": "art_123"}
        result = inject_gateway_context(args, session_id="test_session")
        assert result["_gateway_context"] == {"session_id": "test_session"}
        assert result["action"] == "query"

    def test_does_not_mutate_original(self) -> None:
        args = {"action": "query"}
        inject_gateway_context(args, session_id="s1")
        assert "_gateway_context" not in args

    def test_overwrites_existing_context(self) -> None:
        args = {
            "_gateway_context": {"session_id": "old"},
            "action": "query",
        }
        result = inject_gateway_context(args, session_id="new")
        assert result["_gateway_context"]["session_id"] == "new"


class TestClassifyToolCall:
    def test_mirrored_tool(self) -> None:
        assert classify_tool_call("bench_get_earthquakes", {}) == "mirrored"

    def test_code_query(self) -> None:
        assert (
            classify_tool_call(
                "artifact",
                {"action": "query", "query_kind": "code"},
            )
            == "code_query"
        )

    def test_next_page(self) -> None:
        assert (
            classify_tool_call(
                "artifact",
                {"action": "next_page", "artifact_id": "art_1"},
            )
            == "next_page"
        )

    def test_describe(self) -> None:
        assert (
            classify_tool_call("artifact", {"action": "describe"}) == "describe"
        )

    def test_gateway_status(self) -> None:
        assert classify_tool_call("gateway_status", {}) == "status"

    def test_artifact_unknown_action(self) -> None:
        assert classify_tool_call("artifact", {"action": "unknown"}) == "other"

    def test_unknown_tool(self) -> None:
        assert classify_tool_call("sometool", {}) == "other"
