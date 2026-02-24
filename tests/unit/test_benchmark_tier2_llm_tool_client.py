"""Unit tests for Tier 2 tool-use LLM client."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from benchmarks.tier1.llm_client import LLMAPIError
from benchmarks.tier2.llm_tool_client import (
    TextBlock,
    ToolDefinition,
    ToolUseBlock,
    ToolUseResponse,
    _parse_content_blocks,
    _tools_to_api_format,
    call_llm_with_tools,
)
import pytest


class TestToolsToApiFormat:
    def test_converts_definitions(self) -> None:
        tools = [
            ToolDefinition(
                name="get_data",
                description="Get data",
                input_schema={
                    "type": "object",
                    "properties": {},
                },
            ),
        ]
        result = _tools_to_api_format(tools)
        assert len(result) == 1
        assert result[0]["name"] == "get_data"
        assert result[0]["description"] == "Get data"
        assert result[0]["input_schema"] == {
            "type": "object",
            "properties": {},
        }

    def test_empty_list(self) -> None:
        assert _tools_to_api_format([]) == []


class TestParseContentBlocks:
    def test_parses_tool_use(self) -> None:
        content = [
            {
                "type": "tool_use",
                "id": "tu_1",
                "name": "get_data",
                "input": {"limit": 10},
            },
        ]
        blocks = _parse_content_blocks(content)
        assert len(blocks) == 1
        assert isinstance(blocks[0], ToolUseBlock)
        assert blocks[0].id == "tu_1"
        assert blocks[0].name == "get_data"
        assert blocks[0].input == {"limit": 10}

    def test_parses_text(self) -> None:
        content = [
            {"type": "text", "text": "The answer is 42"},
        ]
        blocks = _parse_content_blocks(content)
        assert len(blocks) == 1
        assert isinstance(blocks[0], TextBlock)
        assert blocks[0].text == "The answer is 42"

    def test_mixed_blocks(self) -> None:
        content = [
            {"type": "text", "text": "Let me check"},
            {
                "type": "tool_use",
                "id": "tu_1",
                "name": "get_data",
                "input": {},
            },
        ]
        blocks = _parse_content_blocks(content)
        assert len(blocks) == 2
        assert isinstance(blocks[0], TextBlock)
        assert isinstance(blocks[1], ToolUseBlock)

    def test_skips_empty_text(self) -> None:
        content = [{"type": "text", "text": ""}]
        blocks = _parse_content_blocks(content)
        assert len(blocks) == 0

    def test_skips_unknown_types(self) -> None:
        content = [{"type": "image", "source": "..."}]
        blocks = _parse_content_blocks(content)
        assert len(blocks) == 0


class TestCallLlmWithTools:
    def test_rejects_non_anthropic_model(self) -> None:
        with pytest.raises(LLMAPIError, match="only supports"):
            call_llm_with_tools(
                model="gpt-4o",
                system_prompt="test",
                messages=[],
                tools=[],
                api_key="key",
            )

    def test_successful_call(self) -> None:
        response_body = {
            "content": [
                {
                    "type": "tool_use",
                    "id": "tu_1",
                    "name": "bench_get_earthquakes",
                    "input": {},
                },
            ],
            "stop_reason": "tool_use",
            "usage": {
                "input_tokens": 100,
                "output_tokens": 50,
            },
            "model": "claude-sonnet-4-6",
        }
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(response_body).encode("utf-8")
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)

        with patch(
            "benchmarks.tier2.llm_tool_client.urllib.request.urlopen",
            return_value=mock_resp,
        ):
            result = call_llm_with_tools(
                model="claude-sonnet-4-6",
                system_prompt="test",
                messages=[{"role": "user", "content": "question"}],
                tools=[
                    ToolDefinition(
                        name="bench_get_earthquakes",
                        description="Get data",
                        input_schema={
                            "type": "object",
                            "properties": {},
                        },
                    ),
                ],
                api_key="test-key",
            )

        assert isinstance(result, ToolUseResponse)
        assert result.stop_reason == "tool_use"
        assert result.input_tokens == 100
        assert result.output_tokens == 50
        assert len(result.content) == 1
        assert isinstance(result.content[0], ToolUseBlock)

    def test_text_only_response(self) -> None:
        response_body = {
            "content": [
                {"type": "text", "text": "42"},
            ],
            "stop_reason": "end_turn",
            "usage": {
                "input_tokens": 50,
                "output_tokens": 10,
            },
            "model": "claude-sonnet-4-6",
        }
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(response_body).encode("utf-8")
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)

        with patch(
            "benchmarks.tier2.llm_tool_client.urllib.request.urlopen",
            return_value=mock_resp,
        ):
            result = call_llm_with_tools(
                model="claude-sonnet-4-6",
                system_prompt="test",
                messages=[{"role": "user", "content": "question"}],
                tools=[],
                api_key="test-key",
            )

        assert result.stop_reason == "end_turn"
        assert len(result.content) == 1
        assert isinstance(result.content[0], TextBlock)
        assert result.content[0].text == "42"
