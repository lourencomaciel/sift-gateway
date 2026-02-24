"""Unit tests for Tier 2 agent loop."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from benchmarks.tier2.agent_loop import (
    _extract_text_answer,
    _is_error_result,
    run_agent_loop,
)
from benchmarks.tier2.llm_tool_client import (
    TextBlock,
    ToolDefinition,
    ToolUseBlock,
    ToolUseResponse,
)


def _make_tools() -> list[ToolDefinition]:
    return [
        ToolDefinition(
            name="bench_get_earthquakes",
            description="Get earthquake data",
            input_schema={
                "type": "object",
                "properties": {},
            },
        ),
        ToolDefinition(
            name="artifact",
            description="Artifact tool",
            input_schema={
                "type": "object",
                "properties": {
                    "action": {"type": "string"},
                    "query_kind": {"type": "string"},
                    "artifact_id": {"type": "string"},
                    "root_path": {"type": "string"},
                    "scope": {"type": "string"},
                    "code": {"type": "string"},
                },
            },
        ),
    ]


def _tool_use_response(
    content: list[ToolUseBlock | TextBlock],
    stop_reason: str = "tool_use",
) -> ToolUseResponse:
    return ToolUseResponse(
        content=content,
        stop_reason=stop_reason,
        input_tokens=100,
        output_tokens=50,
        model="claude-test",
        latency_ms=500.0,
    )


class TestExtractTextAnswer:
    def test_extracts_text(self) -> None:
        content = [TextBlock(text="42")]
        assert _extract_text_answer(content) == "42"

    def test_concatenates_multiple(self) -> None:
        content = [
            TextBlock(text="The answer"),
            TextBlock(text="is 42"),
        ]
        assert _extract_text_answer(content) == "The answer\nis 42"

    def test_ignores_tool_use_blocks(self) -> None:
        content: list[ToolUseBlock | TextBlock] = [
            ToolUseBlock(id="tu_1", name="tool", input={}),
            TextBlock(text="42"),
        ]
        assert _extract_text_answer(content) == "42"

    def test_empty_content(self) -> None:
        assert _extract_text_answer([]) == ""


class TestIsErrorResult:
    def test_gateway_error_type(self) -> None:
        assert _is_error_result(
            {
                "type": "gateway_error",
                "code": "ERR",
                "message": "fail",
            }
        )

    def test_code_message_without_artifact(self) -> None:
        assert _is_error_result({"code": "ERR", "message": "fail"})

    def test_success_with_artifact(self) -> None:
        assert not _is_error_result(
            {
                "code": "OK",
                "message": "ok",
                "artifact_id": "art_1",
            }
        )

    def test_normal_response(self) -> None:
        assert not _is_error_result({"artifact_id": "art_1", "schemas": []})


class TestRunAgentLoop:
    def test_single_turn_text_answer(self) -> None:
        """LLM responds immediately with text — no tool calls."""
        response = _tool_use_response(
            [TextBlock(text="42")],
            stop_reason="end_turn",
        )

        with patch(
            "benchmarks.tier2.agent_loop.call_llm_with_tools",
            return_value=response,
        ):
            result = run_agent_loop(
                question="How many?",
                runtime=MagicMock(),
                tools=_make_tools(),
                model="claude-test",
                system_prompt="test",
                session_id="s1",
                api_key="key",
            )

        assert result.answer == "42"
        assert result.turns == 1
        assert not result.max_turns_reached
        assert result.total_input_tokens == 100
        assert result.total_output_tokens == 50

    def test_multi_turn_tool_then_answer(self) -> None:
        """LLM calls a tool, then answers on the next turn."""
        call_count = 0

        def mock_llm(**_kwargs: object) -> ToolUseResponse:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return _tool_use_response(
                    [
                        ToolUseBlock(
                            id="tu_1",
                            name="bench_get_earthquakes",
                            input={},
                        ),
                    ]
                )
            return _tool_use_response(
                [TextBlock(text="13")],
                stop_reason="end_turn",
            )

        mock_runtime = MagicMock()
        mock_runtime.call_tool.return_value = {
            "artifact_id": "art_1",
            "schemas": [],
            "response_mode": "schema_ref",
        }

        with patch(
            "benchmarks.tier2.agent_loop.call_llm_with_tools",
            side_effect=mock_llm,
        ):
            result = run_agent_loop(
                question="How many earthquakes?",
                runtime=mock_runtime,
                tools=_make_tools(),
                model="claude-test",
                system_prompt="test",
                session_id="s1",
                api_key="key",
            )

        assert result.answer == "13"
        assert result.turns == 2
        assert result.tool_call_counts.get("mirrored") == 1

    def test_code_query_error_recovery(self) -> None:
        """LLM writes code, it fails, LLM retries."""
        call_count = 0

        def mock_llm(**_kwargs: object) -> ToolUseResponse:
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                # Turns 1&2: code query attempts.
                return _tool_use_response(
                    [
                        ToolUseBlock(
                            id=f"tu_{call_count}",
                            name="artifact",
                            input={
                                "action": "query",
                                "query_kind": "code",
                                "artifact_id": "art_1",
                                "root_path": "$",
                                "scope": "single",
                                "code": "def run(d,s,p): ...",
                            },
                        ),
                    ]
                )
            return _tool_use_response(
                [TextBlock(text="42")],
                stop_reason="end_turn",
            )

        tool_call_count = 0

        def mock_call_tool(name: str, args: dict) -> dict:
            nonlocal tool_call_count
            tool_call_count += 1
            if tool_call_count == 1:
                return {
                    "type": "gateway_error",
                    "code": "CODE_ERROR",
                    "message": "NameError: x not defined",
                }
            return {
                "artifact_id": "art_1",
                "items": [42],
            }

        mock_runtime = MagicMock()
        mock_runtime.call_tool.side_effect = mock_call_tool

        with patch(
            "benchmarks.tier2.agent_loop.call_llm_with_tools",
            side_effect=mock_llm,
        ):
            result = run_agent_loop(
                question="What is the count?",
                runtime=mock_runtime,
                tools=_make_tools(),
                model="claude-test",
                system_prompt="test",
                session_id="s1",
                api_key="key",
            )

        assert result.answer == "42"
        assert result.code_query_attempts == 2
        assert result.code_query_errors == 1

    def test_max_turns_exhaustion(self) -> None:
        """Agent exhausts max_turns without producing an answer."""
        response = _tool_use_response(
            [
                ToolUseBlock(
                    id="tu_1",
                    name="bench_get_earthquakes",
                    input={},
                ),
            ]
        )

        mock_runtime = MagicMock()
        mock_runtime.call_tool.return_value = {
            "artifact_id": "art_1",
            "schemas": [],
        }

        with patch(
            "benchmarks.tier2.agent_loop.call_llm_with_tools",
            return_value=response,
        ):
            result = run_agent_loop(
                question="How many?",
                runtime=mock_runtime,
                tools=_make_tools(),
                model="claude-test",
                system_prompt="test",
                session_id="s1",
                api_key="key",
                max_turns=2,
            )

        assert result.max_turns_reached
        assert result.turns == 2
        assert result.answer == ""

    def test_gateway_context_injected(self) -> None:
        """Verify _gateway_context is added to tool calls."""
        call_count = 0

        def mock_llm(**_kwargs: object) -> ToolUseResponse:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return _tool_use_response(
                    [
                        ToolUseBlock(
                            id="tu_1",
                            name="bench_get_earthquakes",
                            input={},
                        ),
                    ]
                )
            return _tool_use_response(
                [TextBlock(text="done")],
                stop_reason="end_turn",
            )

        mock_runtime = MagicMock()
        mock_runtime.call_tool.return_value = {
            "artifact_id": "art_1",
            "schemas": [],
        }

        with patch(
            "benchmarks.tier2.agent_loop.call_llm_with_tools",
            side_effect=mock_llm,
        ):
            run_agent_loop(
                question="test",
                runtime=mock_runtime,
                tools=_make_tools(),
                model="claude-test",
                system_prompt="test",
                session_id="test_session",
                api_key="key",
            )

        # Check the call_tool was called with _gateway_context.
        call_args = mock_runtime.call_tool.call_args
        assert call_args[0][1]["_gateway_context"] == {
            "session_id": "test_session"
        }

    def test_page_limit_enforced(self) -> None:
        """next_page calls beyond limit return error."""
        call_count = 0

        def mock_llm(**_kwargs: object) -> ToolUseResponse:
            nonlocal call_count
            call_count += 1
            if call_count <= 3:
                return _tool_use_response(
                    [
                        ToolUseBlock(
                            id=f"tu_{call_count}",
                            name="artifact",
                            input={
                                "action": "next_page",
                                "artifact_id": "art_1",
                            },
                        ),
                    ]
                )
            return _tool_use_response(
                [TextBlock(text="done")],
                stop_reason="end_turn",
            )

        mock_runtime = MagicMock()
        mock_runtime.call_tool.return_value = {
            "artifact_id": "art_1",
            "items": [1, 2, 3],
        }

        with patch(
            "benchmarks.tier2.agent_loop.call_llm_with_tools",
            side_effect=mock_llm,
        ):
            result = run_agent_loop(
                question="test",
                runtime=mock_runtime,
                tools=_make_tools(),
                model="claude-test",
                system_prompt="test",
                session_id="s1",
                api_key="key",
                max_pages=2,
            )

        assert result.pages_fetched == 3
        # Third call exceeds limit — runtime.call_tool should
        # NOT be called for it (error returned directly).
        assert mock_runtime.call_tool.call_count == 2

    def test_tool_exception_returned_as_error(self) -> None:
        """Runtime exceptions are returned as error tool results."""
        call_count = 0

        def mock_llm(**_kwargs: object) -> ToolUseResponse:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return _tool_use_response(
                    [
                        ToolUseBlock(
                            id="tu_1",
                            name="bench_get_earthquakes",
                            input={},
                        ),
                    ]
                )
            return _tool_use_response(
                [TextBlock(text="error handled")],
                stop_reason="end_turn",
            )

        mock_runtime = MagicMock()
        mock_runtime.call_tool.side_effect = RuntimeError("connection failed")

        with patch(
            "benchmarks.tier2.agent_loop.call_llm_with_tools",
            side_effect=mock_llm,
        ):
            result = run_agent_loop(
                question="test",
                runtime=mock_runtime,
                tools=_make_tools(),
                model="claude-test",
                system_prompt="test",
                session_id="s1",
                api_key="key",
            )

        assert result.answer == "error handled"
        assert result.turns == 2

    def test_token_budget_saves_conversation(self) -> None:
        """Conversation is saved when token budget is reached."""
        call_count = 0

        def mock_llm(**_kwargs: object) -> ToolUseResponse:
            nonlocal call_count
            call_count += 1
            return _tool_use_response(
                [
                    ToolUseBlock(
                        id=f"tu_{call_count}",
                        name="bench_get_earthquakes",
                        input={},
                    ),
                ],
            )

        mock_runtime = MagicMock()
        mock_runtime.call_tool.return_value = {
            "artifact_id": "art_1",
            "schemas": [],
        }

        with patch(
            "benchmarks.tier2.agent_loop.call_llm_with_tools",
            side_effect=mock_llm,
        ):
            result = run_agent_loop(
                question="test",
                runtime=mock_runtime,
                tools=_make_tools(),
                model="claude-test",
                system_prompt="test",
                session_id="s1",
                api_key="key",
                max_input_tokens=50,
            )

        assert result.token_budget_reached
        assert len(result.conversation) > 0
