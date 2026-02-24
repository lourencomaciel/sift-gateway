"""Core agent loop: LLM <-> tool execution cycle.

The LLM autonomously decides which tools to call, when to paginate,
when to write code, and how to recover from errors.  The loop runs
until the LLM produces a final text answer (no tool_use blocks) or
the turn/token budget is exhausted.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import sys
from typing import Any

from benchmarks.tier1.sift_runtime import _is_error_response, _MCPRuntime
from benchmarks.tier2.llm_tool_client import (
    TextBlock,
    ToolDefinition,
    ToolUseBlock,
    call_llm_with_tools,
)
from benchmarks.tier2.tool_bridge import (
    classify_tool_call,
    inject_gateway_context,
)


@dataclass
class TurnMetrics:
    """Metrics for a single agent turn."""

    input_tokens: int = 0
    output_tokens: int = 0
    latency_ms: float = 0.0
    tool_calls: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class AgentResult:
    """Result of an agent loop execution."""

    answer: str
    turns: int
    max_turns_reached: bool
    token_budget_reached: bool
    turn_metrics: list[TurnMetrics] = field(default_factory=list)
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_latency_ms: float = 0.0
    tool_call_counts: dict[str, int] = field(default_factory=dict)
    code_query_attempts: int = 0
    code_query_errors: int = 0
    pages_fetched: int = 0
    conversation: list[dict[str, Any]] = field(default_factory=list)


_DEFAULT_MAX_TURNS = 15
_DEFAULT_MAX_PAGES = 10
_DEFAULT_MAX_INPUT_TOKENS = 200_000


def _extract_text_answer(
    content: list[ToolUseBlock | TextBlock],
) -> str:
    """Extract final text answer from content blocks."""
    texts = [block.text for block in content if isinstance(block, TextBlock)]
    return "\n".join(texts).strip()


def _tool_result_to_content(
    result: dict[str, Any],
) -> str:
    """Serialize a tool result for the LLM conversation."""
    return json.dumps(result, default=str)


def _is_error_result(result: dict[str, Any]) -> bool:
    """Detect whether a gateway tool result is an error.

    Delegates to ``_is_error_response`` from the tier1 runtime
    to avoid duplicating the detection heuristic.
    """
    return _is_error_response(result)


def run_agent_loop(
    *,
    question: str,
    runtime: _MCPRuntime,
    tools: list[ToolDefinition],
    model: str,
    system_prompt: str,
    session_id: str,
    api_key: str | None = None,
    temperature: float = 0.0,
    max_tokens: int = 4096,
    max_turns: int = _DEFAULT_MAX_TURNS,
    max_pages: int = _DEFAULT_MAX_PAGES,
    max_input_tokens: int = _DEFAULT_MAX_INPUT_TOKENS,
) -> AgentResult:
    """Run the autonomous agent loop for a single question.

    The LLM receives the question and available tools, then
    autonomously decides which tools to call.  The loop ends when
    the LLM produces a text-only response (no tool_use blocks)
    or the budget is exhausted.

    Args:
        question: The natural language question to answer.
        runtime: MCP runtime for executing tool calls.
        tools: Available tool definitions.
        model: LLM model identifier.
        system_prompt: System prompt for the agent.
        session_id: Session ID for ``_gateway_context``.
        api_key: API key (falls back to env var).
        temperature: Sampling temperature.
        max_tokens: Max output tokens per LLM call.
        max_turns: Maximum LLM round-trips before aborting.
        max_pages: Maximum pagination calls per question.
        max_input_tokens: Token budget safety valve.

    Returns:
        ``AgentResult`` with answer, metrics, and conversation.
    """
    messages: list[dict[str, Any]] = [
        {"role": "user", "content": question},
    ]

    result = AgentResult(
        answer="",
        turns=0,
        max_turns_reached=False,
        token_budget_reached=False,
    )

    pages_used = 0

    for _turn in range(max_turns):
        # Token budget check.
        if result.total_input_tokens >= max_input_tokens:
            result.token_budget_reached = True
            result.conversation = list(messages)
            print(
                f"  [budget] input tokens "
                f"({result.total_input_tokens:,}) "
                f">= limit ({max_input_tokens:,}), stopping",
                file=sys.stderr,
            )
            break

        # Call LLM.
        response = call_llm_with_tools(
            model=model,
            system_prompt=system_prompt,
            messages=messages,
            tools=tools,
            api_key=api_key,
            temperature=temperature,
            max_tokens=max_tokens,
        )

        result.turns += 1

        # Record turn metrics.
        turn_metrics = TurnMetrics(
            input_tokens=response.input_tokens,
            output_tokens=response.output_tokens,
            latency_ms=response.latency_ms,
        )
        result.total_input_tokens += response.input_tokens
        result.total_output_tokens += response.output_tokens
        result.total_latency_ms += response.latency_ms

        # Check for tool_use blocks.
        tool_uses = [b for b in response.content if isinstance(b, ToolUseBlock)]

        if not tool_uses:
            # No tool calls — LLM is done, extract text answer.
            result.answer = _extract_text_answer(response.content)
            result.turn_metrics.append(turn_metrics)
            # Record conversation.
            result.conversation = list(messages)
            result.conversation.append(_assistant_message(response.content))
            break

        # Append assistant message with tool_use blocks.
        assistant_msg = _assistant_message(response.content)
        messages.append(assistant_msg)

        # Execute each tool call.
        tool_results: list[dict[str, Any]] = []
        for tool_use in tool_uses:
            category = classify_tool_call(tool_use.name, tool_use.input)
            turn_metrics.tool_calls.append(
                {
                    "name": tool_use.name,
                    "category": category,
                }
            )

            # Track counts.
            result.tool_call_counts[category] = (
                result.tool_call_counts.get(category, 0) + 1
            )

            if category == "code_query":
                result.code_query_attempts += 1
            if category == "next_page":
                pages_used += 1
                result.pages_fetched += 1

            # Enforce page limit.
            if category == "next_page" and pages_used > max_pages:
                tool_result_content = json.dumps(
                    {
                        "type": "gateway_error",
                        "code": "PAGE_LIMIT",
                        "message": (
                            f"Page limit ({max_pages}) reached. "
                            f"Provide your best answer now."
                        ),
                    }
                )
                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": tool_use.id,
                        "content": tool_result_content,
                        "is_error": True,
                    }
                )
                continue

            # Inject _gateway_context and execute.
            augmented_args = inject_gateway_context(
                tool_use.input, session_id=session_id
            )

            try:
                tool_output = runtime.call_tool(tool_use.name, augmented_args)
            except Exception as exc:
                print(
                    f"  [tool-error] {type(exc).__name__}: {exc}",
                    file=sys.stderr,
                )
                tool_output = {
                    "type": "gateway_error",
                    "code": "TOOL_ERROR",
                    "message": str(exc),
                }

            is_error = _is_error_result(tool_output)
            if is_error and category == "code_query":
                result.code_query_errors += 1

            tool_result_content = _tool_result_to_content(tool_output)
            tool_results.append(
                {
                    "type": "tool_result",
                    "tool_use_id": tool_use.id,
                    "content": tool_result_content,
                    "is_error": is_error,
                }
            )

        # Append tool results as user message.
        messages.append({"role": "user", "content": tool_results})

        result.turn_metrics.append(turn_metrics)
    else:
        # Loop exhausted without a final answer.
        result.max_turns_reached = True
        result.conversation = list(messages)

    return result


def _assistant_message(
    content: list[ToolUseBlock | TextBlock],
) -> dict[str, Any]:
    """Build an Anthropic assistant message from content blocks."""
    blocks: list[dict[str, Any]] = []
    for block in content:
        if isinstance(block, ToolUseBlock):
            blocks.append(
                {
                    "type": "tool_use",
                    "id": block.id,
                    "name": block.name,
                    "input": block.input,
                }
            )
        elif isinstance(block, TextBlock):
            blocks.append(
                {
                    "type": "text",
                    "text": block.text,
                }
            )
    return {"role": "assistant", "content": blocks}
