"""Tool-use LLM client using urllib only (no third-party deps).

Wraps the Anthropic Messages API tool-use flow, following the same
retry/backoff pattern as ``tier1/llm_client.py``.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import sys
import time
from typing import Any
import urllib.error
import urllib.request

from benchmarks.tier1.llm_client import (
    LLMAPIError,
    _detect_provider,
    _resolve_api_key,
)

_MAX_RETRIES = 5
_INITIAL_BACKOFF_S = 2.0


@dataclass(frozen=True)
class ToolDefinition:
    """An LLM tool definition (name + description + schema)."""

    name: str
    description: str
    input_schema: dict[str, Any]


@dataclass(frozen=True)
class ToolUseBlock:
    """A tool-use request from the LLM."""

    id: str
    name: str
    input: dict[str, Any]


@dataclass(frozen=True)
class TextBlock:
    """A text content block from the LLM."""

    text: str


@dataclass(frozen=True)
class ToolUseResponse:
    """Response from a tool-use LLM call."""

    content: list[ToolUseBlock | TextBlock]
    stop_reason: str
    input_tokens: int
    output_tokens: int
    model: str
    latency_ms: float


def _log_retry(status: int, attempt: int, backoff: float) -> None:
    print(
        f"  [rate-limit] HTTP {status}, "
        f"retry {attempt + 1}/{_MAX_RETRIES} "
        f"in {backoff:.0f}s",
        file=sys.stderr,
    )


def _tools_to_api_format(
    tools: list[ToolDefinition],
) -> list[dict[str, Any]]:
    """Convert ToolDefinition list to Anthropic API format."""
    return [
        {
            "name": t.name,
            "description": t.description,
            "input_schema": t.input_schema,
        }
        for t in tools
    ]


def _parse_content_blocks(
    content: list[dict[str, Any]],
) -> list[ToolUseBlock | TextBlock]:
    """Parse Anthropic content blocks into typed objects."""
    blocks: list[ToolUseBlock | TextBlock] = []
    for block in content:
        block_type = block.get("type", "")
        if block_type == "tool_use":
            blocks.append(
                ToolUseBlock(
                    id=block.get("id", ""),
                    name=block.get("name", ""),
                    input=block.get("input", {}),
                )
            )
        elif block_type == "text":
            text = block.get("text", "")
            if text:
                blocks.append(TextBlock(text=text))
    return blocks


def call_llm_with_tools(
    *,
    model: str,
    system_prompt: str,
    messages: list[dict[str, Any]],
    tools: list[ToolDefinition],
    api_key: str | None = None,
    temperature: float = 0.0,
    max_tokens: int = 4096,
) -> ToolUseResponse:
    """Call the Anthropic Messages API with tool-use support.

    Args:
        model: Model identifier (e.g. ``claude-sonnet-4-6``).
        system_prompt: System prompt text.
        messages: Conversation history in Anthropic format.
        tools: Available tool definitions.
        api_key: API key (falls back to env var).
        temperature: Sampling temperature.
        max_tokens: Maximum output tokens.

    Returns:
        Parsed response with content blocks and metadata.

    Raises:
        LLMAPIError: On network, auth, or rate-limit failures.
    """
    provider = _detect_provider(model)
    if provider != "anthropic":
        msg = f"Tool-use client only supports Anthropic models, got: {model}"
        raise LLMAPIError(msg)

    resolved_key = _resolve_api_key(
        provider=provider,
        api_key=api_key,
    )

    payload: dict[str, Any] = {
        "model": model,
        "max_tokens": max_tokens,
        "temperature": temperature,
        "system": system_prompt,
        "messages": messages,
        "tools": _tools_to_api_format(tools),
    }
    data = json.dumps(payload).encode("utf-8")
    headers = {
        "x-api-key": resolved_key,
        "anthropic-version": "2023-06-01",
        "Content-Type": "application/json",
    }

    backoff = _INITIAL_BACKOFF_S
    start = time.monotonic()
    for attempt in range(_MAX_RETRIES + 1):
        request = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            method="POST",
            data=data,
            headers=headers,
        )
        try:
            with urllib.request.urlopen(request, timeout=120) as resp:
                body = json.loads(resp.read().decode("utf-8", errors="replace"))
            break
        except urllib.error.HTTPError as exc:
            if exc.code in (429, 500, 529) and attempt < _MAX_RETRIES:
                _log_retry(exc.code, attempt, backoff)
                time.sleep(backoff)
                backoff *= 2
                continue
            error_body = exc.read().decode("utf-8", errors="replace")
            raise LLMAPIError(
                f"Anthropic API error ({exc.code}): {error_body}"
            ) from exc
        except urllib.error.URLError as exc:
            raise LLMAPIError(f"Anthropic API request failed: {exc}") from exc
    else:
        raise LLMAPIError("Anthropic API: exhausted retries")
    latency = (time.monotonic() - start) * 1000.0

    content = _parse_content_blocks(body.get("content", []))
    usage = body.get("usage", {})

    return ToolUseResponse(
        content=content,
        stop_reason=body.get("stop_reason", ""),
        input_tokens=usage.get("input_tokens", 0),
        output_tokens=usage.get("output_tokens", 0),
        model=body.get("model", model),
        latency_ms=latency,
    )
