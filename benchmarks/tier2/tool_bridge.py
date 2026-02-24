"""Bridge between MCP tool schemas and LLM tool definitions.

Handles conversion, ``_gateway_context`` injection/stripping,
and tool-call classification for metrics.
"""

from __future__ import annotations

import copy
from typing import Any

from benchmarks.tier2.llm_tool_client import ToolDefinition

_GATEWAY_CONTEXT_KEY = "_gateway_context"

#: Tools built into the gateway (not mirrored from upstreams).
_GATEWAY_NATIVE_TOOLS = frozenset({"artifact", "gateway_status"})


def mcp_tools_to_definitions(
    mcp_tools: list[dict[str, Any]],
) -> list[ToolDefinition]:
    """Convert MCP tool schemas to LLM ToolDefinition list.

    Strips ``_gateway_context`` from input schemas so the LLM
    never sees it and cannot accidentally set it.

    Args:
        mcp_tools: List of dicts with ``name``, ``description``,
            and ``input_schema`` keys (from ``runtime.list_tools()``).

    Returns:
        List of ``ToolDefinition`` instances ready for the LLM.
    """
    definitions: list[ToolDefinition] = []
    for tool in mcp_tools:
        name = tool.get("name", "")
        description = tool.get("description", "")
        raw_schema = tool.get("input_schema", {})

        # Deep-copy to avoid mutating the original.
        schema = copy.deepcopy(raw_schema)

        # Remove _gateway_context from properties and required.
        props = schema.get("properties", {})
        props.pop(_GATEWAY_CONTEXT_KEY, None)
        required = schema.get("required", [])
        if _GATEWAY_CONTEXT_KEY in required:
            schema["required"] = [
                r for r in required if r != _GATEWAY_CONTEXT_KEY
            ]

        definitions.append(
            ToolDefinition(
                name=name,
                description=description,
                input_schema=schema,
            )
        )
    return definitions


def inject_gateway_context(
    arguments: dict[str, Any],
    *,
    session_id: str,
) -> dict[str, Any]:
    """Add ``_gateway_context`` to tool arguments before dispatch.

    Returns a new dict — does not mutate the input.

    Args:
        arguments: The tool arguments from the LLM.
        session_id: Session identifier to include in context.

    Returns:
        Copy of arguments with ``_gateway_context`` added.
    """
    result = copy.deepcopy(arguments)
    result[_GATEWAY_CONTEXT_KEY] = {"session_id": session_id}
    return result


def classify_tool_call(
    tool_name: str,
    arguments: dict[str, Any],
) -> str:
    """Classify a tool call for metric tracking.

    Categories:
    - ``mirrored``: upstream dataset tool (any non-gateway-native tool)
    - ``code_query``: ``artifact`` with ``query_kind=code``
    - ``next_page``: ``artifact`` with ``action=next_page``
    - ``describe``: ``artifact`` with ``action=describe``
    - ``status``: ``gateway_status`` tool
    - ``query_other``: ``artifact`` with ``action=query`` but
      non-code ``query_kind`` (e.g. ``jsonpath``)
    - ``other``: anything else

    Args:
        tool_name: The MCP tool name.
        arguments: The tool arguments dict.

    Returns:
        Category string.
    """
    if tool_name == "gateway_status":
        return "status"

    if tool_name == "artifact":
        action = arguments.get("action", "")
        if action == "next_page":
            return "next_page"
        if action == "describe":
            return "describe"
        query_kind = arguments.get("query_kind", "")
        if query_kind == "code":
            return "code_query"
        if action == "query":
            return "query_other"
        return "other"

    # Anything not gateway-native is a mirrored upstream tool.
    if tool_name not in _GATEWAY_NATIVE_TOOLS:
        return "mirrored"

    return "other"
