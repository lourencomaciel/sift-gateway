"""Helpers for compact tool listings and ``gateway.inspect_tool``."""

from __future__ import annotations

from typing import Any

DEFAULT_LIST_DESCRIPTION_MAX_CHARS = 600


def compact_tool_description_for_list(
    description: str,
    *,
    inspect_tool_name: str,
    safe_tool_name: str,
    max_chars: int = DEFAULT_LIST_DESCRIPTION_MAX_CHARS,
) -> tuple[str, bool]:
    """Return a compact single-line description for ``tools/list``.

    Args:
        description: Original tool description text.
        inspect_tool_name: MCP-safe name of the inspect helper tool.
        safe_tool_name: MCP-safe name of the tool being described.
        max_chars: Maximum characters for the compacted description body.

    Returns:
        Tuple of ``(description, compacted)`` where ``compacted`` indicates
        whether the returned text was truncated and annotated with an inspect
        hint.
    """
    normalized = " ".join(description.split()).strip()
    if not normalized:
        return "", False
    if len(normalized) <= max_chars:
        return normalized, False

    hint = (
        f' Use `{inspect_tool_name}` with `tool_name="{safe_tool_name}"` '
        "for full documentation."
    )
    ellipsis = "..."
    head_budget = max_chars - len(hint) - len(ellipsis)
    if head_budget <= 0:
        head_budget = max_chars
        hint = ""
        ellipsis = ""

    head = normalized[:head_budget].rstrip()
    last_space = head.rfind(" ")
    if last_space >= max(head_budget // 2, 24):
        head = head[:last_space].rstrip()
    compacted = f"{head}{ellipsis}{hint}".strip()
    return compacted, True


def trim_description_for_inspect(
    description: str,
    *,
    max_chars: int | None,
) -> tuple[str, bool]:
    """Trim a full description for inspect responses when requested."""
    if max_chars is None or len(description) <= max_chars:
        return description, False
    if max_chars <= 3:
        return description[:max_chars], True

    head = description[: max_chars - 3].rstrip()
    if "\n" not in head:
        last_space = head.rfind(" ")
        if last_space >= max((max_chars - 3) // 2, 24):
            head = head[:last_space].rstrip()
    return f"{head}...", True


def build_tool_inspect_response(
    *,
    safe_name: str,
    qualified_name: str,
    source_kind: str,
    description: str,
    tools_list_description: str,
    description_compacted_in_tools_list: bool,
    input_schema: dict[str, Any] | None,
    max_description_chars: int | None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the structured ``gateway.inspect_tool`` response payload."""
    returned_description, description_truncated = trim_description_for_inspect(
        description,
        max_chars=max_description_chars,
    )
    payload_metadata: dict[str, Any] = {
        "description_compacted_in_tools_list": (
            description_compacted_in_tools_list
        ),
        "description_truncated": description_truncated,
        "original_description_chars": len(description),
        "returned_description_chars": len(returned_description),
        "tools_list_description_chars": len(tools_list_description),
        "input_schema_included": input_schema is not None,
    }
    if metadata:
        payload_metadata.update(metadata)

    result: dict[str, Any] = {
        "type": "gateway_tool_inspect",
        "name": safe_name,
        "qualified_name": qualified_name,
        "source_kind": source_kind,
        "description": returned_description,
        "tools_list_description": tools_list_description,
        "metadata": payload_metadata,
    }
    if input_schema is not None:
        result["input_schema"] = dict(input_schema)
    return result
