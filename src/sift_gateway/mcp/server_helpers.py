"""Helper utilities shared by the MCP gateway server module."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
import importlib.util
import json
from pathlib import Path
import shutil
from typing import Any

from fastmcp.server.dependencies import get_context
from fastmcp.tools.tool import Tool, ToolResult

from sift_gateway.cursor.payload import CursorStaleError
from sift_gateway.cursor.token import CursorTokenError
from sift_gateway.envelope.responses import gateway_error
from sift_gateway.tools.usage_hint import PAGINATION_COMPLETENESS_RULE

_SUPPORTED_ENVELOPE_PARTS = {
    "json",
    "text",
    "resource_ref",
    "binary_ref",
    "image_ref",
}


def artifact_tool_description(
    *,
    code_query_package_summary: str,
) -> str:
    """Build artifact-tool description with compact package summary."""
    return (
        "Interact with stored artifacts. "
        "Actions: query and next_page. "
        'Use action="query" with query_kind="code" to run Python over '
        "stored artifacts. "
        f"Code-query packages: {code_query_package_summary}. "
        "Use action=\"next_page\" to fetch additional upstream pages for a "
        "paginated artifact. "
        f"{PAGINATION_COMPLETENESS_RULE}"
    )


def not_implemented(tool_name: str) -> dict[str, Any]:
    """Return a NOT_IMPLEMENTED gateway error for a tool."""
    return gateway_error(
        "NOT_IMPLEMENTED",
        f"{tool_name} is not wired to persistence yet",
    )


def cursor_position(payload: dict[str, Any]) -> dict[str, Any]:
    """Extract position_state dict from cursor payload."""
    position = payload.get("position_state")
    if not isinstance(position, dict):
        msg = "cursor missing position_state"
        raise CursorTokenError(msg)
    return position


def assert_cursor_field(
    payload: Mapping[str, Any],
    *,
    field: str,
    expected: object,
) -> None:
    """Raise CursorStaleError if a cursor field does not match."""
    actual = payload.get(field)
    if actual != expected:
        msg = f"cursor {field} mismatch"
        raise CursorStaleError(msg)


def check_sample_corruption(
    root_row: dict[str, Any],
    sample_rows: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Return INTERNAL error if expected sample indices are missing rows."""
    expected_raw = root_row.get("sample_indices")
    if not isinstance(expected_raw, list) or not expected_raw:
        return None
    expected = {int(i) for i in expected_raw if isinstance(i, int)}
    actual = {
        int(row["sample_index"])
        for row in sample_rows
        if isinstance(row.get("sample_index"), int)
    }
    missing = sorted(expected - actual)
    if missing:
        return gateway_error(
            "INTERNAL",
            "sample data corruption: expected sample rows missing",
            details={
                "root_key": root_row.get("root_key"),
                "missing_indices": missing,
                "expected_count": len(expected),
                "actual_count": len(actual),
            },
        )
    return None


def mcp_safe_name(qualified_name: str) -> str:
    """Convert a dotted qualified name to an MCP-safe name."""
    return qualified_name.replace(".", "_")


def assert_unique_safe_tool_name(
    seen: dict[str, str],
    *,
    safe_name: str,
    qualified_name: str,
) -> None:
    """Ensure MCP-safe tool names remain collision-free."""
    existing = seen.get(safe_name)
    if existing is not None and existing != qualified_name:
        msg = (
            "tool name collision after MCP-safe sanitization: "
            f"{existing!r} and {qualified_name!r} -> {safe_name!r}"
        )
        raise ValueError(msg)
    seen[safe_name] = qualified_name


def command_resolvable(command: str | None) -> bool:
    """Return whether a stdio command appears resolvable on this host."""
    if not command:
        return False
    if "/" in command:
        candidate = Path(command)
        return candidate.exists() and candidate.is_file()
    return shutil.which(command) is not None


def stdio_module_probe(args: list[str]) -> dict[str, Any] | None:
    """Return module import diagnostics for ``python -m <module>`` launches."""
    if len(args) < 2 or args[0] != "-m":
        return None
    module = args[1]
    probe: dict[str, Any] = {"module": module}
    try:
        spec = importlib.util.find_spec(module)
    except ModuleNotFoundError as exc:
        probe["importable"] = False
        probe["error"] = str(exc)
        return probe
    probe["importable"] = spec is not None
    if spec is None:
        probe["error"] = "module not found"
    return probe


def ensure_gateway_context(arguments: dict[str, Any]) -> dict[str, Any]:
    """Auto-inject ``_gateway_context.session_id`` from MCP transport."""
    ctx = arguments.get("_gateway_context")
    if isinstance(ctx, dict) and ctx.get("session_id"):
        return arguments
    try:
        mcp_ctx = get_context()
        session_id = mcp_ctx.session_id
    except RuntimeError:
        return arguments
    gw_ctx: dict[str, Any] = dict(ctx) if isinstance(ctx, dict) else {}
    gw_ctx.setdefault("session_id", session_id)
    return {**arguments, "_gateway_context": gw_ctx}


class RuntimeTool(Tool):
    """FastMCP tool subclass that accepts raw argument dicts."""

    handler: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]
    response_sanitizer: Callable[[dict[str, Any]], dict[str, Any]] | None = None

    async def run(self, arguments: dict[str, Any]) -> ToolResult:
        """Execute the handler with raw arguments."""
        arguments = ensure_gateway_context(arguments)
        result = await self.handler(arguments)
        if self.response_sanitizer is not None:
            result = self.response_sanitizer(result)
        return ToolResult(structured_content=result)


def upstream_error_message(result: dict[str, Any]) -> str:
    """Extract a human-readable error message from an upstream result."""
    content = result.get("content")
    if isinstance(content, list):
        for block in content:
            if isinstance(block, dict):
                text = block.get("text")
                if isinstance(text, str) and text.strip():
                    return text
    return "upstream tool returned an error"


def normalize_upstream_content(
    *,
    content: list[dict[str, Any]] | None,
    structured_content: Any,
) -> list[Mapping[str, Any]]:
    """Normalize upstream content blocks into envelope parts."""
    normalized: list[Mapping[str, Any]] = []
    if isinstance(structured_content, (dict, list)):
        normalized.append({"type": "json", "value": structured_content})
    elif structured_content is not None:
        normalized.append(
            {
                "type": "text",
                "text": json.dumps(structured_content, ensure_ascii=False),
            }
        )

    for block in content or []:
        part_type = block.get("type")
        if part_type in _SUPPORTED_ENVELOPE_PARTS:
            normalized.append(block)
            continue
        if isinstance(block.get("text"), str):
            normalized.append({"type": "text", "text": block["text"]})
            continue
        normalized.append(
            {
                "type": "text",
                "text": json.dumps(block, sort_keys=True, ensure_ascii=False),
            }
        )
    return normalized

