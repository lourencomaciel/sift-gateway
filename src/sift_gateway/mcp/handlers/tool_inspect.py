"""``gateway.inspect_tool`` handler."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sift_gateway.envelope.responses import gateway_error
from sift_gateway.mcp.server import (
    _BUILTIN_TOOL_SCHEMAS,
    _builtin_tool_description,
    _mirrored_tool_full_description,
    _mirrored_tool_list_description,
)
from sift_gateway.mcp.server_helpers import mcp_safe_name as _mcp_safe_name
from sift_gateway.tools.tool_inspect import build_tool_inspect_response

if TYPE_CHECKING:
    from sift_gateway.mcp.server import GatewayServer


def _build_builtin_entry(
    ctx: GatewayServer,
    qualified_name: str,
) -> dict[str, Any]:
    safe_name = _mcp_safe_name(qualified_name)
    description = _builtin_tool_description(ctx, qualified_name)
    return {
        "safe_name": safe_name,
        "qualified_name": qualified_name,
        "source_kind": "builtin",
        "description": description,
        "tools_list_description": description,
        "description_compacted_in_tools_list": False,
        "input_schema": dict(_BUILTIN_TOOL_SCHEMAS.get(qualified_name, {})),
        "metadata": {},
    }


def _build_mirrored_entry(
    qualified_name: str,
    mirrored: Any,
) -> dict[str, Any]:
    safe_name = _mcp_safe_name(qualified_name)
    list_description, compacted = _mirrored_tool_list_description(
        qualified_name,
        mirrored,
    )
    return {
        "safe_name": safe_name,
        "qualified_name": qualified_name,
        "source_kind": "mirrored",
        "description": _mirrored_tool_full_description(mirrored),
        "tools_list_description": list_description,
        "description_compacted_in_tools_list": compacted,
        "input_schema": dict(mirrored.upstream_tool.input_schema),
        "metadata": {
            "upstream_prefix": mirrored.prefix,
            "upstream_tool_name": mirrored.original_name,
            "upstream_instance_id": mirrored.upstream.instance_id,
            "upstream_tool_schema_hash": mirrored.upstream_tool.schema_hash,
        },
    }


def _resolve_tool_entry(
    ctx: GatewayServer,
    tool_name: str,
) -> dict[str, Any] | None:
    builtins: dict[str, dict[str, Any]] = {}
    safe_to_qualified: dict[str, str] = {}
    for qualified_name in ctx.register_tools():
        entry = _build_builtin_entry(ctx, qualified_name)
        builtins[qualified_name] = entry
        safe_to_qualified[entry["safe_name"]] = qualified_name

    if tool_name in builtins:
        return builtins[tool_name]
    qualified_from_safe = safe_to_qualified.get(tool_name)
    if qualified_from_safe is not None:
        return builtins[qualified_from_safe]

    if tool_name in ctx.mirrored_tools:
        return _build_mirrored_entry(tool_name, ctx.mirrored_tools[tool_name])
    qualified_from_safe = None
    for qualified_name in ctx.mirrored_tools:
        safe_name = _mcp_safe_name(qualified_name)
        if tool_name == safe_name:
            qualified_from_safe = qualified_name
            break
    if qualified_from_safe is None:
        return None
    return _build_mirrored_entry(
        qualified_from_safe,
        ctx.mirrored_tools[qualified_from_safe],
    )


async def handle_inspect_tool(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Handle the ``gateway.inspect_tool`` tool call."""
    tool_name = arguments.get("tool_name")
    if not isinstance(tool_name, str) or not tool_name.strip():
        return gateway_error(
            "INVALID_ARGUMENT",
            "tool_name must be a non-empty string",
        )

    include_input_schema = arguments.get("include_input_schema", True)
    if not isinstance(include_input_schema, bool):
        return gateway_error(
            "INVALID_ARGUMENT",
            "include_input_schema must be a boolean when provided",
        )

    max_description_chars = arguments.get("max_description_chars")
    if max_description_chars is not None and (
        isinstance(max_description_chars, bool)
        or not isinstance(max_description_chars, int)
        or max_description_chars <= 0
    ):
        return gateway_error(
            "INVALID_ARGUMENT",
            "max_description_chars must be a positive integer when provided",
        )

    entry = _resolve_tool_entry(ctx, tool_name.strip())
    if entry is None:
        return gateway_error(
            "NOT_FOUND",
            f"tool not found: {tool_name}",
        )

    input_schema = entry["input_schema"] if include_input_schema else None
    return build_tool_inspect_response(
        safe_name=entry["safe_name"],
        qualified_name=entry["qualified_name"],
        source_kind=entry["source_kind"],
        description=entry["description"],
        tools_list_description=entry["tools_list_description"],
        description_compacted_in_tools_list=entry[
            "description_compacted_in_tools_list"
        ],
        input_schema=input_schema,
        max_description_chars=max_description_chars,
        metadata=entry["metadata"],
    )
