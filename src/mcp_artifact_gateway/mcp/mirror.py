"""Tool mirroring: expose upstream tools as {prefix}.{tool}."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from mcp_artifact_gateway.constants import RESERVED_EXACT_KEYS, RESERVED_PREFIX
from mcp_artifact_gateway.mcp.upstream import UpstreamInstance, UpstreamToolSchema


@dataclass(frozen=True)
class MirroredTool:
    """A tool mirrored from upstream with prefixed name."""

    qualified_name: str  # "{prefix}.{tool}"
    upstream: UpstreamInstance
    upstream_tool: UpstreamToolSchema

    @property
    def prefix(self) -> str:
        return self.upstream.prefix

    @property
    def original_name(self) -> str:
        return self.upstream_tool.name


def build_mirrored_tools(upstreams: list[UpstreamInstance]) -> dict[str, MirroredTool]:
    """Build mapping of qualified tool names to MirroredTool objects."""
    tools: dict[str, MirroredTool] = {}
    for upstream in upstreams:
        for tool_schema in upstream.tools:
            qualified = f"{upstream.prefix}.{tool_schema.name}"
            if qualified in tools:
                raise ValueError(f"duplicate mirrored tool name: {qualified}")
            tools[qualified] = MirroredTool(
                qualified_name=qualified,
                upstream=upstream,
                upstream_tool=tool_schema,
            )
    return tools


def strip_reserved_gateway_args(args: dict[str, Any]) -> dict[str, Any]:
    """Remove reserved gateway args exactly per spec.

    Removes:
    - Exact keys: _gateway_context, _gateway_parent_artifact_id, _gateway_chain_seq
    - Any key starting with prefix "_gateway_"

    Does NOT remove: gateway_url, _gatewa, gateway_*, etc.
    """
    return {
        key: value
        for key, value in args.items()
        if key not in RESERVED_EXACT_KEYS and not key.startswith(RESERVED_PREFIX)
    }


def extract_gateway_context(args: dict[str, Any]) -> dict[str, Any] | None:
    """Extract _gateway_context from args if present."""
    ctx = args.get("_gateway_context")
    if isinstance(ctx, dict):
        return ctx
    return None


def validate_against_schema(
    args: dict[str, Any],
    schema: dict[str, Any],
) -> list[str]:
    """Validate forwarded args against upstream tool schema (strict enforcement).

    Returns list of violation strings. Non-empty means strict rejection.
    """
    # Basic validation - check required properties exist
    violations: list[str] = []
    required = schema.get("required", [])
    properties = schema.get("properties", {})
    for key in required:
        if key not in args:
            violations.append(f"missing required argument: {key}")
    for key in args:
        if properties and key not in properties:
            violations.append(f"unknown argument: {key}")
    return violations
