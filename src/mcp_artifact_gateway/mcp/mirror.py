"""Mirror upstream tools under namespaced qualified names.

Expose each upstream tool as ``{prefix}.{tool}`` in the gateway,
stripping reserved ``_gateway_*`` arguments before forwarding.
Exports ``MirroredTool``, ``build_mirrored_tools``, and argument
helpers for schema validation and reserved-key stripping.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from mcp_artifact_gateway.constants import RESERVED_EXACT_KEYS, RESERVED_PREFIX
from mcp_artifact_gateway.mcp.upstream import (
    UpstreamInstance,
    UpstreamToolSchema,
)


@dataclass(frozen=True)
class MirroredTool:
    """A single upstream tool exposed under a qualified name.

    Attributes:
        qualified_name: Namespaced name ``{prefix}.{tool}``.
        upstream: The upstream instance owning this tool.
        upstream_tool: Schema descriptor from tool discovery.
    """

    qualified_name: str  # "{prefix}.{tool}"
    upstream: UpstreamInstance
    upstream_tool: UpstreamToolSchema

    @property
    def prefix(self) -> str:
        """The upstream namespace prefix."""
        return self.upstream.prefix

    @property
    def original_name(self) -> str:
        """The original upstream tool name."""
        return self.upstream_tool.name


def build_mirrored_tools(
    upstreams: list[UpstreamInstance],
) -> dict[str, MirroredTool]:
    """Build mapping of qualified names to ``MirroredTool`` objects.

    Args:
        upstreams: Connected upstream instances with discovered
            tool schemas.

    Returns:
        Dict keyed by ``{prefix}.{tool}`` qualified names.

    Raises:
        ValueError: If two upstreams produce the same qualified
            tool name.
    """
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
    """Remove reserved ``_gateway_*`` keys before forwarding.

    Strips exact reserved keys and any key starting with the
    ``_gateway_`` prefix. Does not remove ``gateway_*`` or
    partial-prefix keys like ``_gatewa``.

    Args:
        args: Raw tool arguments dict.

    Returns:
        New dict with reserved keys removed.
    """
    return {
        key: value
        for key, value in args.items()
        if key not in RESERVED_EXACT_KEYS
        and not key.startswith(RESERVED_PREFIX)
    }


def extract_gateway_context(args: dict[str, Any]) -> dict[str, Any] | None:
    """Extract ``_gateway_context`` from arguments.

    Args:
        args: Raw tool arguments dict.

    Returns:
        The context dict, or ``None`` if absent or not a dict.
    """
    ctx = args.get("_gateway_context")
    if isinstance(ctx, dict):
        return ctx
    return None


def validate_against_schema(
    args: dict[str, Any],
    schema: dict[str, Any],
) -> list[str]:
    """Validate forwarded args against the upstream tool schema.

    Performs basic checks: required properties must be present,
    and when ``additionalProperties`` is ``False``, unknown keys
    are rejected.

    Args:
        args: Forwarded tool arguments (reserved keys already
            stripped).
        schema: JSON Schema dict from the upstream tool.

    Returns:
        List of violation description strings. An empty list
        indicates valid arguments.
    """
    # Basic validation - check required properties exist
    violations: list[str] = []
    required = schema.get("required", [])
    properties = schema.get("properties", {})
    for key in required:
        if key not in args:
            violations.append(f"missing required argument: {key}")
    additional = schema.get("additionalProperties", True)
    if additional is False:
        for key in args:
            if properties and key not in properties:
                violations.append(f"unknown argument: {key}")
    return violations
