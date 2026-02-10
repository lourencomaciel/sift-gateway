"""Connect to upstream MCP servers and discover their tools.

Manage client transport configuration, tool schema hashing, and
stable upstream identity computation.  Exports ``UpstreamInstance``,
``connect_upstreams``, and ``call_upstream_tool``.

Typical usage example::

    upstreams = await connect_upstreams(config.upstreams)
    result = await call_upstream_tool(
        upstreams[0], "list_repos", {"org": "acme"}
    )
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from fastmcp import Client
from fastmcp.client.transports import StdioTransport

from sidepouch_mcp.canon.rfc8785 import canonical_bytes
from sidepouch_mcp.config.settings import UpstreamConfig
from sidepouch_mcp.util.hashing import sha256_trunc


@dataclass(frozen=True)
class UpstreamToolSchema:
    """Schema descriptor for a single tool discovered from upstream.

    Attributes:
        name: Tool name as reported by the upstream server.
        description: Human-readable tool description.
        input_schema: JSON Schema dict for the tool's arguments.
        schema_hash: Truncated SHA-256 of the canonical schema.
    """

    name: str
    description: str
    input_schema: dict[str, Any]
    schema_hash: str  # sha256(canonical_bytes(input_schema))[:32]


@dataclass(frozen=True)
class UpstreamInstance:
    """Immutable runtime descriptor for a connected upstream server.

    Created during upstream discovery and held for the lifetime of
    the gateway process.

    Attributes:
        config: Original upstream configuration.
        instance_id: Stable identity hash for cache keying.
        tools: Discovered tool schemas from the upstream.
        auth_fingerprint: Optional hash of auth-relevant values.
    """

    config: UpstreamConfig
    instance_id: str  # upstream_instance_id
    tools: list[UpstreamToolSchema]
    auth_fingerprint: str | None = None

    @property
    def prefix(self) -> str:
        """The upstream namespace prefix."""
        return self.config.prefix


def _client_transport(config: UpstreamConfig) -> Any:
    """Build a fastmcp client transport for ``Client``.

    Args:
        config: Upstream configuration with transport type and
            connection details.

    Returns:
        A fastmcp transport object suitable for ``Client``.
    """
    if config.transport == "stdio":
        merged_env: dict[str, str] | None = None
        if config.env:
            merged_env = {**os.environ, **config.env}
        return StdioTransport(
            command=config.command or "",
            args=list(config.args),
            env=merged_env,
        )

    return config.url or ""


def _client_result_content(result: Any) -> list[dict[str, Any]]:
    """Normalize content blocks from an MCP client result.

    Handles raw dicts, Pydantic models with ``model_dump``,
    and fallback string coercion.

    Args:
        result: Raw MCP tool call result object.

    Returns:
        List of normalized content block dicts.
    """
    content_blocks = getattr(result, "content", None)
    if not isinstance(content_blocks, list):
        return []

    normalized: list[dict[str, Any]] = []
    for block in content_blocks:
        if isinstance(block, dict):
            normalized.append(dict(block))
            continue
        model_dump = getattr(block, "model_dump", None)
        if callable(model_dump):
            normalized.append(model_dump(by_alias=True, exclude_none=True))
            continue
        normalized.append({"type": "text", "text": str(block)})
    return normalized


def compute_upstream_instance_id(config: UpstreamConfig) -> str:
    """Compute a stable upstream instance identity hash.

    The identity includes transport type, stable endpoint data,
    prefix, and optional semantic salt values. It excludes
    rotating auth headers, tokens, and secret env values.

    Args:
        config: Upstream configuration.

    Returns:
        Truncated SHA-256 hex string (32 chars) suitable for
        cache keying.
    """
    identity: dict[str, Any] = {
        "transport": config.transport,
        "prefix": config.prefix,
    }
    if config.transport == "stdio":
        identity["command"] = config.command or ""
        identity["args"] = config.args
        # Include semantic salt env values (stable, non-secret)
        for key in sorted(config.semantic_salt_env_keys):
            val = config.env.get(key, "")
            identity[f"salt_env_{key}"] = val
    elif config.transport == "http":
        identity["url"] = config.url or ""
        # Include semantic salt header values (stable, non-secret)
        for key in sorted(config.semantic_salt_headers):
            val = config.headers.get(key, "")
            identity[f"salt_header_{key}"] = val

    return sha256_trunc(canonical_bytes(identity), 32)


def compute_auth_fingerprint(config: UpstreamConfig) -> str | None:
    """Compute an optional auth fingerprint for diagnostics.

    Hashes non-salt headers and env values. Excluded from the
    request identity used for caching.

    Args:
        config: Upstream configuration.

    Returns:
        Truncated SHA-256 hex string (16 chars), or ``None``
        when no auth-relevant values exist.
    """
    auth_values: dict[str, str] = {}

    if config.transport == "stdio":
        salt_keys = set(config.semantic_salt_env_keys)
        for key, val in sorted(config.env.items()):
            if key not in salt_keys:
                auth_values[f"env_{key}"] = val
    elif config.transport == "http":
        salt_keys = set(config.semantic_salt_headers)
        for key, val in sorted(config.headers.items()):
            if key not in salt_keys:
                auth_values[f"header_{key}"] = val

    if not auth_values:
        return None

    return sha256_trunc(canonical_bytes(auth_values), 16)


async def discover_tools(config: UpstreamConfig) -> list[UpstreamToolSchema]:
    """Fetch and normalize tool list from an upstream server.

    Args:
        config: Upstream configuration with transport details.

    Returns:
        List of ``UpstreamToolSchema`` descriptors with hashed
        input schemas.
    """
    transport = _client_transport(config)
    async with Client(transport, timeout=30.0) as client:
        tools = await client.list_tools()

    discovered: list[UpstreamToolSchema] = []
    for tool in tools:
        input_schema = dict(getattr(tool, "inputSchema", {}) or {})
        schema_hash = sha256_trunc(canonical_bytes(input_schema), 32)
        discovered.append(
            UpstreamToolSchema(
                name=str(getattr(tool, "name")),
                description=str(getattr(tool, "description", "") or ""),
                input_schema=input_schema,
                schema_hash=schema_hash,
            )
        )
    return discovered


async def connect_upstream(config: UpstreamConfig) -> UpstreamInstance:
    """Discover one upstream and build its runtime descriptor.

    Args:
        config: Upstream configuration.

    Returns:
        Immutable ``UpstreamInstance`` with discovered tools,
        computed instance ID, and optional auth fingerprint.
    """
    tools = await discover_tools(config)
    return UpstreamInstance(
        config=config,
        instance_id=compute_upstream_instance_id(config),
        tools=tools,
        auth_fingerprint=compute_auth_fingerprint(config),
    )


async def connect_upstreams(
    configs: list[UpstreamConfig],
) -> list[UpstreamInstance]:
    """Discover all configured upstreams sequentially.

    Args:
        configs: List of upstream configurations.

    Returns:
        List of connected ``UpstreamInstance`` descriptors in
        the same order as *configs*.
    """
    upstreams: list[UpstreamInstance] = []
    for config in configs:
        upstreams.append(await connect_upstream(config))
    return upstreams


async def call_upstream_tool(
    instance: UpstreamInstance,
    tool_name: str,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Call a tool on the upstream and return the raw response.

    Args:
        instance: Connected upstream runtime descriptor.
        tool_name: Name of the tool to invoke on the upstream.
        arguments: Forwarded tool arguments (reserved keys
            already stripped).

    Returns:
        Dict with ``content`` (list), ``structuredContent``,
        ``isError`` (bool), and ``meta`` keys.
    """
    transport = _client_transport(instance.config)
    async with Client(transport, timeout=30.0) as client:
        result = await client.call_tool(tool_name, arguments)

    normalized_content = _client_result_content(result)
    structured = getattr(result, "structured_content", None)
    is_error = bool(getattr(result, "is_error", False))
    meta = getattr(result, "meta", None)

    return {
        "content": normalized_content,
        "structuredContent": structured,
        "isError": is_error,
        "meta": meta if isinstance(meta, dict) else {},
    }
