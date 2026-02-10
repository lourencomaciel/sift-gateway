"""Upstream MCP client connections and tool discovery."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fastmcp import Client

from mcp_artifact_gateway.canon.rfc8785 import canonical_bytes
from mcp_artifact_gateway.config.settings import UpstreamConfig
from mcp_artifact_gateway.util.hashing import sha256_trunc


@dataclass(frozen=True)
class UpstreamToolSchema:
    """Discovered tool from an upstream."""

    name: str
    description: str
    input_schema: dict[str, Any]
    schema_hash: str  # sha256(canonical_bytes(input_schema))[:32]


@dataclass(frozen=True)
class UpstreamInstance:
    """Represents a connected upstream MCP server."""

    config: UpstreamConfig
    instance_id: str  # upstream_instance_id
    tools: list[UpstreamToolSchema]
    auth_fingerprint: str | None = None

    @property
    def prefix(self) -> str:
        return self.config.prefix


def _client_transport(config: UpstreamConfig) -> dict[str, Any]:
    """Build canonical MCP client transport config for fastmcp.Client."""
    if config.transport == "stdio":
        return {
            "command": config.command,
            "args": list(config.args),
            "env": dict(config.env),
            "transport": "stdio",
        }

    return {
        "url": config.url,
        "headers": dict(config.headers),
        "transport": "http",
    }


def _client_result_content(result: Any) -> list[dict[str, Any]]:
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
    """Compute upstream_instance_id = sha256(canonical_semantic_identity)[:32].

    Includes: transport, stable endpoint identity, prefix/name, optional semantic salt.
    Excludes: rotating auth headers, tokens, secret env values, private key paths.
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
    """Optional auth fingerprint for debugging (excluded from request identity).

    Hashes non-salt headers and env values for debugging only.
    Returns None if there are no auth-relevant values to fingerprint.
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
    """Fetch and normalize tool list from an upstream MCP server."""
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
    """Discover one upstream and return its immutable runtime descriptor."""
    tools = await discover_tools(config)
    return UpstreamInstance(
        config=config,
        instance_id=compute_upstream_instance_id(config),
        tools=tools,
        auth_fingerprint=compute_auth_fingerprint(config),
    )


async def connect_upstreams(configs: list[UpstreamConfig]) -> list[UpstreamInstance]:
    """Discover all configured upstreams sequentially."""
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

    Returns a dict with keys: content (list), isError (bool), optionally error details.
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
