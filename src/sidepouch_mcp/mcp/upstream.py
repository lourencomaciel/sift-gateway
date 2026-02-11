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

from dataclasses import dataclass
import os
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
    secret_data: dict | None = None

    @property
    def prefix(self) -> str:
        """The upstream namespace prefix."""
        return self.config.prefix


_UNSET: dict = {}  # Sentinel for "not provided" secret_data

_ENV_ALLOWLIST: frozenset[str] = frozenset(
    {
        "PATH",
        "HOME",
        "LANG",
        "LC_ALL",
        "TMPDIR",
        "TMP",
        "TEMP",
        "USER",
        "LOGNAME",
        "SHELL",
    }
)


def _build_stdio_env(
    config: UpstreamConfig,
    data_dir: str | None = None,
    *,
    secret: dict | None = _UNSET,
) -> dict[str, str]:
    """Build isolated environment for a stdio upstream.

    Composes a minimal environment from an allowlist of safe
    parent env vars, optional secret ref values, and explicit
    config env overrides.

    Args:
        config: Upstream configuration with env, secret_ref,
            and inherit_parent_env fields.
        data_dir: Root data directory for secret file lookup.
            Falls back to ``DEFAULT_DATA_DIR`` when ``None``.
        secret: Pre-resolved secret data dict, or sentinel
            ``_UNSET`` to resolve on demand.

    Returns:
        Merged environment dict for subprocess execution.
    """
    if config.inherit_parent_env:
        base: dict[str, str] = dict(os.environ)
    else:
        base = {k: v for k, v in os.environ.items() if k in _ENV_ALLOWLIST}

    if secret is _UNSET:
        secret = _resolve_secret_data(config, data_dir)
    if secret:
        secret_env: dict[str, str] = secret.get("env") or {}
        base.update(secret_env)

    base.update(config.env)
    return base


def _resolve_secret_data(
    config: UpstreamConfig,
    data_dir: str | None = None,
) -> dict | None:
    """Load secret file contents for an upstream, if configured.

    Args:
        config: Upstream configuration with optional secret_ref.
        data_dir: Root data directory for secret file lookup.

    Returns:
        Parsed secret dict, or ``None`` when no secret_ref is
        set.
    """
    if not config.secret_ref:
        return None
    from sidepouch_mcp.config.upstream_secrets import (
        resolve_secret_ref,
    )
    from sidepouch_mcp.constants import DEFAULT_DATA_DIR

    resolved_dir = data_dir or DEFAULT_DATA_DIR
    return resolve_secret_ref(resolved_dir, config.secret_ref)


def _build_http_headers(
    config: UpstreamConfig,
    data_dir: str | None = None,
    *,
    secret: dict | None = _UNSET,
) -> dict[str, str]:
    """Build merged HTTP headers for an HTTP upstream.

    Merges secret file headers (if ``secret_ref`` is set) with
    inline config headers.  Config headers take precedence.

    Args:
        config: Upstream configuration.
        data_dir: Root data directory for secret file lookup.
        secret: Pre-resolved secret data dict, or sentinel
            ``_UNSET`` to resolve on demand.

    Returns:
        Merged headers dict.
    """
    base: dict[str, str] = {}
    if secret is _UNSET:
        secret = _resolve_secret_data(config, data_dir)
    if secret:
        secret_headers: dict[str, str] = secret.get("headers") or {}
        base.update(secret_headers)
    base.update(config.headers)
    return base


def _client_transport(
    config: UpstreamConfig,
    data_dir: str | None = None,
    *,
    secret: dict | None = _UNSET,
) -> Any:
    """Build a fastmcp client transport for ``Client``.

    Args:
        config: Upstream configuration with transport type and
            connection details.
        data_dir: Root data directory for secret file lookup.
            Passed through to ``_build_stdio_env``.
        secret: Pre-resolved secret data dict, or sentinel
            ``_UNSET`` to resolve on demand.

    Returns:
        A fastmcp transport object suitable for ``Client``.
    """
    if config.transport == "stdio":
        env = _build_stdio_env(config, data_dir, secret=secret)
        return StdioTransport(
            command=config.command or "",
            args=list(config.args),
            env=env,
        )

    # HTTP transport — merge headers from secret_ref
    headers = _build_http_headers(config, data_dir, secret=secret)
    url = config.url or ""
    if headers:
        from fastmcp.mcp_config import (
            infer_transport_type_from_url,
        )

        inferred = infer_transport_type_from_url(url)
        if inferred == "sse":
            from fastmcp.client.transports import (
                SSETransport,
            )

            return SSETransport(url=url, headers=headers)
        from fastmcp.client.transports import (
            StreamableHttpTransport,
        )

        return StreamableHttpTransport(url=url, headers=headers)
    return url


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


def compute_upstream_instance_id(
    config: UpstreamConfig,
    data_dir: str | None = None,
) -> str:
    """Compute a stable upstream instance identity hash.

    The identity includes transport type, stable endpoint data,
    prefix, and optional semantic salt values.  When secrets are
    externalized via ``secret_ref``, salt values are read from the
    secret file so the identity remains correct.

    Args:
        config: Upstream configuration.
        data_dir: Root data directory for resolving secret refs.

    Returns:
        Truncated SHA-256 hex string (32 chars) suitable for
        cache keying.
    """
    identity: dict[str, Any] = {
        "transport": config.transport,
        "prefix": config.prefix,
    }
    secret = _resolve_secret_data(config, data_dir)
    if config.transport == "stdio":
        identity["command"] = config.command or ""
        identity["args"] = config.args
        # Include semantic salt env values (stable, non-secret)
        # Check both inline config.env and secret file env
        secret_env: dict[str, str] = (secret.get("env") or {}) if secret else {}
        for key in sorted(config.semantic_salt_env_keys):
            val = config.env.get(key, secret_env.get(key, ""))
            identity[f"salt_env_{key}"] = val
    elif config.transport == "http":
        identity["url"] = config.url or ""
        secret_hdrs: dict[str, str] = (
            (secret.get("headers") or {}) if secret else {}
        )
        for key in sorted(config.semantic_salt_headers):
            val = config.headers.get(key, secret_hdrs.get(key, ""))
            identity[f"salt_header_{key}"] = val

    return sha256_trunc(canonical_bytes(identity), 32)


def compute_auth_fingerprint(
    config: UpstreamConfig,
    data_dir: str | None = None,
) -> str | None:
    """Compute an optional auth fingerprint for diagnostics.

    Hashes non-salt headers and env values, including values
    from externalized secret files.

    Args:
        config: Upstream configuration.
        data_dir: Root data directory for resolving secret refs.

    Returns:
        Truncated SHA-256 hex string (16 chars), or ``None``
        when no auth-relevant values exist.
    """
    auth_values: dict[str, str] = {}
    secret = _resolve_secret_data(config, data_dir)

    if config.transport == "stdio":
        salt_keys = set(config.semantic_salt_env_keys)
        # Merge inline + secret env for fingerprinting
        all_env: dict[str, str] = {}
        if secret:
            all_env.update(secret.get("env") or {})
        all_env.update(config.env)
        for key, val in sorted(all_env.items()):
            if key not in salt_keys:
                auth_values[f"env_{key}"] = val
    elif config.transport == "http":
        salt_keys = set(config.semantic_salt_headers)
        all_headers: dict[str, str] = {}
        if secret:
            all_headers.update(secret.get("headers") or {})
        all_headers.update(config.headers)
        for key, val in sorted(all_headers.items()):
            if key not in salt_keys:
                auth_values[f"header_{key}"] = val

    if not auth_values:
        return None

    return sha256_trunc(canonical_bytes(auth_values), 16)


async def discover_tools(
    config: UpstreamConfig,
    data_dir: str | None = None,
) -> list[UpstreamToolSchema]:
    """Fetch and normalize tool list from an upstream server.

    Args:
        config: Upstream configuration with transport details.
        data_dir: Root data directory for resolving secret refs.

    Returns:
        List of ``UpstreamToolSchema`` descriptors with hashed
        input schemas.
    """
    transport = _client_transport(config, data_dir)
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


async def connect_upstream(
    config: UpstreamConfig,
    data_dir: str | None = None,
) -> UpstreamInstance:
    """Discover one upstream and build its runtime descriptor.

    Args:
        config: Upstream configuration.
        data_dir: Root data directory for resolving secret refs.

    Returns:
        Immutable ``UpstreamInstance`` with discovered tools,
        computed instance ID, and optional auth fingerprint.
    """
    tools = await discover_tools(config, data_dir)
    return UpstreamInstance(
        config=config,
        instance_id=compute_upstream_instance_id(config, data_dir),
        tools=tools,
        auth_fingerprint=compute_auth_fingerprint(config, data_dir),
        secret_data=_resolve_secret_data(config, data_dir),
    )


async def connect_upstreams(
    configs: list[UpstreamConfig],
    data_dir: str | None = None,
) -> list[UpstreamInstance]:
    """Discover all configured upstreams sequentially.

    Args:
        configs: List of upstream configurations.
        data_dir: Root data directory for resolving secret refs.

    Returns:
        List of connected ``UpstreamInstance`` descriptors in
        the same order as *configs*.
    """
    upstreams: list[UpstreamInstance] = []
    for config in configs:
        upstreams.append(await connect_upstream(config, data_dir))
    return upstreams


async def call_upstream_tool(
    instance: UpstreamInstance,
    tool_name: str,
    arguments: dict[str, Any],
    data_dir: str | None = None,
) -> dict[str, Any]:
    """Call a tool on the upstream and return the raw response.

    Args:
        instance: Connected upstream runtime descriptor.
        tool_name: Name of the tool to invoke on the upstream.
        arguments: Forwarded tool arguments (reserved keys
            already stripped).
        data_dir: Root data directory for resolving secret refs.

    Returns:
        Dict with ``content`` (list), ``structuredContent``,
        ``isError`` (bool), and ``meta`` keys.
    """
    transport = _client_transport(
        instance.config, data_dir, secret=instance.secret_data,
    )
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
