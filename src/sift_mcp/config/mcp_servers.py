"""Parse standard MCP client config formats into UpstreamConfig objects.

Supports the standard config formats used by Claude Desktop, Cursor,
Claude Code, VS Code, and Zed. Users can copy-paste their
existing MCP server config or use ``sift-mcp init --from <file>`` to
migrate automatically.

Gateway config format::

    {
      "mcpServers": {
        "github": {
          "command": "npx",
          "args": ["-y", "@modelcontextprotocol/server-github"],
          "env": {"GITHUB_TOKEN": "..."},
          "_gateway": {"semantic_salt_env_keys": ["GITHUB_ORG"]}
        }
      }
    }
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

# Gateway extension fields that live under ``_gateway`` in each server entry.
_GATEWAY_EXTENSION_FIELDS = frozenset(
    {
        "semantic_salt_headers",
        "semantic_salt_env_keys",
        "passthrough_allowed",
        "pagination",
        "auto_paginate_max_pages",
        "auto_paginate_max_records",
        "auto_paginate_timeout_seconds",
        "secret_ref",
        "inherit_parent_env",
        "external_user_id",
    }
)


def extract_mcp_servers(raw: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Extract server definitions from a raw config dict.

    Supports three formats:
    - ``mcpServers`` key (Claude Desktop, Cursor, Claude Code)
    - ``mcp.servers`` nested key (VS Code)
    - ``context_servers`` key (Zed)

    Returns a dict mapping server name to server config.
    """
    # Claude Desktop / Cursor / Claude Code format
    if "mcpServers" in raw:
        servers = raw["mcpServers"]
        if not isinstance(servers, dict):
            msg = "'mcpServers' must be a JSON object"
            raise ValueError(msg)
        return dict(servers)

    # VS Code format: { "mcp": { "servers": { ... } } }
    mcp_block = raw.get("mcp")
    if isinstance(mcp_block, dict):
        servers = mcp_block.get("servers")
        if isinstance(servers, dict):
            return dict(servers)

    # Zed format: { "context_servers": { ... } }
    zed_servers = _extract_zed_context_servers(raw)
    if zed_servers:
        return zed_servers

    return {}


def _extract_zed_context_servers(
    raw: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    """Extract and normalize Zed's ``context_servers`` config format."""
    servers = raw.get("context_servers")
    if not isinstance(servers, dict):
        return {}

    normalized: dict[str, dict[str, Any]] = {}
    for name, entry in servers.items():
        if not isinstance(entry, dict):
            msg = f"zed context server '{name}' must be a JSON object"
            raise ValueError(msg)

        normalized[name] = _normalize_zed_server_entry(name, entry)

    return normalized


def _normalize_zed_server_entry(
    name: str, entry: dict[str, Any]
) -> dict[str, Any]:
    """Convert a single Zed server entry to mcpServers-compatible shape."""
    if "url" in entry:
        if not isinstance(entry["url"], str):
            msg = f"zed context server '{name}' url must be a string"
            raise ValueError(msg)
        result: dict[str, Any] = {"url": entry["url"]}
        headers = entry.get("headers")
        if headers is not None:
            result["headers"] = headers
        return result

    cmd = entry.get("command")
    if isinstance(cmd, str):
        result = {"command": cmd}
        if "args" in entry:
            result["args"] = entry["args"]
        if "env" in entry:
            result["env"] = entry["env"]
        return result

    if isinstance(cmd, dict):
        path = cmd.get("path")
        if not isinstance(path, str):
            msg = f"zed context server '{name}' command.path must be a string"
            raise ValueError(msg)

        result = {"command": path}
        if "args" in cmd:
            result["args"] = cmd["args"]
        if "env" in cmd:
            result["env"] = cmd["env"]
        return result

    msg = (
        f"zed context server '{name}' must define either "
        "'url', 'command' string, or 'command.path'"
    )
    raise ValueError(msg)


def read_config_file(path: Path) -> dict[str, Any]:
    """Read and parse a JSON config file."""
    if not path.exists():
        msg = f"config file not found: {path}"
        raise FileNotFoundError(msg)

    text = path.read_text(encoding="utf-8")
    raw = json.loads(text)
    if not isinstance(raw, dict):
        msg = f"config file must contain a JSON object: {path}"
        raise ValueError(msg)
    return raw


def _infer_transport(name: str, entry: dict[str, Any]) -> str:
    """Infer transport type from server entry fields."""
    has_command = "command" in entry
    has_url = "url" in entry

    if has_command and has_url:
        msg = f"server '{name}' has both 'command' and 'url'; specify only one"
        raise ValueError(msg)
    if has_command:
        return "stdio"
    if has_url:
        return "http"

    msg = f"server '{name}' has neither 'command' nor 'url'; one is required"
    raise ValueError(msg)


def to_upstream_configs(
    servers: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Convert server definitions to UpstreamConfig-compatible dicts.

    Returns a list of dicts ready to be passed to ``UpstreamConfig(**d)``.
    Transport is inferred from whether ``command`` or ``url`` is present.
    The ``_gateway`` block is extracted and its fields are promoted to
    top-level UpstreamConfig fields.
    """
    configs: list[dict[str, Any]] = []

    for name, entry in servers.items():
        if not isinstance(entry, dict):
            msg = f"server '{name}' config must be a JSON object"
            raise ValueError(msg)

        # Extract _gateway extensions
        gateway_ext = entry.get("_gateway", {})
        if not isinstance(gateway_ext, dict):
            msg = f"server '{name}' _gateway must be a JSON object"
            raise ValueError(msg)

        # Build UpstreamConfig-compatible dict
        transport = _infer_transport(name, entry)
        config: dict[str, Any] = {
            "prefix": name,
            "transport": transport,
        }

        # Copy transport fields
        if transport == "stdio":
            config["command"] = entry["command"]
            if "args" in entry:
                config["args"] = entry["args"]
            if "env" in entry:
                config["env"] = entry["env"]
        else:
            config["url"] = entry["url"]
            if "headers" in entry:
                config["headers"] = entry["headers"]

        # Promote _gateway extension fields to top level
        for field in _GATEWAY_EXTENSION_FIELDS:
            if field in gateway_ext:
                config[field] = gateway_ext[field]

        configs.append(config)

    return configs


def resolve_mcp_servers_config(
    raw: dict[str, Any],
) -> list[dict[str, Any]] | None:
    """Main entry point: resolve mcpServers into UpstreamConfig dicts.

    Returns None if the config doesn't use the mcpServers format.
    """
    has_mcp_servers = "mcpServers" in raw
    has_vscode = isinstance(raw.get("mcp"), dict) and "servers" in raw.get(
        "mcp", {}
    )
    has_zed = isinstance(raw.get("context_servers"), dict)

    if not has_mcp_servers and not has_vscode and not has_zed:
        return None

    servers = extract_mcp_servers(raw)
    return to_upstream_configs(servers)
