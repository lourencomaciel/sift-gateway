"""Add upstream MCP servers from a JSON snippet.

Usage::

    sift-gateway upstream add '{"github": {"command": "npx", ...}}'

Accepts the same ``mcpServers`` format used by Claude Desktop,
Cursor, and VS Code.  Secrets (``env``, ``headers``) are
automatically externalized to per-upstream secret files.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from sift_gateway.config.mcp_servers import (
    _infer_transport,
    extract_mcp_servers,
)
from sift_gateway.config.shared import (
    ensure_gateway_config_path,
    gateway_config_path,
    load_gateway_config_dict,
    write_json,
)
from sift_gateway.config.upstream_secrets import (
    validate_prefix,
    write_secret,
)
from sift_gateway.constants import DEFAULT_DATA_DIR


def _externalize_secrets_for_server(
    data_dir: Path,
    name: str,
    entry: dict[str, Any],
) -> dict[str, Any]:
    """Externalize inline secrets for a single server entry.

    If the entry has ``env`` or ``headers``, writes them to a
    secret file and replaces them with ``_gateway.secret_ref``.

    Args:
        data_dir: Root data directory for Sift state.
        name: Server name used as the secret prefix.
        entry: Server config dict (modified in place).

    Returns:
        The modified entry dict.
    """
    env = entry.get("env")
    headers = entry.get("headers")
    if not env and not headers:
        return entry

    transport = "http" if "url" in entry else "stdio"
    write_secret(
        data_dir,
        name,
        transport=transport,
        env=env if env else None,
        headers=headers if headers else None,
    )

    gateway_ext = entry.get("_gateway", {})
    if not isinstance(gateway_ext, dict):
        gateway_ext = {}
    gateway_ext["secret_ref"] = name
    entry["_gateway"] = gateway_ext

    entry.pop("env", None)
    entry.pop("headers", None)
    return entry


def _validate_gateway_block(name: str, entry: dict[str, Any]) -> None:
    """Reject non-dict ``_gateway`` values.

    Args:
        name: Server name (for error messages).
        entry: Server config dict.

    Raises:
        ValueError: If ``_gateway`` is present but not a dict.
    """
    gw = entry.get("_gateway")
    if gw is not None and not isinstance(gw, dict):
        msg = (
            f"server '{name}' _gateway must be a JSON object, "
            f"got {type(gw).__name__}"
        )
        raise ValueError(msg)


def _validate_transport_values(
    name: str,
    entry: dict[str, Any],
) -> None:
    """Reject empty or non-string ``command`` / ``url`` values.

    Args:
        name: Server name (for error messages).
        entry: Server config dict.

    Raises:
        ValueError: If ``command`` or ``url`` is present but not
            a non-empty string.
    """
    if "command" in entry:
        cmd = entry["command"]
        if not isinstance(cmd, str) or not cmd:
            msg = f"server '{name}' command must be a non-empty string"
            raise ValueError(msg)
    if "url" in entry:
        url = entry["url"]
        if not isinstance(url, str) or not url:
            msg = f"server '{name}' url must be a non-empty string"
            raise ValueError(msg)


def _validate_secret_shapes(name: str, entry: dict[str, Any]) -> None:
    """Reject non-dict ``env`` and ``headers`` values.

    Args:
        name: Server name (for error messages).
        entry: Server config dict.

    Raises:
        ValueError: If ``env`` or ``headers`` is present but
            not a dict.
    """
    env = entry.get("env")
    if env is not None and not isinstance(env, dict):
        msg = (
            f"server '{name}' env must be a JSON object, "
            f"got {type(env).__name__}"
        )
        raise ValueError(msg)
    headers = entry.get("headers")
    if headers is not None and not isinstance(headers, dict):
        msg = (
            f"server '{name}' headers must be a JSON object, "
            f"got {type(headers).__name__}"
        )
        raise ValueError(msg)


def _resolve_data_dir(data_dir: Path | None) -> Path:
    """Resolve effective data directory from argument/env/default."""
    if data_dir is not None:
        return data_dir
    env_dir = os.environ.get("SIFT_GATEWAY_DATA_DIR")
    return Path(env_dir if env_dir else DEFAULT_DATA_DIR).resolve()


def _normalize_servers_input(
    servers: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Accept wrapped snippets and normalize to bare server map."""
    is_wrapped = "mcpServers" in servers or (
        isinstance(servers.get("mcp"), dict) and "servers" in servers["mcp"]
    )
    if is_wrapped:
        return extract_mcp_servers(servers)
    return servers


def _validate_servers_for_add(servers: dict[str, dict[str, Any]]) -> None:
    """Validate server entries before any write side effects."""
    for name, entry in servers.items():
        if not isinstance(entry, dict):
            msg = f"server '{name}' config must be a JSON object"
            raise ValueError(msg)
        validate_prefix(name)
        _infer_transport(name, entry)
        _validate_transport_values(name, entry)
        _validate_gateway_block(name, entry)
        _validate_secret_shapes(name, entry)


def _partition_added_and_skipped(
    *,
    servers: dict[str, dict[str, Any]],
    existing_servers: dict[str, Any],
) -> tuple[list[str], list[str]]:
    """Partition requested servers into addable and already-existing."""
    added: list[str] = []
    skipped: list[str] = []
    for name in servers:
        if name in existing_servers:
            skipped.append(name)
            continue
        added.append(name)
    return added, skipped


def run_upstream_add(
    servers: dict[str, dict[str, Any]],
    *,
    data_dir: Path | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Add one or more upstream MCP servers to the gateway config.

    Validates each server entry, externalizes secrets, and merges
    into the existing ``state/config.json``.

    Args:
        servers: Dict mapping server name to config, using the
            standard ``mcpServers`` entry format (same as Claude
            Desktop / Cursor / VS Code).
        data_dir: Gateway data directory.  Defaults to
            ``.sift-gateway``.
        dry_run: If True, validate and report without writing.

    Returns:
        Summary dict with keys ``added`` (list of names),
        ``skipped`` (list of already-existing names), and
        ``config_path``.

    Raises:
        ValueError: If a server entry is invalid (missing
            ``command``/``url``, has both, or has an invalid
            name).
    """
    data_dir = _resolve_data_dir(data_dir)
    servers = _normalize_servers_input(servers)

    if not servers:
        msg = "no servers provided in snippet"
        raise ValueError(msg)

    _validate_servers_for_add(servers)

    # Compute config path; only create dirs when writing
    config_path = gateway_config_path(data_dir)
    gw_config = load_gateway_config_dict(config_path)

    existing_servers = gw_config.get("mcpServers", {})
    if not isinstance(existing_servers, dict):
        existing_servers = {}

    added, skipped = _partition_added_and_skipped(
        servers=servers,
        existing_servers=existing_servers,
    )

    if not dry_run:
        ensure_gateway_config_path(data_dir)
        for name in added:
            entry = dict(servers[name])  # shallow copy
            entry = _externalize_secrets_for_server(data_dir, name, entry)
            existing_servers[name] = entry

        gw_config["mcpServers"] = existing_servers
        gw_config.pop("upstreams", None)
        write_json(config_path, gw_config)

    return {
        "added": sorted(added),
        "skipped": sorted(skipped),
        "config_path": str(config_path),
    }


def print_add_summary(
    summary: dict[str, Any],
    *,
    dry_run: bool = False,
) -> None:
    """Print a human-readable summary of the add operation.

    Args:
        summary: Result dict from ``run_upstream_add``.
        dry_run: Whether this was a dry-run invocation.
    """
    prefix = "[dry run] " if dry_run else ""
    added = summary["added"]
    skipped = summary["skipped"]

    if added:
        print(f"{prefix}Added {len(added)} upstream(s):")
        for name in added:
            print(f"  + {name}")
    else:
        print(f"{prefix}No new upstreams added.")

    if skipped:
        print(f"{prefix}Skipped {len(skipped)} (already exist):")
        for name in skipped:
            print(f"  - {name}")

    print(f"{prefix}Config: {summary['config_path']}")
