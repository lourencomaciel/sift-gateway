"""Startup auto-sync for newly added MCP servers.

Reads sync metadata from the gateway config, parses the original
source config file, and imports any non-gateway MCP server entries
that were added since the last sync.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
from pathlib import Path
import shutil
from typing import Any

from sift_mcp.config.mcp_servers import (
    extract_mcp_servers,
)
from sift_mcp.config.upstream_secrets import write_secret
from sift_mcp.constants import (
    CONFIG_FILENAME,
    STATE_SUBDIR,
)


@contextlib.contextmanager
def _suppress_os_error():
    """Suppress OSError during cleanup (e.g. unlinking a tmp)."""
    try:
        yield
    except OSError:
        pass

logger = logging.getLogger(__name__)


def _is_gateway_entry(
    name: str,
    server_config: dict[str, Any],
    gateway_name: str,
) -> bool:
    """Check whether a server entry represents the gateway.

    Args:
        name: Server name from the config.
        server_config: Server configuration dict.
        gateway_name: Expected gateway name from sync
            metadata.

    Returns:
        True if the entry is the gateway itself.
    """
    if name == gateway_name:
        return True

    if not isinstance(server_config, dict):
        return False

    cmd = server_config.get("command", "")
    if cmd == "sift-mcp":
        return True

    return False


def _load_config(config_path: Path) -> dict[str, Any]:
    """Load a JSON config file, returning empty dict on error.

    Args:
        config_path: Path to JSON file.

    Returns:
        Parsed dict, or empty dict if file is missing or
        invalid.
    """
    if not config_path.exists():
        return {}
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    if not isinstance(raw, dict):
        return {}
    return raw


def _write_json(path: Path, data: dict[str, Any]) -> None:
    """Atomically write JSON with consistent formatting.

    Writes to a temporary file in the same directory, then
    renames into place so readers never see a partial file.

    Args:
        path: Destination file path.
        data: Dict to serialize.
    """
    import tempfile

    content = json.dumps(data, indent=2, ensure_ascii=False) + "\n"
    fd, tmp = tempfile.mkstemp(
        dir=str(path.parent),
        suffix=".tmp",
    )
    try:
        os.write(fd, content.encode("utf-8"))
        os.close(fd)
        fd = -1
        os.replace(tmp, str(path))
    except BaseException:
        if fd >= 0:
            os.close(fd)
        with _suppress_os_error():
            os.unlink(tmp)
        raise


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
        name: Server/upstream name used as the secret prefix.
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


def run_sync(data_dir: str | Path) -> dict[str, Any]:
    """Auto-sync newly added MCP servers from the source config.

    Reads the gateway config's ``_gateway_sync`` metadata, checks
    the original source config for new MCP server entries, imports
    them into the gateway config, and rewrites the source to
    contain only the gateway entry.

    Args:
        data_dir: Root data directory for Sift state.

    Returns:
        Dict with ``synced`` count and optional ``warning``.
    """
    data_dir = Path(data_dir).resolve()
    config_path = data_dir / STATE_SUBDIR / CONFIG_FILENAME

    gw_config = _load_config(config_path)
    sync_meta = gw_config.get("_gateway_sync")

    if not isinstance(sync_meta, dict):
        return {"synced": 0}

    if not sync_meta.get("enabled", False):
        return {"synced": 0}

    source_path_str = sync_meta.get("source_path")
    if not source_path_str:
        return {"synced": 0}

    source_path = Path(source_path_str)
    gateway_name = sync_meta.get("gateway_name", "artifact-gateway")

    # Read the source config file
    if not source_path.exists():
        warning = f"Sync source file not found: {source_path}"
        logger.warning(warning)
        return {"synced": 0, "warning": warning}

    try:
        source_raw = json.loads(source_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        warning = f"Cannot read sync source: {source_path}: {exc}"
        logger.warning(warning)
        return {"synced": 0, "warning": warning}

    if not isinstance(source_raw, dict):
        warning = f"Sync source is not a JSON object: {source_path}"
        logger.warning(warning)
        return {"synced": 0, "warning": warning}

    # Extract servers from source
    try:
        source_servers = extract_mcp_servers(source_raw)
    except ValueError:
        return {"synced": 0}

    if not source_servers:
        return {"synced": 0}

    # Get existing gateway servers
    gw_servers = gw_config.get("mcpServers", {})
    if not isinstance(gw_servers, dict):
        gw_servers = {}

    # Find new non-gateway servers not already imported
    new_servers: dict[str, dict[str, Any]] = {}
    for name, entry in source_servers.items():
        if _is_gateway_entry(name, entry, gateway_name):
            continue
        if name in gw_servers:
            continue
        new_servers[name] = entry

    if not new_servers:
        return {"synced": 0}

    # Import new servers, externalizing secrets
    for name, entry in new_servers.items():
        entry = _externalize_secrets_for_server(data_dir, name, entry)
        gw_servers[name] = entry

    gw_config["mcpServers"] = gw_servers

    # Rewrite source to keep only the gateway entry
    is_vscode = "mcpServers" not in source_raw and isinstance(
        source_raw.get("mcp"), dict
    )

    # Find the gateway entry to preserve
    gw_entry: dict[str, Any] | None = None
    for sname, sconf in source_servers.items():
        if _is_gateway_entry(sname, sconf, gateway_name):
            gw_entry = sconf
            break

    if gw_entry is None:
        gw_entry = {"command": "sift-mcp"}

    new_source = dict(source_raw)
    if is_vscode:
        new_source["mcp"] = {
            "servers": {gateway_name: gw_entry},
        }
    else:
        new_source["mcpServers"] = {
            gateway_name: gw_entry,
        }

    # Persist changes (backup source first for safety)
    backup_path = source_path.with_suffix(source_path.suffix + ".sync-backup")
    shutil.copy2(source_path, backup_path)
    os.chmod(backup_path, 0o600)
    _write_json(config_path, gw_config)
    _write_json(source_path, new_source)

    return {"synced": len(new_servers)}
