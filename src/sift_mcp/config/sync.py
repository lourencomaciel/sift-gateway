"""Startup auto-sync for newly added MCP servers.

Reads sync metadata from the gateway config, parses the original
source config file, and imports any non-gateway MCP server entries
that were added since the last sync.
"""

from __future__ import annotations

from collections.abc import Iterator
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
def _suppress_os_error() -> Iterator[None]:
    """Suppress OSError during cleanup (e.g. unlinking a tmp)."""
    with contextlib.suppress(OSError):
        yield


logger = logging.getLogger(__name__)


def _is_sift_command(command: str) -> bool:
    """Return whether a command string invokes ``sift-mcp``."""
    command_name = Path(command).name.lower()
    return command_name in {"sift-mcp", "sift-mcp.exe"}


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

    cmd = server_config.get("command")
    return isinstance(cmd, str) and _is_sift_command(cmd)


def _ensure_gateway_data_dir_arg(
    entry: dict[str, Any],
    data_dir: Path,
) -> dict[str, Any]:
    """Ensure gateway command entry includes ``--data-dir <path>``."""
    if "url" in entry:
        return dict(entry)

    command = entry.get("command")
    if not isinstance(command, str) or not _is_sift_command(command):
        return dict(entry)

    updated = dict(entry)
    raw_args = updated.get("args")
    args = (
        [str(value) for value in raw_args] if isinstance(raw_args, list) else []
    )
    data_dir_str = str(data_dir)

    if "--data-dir" in args:
        idx = args.index("--data-dir")
        if idx + 1 < len(args):
            args[idx + 1] = data_dir_str
        else:
            args.append(data_dir_str)
    else:
        args.extend(["--data-dir", data_dir_str])

    updated["args"] = args
    return updated


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


def _resolve_gateway_config_redirect(
    *,
    data_dir: Path,
    config_path: Path,
    gw_config: dict[str, Any],
    initial_sync_meta: dict[str, Any],
) -> tuple[Path, Path, dict[str, Any]]:
    """Resolve optional metadata redirect to another gateway data dir."""
    gateway_data_dir = data_dir
    configured_data_dir = initial_sync_meta.get("data_dir")
    if not isinstance(configured_data_dir, str):
        return gateway_data_dir, config_path, gw_config

    candidate_data_dir = Path(configured_data_dir).expanduser().resolve()
    candidate_config_path = candidate_data_dir / STATE_SUBDIR / CONFIG_FILENAME
    if candidate_config_path == config_path:
        return candidate_data_dir, config_path, gw_config
    if not candidate_config_path.is_file():
        return gateway_data_dir, config_path, gw_config

    configured_gw_config = _load_config(candidate_config_path)
    if not configured_gw_config:
        return gateway_data_dir, config_path, gw_config
    return candidate_data_dir, candidate_config_path, configured_gw_config


def _new_non_gateway_servers(
    *,
    source_servers: dict[str, dict[str, Any]],
    gw_servers: dict[str, dict[str, Any]],
    gateway_name: str,
) -> dict[str, dict[str, Any]]:
    """Return source servers that are new and not the gateway entry."""
    new_servers: dict[str, dict[str, Any]] = {}
    for name, entry in source_servers.items():
        if _is_gateway_entry(name, entry, gateway_name):
            continue
        if name in gw_servers:
            continue
        new_servers[name] = entry
    return new_servers


def _build_gateway_only_source_config(
    *,
    source_raw: dict[str, Any],
    source_servers: dict[str, dict[str, Any]],
    gateway_name: str,
    gateway_data_dir: Path,
) -> dict[str, Any]:
    """Build source config that preserves only the gateway entry."""
    is_vscode = "mcpServers" not in source_raw and isinstance(
        source_raw.get("mcp"), dict
    )
    is_zed = isinstance(source_raw.get("context_servers"), dict)

    gw_entry: dict[str, Any] | None = None
    for sname, sconf in source_servers.items():
        if _is_gateway_entry(sname, sconf, gateway_name):
            gw_entry = sconf
            break

    if gw_entry is None:
        gw_entry = {
            "command": "sift-mcp",
            "args": ["--data-dir", str(gateway_data_dir)],
        }
    else:
        gw_entry = _ensure_gateway_data_dir_arg(gw_entry, gateway_data_dir)

    new_source = dict(source_raw)
    if is_vscode:
        new_source["mcp"] = {"servers": {gateway_name: gw_entry}}
        return new_source
    if is_zed:
        context_servers = source_raw.get("context_servers", {})
        zed_entry: dict[str, Any] = {
            "source": "custom",
            "command": "sift-mcp",
            "args": [],
        }
        if isinstance(context_servers, dict) and isinstance(
            context_servers.get(gateway_name), dict
        ):
            zed_entry = dict(context_servers[gateway_name])
        new_source["context_servers"] = {gateway_name: zed_entry}
        return new_source
    new_source["mcpServers"] = {gateway_name: gw_entry}
    return new_source


def _load_sync_context(
    data_dir: Path,
) -> (
    tuple[Path, Path, dict[str, Any], Path, str]
    | dict[str, Any]
):
    """Load gateway config + sync metadata required for run_sync."""
    config_path = data_dir / STATE_SUBDIR / CONFIG_FILENAME
    gw_config = _load_config(config_path)
    initial_sync_meta = gw_config.get("_gateway_sync")
    if not isinstance(initial_sync_meta, dict):
        return {"synced": 0}

    gateway_data_dir, config_path, gw_config = (
        _resolve_gateway_config_redirect(
            data_dir=data_dir,
            config_path=config_path,
            gw_config=gw_config,
            initial_sync_meta=initial_sync_meta,
        )
    )
    sync_meta = gw_config.get("_gateway_sync")
    if not isinstance(sync_meta, dict):
        sync_meta = initial_sync_meta
    if not sync_meta.get("enabled", False):
        return {"synced": 0}

    source_path_str = sync_meta.get("source_path")
    if not source_path_str:
        return {"synced": 0}
    source_path = Path(source_path_str)
    gateway_name = sync_meta.get("gateway_name", "artifact-gateway")

    normalized_sync_meta = dict(sync_meta)
    normalized_sync_meta["data_dir"] = str(gateway_data_dir)
    gw_config["_gateway_sync"] = normalized_sync_meta
    return gateway_data_dir, config_path, gw_config, source_path, gateway_name


def _read_sync_source_config(
    source_path: Path,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Read source JSON config for sync, returning warning payload on failure."""
    if not source_path.exists():
        warning = f"Sync source file not found: {source_path}"
        logger.warning(warning)
        return None, {"synced": 0, "warning": warning}
    try:
        source_raw = json.loads(source_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        warning = f"Cannot read sync source: {source_path}: {exc}"
        logger.warning(warning)
        return None, {"synced": 0, "warning": warning}
    if not isinstance(source_raw, dict):
        warning = f"Sync source is not a JSON object: {source_path}"
        logger.warning(warning)
        return None, {"synced": 0, "warning": warning}
    return source_raw, None


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
    context = _load_sync_context(data_dir)
    if isinstance(context, dict):
        return context
    (
        gateway_data_dir,
        config_path,
        gw_config,
        source_path,
        gateway_name,
    ) = context

    source_raw, source_error = _read_sync_source_config(source_path)
    if source_error is not None:
        return source_error
    if source_raw is None:
        return {"synced": 0}

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
    new_servers = _new_non_gateway_servers(
        source_servers=source_servers,
        gw_servers=gw_servers,
        gateway_name=gateway_name,
    )

    if not new_servers:
        return {"synced": 0}

    # Import new servers, externalizing secrets
    for name, entry in new_servers.items():
        entry = _externalize_secrets_for_server(gateway_data_dir, name, entry)
        gw_servers[name] = entry

    gw_config["mcpServers"] = gw_servers

    new_source = _build_gateway_only_source_config(
        source_raw=source_raw,
        source_servers=source_servers,
        gateway_name=gateway_name,
        gateway_data_dir=gateway_data_dir,
    )

    # Persist changes (backup source first for safety)
    backup_path = source_path.with_suffix(source_path.suffix + ".sync-backup")
    shutil.copy2(source_path, backup_path)
    os.chmod(backup_path, 0o600)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    _write_json(config_path, gw_config)
    _write_json(source_path, new_source)

    return {"synced": len(new_servers)}
