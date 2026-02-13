r"""Migrate MCP server config from an external tool into the gateway.

Usage::

    sift-mcp init --from ~/Library/Application\ Support\
        /Claude/claude_desktop_config.json

This command:
1. Reads the source file and extracts ``mcpServers``
2. Copies those servers into the gateway's own config
3. Backs up the source file to ``<file>.backup``
4. Rewrites the source file with only the gateway as the MCP server

Use ``sift-mcp init --from <file> --revert`` to restore the backup.
"""

from __future__ import annotations

import contextlib
import json
import os
from pathlib import Path
import shutil
from typing import Any, Literal


@contextlib.contextmanager
def _suppress_os_error():
    """Suppress OSError during cleanup (e.g. unlinking a tmp)."""
    try:
        yield
    except OSError:
        pass


from sift_mcp.config.mcp_servers import (
    extract_mcp_servers,
    read_config_file,
)
from sift_mcp.config.upstream_secrets import write_secret
from sift_mcp.constants import (
    CONFIG_FILENAME,
    DEFAULT_DATA_DIR,
    STATE_SUBDIR,
)


def _gateway_server_entry(
    gateway_url: str | None = None,
) -> dict[str, Any]:
    """Build the server entry for the gateway itself.

    Args:
        gateway_url: Optional URL for the gateway. When provided,
            the entry uses URL-based transport instead of command.

    Returns:
        Server entry dict with either ``command`` or ``url`` key.
    """
    if gateway_url:
        return {"url": gateway_url}
    return {"command": "sift-mcp"}


def _ensure_gateway_config_dir(data_dir: Path) -> Path:
    """Ensure the gateway state directory exists, return config.json path."""
    state_dir = data_dir / STATE_SUBDIR
    state_dir.mkdir(parents=True, exist_ok=True)
    return state_dir / CONFIG_FILENAME


def _load_gateway_config(config_path: Path) -> dict[str, Any]:
    """Load existing gateway config.json or return empty dict."""
    if not config_path.exists():
        return {}
    text = config_path.read_text(encoding="utf-8")
    raw = json.loads(text)
    if not isinstance(raw, dict):
        return {}
    return raw


def _write_json(path: Path, data: dict[str, Any]) -> None:
    """Atomically write JSON with consistent formatting.

    Writes to a temporary file in the same directory, then
    renames into place so readers never see a partial file.
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


def _externalize_server_secrets(
    data_dir: Path,
    servers: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Externalize inline secrets from server configs.

    For each server that has inline ``env`` or ``headers``, writes
    the secrets to a per-upstream file and replaces the inline
    values with a ``_gateway.secret_ref``.

    Args:
        data_dir: Root data directory for Sift state.
        servers: Mutable dict mapping server name to config.
            Modified in place.

    Returns:
        The same *servers* dict, with inline secrets replaced by
        ``_gateway.secret_ref`` entries.
    """
    for name, entry in servers.items():
        if not isinstance(entry, dict):
            continue

        env = entry.get("env")
        headers = entry.get("headers")
        if not env and not headers:
            continue

        transport = "http" if "url" in entry else "stdio"
        write_secret(
            data_dir,
            name,
            transport=transport,
            env=env if env else None,
            headers=headers if headers else None,
        )

        # Replace inline secrets with a reference
        gateway_ext = entry.get("_gateway", {})
        if not isinstance(gateway_ext, dict):
            gateway_ext = {}
        gateway_ext["secret_ref"] = name
        entry["_gateway"] = gateway_ext

        # Remove inline secrets from the config entry
        entry.pop("env", None)
        entry.pop("headers", None)

    return servers


def run_init(
    source_path: Path,
    *,
    data_dir: Path | None = None,
    gateway_name: str = "artifact-gateway",
    gateway_url: str | None = None,
    dry_run: bool = False,
    db_backend: Literal["sqlite", "postgres"] = "sqlite",
    postgres_dsn: str | None = None,
) -> dict[str, Any]:
    """Migrate MCP servers from source file into the gateway.

    Args:
        source_path: Path to the source config file
            (e.g., claude_desktop_config.json).
        data_dir: Gateway data directory. Defaults to
            ``.sift-mcp``.
        gateway_name: Name for the gateway entry in the
            rewritten source file.
        gateway_url: Optional URL for the gateway entry.
            When provided, the rewritten source uses URL-based
            transport instead of command.
        dry_run: If True, print what would happen without
            making changes.
        db_backend: Database backend for the generated gateway
            config. Defaults to ``sqlite``.
        postgres_dsn: Explicit Postgres DSN. Skips Docker
            auto-provisioning when set (only used when
            ``db_backend="postgres"``).

    Returns:
        Summary dict with keys: servers_migrated, backup_path,
        source_path, gateway_config_path.
    """
    source_path = source_path.expanduser().resolve()
    if data_dir is None:
        data_dir = Path(DEFAULT_DATA_DIR).resolve()

    # 1. Read and validate source file
    source_raw = read_config_file(source_path)
    servers = extract_mcp_servers(source_raw)

    if not servers:
        msg = f"no mcpServers found in {source_path}"
        raise ValueError(msg)

    server_names = sorted(servers.keys())

    # 2. Prepare gateway config
    gateway_config_path = _ensure_gateway_config_dir(data_dir)
    existing_gateway_config = _load_gateway_config(gateway_config_path)

    # Merge: existing gateway mcpServers + newly imported servers
    existing_servers = existing_gateway_config.get("mcpServers", {})
    if not isinstance(existing_servers, dict):
        existing_servers = {}

    merged_servers = dict(servers)  # New servers as base
    merged_servers.update(existing_servers)  # Existing gateway config wins

    # 2.1. Externalize secrets only for servers that won the merge
    #       (skip those overridden by existing gateway config)
    if not dry_run:
        new_only = {
            k: v for k, v in servers.items() if k not in existing_servers
        }
        _externalize_server_secrets(data_dir, new_only)
        # Update merged_servers with externalized versions
        merged_servers.update(new_only)

    new_gateway_config = dict(existing_gateway_config)
    new_gateway_config["mcpServers"] = merged_servers
    new_gateway_config["db_backend"] = db_backend
    # Remove legacy format if present
    new_gateway_config.pop("upstreams", None)

    # 2.3. Write sync metadata
    new_gateway_config["_gateway_sync"] = {
        "enabled": True,
        "source_path": str(source_path),
        "gateway_name": gateway_name,
    }

    # 2.5. Optional Postgres provisioning
    pg_result = None
    resolved_postgres_dsn: str | None = None
    if db_backend == "postgres":
        dsn_from_env = os.environ.get("SIFT_MCP_POSTGRES_DSN")
        raw_dsn_from_config = existing_gateway_config.get("postgres_dsn")
        dsn_from_config = (
            raw_dsn_from_config
            if isinstance(raw_dsn_from_config, str)
            else None
        )
        resolved_postgres_dsn = postgres_dsn or dsn_from_env or dsn_from_config

        if resolved_postgres_dsn:
            new_gateway_config["postgres_dsn"] = resolved_postgres_dsn
        else:
            from sift_mcp.config.docker_postgres import (
                DockerNotFoundError,
                provision_postgres,
            )

            try:
                pg_result = provision_postgres(dry_run=dry_run)
                new_gateway_config["postgres_dsn"] = pg_result.dsn
            except DockerNotFoundError:
                pg_result = None
    else:
        # Keep sqlite setup explicit and avoid carrying stale DSNs.
        new_gateway_config.pop("postgres_dsn", None)

    # 3. Prepare rewritten source file
    #    Preserve the original format (mcpServers vs mcp.servers)
    gw_entry = _gateway_server_entry(gateway_url)
    new_source = dict(source_raw)
    is_vscode = "mcpServers" not in source_raw and isinstance(
        source_raw.get("mcp"), dict
    )
    if is_vscode:
        new_source["mcp"] = {
            "servers": {gateway_name: gw_entry},
        }
    else:
        new_source["mcpServers"] = {
            gateway_name: gw_entry,
        }

    # 4. Backup path
    backup_path = source_path.with_suffix(source_path.suffix + ".backup")

    summary: dict[str, Any] = {
        "servers_migrated": server_names,
        "db_backend": db_backend,
        "backup_path": str(backup_path),
        "source_path": str(source_path),
        "gateway_config_path": str(gateway_config_path),
    }

    if db_backend == "postgres" and resolved_postgres_dsn is None:
        if pg_result is not None:
            summary["docker_postgres"] = {
                "container": pg_result.container_name,
                "port": pg_result.port,
                "already_running": pg_result.already_running,
            }
        else:
            summary["docker_postgres_skipped"] = (
                "Docker not found. Install Docker or set --postgres-dsn."
            )

    if dry_run:
        return summary

    # 5. Execute: backup, write gateway config, rewrite source
    shutil.copy2(source_path, backup_path)
    _write_json(gateway_config_path, new_gateway_config)
    _write_json(source_path, new_source)

    return summary


def run_revert(source_path: Path) -> dict[str, Any]:
    """Restore a source config file from its backup.

    Parameters
    ----------
    source_path:
        Path to the source config file that was previously migrated.

    Returns:
    -------
    Summary dict with keys: restored_path, backup_path.
    """
    source_path = source_path.expanduser().resolve()
    backup_path = source_path.with_suffix(source_path.suffix + ".backup")

    if not backup_path.exists():
        msg = f"no backup found at {backup_path}"
        raise FileNotFoundError(msg)

    shutil.copy2(backup_path, source_path)
    backup_path.unlink()

    return {
        "restored_path": str(source_path),
        "backup_path": str(backup_path),
    }


def print_init_summary(
    summary: dict[str, Any], *, dry_run: bool = False
) -> None:
    """Print a human-readable summary of the init operation."""
    prefix = "[dry run] " if dry_run else ""
    servers = summary["servers_migrated"]
    db_backend = summary.get("db_backend", "sqlite")

    print(f"{prefix}Migrated {len(servers)} server(s) into gateway config:")
    for name in servers:
        print(f"  - {name}")
    print()
    print(f"{prefix}DB backend:     {db_backend}")
    if "docker_postgres" in summary:
        pg = summary["docker_postgres"]
        status = "reused" if pg["already_running"] else "started"
        ctr = pg["container"]
        port = pg["port"]
        print(
            f"{prefix}Postgres:       {status} container '{ctr}' on port {port}"
        )
    elif "docker_postgres_skipped" in summary:
        print(f"{prefix}Postgres:       {summary['docker_postgres_skipped']}")

    print(f"{prefix}Backup:         {summary['backup_path']}")
    print(f"{prefix}Gateway config: {summary['gateway_config_path']}")
    print(f"{prefix}Source updated:  {summary['source_path']}")

    if not dry_run:
        print()
        print(
            "To revert: sift-mcp init --from "
            + summary["source_path"]
            + " --revert"
        )
