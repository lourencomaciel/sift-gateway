"""Migrate MCP server config from an external tool into the gateway.

Usage::

    mcp-gateway init --from ~/Library/Application\\ Support/Claude/claude_desktop_config.json

This command:
1. Reads the source file and extracts ``mcpServers``
2. Copies those servers into the gateway's own config
3. Backs up the source file to ``<file>.backup``
4. Rewrites the source file with only the gateway as the MCP server

Use ``mcp-gateway init --from <file> --revert`` to restore the backup.
"""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Any

from mcp_artifact_gateway.config.mcp_servers import extract_mcp_servers, read_config_file
from mcp_artifact_gateway.constants import CONFIG_FILENAME, DEFAULT_DATA_DIR, STATE_SUBDIR


def _gateway_server_entry() -> dict[str, Any]:
    """Build the server entry for the gateway itself."""
    return {"command": "mcp-gateway"}


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
    """Write JSON with consistent formatting."""
    path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def run_init(
    source_path: Path,
    *,
    data_dir: Path | None = None,
    gateway_name: str = "artifact-gateway",
    dry_run: bool = False,
    postgres_dsn: str | None = None,
) -> dict[str, Any]:
    """Migrate MCP servers from source file into the gateway.

    Parameters
    ----------
    source_path:
        Path to the source config file (e.g., claude_desktop_config.json).
    data_dir:
        Gateway data directory. Defaults to ``.mcp_gateway``.
    gateway_name:
        Name for the gateway entry in the rewritten source file.
    dry_run:
        If True, print what would happen without making changes.

    Returns
    -------
    Summary dict with keys: servers_migrated, backup_path, source_path,
    gateway_config_path.
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

    new_gateway_config = dict(existing_gateway_config)
    new_gateway_config["mcpServers"] = merged_servers
    # Remove legacy format if present
    new_gateway_config.pop("upstreams", None)

    # 2.5. Postgres provisioning
    dsn_from_env = os.environ.get("MCP_GATEWAY_POSTGRES_DSN")
    dsn_from_config = existing_gateway_config.get("postgres_dsn")
    dsn_explicitly_set = postgres_dsn or dsn_from_env or dsn_from_config

    if dsn_explicitly_set:
        resolved_dsn = postgres_dsn or dsn_from_env or dsn_from_config
        new_gateway_config["postgres_dsn"] = resolved_dsn
    else:
        from mcp_artifact_gateway.config.docker_postgres import (
            DockerNotFoundError,
            provision_postgres,
        )

        try:
            pg_result = provision_postgres(dry_run=dry_run)
            new_gateway_config["postgres_dsn"] = pg_result.dsn
        except DockerNotFoundError:
            pg_result = None

    # 3. Prepare rewritten source file (only the gateway as MCP server)
    #    Preserve the original format (mcpServers vs mcp.servers)
    new_source = dict(source_raw)
    is_vscode = "mcpServers" not in source_raw and isinstance(source_raw.get("mcp"), dict)
    if is_vscode:
        new_source["mcp"] = {"servers": {gateway_name: _gateway_server_entry()}}
    else:
        new_source["mcpServers"] = {gateway_name: _gateway_server_entry()}

    # 4. Backup path
    backup_path = source_path.with_suffix(source_path.suffix + ".backup")

    summary: dict[str, Any] = {
        "servers_migrated": server_names,
        "backup_path": str(backup_path),
        "source_path": str(source_path),
        "gateway_config_path": str(gateway_config_path),
    }

    if not dsn_explicitly_set:
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

    Returns
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


def print_init_summary(summary: dict[str, Any], *, dry_run: bool = False) -> None:
    """Print a human-readable summary of the init operation."""
    prefix = "[dry run] " if dry_run else ""
    servers = summary["servers_migrated"]

    print(f"{prefix}Migrated {len(servers)} server(s) into gateway config:")
    for name in servers:
        print(f"  - {name}")
    print()
    if "docker_postgres" in summary:
        pg = summary["docker_postgres"]
        status = "reused" if pg["already_running"] else "started"
        print(
            f"{prefix}Postgres:       {status} container '{pg['container']}' on port {pg['port']}"
        )
    elif "docker_postgres_skipped" in summary:
        print(f"{prefix}Postgres:       {summary['docker_postgres_skipped']}")

    print(f"{prefix}Backup:         {summary['backup_path']}")
    print(f"{prefix}Gateway config: {summary['gateway_config_path']}")
    print(f"{prefix}Source updated:  {summary['source_path']}")

    if not dry_run:
        print()
        print("To revert: mcp-gateway init --from " + summary["source_path"] + " --revert")
