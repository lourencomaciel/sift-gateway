"""SQLite-backed upstream registry orchestration and config mirror.

This module provides the high-level sync flows (bootstrap, merge, mirror)
and re-exports all public names from the repo and convert sub-modules
so that existing imports continue to work.
"""

from __future__ import annotations

import contextlib
import logging
import os
from pathlib import Path
import tempfile
from typing import Any

from sift_gateway.config.mcp_servers import extract_mcp_servers
from sift_gateway.config.shared import (
    ensure_gateway_config_path,
    gateway_config_path,
    load_gateway_config_dict,
    write_json,
)
from sift_gateway.config.upstream_registry_convert import (
    entry_to_registry_payload,
    record_to_mcp_server_entry,
    record_to_upstream_dict,
)
from sift_gateway.config.upstream_registry_repo import (
    connect_migrated,
    get_registry_upstream_record,
    load_registry_upstream_records,
    remove_registry_upstream,
    set_registry_upstream_enabled,
    set_registry_upstream_secret_ref,
    upsert_payload,
)
from sift_gateway.config.upstream_secrets import (
    secret_file_path,
    write_secret,
)
from sift_gateway.constants import WORKSPACE_ID

_logger = logging.getLogger(__name__)

# Re-export public API so ``from upstream_registry import X`` keeps working.
__all__ = [
    "bootstrap_registry_from_config",
    "entry_to_registry_payload",
    "get_registry_upstream_record",
    "load_registry_mcp_servers",
    "load_registry_upstream_dicts",
    "load_registry_upstream_records",
    "merge_missing_registry_from_config",
    "mirror_registry_to_config",
    "record_to_mcp_server_entry",
    "record_to_upstream_dict",
    "remove_registry_upstream",
    "replace_registry_from_mcp_servers",
    "set_registry_upstream_enabled",
    "set_registry_upstream_secret_ref",
    "upsert_registry_from_mcp_servers",
]


# ── Secret snapshot / rollback ─────────────────────────────────────


def _snapshot_secret_files(
    *,
    data_dir: Path,
    pending_secret_writes: list[
        tuple[
            str,
            str,
            dict[str, str] | None,
            dict[str, str] | None,
        ]
    ],
) -> dict[str, bytes | None]:
    """Capture current secret-file bytes for rollback."""
    snapshots: dict[str, bytes | None] = {}
    for secret_ref, _transport, _env, _headers in pending_secret_writes:
        if secret_ref in snapshots:
            continue
        path = secret_file_path(data_dir, secret_ref)
        if path.is_file():
            snapshots[secret_ref] = path.read_bytes()
        else:
            snapshots[secret_ref] = None
    return snapshots


def _restore_secret_snapshots(
    *,
    data_dir: Path,
    snapshots: dict[str, bytes | None],
) -> None:
    """Restore secret files to their pre-write state."""
    for secret_ref, prior_bytes in snapshots.items():
        path = secret_file_path(data_dir, secret_ref)
        if prior_bytes is None:
            with contextlib.suppress(FileNotFoundError):
                path.unlink()
            continue

        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_path_raw = tempfile.mkstemp(
            dir=str(path.parent),
            suffix=".tmp",
        )
        tmp_path = Path(tmp_path_raw)
        try:
            os.write(fd, prior_bytes)
            os.fchmod(fd, 0o600)
            os.close(fd)
            fd = -1
            tmp_path.replace(path)
        except BaseException:
            if fd >= 0:
                os.close(fd)
            with contextlib.suppress(OSError):
                tmp_path.unlink(missing_ok=True)
            raise


def _write_secrets_and_commit(
    *,
    conn: Any,
    data_dir: Path,
    pending_secret_writes: list[
        tuple[
            str,
            str,
            dict[str, str] | None,
            dict[str, str] | None,
        ]
    ],
) -> None:
    """Write pending secret files and commit DB transaction."""
    snapshots: dict[str, bytes | None] | None = None
    try:
        snapshots = _snapshot_secret_files(
            data_dir=data_dir,
            pending_secret_writes=pending_secret_writes,
        )
        for secret_ref, transport, env, headers in pending_secret_writes:
            write_secret(
                data_dir,
                secret_ref,
                transport=transport,
                env=env,
                headers=headers,
            )
        conn.commit()
    except Exception:
        conn.rollback()
        if snapshots is not None:
            try:
                _restore_secret_snapshots(
                    data_dir=data_dir,
                    snapshots=snapshots,
                )
            except Exception:
                _logger.warning(
                    "failed to restore secret file snapshots after rollback",
                    exc_info=True,
                )
        raise


# ── Convenience loaders ────────────────────────────────────────────


def load_registry_upstream_dicts(
    data_dir: Path,
    *,
    enabled_only: bool,
) -> list[dict[str, Any]]:
    """Load registry rows as UpstreamConfig-compatible dicts."""
    records = load_registry_upstream_records(
        data_dir,
        include_disabled=not enabled_only,
    )
    return [record_to_upstream_dict(record) for record in records]


def load_registry_mcp_servers(
    data_dir: Path,
) -> dict[str, dict[str, Any]]:
    """Load registry rows as mcpServers config map."""
    records = load_registry_upstream_records(data_dir, include_disabled=True)
    servers: dict[str, dict[str, Any]] = {}
    for record in records:
        servers[record["prefix"]] = record_to_mcp_server_entry(record)
    return servers


# ── Batch upsert / replace ─────────────────────────────────────────


def upsert_registry_from_mcp_servers(
    *,
    data_dir: Path,
    servers: dict[str, dict[str, Any]],
    merge_missing: bool,
    source_kind: str,
    source_ref: str | None = None,
) -> int:
    """Upsert registry rows from mcpServers map."""
    if not servers:
        return 0

    pending_secret_writes: list[
        tuple[
            str,
            str,
            dict[str, str] | None,
            dict[str, str] | None,
        ]
    ] = []
    with connect_migrated(data_dir) as conn:
        existing_prefixes: set[str] = set()
        if merge_missing:
            rows = conn.execute(
                (
                    "SELECT prefix FROM upstream_registry "
                    "WHERE workspace_id = %s"
                ),
                (WORKSPACE_ID,),
            ).fetchall()
            existing_prefixes = {
                str(row[0]) for row in rows if row and isinstance(row[0], str)
            }

        changed = 0
        for prefix, entry in servers.items():
            if merge_missing and prefix in existing_prefixes:
                continue
            if not isinstance(entry, dict):
                msg = f"server '{prefix}' config must be a JSON object"
                raise ValueError(msg)
            payload = entry_to_registry_payload(
                data_dir=data_dir,
                prefix=prefix,
                entry=entry,
                source_kind=source_kind,
                source_ref=source_ref,
                pending_secret_writes=pending_secret_writes,
            )
            upsert_payload(conn, payload)
            changed += 1

        _write_secrets_and_commit(
            conn=conn,
            data_dir=data_dir,
            pending_secret_writes=pending_secret_writes,
        )
    return changed


def replace_registry_from_mcp_servers(
    *,
    data_dir: Path,
    servers: dict[str, dict[str, Any]],
    source_kind: str,
    source_ref: str | None = None,
) -> int:
    """Replace full registry snapshot from mcpServers map."""
    pending_secret_writes: list[
        tuple[
            str,
            str,
            dict[str, str] | None,
            dict[str, str] | None,
        ]
    ] = []
    with connect_migrated(data_dir) as conn:
        conn.execute(
            "DELETE FROM upstream_registry WHERE workspace_id = %s",
            (WORKSPACE_ID,),
        )
        changed = 0
        for prefix, entry in servers.items():
            if not isinstance(entry, dict):
                msg = f"server '{prefix}' config must be a JSON object"
                raise ValueError(msg)
            payload = entry_to_registry_payload(
                data_dir=data_dir,
                prefix=prefix,
                entry=entry,
                source_kind=source_kind,
                source_ref=source_ref,
                pending_secret_writes=pending_secret_writes,
            )
            upsert_payload(conn, payload)
            changed += 1

        _write_secrets_and_commit(
            conn=conn,
            data_dir=data_dir,
            pending_secret_writes=pending_secret_writes,
        )
    return changed


# ── Config <-> registry sync ───────────────────────────────────────


def _load_config_mcp_servers(
    data_dir: Path,
) -> dict[str, dict[str, Any]]:
    """Load mcpServers-compatible map from state/config.json."""
    config_path = gateway_config_path(data_dir)
    raw_config = load_gateway_config_dict(config_path)
    servers = extract_mcp_servers(raw_config)
    normalized: dict[str, dict[str, Any]] = {}
    for prefix, entry in servers.items():
        if not isinstance(entry, dict):
            msg = f"server '{prefix}' config must be a JSON object"
            raise ValueError(msg)
        normalized[str(prefix)] = entry
    return normalized


def bootstrap_registry_from_config(data_dir: Path) -> int:
    """Bootstrap registry from config when the registry is empty."""
    existing = load_registry_upstream_records(data_dir, include_disabled=True)
    if existing:
        return 0
    servers = _load_config_mcp_servers(data_dir)
    if not servers:
        return 0
    changed = upsert_registry_from_mcp_servers(
        data_dir=data_dir,
        servers=servers,
        merge_missing=False,
        source_kind="init_sync",
    )
    if changed > 0:
        mirror_registry_to_config(data_dir)
    return changed


def merge_missing_registry_from_config(data_dir: Path) -> int:
    """Merge config-defined servers missing in the registry."""
    existing = load_registry_upstream_records(data_dir, include_disabled=True)
    try:
        servers = _load_config_mcp_servers(data_dir)
    except ValueError as exc:
        if existing:
            _logger.warning(
                "skipped config merge: invalid mcpServers mirror: %s",
                exc,
            )
            return 0
        raise
    if not servers:
        return 0
    try:
        changed = upsert_registry_from_mcp_servers(
            data_dir=data_dir,
            servers=servers,
            merge_missing=True,
            source_kind="init_sync",
        )
    except ValueError as exc:
        if existing:
            _logger.warning(
                "skipped config merge: validation error: %s",
                exc,
            )
            return 0
        raise
    if changed > 0:
        mirror_registry_to_config(data_dir)
    return changed


def mirror_registry_to_config(data_dir: Path) -> Path:
    """Mirror registry snapshot into config.json mcpServers."""
    config_path = gateway_config_path(data_dir)
    raw_config = load_gateway_config_dict(config_path)
    raw_config["mcpServers"] = load_registry_mcp_servers(data_dir)
    raw_config.pop("upstreams", None)
    ensure_gateway_config_path(data_dir)
    write_json(config_path, raw_config)
    return config_path
