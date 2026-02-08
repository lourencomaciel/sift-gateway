"""gateway.status tool implementation."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from mcp_artifact_gateway.config.settings import GatewayConfig
from mcp_artifact_gateway.constants import (
    CANONICALIZER_VERSION,
    CURSOR_VERSION,
    MAPPER_VERSION,
    PRNG_VERSION,
    TRAVERSAL_CONTRACT_VERSION,
)


def probe_db(db_pool: Any) -> dict[str, Any]:
    """Probe database pool connectivity.

    Returns a dict with 'ok' bool and optional 'error' string.
    """
    if db_pool is None:
        return {"ok": False, "error": "no db pool configured"}
    try:
        with db_pool.connection() as conn:
            conn.execute("SELECT 1")
        return {"ok": True}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def probe_fs(config: GatewayConfig) -> dict[str, Any]:
    """Probe filesystem path existence for required directories.

    Returns a dict with 'ok' bool, 'paths' dict mapping each required
    directory to its existence status, and optional 'error' string.
    """
    required_paths: dict[str, Path] = {
        "data_dir": config.data_dir,
        "state_dir": config.state_dir,
        "blobs_bin_dir": config.blobs_bin_dir,
    }
    path_status: dict[str, bool] = {}
    all_ok = True
    for name, path in required_paths.items():
        exists = path.is_dir()
        path_status[name] = exists
        if not exists:
            all_ok = False

    result: dict[str, Any] = {"ok": all_ok, "paths": path_status}
    if not all_ok:
        missing = [name for name, exists in path_status.items() if not exists]
        result["error"] = f"missing directories: {', '.join(missing)}"
    return result


def build_status_response(config: GatewayConfig) -> dict[str, Any]:
    """Build the gateway.status response payload.

    Returns: upstream connectivity, DB ok, FS ok, versions, budgets, cursor settings.
    """
    return build_status_response_with_runtime(config)


def build_status_response_with_runtime(
    config: GatewayConfig,
    *,
    db_health: dict[str, Any] | None = None,
    fs_health: dict[str, Any] | None = None,
    upstreams: list[dict[str, Any]] | None = None,
    cursor_secrets_info: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build status payload with runtime health probes and secret metadata.

    Parameters
    ----------
    config:
        Gateway configuration.
    db_health:
        Result of ``probe_db()``, or None if not probed.
    fs_health:
        Result of ``probe_fs()``, or None if not probed.
    upstreams:
        List of upstream connectivity dicts from ``GatewayServer._status_upstreams()``.
    cursor_secrets_info:
        Dict with ``signing_version`` and ``active_versions`` from cursor secrets.
    """
    cursor_section: dict[str, Any] = {
        "cursor_ttl_minutes": config.cursor_ttl_minutes,
    }
    if cursor_secrets_info is not None:
        cursor_section["signing_version"] = cursor_secrets_info.get("signing_version")
        cursor_section["active_versions"] = cursor_secrets_info.get("active_versions", [])

    return {
        "type": "gateway_status",
        "versions": {
            "canonicalizer_version": CANONICALIZER_VERSION,
            "mapper_version": MAPPER_VERSION,
            "traversal_contract_version": TRAVERSAL_CONTRACT_VERSION,
            "cursor_version": CURSOR_VERSION,
            "prng_version": PRNG_VERSION,
        },
        "where_canonicalization_mode": config.where_canonicalization_mode.value,
        "mapping_mode": config.mapping_mode.value,
        "budgets": {
            "max_items": config.max_items,
            "max_bytes_out": config.max_bytes_out,
            "max_wildcards": config.max_wildcards,
            "max_compute_steps": config.max_compute_steps,
            "max_json_part_parse_bytes": config.max_json_part_parse_bytes,
            "max_full_map_bytes": config.max_full_map_bytes,
            "max_bytes_read_partial_map": config.max_bytes_read_partial_map,
            "max_compute_steps_partial_map": config.max_compute_steps_partial_map,
            "max_depth_partial_map": config.max_depth_partial_map,
            "max_records_sampled_partial": config.max_records_sampled_partial,
            "max_record_bytes_partial": config.max_record_bytes_partial,
            "max_leaf_paths_partial": config.max_leaf_paths_partial,
            "artifact_search_max_limit": config.artifact_search_max_limit,
        },
        "storage_caps": {
            "max_binary_blob_bytes": config.max_binary_blob_bytes,
            "max_payload_total_bytes": config.max_payload_total_bytes,
            "max_total_storage_bytes": config.max_total_storage_bytes,
        },
        "cursor": cursor_section,
        "db": db_health if db_health is not None else {"ok": False, "error": "not probed"},
        "fs": fs_health if fs_health is not None else {"ok": False, "error": "not probed"},
        "upstreams": list(upstreams or []),
    }
