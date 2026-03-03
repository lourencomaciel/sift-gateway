"""Build the ``gateway.status`` health and configuration response.

Probe database connectivity, filesystem directory existence, and
upstream status, then assemble a structured status payload with
version strings, retrieval budgets, and cursor settings.

Typical usage example::

    db_health = probe_db(db_pool)
    fs_health = probe_fs(config)
    payload = build_status_response_with_runtime(
        config, db_health=db_health, fs_health=fs_health,
    )
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from sift_gateway.config.settings import GatewayConfig
from sift_gateway.constants import (
    CANONICALIZER_VERSION,
    CURSOR_VERSION,
    MAPPER_VERSION,
    PRNG_VERSION,
    TRAVERSAL_CONTRACT_VERSION,
)


def probe_db(db_pool: Any) -> dict[str, Any]:
    """Probe database pool connectivity.

    Args:
        db_pool: Database backend exposing a ``connection()``
            context manager. May be ``None``.

    Returns:
        A dict with ``ok`` bool and optional ``error`` string.
    """
    if db_pool is None:
        return {"ok": False, "error": "no db pool configured"}
    try:
        with db_pool.connection() as conn:
            conn.execute("SELECT 1")
        return {"ok": True}
    except Exception as exc:
        return {
            "ok": False,
            "error": "db probe failed",
            "error_type": type(exc).__name__,
        }


def probe_fs(config: GatewayConfig) -> dict[str, Any]:
    """Probe filesystem path existence for required directories.

    Args:
        config: Gateway configuration providing directory paths.

    Returns:
        A dict with ``ok`` bool, ``paths`` mapping each
        directory name to its existence status, and an optional
        ``error`` string listing missing directories.
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
    """Build the ``gateway.status`` response payload.

    Delegates to ``build_status_response_with_runtime`` with no
    runtime probes.

    Args:
        config: Gateway configuration.

    Returns:
        Status dict with versions, budgets, and placeholder
        health sections.
    """
    return build_status_response_with_runtime(config)


def build_status_response_with_runtime(
    config: GatewayConfig,
    *,
    db_health: dict[str, Any] | None = None,
    fs_health: dict[str, Any] | None = None,
    upstreams: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build status payload with runtime health probes.

    Args:
        config: Gateway configuration.
        db_health: Result of ``probe_db()``, or ``None`` if
            not probed.
        fs_health: Result of ``probe_fs()``, or ``None`` if
            not probed.
        upstreams: Upstream connectivity dicts from
            ``GatewayServer._status_upstreams(...)``.

    Returns:
        Structured status dict containing versions, budgets,
        storage caps, cursor settings, and health sections.
    """
    cursor_section: dict[str, Any] = {
        "cursor_ttl_minutes": config.cursor_ttl_minutes,
    }

    return {
        "type": "gateway_status",
        "versions": {
            "canonicalizer_version": CANONICALIZER_VERSION,
            "mapper_version": MAPPER_VERSION,
            "traversal_contract_version": TRAVERSAL_CONTRACT_VERSION,
            "cursor_version": CURSOR_VERSION,
            "prng_version": PRNG_VERSION,
        },
        "budgets": {
            "max_items": config.max_items,
            "max_bytes_out": config.max_bytes_out,
            "passthrough_max_bytes": config.passthrough_max_bytes,
            "max_wildcards": config.max_wildcards,
            "max_compute_steps": config.max_compute_steps,
            "max_json_part_parse_bytes": config.max_json_part_parse_bytes,
            "max_full_map_bytes": config.max_full_map_bytes,
            "max_in_memory_mapping_bytes": config.max_in_memory_mapping_bytes,
            "max_bytes_read_partial_map": config.max_bytes_read_partial_map,
            "max_compute_steps_partial_map": (
                config.max_compute_steps_partial_map
            ),
            "max_depth_partial_map": config.max_depth_partial_map,
            "max_records_sampled_partial": config.max_records_sampled_partial,
            "max_record_bytes_partial": config.max_record_bytes_partial,
            "max_leaf_paths_partial": config.max_leaf_paths_partial,
            "artifact_search_max_limit": config.artifact_search_max_limit,
            "code_query_max_bytes_out": config.code_query_max_bytes_out,
        },
        "storage_caps": {
            "max_binary_blob_bytes": config.max_binary_blob_bytes,
            "max_payload_total_bytes": config.max_payload_total_bytes,
            "max_total_storage_bytes": config.max_total_storage_bytes,
        },
        "cursor": cursor_section,
        "db": db_health
        if db_health is not None
        else {"ok": False, "error": "not probed"},
        "fs": fs_health
        if fs_health is not None
        else {"ok": False, "error": "not probed"},
        "upstreams": list(upstreams or []),
    }
