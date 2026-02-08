"""Artifacts repository validation helpers."""

from __future__ import annotations

from typing import Any, Mapping

from mcp_artifact_gateway.constants import ARTIFACT_ID_PREFIX, WORKSPACE_ID

_VALID_MAP_KINDS = {"none", "full", "partial"}
_VALID_MAP_STATUS = {"pending", "ready", "failed", "stale"}
_VALID_INDEX_STATUS = {"off", "ready", "failed"}


def validate_artifact_row(row: Mapping[str, Any]) -> None:
    workspace_id = row.get("workspace_id")
    if workspace_id != WORKSPACE_ID:
        msg = f"workspace_id must be '{WORKSPACE_ID}'"
        raise ValueError(msg)

    artifact_id = row.get("artifact_id")
    if not isinstance(artifact_id, str) or not artifact_id.startswith(ARTIFACT_ID_PREFIX):
        msg = f"artifact_id must start with '{ARTIFACT_ID_PREFIX}'"
        raise ValueError(msg)

    map_kind = row.get("map_kind", "none")
    if map_kind not in _VALID_MAP_KINDS:
        msg = f"invalid map_kind: {map_kind}"
        raise ValueError(msg)

    map_status = row.get("map_status", "pending")
    if map_status not in _VALID_MAP_STATUS:
        msg = f"invalid map_status: {map_status}"
        raise ValueError(msg)

    index_status = row.get("index_status", "off")
    if index_status not in _VALID_INDEX_STATUS:
        msg = f"invalid index_status: {index_status}"
        raise ValueError(msg)

    for key in ("payload_json_bytes", "payload_binary_bytes_total", "payload_total_bytes"):
        value = row.get(key, 0)
        if not isinstance(value, int) or value < 0:
            msg = f"{key} must be a non-negative integer"
            raise ValueError(msg)

