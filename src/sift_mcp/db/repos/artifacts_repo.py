"""Artifacts repository validation helpers."""

from __future__ import annotations

from collections.abc import Mapping
import json
from typing import Any

from sift_mcp.constants import ARTIFACT_ID_PREFIX, WORKSPACE_ID

_VALID_MAP_KINDS = {"none", "full", "partial"}
_VALID_MAP_STATUS = {"pending", "ready", "failed", "stale"}
_VALID_INDEX_STATUS = {"off", "pending", "ready", "partial", "failed"}
_VALID_KINDS = {"data", "derived_query", "derived_codegen"}


def validate_artifact_row(row: Mapping[str, Any]) -> None:
    """Validate an artifact row against invariant constraints.

    Args:
        row: Artifact row as a mapping.

    Raises:
        ValueError: If any field fails validation.
    """
    workspace_id = row.get("workspace_id")
    if workspace_id != WORKSPACE_ID:
        msg = f"workspace_id must be '{WORKSPACE_ID}'"
        raise ValueError(msg)

    artifact_id = row.get("artifact_id")
    if not isinstance(artifact_id, str) or not artifact_id.startswith(
        ARTIFACT_ID_PREFIX
    ):
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

    kind = row.get("kind", "data")
    if kind not in _VALID_KINDS:
        msg = f"invalid kind: {kind}"
        raise ValueError(msg)

    derivation = row.get("derivation")
    parent_artifact_id = row.get("parent_artifact_id")
    if kind == "data":
        if derivation is not None:
            msg = "data artifacts must not set derivation"
            raise ValueError(msg)
    else:
        if not isinstance(parent_artifact_id, str) or not parent_artifact_id:
            msg = "derived artifacts require parent_artifact_id"
            raise ValueError(msg)
        if not isinstance(derivation, str) or not derivation:
            msg = "derived artifacts require derivation"
            raise ValueError(msg)
        try:
            parsed_derivation = json.loads(derivation)
        except (json.JSONDecodeError, ValueError):
            msg = "derived artifact derivation must be valid JSON"
            raise ValueError(msg) from None
        if not isinstance(parsed_derivation, dict):
            msg = "derived artifact derivation must be a JSON object"
            raise ValueError(msg)

    index_status = row.get("index_status", "off")
    if index_status not in _VALID_INDEX_STATUS:
        msg = f"invalid index_status: {index_status}"
        raise ValueError(msg)

    for key in (
        "payload_json_bytes",
        "payload_binary_bytes_total",
        "payload_total_bytes",
    ):
        value = row.get(key, 0)
        if not isinstance(value, int) or value < 0:
            msg = f"{key} must be a non-negative integer"
            raise ValueError(msg)
