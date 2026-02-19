"""Artifacts repository validation helpers."""

from __future__ import annotations

from collections.abc import Mapping
import json
from typing import Any

from sift_gateway.constants import (
    ARTIFACT_ID_PREFIX,
    CAPTURE_KIND_CLI_COMMAND,
    CAPTURE_KIND_DERIVED_CODEGEN,
    CAPTURE_KIND_DERIVED_QUERY,
    CAPTURE_KIND_FILE_INGEST,
    CAPTURE_KIND_MCP_TOOL,
    CAPTURE_KIND_STDIN_PIPE,
    WORKSPACE_ID,
)

_VALID_MAP_KINDS = {"none", "full", "partial"}
_VALID_MAP_STATUS = {"pending", "ready", "failed", "stale"}
_VALID_INDEX_STATUS = {"off", "pending", "ready", "partial", "failed"}
_VALID_KINDS = {"data", "derived_query", "derived_codegen"}
_VALID_CAPTURE_KINDS = {
    CAPTURE_KIND_MCP_TOOL,
    CAPTURE_KIND_CLI_COMMAND,
    CAPTURE_KIND_STDIN_PIPE,
    CAPTURE_KIND_FILE_INGEST,
    CAPTURE_KIND_DERIVED_QUERY,
    CAPTURE_KIND_DERIVED_CODEGEN,
}


def _validate_workspace_id(row: Mapping[str, Any]) -> None:
    """Validate static workspace scope invariant."""
    workspace_id = row.get("workspace_id")
    if workspace_id != WORKSPACE_ID:
        msg = f"workspace_id must be '{WORKSPACE_ID}'"
        raise ValueError(msg)


def _validate_artifact_id(row: Mapping[str, Any]) -> None:
    """Validate artifact_id prefix invariant."""
    artifact_id = row.get("artifact_id")
    if not isinstance(artifact_id, str) or not artifact_id.startswith(
        ARTIFACT_ID_PREFIX
    ):
        msg = f"artifact_id must start with '{ARTIFACT_ID_PREFIX}'"
        raise ValueError(msg)


def _validate_enum_value(
    *,
    field_name: str,
    value: Any,
    valid_values: set[str],
) -> None:
    """Validate one enum-like field."""
    if value not in valid_values:
        msg = f"invalid {field_name}: {value}"
        raise ValueError(msg)


def _validate_derivation_fields(
    *,
    kind: str,
    derivation: Any,
    parent_artifact_id: Any,
) -> None:
    """Validate derived/data invariants and derivation JSON shape."""
    if kind == "data":
        if derivation is not None:
            msg = "data artifacts must not set derivation"
            raise ValueError(msg)
        return

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


def _validate_payload_sizes(row: Mapping[str, Any]) -> None:
    """Validate payload byte counters are non-negative integers."""
    for key in (
        "payload_json_bytes",
        "payload_binary_bytes_total",
        "payload_total_bytes",
    ):
        value = row.get(key, 0)
        if not isinstance(value, int) or value < 0:
            msg = f"{key} must be a non-negative integer"
            raise ValueError(msg)


def _validate_capture_fields(row: Mapping[str, Any]) -> None:
    """Validate protocol-neutral capture identity fields."""
    capture_kind = row.get("capture_kind")
    if capture_kind not in _VALID_CAPTURE_KINDS:
        msg = f"invalid capture_kind: {capture_kind}"
        raise ValueError(msg)

    capture_key = row.get("capture_key")
    if not isinstance(capture_key, str) or not capture_key:
        msg = "capture_key must be a non-empty string"
        raise ValueError(msg)

    capture_origin = row.get("capture_origin")
    if not isinstance(capture_origin, Mapping):
        msg = "capture_origin must be an object"
        raise ValueError(msg)


def validate_artifact_row(row: Mapping[str, Any]) -> None:
    """Validate an artifact row against invariant constraints.

    Args:
        row: Artifact row as a mapping.

    Raises:
        ValueError: If any field fails validation.
    """
    _validate_workspace_id(row)
    _validate_artifact_id(row)
    _validate_enum_value(
        field_name="map_kind",
        value=row.get("map_kind", "none"),
        valid_values=_VALID_MAP_KINDS,
    )
    _validate_enum_value(
        field_name="map_status",
        value=row.get("map_status", "pending"),
        valid_values=_VALID_MAP_STATUS,
    )
    kind = row.get("kind", "data")
    _validate_enum_value(
        field_name="kind",
        value=kind,
        valid_values=_VALID_KINDS,
    )
    _validate_derivation_fields(
        kind=kind,
        derivation=row.get("derivation"),
        parent_artifact_id=row.get("parent_artifact_id"),
    )
    _validate_enum_value(
        field_name="index_status",
        value=row.get("index_status", "off"),
        valid_values=_VALID_INDEX_STATUS,
    )
    _validate_payload_sizes(row)
    _validate_capture_fields(row)
