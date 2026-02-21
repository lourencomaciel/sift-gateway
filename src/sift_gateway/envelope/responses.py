"""Gateway response contracts for run/code-style tool calls."""

from __future__ import annotations

import json
from typing import Any

from sift_gateway.constants import RESPONSE_TYPE_ERROR


def _json_size_bytes(payload: Any) -> int:
    """Return UTF-8 byte size for one JSON-serializable payload."""
    return len(
        json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    )


def select_response_mode(
    *,
    has_pagination: bool,
    full_payload: dict[str, Any],
    schema_ref_payload: dict[str, Any],
    max_bytes: int,
) -> str:
    """Choose response mode according to contract-v1 rules."""
    if has_pagination:
        return "schema_ref"
    try:
        full_bytes = _json_size_bytes(full_payload)
    except Exception:
        return "schema_ref"
    if full_bytes > max_bytes:
        return "schema_ref"
    try:
        schema_ref_bytes = _json_size_bytes(schema_ref_payload)
    except Exception:
        return "full"
    if schema_ref_bytes * 2 <= full_bytes:
        return "schema_ref"
    return "full"


def gateway_tool_result(
    *,
    response_mode: str,
    artifact_id: str,
    payload: Any | None = None,
    schemas: list[dict[str, Any]] | None = None,
    lineage: dict[str, Any] | None = None,
    pagination: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create a run/code result payload in contract-v1 shape.

    Args:
        response_mode: ``"full"`` or ``"schema_ref"``.
        artifact_id: Unique artifact identifier.
        payload: Inline payload when ``response_mode == "full"``.
        schemas: Schema list for ``schema_ref`` mode.
        lineage: Lineage/chain metadata.
        pagination: Pagination metadata when present.
        metadata: Optional additional metadata.

    Returns:
        Structured response payload.
    """
    if response_mode not in {"full", "schema_ref"}:
        msg = f"invalid response_mode: {response_mode}"
        raise ValueError(msg)

    result: dict[str, Any] = {
        "response_mode": response_mode,
        "artifact_id": artifact_id,
    }
    if response_mode == "full":
        result["payload"] = payload
    else:
        result["schemas"] = list(schemas) if isinstance(schemas, list) else []
    if lineage is not None:
        result["lineage"] = lineage
    if pagination is not None:
        result["pagination"] = pagination
    if metadata is not None:
        result["metadata"] = metadata
    return result


def gateway_error(
    code: str,
    message: str,
    *,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a gateway error response dict.

    Args:
        code: Machine-readable error code.
        message: Human-readable error message.
        details: Optional additional error context.

    Returns:
        Structured error response dict.
    """
    return {
        "type": RESPONSE_TYPE_ERROR,
        "code": code,
        "message": message,
        "details": details or {},
    }
