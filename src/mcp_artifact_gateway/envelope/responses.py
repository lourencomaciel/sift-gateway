"""Gateway response contracts for mirrored tool calls."""

from __future__ import annotations

from typing import Any

from mcp_artifact_gateway.constants import RESPONSE_TYPE_ERROR, RESPONSE_TYPE_RESULT


def can_passthrough(
    *,
    payload_total_bytes: int,
    contains_binary_refs: bool,
    passthrough_allowed: bool,
    max_bytes: int,
) -> bool:
    """Check if a result is eligible for passthrough (raw upstream return)."""
    return (
        passthrough_allowed
        and max_bytes > 0
        and not contains_binary_refs
        and payload_total_bytes < max_bytes
    )


def gateway_tool_result(
    *,
    artifact_id: str,
    cache_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create handle-only tool response (artifact_id + cache metadata)."""
    return {
        "type": RESPONSE_TYPE_RESULT,
        "artifact_id": artifact_id,
        "meta": {"cache": cache_meta or {}},
    }


def gateway_error(
    code: str,
    message: str,
    *,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "type": RESPONSE_TYPE_ERROR,
        "code": code,
        "message": message,
        "details": details or {},
    }
