"""Gateway response contracts for mirrored tool calls."""

from __future__ import annotations

from typing import Any

from sidepouch_mcp.constants import (
    RESPONSE_TYPE_ERROR,
    RESPONSE_TYPE_RESULT,
)


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
    describe: dict[str, Any] | None = None,
    usage_hint: str | None = None,
    pagination: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create a gateway tool response with inline describe data.

    Args:
        artifact_id: Unique artifact identifier.
        cache_meta: Cache metadata dict (reused, reason, etc.).
        describe: Inline describe response with mapping metadata
            and discovered roots.
        usage_hint: Natural language hint for the calling model
            describing what the artifact contains and which
            tools to call next.
        pagination: Pagination metadata when more pages are
            available or pagination is configured upstream.
            Includes canonical fields (layer, retrieval_status,
            partial_reason, has_more, next_action) plus legacy
            compatibility fields (``has_next_page`` and ``hint``).

    Returns:
        Structured result dict with artifact handle, cache info,
        describe data, and usage hint.
    """
    result: dict[str, Any] = {
        "type": RESPONSE_TYPE_RESULT,
        "artifact_id": artifact_id,
        "meta": {"cache": cache_meta or {}},
    }
    if describe is not None:
        result["describe"] = describe
    if usage_hint is not None:
        result["usage_hint"] = usage_hint
    if pagination is not None:
        result["pagination"] = pagination
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
