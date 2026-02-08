"""Gateway response contracts for mirrored tool calls."""

from __future__ import annotations

from typing import Any

from mcp_artifact_gateway.constants import RESPONSE_TYPE_ERROR, RESPONSE_TYPE_RESULT
from mcp_artifact_gateway.envelope.model import Envelope


def can_inline_envelope(
    *,
    payload_json_bytes: int,
    payload_total_bytes: int,
    contains_binary_refs: bool,
    inline_allowed: bool,
    max_json_bytes: int,
    max_total_bytes: int,
) -> bool:
    return (
        inline_allowed
        and not contains_binary_refs
        and payload_json_bytes <= max_json_bytes
        and payload_total_bytes <= max_total_bytes
    )


def gateway_tool_result(
    *,
    artifact_id: str,
    envelope: Envelope,
    payload_json_bytes: int,
    payload_total_bytes: int,
    contains_binary_refs: bool,
    inline_allowed: bool,
    max_json_bytes: int = 32_768,
    max_total_bytes: int = 65_536,
    cache_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create handle-first tool response with optional inline envelope."""
    inline = can_inline_envelope(
        payload_json_bytes=payload_json_bytes,
        payload_total_bytes=payload_total_bytes,
        contains_binary_refs=contains_binary_refs,
        inline_allowed=inline_allowed,
        max_json_bytes=max_json_bytes,
        max_total_bytes=max_total_bytes,
    )

    response: dict[str, Any] = {
        "type": RESPONSE_TYPE_RESULT,
        "artifact_id": artifact_id,
        "meta": {"inline": inline, "cache": cache_meta or {}},
    }
    if inline:
        response["envelope"] = envelope.to_dict()
    return response


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

