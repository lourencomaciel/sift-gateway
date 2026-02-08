"""JSONB persistence policy for envelope storage."""

from __future__ import annotations

from typing import Any

from mcp_artifact_gateway.canon.rfc8785 import canonical_bytes
from mcp_artifact_gateway.envelope.model import Envelope


def envelope_to_jsonb(
    envelope: Envelope,
    *,
    mode: str,
    minimize_threshold_bytes: int,
) -> dict[str, Any] | None:
    """Return JSONB payload per configured storage mode."""
    if mode == "none":
        return None

    payload = envelope.to_dict()
    if mode == "full":
        return payload

    if mode == "minimal_for_large":
        raw_len = len(canonical_bytes(payload))
        if raw_len <= minimize_threshold_bytes:
            return payload
        return {
            "type": payload["type"],
            "upstream_instance_id": payload["upstream_instance_id"],
            "upstream_prefix": payload["upstream_prefix"],
            "tool": payload["tool"],
            "status": payload["status"],
            "error": payload["error"],
            "meta": payload["meta"],
            "content_summary": {
                "part_count": len(payload["content"]),
                "part_types": [part["type"] for part in payload["content"]],
            },
        }

    msg = f"unsupported envelope_jsonb_mode: {mode}"
    raise ValueError(msg)

