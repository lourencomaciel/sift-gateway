"""Gateway response models per Addendum A.

These models define the shapes returned to MCP clients by the gateway's
tool handlers.  They are distinct from the internal :class:`Envelope` type
which represents the canonical storage form.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Artifact handle (Addendum A.1)
# ---------------------------------------------------------------------------
class ArtifactHandle(BaseModel):
    """Lightweight descriptor of a persisted artifact.

    Returned inside :class:`GatewayToolResult` so that callers can reference
    the artifact in subsequent retrieval / query calls.
    """

    model_config = {"extra": "forbid"}

    workspace_id: str
    artifact_id: str
    created_seq: int
    created_at: datetime
    session_id: str
    source_tool: str
    upstream_instance_id: str
    payload_hash_full: str
    canonicalizer_version: str
    status: Literal["ok", "error"]
    payload_json_bytes: int
    payload_binary_bytes_total: int
    payload_total_bytes: int
    contains_binary_refs: bool
    map_kind: Literal["none", "full", "partial"] | None
    map_status: Literal["pending", "ready", "failed", "stale"] | None
    index_status: Literal["off", "pending", "ready", "partial", "failed"] | None


# ---------------------------------------------------------------------------
# Cache reuse info (Addendum A.1.1)
# ---------------------------------------------------------------------------
class CacheInfo(BaseModel):
    """Describes whether (and why) an existing artifact was reused."""

    model_config = {"extra": "forbid"}

    reused: bool
    reuse_reason: Literal["none", "request_key", "dedupe_alias"] = "none"
    reused_artifact_id: str | None = None


# ---------------------------------------------------------------------------
# Gateway tool result (Addendum A.1)
# ---------------------------------------------------------------------------
class GatewayToolResult(BaseModel):
    """Successful gateway response wrapping an artifact handle.

    Optionally includes an inline copy of the envelope (when the payload is
    small enough per the configured inline thresholds).
    """

    model_config = {"extra": "forbid"}

    type: Literal["gateway_tool_result"] = "gateway_tool_result"
    artifact: ArtifactHandle
    cache: CacheInfo | None = None
    inline: dict[str, Any] | None = None
    warnings: list[dict[str, Any]] = Field(default_factory=list)
    meta: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Gateway error (Addendum A.2)
# ---------------------------------------------------------------------------
class GatewayError(BaseModel):
    """Error response returned to MCP clients.

    ``code`` is one of the well-known gateway error codes; ``message``
    provides a human-readable explanation.
    """

    model_config = {"extra": "forbid"}

    type: Literal["gateway_error"] = "gateway_error"
    code: Literal[
        "INVALID_ARGUMENT",
        "NOT_FOUND",
        "GONE",
        "INTERNAL",
        "CURSOR_INVALID",
        "CURSOR_EXPIRED",
        "CURSOR_STALE",
        "BUDGET_EXCEEDED",
        "UNSUPPORTED",
    ]
    message: str
    details: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Helper: quick error construction
# ---------------------------------------------------------------------------
def make_error(code: str, message: str, **details: Any) -> GatewayError:
    """Create a :class:`GatewayError` with the given code, message, and details.

    Example::

        err = make_error("NOT_FOUND", "Artifact art_abc123 not found")
    """
    return GatewayError(
        code=code,  # type: ignore[arg-type]
        message=message,
        details=details,
    )
