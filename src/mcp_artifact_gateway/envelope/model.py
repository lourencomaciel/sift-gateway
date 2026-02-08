"""Typed Pydantic models for the canonical envelope format.

Spec reference: §5 — Envelope shape, content parts, error blocks,
upstream pagination, and validation invariants.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, model_validator


# ---------------------------------------------------------------------------
# Content parts (§5.2)
# ---------------------------------------------------------------------------
class ContentPartJson(BaseModel):
    """A JSON content part.  ``value`` may contain :class:`Decimal` instances."""

    model_config = {"extra": "forbid"}

    type: Literal["json"] = "json"
    value: Any


class ContentPartText(BaseModel):
    """A plain-text content part."""

    model_config = {"extra": "forbid"}

    type: Literal["text"] = "text"
    text: str


class ContentPartResourceRef(BaseModel):
    """A reference to an upstream MCP resource."""

    model_config = {"extra": "forbid"}

    type: Literal["resource_ref"] = "resource_ref"
    uri: str
    mime: str | None = None
    name: str | None = None
    durability: Literal["internal", "external_ref"]
    content_hash: str | None = None


class ContentPartBinaryRef(BaseModel):
    """A reference to a stored binary blob — binary bytes never appear inline."""

    model_config = {"extra": "forbid"}

    type: Literal["binary_ref"] = "binary_ref"
    blob_id: str
    binary_hash: str
    mime: str | None = None
    byte_count: int


# ---------------------------------------------------------------------------
# Discriminated union of all content part types
# ---------------------------------------------------------------------------
ContentPart = Annotated[
    ContentPartJson | ContentPartText | ContentPartResourceRef | ContentPartBinaryRef,
    Field(discriminator="type"),
]


# ---------------------------------------------------------------------------
# Error block (§5.3)
# ---------------------------------------------------------------------------
class ErrorBlock(BaseModel):
    """Structured error block attached to an error envelope."""

    model_config = {"extra": "forbid"}

    code: Literal[
        "UPSTREAM_TIMEOUT",
        "UPSTREAM_ERROR",
        "TRANSPORT_ERROR",
        "INVALID_RESPONSE",
        "INTERNAL",
    ]
    message: str
    retryable: bool = False
    upstream_trace_id: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Upstream pagination (§5.4)
# ---------------------------------------------------------------------------
class UpstreamPagination(BaseModel):
    """Pagination metadata forwarded from the upstream MCP server."""

    model_config = {"extra": "forbid"}

    next_cursor: str | None = None
    has_more: bool = False
    total: int | None = None


# ---------------------------------------------------------------------------
# Envelope meta (§5.5)
# ---------------------------------------------------------------------------
class EnvelopeMeta(BaseModel):
    """Non-content metadata carried on the envelope."""

    model_config = {"extra": "forbid"}

    upstream_pagination: UpstreamPagination | None = None
    warnings: list[dict[str, Any]] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Decimal-aware serialisation helper
# ---------------------------------------------------------------------------
def _decimal_safe_default(obj: object) -> object:
    """JSON default handler that keeps :class:`Decimal` as-is for dicts.

    Used by :meth:`Envelope.to_dict` — we walk the tree manually so this is
    only a fallback for objects that ``json`` cannot handle natively.
    """
    if isinstance(obj, Decimal):
        # Return the Decimal unchanged — callers that need JSON strings can
        # use a custom encoder; to_dict preserves Decimal natively.
        return obj  # pragma: no cover – only hit when json.dumps is used
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _deep_convert(obj: Any) -> Any:
    """Recursively convert a Pydantic-serialised structure to plain dicts/lists.

    :class:`Decimal` values are preserved as-is (never cast to float).
    """
    if isinstance(obj, dict):
        return {k: _deep_convert(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_deep_convert(item) for item in obj]
    if isinstance(obj, Decimal):
        return obj
    return obj


# ---------------------------------------------------------------------------
# Envelope (§5.1)
# ---------------------------------------------------------------------------
class Envelope(BaseModel):
    """Canonical envelope wrapping one upstream MCP tool result.

    Invariants
    ----------
    * ``status == "ok"`` implies ``error is None``.
    * ``status == "error"`` implies ``error is not None``.
    * Binary bytes never appear inline — only ``binary_ref`` parts are allowed.
    """

    model_config = {"extra": "forbid"}

    type: Literal["mcp_envelope"] = "mcp_envelope"
    upstream_instance_id: str
    upstream_prefix: str
    tool: str
    status: Literal["ok", "error"]
    content: list[ContentPart] = Field(default_factory=list)
    error: ErrorBlock | None = None
    meta: EnvelopeMeta = Field(default_factory=EnvelopeMeta)

    # -- validators ---------------------------------------------------------

    @model_validator(mode="after")
    def _check_status_error_consistency(self) -> Envelope:
        if self.status == "ok" and self.error is not None:
            raise ValueError("status is 'ok' but an error block is present")
        if self.status == "error" and self.error is None:
            raise ValueError("status is 'error' but no error block is present")
        return self

    # -- serialisation ------------------------------------------------------

    def to_dict(self) -> dict:
        """Return a plain dict suitable for canonical serialisation.

        :class:`Decimal` values inside JSON content parts are preserved — they
        are **not** converted to ``float``.
        """
        raw = self.model_dump(mode="python")
        return _deep_convert(raw)
