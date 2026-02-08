"""Convert raw upstream MCP tool responses into canonical Envelope objects.

Spec reference: §5 — normalisation of heterogeneous upstream content parts
into typed :class:`ContentPart` variants and error envelopes.
"""

from __future__ import annotations

from typing import Any

from mcp_artifact_gateway.envelope.model import (
    ContentPart,
    ContentPartBinaryRef,
    ContentPartJson,
    ContentPartResourceRef,
    ContentPartText,
    Envelope,
    EnvelopeMeta,
    ErrorBlock,
    UpstreamPagination,
)


# ---------------------------------------------------------------------------
# Internal: classify a single raw content dict into a typed ContentPart
# ---------------------------------------------------------------------------
def _classify_part(raw: dict[str, Any]) -> ContentPartJson | ContentPartText | ContentPartResourceRef | ContentPartBinaryRef:
    """Map a raw upstream MCP content dict to the appropriate ContentPart.

    The ``type`` field in *raw* determines which model is instantiated:

    * ``"json"`` -> :class:`ContentPartJson`
    * ``"text"`` -> :class:`ContentPartText`
    * ``"resource_ref"`` -> :class:`ContentPartResourceRef`
    * ``"binary_ref"`` -> :class:`ContentPartBinaryRef`

    Raises :class:`ValueError` for unrecognised types.
    """
    part_type = raw.get("type")

    if part_type == "json":
        return ContentPartJson(value=raw.get("value"))

    if part_type == "text":
        return ContentPartText(text=raw.get("text", ""))

    if part_type == "resource_ref":
        return ContentPartResourceRef(
            uri=raw["uri"],
            mime=raw.get("mime"),
            name=raw.get("name"),
            durability=raw.get("durability", "external_ref"),
            content_hash=raw.get("content_hash"),
        )

    if part_type == "binary_ref":
        return ContentPartBinaryRef(
            blob_id=raw["blob_id"],
            binary_hash=raw["binary_hash"],
            mime=raw.get("mime"),
            byte_count=raw["byte_count"],
        )

    raise ValueError(f"Unrecognised content part type: {part_type!r}")


def _classify_parts(raw_parts: list[dict[str, Any]]) -> list[ContentPartJson | ContentPartText | ContentPartResourceRef | ContentPartBinaryRef]:
    """Classify a list of raw content dicts."""
    return [_classify_part(part) for part in raw_parts]


def _build_meta(meta_dict: dict[str, Any] | None) -> EnvelopeMeta:
    """Construct an :class:`EnvelopeMeta` from an optional raw dict."""
    if meta_dict is None:
        return EnvelopeMeta()

    pagination_raw = meta_dict.get("upstream_pagination")
    pagination = UpstreamPagination(**pagination_raw) if pagination_raw else None

    return EnvelopeMeta(
        upstream_pagination=pagination,
        warnings=meta_dict.get("warnings", []),
    )


# ---------------------------------------------------------------------------
# Public: normalisation entry points
# ---------------------------------------------------------------------------
def normalize_success(
    upstream_instance_id: str,
    upstream_prefix: str,
    tool: str,
    mcp_result: list[dict[str, Any]],
    meta: dict[str, Any] | None = None,
) -> Envelope:
    """Normalise a successful upstream MCP tool response into an :class:`Envelope`.

    Parameters
    ----------
    upstream_instance_id:
        Stable identifier for the upstream server instance (§4.3).
    upstream_prefix:
        The namespace prefix that was stripped from the tool name (§4.1).
    tool:
        The original (un-prefixed) upstream tool name.
    mcp_result:
        Raw content parts returned by the upstream MCP ``tools/call`` response.
        Each element is a dict with at least a ``type`` key.
    meta:
        Optional envelope metadata (pagination, warnings).
    """
    content_parts = _classify_parts(mcp_result)
    envelope_meta = _build_meta(meta)

    return Envelope(
        upstream_instance_id=upstream_instance_id,
        upstream_prefix=upstream_prefix,
        tool=tool,
        status="ok",
        content=content_parts,
        error=None,
        meta=envelope_meta,
    )


def normalize_error(
    upstream_instance_id: str,
    upstream_prefix: str,
    tool: str,
    error_code: str,
    message: str,
    retryable: bool = False,
    upstream_trace_id: str | None = None,
    details: dict[str, Any] | None = None,
    partial_content: list[dict[str, Any]] | None = None,
) -> Envelope:
    """Create an error :class:`Envelope`.

    Parameters
    ----------
    error_code:
        One of the ``ErrorBlock.code`` literals
        (``UPSTREAM_TIMEOUT``, ``UPSTREAM_ERROR``, etc.).
    message:
        Human-readable error description.
    retryable:
        Whether the caller may retry the request.
    upstream_trace_id:
        Optional trace / request ID from the upstream server.
    details:
        Arbitrary key-value pairs for debugging.
    partial_content:
        If the upstream returned partial results before the error, they
        are included in the envelope's ``content`` list.
    """
    content_parts = _classify_parts(partial_content) if partial_content else []

    error_block = ErrorBlock(
        code=error_code,  # type: ignore[arg-type]
        message=message,
        retryable=retryable,
        upstream_trace_id=upstream_trace_id,
        details=details or {},
    )

    return Envelope(
        upstream_instance_id=upstream_instance_id,
        upstream_prefix=upstream_prefix,
        tool=tool,
        status="error",
        content=content_parts,
        error=error_block,
    )


def normalize_timeout(
    upstream_instance_id: str,
    upstream_prefix: str,
    tool: str,
    timeout_seconds: float,
) -> Envelope:
    """Create an :class:`Envelope` for an upstream timeout.

    Sets ``error.code`` to ``UPSTREAM_TIMEOUT`` and marks the error as
    retryable.
    """
    return normalize_error(
        upstream_instance_id=upstream_instance_id,
        upstream_prefix=upstream_prefix,
        tool=tool,
        error_code="UPSTREAM_TIMEOUT",
        message=f"Upstream did not respond within {timeout_seconds}s",
        retryable=True,
        details={"timeout_seconds": timeout_seconds},
    )


def normalize_transport_error(
    upstream_instance_id: str,
    upstream_prefix: str,
    tool: str,
    message: str,
) -> Envelope:
    """Create an :class:`Envelope` for a transport-level failure.

    Sets ``error.code`` to ``TRANSPORT_ERROR`` and marks the error as
    retryable (transient network issues typically warrant a retry).
    """
    return normalize_error(
        upstream_instance_id=upstream_instance_id,
        upstream_prefix=upstream_prefix,
        tool=tool,
        error_code="TRANSPORT_ERROR",
        message=message,
        retryable=True,
    )
