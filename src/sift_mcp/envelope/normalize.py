"""Normalize upstream responses into gateway envelope shape.

Convert raw upstream MCP tool responses into validated,
typed ``Envelope`` instances.  Enforce status invariants,
strip reserved gateway keys from arguments, and delegate
oversized JSON parts to binary blob storage.  Key exports
are ``normalize_envelope`` and ``strip_reserved_args``.
"""

from __future__ import annotations

from typing import Any, Mapping

from sift_mcp.constants import RESERVED_EXACT_KEYS, RESERVED_PREFIX
from sift_mcp.envelope.model import (
    BinaryRefContentPart,
    ContentPart,
    Envelope,
    ErrorBlock,
    JsonContentPart,
    ResourceRefContentPart,
    TextContentPart,
)
from sift_mcp.envelope.oversize import replace_oversized_json_parts
from sift_mcp.fs.blob_store import BlobStore


def strip_reserved_args(args: Mapping[str, Any]) -> dict[str, Any]:
    """Remove gateway-reserved keys before forwarding upstream.

    Strip exact reserved key names and keys starting with the
    ``_gateway_`` prefix so they are excluded from upstream
    requests and request-key hashing.

    Args:
        args: Raw request arguments from the client.

    Returns:
        A new dict with all reserved keys removed.
    """
    return {
        key: value
        for key, value in args.items()
        if key not in RESERVED_EXACT_KEYS
        and not key.startswith(RESERVED_PREFIX)
    }


def _normalize_error(raw: Mapping[str, Any]) -> ErrorBlock:
    """Build an ErrorBlock from a raw error mapping.

    Args:
        raw: Upstream error dict with optional code, message,
            retryable, upstream_trace_id, and details fields.

    Returns:
        A validated ErrorBlock with defaults applied.
    """
    return ErrorBlock(
        code=str(raw.get("code", "UPSTREAM_ERROR")),
        message=str(raw.get("message", "unknown upstream error")),
        retryable=bool(raw.get("retryable", False)),
        upstream_trace_id=raw.get("upstream_trace_id"),
        details=dict(raw.get("details", {}))
        if isinstance(raw.get("details"), Mapping)
        else {},
    )


def _normalize_resource_ref_part(
    raw: Mapping[str, Any],
) -> ResourceRefContentPart:
    """Validate and build a resource-ref content part.

    Args:
        raw: Raw content part dict with uri and optional
            mime, name, durability, and content_hash fields.

    Returns:
        A validated ResourceRefContentPart.

    Raises:
        ValueError: If uri is missing or durability is invalid.
    """
    uri = raw.get("uri")
    if not isinstance(uri, str) or not uri:
        msg = "resource_ref part requires non-empty uri"
        raise ValueError(msg)
    durability = raw.get("durability", "external_ref")
    if durability not in {"internal", "external_ref"}:
        msg = f"invalid resource_ref durability: {durability}"
        raise ValueError(msg)
    return ResourceRefContentPart(
        uri=uri,
        mime=raw.get("mime"),
        name=raw.get("name"),
        durability=durability,
        content_hash=raw.get("content_hash"),
    )


def _normalize_binary_ref_part(
    raw: Mapping[str, Any],
) -> BinaryRefContentPart:
    """Validate and build a binary-ref content part.

    Args:
        raw: Raw content part dict with blob_id, binary_hash,
            mime, and byte_count fields.

    Returns:
        A validated BinaryRefContentPart.

    Raises:
        ValueError: If required fields are missing, empty, or
            have invalid types (e.g. inline bytes present).
    """
    if "bytes" in raw:
        msg = "binary bytes are not allowed inline in envelope"
        raise ValueError(msg)
    blob_id = raw.get("blob_id")
    if not isinstance(blob_id, str) or not blob_id:
        msg = "binary_ref part requires non-empty blob_id"
        raise ValueError(msg)
    binary_hash = raw.get("binary_hash")
    if not isinstance(binary_hash, str) or not binary_hash:
        msg = "binary_ref part requires non-empty binary_hash"
        raise ValueError(msg)
    mime = raw.get("mime", "application/octet-stream")
    if not isinstance(mime, str) or not mime:
        msg = "binary_ref part requires non-empty mime"
        raise ValueError(msg)
    byte_count = raw.get("byte_count")
    if not isinstance(byte_count, int) or byte_count < 0:
        msg = "binary_ref part requires non-negative integer byte_count"
        raise ValueError(msg)
    return BinaryRefContentPart(
        blob_id=blob_id,
        binary_hash=binary_hash,
        mime=mime,
        byte_count=byte_count,
    )


def _normalize_part(raw: Mapping[str, Any]) -> ContentPart:
    """Dispatch a raw content part dict to the correct type.

    Args:
        raw: Raw content part dict with a ``type`` discriminator.

    Returns:
        A typed ContentPart instance.

    Raises:
        ValueError: If the part type is unsupported.
    """
    part_type = raw.get("type")

    if part_type == "json":
        return JsonContentPart(value=raw.get("value"))
    if part_type == "text":
        return TextContentPart(
            text=str(raw.get("text", "")),
        )
    if part_type == "resource_ref":
        return _normalize_resource_ref_part(raw)
    if part_type in {"binary_ref", "image_ref"}:
        return _normalize_binary_ref_part(raw)

    msg = f"unsupported content part type: {part_type!r}"
    raise ValueError(msg)


def normalize_envelope(
    *,
    upstream_instance_id: str,
    upstream_prefix: str,
    tool: str,
    status: str | None = None,
    content: list[Mapping[str, Any]] | None = None,
    error: Mapping[str, Any] | None = None,
    meta: Mapping[str, Any] | None = None,
    max_json_part_parse_bytes: int | None = None,
    blob_store: BlobStore | None = None,
) -> Envelope:
    """Build a normalized Envelope with status invariants enforced.

    Infer status from error presence when not explicit, validate
    that status and error block are consistent, normalize each
    content part, and optionally replace oversized JSON parts
    with binary blob refs.

    Args:
        upstream_instance_id: Identity of the upstream server.
        upstream_prefix: Namespace prefix for the tool.
        tool: Bare upstream tool name.
        status: Explicit status ("ok" or "error"), or None to
            infer from error presence.
        content: Raw content part dicts to normalize.
        error: Raw error dict, or None for success.
        meta: Auxiliary metadata dict (warnings, etc.).
        max_json_part_parse_bytes: Byte threshold above which
            JSON parts are replaced with binary refs.  Requires
            blob_store to be set.
        blob_store: Blob store for oversized JSON replacement.

    Returns:
        A fully normalized, immutable Envelope.

    Raises:
        ValueError: If status is invalid, or if status/error
            consistency invariants are violated.
    """
    inferred_status = status or ("error" if error is not None else "ok")
    if inferred_status not in {"ok", "error"}:
        msg = f"invalid envelope status: {inferred_status}"
        raise ValueError(msg)

    normalized_error = _normalize_error(error) if error is not None else None
    if inferred_status == "ok" and normalized_error is not None:
        msg = "status=ok cannot include error block"
        raise ValueError(msg)
    if inferred_status == "error" and normalized_error is None:
        msg = "status=error requires error block"
        raise ValueError(msg)

    normalized_content: list[ContentPart] = []
    for raw_part in content or []:
        normalized_content.append(_normalize_part(raw_part))

    normalized_meta = dict(meta or {})
    warnings = normalized_meta.get("warnings")
    if warnings is None:
        normalized_meta["warnings"] = []

    envelope = Envelope(
        upstream_instance_id=upstream_instance_id,
        upstream_prefix=upstream_prefix,
        tool=tool,
        status=inferred_status,
        content=normalized_content,
        error=normalized_error,
        meta=normalized_meta,
    )

    # Apply oversize JSON handling when configured
    if max_json_part_parse_bytes is not None and blob_store is not None:
        envelope = replace_oversized_json_parts(
            envelope,
            max_json_part_parse_bytes=max_json_part_parse_bytes,
            blob_store=blob_store,
        )

    return envelope
