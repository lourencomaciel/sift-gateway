"""Normalize upstream responses into gateway envelope shape."""

from __future__ import annotations

from typing import Any, Mapping

from mcp_artifact_gateway.constants import RESERVED_EXACT_KEYS, RESERVED_PREFIX
from mcp_artifact_gateway.envelope.model import (
    BinaryRefContentPart,
    ContentPart,
    Envelope,
    ErrorBlock,
    JsonContentPart,
    ResourceRefContentPart,
    TextContentPart,
)


def strip_reserved_args(args: Mapping[str, Any]) -> dict[str, Any]:
    """Remove gateway-reserved keys before forwarding upstream."""
    return {
        key: value
        for key, value in args.items()
        if key not in RESERVED_EXACT_KEYS and not key.startswith(RESERVED_PREFIX)
    }


def _normalize_error(raw: Mapping[str, Any]) -> ErrorBlock:
    return ErrorBlock(
        code=str(raw.get("code", "UPSTREAM_ERROR")),
        message=str(raw.get("message", "unknown upstream error")),
        retryable=bool(raw.get("retryable", False)),
        upstream_trace_id=raw.get("upstream_trace_id"),
        details=dict(raw.get("details", {})) if isinstance(raw.get("details"), Mapping) else {},
    )


def _normalize_part(raw: Mapping[str, Any]) -> ContentPart:
    part_type = raw.get("type")

    if part_type == "json":
        return JsonContentPart(value=raw.get("value"))
    if part_type == "text":
        return TextContentPart(text=str(raw.get("text", "")))
    if part_type == "resource_ref":
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
    if part_type in {"binary_ref", "image_ref"}:
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
) -> Envelope:
    """Build normalized envelope with status invariants enforced."""
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

    return Envelope(
        upstream_instance_id=upstream_instance_id,
        upstream_prefix=upstream_prefix,
        tool=tool,
        status=inferred_status,
        content=normalized_content,
        error=normalized_error,
        meta=normalized_meta,
    )
