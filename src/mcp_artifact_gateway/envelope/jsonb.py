"""JSONB storage mode handling for ``payload_blobs.envelope`` column.

Spec reference: §7.3, §8.4 — the ``envelope_jsonb_mode`` configuration
controls whether the full envelope, a minimal projection, or nothing is
stored in the Postgres JSONB column alongside the canonical binary blob.

Modes
-----
* **full** — store the complete envelope as-is.
* **minimal_for_large** — store a minimal projection when the JSON payload
  exceeds ``threshold_bytes``; otherwise store the full envelope.
* **none** — always store the minimal projection (useful for very
  resource-constrained deployments).
"""

from __future__ import annotations

from typing import Any

from mcp_artifact_gateway.envelope.model import (
    ContentPartBinaryRef,
    ContentPartJson,
    ContentPartResourceRef,
    ContentPartText,
    Envelope,
    _deep_convert,
)


# ---------------------------------------------------------------------------
# Minimal projection
# ---------------------------------------------------------------------------
def minimal_projection(envelope: Envelope) -> dict[str, Any]:
    """Return a minimal JSONB projection of the envelope.

    The projection retains:

    * Top-level scalars: ``type``, ``upstream_instance_id``,
      ``upstream_prefix``, ``tool``, ``status``.
    * Content *descriptors* only — each part is reduced to its type,
      structural identifiers, sizes, and MIME type.  **Actual values**
      (``ContentPartJson.value``, ``ContentPartText.text``) are omitted.
    * Error summary fields (if present).
    * ``meta.warnings`` and ``meta.upstream_pagination``.
    """
    projection: dict[str, Any] = {
        "type": envelope.type,
        "upstream_instance_id": envelope.upstream_instance_id,
        "upstream_prefix": envelope.upstream_prefix,
        "tool": envelope.tool,
        "status": envelope.status,
    }

    # -- Content descriptors ------------------------------------------------
    content_descriptors: list[dict[str, Any]] = []
    for part in envelope.content:
        descriptor = _content_descriptor(part)
        content_descriptors.append(descriptor)
    projection["content"] = content_descriptors

    # -- Error summary ------------------------------------------------------
    if envelope.error is not None:
        projection["error"] = {
            "code": envelope.error.code,
            "message": envelope.error.message,
            "retryable": envelope.error.retryable,
        }
        if envelope.error.upstream_trace_id is not None:
            projection["error"]["upstream_trace_id"] = envelope.error.upstream_trace_id

    # -- Meta ---------------------------------------------------------------
    meta_dict: dict[str, Any] = {}

    if envelope.meta.warnings:
        meta_dict["warnings"] = envelope.meta.warnings

    if envelope.meta.upstream_pagination is not None:
        pagination = envelope.meta.upstream_pagination
        meta_dict["upstream_pagination"] = {
            "next_cursor": pagination.next_cursor,
            "has_more": pagination.has_more,
            "total": pagination.total,
        }

    if meta_dict:
        projection["meta"] = meta_dict

    return projection


# ---------------------------------------------------------------------------
# Content descriptor helpers
# ---------------------------------------------------------------------------
def _content_descriptor(
    part: ContentPartJson | ContentPartText | ContentPartResourceRef | ContentPartBinaryRef,
) -> dict[str, Any]:
    """Reduce a typed content part to a lightweight descriptor dict."""
    if isinstance(part, ContentPartJson):
        return {"type": "json"}

    if isinstance(part, ContentPartText):
        return {"type": "text"}

    if isinstance(part, ContentPartResourceRef):
        desc: dict[str, Any] = {
            "type": "resource_ref",
            "uri": part.uri,
            "durability": part.durability,
        }
        if part.mime is not None:
            desc["mime"] = part.mime
        if part.content_hash is not None:
            desc["content_hash"] = part.content_hash
        return desc

    if isinstance(part, ContentPartBinaryRef):
        desc = {
            "type": "binary_ref",
            "blob_id": part.blob_id,
            "binary_hash": part.binary_hash,
            "byte_count": part.byte_count,
        }
        if part.mime is not None:
            desc["mime"] = part.mime
        return desc

    # Defensive fallback — should never be reached.
    return {"type": "unknown"}  # pragma: no cover


# ---------------------------------------------------------------------------
# Public: prepare envelope for JSONB storage
# ---------------------------------------------------------------------------
def prepare_envelope_jsonb(
    envelope: Envelope,
    mode: str,
    threshold_bytes: int,
    payload_json_bytes: int,
) -> dict[str, Any]:
    """Prepare an envelope dict for storage in the ``envelope`` JSONB column.

    Parameters
    ----------
    envelope:
        The fully populated :class:`Envelope` to store.
    mode:
        One of ``"full"``, ``"minimal_for_large"``, or ``"none"``.
        Corresponds to :attr:`GatewayConfig.envelope_jsonb_mode`.
    threshold_bytes:
        Byte threshold used by the ``"minimal_for_large"`` mode.
        Corresponds to :attr:`GatewayConfig.envelope_jsonb_minimize_threshold_bytes`.
    payload_json_bytes:
        Estimated byte count of the canonical JSON payload for this envelope.
    """
    if mode == "full":
        return _deep_convert(envelope.to_dict())

    if mode == "minimal_for_large":
        if payload_json_bytes > threshold_bytes:
            return minimal_projection(envelope)
        return _deep_convert(envelope.to_dict())

    # mode == "none" (or any unrecognised value — default to minimal)
    return minimal_projection(envelope)
