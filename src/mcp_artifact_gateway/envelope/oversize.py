"""Handle oversized JSON content parts that exceed max_json_part_parse_bytes.

Spec reference: §16.1 — when a JSON content part's raw byte size exceeds
the configured ``max_json_part_parse_bytes`` threshold, the raw bytes are
offloaded to the binary blob store and the content part is replaced with a
:class:`ContentPartBinaryRef`.

This avoids parsing extremely large JSON payloads into Python objects, which
would risk excessive memory consumption and slow serialisation.
"""

from __future__ import annotations

import json
from typing import Any, Callable, Awaitable

from mcp_artifact_gateway.envelope.model import (
    ContentPart,
    ContentPartBinaryRef,
    ContentPartJson,
    ContentPartResourceRef,
    ContentPartText,
)
from mcp_artifact_gateway.fs.blob_store import BinaryRef


# ---------------------------------------------------------------------------
# Size estimation
# ---------------------------------------------------------------------------
def estimate_json_part_bytes(part: dict[str, Any]) -> int:
    """Estimate the byte size of a JSON content part from raw upstream data.

    The heuristic works as follows:

    * If ``part["value"]`` is already ``bytes``, return ``len(value)``.
    * If it is a ``str``, return the length of its UTF-8 encoding.
    * Otherwise (parsed object), fall back to ``json.dumps`` and measure.
    """
    value = part.get("value")

    if isinstance(value, bytes):
        return len(value)

    if isinstance(value, str):
        return len(value.encode("utf-8"))

    # Fallback: serialise the parsed object to get a byte-length estimate.
    # This is necessarily approximate (whitespace, key ordering) but provides
    # a reasonable upper bound.
    try:
        return len(json.dumps(value, ensure_ascii=False).encode("utf-8"))
    except (TypeError, ValueError, OverflowError):
        # If serialisation fails (e.g. non-serialisable nested objects),
        # return 0 so the part is kept inline rather than silently dropped.
        return 0


# ---------------------------------------------------------------------------
# Oversize offloading
# ---------------------------------------------------------------------------
async def check_and_offload_oversize_json(
    content_parts: list[dict[str, Any]],
    max_json_part_parse_bytes: int,
    blob_store_put_fn: Callable[[bytes, str | None], Awaitable[BinaryRef]],
) -> tuple[list[ContentPartJson | ContentPartText | ContentPartResourceRef | ContentPartBinaryRef], list[dict[str, Any]]]:
    """Examine each content part and offload oversized JSON to the blob store.

    Parameters
    ----------
    content_parts:
        Raw content dicts from the upstream MCP response.
    max_json_part_parse_bytes:
        Threshold in bytes.  JSON parts whose estimated size exceeds this
        value are offloaded instead of parsed.
    blob_store_put_fn:
        An async callable with signature ``(raw_bytes, mime) -> BinaryRef``.
        Typically :meth:`BlobStore.put_bytes`.

    Returns
    -------
    (converted_parts, warnings)
        *converted_parts* is the list of typed :class:`ContentPart` objects
        (with oversized JSON parts replaced by :class:`ContentPartBinaryRef`).
        *warnings* collects one warning dict per offloaded part.
    """
    converted: list[ContentPartJson | ContentPartText | ContentPartResourceRef | ContentPartBinaryRef] = []
    warnings: list[dict[str, Any]] = []

    for i, part in enumerate(content_parts):
        part_type = part.get("type")

        if part_type != "json":
            # Non-JSON parts pass through unchanged — use the normalisation
            # layer for type dispatch.
            converted.append(_passthrough_part(part))
            continue

        byte_size = estimate_json_part_bytes(part)

        if byte_size <= max_json_part_parse_bytes:
            # Within budget — keep the JSON value inline.
            converted.append(ContentPartJson(value=part.get("value")))
            continue

        # ----- Oversize: offload raw bytes to the blob store -----
        raw_bytes = _raw_bytes_for_json(part)
        ref: BinaryRef = await blob_store_put_fn(raw_bytes, "application/json")

        converted.append(
            ContentPartBinaryRef(
                blob_id=ref.blob_id,
                binary_hash=ref.binary_hash,
                mime="application/json",
                byte_count=ref.byte_count,
            )
        )

        warnings.append(
            {
                "type": "oversize_json_offloaded",
                "original_part_index": i,
                "byte_count": ref.byte_count,
                "encoding": "utf-8",
            }
        )

    return converted, warnings


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
def _raw_bytes_for_json(part: dict[str, Any]) -> bytes:
    """Extract or produce raw UTF-8 bytes from a JSON content dict.

    Prefers already-encoded representations to avoid a round-trip through
    ``json.dumps``.
    """
    value = part.get("value")

    if isinstance(value, bytes):
        return value

    if isinstance(value, str):
        return value.encode("utf-8")

    # Parsed object — re-serialise.  ``ensure_ascii=False`` preserves non-ASCII
    # characters in their native form; ``separators`` produces compact output.
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _passthrough_part(
    raw: dict[str, Any],
) -> ContentPartJson | ContentPartText | ContentPartResourceRef | ContentPartBinaryRef:
    """Convert a non-JSON raw part dict to its typed model.

    Mirrors the classification logic in :mod:`normalize` but is kept inline
    here to avoid a circular import and to keep this module self-contained.
    """
    part_type = raw.get("type")

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

    # Fallback — treat as JSON (should not normally be reached for non-JSON).
    return ContentPartJson(value=raw.get("value"))
