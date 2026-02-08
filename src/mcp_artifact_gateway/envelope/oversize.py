"""Oversized JSON handling for envelope ingest."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from mcp_artifact_gateway.canon.rfc8785 import canonical_bytes
from mcp_artifact_gateway.envelope.model import BinaryRefContentPart, Envelope, JsonContentPart
from mcp_artifact_gateway.fs.blob_store import BlobStore


def replace_oversized_json_parts(
    envelope: Envelope,
    *,
    max_json_part_parse_bytes: int,
    blob_store: BlobStore,
) -> Envelope:
    """Replace oversized JSON parts with binary refs and emit warnings."""
    warnings: list[dict[str, Any]] = list(envelope.meta.get("warnings", []))
    next_content = []

    for idx, part in enumerate(envelope.content):
        if isinstance(part, JsonContentPart):
            encoded = canonical_bytes(part.value)
            if len(encoded) > max_json_part_parse_bytes:
                blob_ref = blob_store.put_bytes(encoded, mime="application/json")
                next_content.append(
                    BinaryRefContentPart(
                        blob_id=blob_ref.blob_id,
                        binary_hash=blob_ref.binary_hash,
                        mime="application/json",
                        byte_count=blob_ref.byte_count,
                    )
                )
                warnings.append(
                    {
                        "code": "oversized_json_part",
                        "part_index": idx,
                        "encoding": "utf-8",
                        "byte_count": len(encoded),
                    }
                )
                continue
        next_content.append(part)

    next_meta = dict(envelope.meta)
    next_meta["warnings"] = warnings
    return replace(envelope, content=next_content, meta=next_meta)

