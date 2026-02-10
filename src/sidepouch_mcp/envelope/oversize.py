"""Replace oversized JSON content parts with binary blob refs.

Scan envelope content parts during ingest and replace any
``JsonContentPart`` whose canonical encoding exceeds the
configured byte threshold with a ``BinaryRefContentPart``
pointing to the blob store.  Emit warnings in the envelope
meta for each replaced part.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from sidepouch_mcp.canon.rfc8785 import canonical_bytes
from sidepouch_mcp.envelope.model import (
    BinaryRefContentPart,
    Envelope,
    JsonContentPart,
)
from sidepouch_mcp.fs.blob_store import BlobStore


def replace_oversized_json_parts(
    envelope: Envelope,
    *,
    max_json_part_parse_bytes: int,
    blob_store: BlobStore,
) -> Envelope:
    """Replace oversized JSON parts with binary blob refs.

    Scan each JsonContentPart and, if its canonical encoding
    exceeds the byte threshold, store the bytes in the blob
    store and substitute a BinaryRefContentPart.  Append an
    ``oversized_json_part`` warning to ``meta.warnings`` for
    each replacement.

    Args:
        envelope: Source envelope (not mutated).
        max_json_part_parse_bytes: Byte threshold above which
            a JSON part is considered oversized.
        blob_store: Blob store for writing oversized content.

    Returns:
        A new Envelope with oversized parts replaced and
        warnings appended.
    """
    warnings: list[dict[str, Any]] = list(envelope.meta.get("warnings", []))
    next_content = []

    for idx, part in enumerate(envelope.content):
        if isinstance(part, JsonContentPart):
            encoded = canonical_bytes(part.value)
            if len(encoded) > max_json_part_parse_bytes:
                blob_ref = blob_store.put_bytes(
                    encoded, mime="application/json"
                )
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
