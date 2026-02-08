"""Standard bounded retrieval response shape."""

from __future__ import annotations

import json
from typing import Any, Sequence

from mcp_artifact_gateway.canon.rfc8785 import canonical_bytes


def apply_output_budgets(
    items: Sequence[Any],
    *,
    max_items: int,
    max_bytes_out: int,
) -> tuple[list[Any], bool, int, int]:
    """Deterministically truncate by item and byte budgets."""
    selected: list[Any] = []
    used_bytes = 0

    for item in items:
        if len(selected) >= max_items:
            break
        try:
            item_bytes = len(json.dumps(item, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
        except TypeError:
            # Fallback for Decimal-safe canonical payloads that stdlib json cannot encode.
            item_bytes = len(canonical_bytes(item))
        if selected and used_bytes + item_bytes > max_bytes_out:
            break
        if not selected and item_bytes > max_bytes_out:
            # Always allow at least one item so callers can return context.
            selected.append(item)
            used_bytes += item_bytes
            break
        selected.append(item)
        used_bytes += item_bytes

    omitted = len(items) - len(selected)
    truncated = omitted > 0
    return selected, truncated, omitted, used_bytes


def build_retrieval_response(
    *,
    items: list[Any],
    truncated: bool,
    cursor: str | None,
    omitted: int = 0,
    stats: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if truncated and not cursor:
        msg = "cursor is required when response is truncated"
        raise ValueError(msg)

    return {
        "items": items,
        "truncated": truncated,
        "cursor": cursor if truncated else None,
        "omitted": omitted,
        "stats": stats or {},
    }
