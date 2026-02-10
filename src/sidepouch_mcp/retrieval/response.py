"""Build bounded retrieval responses with item and byte budgets.

Apply deterministic truncation to result item sequences,
enforcing both item count and byte size limits, then wrap
the output into the standard retrieval response dict with
cursor and stats.  Key exports are ``apply_output_budgets``
and ``build_retrieval_response``.
"""

from __future__ import annotations

import json
from typing import Any, Sequence

from sidepouch_mcp.canon.rfc8785 import canonical_bytes


def apply_output_budgets(
    items: Sequence[Any],
    *,
    max_items: int,
    max_bytes_out: int,
) -> tuple[list[Any], bool, int, int]:
    """Truncate items by item count and byte size budgets.

    Always include at least one item (even if oversized) so
    callers can return context.  Use compact JSON encoding
    to measure byte cost, falling back to RFC 8785 canonical
    bytes for Decimal-safe payloads.

    Args:
        items: Candidate items to include in the response.
        max_items: Maximum number of items to return.
        max_bytes_out: Maximum total byte budget.

    Returns:
        Tuple of (selected_items, truncated, omitted_count,
        used_bytes).
    """
    selected: list[Any] = []
    used_bytes = 0

    for item in items:
        if len(selected) >= max_items:
            break
        try:
            item_bytes = len(
                json.dumps(
                    item, ensure_ascii=False, separators=(",", ":")
                ).encode("utf-8")
            )
        except TypeError:
            # Fallback for Decimal-safe canonical payloads
            # that stdlib json cannot encode.
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
    """Build a standard retrieval response dict.

    Wrap items with truncation status, cursor, omitted
    count, and optional stats into the canonical response
    shape returned by retrieval tools.

    Args:
        items: Result items to include.
        truncated: Whether the result set was truncated.
        cursor: Opaque cursor token for pagination.
            Required when truncated is True.
        omitted: Number of items omitted by truncation.
        stats: Optional stats dict to include.

    Returns:
        Dict with items, truncated, cursor, omitted,
        and stats keys.

    Raises:
        ValueError: If truncated is True but no cursor
            is provided.
    """
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
