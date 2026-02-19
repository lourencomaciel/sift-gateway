"""Build bounded retrieval responses with item and byte budgets.

Apply deterministic truncation to result item sequences,
enforcing both item count and byte size limits.  Key export
is ``apply_output_budgets``.
"""

from __future__ import annotations

from collections.abc import Sequence
import json
from typing import Any

from sift_gateway.canon.rfc8785 import canonical_bytes


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
