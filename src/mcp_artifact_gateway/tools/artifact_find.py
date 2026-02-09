"""artifact.find tool implementation.

``artifact.find`` searches sample rows across mapped roots.  It operates
in *sampled-only* mode unless a full-text index is available (``index_status
== "ready"``).

In sampled-only mode the traversal contract is:

- Sample rows are fetched ``ORDER BY sample_index ASC`` (SQL guarantee).
- The ``sampled_only`` flag is set on the response so callers know only a
  subset of the data was searched.

This ensures determinism consistent with the traversal_v1 contract:
sampled indices ascending, same as ``traverse_sampled`` in
``retrieval.traversal``.
"""

from __future__ import annotations

from typing import Any, Sequence


def validate_find_args(arguments: dict[str, Any]) -> dict[str, Any] | None:
    """Validate artifact.find arguments."""
    ctx = arguments.get("_gateway_context")
    if not isinstance(ctx, dict) or not ctx.get("session_id"):
        return {
            "code": "INVALID_ARGUMENT",
            "message": "missing _gateway_context.session_id",
        }

    if not arguments.get("artifact_id"):
        return {"code": "INVALID_ARGUMENT", "message": "missing artifact_id"}

    return None


def sampled_indices_from_rows(sample_rows: Sequence[dict[str, Any]]) -> list[int]:
    """Extract sample indices in ascending order from find sample rows.

    This mirrors the same ascending-order guarantee used by
    ``artifact_select.sampled_indices_ascending`` and the SQL
    ``ORDER BY sample_index ASC``.
    """
    return sorted(
        int(idx)
        for row in sample_rows
        if isinstance((idx := row.get("sample_index")), int)
    )


def build_find_response(
    *,
    items: list[dict[str, Any]],
    truncated: bool,
    cursor: str | None = None,
    sampled_only: bool = True,
    index_status: str = "off",
    determinism: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build artifact.find response.

    Works in sample-only mode unless indexing is enabled.
    """
    result: dict[str, Any] = {
        "items": items,
        "truncated": truncated,
        "sampled_only": sampled_only and index_status != "ready",
    }
    if cursor:
        result["cursor"] = cursor
    if determinism:
        result["determinism"] = determinism
    return result
