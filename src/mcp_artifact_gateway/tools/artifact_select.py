"""artifact.select tool implementation.

Supports both *full* and *sampled-only* retrieval modes:

- **Full mode** (``map_kind == "full"``): evaluates the root_path against the
  full envelope, enumerates records in ascending index order, applies where
  filtering, then projects select_paths.
- **Sampled-only mode** (``map_kind == "partial"``): iterates pre-materialised
  sample rows in ascending ``sample_index`` order (guaranteed by SQL
  ``ORDER BY sample_index ASC``), applies where filtering, then projects
  select_paths.

Both modes honour the traversal_v1 determinism contract.
"""

from __future__ import annotations

from typing import Any, Sequence

from mcp_artifact_gateway.constants import WORKSPACE_ID


def validate_select_args(arguments: dict[str, Any]) -> dict[str, Any] | None:
    """Validate artifact.select arguments. Returns error dict or None."""
    ctx = arguments.get("_gateway_context")
    if not isinstance(ctx, dict) or not ctx.get("session_id"):
        return {
            "code": "INVALID_ARGUMENT",
            "message": "missing _gateway_context.session_id",
        }

    if not arguments.get("artifact_id"):
        return {"code": "INVALID_ARGUMENT", "message": "missing artifact_id"}

    if not arguments.get("root_path"):
        return {"code": "INVALID_ARGUMENT", "message": "missing root_path"}

    select_paths = arguments.get("select_paths")
    if not isinstance(select_paths, list) or not select_paths:
        return {
            "code": "INVALID_ARGUMENT",
            "message": "select_paths must be a non-empty list",
        }

    # Validate select_paths don't start with $ (must be relative)
    for path in select_paths:
        if isinstance(path, str) and path.startswith("$"):
            return {
                "code": "INVALID_ARGUMENT",
                "message": f"select_path must be relative (no $): {path}",
            }

    return None


# SQL for fetching root info
FETCH_ROOT_SQL = """
SELECT root_key, root_path, count_estimate, root_shape,
       fields_top, sample_indices, root_summary
FROM artifact_roots
WHERE workspace_id = %s AND artifact_id = %s AND root_path = %s
"""

# SQL for fetching samples for a root
FETCH_SAMPLES_SQL = """
SELECT sample_index, record, record_bytes, record_hash
FROM artifact_samples
WHERE workspace_id = %s AND artifact_id = %s AND root_key = %s
ORDER BY sample_index ASC
"""


def sampled_indices_ascending(sample_rows: Sequence[dict[str, Any]]) -> list[int]:
    """Extract and return sample indices in ascending order from sample rows.

    This enforces the traversal_v1 contract for partial/sampled mode:
    sampled indices are always enumerated in ascending order.
    """
    return sorted(
        int(idx)
        for row in sample_rows
        if isinstance((idx := row.get("sample_index")), int)
    )


def build_select_result(
    *,
    items: list[dict[str, Any]],
    truncated: bool,
    cursor: str | None,
    sampled_only: bool = False,
    sample_indices_used: list[int] | None = None,
    sampled_prefix_len: int | None = None,
    omitted: dict[str, Any] | None = None,
    stats: dict[str, Any] | None = None,
    determinism: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build artifact.select response."""
    result: dict[str, Any] = {
        "items": items,
        "truncated": truncated,
    }
    if cursor:
        result["cursor"] = cursor
    if omitted:
        result["omitted"] = omitted
    if stats:
        result["stats"] = stats
    if sampled_only:
        result["sampled_only"] = True
        if sample_indices_used is not None:
            result["sample_indices_used"] = sample_indices_used
        if sampled_prefix_len is not None:
            result["sampled_prefix_len"] = sampled_prefix_len
    if determinism:
        result["determinism"] = determinism
    return result
