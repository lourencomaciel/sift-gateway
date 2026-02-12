"""Validate arguments and build responses for ``artifact.select``.

Project and filter artifact data with bounded traversal in two
modes: full (evaluate root_path against the complete envelope) and
sampled-only (iterate pre-materialised sample rows).  Both modes
honour the traversal_v1 determinism contract.  Exports
``validate_select_args``, ``build_select_result``, and fetch SQL.

Typical usage example::

    error = validate_select_args(arguments)
    if error:
        return error
    result = build_select_result(items=items, truncated=False)
"""

from __future__ import annotations

from typing import Any, Sequence

from sidepouch_mcp.pagination.contract import (
    build_retrieval_pagination_meta,
)


def validate_select_args(arguments: dict[str, Any]) -> dict[str, Any] | None:
    """Validate ``artifact.select`` arguments.

    Checks for required gateway context, ``artifact_id``,
    ``root_path``, and a non-empty ``select_paths`` list with
    relative (non-``$``) paths.

    When a ``cursor`` is present, ``root_path``, ``select_paths``,
    and ``where`` are optional — they will be extracted from the
    signed cursor payload by the handler.

    Args:
        arguments: Raw tool arguments.

    Returns:
        Error dict on validation failure, ``None`` when valid.
    """
    ctx = arguments.get("_gateway_context")
    if not isinstance(ctx, dict) or not ctx.get("session_id"):
        return {
            "code": "INVALID_ARGUMENT",
            "message": "missing _gateway_context.session_id",
        }

    if not arguments.get("artifact_id"):
        return {"code": "INVALID_ARGUMENT", "message": "missing artifact_id"}

    has_cursor = isinstance(arguments.get("cursor"), str) and bool(
        arguments["cursor"]
    )
    count_only = arguments.get("count_only") is True

    if not arguments.get("root_path") and not has_cursor:
        return {"code": "INVALID_ARGUMENT", "message": "missing root_path"}

    select_paths = arguments.get("select_paths")
    if not has_cursor and not count_only:
        if not isinstance(select_paths, list) or not select_paths:
            return {
                "code": "INVALID_ARGUMENT",
                "message": "select_paths must be a non-empty list",
            }

    # Validate individual select_paths entries.
    if isinstance(select_paths, list):
        for path in select_paths:
            if not isinstance(path, str):
                continue
            if path == "*":
                return {
                    "code": "INVALID_ARGUMENT",
                    "message": (
                        "Wildcard '*' is not supported in "
                        "select_paths. Use explicit field names "
                        "(e.g. ['ad_name', 'spend']). Run "
                        "artifact(action='describe') to see "
                        "available fields."
                    ),
                }
            if path.startswith("$"):
                return {
                    "code": "INVALID_ARGUMENT",
                    "message": (f"select_path must be relative (no $): {path}"),
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


def sampled_indices_ascending(
    sample_rows: Sequence[dict[str, Any]],
) -> list[int]:
    """Extract sample indices in ascending order from sample rows.

    Enforces the traversal_v1 contract: sampled indices are
    always enumerated in ascending order.

    Args:
        sample_rows: Sequence of sample row dicts, each
            containing a ``sample_index`` key.

    Returns:
        Sorted list of integer sample indices.
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
    total_matched: int | None = None,
    sampled_only: bool = False,
    sample_indices_used: list[int] | None = None,
    sampled_prefix_len: int | None = None,
    omitted: dict[str, Any] | None = None,
    stats: dict[str, Any] | None = None,
    determinism: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build the ``artifact.select`` response dict.

    Args:
        items: Projected records matching the select criteria.
        truncated: Whether the result set was truncated by
            budget limits.
        cursor: Opaque pagination cursor, or ``None``.
        total_matched: Total number of records that passed
            the where filter, before pagination/truncation.
        sampled_only: Whether results come from partial
            (sampled) data rather than the full envelope.
        sample_indices_used: Indices of samples that
            contributed to the result.
        sampled_prefix_len: Length of the contiguous prefix
            of sampled records.
        omitted: Dict describing omitted fields or records.
        stats: Traversal statistics dict.
        determinism: Dict with determinism contract metadata.

    Returns:
        Structured response dict for the ``artifact.select``
        tool.
    """
    result: dict[str, Any] = {
        "items": items,
        "truncated": truncated,
        "pagination": build_retrieval_pagination_meta(
            truncated=truncated,
            cursor=cursor if cursor else None,
        ),
    }
    if total_matched is not None:
        result["total_matched"] = total_matched
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
