"""Validate arguments and build responses for ``artifact.find``.

Search sample rows across mapped roots in sampled-only mode (unless
a full-text index is ready).  Sample rows are fetched in ascending
``sample_index`` order per the traversal_v1 contract.  Exports
``validate_find_args``, ``build_find_response``, and helpers.

Typical usage example::

    error = validate_find_args(arguments)
    if error:
        return error
    response = build_find_response(items=matches, truncated=False)
"""

from __future__ import annotations

from typing import Any, Sequence


def validate_find_args(arguments: dict[str, Any]) -> dict[str, Any] | None:
    """Validate ``artifact.find`` arguments.

    Args:
        arguments: Raw tool arguments including gateway context
            and ``artifact_id``.

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

    return None


def sampled_indices_from_rows(
    sample_rows: Sequence[dict[str, Any]],
) -> list[int]:
    """Extract sample indices in ascending order.

    Mirrors the ascending-order guarantee of
    ``artifact_select.sampled_indices_ascending`` and the SQL
    ``ORDER BY sample_index ASC``.

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


def build_find_response(
    *,
    items: list[dict[str, Any]],
    truncated: bool,
    cursor: str | None = None,
    sampled_only: bool = True,
    index_status: str = "off",
    determinism: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build the ``artifact.find`` response dict.

    Operates in sample-only mode unless a full-text index is
    ready (``index_status == "ready"``).

    Args:
        items: Matched records from the find operation.
        truncated: Whether the result set was truncated.
        cursor: Opaque pagination cursor, or ``None``.
        sampled_only: Whether results come from sampled data.
        index_status: Full-text index readiness status.
        determinism: Dict with determinism contract metadata.

    Returns:
        Structured response dict for ``artifact.find``.
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
