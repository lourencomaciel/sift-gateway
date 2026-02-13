"""Validate arguments and build responses for ``artifact.chain_pages``.

Return child artifacts of a parent in chain-sequence order, useful
for paginated or multi-part upstream responses.  Exports
``validate_chain_pages_args``, ``build_chain_pages_response``, and
fetch SQL constants.

Typical usage example::

    error = validate_chain_pages_args(arguments)
    if error:
        return error
    response = build_chain_pages_response(rows, truncated=False)
"""

from __future__ import annotations

from typing import Any

from sift_mcp.pagination.contract import (
    build_retrieval_pagination_meta,
)


def validate_chain_pages_args(
    arguments: dict[str, Any],
) -> dict[str, Any] | None:
    """Validate ``artifact.chain_pages`` arguments.

    Args:
        arguments: Raw tool arguments including gateway context
            and ``parent_artifact_id``.

    Returns:
        Error dict on validation failure, ``None`` when valid.
    """
    ctx = arguments.get("_gateway_context")
    if not isinstance(ctx, dict) or not ctx.get("session_id"):
        return {
            "code": "INVALID_ARGUMENT",
            "message": "missing _gateway_context.session_id",
        }

    if not arguments.get("parent_artifact_id"):
        return {
            "code": "INVALID_ARGUMENT",
            "message": "missing parent_artifact_id",
        }

    return None


# SQL for chain pages - ordered by chain_seq ASC, then created_seq ASC
FETCH_CHAIN_PAGES_SQL = """
SELECT a.artifact_id, a.created_seq, a.created_at, a.chain_seq,
       a.source_tool, a.payload_total_bytes, a.map_kind, a.map_status
FROM artifacts a
WHERE a.workspace_id = %s
  AND a.parent_artifact_id = %s
  AND a.deleted_at IS NULL
ORDER BY a.chain_seq ASC NULLS LAST, a.created_seq ASC
LIMIT %s OFFSET %s
"""

# SQL for allocating chain_seq with retry
ALLOCATE_CHAIN_SEQ_SQL = """
SELECT COALESCE(MAX(chain_seq), -1) + 1 AS next_seq
FROM artifacts
WHERE workspace_id = %s AND parent_artifact_id = %s
"""


def build_chain_pages_response(
    rows: list[dict[str, Any]],
    *,
    truncated: bool = False,
    cursor: str | None = None,
) -> dict[str, Any]:
    """Build the ``artifact.chain_pages`` response dict.

    Args:
        rows: Child artifact row dicts ordered by
            ``chain_seq`` ascending.
        truncated: Whether more pages exist beyond this batch.
        cursor: Opaque pagination cursor, or ``None``.

    Returns:
        Response dict with ``items``, ``truncated``, and
        ``cursor`` keys.
    """
    return {
        "items": [
            {
                "artifact_id": row["artifact_id"],
                "created_seq": row["created_seq"],
                "created_at": str(row["created_at"]),
                "chain_seq": row.get("chain_seq"),
                "source_tool": row.get("source_tool"),
                "payload_total_bytes": row.get("payload_total_bytes"),
                "map_kind": row.get("map_kind"),
                "map_status": row.get("map_status"),
            }
            for row in rows
        ],
        "truncated": truncated,
        "cursor": cursor,
        "pagination": build_retrieval_pagination_meta(
            truncated=truncated,
            cursor=cursor if cursor else None,
        ),
    }
