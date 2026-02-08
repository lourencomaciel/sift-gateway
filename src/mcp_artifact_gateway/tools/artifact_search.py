"""artifact.search tool implementation."""

from __future__ import annotations

from typing import Any

from mcp_artifact_gateway.constants import WORKSPACE_ID


# All Addendum B filters
SEARCH_FILTERS = {
    "include_deleted",
    "status",
    "source_tool_prefix",
    "source_tool",
    "upstream_instance_id",
    "request_key",
    "payload_hash_full",
    "parent_artifact_id",
    "has_binary_refs",
    "created_seq_min",
    "created_seq_max",
    "created_at_after",
    "created_at_before",
}


def validate_search_args(
    arguments: dict[str, Any], *, max_limit: int
) -> dict[str, Any]:
    """Validate and normalize search arguments.

    Requires _gateway_context.session_id.
    """
    ctx = arguments.get("_gateway_context")
    if not isinstance(ctx, dict) or not ctx.get("session_id"):
        return {
            "error": "INVALID_ARGUMENT",
            "message": "missing _gateway_context.session_id",
        }

    session_id = ctx["session_id"]
    filters = arguments.get("filters", {})
    if filters is None:
        filters = {}
    if not isinstance(filters, dict):
        return {
            "error": "INVALID_ARGUMENT",
            "message": "filters must be an object",
        }
    order_by = arguments.get("order_by", "created_seq_desc")
    limit = min(arguments.get("limit", 50), max_limit)
    cursor = arguments.get("cursor")

    if order_by not in ("created_seq_desc", "last_seen_desc"):
        return {
            "error": "INVALID_ARGUMENT",
            "message": f"invalid order_by: {order_by}",
        }

    status = filters.get("status")
    if status is not None and status not in ("ok", "error"):
        return {
            "error": "INVALID_ARGUMENT",
            "message": f"invalid status filter: {status}",
        }

    return {
        "session_id": session_id,
        "filters": filters,
        "order_by": order_by,
        "limit": limit,
        "cursor": cursor,
    }


def build_search_query(
    session_id: str,
    filters: dict[str, Any],
    order_by: str,
    limit: int,
    *,
    offset: int = 0,
) -> tuple[str, list[Any]]:
    """Build SQL query for artifact search using artifact_refs only.

    Search discovers artifacts ONLY through artifact_refs for the given session.
    """
    params: list[Any] = [WORKSPACE_ID, session_id]

    base = """
    SELECT a.artifact_id, a.created_seq, a.created_at,
           ar.last_seen_at, a.source_tool, a.upstream_instance_id,
           CASE WHEN a.error_summary IS NULL THEN 'ok' ELSE 'error' END AS status,
           a.payload_total_bytes, a.error_summary,
           a.map_kind, a.map_status
    FROM artifact_refs ar
    JOIN artifacts a ON a.workspace_id = ar.workspace_id AND a.artifact_id = ar.artifact_id
    WHERE ar.workspace_id = %s AND ar.session_id = %s
    """

    conditions: list[str] = []

    if not filters.get("include_deleted", False):
        conditions.append("a.deleted_at IS NULL")

    status = filters.get("status")
    if status == "error":
        conditions.append("a.error_summary IS NOT NULL")
    elif status == "ok":
        conditions.append("a.error_summary IS NULL")

    if filters.get("source_tool_prefix"):
        conditions.append("a.source_tool LIKE %s")
        # Escape LIKE wildcards in user input to prevent injection
        escaped = (
            filters["source_tool_prefix"]
            .replace("\\", "\\\\")
            .replace("%", "\\%")
            .replace("_", "\\_")
        )
        params.append(f"{escaped}.%")

    if filters.get("source_tool"):
        conditions.append("a.source_tool = %s")
        params.append(filters["source_tool"])

    if filters.get("upstream_instance_id"):
        conditions.append("a.upstream_instance_id = %s")
        params.append(filters["upstream_instance_id"])

    if filters.get("request_key"):
        conditions.append("a.request_key = %s")
        params.append(filters["request_key"])

    if filters.get("payload_hash_full"):
        conditions.append("a.payload_hash_full = %s")
        params.append(filters["payload_hash_full"])

    if filters.get("parent_artifact_id"):
        conditions.append("a.parent_artifact_id = %s")
        params.append(filters["parent_artifact_id"])

    if filters.get("has_binary_refs") is not None:
        conditions.append(
            "EXISTS (SELECT 1 FROM payload_blobs pb"
            " WHERE pb.workspace_id = a.workspace_id"
            " AND pb.payload_hash_full = a.payload_hash_full"
            " AND pb.contains_binary_refs = %s)"
        )
        params.append(filters["has_binary_refs"])

    if filters.get("created_seq_min") is not None:
        conditions.append("a.created_seq >= %s")
        params.append(filters["created_seq_min"])

    if filters.get("created_seq_max") is not None:
        conditions.append("a.created_seq <= %s")
        params.append(filters["created_seq_max"])

    if filters.get("created_at_after"):
        conditions.append("a.created_at >= %s")
        params.append(filters["created_at_after"])

    if filters.get("created_at_before"):
        conditions.append("a.created_at <= %s")
        params.append(filters["created_at_before"])

    if conditions:
        base += " AND " + " AND ".join(conditions)

    # Ordering
    if order_by == "created_seq_desc":
        base += " ORDER BY a.created_seq DESC"
    else:
        base += " ORDER BY ar.last_seen_at DESC"

    base += " LIMIT %s"
    params.append(limit + 1)  # fetch one extra for pagination detection
    if offset > 0:
        base += " OFFSET %s"
        params.append(offset)

    return base, params
