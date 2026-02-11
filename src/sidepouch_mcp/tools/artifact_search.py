"""Validate arguments and build SQL for ``artifact.search``.

Discover artifacts visible to a session through ``artifact_refs``,
with support for filtering by status, source tool, timestamps, and
other metadata columns.  Exports ``validate_search_args`` and
``build_search_query``.

Typical usage example::

    validated = validate_search_args(arguments, max_limit=200)
    sql, params = build_search_query(
        validated["session_id"],
        validated["filters"],
        validated["order_by"],
        validated["limit"],
    )
"""

from __future__ import annotations

from typing import Any

from sidepouch_mcp.constants import WORKSPACE_ID

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

_VALID_ORDER_BY = ("created_seq_desc", "last_seen_desc")
_VALID_STATUS = ("ok", "error")


def _invalid_arg(message: str) -> dict[str, Any]:
    """Build an INVALID_ARGUMENT error response.

    Args:
        message: Human-readable error description.

    Returns:
        Error dict with ``code`` and ``message`` keys.
    """
    return {
        "code": "INVALID_ARGUMENT",
        "message": message,
    }


def _validate_context(
    arguments: dict[str, Any],
) -> str | dict[str, Any]:
    """Extract and validate ``session_id`` from gateway context.

    Args:
        arguments: Raw tool arguments containing
            ``_gateway_context``.

    Returns:
        The ``session_id`` string, or an error dict when the
        context is missing or invalid.
    """
    ctx = arguments.get("_gateway_context")
    if not isinstance(ctx, dict) or not ctx.get("session_id"):
        return _invalid_arg("missing _gateway_context.session_id")
    return ctx["session_id"]


def _validate_filters(
    arguments: dict[str, Any],
) -> dict[str, Any] | tuple[dict[str, Any], None]:
    """Extract and validate the filters dict.

    Args:
        arguments: Raw tool arguments potentially containing
            a ``filters`` key.

    Returns:
        Validated filters dict, or a ``(error_dict, None)``
        tuple when validation fails.
    """
    filters = arguments.get("filters", {})
    if filters is None:
        return {}
    if not isinstance(filters, dict):
        return _invalid_arg("filters must be an object"), None
    unknown = set(filters.keys()) - SEARCH_FILTERS
    if unknown:
        return _invalid_arg(
            f"unknown filter keys: {', '.join(sorted(unknown))}"
        ), None
    return filters


def _validate_order_by(
    order_by: str,
) -> dict[str, Any] | None:
    """Validate the ``order_by`` field.

    Args:
        order_by: Requested ordering value.

    Returns:
        Error dict if the value is invalid, ``None`` otherwise.
    """
    if order_by not in _VALID_ORDER_BY:
        return _invalid_arg(f"invalid order_by: {order_by}")
    return None


def _validate_status_filter(
    filters: dict[str, Any],
) -> dict[str, Any] | None:
    """Validate the ``status`` filter value.

    Args:
        filters: Filters dict potentially containing ``status``.

    Returns:
        Error dict if the status value is invalid, ``None``
        otherwise.
    """
    status = filters.get("status")
    if status is not None and status not in _VALID_STATUS:
        return _invalid_arg(f"invalid status filter: {status}")
    return None


def _validate_limit(
    arguments: dict[str, Any], *, max_limit: int
) -> int | dict[str, Any]:
    """Validate and normalize the ``limit`` field.

    Accept integer values and numeric strings. Reject booleans,
    non-numeric strings, and non-positive values.

    Args:
        arguments: Raw tool arguments.
        max_limit: Upper bound for the normalized limit.

    Returns:
        Normalized integer limit, or an INVALID_ARGUMENT error dict.
    """
    raw_limit = arguments.get("limit", 50)
    if isinstance(raw_limit, bool):
        return _invalid_arg("limit must be a positive integer")
    if isinstance(raw_limit, int):
        limit = raw_limit
    elif isinstance(raw_limit, str):
        try:
            limit = int(raw_limit)
        except ValueError:
            return _invalid_arg("limit must be a positive integer")
    else:
        return _invalid_arg("limit must be a positive integer")
    if limit <= 0:
        return _invalid_arg("limit must be a positive integer")
    return min(limit, max_limit)


def validate_search_args(
    arguments: dict[str, Any], *, max_limit: int
) -> dict[str, Any]:
    """Validate and normalize ``artifact.search`` arguments.

    Args:
        arguments: Raw tool arguments including gateway context,
            optional filters, ordering, limit, and cursor.
        max_limit: Upper bound for the ``limit`` parameter.

    Returns:
        Validated dict with ``session_id``, ``filters``,
        ``order_by``, ``limit``, and ``cursor`` keys, or an
        error dict with ``code`` and ``message`` on failure.
    """
    session_id = _validate_context(arguments)
    if isinstance(session_id, dict):
        return session_id

    filters_result = _validate_filters(arguments)
    if isinstance(filters_result, tuple):
        return filters_result[0]
    filters = filters_result

    order_by = arguments.get("order_by", "created_seq_desc")
    err = _validate_order_by(order_by)
    if err is not None:
        return err

    err = _validate_status_filter(filters)
    if err is not None:
        return err

    limit_result = _validate_limit(arguments, max_limit=max_limit)
    if isinstance(limit_result, dict):
        return limit_result
    limit = limit_result
    cursor = arguments.get("cursor")

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
    """Build SQL query for artifact search via ``artifact_refs``.

    Artifacts are discovered only through the ``artifact_refs``
    join for the given session. One extra row is fetched to
    detect whether a next page exists.

    Args:
        session_id: Current session identifier.
        filters: Validated filter dict (Addendum B columns).
        order_by: Sort key (``created_seq_desc`` or
            ``last_seen_desc``).
        limit: Maximum rows to return (before +1 overfetch).
        offset: Number of rows to skip for pagination.

    Returns:
        A ``(sql, params)`` tuple ready for execution.
    """
    params: list[Any] = [WORKSPACE_ID, session_id]

    base = """
    SELECT a.artifact_id, a.created_seq, a.created_at,
           ar.last_seen_at, a.source_tool,
           a.upstream_instance_id,
           CASE WHEN a.error_summary IS NULL
                THEN 'ok' ELSE 'error'
           END AS status,
           a.payload_total_bytes, a.error_summary,
           a.map_kind, a.map_status
    FROM artifact_refs ar
    JOIN artifacts a
      ON a.workspace_id = ar.workspace_id
     AND a.artifact_id = ar.artifact_id
    WHERE ar.workspace_id = %s
      AND ar.session_id = %s
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
        # Escape LIKE wildcards to prevent injection
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
            " AND pb.payload_hash_full"
            " = a.payload_hash_full"
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
    # fetch one extra for pagination detection
    params.append(limit + 1)
    if offset > 0:
        base += " OFFSET %s"
        params.append(offset)

    return base, params
