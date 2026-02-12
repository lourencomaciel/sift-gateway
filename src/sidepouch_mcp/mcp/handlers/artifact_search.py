"""artifact.search handler."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sidepouch_mcp.cursor.hmac import (
    CursorExpiredError,
    CursorTokenError,
)
from sidepouch_mcp.cursor.payload import CursorStaleError
from sidepouch_mcp.envelope.responses import gateway_error
from sidepouch_mcp.mcp.handlers.common import rows_to_dicts
from sidepouch_mcp.pagination.contract import (
    build_retrieval_pagination_meta,
)

if TYPE_CHECKING:
    from sidepouch_mcp.mcp.server import GatewayServer

_SEARCH_COLUMNS = [
    "artifact_id",
    "created_seq",
    "created_at",
    "last_seen_at",
    "source_tool",
    "upstream_instance_id",
    "status",
    "payload_total_bytes",
    "error_summary",
    "map_kind",
    "map_status",
]


async def handle_artifact_search(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Handle the ``artifact.search`` tool call.

    Args:
        ctx: Gateway server instance providing DB and cursor helpers.
        arguments: Tool arguments including session context, optional
            filters, ``order_by``, ``limit``, and ``cursor``.

    Returns:
        Paginated search response with artifact summaries, or a
        gateway error.
    """
    from sidepouch_mcp.tools.artifact_search import (
        build_search_query,
        validate_search_args,
    )

    parsed = validate_search_args(
        arguments,
        max_limit=ctx.config.artifact_search_max_limit,
    )
    if "code" in parsed:
        return gateway_error(str(parsed["code"]), str(parsed["message"]))
    if ctx.db_pool is None:
        return ctx._not_implemented("artifact.search")

    session_id = str(parsed["session_id"])
    order_by = str(parsed["order_by"])
    limit = min(int(parsed["limit"]), ctx.config.artifact_search_max_limit)
    offset = 0
    cursor_token = parsed.get("cursor")
    if isinstance(cursor_token, str) and cursor_token:
        try:
            position = ctx._verify_cursor(
                token=cursor_token,
                tool="artifact.search",
                artifact_id=ctx._cursor_session_artifact_id(
                    session_id, order_by
                ),
            )
        except (CursorTokenError, CursorExpiredError, CursorStaleError) as exc:
            return ctx._cursor_error(exc)
        raw_offset = position.get("offset", 0)
        if not isinstance(raw_offset, int) or raw_offset < 0:
            return gateway_error("INVALID_ARGUMENT", "invalid cursor offset")
        offset = raw_offset

    sql, params = build_search_query(
        session_id,
        dict(parsed["filters"]),
        order_by,
        limit,
        offset=offset,
    )

    with ctx.db_pool.connection() as connection:
        rows = connection.execute(sql, tuple(params)).fetchall()
        mapped_rows = rows_to_dicts(rows, _SEARCH_COLUMNS)
        page_rows = mapped_rows[:limit]
        truncated = len(mapped_rows) > limit
        artifact_ids = [
            str(row["artifact_id"])
            for row in page_rows
            if isinstance(row.get("artifact_id"), str)
        ]
        ctx._safe_touch_for_search(
            connection,
            session_id=session_id,
            artifact_ids=artifact_ids,
        )
        commit = getattr(connection, "commit", None)
        if callable(commit):
            commit()

    next_cursor: str | None = None
    if truncated:
        next_cursor = ctx._issue_cursor(
            tool="artifact.search",
            artifact_id=ctx._cursor_session_artifact_id(session_id, order_by),
            position_state={"offset": offset + len(page_rows)},
        )

    return {
        "items": [
            {
                "artifact_id": row["artifact_id"],
                "created_seq": row["created_seq"],
                "created_at": (
                    str(row["created_at"])
                    if row.get("created_at") is not None
                    else None
                ),
                "last_seen_at": (
                    str(row["last_seen_at"])
                    if row.get("last_seen_at") is not None
                    else None
                ),
                "source_tool": row["source_tool"],
                "upstream_instance_id": row["upstream_instance_id"],
                "status": row["status"],
                "payload_total_bytes": row["payload_total_bytes"],
                "error_summary": row["error_summary"],
                "map_kind": row["map_kind"],
                "map_status": row["map_status"],
            }
            for row in page_rows
        ],
        "truncated": truncated,
        "cursor": next_cursor,
        "omitted": len(mapped_rows) - len(page_rows) if truncated else 0,
        "pagination": build_retrieval_pagination_meta(
            truncated=truncated,
            cursor=next_cursor if next_cursor else None,
        ),
    }
