"""artifact.chain_pages handler."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from mcp_artifact_gateway.constants import WORKSPACE_ID
from mcp_artifact_gateway.cursor.hmac import CursorExpiredError, CursorTokenError
from mcp_artifact_gateway.cursor.payload import CursorStaleError
from mcp_artifact_gateway.envelope.responses import gateway_error
from mcp_artifact_gateway.mcp.handlers.common import rows_to_dicts

if TYPE_CHECKING:
    from mcp_artifact_gateway.mcp.server import GatewayServer

_CHAIN_COLUMNS = [
    "artifact_id",
    "created_seq",
    "created_at",
    "chain_seq",
    "source_tool",
    "payload_total_bytes",
    "map_kind",
    "map_status",
]


async def handle_artifact_chain_pages(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    from mcp_artifact_gateway.tools.artifact_chain_pages import (
        FETCH_CHAIN_PAGES_SQL,
        build_chain_pages_response,
        validate_chain_pages_args,
    )

    err = validate_chain_pages_args(arguments)
    if err is not None:
        return gateway_error(str(err["code"]), str(err["message"]))
    if ctx.db_pool is None:
        return ctx._not_implemented("artifact.chain_pages")

    raw_ctx = arguments.get("_gateway_context")
    session_id = str(raw_ctx["session_id"]) if isinstance(raw_ctx, dict) else ""
    parent_artifact_id = str(arguments["parent_artifact_id"])

    offset = 0
    cursor_token = arguments.get("cursor")
    if isinstance(cursor_token, str) and cursor_token:
        try:
            position = ctx._verify_cursor(
                token=cursor_token,
                tool="artifact.chain_pages",
                artifact_id=parent_artifact_id,
            )
        except (CursorTokenError, CursorExpiredError, CursorStaleError) as exc:
            return ctx._cursor_error(exc)
        raw_offset = position.get("offset", 0)
        if not isinstance(raw_offset, int) or raw_offset < 0:
            return gateway_error("INVALID_ARGUMENT", "invalid cursor offset")
        offset = raw_offset

    limit = ctx._bounded_limit(arguments.get("limit"))

    with ctx.db_pool.connection() as connection:
        if not ctx._artifact_visible(
            connection,
            session_id=session_id,
            artifact_id=parent_artifact_id,
        ):
            return gateway_error("NOT_FOUND", "parent artifact not found")

        rows = connection.execute(
            FETCH_CHAIN_PAGES_SQL,
            (WORKSPACE_ID, parent_artifact_id, limit + 1, offset),
        ).fetchall()
        mapped_rows = rows_to_dicts(rows, _CHAIN_COLUMNS)
        page_rows = mapped_rows[:limit]
        truncated = len(mapped_rows) > limit

        touch_artifacts = [parent_artifact_id] + [
            str(row["artifact_id"])
            for row in page_rows
            if isinstance(row.get("artifact_id"), str)
        ]
        ctx._safe_touch_for_search(
            connection,
            session_id=session_id,
            artifact_ids=touch_artifacts,
        )
        commit = getattr(connection, "commit", None)
        if callable(commit):
            commit()

    next_cursor: str | None = None
    if truncated:
        next_cursor = ctx._issue_cursor(
            tool="artifact.chain_pages",
            artifact_id=parent_artifact_id,
            position_state={"offset": offset + len(page_rows)},
        )
    return build_chain_pages_response(
        page_rows,
        truncated=truncated,
        cursor=next_cursor,
    )
