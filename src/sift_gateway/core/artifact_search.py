"""Protocol-agnostic artifact search execution service."""

from __future__ import annotations

from typing import Any

from sift_gateway.core.rows import rows_to_dicts
from sift_gateway.core.runtime import ArtifactSearchRuntime
from sift_gateway.envelope.responses import gateway_error
from sift_gateway.pagination.contract import (
    build_retrieval_pagination_meta,
)
from sift_gateway.tools.artifact_search import (
    build_search_query,
    validate_search_args,
)

_SEARCH_COLUMNS = [
    "artifact_id",
    "created_seq",
    "created_at",
    "last_seen_at",
    "source_tool",
    "upstream_instance_id",
    "capture_kind",
    "capture_key",
    "status",
    "payload_total_bytes",
    "error_summary",
    "map_kind",
    "map_status",
    "chain_seq",
    "kind",
]


def execute_artifact_search(
    runtime: ArtifactSearchRuntime,
    *,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Run artifact search using runtime hooks provided by an adapter."""
    parsed = validate_search_args(
        arguments,
        max_limit=runtime.artifact_search_max_limit,
    )
    if "code" in parsed:
        return gateway_error(str(parsed["code"]), str(parsed["message"]))
    if runtime.db_pool is None:
        return runtime.not_implemented("artifact")

    session_id = str(parsed["session_id"])
    order_by = str(parsed["order_by"])
    filters = dict(parsed["filters"])
    parent_filter = filters.get("parent_artifact_id", "")
    cursor_artifact_id = runtime.cursor_session_artifact_id(
        session_id, order_by
    )
    if parent_filter:
        cursor_artifact_id = f"{cursor_artifact_id}:p={parent_filter}"
    limit = min(
        int(parsed["limit"]),
        runtime.artifact_search_max_limit,
    )
    offset = 0
    cursor_token = parsed.get("cursor")
    if isinstance(cursor_token, str) and cursor_token:
        try:
            position = runtime.verify_cursor(
                token=cursor_token,
                tool="artifact",
                artifact_id=cursor_artifact_id,
            )
        except Exception as exc:
            return runtime.cursor_error(exc)
        raw_offset = position.get("offset", 0)
        if not isinstance(raw_offset, int) or raw_offset < 0:
            return gateway_error("INVALID_ARGUMENT", "invalid cursor offset")
        offset = raw_offset

    sql, params = build_search_query(
        filters,
        order_by,
        limit,
        query=(
            str(parsed.get("query"))
            if isinstance(parsed.get("query"), str)
            else None
        ),
        offset=offset,
    )

    with runtime.db_pool.connection() as connection:
        rows = connection.execute(sql, tuple(params)).fetchall()
        mapped_rows = rows_to_dicts(rows, _SEARCH_COLUMNS)
        page_rows = mapped_rows[:limit]
        truncated = len(mapped_rows) > limit
        touch_ids = [
            artifact_id
            for row in page_rows
            if isinstance((artifact_id := row.get("artifact_id")), str)
        ]
        touched = runtime.safe_touch_for_search(
            connection,
            session_id=session_id,
            artifact_ids=touch_ids,
        )
        if touched:
            commit = getattr(connection, "commit", None)
            if callable(commit):
                commit()

    next_cursor: str | None = None
    if truncated:
        next_cursor = runtime.issue_cursor(
            tool="artifact",
            artifact_id=cursor_artifact_id,
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
                "capture_kind": row.get("capture_kind"),
                "capture_key": row.get("capture_key"),
                "status": row["status"],
                "payload_total_bytes": row["payload_total_bytes"],
                "error_summary": row["error_summary"],
                "map_kind": row["map_kind"],
                "map_status": row["map_status"],
                "chain_seq": row.get("chain_seq"),
                "kind": row.get("kind"),
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
