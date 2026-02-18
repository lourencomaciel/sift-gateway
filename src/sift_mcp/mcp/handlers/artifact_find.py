"""Find handler using SQL queries on artifact_records.

Prefer ``artifact(action="query", query_kind="select", where=...)`` for
new callers.
"""

from __future__ import annotations

from collections.abc import Mapping
import contextlib
import json
from typing import TYPE_CHECKING, Any

from sift_mcp.canon.rfc8785 import canonical_bytes as _canon
from sift_mcp.constants import WORKSPACE_ID
from sift_mcp.cursor.payload import CursorStaleError
from sift_mcp.cursor.token import (
    CursorExpiredError,
    CursorTokenError,
)
from sift_mcp.envelope.responses import gateway_error
from sift_mcp.mcp.handlers.common import (
    ARTIFACT_META_COLUMNS,
    FETCH_ARTIFACT_META_SQL,
    ROOT_COLUMNS,
    row_to_dict,
    rows_to_dicts,
)
from sift_mcp.query.filters import (
    compile_filter,
    filter_hash,
    parse_filter_dict,
)
from sift_mcp.retrieval.response import apply_output_budgets
from sift_mcp.util.hashing import sha256_trunc

if TYPE_CHECKING:
    from sift_mcp.mcp.server import GatewayServer


async def handle_artifact_find(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Handle the ``artifact.find`` tool call.

    Args:
        ctx: Gateway server instance providing DB and cursor helpers.
        arguments: Tool arguments including ``artifact_id``, optional
            ``root_path``, ``where``, ``cursor``, and ``limit``.

    Returns:
        Paginated find response with matching records, or a gateway
        error.
    """
    from sift_mcp.tools.artifact_describe import FETCH_ROOTS_SQL
    from sift_mcp.tools.artifact_find import (
        build_find_response,
        validate_find_args,
    )

    err = validate_find_args(arguments)
    if err is not None:
        return gateway_error(str(err["code"]), str(err["message"]))
    if ctx.db_pool is None:
        return ctx._not_implemented("artifact.find")

    raw_ctx = arguments.get("_gateway_context")
    session_id = (
        str(raw_ctx["session_id"]) if isinstance(raw_ctx, dict) else ""
    )
    artifact_id = str(arguments["artifact_id"])
    root_path_filter = arguments.get("root_path")
    if root_path_filter is not None and not isinstance(
        root_path_filter, str
    ):
        return gateway_error(
            "INVALID_ARGUMENT", "root_path must be a string"
        )
    where_expr = arguments.get("where")
    if where_expr is not None and not isinstance(where_expr, Mapping):
        return gateway_error(
            "INVALID_ARGUMENT",
            "where must be a filter object",
        )

    # Compile where filter to SQL.
    filter_sql: str | None = None
    filter_params: list[Any] = []
    if where_expr is not None:
        try:
            filter_obj = parse_filter_dict(dict(where_expr))
            filter_sql, filter_params = compile_filter(filter_obj)
            where_binding_hash = filter_hash(filter_obj)
        except (ValueError, KeyError, TypeError) as exc:
            return gateway_error(
                "INVALID_ARGUMENT",
                f"invalid where filter: {exc}",
            )
    else:
        where_binding_hash = "__none__"

    root_path_binding = (
        root_path_filter
        if isinstance(root_path_filter, str)
        else "__any__"
    )

    offset = 0
    cursor_payload: dict[str, Any] | None = None
    cursor_token = arguments.get("cursor")
    if isinstance(cursor_token, str) and cursor_token:
        try:
            cursor_payload = ctx._verify_cursor_payload(
                token=cursor_token,
                tool="artifact.find",
                artifact_id=artifact_id,
            )
            position = ctx._cursor_position(cursor_payload)
        except (
            CursorTokenError,
            CursorExpiredError,
            CursorStaleError,
        ) as exc:
            return ctx._cursor_error(exc)
        raw_offset = position.get("offset", 0)
        if not isinstance(raw_offset, int) or raw_offset < 0:
            return gateway_error(
                "INVALID_ARGUMENT", "invalid cursor offset"
            )
        offset = raw_offset

    with ctx.db_pool.connection() as connection:
        if not ctx._artifact_visible(
            connection,
            session_id=session_id,
            artifact_id=artifact_id,
        ):
            return gateway_error("NOT_FOUND", "artifact not found")

        artifact_meta = row_to_dict(
            connection.execute(
                FETCH_ARTIFACT_META_SQL,
                (WORKSPACE_ID, artifact_id),
            ).fetchone(),
            ARTIFACT_META_COLUMNS,
        )
        if artifact_meta is None:
            return gateway_error("NOT_FOUND", "artifact not found")
        if artifact_meta.get("deleted_at") is not None:
            ctx._safe_touch_for_retrieval(
                connection,
                session_id=session_id,
                artifact_id=artifact_id,
            )
            commit = getattr(connection, "commit", None)
            if callable(commit):
                commit()
            return gateway_error(
                "GONE", "artifact has been deleted"
            )
        if artifact_meta.get("map_status") != "ready":
            return gateway_error(
                "INVALID_ARGUMENT",
                "artifact mapping is not ready",
            )
        map_budget_fingerprint = (
            str(artifact_meta.get("map_budget_fingerprint"))
            if isinstance(
                artifact_meta.get("map_budget_fingerprint"), str
            )
            else ""
        )
        if cursor_payload is not None:
            try:
                ctx._assert_cursor_field(
                    cursor_payload,
                    field="root_path_filter",
                    expected=root_path_binding,
                )
                ctx._assert_cursor_field(
                    cursor_payload,
                    field="where_hash",
                    expected=where_binding_hash,
                )
                generation = artifact_meta.get("generation")
                if isinstance(generation, int):
                    ctx._assert_cursor_field(
                        cursor_payload,
                        field="artifact_generation",
                        expected=generation,
                    )
                if (
                    str(artifact_meta.get("map_kind", "none"))
                    == "partial"
                ):
                    ctx._assert_cursor_field(
                        cursor_payload,
                        field="map_budget_fingerprint",
                        expected=map_budget_fingerprint,
                    )
            except CursorStaleError as exc:
                return ctx._cursor_error(exc)

        # Determine root_paths to query.
        roots = rows_to_dicts(
            connection.execute(
                FETCH_ROOTS_SQL,
                (WORKSPACE_ID, artifact_id),
            ).fetchall(),
            ROOT_COLUMNS,
        )
        if root_path_filter is not None:
            roots = [
                root
                for root in roots
                if root.get("root_path") == root_path_filter
            ]
        root_paths = [
            root.get("root_path", "$") for root in roots
        ]

        # Query artifact_records for matching records.
        items: list[dict[str, Any]] = []
        map_kind = str(artifact_meta.get("map_kind", "none"))
        sampled_only = map_kind == "partial"

        for rp in root_paths:
            query_sql = (
                "SELECT idx, record FROM artifact_records"
                " WHERE workspace_id = ?"
                " AND artifact_id = ?"
                " AND root_path = ?"
            )
            query_params: list[Any] = [
                WORKSPACE_ID,
                artifact_id,
                rp,
            ]
            if filter_sql:
                query_sql += f" AND ({filter_sql})"
                query_params.extend(filter_params)
            query_sql += " ORDER BY idx ASC"

            rows = connection.execute(
                query_sql, query_params
            ).fetchall()

            for row in rows:
                idx = row[0]
                record = row[1]
                record_hash: str | None = None
                if isinstance(record, dict):
                    with contextlib.suppress(
                        TypeError, ValueError
                    ):
                        record_hash = sha256_trunc(
                            _canon(record), 32
                        )
                elif isinstance(record, str):
                    with contextlib.suppress(
                        TypeError, ValueError
                    ):
                        parsed = json.loads(record)
                        if isinstance(parsed, dict):
                            record_hash = sha256_trunc(
                                _canon(parsed), 32
                            )

                item: dict[str, Any] = {
                    "root_path": rp,
                    "record_hash": record_hash,
                }
                if sampled_only:
                    item["sample_index"] = idx
                else:
                    item["index"] = idx
                items.append(item)

        ctx._safe_touch_for_retrieval(
            connection,
            session_id=session_id,
            artifact_id=artifact_id,
        )
        commit = getattr(connection, "commit", None)
        if callable(commit):
            commit()

        index_status = str(
            artifact_meta.get("index_status", "off")
        )

    max_items = ctx._bounded_limit(arguments.get("limit"))
    selected, truncated, _omitted, _used_bytes = (
        apply_output_budgets(
            items[offset:],
            max_items=max_items,
            max_bytes_out=ctx.config.max_bytes_out,
        )
    )
    next_cursor: str | None = None
    if truncated:
        extra: dict[str, Any] = {
            "root_path_filter": root_path_binding,
            "where_hash": where_binding_hash,
        }
        generation = artifact_meta.get("generation")
        if isinstance(generation, int):
            extra["artifact_generation"] = generation
        if map_kind == "partial":
            extra["map_budget_fingerprint"] = (
                map_budget_fingerprint
            )
        next_cursor = ctx._issue_cursor(
            tool="artifact.find",
            artifact_id=artifact_id,
            position_state={"offset": offset + len(selected)},
            extra=extra,
        )
    determinism: dict[str, str] | None = None
    if map_kind == "partial" and map_budget_fingerprint:
        from sift_mcp.constants import TRAVERSAL_CONTRACT_VERSION

        determinism = {
            "traversal_contract_version": (
                TRAVERSAL_CONTRACT_VERSION
            ),
            "map_budget_fingerprint": map_budget_fingerprint,
        }
    return build_find_response(
        items=selected,
        truncated=truncated,
        cursor=next_cursor,
        sampled_only=sampled_only,
        index_status=index_status,
        determinism=determinism,
        matched_count=len(items),
    )
