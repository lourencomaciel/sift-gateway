"""artifact.find handler."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping

from mcp_artifact_gateway.constants import WORKSPACE_ID
from mcp_artifact_gateway.cursor.hmac import CursorExpiredError, CursorTokenError
from mcp_artifact_gateway.cursor.payload import CursorStaleError
from mcp_artifact_gateway.envelope.responses import gateway_error
from mcp_artifact_gateway.mcp.handlers.common import (
    ARTIFACT_META_COLUMNS,
    FETCH_ARTIFACT_META_SQL,
    ROOT_COLUMNS,
    SAMPLE_COLUMNS,
    row_to_dict,
    rows_to_dicts,
)
from mcp_artifact_gateway.query.where_dsl import WhereDslError, evaluate_where
from mcp_artifact_gateway.query.where_hash import where_hash
from mcp_artifact_gateway.retrieval.response import apply_output_budgets

if TYPE_CHECKING:
    from mcp_artifact_gateway.mcp.server import GatewayServer


async def handle_artifact_find(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    from mcp_artifact_gateway.tools.artifact_describe import FETCH_ROOTS_SQL
    from mcp_artifact_gateway.tools.artifact_find import (
        build_find_response,
        validate_find_args,
    )
    from mcp_artifact_gateway.tools.artifact_select import FETCH_SAMPLES_SQL

    err = validate_find_args(arguments)
    if err is not None:
        return err
    if ctx.db_pool is None:
        return ctx._not_implemented("artifact.find")

    raw_ctx = arguments.get("_gateway_context")
    session_id = str(raw_ctx["session_id"]) if isinstance(raw_ctx, dict) else ""
    artifact_id = str(arguments["artifact_id"])
    root_path_filter = arguments.get("root_path")
    if root_path_filter is not None and not isinstance(root_path_filter, str):
        return gateway_error("INVALID_ARGUMENT", "root_path must be a string")
    where_expr = arguments.get("where")
    if where_expr is not None and not isinstance(where_expr, (Mapping, str)):
        return gateway_error("INVALID_ARGUMENT", "where must be an object or string")
    if where_expr is None:
        where_binding_hash = "__none__"
    else:
        try:
            where_binding_hash = where_hash(
                where_expr,
                mode=ctx.config.where_canonicalization_mode.value,
            )
        except ValueError as exc:
            return gateway_error(
                "INVALID_ARGUMENT",
                f"invalid where expression: {exc}",
            )
    root_path_binding = root_path_filter if isinstance(root_path_filter, str) else "__any__"

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
        except (CursorTokenError, CursorExpiredError, CursorStaleError) as exc:
            return ctx._cursor_error(exc)
        raw_offset = position.get("offset", 0)
        if not isinstance(raw_offset, int) or raw_offset < 0:
            return gateway_error("INVALID_ARGUMENT", "invalid cursor offset")
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
            return gateway_error("GONE", "artifact has been deleted")
        if artifact_meta.get("map_status") != "ready":
            return gateway_error(
                "INVALID_ARGUMENT",
                "artifact mapping is not ready",
            )
        map_budget_fingerprint = (
            str(artifact_meta.get("map_budget_fingerprint"))
            if isinstance(artifact_meta.get("map_budget_fingerprint"), str)
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
                if str(artifact_meta.get("map_kind", "none")) == "partial":
                    ctx._assert_cursor_field(
                        cursor_payload,
                        field="map_budget_fingerprint",
                        expected=map_budget_fingerprint,
                    )
            except CursorStaleError as exc:
                return ctx._cursor_error(exc)

        roots = rows_to_dicts(
            connection.execute(
                FETCH_ROOTS_SQL,
                (WORKSPACE_ID, artifact_id),
            ).fetchall(),
            ROOT_COLUMNS,
        )
        if root_path_filter is not None:
            roots = [root for root in roots if root.get("root_path") == root_path_filter]

        items: list[dict[str, Any]] = []
        for root in roots:
            sample_rows = rows_to_dicts(
                connection.execute(
                    FETCH_SAMPLES_SQL,
                    (WORKSPACE_ID, artifact_id, root["root_key"]),
                ).fetchall(),
                SAMPLE_COLUMNS,
            )
            for sample in sample_rows:
                record = sample.get("record")
                if where_expr is not None:
                    try:
                        matches = evaluate_where(
                            record,
                            where_expr,
                            max_compute_steps=ctx.config.max_compute_steps,
                            max_wildcard_expansion=ctx.config.max_wildcards,
                        )
                    except WhereDslError as exc:
                        return gateway_error("INVALID_ARGUMENT", str(exc))
                    if not matches:
                        continue
                items.append(
                    {
                        "root_path": root.get("root_path"),
                        "sample_index": sample.get("sample_index"),
                        "record": record,
                        "record_hash": sample.get("record_hash"),
                    }
                )

        ctx._safe_touch_for_retrieval(
            connection,
            session_id=session_id,
            artifact_id=artifact_id,
        )
        commit = getattr(connection, "commit", None)
        if callable(commit):
            commit()

        index_status = str(artifact_meta.get("index_status", "off"))

    max_items = ctx._bounded_limit(arguments.get("limit"))
    selected, truncated, _omitted, _used_bytes = apply_output_budgets(
        items[offset:],
        max_items=max_items,
        max_bytes_out=ctx.config.max_bytes_out,
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
        if str(artifact_meta.get("map_kind", "none")) == "partial":
            map_budget_fingerprint = (
                str(artifact_meta.get("map_budget_fingerprint"))
                if isinstance(artifact_meta.get("map_budget_fingerprint"), str)
                else ""
            )
            extra["map_budget_fingerprint"] = map_budget_fingerprint
        next_cursor = ctx._issue_cursor(
            tool="artifact.find",
            artifact_id=artifact_id,
            position_state={"offset": offset + len(selected)},
            extra=extra,
        )
    return build_find_response(
        items=selected,
        truncated=truncated,
        cursor=next_cursor,
        sampled_only=True,
        index_status=index_status,
    )
