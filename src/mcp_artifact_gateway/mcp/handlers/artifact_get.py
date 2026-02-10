"""artifact.get handler."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from mcp_artifact_gateway.constants import WORKSPACE_ID
from mcp_artifact_gateway.cursor.hmac import (
    CursorExpiredError,
    CursorTokenError,
)
from mcp_artifact_gateway.cursor.payload import CursorStaleError
from mcp_artifact_gateway.envelope.responses import gateway_error
from mcp_artifact_gateway.mcp.handlers.common import (
    ENVELOPE_COLUMNS,
    ROOT_COLUMNS,
    row_to_dict,
    rows_to_dicts,
)
from mcp_artifact_gateway.query.jsonpath import (
    JsonPathError,
    canonicalize_jsonpath,
    evaluate_jsonpath,
)
from mcp_artifact_gateway.retrieval.response import apply_output_budgets
from mcp_artifact_gateway.storage.payload_store import reconstruct_envelope

if TYPE_CHECKING:
    from mcp_artifact_gateway.mcp.server import GatewayServer


async def handle_artifact_get(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Handle the ``artifact.get`` tool call.

    Args:
        ctx: Gateway server instance providing DB and cursor helpers.
        arguments: Tool arguments including ``artifact_id``, optional
            ``target``, ``jsonpath``, ``cursor``, and ``limit``.

    Returns:
        Envelope or mapped-view response dict, or a gateway error.
    """
    from mcp_artifact_gateway.tools.artifact_describe import FETCH_ROOTS_SQL
    from mcp_artifact_gateway.tools.artifact_get import (
        FETCH_ARTIFACT_SQL,
        check_get_preconditions,
        validate_get_args,
    )

    err = validate_get_args(arguments)
    if err is not None:
        return gateway_error(str(err["code"]), str(err["message"]))
    if ctx.db_pool is None:
        return ctx._not_implemented("artifact.get")

    raw_ctx = arguments.get("_gateway_context")
    session_id = str(raw_ctx["session_id"]) if isinstance(raw_ctx, dict) else ""
    artifact_id = str(arguments["artifact_id"])
    target = str(arguments.get("target", "envelope"))
    jsonpath = arguments.get("jsonpath")
    if jsonpath is not None and not isinstance(jsonpath, str):
        return gateway_error(
            "INVALID_ARGUMENT",
            "jsonpath must be a string when provided",
        )
    normalized_jsonpath = "$"
    if isinstance(jsonpath, str):
        try:
            normalized_jsonpath = canonicalize_jsonpath(
                jsonpath,
                max_length=ctx.config.max_jsonpath_length,
                max_segments=ctx.config.max_path_segments,
            )
        except JsonPathError as exc:
            return gateway_error(
                "INVALID_ARGUMENT",
                f"invalid jsonpath: {exc}",
            )

    offset = 0
    cursor_payload: dict[str, Any] | None = None
    cursor_token = arguments.get("cursor")
    if isinstance(cursor_token, str) and cursor_token:
        try:
            cursor_payload = ctx._verify_cursor_payload(
                token=cursor_token,
                tool="artifact.get",
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

        row = row_to_dict(
            connection.execute(
                FETCH_ARTIFACT_SQL,
                (WORKSPACE_ID, artifact_id),
            ).fetchone(),
            ENVELOPE_COLUMNS,
        )
        precondition = check_get_preconditions(row, target)
        if precondition is not None:
            if row is not None:
                ctx._safe_touch_for_retrieval(
                    connection,
                    session_id=session_id,
                    artifact_id=artifact_id,
                )
                commit = getattr(connection, "commit", None)
                if callable(commit):
                    commit()
            return gateway_error(
                str(precondition["code"]), str(precondition["message"])
            )

        if row is None:
            return gateway_error("NOT_FOUND", "artifact not found")
        if cursor_payload is not None:
            try:
                ctx._assert_cursor_field(
                    cursor_payload, field="target", expected=target
                )
                ctx._assert_cursor_field(
                    cursor_payload,
                    field="normalized_jsonpath",
                    expected=normalized_jsonpath,
                )
                generation = row.get("generation")
                if isinstance(generation, int):
                    ctx._assert_cursor_field(
                        cursor_payload,
                        field="artifact_generation",
                        expected=generation,
                    )
            except CursorStaleError as exc:
                return ctx._cursor_error(exc)

        ctx._safe_touch_for_retrieval(
            connection,
            session_id=session_id,
            artifact_id=artifact_id,
        )
        commit = getattr(connection, "commit", None)
        if callable(commit):
            commit()

        if target == "mapped":
            roots_rows = connection.execute(
                FETCH_ROOTS_SQL,
                (WORKSPACE_ID, artifact_id),
            ).fetchall()
            roots = rows_to_dicts(roots_rows, ROOT_COLUMNS)
            return {
                "artifact_id": artifact_id,
                "target": "mapped",
                "mapping": {
                    "map_kind": row.get("map_kind"),
                    "map_status": row.get("map_status"),
                    "mapped_part_index": row.get("mapped_part_index"),
                    "map_budget_fingerprint": row.get("map_budget_fingerprint"),
                },
                "roots": roots,
            }

        envelope_value = row.get("envelope")
        canonical_bytes_raw = row.get("envelope_canonical_bytes")
        if isinstance(envelope_value, dict) and "content" in envelope_value:
            envelope = envelope_value
        elif canonical_bytes_raw is None:
            return gateway_error(
                "INTERNAL_ERROR", "missing canonical bytes for artifact"
            )
        else:
            try:
                envelope = reconstruct_envelope(
                    compressed_bytes=bytes(canonical_bytes_raw),
                    encoding=str(
                        row.get("envelope_canonical_encoding", "none")
                    ),
                    expected_hash=str(row.get("payload_hash_full", "")),
                )
            except ValueError as exc:
                return gateway_error(
                    "INTERNAL_ERROR", f"envelope reconstruction failed: {exc}"
                )

        if jsonpath is not None:
            values = evaluate_jsonpath(
                envelope,
                normalized_jsonpath,
                max_length=ctx.config.max_jsonpath_length,
                max_segments=ctx.config.max_path_segments,
                max_wildcard_expansion_total=ctx.config.max_wildcard_expansion_total,
            )
        else:
            values = [envelope]

    values_page = values[offset:]
    max_items = ctx._bounded_limit(arguments.get("limit"))
    selected, truncated, omitted, used_bytes = apply_output_budgets(
        values_page,
        max_items=max_items,
        max_bytes_out=ctx.config.max_bytes_out,
    )
    next_cursor: str | None = None
    if truncated:
        extra: dict[str, Any] = {
            "target": target,
            "normalized_jsonpath": normalized_jsonpath,
        }
        generation = row.get("generation")
        if isinstance(generation, int):
            extra["artifact_generation"] = generation
        next_cursor = ctx._issue_cursor(
            tool="artifact.get",
            artifact_id=artifact_id,
            position_state={
                "offset": offset + len(selected),
            },
            extra=extra,
        )

    return {
        "artifact_id": artifact_id,
        "target": "envelope",
        "items": selected,
        "truncated": truncated,
        "cursor": next_cursor,
        "omitted": omitted,
        "stats": {"bytes_out": used_bytes},
    }
