"""Legacy get handler for ``artifact(action="query", query_kind="get")``."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sift_mcp.constants import WORKSPACE_ID
from sift_mcp.cursor.payload import CursorStaleError
from sift_mcp.cursor.token import (
    CursorExpiredError,
    CursorTokenError,
)
from sift_mcp.envelope.responses import gateway_error
from sift_mcp.mcp.handlers.common import (
    ENVELOPE_COLUMNS,
    ROOT_COLUMNS,
    extract_json_target,
    row_to_dict,
    rows_to_dicts,
)
from sift_mcp.mcp.lineage import (
    build_lineage_root_catalog,
    compute_related_set_hash,
    resolve_related_artifacts,
)
from sift_mcp.pagination.contract import (
    build_retrieval_pagination_meta,
)
from sift_mcp.query.jsonpath import (
    JsonPathError,
    canonicalize_jsonpath,
    evaluate_jsonpath,
)
from sift_mcp.retrieval.response import apply_output_budgets
from sift_mcp.storage.payload_store import reconstruct_envelope

if TYPE_CHECKING:
    from sift_mcp.mcp.server import GatewayServer


async def handle_artifact_get(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Handle get-mode artifact queries."""
    from sift_mcp.tools.artifact_describe import FETCH_ROOTS_SQL
    from sift_mcp.tools.artifact_get import (
        FETCH_ARTIFACT_SQL,
        check_get_preconditions,
        validate_get_args,
    )

    err = validate_get_args(arguments)
    if err is not None:
        return gateway_error(str(err["code"]), str(err["message"]))
    if ctx.db_pool is None:
        return ctx._not_implemented("artifact")

    raw_ctx = arguments.get("_gateway_context")
    session_id = str(raw_ctx["session_id"]) if isinstance(raw_ctx, dict) else ""
    anchor_artifact_id = str(arguments["artifact_id"])
    target = str(arguments.get("target", "envelope"))
    jsonpath = arguments.get("jsonpath")
    raw_scope = arguments.get("scope")
    scope: str | None = None
    if raw_scope is not None:
        if not isinstance(raw_scope, str) or raw_scope not in {
            "all_related",
            "single",
        }:
            return gateway_error(
                "INVALID_ARGUMENT",
                "scope must be one of: all_related, single",
            )
        scope = raw_scope
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
                tool="artifact",
                artifact_id=anchor_artifact_id,
            )
            position = ctx._cursor_position(cursor_payload)
        except (CursorTokenError, CursorExpiredError, CursorStaleError) as exc:
            return ctx._cursor_error(exc)
        raw_offset = position.get("offset", 0)
        if not isinstance(raw_offset, int) or raw_offset < 0:
            return gateway_error("INVALID_ARGUMENT", "invalid cursor offset")
        offset = raw_offset
        if scope is None:
            cursor_scope = cursor_payload.get("scope")
            if cursor_scope in {"all_related", "single"}:
                scope = str(cursor_scope)
            elif isinstance(cursor_payload.get("artifact_generation"), int):
                # Backward-compatibility for pre-scope cursors.
                scope = "single"

    if scope is None:
        scope = "all_related"

    related_rows: list[dict[str, Any]] = []
    related_ids: list[str] = []
    related_set_hash: str | None = None
    artifact_rows: list[dict[str, Any]] = []
    anchor_row: dict[str, Any] | None = None
    warnings: list[dict[str, Any]] = []

    with ctx.db_pool.connection() as connection:
        if scope == "single":
            if not ctx._artifact_visible(
                connection,
                session_id=session_id,
                artifact_id=anchor_artifact_id,
            ):
                return gateway_error("NOT_FOUND", "artifact not found")
            related_ids = [anchor_artifact_id]
        else:
            related_rows = resolve_related_artifacts(
                connection,
                session_id=session_id,
                anchor_artifact_id=anchor_artifact_id,
            )
            if not related_rows:
                return gateway_error("NOT_FOUND", "artifact not found")
            if len(related_rows) > ctx.config.related_query_max_artifacts:
                return gateway_error(
                    "RESOURCE_EXHAUSTED",
                    "lineage query exceeds related artifact limit",
                    details={
                        "artifact_count": len(related_rows),
                        "max_artifacts": ctx.config.related_query_max_artifacts,
                    },
                )
            related_ids = [
                artifact_id
                for row in related_rows
                if isinstance((artifact_id := row.get("artifact_id")), str)
            ]
            related_set_hash = compute_related_set_hash(related_rows)

        if not related_ids:
            return gateway_error("NOT_FOUND", "artifact not found")

        for artifact_id in related_ids:
            row = row_to_dict(
                connection.execute(
                    FETCH_ARTIFACT_SQL,
                    (WORKSPACE_ID, artifact_id),
                ).fetchone(),
                ENVELOPE_COLUMNS,
            )
            if row is None:
                continue
            if artifact_id == anchor_artifact_id:
                anchor_row = row
            precondition = check_get_preconditions(row, target)
            if precondition is not None:
                if scope == "all_related" and target == "mapped":
                    warnings.append(
                        {
                            "code": "SKIPPED_ARTIFACT",
                            "artifact_id": artifact_id,
                            "reason": str(precondition["message"]),
                        }
                    )
                    continue
                return gateway_error(
                    str(precondition["code"]),
                    str(precondition["message"]),
                )
            artifact_rows.append(row)

        if anchor_row is None:
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
                if isinstance(cursor_payload.get("scope"), str):
                    ctx._assert_cursor_field(
                        cursor_payload,
                        field="scope",
                        expected=scope,
                    )
                if scope == "all_related":
                    ctx._assert_cursor_field(
                        cursor_payload,
                        field="anchor_artifact_id",
                        expected=anchor_artifact_id,
                    )
                    ctx._assert_cursor_field(
                        cursor_payload,
                        field="related_set_hash",
                        expected=related_set_hash,
                    )
                else:
                    generation = anchor_row.get("generation")
                    if isinstance(generation, int):
                        ctx._assert_cursor_field(
                            cursor_payload,
                            field="artifact_generation",
                            expected=generation,
                        )
            except CursorStaleError as exc:
                return ctx._cursor_error(exc)

        if target == "mapped":
            root_entries: list[dict[str, Any]] = []
            for artifact_row in artifact_rows:
                artifact_id = artifact_row.get("artifact_id")
                if not isinstance(artifact_id, str):
                    continue
                if artifact_row.get("map_status") != "ready":
                    warnings.append(
                        {
                            "code": "SKIPPED_ARTIFACT",
                            "artifact_id": artifact_id,
                            "reason": "map_status is not ready",
                        }
                    )
                    continue
                roots_rows = connection.execute(
                    FETCH_ROOTS_SQL,
                    (WORKSPACE_ID, artifact_id),
                ).fetchall()
                roots = rows_to_dicts(roots_rows, ROOT_COLUMNS)
                root_entries.extend(
                    {
                        "artifact_id": artifact_id,
                        "root_path": root.get("root_path"),
                        "root_shape": root.get("root_shape"),
                        "fields_top": root.get("fields_top"),
                        "count_estimate": root.get("count_estimate"),
                        "map_kind": artifact_row.get("map_kind"),
                    }
                    for root in roots
                )
            mapped_lineage: dict[str, Any] = {
                "scope": scope,
                "anchor_artifact_id": anchor_artifact_id,
                "artifact_count": len(related_ids),
                "artifact_ids": related_ids,
            }
            if scope == "all_related":
                mapped_lineage["related_set_hash"] = related_set_hash
            response: dict[str, Any] = {
                "artifact_id": anchor_artifact_id,
                "scope": scope,
                "target": "mapped",
                "lineage": mapped_lineage,
                "roots": build_lineage_root_catalog(root_entries),
                "pagination": build_retrieval_pagination_meta(
                    truncated=False,
                    cursor=None,
                ),
            }
            if warnings:
                response["warnings"] = warnings
            return {
                **response,
            }

    merged_items: list[dict[str, Any]] = []
    for artifact_row in artifact_rows:
        artifact_id = artifact_row.get("artifact_id")
        if not isinstance(artifact_id, str):
            continue
        envelope_value = artifact_row.get("envelope")
        canonical_bytes_raw = artifact_row.get("envelope_canonical_bytes")
        if isinstance(envelope_value, dict) and "content" in envelope_value:
            envelope = envelope_value
        elif canonical_bytes_raw is None:
            return gateway_error(
                "INTERNAL",
                "missing canonical bytes for artifact",
            )
        else:
            try:
                envelope = reconstruct_envelope(
                    compressed_bytes=bytes(canonical_bytes_raw),
                    encoding=str(
                        artifact_row.get("envelope_canonical_encoding", "none")
                    ),
                    expected_hash=str(
                        artifact_row.get("payload_hash_full", "")
                    ),
                )
            except ValueError as exc:
                return gateway_error(
                    "INTERNAL",
                    f"envelope reconstruction failed: {exc}",
                )

        if jsonpath is not None:
            json_target = extract_json_target(
                envelope,
                artifact_row.get("mapped_part_index"),
            )
            values = evaluate_jsonpath(
                json_target,
                normalized_jsonpath,
                max_length=ctx.config.max_jsonpath_length,
                max_segments=ctx.config.max_path_segments,
                max_wildcard_expansion_total=ctx.config.max_wildcard_expansion_total,
            )
            for index, value in enumerate(values):
                merged_items.append(
                    {
                        "_locator": {
                            "artifact_id": artifact_id,
                            "jsonpath": normalized_jsonpath,
                            "index": index,
                        },
                        "value": value,
                    }
                )
        else:
            merged_items.append(
                {
                    "_locator": {"artifact_id": artifact_id},
                    "value": envelope,
                }
            )

    max_items = ctx._bounded_limit(arguments.get("limit"))
    selected, truncated, omitted, used_bytes = apply_output_budgets(
        merged_items[offset:],
        max_items=max_items,
        max_bytes_out=ctx.config.max_bytes_out,
    )
    next_cursor: str | None = None
    if truncated:
        extra: dict[str, Any] = {
            "target": target,
            "normalized_jsonpath": normalized_jsonpath,
            "scope": scope,
        }
        if scope == "all_related":
            extra["anchor_artifact_id"] = anchor_artifact_id
            extra["related_set_hash"] = related_set_hash
        else:
            generation = anchor_row.get("generation") if anchor_row else None
            if isinstance(generation, int):
                extra["artifact_generation"] = generation
        next_cursor = ctx._issue_cursor(
            tool="artifact",
            artifact_id=anchor_artifact_id,
            position_state={
                "offset": offset + len(selected),
            },
            extra=extra,
        )

    lineage: dict[str, Any] = {
        "scope": scope,
        "anchor_artifact_id": anchor_artifact_id,
        "artifact_count": len(related_ids),
        "artifact_ids": related_ids,
    }
    if scope == "all_related":
        lineage["related_set_hash"] = related_set_hash
    response = {
        "artifact_id": anchor_artifact_id,
        "scope": scope,
        "target": "envelope",
        "items": selected,
        "truncated": truncated,
        "cursor": next_cursor,
        "omitted": omitted,
        "stats": {"bytes_out": used_bytes},
        "lineage": lineage,
        "pagination": build_retrieval_pagination_meta(
            truncated=truncated,
            cursor=next_cursor if next_cursor else None,
        ),
    }
    if warnings:
        response["warnings"] = warnings
    return response
