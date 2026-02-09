"""artifact.select handler."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping

from mcp_artifact_gateway.constants import WORKSPACE_ID
from mcp_artifact_gateway.cursor.hmac import CursorExpiredError, CursorTokenError
from mcp_artifact_gateway.cursor.payload import CursorStaleError
from mcp_artifact_gateway.cursor.sample_set_hash import (
    SampleSetHashBindingError,
    assert_sample_set_hash_binding,
    compute_sample_set_hash,
)
from mcp_artifact_gateway.envelope.responses import gateway_error
from mcp_artifact_gateway.mcp.handlers.common import (
    ARTIFACT_META_COLUMNS,
    ENVELOPE_COLUMNS,
    FETCH_ARTIFACT_META_SQL,
    SAMPLE_COLUMNS,
    extract_json_target,
    row_to_dict,
    rows_to_dicts,
)
from mcp_artifact_gateway.query.jsonpath import JsonPathError, evaluate_jsonpath
from mcp_artifact_gateway.query.select_paths import (
    canonicalize_select_paths,
    project_select_paths,
    select_paths_hash,
)
from mcp_artifact_gateway.query.where_dsl import WhereDslError, evaluate_where
from mcp_artifact_gateway.query.where_hash import where_hash
from mcp_artifact_gateway.retrieval.response import apply_output_budgets
from mcp_artifact_gateway.storage.payload_store import reconstruct_envelope

if TYPE_CHECKING:
    from mcp_artifact_gateway.mcp.server import GatewayServer

_SELECT_ROOT_COLUMNS = [
    "root_key",
    "root_path",
    "count_estimate",
    "root_shape",
    "fields_top",
    "sample_indices",
    "root_summary",
]

async def handle_artifact_select(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    from mcp_artifact_gateway.tools.artifact_get import FETCH_ARTIFACT_SQL
    from mcp_artifact_gateway.tools.artifact_select import (
        FETCH_ROOT_SQL,
        FETCH_SAMPLES_SQL,
        build_select_result,
        validate_select_args,
    )

    err = validate_select_args(arguments)
    if err is not None:
        return gateway_error(str(err["code"]), str(err["message"]))
    if ctx.db_pool is None:
        return ctx._not_implemented("artifact.select")

    raw_ctx = arguments.get("_gateway_context")
    session_id = str(raw_ctx["session_id"]) if isinstance(raw_ctx, dict) else ""
    artifact_id = str(arguments["artifact_id"])
    root_path = str(arguments["root_path"])
    select_paths_raw = arguments.get("select_paths", [])
    where_expr = arguments.get("where")
    if where_expr is not None and not isinstance(where_expr, (Mapping, str)):
        return gateway_error("INVALID_ARGUMENT", "where must be an object or string")
    absolute_paths = [
        str(path)
        if str(path).startswith("$")
        else (f"${path}" if str(path).startswith("[") else f"$.{path}")
        for path in select_paths_raw
    ]
    try:
        select_paths = canonicalize_select_paths(
            absolute_paths,
            max_jsonpath_length=ctx.config.max_jsonpath_length,
            max_path_segments=ctx.config.max_path_segments,
        )
    except (ValueError, TypeError) as exc:
        return gateway_error("INVALID_ARGUMENT", f"invalid select_paths: {exc}")
    select_paths_binding_hash = select_paths_hash(
        select_paths,
        max_jsonpath_length=ctx.config.max_jsonpath_length,
        max_path_segments=ctx.config.max_path_segments,
    )
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

    offset = 0
    cursor_payload: dict[str, Any] | None = None
    cursor_token = arguments.get("cursor")
    if isinstance(cursor_token, str) and cursor_token:
        try:
            cursor_payload = ctx._verify_cursor_payload(
                token=cursor_token,
                tool="artifact.select",
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

        root_row = row_to_dict(
            connection.execute(
                FETCH_ROOT_SQL,
                (WORKSPACE_ID, artifact_id, root_path),
            ).fetchone(),
            _SELECT_ROOT_COLUMNS,
        )
        if root_row is None:
            return gateway_error("NOT_FOUND", "root_path not found")

        items: list[dict[str, Any]] = []
        map_kind = str(artifact_meta.get("map_kind", "none"))
        sampled_only = map_kind == "partial"
        sample_rows: list[dict[str, Any]] = []
        map_budget_fingerprint = (
            str(artifact_meta.get("map_budget_fingerprint"))
            if isinstance(artifact_meta.get("map_budget_fingerprint"), str)
            else ""
        )

        if sampled_only:
            sample_rows = rows_to_dicts(
                connection.execute(
                    FETCH_SAMPLES_SQL,
                    (WORKSPACE_ID, artifact_id, root_row["root_key"]),
                ).fetchall(),
                SAMPLE_COLUMNS,
            )
            corruption = ctx._check_sample_corruption(root_row, sample_rows)
            if corruption is not None:
                return corruption

        if cursor_payload is not None:
            try:
                ctx._assert_cursor_field(
                    cursor_payload,
                    field="root_path",
                    expected=root_path,
                )
                ctx._assert_cursor_field(
                    cursor_payload,
                    field="select_paths_hash",
                    expected=select_paths_binding_hash,
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
                if sampled_only:
                    ctx._assert_cursor_field(
                        cursor_payload,
                        field="map_budget_fingerprint",
                        expected=map_budget_fingerprint,
                    )
                    sample_indices = sorted(
                        int(sample_index)
                        for sample in sample_rows
                        if isinstance((sample_index := sample.get("sample_index")), int)
                    )
                    expected_sample_set_hash = compute_sample_set_hash(
                        root_path=root_path,
                        sample_indices=sample_indices,
                        map_budget_fingerprint=map_budget_fingerprint,
                    )
                    assert_sample_set_hash_binding(cursor_payload, expected_sample_set_hash)
            except (CursorStaleError, SampleSetHashBindingError) as exc:
                if isinstance(exc, SampleSetHashBindingError):
                    return ctx._cursor_error(CursorStaleError(str(exc)))
                return ctx._cursor_error(exc)

        if sampled_only:
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
                projection = project_select_paths(
                    record,
                    select_paths,
                    missing_as_null=ctx.config.select_missing_as_null,
                    max_jsonpath_length=ctx.config.max_jsonpath_length,
                    max_path_segments=ctx.config.max_path_segments,
                    max_wildcard_expansion_total=ctx.config.max_wildcard_expansion_total,
                )
                items.append(
                    {
                        "_locator": {
                            "root_path": root_path,
                            "sample_index": sample.get("sample_index"),
                        },
                        "projection": projection,
                    }
                )
        else:
            artifact_row = row_to_dict(
                connection.execute(
                    FETCH_ARTIFACT_SQL,
                    (WORKSPACE_ID, artifact_id),
                ).fetchone(),
                ENVELOPE_COLUMNS,
            )
            if artifact_row is None:
                return gateway_error("NOT_FOUND", "artifact not found")
            envelope_value = artifact_row.get("envelope")
            canonical_bytes_raw = artifact_row.get("envelope_canonical_bytes")
            if isinstance(envelope_value, dict) and "content" in envelope_value:
                envelope = envelope_value
            elif canonical_bytes_raw is None:
                return gateway_error("INTERNAL_ERROR", "missing canonical bytes for artifact")
            else:
                try:
                    envelope = reconstruct_envelope(
                        compressed_bytes=bytes(canonical_bytes_raw),
                        encoding=str(artifact_row.get("envelope_canonical_encoding", "none")),
                        expected_hash=str(artifact_row.get("payload_hash_full", "")),
                    )
                except ValueError as exc:
                    return gateway_error("INTERNAL_ERROR", f"envelope reconstruction failed: {exc}")

            json_target = extract_json_target(
                envelope, artifact_row.get("mapped_part_index")
            )

            try:
                root_values = evaluate_jsonpath(
                    json_target,
                    root_path,
                    max_length=ctx.config.max_jsonpath_length,
                    max_segments=ctx.config.max_path_segments,
                    max_wildcard_expansion_total=ctx.config.max_wildcard_expansion_total,
                )
            except JsonPathError as exc:
                return gateway_error("INVALID_ARGUMENT", str(exc))

            records: list[Any]
            if len(root_values) == 1 and isinstance(root_values[0], list):
                records = list(root_values[0])
            else:
                records = list(root_values)

            for index, record in enumerate(records):
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
                projection = project_select_paths(
                    record,
                    select_paths,
                    missing_as_null=ctx.config.select_missing_as_null,
                    max_jsonpath_length=ctx.config.max_jsonpath_length,
                    max_path_segments=ctx.config.max_path_segments,
                    max_wildcard_expansion_total=ctx.config.max_wildcard_expansion_total,
                )
                items.append(
                    {
                        "_locator": {
                            "root_path": root_path,
                            "index": index,
                        },
                        "projection": projection,
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

    max_items = ctx._bounded_limit(arguments.get("limit"))
    selected, truncated, omitted, used_bytes = apply_output_budgets(
        items[offset:],
        max_items=max_items,
        max_bytes_out=ctx.config.max_bytes_out,
    )
    next_cursor: str | None = None
    if truncated:
        extra: dict[str, Any] = {
            "root_path": root_path,
            "select_paths_hash": select_paths_binding_hash,
            "where_hash": where_binding_hash,
        }
        generation = artifact_meta.get("generation")
        if isinstance(generation, int):
            extra["artifact_generation"] = generation
        if sampled_only:
            sample_indices = sorted(
                int(sample_index)
                for sample in sample_rows
                if isinstance((sample_index := sample.get("sample_index")), int)
            )
            sample_set_hash_val = compute_sample_set_hash(
                root_path=root_path,
                sample_indices=sample_indices,
                map_budget_fingerprint=map_budget_fingerprint,
            )
            extra["map_budget_fingerprint"] = map_budget_fingerprint
            extra["sample_set_hash"] = sample_set_hash_val
        next_cursor = ctx._issue_cursor(
            tool="artifact.select",
            artifact_id=artifact_id,
            position_state={"offset": offset + len(selected)},
            extra=extra,
        )
    sample_indices_used = [
        int(item["_locator"]["sample_index"])
        for item in selected
        if isinstance(item.get("_locator"), dict)
        and isinstance(item["_locator"].get("sample_index"), int)
    ]
    sampled_prefix_len: int | None = None
    root_summary = root_row.get("root_summary")
    if sampled_only and isinstance(root_summary, Mapping):
        raw_sampled_prefix_len = root_summary.get("sampled_prefix_len")
        if isinstance(raw_sampled_prefix_len, int) and raw_sampled_prefix_len >= 0:
            sampled_prefix_len = raw_sampled_prefix_len
    return build_select_result(
        items=selected,
        truncated=truncated,
        cursor=next_cursor,
        sampled_only=sampled_only,
        sample_indices_used=sample_indices_used if sampled_only else None,
        sampled_prefix_len=sampled_prefix_len,
        omitted={"count": omitted, "reason": "budget"} if truncated else None,
        stats={"bytes_out": used_bytes},
    )
