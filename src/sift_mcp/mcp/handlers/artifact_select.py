"""Legacy select handler for ``artifact(action="query", query_kind="select")``."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Mapping

from sift_mcp.constants import WORKSPACE_ID
from sift_mcp.cursor.hmac import (
    CursorExpiredError,
    CursorTokenError,
)
from sift_mcp.cursor.payload import CursorStaleError
from sift_mcp.cursor.sample_set_hash import (
    SampleSetHashBindingError,
    assert_sample_set_hash_binding,
    compute_sample_set_hash,
)
from sift_mcp.envelope.responses import gateway_error
from sift_mcp.mcp.handlers.common import (
    ARTIFACT_META_COLUMNS,
    ENVELOPE_COLUMNS,
    FETCH_ARTIFACT_META_SQL,
    SAMPLE_COLUMNS,
    extract_json_target,
    row_to_dict,
    rows_to_dicts,
)
from sift_mcp.mcp.lineage import (
    compute_related_set_hash,
    compute_root_signature,
    resolve_related_artifacts,
)
from sift_mcp.pagination.contract import build_retrieval_pagination_meta
from sift_mcp.query.jsonpath import JsonPathError, evaluate_jsonpath
from sift_mcp.query.select_paths import (
    canonicalize_select_paths,
    project_select_paths,
    select_paths_hash,
)
from sift_mcp.query.where_dsl import WhereDslError, evaluate_where
from sift_mcp.query.where_hash import where_hash
from sift_mcp.retrieval.response import apply_output_budgets
from sift_mcp.storage.payload_store import reconstruct_envelope

if TYPE_CHECKING:
    from sift_mcp.mcp.server import GatewayServer

_SELECT_ROOT_COLUMNS = [
    "root_key",
    "root_path",
    "count_estimate",
    "root_shape",
    "fields_top",
    "sample_indices",
    "root_summary",
]

_SCHEMA_ROOT_COLUMNS = [
    "root_key",
    "root_path",
    "schema_version",
    "schema_hash",
    "mode",
    "completeness",
    "observed_records",
    "dataset_hash",
    "traversal_contract_version",
    "map_budget_fingerprint",
]


async def handle_artifact_select(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Handle select-mode artifact queries.

    Args:
        ctx: Gateway server instance providing DB and cursor helpers.
        arguments: Tool arguments including ``artifact_id``,
            ``root_path``, ``select_paths``, optional ``where``,
            ``cursor``, and ``limit``.

    Returns:
        Paginated select response with projected records, or a
        gateway error.
    """
    from sift_mcp.tools.artifact_get import FETCH_ARTIFACT_SQL
    from sift_mcp.tools.artifact_schema import FETCH_SCHEMA_ROOT_BY_PATH_SQL
    from sift_mcp.tools.artifact_select import (
        FETCH_ROOT_SQL,
        FETCH_SAMPLES_SQL,
        _apply_select_sort,
        build_select_result,
        parse_select_order_by,
        validate_select_args,
        validate_select_order_by,
    )

    err = validate_select_args(arguments)
    if err is not None:
        return gateway_error(str(err["code"]), str(err["message"]))
    if ctx.db_pool is None:
        return ctx._not_implemented("artifact")

    raw_ctx = arguments.get("_gateway_context")
    session_id = str(raw_ctx["session_id"]) if isinstance(raw_ctx, dict) else ""
    anchor_artifact_id = str(arguments["artifact_id"])
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

    # Verify cursor first — we may need embedded values.
    offset = 0
    cursor_payload: dict[str, Any] | None = None
    cursor_token = arguments.get("cursor")
    cursor_has_embedded = False
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
        cursor_has_embedded = isinstance(
            cursor_payload.get("select_paths"), list
        )

    if scope is None:
        scope = "all_related"

    # Extract root_path, select_paths, where — from args or cursor.
    caller_root_path = arguments.get("root_path")
    caller_select_paths = arguments.get("select_paths")
    caller_where = arguments.get("where")

    if cursor_has_embedded and cursor_payload is not None:
        # Use embedded values for anything the caller omitted.
        if not caller_root_path:
            caller_root_path = cursor_payload.get("root_path")
        if not caller_select_paths:
            caller_select_paths = cursor_payload.get("select_paths")
        if caller_where is None:
            caller_where = cursor_payload.get("where_serialized")
        # Restore distinct flag from cursor if caller didn't set it.
        if arguments.get("distinct") is None:
            if cursor_payload.get("distinct") is True:
                arguments["distinct"] = True
        # Restore order_by from cursor if caller omitted it.
        if arguments.get("order_by") is None:
            cursor_order_by = cursor_payload.get("order_by")
            if isinstance(cursor_order_by, str) and cursor_order_by:
                arguments["order_by"] = cursor_order_by

    root_path = str(caller_root_path) if caller_root_path else ""
    if not root_path:
        return gateway_error("INVALID_ARGUMENT", "missing root_path")

    select_paths_raw = caller_select_paths if caller_select_paths else []
    where_expr = caller_where
    if where_expr is not None and not isinstance(where_expr, (Mapping, str)):
        return gateway_error(
            "INVALID_ARGUMENT", "where must be an object or string"
        )
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

    related_rows: list[dict[str, Any]] = []
    related_ids: list[str] = []
    related_set_hash: str | None = None
    missing_root_artifacts: list[str] = []
    warnings: list[dict[str, Any]] = []
    items: list[dict[str, Any]] = []
    sampled_only_single = False
    single_sample_rows: list[dict[str, Any]] = []
    single_root_row: dict[str, Any] | None = None
    single_map_budget_fingerprint = ""
    anchor_meta: dict[str, Any] | None = None

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

        candidate_rows: list[
            tuple[str, dict[str, Any], dict[str, Any], dict[str, Any]]
        ] = []
        signature_groups: dict[str, list[str]] = {}
        for artifact_id in related_ids:
            artifact_meta = row_to_dict(
                connection.execute(
                    FETCH_ARTIFACT_META_SQL,
                    (WORKSPACE_ID, artifact_id),
                ).fetchone(),
                ARTIFACT_META_COLUMNS,
            )
            if artifact_meta is None:
                continue
            if artifact_id == anchor_artifact_id:
                anchor_meta = artifact_meta
            if artifact_meta.get("deleted_at") is not None:
                if scope == "single":
                    ctx._safe_touch_for_retrieval(
                        connection,
                        session_id=session_id,
                        artifact_id=artifact_id,
                    )
                    commit = getattr(connection, "commit", None)
                    if callable(commit):
                        commit()
                    return gateway_error("GONE", "artifact has been deleted")
                continue
            if artifact_meta.get("map_status") != "ready":
                if scope == "single":
                    return gateway_error(
                        "INVALID_ARGUMENT",
                        "artifact mapping is not ready",
                    )
                missing_root_artifacts.append(artifact_id)
                continue
            root_row = row_to_dict(
                connection.execute(
                    FETCH_ROOT_SQL,
                    (WORKSPACE_ID, artifact_id, root_path),
                ).fetchone(),
                _SELECT_ROOT_COLUMNS,
            )
            if root_row is None:
                missing_root_artifacts.append(artifact_id)
                continue
            schema_root = row_to_dict(
                connection.execute(
                    FETCH_SCHEMA_ROOT_BY_PATH_SQL,
                    (WORKSPACE_ID, artifact_id, root_path),
                ).fetchone(),
                _SCHEMA_ROOT_COLUMNS,
            )
            if schema_root is None:
                missing_root_artifacts.append(artifact_id)
                continue
            signature = compute_root_signature(
                root_path=root_path,
                schema_hash=schema_root.get("schema_hash"),
                schema_mode=schema_root.get("mode"),
                schema_completeness=schema_root.get("completeness"),
            )
            signature_groups.setdefault(signature, []).append(artifact_id)
            candidate_rows.append(
                (artifact_id, artifact_meta, root_row, schema_root)
            )

        if not candidate_rows:
            details: dict[str, Any] = {}
            if missing_root_artifacts:
                details = {
                    "root_path": root_path,
                    "skipped_artifacts": len(missing_root_artifacts),
                    "artifact_ids": missing_root_artifacts,
                }
            return gateway_error(
                "NOT_FOUND",
                "root_path not found",
                details=details,
            )

        if len(signature_groups) > 1:
            return gateway_error(
                "INVALID_ARGUMENT",
                "incompatible lineage schema for root_path",
                details={
                    "code": "INCOMPATIBLE_LINEAGE_SCHEMA",
                    "root_path": root_path,
                    "signature_groups": [
                        {
                            "signature": signature,
                            "artifact_ids": sorted(artifact_ids),
                        }
                        for signature, artifact_ids in sorted(
                            signature_groups.items()
                        )
                    ],
                },
            )

        if missing_root_artifacts:
            warnings.append(
                {
                    "code": "MISSING_ROOT_PATH",
                    "root_path": root_path,
                    "skipped_artifacts": len(missing_root_artifacts),
                    "artifact_ids": missing_root_artifacts,
                }
            )

        if cursor_payload is not None:
            try:
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
                    generation = anchor_meta.get("generation") if anchor_meta else None
                    if isinstance(generation, int):
                        ctx._assert_cursor_field(
                            cursor_payload,
                            field="artifact_generation",
                            expected=generation,
                        )
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
                cursor_order_by = cursor_payload.get("order_by")
                effective_order_by = arguments.get("order_by")
                if (
                    cursor_order_by is not None
                    or effective_order_by is not None
                ):
                    ctx._assert_cursor_field(
                        cursor_payload,
                        field="order_by",
                        expected=effective_order_by or "",
                    )
            except CursorStaleError as exc:
                return ctx._cursor_error(exc)

        for artifact_id, artifact_meta, root_row, _schema_root in candidate_rows:
            map_kind = str(artifact_meta.get("map_kind", "none"))
            sampled_only = map_kind == "partial"

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

                if scope == "single" and artifact_id == anchor_artifact_id:
                    sampled_only_single = True
                    single_sample_rows = sample_rows
                    single_root_row = root_row
                    single_map_budget_fingerprint = (
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
                                field="map_budget_fingerprint",
                                expected=single_map_budget_fingerprint,
                            )
                            sample_indices = sorted(
                                int(sample_index)
                                for sample in sample_rows
                                if isinstance(
                                    (
                                        sample_index := sample.get(
                                            "sample_index"
                                        )
                                    ),
                                    int,
                                )
                            )
                            expected_sample_set_hash = compute_sample_set_hash(
                                root_path=root_path,
                                sample_indices=sample_indices,
                                map_budget_fingerprint=single_map_budget_fingerprint,
                            )
                            assert_sample_set_hash_binding(
                                cursor_payload, expected_sample_set_hash
                            )
                        except SampleSetHashBindingError as exc:
                            return ctx._cursor_error(
                                CursorStaleError(str(exc))
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
                                "artifact_id": artifact_id,
                                "root_path": root_path,
                                "sample_index": sample.get("sample_index"),
                            },
                            "projection": projection,
                        }
                    )
                continue

            sampled_only_single = False
            artifact_row = row_to_dict(
                connection.execute(
                    FETCH_ARTIFACT_SQL,
                    (WORKSPACE_ID, artifact_id),
                ).fetchone(),
                ENVELOPE_COLUMNS,
            )
            if artifact_row is None:
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
                            "artifact_id": artifact_id,
                            "root_path": root_path,
                            "index": index,
                        },
                        "projection": projection,
                    }
                )

        ctx._safe_touch_for_retrieval_many(
            connection,
            session_id=session_id,
            artifact_ids=related_ids,
        )
        commit = getattr(connection, "commit", None)
        if callable(commit):
            commit()

    # Deduplicate by projection when distinct is requested.
    use_distinct = arguments.get("distinct") is True
    if use_distinct:
        seen: set[str] = set()
        unique_items: list[dict[str, Any]] = []
        for item in items:
            key = json.dumps(item.get("projection", {}), sort_keys=True)
            if key not in seen:
                seen.add(key)
                unique_items.append(item)
        items = unique_items

    # Apply select-style order_by sorting.
    raw_order_by = arguments.get("order_by")
    order_by_binding: str | None = None
    parsed_order = None
    if isinstance(raw_order_by, str) and raw_order_by.strip():
        order_by_binding = raw_order_by
        order_by_for_parse = raw_order_by.strip()
        order_err = validate_select_order_by(order_by_for_parse)
        if order_err is not None:
            return gateway_error(
                str(order_err["code"]), str(order_err["message"])
            )
        parsed_order = parse_select_order_by(order_by_for_parse)
        if parsed_order is not None:
            items = _apply_select_sort(items, parsed_order)

    total_matched = len(items)

    # count_only: return just the count, no items or pagination.
    if arguments.get("count_only") is True:
        return {
            "count": total_matched,
            "truncated": False,
            "pagination": build_retrieval_pagination_meta(
                truncated=False,
                cursor=None,
            ),
        }

    max_items = ctx._bounded_limit(arguments.get("limit"))
    selected, truncated, omitted, used_bytes = apply_output_budgets(
        items[offset:],
        max_items=max_items,
        max_bytes_out=ctx.config.max_bytes_out,
    )
    next_cursor: str | None = None
    if truncated:
        # Serialize where for cursor embedding.
        where_serialized: str | dict[str, Any] | None = None
        if where_expr is not None:
            if isinstance(where_expr, str):
                where_serialized = where_expr
            elif isinstance(where_expr, Mapping):
                where_serialized = dict(where_expr)
        extra: dict[str, Any] = {
            "root_path": root_path,
            "select_paths": list(select_paths),
            "where_serialized": where_serialized,
            "select_paths_hash": select_paths_binding_hash,
            "where_hash": where_binding_hash,
        }
        if use_distinct:
            extra["distinct"] = True
        if order_by_binding is not None:
            extra["order_by"] = order_by_binding
        extra["scope"] = scope
        if scope == "all_related":
            extra["anchor_artifact_id"] = anchor_artifact_id
            extra["related_set_hash"] = related_set_hash
        else:
            generation = anchor_meta.get("generation") if anchor_meta else None
            if isinstance(generation, int):
                extra["artifact_generation"] = generation
        if sampled_only_single:
            sample_indices = sorted(
                int(sample_index)
                for sample in single_sample_rows
                if isinstance((sample_index := sample.get("sample_index")), int)
            )
            sample_set_hash_val = compute_sample_set_hash(
                root_path=root_path,
                sample_indices=sample_indices,
                map_budget_fingerprint=single_map_budget_fingerprint,
            )
            extra["map_budget_fingerprint"] = single_map_budget_fingerprint
            extra["sample_set_hash"] = sample_set_hash_val
        next_cursor = ctx._issue_cursor(
            tool="artifact",
            artifact_id=anchor_artifact_id,
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
    root_summary = single_root_row.get("root_summary") if single_root_row else None
    if sampled_only_single and isinstance(root_summary, Mapping):
        raw_sampled_prefix_len = root_summary.get("sampled_prefix_len")
        if (
            isinstance(raw_sampled_prefix_len, int)
            and raw_sampled_prefix_len >= 0
        ):
            sampled_prefix_len = raw_sampled_prefix_len
    determinism: dict[str, str] | None = None
    if sampled_only_single and single_map_budget_fingerprint:
        from sift_mcp.constants import TRAVERSAL_CONTRACT_VERSION

        all_sample_indices = sorted(
            int(si)
            for sample in single_sample_rows
            if isinstance((si := sample.get("sample_index")), int)
        )
        ssh = compute_sample_set_hash(
            root_path=root_path,
            sample_indices=all_sample_indices,
            map_budget_fingerprint=single_map_budget_fingerprint,
        )
        determinism = {
            "traversal_contract_version": TRAVERSAL_CONTRACT_VERSION,
            "map_budget_fingerprint": single_map_budget_fingerprint,
            "sample_set_hash": ssh,
        }
    response = build_select_result(
        items=selected,
        truncated=truncated,
        cursor=next_cursor,
        total_matched=total_matched,
        sampled_only=sampled_only_single,
        sample_indices_used=sample_indices_used if sampled_only_single else None,
        sampled_prefix_len=sampled_prefix_len,
        omitted={"count": omitted, "reason": "budget"} if truncated else None,
        stats={"bytes_out": used_bytes},
        determinism=determinism,
    )
    lineage: dict[str, Any] = {
        "scope": scope,
        "anchor_artifact_id": anchor_artifact_id,
        "artifact_count": len(related_ids),
        "artifact_ids": related_ids,
    }
    if scope == "all_related":
        lineage["related_set_hash"] = related_set_hash
    response["scope"] = scope
    response["lineage"] = lineage
    if warnings:
        response["warnings"] = warnings
    return response
