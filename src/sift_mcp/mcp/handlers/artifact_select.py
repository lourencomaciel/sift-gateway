"""Select handler using SQL queries on artifact_records."""

from __future__ import annotations

from collections.abc import Mapping
import json
from typing import TYPE_CHECKING, Any

from sift_mcp.constants import WORKSPACE_ID
from sift_mcp.cursor.payload import CursorStaleError
from sift_mcp.cursor.sample_set_hash import (
    SampleSetHashBindingError,
    assert_sample_set_hash_binding,
    compute_sample_set_hash,
)
from sift_mcp.cursor.token import (
    CursorExpiredError,
    CursorTokenError,
)
from sift_mcp.envelope.responses import gateway_error
from sift_mcp.mcp.handlers.common import (
    ARTIFACT_META_COLUMNS,
    FETCH_ARTIFACT_META_SQL,
    row_to_dict,
)
from sift_mcp.mcp.handlers.lineage_roots import (
    resolve_all_related_root_candidates,
)
from sift_mcp.mcp.lineage import (
    compute_related_set_hash,
    compute_root_signature,
    resolve_related_artifacts,
)
from sift_mcp.pagination.contract import build_retrieval_pagination_meta
from sift_mcp.query.filters import (
    compile_filter,
    filter_hash,
    parse_filter_dict,
)
from sift_mcp.query.select_paths import (
    canonicalize_select_paths,
    select_paths_hash,
)
from sift_mcp.query.select_sql import compile_select
from sift_mcp.retrieval.response import apply_output_budgets

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


def _distinct_key(raw: Any) -> str:
    """Produce a canonical dedup key for a projected value.

    SQLite returns JSON objects/arrays as strings and scalars
    as native Python types.  For strings that are valid JSON
    (objects, arrays, quoted strings) we round-trip through
    ``json.loads`` / ``json.dumps(sort_keys=True)`` for key
    stability.  Plain scalar strings that are *not* valid JSON
    (e.g. bare ``alpha`` from a scalar record column) are
    wrapped in ``json.dumps`` directly so they still produce a
    deterministic key without raising ``JSONDecodeError``.

    Args:
        raw: Value returned by ``cursor.fetchall()`` for a
            single projection column.

    Returns:
        Deterministic string key suitable for set membership.
    """
    if raw is None:
        return "null"
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            return json.dumps(raw, sort_keys=True)
        return json.dumps(parsed, sort_keys=True)
    return json.dumps(raw, sort_keys=True)


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
    from sift_mcp.tools.artifact_schema import FETCH_SCHEMA_ROOT_BY_PATH_SQL
    from sift_mcp.tools.artifact_select import (
        FETCH_ROOT_SQL,
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
    session_id = (
        str(raw_ctx["session_id"]) if isinstance(raw_ctx, dict) else ""
    )
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
        if scope is None:
            cursor_scope = cursor_payload.get("scope")
            if cursor_scope in {"all_related", "single"}:
                scope = str(cursor_scope)
            elif isinstance(
                cursor_payload.get("artifact_generation"), int
            ):
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
        if not caller_root_path:
            caller_root_path = cursor_payload.get("root_path")
        if not caller_select_paths:
            caller_select_paths = cursor_payload.get("select_paths")
        if caller_where is None:
            caller_where = cursor_payload.get("where_serialized")
        if (
            arguments.get("distinct") is None
            and cursor_payload.get("distinct") is True
        ):
            arguments["distinct"] = True
        if arguments.get("order_by") is None:
            cursor_order_by = cursor_payload.get("order_by")
            if isinstance(cursor_order_by, str) and cursor_order_by:
                arguments["order_by"] = cursor_order_by

    root_path = str(caller_root_path) if caller_root_path else ""
    if not root_path:
        return gateway_error("INVALID_ARGUMENT", "missing root_path")

    select_paths_raw = caller_select_paths if caller_select_paths else []
    where_expr = caller_where
    if where_expr is not None and not isinstance(where_expr, Mapping):
        return gateway_error(
            "INVALID_ARGUMENT",
            "where must be a filter object",
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
        return gateway_error(
            "INVALID_ARGUMENT", f"invalid select_paths: {exc}"
        )
    select_paths_binding_hash = select_paths_hash(
        select_paths,
        max_jsonpath_length=ctx.config.max_jsonpath_length,
        max_path_segments=ctx.config.max_path_segments,
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

    # Compile select projection to SQL.
    select_sql_expr, select_sql_params = compile_select(select_paths)

    related_ids: list[str] = []
    related_set_hash: str | None = None
    missing_root_artifacts: list[str] = []
    warnings: list[dict[str, Any]] = []
    items: list[dict[str, Any]] = []
    sampled_only_single = False
    all_record_indices: list[int] = []
    single_root_row: dict[str, Any] | None = None
    single_map_budget_fingerprint = ""
    anchor_meta: dict[str, Any] | None = None
    candidate_rows: list[
        tuple[str, dict[str, Any], dict[str, Any], dict[str, Any]]
    ] = []

    with ctx.db_pool.connection() as connection:
        if scope == "single":
            if not ctx._artifact_visible(
                connection,
                session_id=session_id,
                artifact_id=anchor_artifact_id,
            ):
                return gateway_error("NOT_FOUND", "artifact not found")
            related_ids = [anchor_artifact_id]
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
                    return gateway_error(
                        "GONE", "artifact has been deleted"
                    )
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
                    schema_completeness=schema_root.get(
                        "completeness"
                    ),
                )
                signature_groups.setdefault(
                    signature, []
                ).append(artifact_id)
                candidate_rows.append(
                    (
                        artifact_id,
                        artifact_meta,
                        root_row,
                        schema_root,
                    )
                )

            if not candidate_rows:
                details: dict[str, Any] = {}
                if missing_root_artifacts:
                    details = {
                        "root_path": root_path,
                        "skipped_artifacts": len(
                            missing_root_artifacts
                        ),
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
                                "signature": sig,
                                "artifact_ids": sorted(aids),
                            }
                            for sig, aids in sorted(
                                signature_groups.items()
                            )
                        ],
                    },
                )
        else:
            resolved_candidates = (
                resolve_all_related_root_candidates(
                    connection,
                    session_id=session_id,
                    anchor_artifact_id=anchor_artifact_id,
                    root_path=root_path,
                    max_related_artifacts=(
                        ctx.config.related_query_max_artifacts
                    ),
                    resolve_related_fn=resolve_related_artifacts,
                    compute_related_set_hash_fn=(
                        compute_related_set_hash
                    ),
                )
            )
            if isinstance(resolved_candidates, dict):
                return resolved_candidates
            related_ids = resolved_candidates.related_ids
            related_set_hash = resolved_candidates.related_set_hash
            candidate_rows = resolved_candidates.candidate_rows
            missing_root_artifacts = (
                resolved_candidates.missing_root_artifacts
            )

        if missing_root_artifacts:
            warnings.append(
                {
                    "code": "MISSING_ROOT_PATH",
                    "root_path": root_path,
                    "skipped_artifacts": len(
                        missing_root_artifacts
                    ),
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
                    generation = (
                        anchor_meta.get("generation")
                        if anchor_meta
                        else None
                    )
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

        # ── count_only: SQL COUNT for efficiency ────────────
        if arguments.get("count_only") is True:
            count_distinct = arguments.get("distinct") is True
            if count_distinct:
                # Distinct requires projection-level dedup;
                # collect projected values and deduplicate.
                _seen_projs: set[str] = set()
                total_count = 0
                for aid, _, _, _ in candidate_rows:
                    proj_sql = (
                        f"SELECT {select_sql_expr}"
                        " FROM artifact_records"
                        " WHERE workspace_id = ?"
                        " AND artifact_id = ?"
                        " AND root_path = ?"
                    )
                    proj_params: list[Any] = [
                        *select_sql_params,
                        WORKSPACE_ID,
                        aid,
                        root_path,
                    ]
                    if filter_sql:
                        proj_sql += f" AND ({filter_sql})"
                        proj_params.extend(filter_params)
                    d_rows = connection.execute(
                        proj_sql, proj_params
                    ).fetchall()
                    for d_row in d_rows:
                        raw = d_row[0]
                        key = _distinct_key(raw)
                        if key not in _seen_projs:
                            _seen_projs.add(key)
                            total_count += 1
            else:
                total_count = 0
                for aid, _, _, _ in candidate_rows:
                    count_sql = (
                        "SELECT COUNT(*)"
                        " FROM artifact_records"
                        " WHERE workspace_id = ?"
                        " AND artifact_id = ?"
                        " AND root_path = ?"
                    )
                    count_params: list[Any] = [
                        WORKSPACE_ID,
                        aid,
                        root_path,
                    ]
                    if filter_sql:
                        count_sql += f" AND ({filter_sql})"
                        count_params.extend(filter_params)
                    count_row = connection.execute(
                        count_sql, count_params
                    ).fetchone()
                    total_count += (
                        count_row[0] if count_row else 0
                    )

            touched = False
            for artifact_id in related_ids:
                touched = (
                    ctx._safe_touch_for_retrieval(
                        connection,
                        session_id=session_id,
                        artifact_id=artifact_id,
                    )
                    or touched
                )
            if touched:
                commit = getattr(connection, "commit", None)
                if callable(commit):
                    commit()

            return {
                "count": total_count,
                "truncated": False,
                "pagination": build_retrieval_pagination_meta(
                    truncated=False,
                    cursor=None,
                ),
            }

        # ── Query artifact_records per candidate ────────────
        for (
            artifact_id,
            artifact_meta,
            root_row,
            _schema_root,
        ) in candidate_rows:
            map_kind = str(
                artifact_meta.get("map_kind", "none")
            )
            sampled_only = map_kind == "partial"

            if (
                sampled_only
                and scope == "single"
                and artifact_id == anchor_artifact_id
            ):
                sampled_only_single = True
                single_root_row = root_row
                single_map_budget_fingerprint = (
                    str(
                        artifact_meta.get(
                            "map_budget_fingerprint"
                        )
                    )
                    if isinstance(
                        artifact_meta.get(
                            "map_budget_fingerprint"
                        ),
                        str,
                    )
                    else ""
                )
                # Fetch all record indices for hash binding.
                idx_rows = connection.execute(
                    "SELECT idx FROM artifact_records"
                    " WHERE workspace_id = ?"
                    " AND artifact_id = ?"
                    " AND root_path = ?"
                    " ORDER BY idx ASC",
                    (WORKSPACE_ID, artifact_id, root_path),
                ).fetchall()
                all_record_indices = [r[0] for r in idx_rows]

                if cursor_payload is not None:
                    try:
                        ctx._assert_cursor_field(
                            cursor_payload,
                            field="map_budget_fingerprint",
                            expected=(
                                single_map_budget_fingerprint
                            ),
                        )
                        expected_ssh = compute_sample_set_hash(
                            root_path=root_path,
                            sample_indices=all_record_indices,
                            map_budget_fingerprint=(
                                single_map_budget_fingerprint
                            ),
                        )
                        assert_sample_set_hash_binding(
                            cursor_payload, expected_ssh
                        )
                    except SampleSetHashBindingError as exc:
                        return ctx._cursor_error(
                            CursorStaleError(str(exc))
                        )

            # Build and execute SQL query.
            query_sql = (
                f"SELECT idx, {select_sql_expr}"
                " FROM artifact_records"
                " WHERE workspace_id = ?"
                " AND artifact_id = ?"
                " AND root_path = ?"
            )
            query_params: list[Any] = [
                *select_sql_params,
                WORKSPACE_ID,
                artifact_id,
                root_path,
            ]
            if filter_sql:
                query_sql += f" AND ({filter_sql})"
                query_params.extend(filter_params)
            query_sql += " ORDER BY idx ASC"

            rows = connection.execute(
                query_sql, query_params
            ).fetchall()

            strip_missing = (
                select_paths
                and not ctx.config.select_missing_as_null
            )
            for row in rows:
                idx = row[0]
                raw_projection = row[1]
                projection = (
                    json.loads(raw_projection)
                    if isinstance(raw_projection, str)
                    else raw_projection
                )
                if (
                    strip_missing
                    and isinstance(projection, dict)
                ):
                    projection = {
                        k: v
                        for k, v in projection.items()
                        if v is not None
                    }
                locator: dict[str, Any] = {
                    "artifact_id": artifact_id,
                    "root_path": root_path,
                }
                if sampled_only:
                    locator["sample_index"] = idx
                else:
                    locator["index"] = idx
                items.append(
                    {
                        "_locator": locator,
                        "projection": projection,
                    }
                )

        touched = False
        for artifact_id in related_ids:
            touched = (
                ctx._safe_touch_for_retrieval(
                    connection,
                    session_id=session_id,
                    artifact_id=artifact_id,
                )
                or touched
            )
        if touched:
            commit = getattr(connection, "commit", None)
            if callable(commit):
                commit()

    # Deduplicate by projection when distinct is requested.
    use_distinct = arguments.get("distinct") is True
    if use_distinct:
        seen: set[str] = set()
        unique_items: list[dict[str, Any]] = []
        for item in items:
            key = json.dumps(
                item.get("projection", {}), sort_keys=True
            )
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

    max_items = ctx._bounded_limit(arguments.get("limit"))
    selected, truncated, omitted, used_bytes = apply_output_budgets(
        items[offset:],
        max_items=max_items,
        max_bytes_out=ctx.config.max_bytes_out,
    )
    next_cursor: str | None = None
    if truncated:
        where_serialized: dict[str, Any] | None = None
        if where_expr is not None and isinstance(where_expr, Mapping):
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
            generation = (
                anchor_meta.get("generation") if anchor_meta else None
            )
            if isinstance(generation, int):
                extra["artifact_generation"] = generation
        if sampled_only_single:
            sample_set_hash_val = compute_sample_set_hash(
                root_path=root_path,
                sample_indices=all_record_indices,
                map_budget_fingerprint=single_map_budget_fingerprint,
            )
            extra["map_budget_fingerprint"] = (
                single_map_budget_fingerprint
            )
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
    root_summary = (
        single_root_row.get("root_summary")
        if single_root_row
        else None
    )
    if sampled_only_single and isinstance(root_summary, Mapping):
        raw_sampled_prefix_len = root_summary.get(
            "sampled_prefix_len"
        )
        if (
            isinstance(raw_sampled_prefix_len, int)
            and raw_sampled_prefix_len >= 0
        ):
            sampled_prefix_len = raw_sampled_prefix_len
    determinism: dict[str, str] | None = None
    if sampled_only_single and single_map_budget_fingerprint:
        from sift_mcp.constants import TRAVERSAL_CONTRACT_VERSION

        ssh = compute_sample_set_hash(
            root_path=root_path,
            sample_indices=all_record_indices,
            map_budget_fingerprint=single_map_budget_fingerprint,
        )
        determinism = {
            "traversal_contract_version": TRAVERSAL_CONTRACT_VERSION,
            "map_budget_fingerprint": (
                single_map_budget_fingerprint
            ),
            "sample_set_hash": ssh,
        }
    response = build_select_result(
        items=selected,
        truncated=truncated,
        cursor=next_cursor,
        total_matched=total_matched,
        sampled_only=sampled_only_single,
        sample_indices_used=sample_indices_used
        if sampled_only_single
        else None,
        sampled_prefix_len=sampled_prefix_len,
        omitted={"count": omitted, "reason": "budget"}
        if truncated
        else None,
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
