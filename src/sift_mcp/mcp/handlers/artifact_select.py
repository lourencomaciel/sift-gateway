"""Select handler using SQL queries on artifact_records."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
import json
from typing import TYPE_CHECKING, Any, cast

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
    touch_retrieval_artifacts,
)
from sift_mcp.mcp.handlers.lineage_roots import (
    resolve_all_related_root_candidates,
    resolve_single_root_candidate,
)
from sift_mcp.mcp.handlers.query_scope import (
    resolve_cursor_offset,
    resolve_scope,
)
from sift_mcp.mcp.lineage import resolve_related_artifacts
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


_CandidateRow = tuple[str, dict[str, Any], dict[str, Any], dict[str, Any]]


@dataclass(frozen=True)
class _SelectQueryState:
    """Normalized query-level state for select requests."""

    session_id: str
    anchor_artifact_id: str
    scope: str
    offset: int
    cursor_payload: dict[str, Any] | None
    cursor_has_embedded: bool


@dataclass(frozen=True)
class _SelectBindings:
    """Compiled select bindings from args/cursor payload."""

    root_path: str
    select_paths: list[str]
    where_expr: Mapping[str, Any] | None
    filter_sql: str | None
    filter_params: list[Any]
    select_sql_expr: str
    select_sql_params: list[Any]
    select_paths_binding_hash: str
    where_binding_hash: str


@dataclass(frozen=True)
class _SelectCandidates:
    """Resolved lineage candidate set for select requests."""

    related_ids: list[str]
    related_set_hash: str | None
    candidate_rows: list[_CandidateRow]
    missing_root_artifacts: list[str]
    anchor_meta: dict[str, Any] | None


@dataclass
class _SelectSamplingState:
    """Mutable sampled-single metadata used for cursor/determinism bindings."""

    sampled_only_single: bool = False
    all_record_indices: list[int] = field(default_factory=list)
    single_root_row: dict[str, Any] | None = None
    single_map_budget_fingerprint: str = ""


@dataclass(frozen=True)
class _SelectQueryPhaseResult:
    """State produced by the DB-backed selection phase."""

    candidates: _SelectCandidates
    warnings: list[dict[str, Any]]
    items: list[dict[str, Any]]
    sampling_state: _SelectSamplingState


def _resolve_select_query_state(
    *,
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> tuple[_SelectQueryState | None, dict[str, Any] | None]:
    """Resolve session/anchor/scope/cursor bindings for a select query."""
    raw_ctx = arguments.get("_gateway_context")
    session_id = str(raw_ctx["session_id"]) if isinstance(raw_ctx, dict) else ""
    anchor_artifact_id = str(arguments["artifact_id"])
    scope, scope_err = resolve_scope(raw_scope=arguments.get("scope"))
    if scope_err is not None:
        return None, scope_err

    offset = 0
    cursor_payload: dict[str, Any] | None = None
    cursor_has_embedded = False
    cursor_token = arguments.get("cursor")
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
            return None, ctx._cursor_error(exc)
        offset, offset_err = resolve_cursor_offset(position)
        if offset_err is not None:
            return None, offset_err
        scope, scope_err = resolve_scope(
            raw_scope=arguments.get("scope"),
            cursor_payload=cursor_payload,
        )
        if scope_err is not None:
            return None, scope_err
        cursor_has_embedded = isinstance(
            cursor_payload.get("select_paths"), list
        )

    return (
        _SelectQueryState(
            session_id=session_id,
            anchor_artifact_id=anchor_artifact_id,
            scope=scope,
            offset=offset,
            cursor_payload=cursor_payload,
            cursor_has_embedded=cursor_has_embedded,
        ),
        None,
    )


def _resolve_caller_select_inputs(
    *,
    arguments: dict[str, Any],
    cursor_payload: dict[str, Any] | None,
    cursor_has_embedded: bool,
) -> tuple[Any, Any, Any]:
    """Resolve root_path/select_paths/where from args and embedded cursor values."""
    caller_root_path = arguments.get("root_path")
    caller_select_paths = arguments.get("select_paths")
    caller_where = arguments.get("where")
    if not cursor_has_embedded or cursor_payload is None:
        return caller_root_path, caller_select_paths, caller_where

    if not caller_root_path:
        caller_root_path = cursor_payload.get("root_path")
    if not caller_select_paths:
        caller_select_paths = cursor_payload.get("select_paths")
    if caller_where is None:
        caller_where = cursor_payload.get("where_serialized")
    if arguments.get("distinct") is None and cursor_payload.get("distinct") is True:
        arguments["distinct"] = True
    if arguments.get("order_by") is None:
        cursor_order_by = cursor_payload.get("order_by")
        if isinstance(cursor_order_by, str) and cursor_order_by:
            arguments["order_by"] = cursor_order_by
    return caller_root_path, caller_select_paths, caller_where


def _resolve_select_bindings(
    *,
    ctx: GatewayServer,
    caller_root_path: Any,
    caller_select_paths: Any,
    caller_where: Any,
) -> tuple[_SelectBindings | None, dict[str, Any] | None]:
    """Validate and compile root/select/where bindings."""
    root_path = str(caller_root_path) if caller_root_path else ""
    if not root_path:
        return None, gateway_error("INVALID_ARGUMENT", "missing root_path")

    where_expr = caller_where
    if where_expr is not None and not isinstance(where_expr, Mapping):
        return None, gateway_error(
            "INVALID_ARGUMENT",
            "where must be a filter object",
        )

    select_paths_raw = caller_select_paths if caller_select_paths else []
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
        return None, gateway_error(
            "INVALID_ARGUMENT", f"invalid select_paths: {exc}"
        )

    select_paths_binding_hash = select_paths_hash(
        select_paths,
        max_jsonpath_length=ctx.config.max_jsonpath_length,
        max_path_segments=ctx.config.max_path_segments,
    )

    filter_sql: str | None = None
    filter_params: list[Any] = []
    if where_expr is None:
        where_binding_hash = "__none__"
    else:
        try:
            filter_obj = parse_filter_dict(dict(where_expr))
            filter_sql, filter_params = compile_filter(filter_obj)
            where_binding_hash = filter_hash(filter_obj)
        except (ValueError, KeyError, TypeError) as exc:
            return None, gateway_error(
                "INVALID_ARGUMENT",
                f"invalid where filter: {exc}",
            )

    select_sql_expr, select_sql_params = compile_select(select_paths)
    return (
        _SelectBindings(
            root_path=root_path,
            select_paths=select_paths,
            where_expr=where_expr if isinstance(where_expr, Mapping) else None,
            filter_sql=filter_sql,
            filter_params=filter_params,
            select_sql_expr=select_sql_expr,
            select_sql_params=select_sql_params,
            select_paths_binding_hash=select_paths_binding_hash,
            where_binding_hash=where_binding_hash,
        ),
        None,
    )


def _resolve_select_candidates(
    *,
    ctx: GatewayServer,
    connection: Any,
    query_state: _SelectQueryState,
    root_path: str,
) -> _SelectCandidates | dict[str, Any]:
    """Resolve candidate roots for single/all_related select scopes."""
    if query_state.scope == "single":
        if not ctx._artifact_visible(
            connection,
            session_id=query_state.session_id,
            artifact_id=query_state.anchor_artifact_id,
        ):
            return gateway_error("NOT_FOUND", "artifact not found")
        single_candidate = resolve_single_root_candidate(
            connection,
            anchor_artifact_id=query_state.anchor_artifact_id,
            root_path=root_path,
        )
        if isinstance(single_candidate, dict):
            return single_candidate
        return _SelectCandidates(
            related_ids=single_candidate.related_ids,
            related_set_hash=None,
            candidate_rows=single_candidate.candidate_rows,
            missing_root_artifacts=single_candidate.missing_root_artifacts,
            anchor_meta=single_candidate.anchor_meta,
        )

    resolved_candidates = resolve_all_related_root_candidates(
        connection,
        session_id=query_state.session_id,
        anchor_artifact_id=query_state.anchor_artifact_id,
        root_path=root_path,
        max_related_artifacts=ctx.config.related_query_max_artifacts,
        resolve_related_fn=resolve_related_artifacts,
    )
    if isinstance(resolved_candidates, dict):
        return resolved_candidates
    return _SelectCandidates(
        related_ids=resolved_candidates.related_ids,
        related_set_hash=resolved_candidates.related_set_hash,
        candidate_rows=resolved_candidates.candidate_rows,
        missing_root_artifacts=resolved_candidates.missing_root_artifacts,
        anchor_meta=None,
    )


def _missing_root_warning(
    *,
    root_path: str,
    missing_root_artifacts: list[str],
) -> dict[str, Any] | None:
    """Build warning payload when root_path is missing for some artifacts."""
    if not missing_root_artifacts:
        return None
    return {
        "code": "MISSING_ROOT_PATH",
        "root_path": root_path,
        "skipped_artifacts": len(missing_root_artifacts),
        "artifact_ids": missing_root_artifacts,
    }


def _assert_select_cursor_bindings(
    *,
    ctx: GatewayServer,
    cursor_payload: dict[str, Any] | None,
    scope: str,
    anchor_artifact_id: str,
    related_set_hash: str | None,
    anchor_meta: dict[str, Any] | None,
    root_path: str,
    select_paths_binding_hash: str,
    where_binding_hash: str,
    effective_order_by: Any,
) -> dict[str, Any] | None:
    """Assert cursor bindings for select queries."""
    if cursor_payload is None:
        return None
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
        if cursor_order_by is not None or effective_order_by is not None:
            ctx._assert_cursor_field(
                cursor_payload,
                field="order_by",
                expected=effective_order_by or "",
            )
    except CursorStaleError as exc:
        return ctx._cursor_error(exc)
    return None


def _count_select_rows(
    *,
    connection: Any,
    candidate_rows: list[_CandidateRow],
    root_path: str,
    filter_sql: str | None,
    filter_params: list[Any],
) -> int:
    """Count matching rows without projection deduplication."""
    total_count = 0
    for aid, _, _, _ in candidate_rows:
        count_sql = (
            "SELECT COUNT(*)"
            " FROM artifact_records"
            " WHERE workspace_id = ?"
            " AND artifact_id = ?"
            " AND root_path = ?"
        )
        count_params: list[Any] = [WORKSPACE_ID, aid, root_path]
        if filter_sql:
            count_sql += f" AND ({filter_sql})"
            count_params.extend(filter_params)
        count_row = connection.execute(count_sql, count_params).fetchone()
        total_count += count_row[0] if count_row else 0
    return total_count


def _count_select_distinct_rows(
    *,
    connection: Any,
    candidate_rows: list[_CandidateRow],
    root_path: str,
    select_sql_expr: str,
    select_sql_params: list[Any],
    filter_sql: str | None,
    filter_params: list[Any],
) -> int:
    """Count unique projected values across candidate rows."""
    seen: set[str] = set()
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
        for d_row in connection.execute(proj_sql, proj_params).fetchall():
            key = _distinct_key(d_row[0])
            if key in seen:
                continue
            seen.add(key)
            total_count += 1
    return total_count


def _build_count_only_response(total_count: int) -> dict[str, Any]:
    """Build count_only response envelope."""
    return {
        "count": total_count,
        "truncated": False,
        "pagination": build_retrieval_pagination_meta(
            truncated=False,
            cursor=None,
        ),
    }


def _resolve_sampling_state(
    *,
    ctx: GatewayServer,
    cursor_payload: dict[str, Any] | None,
    scope: str,
    anchor_artifact_id: str,
    artifact_id: str,
    artifact_meta: dict[str, Any],
    root_row: dict[str, Any],
    root_path: str,
    connection: Any,
    sampling_state: _SelectSamplingState,
) -> dict[str, Any] | None:
    """Capture sampled-single bindings and validate sample-set cursor hashes."""
    map_kind = str(artifact_meta.get("map_kind", "none"))
    sampled_only = map_kind == "partial"
    if not (
        sampled_only
        and scope == "single"
        and artifact_id == anchor_artifact_id
    ):
        return None

    sampling_state.sampled_only_single = True
    sampling_state.single_root_row = root_row
    raw_fingerprint = artifact_meta.get("map_budget_fingerprint")
    sampling_state.single_map_budget_fingerprint = (
        str(raw_fingerprint) if isinstance(raw_fingerprint, str) else ""
    )
    idx_rows = connection.execute(
        "SELECT idx FROM artifact_records"
        " WHERE workspace_id = ?"
        " AND artifact_id = ?"
        " AND root_path = ?"
        " ORDER BY idx ASC",
        (WORKSPACE_ID, artifact_id, root_path),
    ).fetchall()
    sampling_state.all_record_indices = [row[0] for row in idx_rows]

    if cursor_payload is None:
        return None
    try:
        ctx._assert_cursor_field(
            cursor_payload,
            field="map_budget_fingerprint",
            expected=sampling_state.single_map_budget_fingerprint,
        )
        expected_ssh = compute_sample_set_hash(
            root_path=root_path,
            sample_indices=sampling_state.all_record_indices,
            map_budget_fingerprint=sampling_state.single_map_budget_fingerprint,
        )
        assert_sample_set_hash_binding(cursor_payload, expected_ssh)
    except SampleSetHashBindingError as exc:
        return ctx._cursor_error(CursorStaleError(str(exc)))
    return None


def _collect_candidate_items(
    *,
    ctx: GatewayServer,
    connection: Any,
    candidate_rows: list[_CandidateRow],
    query_state: _SelectQueryState,
    bindings: _SelectBindings,
) -> tuple[list[dict[str, Any]], _SelectSamplingState, dict[str, Any] | None]:
    """Collect projected select items across resolved candidate rows."""
    items: list[dict[str, Any]] = []
    sampling_state = _SelectSamplingState()
    for artifact_id, artifact_meta, root_row, _schema_root in candidate_rows:
        sampling_err = _resolve_sampling_state(
            ctx=ctx,
            cursor_payload=query_state.cursor_payload,
            scope=query_state.scope,
            anchor_artifact_id=query_state.anchor_artifact_id,
            artifact_id=artifact_id,
            artifact_meta=artifact_meta,
            root_row=root_row,
            root_path=bindings.root_path,
            connection=connection,
            sampling_state=sampling_state,
        )
        if sampling_err is not None:
            return [], sampling_state, sampling_err

        map_kind = str(artifact_meta.get("map_kind", "none"))
        sampled_only = map_kind == "partial"
        query_sql = (
            f"SELECT idx, {bindings.select_sql_expr}"
            " FROM artifact_records"
            " WHERE workspace_id = ?"
            " AND artifact_id = ?"
            " AND root_path = ?"
        )
        query_params: list[Any] = [
            *bindings.select_sql_params,
            WORKSPACE_ID,
            artifact_id,
            bindings.root_path,
        ]
        if bindings.filter_sql:
            query_sql += f" AND ({bindings.filter_sql})"
            query_params.extend(bindings.filter_params)
        query_sql += " ORDER BY idx ASC"
        rows = connection.execute(query_sql, query_params).fetchall()

        strip_missing = (
            bool(bindings.select_paths)
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
            if strip_missing and isinstance(projection, dict):
                projection = {
                    key: value
                    for key, value in projection.items()
                    if value is not None
                }
            locator: dict[str, Any] = {
                "artifact_id": artifact_id,
                "root_path": bindings.root_path,
            }
            locator["sample_index" if sampled_only else "index"] = idx
            items.append({"_locator": locator, "projection": projection})
    return items, sampling_state, None


def _apply_select_distinct(
    items: list[dict[str, Any]],
    *,
    use_distinct: bool,
) -> list[dict[str, Any]]:
    """Deduplicate projection rows when distinct=True."""
    if not use_distinct:
        return items
    seen: set[str] = set()
    unique_items: list[dict[str, Any]] = []
    for item in items:
        key = json.dumps(item.get("projection", {}), sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        unique_items.append(item)
    return unique_items


def _apply_select_ordering(
    *,
    arguments: dict[str, Any],
    items: list[dict[str, Any]],
    validate_select_order_by: Any,
    parse_select_order_by: Any,
    apply_select_sort: Any,
) -> tuple[list[dict[str, Any]], str | None, dict[str, Any] | None]:
    """Apply optional order_by sorting and return binding for cursor metadata."""
    raw_order_by = arguments.get("order_by")
    if not isinstance(raw_order_by, str) or not raw_order_by.strip():
        return items, None, None

    order_by_binding = raw_order_by
    order_by_for_parse = raw_order_by.strip()
    order_err = validate_select_order_by(order_by_for_parse)
    if order_err is not None:
        return [], None, gateway_error(
            str(order_err["code"]), str(order_err["message"])
        )
    parsed_order = parse_select_order_by(order_by_for_parse)
    if parsed_order is None:
        return items, order_by_binding, None
    return apply_select_sort(items, parsed_order), order_by_binding, None


def _build_select_cursor_extra(
    *,
    bindings: _SelectBindings,
    query_state: _SelectQueryState,
    related_set_hash: str | None,
    anchor_meta: dict[str, Any] | None,
    use_distinct: bool,
    order_by_binding: str | None,
    sampling_state: _SelectSamplingState,
) -> dict[str, Any]:
    """Build cursor binding metadata for truncated select results."""
    where_serialized = (
        dict(bindings.where_expr) if isinstance(bindings.where_expr, Mapping) else None
    )
    extra: dict[str, Any] = {
        "root_path": bindings.root_path,
        "select_paths": list(bindings.select_paths),
        "where_serialized": where_serialized,
        "select_paths_hash": bindings.select_paths_binding_hash,
        "where_hash": bindings.where_binding_hash,
        "scope": query_state.scope,
    }
    if use_distinct:
        extra["distinct"] = True
    if order_by_binding is not None:
        extra["order_by"] = order_by_binding
    if query_state.scope == "all_related":
        extra["anchor_artifact_id"] = query_state.anchor_artifact_id
        extra["related_set_hash"] = related_set_hash
    else:
        generation = anchor_meta.get("generation") if anchor_meta else None
        if isinstance(generation, int):
            extra["artifact_generation"] = generation
    if sampling_state.sampled_only_single:
        sample_set_hash_val = compute_sample_set_hash(
            root_path=bindings.root_path,
            sample_indices=sampling_state.all_record_indices,
            map_budget_fingerprint=sampling_state.single_map_budget_fingerprint,
        )
        extra["map_budget_fingerprint"] = (
            sampling_state.single_map_budget_fingerprint
        )
        extra["sample_set_hash"] = sample_set_hash_val
    return extra


def _sample_indices_used(selected: list[dict[str, Any]]) -> list[int]:
    """Extract sample_index values from selected result locators."""
    return [
        int(item["_locator"]["sample_index"])
        for item in selected
        if isinstance(item.get("_locator"), dict)
        and isinstance(item["_locator"].get("sample_index"), int)
    ]


def _sampled_prefix_len(
    sampling_state: _SelectSamplingState,
) -> int | None:
    """Resolve sampled_prefix_len from single sampled root summary."""
    if not sampling_state.sampled_only_single:
        return None
    root_summary = (
        sampling_state.single_root_row.get("root_summary")
        if sampling_state.single_root_row
        else None
    )
    if not isinstance(root_summary, Mapping):
        return None
    raw_sampled_prefix_len = root_summary.get("sampled_prefix_len")
    if isinstance(raw_sampled_prefix_len, int) and raw_sampled_prefix_len >= 0:
        return raw_sampled_prefix_len
    return None


def _build_select_determinism(
    *,
    sampling_state: _SelectSamplingState,
    root_path: str,
) -> dict[str, str] | None:
    """Build determinism metadata for sampled single responses."""
    if not (
        sampling_state.sampled_only_single
        and sampling_state.single_map_budget_fingerprint
    ):
        return None
    from sift_mcp.constants import TRAVERSAL_CONTRACT_VERSION

    ssh = compute_sample_set_hash(
        root_path=root_path,
        sample_indices=sampling_state.all_record_indices,
        map_budget_fingerprint=sampling_state.single_map_budget_fingerprint,
    )
    return {
        "traversal_contract_version": TRAVERSAL_CONTRACT_VERSION,
        "map_budget_fingerprint": sampling_state.single_map_budget_fingerprint,
        "sample_set_hash": ssh,
    }


def _count_select_total(
    *,
    connection: Any,
    arguments: dict[str, Any],
    candidates: _SelectCandidates,
    bindings: _SelectBindings,
) -> int:
    """Count total matches for count_only requests."""
    if arguments.get("distinct") is True:
        return _count_select_distinct_rows(
            connection=connection,
            candidate_rows=candidates.candidate_rows,
            root_path=bindings.root_path,
            select_sql_expr=bindings.select_sql_expr,
            select_sql_params=bindings.select_sql_params,
            filter_sql=bindings.filter_sql,
            filter_params=bindings.filter_params,
        )
    return _count_select_rows(
        connection=connection,
        candidate_rows=candidates.candidate_rows,
        root_path=bindings.root_path,
        filter_sql=bindings.filter_sql,
        filter_params=bindings.filter_params,
    )


def _run_select_query_phase(
    *,
    ctx: GatewayServer,
    arguments: dict[str, Any],
    query_state: _SelectQueryState,
    bindings: _SelectBindings,
) -> tuple[_SelectQueryPhaseResult | None, dict[str, Any] | None]:
    """Execute DB-backed candidate resolution and projection collection."""
    with ctx.db_pool.connection() as connection:
        resolved = _resolve_select_candidates(
            ctx=ctx,
            connection=connection,
            query_state=query_state,
            root_path=bindings.root_path,
        )
        if isinstance(resolved, dict):
            return None, resolved
        candidates = resolved
        warnings: list[dict[str, Any]] = []
        missing_root_warning = _missing_root_warning(
            root_path=bindings.root_path,
            missing_root_artifacts=candidates.missing_root_artifacts,
        )
        if missing_root_warning is not None:
            warnings.append(missing_root_warning)

        cursor_err = _assert_select_cursor_bindings(
            ctx=ctx,
            cursor_payload=query_state.cursor_payload,
            scope=query_state.scope,
            anchor_artifact_id=query_state.anchor_artifact_id,
            related_set_hash=candidates.related_set_hash,
            anchor_meta=candidates.anchor_meta,
            root_path=bindings.root_path,
            select_paths_binding_hash=bindings.select_paths_binding_hash,
            where_binding_hash=bindings.where_binding_hash,
            effective_order_by=arguments.get("order_by"),
        )
        if cursor_err is not None:
            return None, cursor_err

        if arguments.get("count_only") is True:
            total_count = _count_select_total(
                connection=connection,
                arguments=arguments,
                candidates=candidates,
                bindings=bindings,
            )
            touch_retrieval_artifacts(
                ctx,
                connection,
                session_id=query_state.session_id,
                artifact_ids=candidates.related_ids,
            )
            return None, _build_count_only_response(total_count)

        items, sampling_state, collect_err = _collect_candidate_items(
            ctx=ctx,
            connection=connection,
            candidate_rows=candidates.candidate_rows,
            query_state=query_state,
            bindings=bindings,
        )
        if collect_err is not None:
            return None, collect_err
        touch_retrieval_artifacts(
            ctx,
            connection,
            session_id=query_state.session_id,
            artifact_ids=candidates.related_ids,
        )
        return (
            _SelectQueryPhaseResult(
                candidates=candidates,
                warnings=warnings,
                items=items,
                sampling_state=sampling_state,
            ),
            None,
        )


def _build_select_lineage(
    *,
    query_state: _SelectQueryState,
    candidates: _SelectCandidates,
) -> dict[str, Any]:
    """Build lineage payload for select responses."""
    lineage: dict[str, Any] = {
        "scope": query_state.scope,
        "anchor_artifact_id": query_state.anchor_artifact_id,
        "artifact_count": len(candidates.related_ids),
        "artifact_ids": candidates.related_ids,
    }
    if query_state.scope == "all_related":
        lineage["related_set_hash"] = candidates.related_set_hash
    return lineage


def _build_select_paginated_response(
    *,
    ctx: GatewayServer,
    arguments: dict[str, Any],
    query_state: _SelectQueryState,
    bindings: _SelectBindings,
    phase: _SelectQueryPhaseResult,
    build_select_result: Any,
    validate_select_order_by: Any,
    parse_select_order_by: Any,
    apply_select_sort: Any,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Build paginated select response after DB phase execution."""
    use_distinct = arguments.get("distinct") is True
    items = _apply_select_distinct(phase.items, use_distinct=use_distinct)
    items, order_by_binding, order_err = _apply_select_ordering(
        arguments=arguments,
        items=items,
        validate_select_order_by=validate_select_order_by,
        parse_select_order_by=parse_select_order_by,
        apply_select_sort=apply_select_sort,
    )
    if order_err is not None:
        return None, order_err

    total_matched = len(items)
    max_items = ctx._bounded_limit(arguments.get("limit"))
    selected, truncated, omitted, used_bytes = apply_output_budgets(
        items[query_state.offset :],
        max_items=max_items,
        max_bytes_out=ctx.config.max_bytes_out,
    )
    next_cursor: str | None = None
    if truncated:
        extra = _build_select_cursor_extra(
            bindings=bindings,
            query_state=query_state,
            related_set_hash=phase.candidates.related_set_hash,
            anchor_meta=phase.candidates.anchor_meta,
            use_distinct=use_distinct,
            order_by_binding=order_by_binding,
            sampling_state=phase.sampling_state,
        )
        next_cursor = ctx._issue_cursor(
            tool="artifact",
            artifact_id=query_state.anchor_artifact_id,
            position_state={"offset": query_state.offset + len(selected)},
            extra=extra,
        )

    sampled_only_single = phase.sampling_state.sampled_only_single
    response = build_select_result(
        items=selected,
        truncated=truncated,
        cursor=next_cursor,
        total_matched=total_matched,
        sampled_only=sampled_only_single,
        sample_indices_used=_sample_indices_used(selected)
        if sampled_only_single
        else None,
        sampled_prefix_len=_sampled_prefix_len(phase.sampling_state),
        omitted={"count": omitted, "reason": "budget"}
        if truncated
        else None,
        stats={"bytes_out": used_bytes},
        determinism=_build_select_determinism(
            sampling_state=phase.sampling_state,
            root_path=bindings.root_path,
        ),
    )
    response["scope"] = query_state.scope
    response["lineage"] = _build_select_lineage(
        query_state=query_state,
        candidates=phase.candidates,
    )
    if phase.warnings:
        response["warnings"] = phase.warnings
    return response, None

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
    from sift_mcp.tools.artifact_select import (
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

    query_state, state_err = _resolve_select_query_state(
        ctx=ctx,
        arguments=arguments,
    )
    if state_err is not None:
        return state_err
    query_state = cast(_SelectQueryState, query_state)

    caller_root_path, caller_select_paths, caller_where = (
        _resolve_caller_select_inputs(
            arguments=arguments,
            cursor_payload=query_state.cursor_payload,
            cursor_has_embedded=query_state.cursor_has_embedded,
        )
    )
    bindings, bindings_err = _resolve_select_bindings(
        ctx=ctx,
        caller_root_path=caller_root_path,
        caller_select_paths=caller_select_paths,
        caller_where=caller_where,
    )
    if bindings_err is not None:
        return bindings_err
    bindings = cast(_SelectBindings, bindings)

    phase, immediate_response = _run_select_query_phase(
        ctx=ctx,
        arguments=arguments,
        query_state=query_state,
        bindings=bindings,
    )
    if immediate_response is not None:
        return immediate_response
    phase = cast(_SelectQueryPhaseResult, phase)

    response, response_err = _build_select_paginated_response(
        ctx=ctx,
        arguments=arguments,
        query_state=query_state,
        bindings=bindings,
        phase=phase,
        build_select_result=build_select_result,
        validate_select_order_by=validate_select_order_by,
        parse_select_order_by=parse_select_order_by,
        apply_select_sort=_apply_select_sort,
    )
    if response_err is not None:
        return response_err
    return cast(dict[str, Any], response)
