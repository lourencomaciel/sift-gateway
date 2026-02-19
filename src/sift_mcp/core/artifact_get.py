"""Protocol-agnostic artifact get execution service."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sift_mcp.constants import WORKSPACE_ID
from sift_mcp.core.query_scope import resolve_cursor_offset, resolve_scope
from sift_mcp.core.rows import row_to_dict, rows_to_dicts
from sift_mcp.core.runtime import ArtifactGetRuntime
from sift_mcp.envelope.responses import gateway_error
from sift_mcp.pagination.contract import build_retrieval_pagination_meta
from sift_mcp.query.jsonpath import (
    JsonPathError,
    canonicalize_jsonpath,
    evaluate_jsonpath,
)
from sift_mcp.retrieval.response import apply_output_budgets
from sift_mcp.storage.payload_store import reconstruct_envelope
from sift_mcp.tools.artifact_describe import FETCH_ROOTS_SQL
from sift_mcp.tools.artifact_get import (
    FETCH_ARTIFACT_SQL,
    check_get_preconditions,
    validate_get_args,
)

ENVELOPE_COLUMNS = [
    "artifact_id",
    "payload_hash_full",
    "deleted_at",
    "map_kind",
    "map_status",
    "generation",
    "mapped_part_index",
    "map_budget_fingerprint",
    "envelope",
    "envelope_canonical_encoding",
    "payload_fs_path",
    "contains_binary_refs",
]

ROOT_COLUMNS = [
    "root_key",
    "root_path",
    "count_estimate",
    "inventory_coverage",
    "root_summary",
    "root_score",
    "root_shape",
    "fields_top",
    "sample_indices",
]


def extract_json_target(
    envelope: dict[str, Any], mapped_part_index: int | None
) -> Any:
    """Extract JSON content target that mapping root_paths are relative to."""
    from sift_mcp.mapping.json_strings import resolve_json_strings

    if not isinstance(mapped_part_index, int):
        return envelope
    content = envelope.get("content", [])
    if 0 <= mapped_part_index < len(content):
        part = content[mapped_part_index]
        if (
            isinstance(part, dict)
            and part.get("type") == "json"
            and "value" in part
        ):
            return resolve_json_strings(part["value"])
    return envelope


@dataclass(frozen=True)
class _GetQueryState:
    """Normalized request state for get-mode queries."""

    session_id: str
    anchor_artifact_id: str
    target: str
    jsonpath: str | None
    normalized_jsonpath: str
    scope: str
    offset: int
    cursor_payload: dict[str, Any] | None


@dataclass(frozen=True)
class _GetRelatedArtifacts:
    """Resolved related-artifact selection for get-mode queries."""

    related_rows: list[dict[str, Any]]
    related_ids: list[str]
    related_set_hash: str | None


def _normalize_jsonpath(
    *,
    runtime: ArtifactGetRuntime,
    jsonpath: Any,
) -> tuple[str | None, str, dict[str, Any] | None]:
    """Validate optional jsonpath and return normalized canonical path."""
    if jsonpath is not None and not isinstance(jsonpath, str):
        return None, "$", gateway_error(
            "INVALID_ARGUMENT",
            "jsonpath must be a string when provided",
        )
    if not isinstance(jsonpath, str):
        return None, "$", None
    try:
        normalized_jsonpath = canonicalize_jsonpath(
            jsonpath,
            max_length=runtime.max_jsonpath_length,
            max_segments=runtime.max_path_segments,
        )
    except JsonPathError as exc:
        return None, "$", gateway_error(
            "INVALID_ARGUMENT",
            f"invalid jsonpath: {exc}",
        )
    return jsonpath, normalized_jsonpath, None


def _resolve_get_query_state(
    *,
    runtime: ArtifactGetRuntime,
    arguments: dict[str, Any],
) -> tuple[_GetQueryState | None, dict[str, Any] | None]:
    """Parse, validate, and normalize get query state."""
    raw_ctx = arguments.get("_gateway_context")
    session_id = str(raw_ctx["session_id"]) if isinstance(raw_ctx, dict) else ""
    anchor_artifact_id = str(arguments["artifact_id"])
    target = str(arguments.get("target", "envelope"))

    scope, scope_err = resolve_scope(raw_scope=arguments.get("scope"))
    if scope_err is not None:
        return None, scope_err

    jsonpath, normalized_jsonpath, jsonpath_err = _normalize_jsonpath(
        runtime=runtime,
        jsonpath=arguments.get("jsonpath"),
    )
    if jsonpath_err is not None:
        return None, jsonpath_err

    offset = 0
    cursor_payload: dict[str, Any] | None = None
    cursor_token = arguments.get("cursor")
    if isinstance(cursor_token, str) and cursor_token:
        try:
            cursor_payload = runtime.verify_cursor_payload(
                token=cursor_token,
                tool="artifact",
                artifact_id=anchor_artifact_id,
            )
            position = runtime.cursor_position(cursor_payload)
        except Exception as exc:
            return None, runtime.cursor_error(exc)
        offset, offset_err = resolve_cursor_offset(position)
        if offset_err is not None:
            return None, offset_err
        scope, scope_err = resolve_scope(
            raw_scope=arguments.get("scope"),
            cursor_payload=cursor_payload,
        )
        if scope_err is not None:
            return None, scope_err

    return (
        _GetQueryState(
            session_id=session_id,
            anchor_artifact_id=anchor_artifact_id,
            target=target,
            jsonpath=jsonpath,
            normalized_jsonpath=normalized_jsonpath,
            scope=scope,
            offset=offset,
            cursor_payload=cursor_payload,
        ),
        None,
    )


def _resolve_get_related_artifacts(
    *,
    runtime: ArtifactGetRuntime,
    connection: Any,
    session_id: str,
    anchor_artifact_id: str,
    scope: str,
) -> _GetRelatedArtifacts | dict[str, Any]:
    """Resolve related artifacts for a get query based on scope."""
    if scope == "single":
        if not runtime.artifact_visible(
            connection,
            session_id=session_id,
            artifact_id=anchor_artifact_id,
        ):
            return gateway_error("NOT_FOUND", "artifact not found")
        return _GetRelatedArtifacts(
            related_rows=[],
            related_ids=[anchor_artifact_id],
            related_set_hash=None,
        )

    related_rows = runtime.resolve_related_artifacts(
        connection,
        session_id=session_id,
        anchor_artifact_id=anchor_artifact_id,
    )
    if not related_rows:
        return gateway_error("NOT_FOUND", "artifact not found")
    if len(related_rows) > runtime.related_query_max_artifacts:
        return gateway_error(
            "RESOURCE_EXHAUSTED",
            "lineage query exceeds related artifact limit",
            details={
                "artifact_count": len(related_rows),
                "max_artifacts": runtime.related_query_max_artifacts,
            },
        )
    related_ids = [
        artifact_id
        for row in related_rows
        if isinstance((artifact_id := row.get("artifact_id")), str)
    ]
    if not related_ids:
        return gateway_error("NOT_FOUND", "artifact not found")
    return _GetRelatedArtifacts(
        related_rows=related_rows,
        related_ids=related_ids,
        related_set_hash=runtime.compute_related_set_hash(related_rows),
    )


def _load_get_artifact_rows(
    *,
    connection: Any,
    related_ids: list[str],
    anchor_artifact_id: str,
    scope: str,
    target: str,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None, list[dict[str, Any]], dict[str, Any] | None]:
    """Load artifact rows and apply preconditions/warning policy."""
    artifact_rows: list[dict[str, Any]] = []
    anchor_row: dict[str, Any] | None = None
    warnings: list[dict[str, Any]] = []

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
            return [], None, warnings, gateway_error(
                str(precondition["code"]),
                str(precondition["message"]),
            )
        artifact_rows.append(row)

    return artifact_rows, anchor_row, warnings, None


def _assert_get_cursor_bindings(
    *,
    runtime: ArtifactGetRuntime,
    cursor_payload: dict[str, Any] | None,
    target: str,
    normalized_jsonpath: str,
    scope: str,
    anchor_artifact_id: str,
    related_set_hash: str | None,
    anchor_row: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Validate cursor binding fields for get queries."""
    if cursor_payload is None:
        return None
    try:
        runtime.assert_cursor_field(
            cursor_payload,
            field="target",
            expected=target,
        )
        runtime.assert_cursor_field(
            cursor_payload,
            field="normalized_jsonpath",
            expected=normalized_jsonpath,
        )
        if isinstance(cursor_payload.get("scope"), str):
            runtime.assert_cursor_field(
                cursor_payload,
                field="scope",
                expected=scope,
            )
        if scope == "all_related":
            runtime.assert_cursor_field(
                cursor_payload,
                field="anchor_artifact_id",
                expected=anchor_artifact_id,
            )
            runtime.assert_cursor_field(
                cursor_payload,
                field="related_set_hash",
                expected=related_set_hash,
            )
        else:
            generation = anchor_row.get("generation") if anchor_row else None
            if isinstance(generation, int):
                runtime.assert_cursor_field(
                    cursor_payload,
                    field="artifact_generation",
                    expected=generation,
                )
    except Exception as exc:
        return runtime.cursor_error(exc)
    return None


def _build_mapped_get_response(
    *,
    runtime: ArtifactGetRuntime,
    scope: str,
    anchor_artifact_id: str,
    related_ids: list[str],
    related_set_hash: str | None,
    root_entries: list[dict[str, Any]],
    warnings: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build final mapped-target response."""
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
        "roots": runtime.build_lineage_root_catalog(root_entries),
        "pagination": build_retrieval_pagination_meta(
            truncated=False,
            cursor=None,
        ),
    }
    if warnings:
        response["warnings"] = warnings
    return response


def _build_get_lineage(
    *,
    scope: str,
    anchor_artifact_id: str,
    related_ids: list[str],
    related_set_hash: str | None,
) -> dict[str, Any]:
    """Build lineage payload for get responses."""
    lineage: dict[str, Any] = {
        "scope": scope,
        "anchor_artifact_id": anchor_artifact_id,
        "artifact_count": len(related_ids),
        "artifact_ids": related_ids,
    }
    if scope == "all_related":
        lineage["related_set_hash"] = related_set_hash
    return lineage


def _build_get_cursor_extra(
    *,
    query_state: _GetQueryState,
    related_set_hash: str | None,
    anchor_row: dict[str, Any] | None,
) -> dict[str, Any]:
    """Build cursor extra bindings for paginated get responses."""
    extra: dict[str, Any] = {
        "target": query_state.target,
        "normalized_jsonpath": query_state.normalized_jsonpath,
        "scope": query_state.scope,
    }
    if query_state.scope == "all_related":
        extra["anchor_artifact_id"] = query_state.anchor_artifact_id
        extra["related_set_hash"] = related_set_hash
        return extra
    generation = anchor_row.get("generation") if anchor_row else None
    if isinstance(generation, int):
        extra["artifact_generation"] = generation
    return extra


def _collect_get_envelope_items(
    *,
    runtime: ArtifactGetRuntime,
    query_state: _GetQueryState,
    artifact_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """Collect envelope/jsonpath-derived items for response budgeting."""
    merged_items: list[dict[str, Any]] = []
    for artifact_row in artifact_rows:
        artifact_id = artifact_row.get("artifact_id")
        if not isinstance(artifact_id, str):
            continue
        envelope, envelope_err = _reconstruct_artifact_envelope(
            artifact_row,
            blobs_payload_dir=runtime.blobs_payload_dir,
        )
        if envelope_err is not None:
            return [], envelope_err
        if envelope is None:
            return [], gateway_error("INTERNAL", "missing envelope")

        if query_state.jsonpath is None:
            merged_items.append(
                {
                    "_locator": {"artifact_id": artifact_id},
                    "value": envelope,
                }
            )
            continue

        json_target = extract_json_target(
            envelope,
            artifact_row.get("mapped_part_index"),
        )
        values = evaluate_jsonpath(
            json_target,
            query_state.normalized_jsonpath,
            max_length=runtime.max_jsonpath_length,
            max_segments=runtime.max_path_segments,
            max_wildcard_expansion_total=runtime.max_wildcard_expansion_total,
        )
        for index, value in enumerate(values):
            merged_items.append(
                {
                    "_locator": {
                        "artifact_id": artifact_id,
                        "jsonpath": query_state.normalized_jsonpath,
                        "index": index,
                    },
                    "value": value,
                }
            )
    return merged_items, None


def _build_mapped_root_entries(
    *,
    connection: Any,
    artifact_rows: list[dict[str, Any]],
    warnings: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Collect mapped root entries across artifact rows."""
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
    return root_entries


def _build_mapped_get_response_from_rows(
    *,
    runtime: ArtifactGetRuntime,
    query_state: _GetQueryState,
    artifact_rows: list[dict[str, Any]],
    related_ids: list[str],
    related_set_hash: str | None,
    warnings: list[dict[str, Any]],
    connection: Any,
) -> dict[str, Any]:
    """Build mapped-target get response from loaded artifact rows."""
    root_entries = _build_mapped_root_entries(
        connection=connection,
        artifact_rows=artifact_rows,
        warnings=warnings,
    )
    return _build_mapped_get_response(
        runtime=runtime,
        scope=query_state.scope,
        anchor_artifact_id=query_state.anchor_artifact_id,
        related_ids=related_ids,
        related_set_hash=related_set_hash,
        root_entries=root_entries,
        warnings=warnings,
    )


def _reconstruct_artifact_envelope(
    artifact_row: dict[str, Any],
    *,
    blobs_payload_dir: Any,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Get envelope dict from row (inline JSONB or payload file)."""
    envelope_value = artifact_row.get("envelope")
    payload_fs_path = artifact_row.get("payload_fs_path")
    if isinstance(envelope_value, dict) and "content" in envelope_value:
        return envelope_value, None
    if not isinstance(payload_fs_path, str) or not payload_fs_path:
        return None, gateway_error(
            "INTERNAL",
            "missing payload file path for artifact",
        )
    try:
        envelope = reconstruct_envelope(
            payload_fs_path=payload_fs_path,
            blobs_payload_dir=blobs_payload_dir,
            encoding=str(
                artifact_row.get("envelope_canonical_encoding", "none")
            ),
            expected_hash=str(artifact_row.get("payload_hash_full", "")),
        )
    except ValueError as exc:
        return None, gateway_error(
            "INTERNAL",
            f"envelope reconstruction failed: {exc}",
        )
    return envelope, None


def _build_get_envelope_response(
    *,
    runtime: ArtifactGetRuntime,
    query_state: _GetQueryState,
    artifact_rows: list[dict[str, Any]],
    anchor_row: dict[str, Any] | None,
    related_ids: list[str],
    related_set_hash: str | None,
    warnings: list[dict[str, Any]],
    limit_value: Any,
) -> dict[str, Any]:
    """Build envelope-target get response, including cursor handling."""
    merged_items, collect_err = _collect_get_envelope_items(
        runtime=runtime,
        query_state=query_state,
        artifact_rows=artifact_rows,
    )
    if collect_err is not None:
        return collect_err

    max_items = runtime.bounded_limit(limit_value)
    selected, truncated, omitted, used_bytes = apply_output_budgets(
        merged_items[query_state.offset :],
        max_items=max_items,
        max_bytes_out=runtime.max_bytes_out,
    )
    next_cursor: str | None = None
    if truncated:
        extra = _build_get_cursor_extra(
            query_state=query_state,
            related_set_hash=related_set_hash,
            anchor_row=anchor_row,
        )
        next_cursor = runtime.issue_cursor(
            tool="artifact",
            artifact_id=query_state.anchor_artifact_id,
            position_state={
                "offset": query_state.offset + len(selected),
            },
            extra=extra,
        )

    lineage = _build_get_lineage(
        scope=query_state.scope,
        anchor_artifact_id=query_state.anchor_artifact_id,
        related_ids=related_ids,
        related_set_hash=related_set_hash,
    )
    response = {
        "artifact_id": query_state.anchor_artifact_id,
        "scope": query_state.scope,
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


def _touch_retrieval_artifacts(
    runtime: ArtifactGetRuntime,
    connection: Any,
    *,
    session_id: str,
    artifact_ids: list[str],
) -> None:
    """Touch retrieval timestamp for artifact ids and commit when needed."""
    touched = False
    for artifact_id in artifact_ids:
        touched = (
            runtime.safe_touch_for_retrieval(
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


def execute_artifact_get(
    runtime: ArtifactGetRuntime,
    *,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Run artifact get using runtime hooks provided by an adapter."""
    err = validate_get_args(arguments)
    if err is not None:
        return gateway_error(str(err["code"]), str(err["message"]))
    if runtime.db_pool is None:
        return runtime.not_implemented("artifact")
    query_state, state_err = _resolve_get_query_state(
        runtime=runtime,
        arguments=arguments,
    )
    if state_err is not None:
        return state_err
    if query_state is None:
        return gateway_error("INTERNAL", "query state unavailable")

    related_ids: list[str] = []
    related_set_hash: str | None = None
    artifact_rows: list[dict[str, Any]] = []
    anchor_row: dict[str, Any] | None = None
    warnings: list[dict[str, Any]] = []

    with runtime.db_pool.connection() as connection:
        related = _resolve_get_related_artifacts(
            runtime=runtime,
            connection=connection,
            session_id=query_state.session_id,
            anchor_artifact_id=query_state.anchor_artifact_id,
            scope=query_state.scope,
        )
        if isinstance(related, dict):
            return related
        related_ids = related.related_ids
        related_set_hash = related.related_set_hash

        (
            artifact_rows,
            anchor_row,
            warnings,
            row_err,
        ) = _load_get_artifact_rows(
            connection=connection,
            related_ids=related_ids,
            anchor_artifact_id=query_state.anchor_artifact_id,
            scope=query_state.scope,
            target=query_state.target,
        )
        if row_err is not None:
            return row_err
        if anchor_row is None:
            return gateway_error("NOT_FOUND", "artifact not found")

        cursor_err = _assert_get_cursor_bindings(
            runtime=runtime,
            cursor_payload=query_state.cursor_payload,
            target=query_state.target,
            normalized_jsonpath=query_state.normalized_jsonpath,
            scope=query_state.scope,
            anchor_artifact_id=query_state.anchor_artifact_id,
            related_set_hash=related_set_hash,
            anchor_row=anchor_row,
        )
        if cursor_err is not None:
            return cursor_err

        _touch_retrieval_artifacts(
            runtime,
            connection,
            session_id=query_state.session_id,
            artifact_ids=related_ids,
        )

        if query_state.target == "mapped":
            return _build_mapped_get_response_from_rows(
                runtime=runtime,
                query_state=query_state,
                artifact_rows=artifact_rows,
                related_ids=related_ids,
                related_set_hash=related_set_hash,
                warnings=warnings,
                connection=connection,
            )

    return _build_get_envelope_response(
        runtime=runtime,
        query_state=query_state,
        artifact_rows=artifact_rows,
        anchor_row=anchor_row,
        related_ids=related_ids,
        related_set_hash=related_set_hash,
        warnings=warnings,
        limit_value=arguments.get("limit"),
    )
