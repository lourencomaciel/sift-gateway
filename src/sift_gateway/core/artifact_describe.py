"""Protocol-agnostic artifact describe execution service."""

from __future__ import annotations

import json
from typing import Any

from sift_gateway.constants import WORKSPACE_ID
from sift_gateway.core.artifact_next_page import _extract_pagination_state
from sift_gateway.core.rows import row_to_dict, rows_to_dicts
from sift_gateway.core.runtime import ArtifactGetRuntime
from sift_gateway.core.schema_payload import build_schema_payload
from sift_gateway.envelope.responses import gateway_error
from sift_gateway.pagination.contract import (
    PAGINATION_WARNING_INCOMPLETE_RESULT_SET,
    RETRIEVAL_STATUS_PARTIAL,
    UPSTREAM_PARTIAL_REASON_MORE_PAGES_AVAILABLE,
    build_upstream_pagination_meta,
)
from sift_gateway.schema_compact import SCHEMA_LEGEND, compact_schema_payload
from sift_gateway.storage.payload_store import reconstruct_envelope
from sift_gateway.tools.artifact_describe import (
    FETCH_DESCRIBE_SQL,
    FETCH_ROOTS_SQL,
    FETCH_SCHEMA_ROOTS_SQL,
    validate_describe_args,
)
from sift_gateway.tools.artifact_schema import FETCH_SCHEMA_FIELDS_SQL

_DESCRIBE_COLUMNS = [
    "artifact_id",
    "map_kind",
    "map_status",
    "mapper_version",
    "map_budget_fingerprint",
    "map_backend_id",
    "prng_version",
    "mapped_part_index",
    "deleted_at",
    "generation",
]

_ROOT_COLUMNS = [
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

_SCHEMA_FIELD_COLUMNS = [
    "field_path",
    "types",
    "nullable",
    "required",
    "observed_count",
    "example_value",
    "distinct_values",
    "cardinality",
]

_DESCRIBE_ENVELOPE_COLUMNS = [
    "artifact_id",
    "payload_hash_full",
    "envelope",
    "envelope_canonical_encoding",
    "payload_fs_path",
]

_FETCH_DESCRIBE_ENVELOPE_SQL = """
SELECT a.artifact_id, a.payload_hash_full,
       pb.envelope, pb.envelope_canonical_encoding,
       pb.payload_fs_path
FROM artifacts a
JOIN payload_blobs pb
  ON pb.workspace_id = a.workspace_id
 AND pb.payload_hash_full = a.payload_hash_full
WHERE a.workspace_id = %s
  AND a.artifact_id = %s
"""


def _resolve_describe_scope(
    arguments: dict[str, Any],
) -> tuple[str | None, dict[str, Any] | None]:
    """Normalize and validate describe scope."""
    raw_scope = arguments.get("scope", "all_related")
    scope = str(raw_scope) if isinstance(raw_scope, str) else "all_related"
    if scope not in {"all_related", "single"}:
        return None, gateway_error(
            "INVALID_ARGUMENT",
            "scope must be one of: all_related, single",
        )
    return scope, None


def _load_schemas_for_artifact(
    *,
    connection: Any,
    artifact_id: str,
) -> list[dict[str, Any]]:
    """Load schema payloads for an artifact."""
    schema_roots = rows_to_dicts(
        connection.execute(
            FETCH_SCHEMA_ROOTS_SQL,
            (WORKSPACE_ID, artifact_id),
        ).fetchall(),
        _SCHEMA_ROOT_COLUMNS,
    )
    schemas_for_artifact: list[dict[str, Any]] = []
    for schema_root in schema_roots:
        root_key = schema_root.get("root_key")
        if not isinstance(root_key, str):
            continue
        field_rows = rows_to_dicts(
            connection.execute(
                FETCH_SCHEMA_FIELDS_SQL,
                (WORKSPACE_ID, artifact_id, root_key),
            ).fetchall(),
            _SCHEMA_FIELD_COLUMNS,
        )
        schemas_for_artifact.append(
            build_schema_payload(
                schema_root=schema_root,
                field_rows=field_rows,
                include_null_example_value=True,
            )
        )
    return schemas_for_artifact


def _resolve_describe_related_rows(
    *,
    runtime: ArtifactGetRuntime,
    connection: Any,
    session_id: str,
    anchor_artifact_id: str,
    scope: str,
) -> tuple[list[dict[str, Any]], list[str], dict[str, Any] | None]:
    """Resolve related artifacts and normalize related_ids."""
    if scope == "single":
        if not runtime.artifact_visible(
            connection,
            session_id=session_id,
            artifact_id=anchor_artifact_id,
        ):
            return [], [], gateway_error("NOT_FOUND", "artifact not found")
        related_rows = [
            {
                "artifact_id": anchor_artifact_id,
                "generation": None,
            }
        ]
    else:
        related_rows = runtime.resolve_related_artifacts(
            connection,
            session_id=session_id,
            anchor_artifact_id=anchor_artifact_id,
        )
        if not related_rows:
            return [], [], gateway_error("NOT_FOUND", "artifact not found")
        if len(related_rows) > runtime.related_query_max_artifacts:
            return (
                [],
                [],
                gateway_error(
                    "RESOURCE_EXHAUSTED",
                    "lineage query exceeds related artifact limit",
                    details={
                        "artifact_count": len(related_rows),
                        "max_artifacts": runtime.related_query_max_artifacts,
                    },
                ),
            )
    related_ids = [
        artifact_id
        for row in related_rows
        if isinstance((artifact_id := row.get("artifact_id")), str)
    ]
    if not related_ids:
        return [], [], gateway_error("NOT_FOUND", "artifact not found")
    return related_rows, related_ids, None


def _load_describe_artifact_rows(
    *,
    connection: Any,
    related_ids: list[str],
) -> dict[str, dict[str, Any]]:
    """Load describe metadata rows keyed by artifact_id."""
    artifact_rows: dict[str, dict[str, Any]] = {}
    for artifact_id in related_ids:
        row = row_to_dict(
            connection.execute(
                FETCH_DESCRIBE_SQL,
                (WORKSPACE_ID, artifact_id),
            ).fetchone(),
            _DESCRIBE_COLUMNS,
        )
        if row is not None:
            artifact_rows[artifact_id] = row
    return artifact_rows


def _schema_completeness(schema: dict[str, Any]) -> Any:
    """Extract schema completeness if coverage payload exists."""
    coverage = schema.get("coverage")
    if isinstance(coverage, dict):
        return coverage.get("completeness")
    return None


def _collect_describe_payload(
    *,
    connection: Any,
    related_ids: list[str],
    anchor_artifact_id: str,
    artifact_rows: dict[str, dict[str, Any]],
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    dict[str, int],
    list[dict[str, Any]],
]:
    """Build root entries, artifact summaries, status counts, and anchor schemas."""
    root_entries: list[dict[str, Any]] = []
    artifact_summaries: list[dict[str, Any]] = []
    map_status_counts: dict[str, int] = {}
    anchor_schemas: list[dict[str, Any]] = []
    for artifact_id in related_ids:
        artifact_row = artifact_rows.get(artifact_id)
        if artifact_row is None:
            continue
        map_status = str(artifact_row.get("map_status", "unknown"))
        map_status_counts[map_status] = map_status_counts.get(map_status, 0) + 1
        artifact_summaries.append(
            {
                "artifact_id": artifact_id,
                "map_kind": artifact_row.get("map_kind"),
                "map_status": artifact_row.get("map_status"),
                "generation": artifact_row.get("generation"),
                "mapped_part_index": artifact_row.get("mapped_part_index"),
            }
        )
        roots = rows_to_dicts(
            connection.execute(
                FETCH_ROOTS_SQL,
                (WORKSPACE_ID, artifact_id),
            ).fetchall(),
            _ROOT_COLUMNS,
        )
        roots_by_path = {
            rp: root
            for root in roots
            if isinstance((rp := root.get("root_path")), str)
        }
        schemas_for_artifact = _load_schemas_for_artifact(
            connection=connection,
            artifact_id=artifact_id,
        )
        if artifact_id == anchor_artifact_id:
            anchor_schemas = list(schemas_for_artifact)
        for schema in schemas_for_artifact:
            schema_root_path = schema.get("root_path")
            if not isinstance(schema_root_path, str):
                continue
            root = roots_by_path.get(schema_root_path, {})
            root_entries.append(
                {
                    "artifact_id": artifact_id,
                    "root_path": schema_root_path,
                    "root_shape": root.get("root_shape"),
                    "count_estimate": root.get("count_estimate"),
                    "schema_hash": schema.get("schema_hash"),
                    "schema_mode": schema.get("mode"),
                    "schema_completeness": _schema_completeness(schema),
                    "schema": schema,
                }
            )
    return root_entries, artifact_summaries, map_status_counts, anchor_schemas


def _validate_describe_anchor_row(
    *,
    artifact_rows: dict[str, dict[str, Any]],
    anchor_artifact_id: str,
) -> dict[str, Any] | None:
    """Validate anchor artifact visibility and deletion state."""
    anchor_row = artifact_rows.get(anchor_artifact_id)
    if anchor_row is None:
        return gateway_error("NOT_FOUND", "artifact not found")
    if anchor_row.get("deleted_at") is not None:
        return gateway_error("GONE", "artifact has been deleted")
    return None


def _compact_root_schemas(
    runtime: ArtifactGetRuntime,
    root_entries: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], bool]:
    """Compact per-root schema payloads in place."""
    roots = runtime.build_lineage_root_catalog(root_entries)
    roots_with_schema = [
        root for root in roots if isinstance(root.get("schema"), dict)
    ]
    if not roots_with_schema:
        return roots, False
    compact_root_schemas = compact_schema_payload(
        [root["schema"] for root in roots_with_schema]
    )
    for root, compact_schema in zip(
        roots_with_schema, compact_root_schemas, strict=True
    ):
        root["schema"] = compact_schema
    return roots, True


def _build_describe_response(
    *,
    runtime: ArtifactGetRuntime,
    scope: str,
    anchor_artifact_id: str,
    related_ids: list[str],
    related_rows: list[dict[str, Any]],
    map_status_counts: dict[str, int],
    artifact_summaries: list[dict[str, Any]],
    roots: list[dict[str, Any]],
    has_root_schema: bool,
    compact_anchor_schemas: list[dict[str, Any]],
    pagination: dict[str, Any] | None,
) -> dict[str, Any]:
    """Build final describe response payload."""
    lineage: dict[str, Any] = {
        "scope": scope,
        "anchor_artifact_id": anchor_artifact_id,
        "artifact_count": len(related_ids),
        "artifact_ids": related_ids,
        "map_status_counts": map_status_counts,
    }
    if scope == "all_related":
        lineage["related_set_hash"] = runtime.compute_related_set_hash(
            related_rows
        )
    response: dict[str, Any] = {
        "artifact_id": anchor_artifact_id,
        "scope": scope,
        "lineage": lineage,
        "artifacts": artifact_summaries,
        "roots": roots,
    }
    if has_root_schema or compact_anchor_schemas:
        response["schema_legend"] = SCHEMA_LEGEND
    if scope == "single" and compact_anchor_schemas:
        response["schemas"] = compact_anchor_schemas
    if isinstance(pagination, dict):
        response["pagination"] = pagination
    return response


def _describe_envelope_dict(
    *,
    runtime: ArtifactGetRuntime,
    row: dict[str, Any],
) -> dict[str, Any] | None:
    """Load envelope dict for describe metadata lookups."""
    envelope_raw = row.get("envelope")
    if isinstance(envelope_raw, dict):
        return envelope_raw
    if isinstance(envelope_raw, str):
        try:
            decoded = json.loads(envelope_raw)
        except (json.JSONDecodeError, ValueError):
            return None
        if isinstance(decoded, dict):
            return decoded
        return None
    payload_fs_path = row.get("payload_fs_path")
    if not isinstance(payload_fs_path, str) or not payload_fs_path:
        return None
    try:
        return reconstruct_envelope(
            payload_fs_path=payload_fs_path,
            blobs_payload_dir=runtime.blobs_payload_dir,
            encoding=str(row.get("envelope_canonical_encoding", "none")),
            expected_hash=str(row.get("payload_hash_full", "")),
        )
    except ValueError:
        return None


def _describe_pagination_meta(
    *,
    runtime: ArtifactGetRuntime,
    connection: Any,
    anchor_artifact_id: str,
) -> dict[str, Any] | None:
    """Build upstream pagination metadata for describe responses."""
    envelope_row = row_to_dict(
        connection.execute(
            _FETCH_DESCRIBE_ENVELOPE_SQL,
            (WORKSPACE_ID, anchor_artifact_id),
        ).fetchone(),
        _DESCRIBE_ENVELOPE_COLUMNS,
    )
    if envelope_row is None:
        return None
    envelope = _describe_envelope_dict(runtime=runtime, row=envelope_row)
    if envelope is None:
        return None
    state = _extract_pagination_state(envelope)
    if state is None or not state.next_params:
        return None
    has_next_page = not (
        state.upstream_prefix == "cli" and state.tool_name == "run"
    )
    meta = build_upstream_pagination_meta(
        artifact_id=anchor_artifact_id,
        page_number=state.page_number,
        retrieval_status=RETRIEVAL_STATUS_PARTIAL,
        has_more=True,
        partial_reason=UPSTREAM_PARTIAL_REASON_MORE_PAGES_AVAILABLE,
        warning=PAGINATION_WARNING_INCOMPLETE_RESULT_SET,
        has_next_page=has_next_page,
        next_params=state.next_params,
        original_args=state.original_args,
    )
    if not has_next_page:
        # Preserve discovered continuation parameters for interface layers
        # that support manual continuation without artifact.next_page replay.
        meta["next_params"] = state.next_params
        if len(state.next_params) == 1:
            cursor_param, cursor_value = next(
                iter(state.next_params.items())
            )
            if isinstance(cursor_param, str) and cursor_param:
                meta["next_cursor_param"] = cursor_param
                meta["next_cursor"] = cursor_value
    return meta


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


def execute_artifact_describe(
    runtime: ArtifactGetRuntime,
    *,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Run artifact describe using runtime hooks provided by an adapter."""
    err = validate_describe_args(arguments)
    if err is not None:
        return gateway_error(str(err["code"]), str(err["message"]))
    if runtime.db_pool is None:
        return runtime.not_implemented("artifact.describe")

    scope, scope_err = _resolve_describe_scope(arguments)
    if scope_err is not None:
        return scope_err
    if scope is None:
        return gateway_error("INTERNAL", "scope resolution failed")

    raw_ctx = arguments.get("_gateway_context")
    session_id = str(raw_ctx["session_id"]) if isinstance(raw_ctx, dict) else ""
    anchor_artifact_id = str(arguments["artifact_id"])

    with runtime.db_pool.connection() as connection:
        related_rows, related_ids, related_err = _resolve_describe_related_rows(
            runtime=runtime,
            connection=connection,
            session_id=session_id,
            anchor_artifact_id=anchor_artifact_id,
            scope=scope,
        )
        if related_err is not None:
            return related_err
        artifact_rows = _load_describe_artifact_rows(
            connection=connection,
            related_ids=related_ids,
        )

        anchor_err = _validate_describe_anchor_row(
            artifact_rows=artifact_rows,
            anchor_artifact_id=anchor_artifact_id,
        )
        if anchor_err is not None:
            return anchor_err

        (
            root_entries,
            artifact_summaries,
            map_status_counts,
            anchor_schemas,
        ) = _collect_describe_payload(
            connection=connection,
            related_ids=related_ids,
            anchor_artifact_id=anchor_artifact_id,
            artifact_rows=artifact_rows,
        )

        _touch_retrieval_artifacts(
            runtime,
            connection,
            session_id=session_id,
            artifact_ids=related_ids,
        )
        pagination_meta = _describe_pagination_meta(
            runtime=runtime,
            connection=connection,
            anchor_artifact_id=anchor_artifact_id,
        )

    roots, has_root_schema = _compact_root_schemas(runtime, root_entries)
    compact_anchor_schemas = compact_schema_payload(anchor_schemas)
    return _build_describe_response(
        runtime=runtime,
        scope=scope,
        anchor_artifact_id=anchor_artifact_id,
        related_ids=related_ids,
        related_rows=related_rows,
        map_status_counts=map_status_counts,
        artifact_summaries=artifact_summaries,
        roots=roots,
        has_root_schema=has_root_schema,
        compact_anchor_schemas=compact_anchor_schemas,
        pagination=pagination_meta,
    )
