"""Legacy describe handler for ``artifact(action="query", query_kind="describe")``."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sift_mcp.constants import WORKSPACE_ID
from sift_mcp.envelope.responses import gateway_error
from sift_mcp.mcp.handlers.common import (
    ROOT_COLUMNS,
    row_to_dict,
    rows_to_dicts,
    touch_retrieval_artifacts,
)
from sift_mcp.mcp.handlers.schema_payload import build_schema_payload
from sift_mcp.mcp.lineage import (
    build_lineage_root_catalog,
    compute_related_set_hash,
    resolve_related_artifacts,
)
from sift_mcp.schema_compact import SCHEMA_LEGEND, compact_schema_payload

if TYPE_CHECKING:
    from sift_mcp.mcp.server import GatewayServer

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
    fetch_schema_roots_sql: str,
    fetch_schema_fields_sql: str,
) -> list[dict[str, Any]]:
    """Load schema payloads for an artifact."""
    schema_roots = rows_to_dicts(
        connection.execute(
            fetch_schema_roots_sql,
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
                fetch_schema_fields_sql,
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
    ctx: GatewayServer,
    connection: Any,
    session_id: str,
    anchor_artifact_id: str,
    scope: str,
) -> tuple[list[dict[str, Any]], list[str], dict[str, Any] | None]:
    """Resolve related artifacts and normalize related_ids."""
    if scope == "single":
        if not ctx._artifact_visible(
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
        related_rows = resolve_related_artifacts(
            connection,
            session_id=session_id,
            anchor_artifact_id=anchor_artifact_id,
        )
        if not related_rows:
            return [], [], gateway_error("NOT_FOUND", "artifact not found")
        if len(related_rows) > ctx.config.related_query_max_artifacts:
            return [], [], gateway_error(
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
    if not related_ids:
        return [], [], gateway_error("NOT_FOUND", "artifact not found")
    return related_rows, related_ids, None


def _load_describe_artifact_rows(
    *,
    connection: Any,
    related_ids: list[str],
    fetch_describe_sql: str,
) -> dict[str, dict[str, Any]]:
    """Load describe metadata rows keyed by artifact_id."""
    artifact_rows: dict[str, dict[str, Any]] = {}
    for artifact_id in related_ids:
        row = row_to_dict(
            connection.execute(
                fetch_describe_sql,
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
    fetch_roots_sql: str,
    fetch_schema_roots_sql: str,
    fetch_schema_fields_sql: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int], list[dict[str, Any]]]:
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
                fetch_roots_sql,
                (WORKSPACE_ID, artifact_id),
            ).fetchall(),
            ROOT_COLUMNS,
        )
        roots_by_path = {
            rp: root
            for root in roots
            if isinstance((rp := root.get("root_path")), str)
        }
        schemas_for_artifact = _load_schemas_for_artifact(
            connection=connection,
            artifact_id=artifact_id,
            fetch_schema_roots_sql=fetch_schema_roots_sql,
            fetch_schema_fields_sql=fetch_schema_fields_sql,
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
    root_entries: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], bool]:
    """Compact per-root schema payloads in place."""
    roots = build_lineage_root_catalog(root_entries)
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
    scope: str,
    anchor_artifact_id: str,
    related_ids: list[str],
    related_rows: list[dict[str, Any]],
    map_status_counts: dict[str, int],
    artifact_summaries: list[dict[str, Any]],
    roots: list[dict[str, Any]],
    has_root_schema: bool,
    compact_anchor_schemas: list[dict[str, Any]],
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
        lineage["related_set_hash"] = compute_related_set_hash(related_rows)
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
    return response


async def handle_artifact_describe(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Handle describe-mode artifact queries."""
    from sift_mcp.tools.artifact_describe import (
        FETCH_DESCRIBE_SQL,
        FETCH_ROOTS_SQL,
        FETCH_SCHEMA_ROOTS_SQL,
        validate_describe_args,
    )
    from sift_mcp.tools.artifact_schema import FETCH_SCHEMA_FIELDS_SQL

    err = validate_describe_args(arguments)
    if err is not None:
        return gateway_error(str(err["code"]), str(err["message"]))
    if ctx.db_pool is None:
        return ctx._not_implemented("artifact.describe")

    scope, scope_err = _resolve_describe_scope(arguments)
    if scope_err is not None:
        return scope_err
    if scope is None:
        return gateway_error("INTERNAL", "scope resolution failed")

    raw_ctx = arguments.get("_gateway_context")
    session_id = str(raw_ctx["session_id"]) if isinstance(raw_ctx, dict) else ""
    anchor_artifact_id = str(arguments["artifact_id"])

    with ctx.db_pool.connection() as connection:
        related_rows, related_ids, related_err = _resolve_describe_related_rows(
            ctx=ctx,
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
            fetch_describe_sql=FETCH_DESCRIBE_SQL,
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
            fetch_roots_sql=FETCH_ROOTS_SQL,
            fetch_schema_roots_sql=FETCH_SCHEMA_ROOTS_SQL,
            fetch_schema_fields_sql=FETCH_SCHEMA_FIELDS_SQL,
        )

        touch_retrieval_artifacts(
            ctx,
            connection,
            session_id=session_id,
            artifact_ids=related_ids,
        )

    roots, has_root_schema = _compact_root_schemas(root_entries)
    compact_anchor_schemas = compact_schema_payload(anchor_schemas)
    return _build_describe_response(
        scope=scope,
        anchor_artifact_id=anchor_artifact_id,
        related_ids=related_ids,
        related_rows=related_rows,
        map_status_counts=map_status_counts,
        artifact_summaries=artifact_summaries,
        roots=roots,
        has_root_schema=has_root_schema,
        compact_anchor_schemas=compact_anchor_schemas,
    )
