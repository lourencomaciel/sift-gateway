"""Legacy describe handler for ``artifact(action="query", query_kind="describe")``."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sift_mcp.constants import WORKSPACE_ID
from sift_mcp.envelope.responses import gateway_error
from sift_mcp.mcp.handlers.common import (
    ROOT_COLUMNS,
    row_to_dict,
    rows_to_dicts,
)
from sift_mcp.mcp.lineage import (
    build_lineage_root_catalog,
    compute_related_set_hash,
    resolve_related_artifacts,
)

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


async def handle_artifact_describe(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Handle describe-mode artifact queries."""
    from sift_mcp.tools.artifact_describe import (
        FETCH_DESCRIBE_SQL,
        FETCH_ROOTS_SQL,
        validate_describe_args,
    )

    err = validate_describe_args(arguments)
    if err is not None:
        return gateway_error(str(err["code"]), str(err["message"]))
    if ctx.db_pool is None:
        return ctx._not_implemented("artifact.describe")

    raw_ctx = arguments.get("_gateway_context")
    session_id = str(raw_ctx["session_id"]) if isinstance(raw_ctx, dict) else ""
    anchor_artifact_id = str(arguments["artifact_id"])
    raw_scope = arguments.get("scope", "all_related")
    scope = str(raw_scope) if isinstance(raw_scope, str) else "all_related"
    if scope not in {"all_related", "single"}:
        return gateway_error(
            "INVALID_ARGUMENT",
            "scope must be one of: all_related, single",
        )

    with ctx.db_pool.connection() as connection:
        related_rows: list[dict[str, Any]]
        if scope == "single":
            if not ctx._artifact_visible(
                connection,
                session_id=session_id,
                artifact_id=anchor_artifact_id,
            ):
                return gateway_error("NOT_FOUND", "artifact not found")
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
        if not related_ids:
            return gateway_error("NOT_FOUND", "artifact not found")

        artifact_rows: dict[str, dict[str, Any]] = {}
        for artifact_id in related_ids:
            row = row_to_dict(
                connection.execute(
                    FETCH_DESCRIBE_SQL,
                    (WORKSPACE_ID, artifact_id),
                ).fetchone(),
                _DESCRIBE_COLUMNS,
            )
            if row is None:
                continue
            artifact_rows[artifact_id] = row

        anchor_row = artifact_rows.get(anchor_artifact_id)
        if anchor_row is None:
            return gateway_error("NOT_FOUND", "artifact not found")
        if anchor_row.get("deleted_at") is not None:
            ctx._safe_touch_for_retrieval(
                connection,
                session_id=session_id,
                artifact_id=anchor_artifact_id,
            )
            commit = getattr(connection, "commit", None)
            if callable(commit):
                commit()
            return gateway_error("GONE", "artifact has been deleted")

        root_entries: list[dict[str, Any]] = []
        artifact_summaries: list[dict[str, Any]] = []
        map_status_counts: dict[str, int] = {}
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
                ROOT_COLUMNS,
            )
            for root in roots:
                root_entries.append(
                    {
                        "artifact_id": artifact_id,
                        "root_path": root.get("root_path"),
                        "root_shape": root.get("root_shape"),
                        "fields_top": root.get("fields_top"),
                        "count_estimate": root.get("count_estimate"),
                        "map_kind": artifact_row.get("map_kind"),
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

    roots = build_lineage_root_catalog(root_entries)
    lineage: dict[str, Any] = {
        "scope": scope,
        "anchor_artifact_id": anchor_artifact_id,
        "artifact_count": len(related_ids),
        "artifact_ids": related_ids,
        "map_status_counts": map_status_counts,
    }
    if scope == "all_related":
        lineage["related_set_hash"] = compute_related_set_hash(related_rows)
    return {
        "artifact_id": anchor_artifact_id,
        "scope": scope,
        "lineage": lineage,
        "artifacts": artifact_summaries,
        "roots": roots,
    }
