"""artifact.describe handler."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from mcp_artifact_gateway.constants import WORKSPACE_ID
from mcp_artifact_gateway.envelope.responses import gateway_error
from mcp_artifact_gateway.mcp.handlers.common import (
    ROOT_COLUMNS,
    row_to_dict,
    rows_to_dicts,
)

if TYPE_CHECKING:
    from mcp_artifact_gateway.mcp.server import GatewayServer

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
    from mcp_artifact_gateway.tools.artifact_describe import (
        FETCH_DESCRIBE_SQL,
        FETCH_ROOTS_SQL,
        build_describe_response,
        validate_describe_args,
    )

    err = validate_describe_args(arguments)
    if err is not None:
        return err
    if ctx.db_pool is None:
        return ctx._not_implemented("artifact.describe")

    raw_ctx = arguments.get("_gateway_context")
    session_id = str(raw_ctx["session_id"]) if isinstance(raw_ctx, dict) else ""
    artifact_id = str(arguments["artifact_id"])

    with ctx.db_pool.connection() as connection:
        if not ctx._artifact_visible(
            connection,
            session_id=session_id,
            artifact_id=artifact_id,
        ):
            return gateway_error("NOT_FOUND", "artifact not found")

        artifact_row = row_to_dict(
            connection.execute(
                FETCH_DESCRIBE_SQL,
                (WORKSPACE_ID, artifact_id),
            ).fetchone(),
            _DESCRIBE_COLUMNS,
        )
        if artifact_row is None:
            return gateway_error("NOT_FOUND", "artifact not found")

        ctx._safe_touch_for_retrieval(
            connection,
            session_id=session_id,
            artifact_id=artifact_id,
        )
        commit = getattr(connection, "commit", None)
        if callable(commit):
            commit()

        if artifact_row.get("deleted_at") is not None:
            return gateway_error("GONE", "artifact has been deleted")

        roots = rows_to_dicts(
            connection.execute(
                FETCH_ROOTS_SQL,
                (WORKSPACE_ID, artifact_id),
            ).fetchall(),
            ROOT_COLUMNS,
        )

    return build_describe_response(artifact_row, roots)
