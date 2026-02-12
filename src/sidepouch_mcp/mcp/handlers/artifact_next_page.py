"""Handle ``artifact.next_page`` for LLM-driven pagination.

Fetch the next page of a paginated upstream response by reading
the pagination state stored in a previous artifact's envelope
metadata and replaying the upstream tool call with updated
pagination parameters.  Exports ``handle_artifact_next_page``.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from sidepouch_mcp.constants import WORKSPACE_ID
from sidepouch_mcp.envelope.responses import gateway_error
from sidepouch_mcp.mcp.handlers.common import row_to_dict
from sidepouch_mcp.pagination.extract import PaginationState

if TYPE_CHECKING:
    from sidepouch_mcp.mcp.server import GatewayServer

_PAGINATION_COLUMNS = [
    "artifact_id",
    "envelope",
]

FETCH_ENVELOPE_META_SQL = """
SELECT a.artifact_id, pb.envelope
FROM artifacts a
JOIN payload_blobs pb ON pb.workspace_id = a.workspace_id
    AND pb.payload_hash_full = a.payload_hash_full
WHERE a.workspace_id = %s AND a.artifact_id = %s
"""


def _extract_pagination_state(
    envelope_raw: Any,
) -> PaginationState | None:
    """Extract pagination state from a raw envelope value.

    Handles both JSON string and pre-parsed dict envelope
    representations.

    Args:
        envelope_raw: Raw envelope from the database, either
            a JSON string or a parsed dict.

    Returns:
        Reconstructed ``PaginationState``, or ``None`` when
        no pagination metadata is present.
    """
    if isinstance(envelope_raw, str):
        try:
            envelope_dict = json.loads(envelope_raw)
        except (json.JSONDecodeError, ValueError):
            return None
    elif isinstance(envelope_raw, dict):
        envelope_dict = envelope_raw
    else:
        return None

    meta = envelope_dict.get("meta")
    if not isinstance(meta, dict):
        return None

    pagination_data = meta.get("_gateway_pagination")
    if not isinstance(pagination_data, dict):
        return None

    return PaginationState.from_dict(pagination_data)


async def handle_artifact_next_page(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Handle the ``artifact.next_page`` tool call.

    Reads the pagination state from the referenced artifact's
    envelope, locates the correct mirrored upstream tool, and
    replays the tool call with updated pagination parameters.
    The new result is chained as a child artifact.

    Args:
        ctx: Gateway server instance.
        arguments: Tool arguments including ``artifact_id`` and
            ``_gateway_context``.

    Returns:
        A gateway tool result from the next-page upstream call,
        or a gateway error dict on failure.
    """
    from sidepouch_mcp.mcp.handlers.mirrored_tool import (
        handle_mirrored_tool,
    )

    raw_ctx = arguments.get("_gateway_context")
    if not isinstance(raw_ctx, dict) or not raw_ctx.get("session_id"):
        return gateway_error(
            "INVALID_ARGUMENT",
            "missing _gateway_context.session_id",
        )
    session_id = str(raw_ctx["session_id"])

    artifact_id = arguments.get("artifact_id")
    if not isinstance(artifact_id, str) or not artifact_id:
        return gateway_error(
            "INVALID_ARGUMENT",
            "missing artifact_id",
        )

    if ctx.db_pool is None:
        return gateway_error(
            "NOT_IMPLEMENTED",
            "artifact.next_page requires a database backend",
        )

    # Phase 1: Read the artifact's envelope to get pagination state.
    with ctx.db_pool.connection() as connection:
        if not ctx._artifact_visible(
            connection,
            session_id=session_id,
            artifact_id=artifact_id,
        ):
            return gateway_error("NOT_FOUND", "artifact not found")

        row = row_to_dict(
            connection.execute(
                FETCH_ENVELOPE_META_SQL,
                (WORKSPACE_ID, artifact_id),
            ).fetchone(),
            _PAGINATION_COLUMNS,
        )

    if row is None:
        return gateway_error("NOT_FOUND", "artifact envelope not found")

    state = _extract_pagination_state(row.get("envelope"))
    if state is None:
        return gateway_error(
            "INVALID_ARGUMENT",
            "artifact has no pagination state; cannot fetch next page",
        )

    # Phase 2: Locate the mirrored tool.
    qualified_name = f"{state.upstream_prefix}.{state.tool_name}"
    mirrored = ctx.mirrored_tools.get(qualified_name)
    if mirrored is None:
        return gateway_error(
            "NOT_FOUND",
            f"upstream tool {qualified_name} not found",
        )

    # Phase 3: Build next-page arguments.
    next_args: dict[str, Any] = {
        **state.original_args,
        **state.next_params,
    }
    next_args["_gateway_context"] = raw_ctx
    next_args["_gateway_parent_artifact_id"] = artifact_id
    next_args["_gateway_chain_seq"] = state.page_number + 1

    # Phase 4: Forward to the mirrored tool handler.
    return await handle_mirrored_tool(ctx, mirrored, next_args)
