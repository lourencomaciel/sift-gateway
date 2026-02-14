"""Handle ``artifact(action="next_page")`` for LLM-driven pagination.

Fetch the next page of a paginated upstream response by reading
the pagination state stored in a previous artifact's envelope
metadata and replaying the upstream tool call with updated
pagination parameters.  Exports ``handle_artifact_next_page``.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from sift_mcp.constants import WORKSPACE_ID
from sift_mcp.envelope.responses import gateway_error
from sift_mcp.mcp.handlers.common import row_to_dict
from sift_mcp.pagination.extract import PaginationState
from sift_mcp.storage.payload_store import reconstruct_envelope

if TYPE_CHECKING:
    from sift_mcp.mcp.server import GatewayServer

_PAGINATION_COLUMNS = [
    "artifact_id",
    "deleted_at",
    "payload_hash_full",
    "envelope",
    "envelope_canonical_encoding",
    "envelope_canonical_bytes",
]

FETCH_ENVELOPE_META_SQL = """
SELECT a.artifact_id, a.deleted_at, a.payload_hash_full,
       pb.envelope, pb.envelope_canonical_encoding,
       pb.envelope_canonical_bytes
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

    try:
        return PaginationState.from_dict(pagination_data)
    except (TypeError, ValueError, KeyError):
        return None


def _extract_envelope_dict(row: dict[str, Any]) -> dict[str, Any] | None:
    """Load an envelope dict from JSONB or canonical bytes.

    Args:
        row: Database row containing envelope storage fields.

    Returns:
        Envelope dict, or ``None`` when loading fails.
    """
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

    canonical_bytes_raw = row.get("envelope_canonical_bytes")
    if canonical_bytes_raw is None:
        return None
    try:
        return reconstruct_envelope(
            compressed_bytes=bytes(canonical_bytes_raw),
            encoding=str(row.get("envelope_canonical_encoding", "none")),
            expected_hash=str(row.get("payload_hash_full", "")),
        )
    except ValueError:
        return None


async def handle_artifact_next_page(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Handle the ``artifact(action="next_page")`` tool call.

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
    from sift_mcp.mcp.handlers.mirrored_tool import (
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
    if row.get("deleted_at") is not None:
        return gateway_error("GONE", "artifact has been deleted")

    envelope_dict = _extract_envelope_dict(row)
    state = _extract_pagination_state(envelope_dict)
    if state is None:
        return gateway_error(
            "INVALID_ARGUMENT",
            "artifact has no upstream pagination state. "
            "next_page fetches additional upstream pages. "
            "To continue an artifact query, use "
            'artifact(action="query", query_kind=..., artifact_id=..., '
            "cursor=...) instead.",
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
    next_gateway_context = dict(raw_ctx)
    # Force a fresh persisted page so chain_pages for this parent remains
    # complete even when the same request_key exists in prior chains.
    next_gateway_context["allow_reuse"] = False
    next_args["_gateway_context"] = next_gateway_context
    next_args["_gateway_parent_artifact_id"] = artifact_id
    next_args["_gateway_chain_seq"] = state.page_number + 1

    # Phase 4: Forward to the mirrored tool handler.
    return await handle_mirrored_tool(ctx, mirrored, next_args)
