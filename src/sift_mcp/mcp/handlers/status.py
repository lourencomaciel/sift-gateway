"""gateway.status handler."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sift_mcp.envelope.responses import gateway_error
from sift_mcp.tools.status import (
    build_status_response_with_runtime,
    probe_db,
    probe_fs,
)

if TYPE_CHECKING:
    from sift_mcp.mcp.server import GatewayServer


async def handle_status(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Handle the ``gateway.status`` tool call.

    Args:
        ctx: Gateway server instance providing config and pools.
        arguments: Tool arguments (unused for status).

    Returns:
        Status response with DB, FS, upstream, and cursor health.
    """
    probe_raw = arguments.get("probe_upstreams", False)
    if not isinstance(probe_raw, bool):
        return gateway_error(
            "INVALID_ARGUMENT",
            "probe_upstreams must be a boolean when provided",
        )

    db_health = probe_db(ctx.db_pool)
    fs_health = probe_fs(ctx.config)
    return build_status_response_with_runtime(
        ctx.config,
        db_health=db_health,
        fs_health=fs_health,
        upstreams=await ctx._status_upstreams(probe_upstreams=probe_raw),
    )
