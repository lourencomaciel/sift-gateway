"""Legacy search handler for ``artifact(action="query", query_kind="search")``."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sift_mcp.core.artifact_search import execute_artifact_search
from sift_mcp.mcp.adapters.artifact_query_runtime import (
    GatewayArtifactQueryRuntime,
)

if TYPE_CHECKING:
    from sift_mcp.mcp.server import GatewayServer


async def handle_artifact_search(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Handle search-mode artifact queries.

    Args:
        ctx: Gateway server instance providing DB and cursor helpers.
        arguments: Tool arguments including session context, optional
            filters, ``order_by``, ``limit``, and ``cursor``.

    Returns:
        Paginated search response with artifact summaries, or a
        gateway error.
    """
    runtime = GatewayArtifactQueryRuntime(gateway=ctx)
    return execute_artifact_search(runtime, arguments=arguments)
