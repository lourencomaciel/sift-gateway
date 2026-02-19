"""Handler for ``artifact(action="next_page")`` for LLM-driven pagination."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sift_mcp.core.artifact_next_page import (
    _extract_pagination_state,
    execute_artifact_next_page,
)
from sift_mcp.mcp.adapters.artifact_query_runtime import (
    GatewayArtifactQueryRuntime,
)

if TYPE_CHECKING:
    from sift_mcp.mcp.server import GatewayServer


async def handle_artifact_next_page(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Handle the ``artifact(action="next_page")`` tool call."""
    runtime = GatewayArtifactQueryRuntime(gateway=ctx)
    return await execute_artifact_next_page(runtime, arguments=arguments)


__all__ = [
    "_extract_pagination_state",
    "handle_artifact_next_page",
]

