"""Legacy select handler for ``artifact(action="query", query_kind="select")``."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sift_mcp.core.artifact_select import execute_artifact_select
from sift_mcp.mcp.adapters.artifact_query_runtime import (
    GatewayArtifactQueryRuntime,
)

if TYPE_CHECKING:
    from sift_mcp.mcp.server import GatewayServer


async def handle_artifact_select(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Handle select-mode artifact queries."""
    runtime = GatewayArtifactQueryRuntime(gateway=ctx)
    return execute_artifact_select(runtime, arguments=arguments)

