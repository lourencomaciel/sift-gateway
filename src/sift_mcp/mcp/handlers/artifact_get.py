"""Legacy get handler for ``artifact(action="query", query_kind="get")``."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sift_mcp.core.artifact_get import execute_artifact_get
from sift_mcp.mcp.adapters.artifact_query_runtime import (
    GatewayArtifactQueryRuntime,
)

if TYPE_CHECKING:
    from sift_mcp.mcp.server import GatewayServer


async def handle_artifact_get(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Handle get-mode artifact queries."""
    runtime = GatewayArtifactQueryRuntime(gateway=ctx)
    return execute_artifact_get(runtime, arguments=arguments)

