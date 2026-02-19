"""Legacy describe handler for ``artifact(action="query", query_kind="describe")``."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sift_mcp.core.artifact_describe import execute_artifact_describe
from sift_mcp.mcp.adapters.artifact_query_runtime import (
    GatewayArtifactQueryRuntime,
)

if TYPE_CHECKING:
    from sift_mcp.mcp.server import GatewayServer


async def handle_artifact_describe(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Handle describe-mode artifact queries."""
    runtime = GatewayArtifactQueryRuntime(gateway=ctx)
    return execute_artifact_describe(runtime, arguments=arguments)

