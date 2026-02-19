"""Legacy adapter for ``artifact(action='query', query_kind='code')``."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sift_mcp.core.artifact_code import (
    _enrich_install_hint,
    _module_to_dist,
    execute_artifact_code,
)
from sift_mcp.mcp.adapters.artifact_query_runtime import (
    GatewayArtifactQueryRuntime,
)

if TYPE_CHECKING:
    from sift_mcp.mcp.server import GatewayServer


async def handle_artifact_code(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Handle code-mode artifact queries."""
    runtime = GatewayArtifactQueryRuntime(gateway=ctx)
    return execute_artifact_code(runtime, arguments=arguments)


__all__ = [
    "_enrich_install_hint",
    "_module_to_dist",
    "handle_artifact_code",
]
