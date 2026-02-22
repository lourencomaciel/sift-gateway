"""Re-export handler functions for all MCP gateway tools."""

from sift_gateway.mcp.handlers.artifact_consolidated import (
    handle_artifact,
)
from sift_gateway.mcp.handlers.mirrored_tool import handle_mirrored_tool
from sift_gateway.mcp.handlers.status import handle_status

__all__ = [
    "handle_artifact",
    "handle_mirrored_tool",
    "handle_status",
]
