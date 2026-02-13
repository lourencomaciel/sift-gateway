"""Re-export handler functions for all MCP gateway tools."""

from sift_mcp.mcp.handlers.artifact_consolidated import (
    handle_artifact,
)
from sift_mcp.mcp.handlers.artifact_describe import (
    handle_artifact_describe,
)
from sift_mcp.mcp.handlers.artifact_get import handle_artifact_get
from sift_mcp.mcp.handlers.artifact_next_page import (
    handle_artifact_next_page,
)
from sift_mcp.mcp.handlers.artifact_search import (
    handle_artifact_search,
)
from sift_mcp.mcp.handlers.artifact_select import (
    handle_artifact_select,
)
from sift_mcp.mcp.handlers.mirrored_tool import handle_mirrored_tool
from sift_mcp.mcp.handlers.status import handle_status

__all__ = [
    "handle_artifact",
    "handle_artifact_describe",
    "handle_artifact_get",
    "handle_artifact_next_page",
    "handle_artifact_search",
    "handle_artifact_select",
    "handle_mirrored_tool",
    "handle_status",
]
