"""Re-export handler functions for all MCP gateway tools."""

from sidepouch_mcp.mcp.handlers.artifact_chain_pages import (
    handle_artifact_chain_pages,
)
from sidepouch_mcp.mcp.handlers.artifact_describe import (
    handle_artifact_describe,
)
from sidepouch_mcp.mcp.handlers.artifact_find import handle_artifact_find
from sidepouch_mcp.mcp.handlers.artifact_get import handle_artifact_get
from sidepouch_mcp.mcp.handlers.artifact_search import (
    handle_artifact_search,
)
from sidepouch_mcp.mcp.handlers.artifact_select import (
    handle_artifact_select,
)
from sidepouch_mcp.mcp.handlers.mirrored_tool import handle_mirrored_tool
from sidepouch_mcp.mcp.handlers.status import handle_status

__all__ = [
    "handle_artifact_chain_pages",
    "handle_artifact_describe",
    "handle_artifact_find",
    "handle_artifact_get",
    "handle_artifact_search",
    "handle_artifact_select",
    "handle_mirrored_tool",
    "handle_status",
]
