"""Shared protocol-agnostic services and contracts.

The ``core`` package contains execution logic designed to be consumed by
multiple interfaces (e.g., MCP handlers and CLI commands) via small adapter
layers.
"""

from sift_mcp.core.artifact_code import execute_artifact_code
from sift_mcp.core.artifact_describe import execute_artifact_describe
from sift_mcp.core.artifact_get import execute_artifact_get
from sift_mcp.core.artifact_next_page import execute_artifact_next_page
from sift_mcp.core.artifact_search import execute_artifact_search
from sift_mcp.core.artifact_select import execute_artifact_select
from sift_mcp.core.runtime import (
    ArtifactCodeRuntime,
    ArtifactGetRuntime,
    ArtifactNextPageRuntime,
    ArtifactSearchRuntime,
    ArtifactSelectRuntime,
)

__all__ = [
    "ArtifactCodeRuntime",
    "ArtifactGetRuntime",
    "ArtifactNextPageRuntime",
    "ArtifactSearchRuntime",
    "ArtifactSelectRuntime",
    "execute_artifact_code",
    "execute_artifact_describe",
    "execute_artifact_get",
    "execute_artifact_next_page",
    "execute_artifact_search",
    "execute_artifact_select",
]
