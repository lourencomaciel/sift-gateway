"""Repository helpers for artifact lineage edge rows."""

from __future__ import annotations

from typing import Any

from sift_mcp.constants import WORKSPACE_ID

INSERT_LINEAGE_EDGE_SQL = """
INSERT INTO artifact_lineage_edges (
    workspace_id, child_artifact_id, parent_artifact_id, ord
) VALUES (
    %s, %s, %s, %s
)
ON CONFLICT (workspace_id, child_artifact_id, parent_artifact_id)
DO UPDATE SET ord = excluded.ord
"""


def lineage_edge_params(
    *,
    child_artifact_id: str,
    parent_artifact_id: str,
    ord: int,
) -> tuple[Any, ...]:
    """Build params tuple for a lineage edge upsert."""
    return (WORKSPACE_ID, child_artifact_id, parent_artifact_id, ord)
