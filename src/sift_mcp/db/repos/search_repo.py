"""SQL helpers for artifact search and lineage listing."""

from __future__ import annotations

from typing import Any

from sift_mcp.constants import WORKSPACE_ID

SEARCH_ARTIFACTS_FTS_SQL = """
SELECT a.artifact_id, a.created_seq, a.created_at,
       a.last_referenced_at AS last_seen_at, a.source_tool,
       a.upstream_instance_id,
       COALESCE(a.capture_kind, CASE
           WHEN a.kind = 'derived_query' THEN 'derived_query'
           WHEN a.kind = 'derived_codegen' THEN 'derived_codegen'
           ELSE 'mcp_tool'
       END) AS capture_kind,
       COALESCE(a.capture_key, a.request_key) AS capture_key,
       CASE WHEN a.error_summary IS NULL
            THEN 'ok' ELSE 'error'
       END AS status,
       a.payload_total_bytes, a.error_summary,
       a.map_kind, a.map_status,
       a.chain_seq, a.kind
FROM artifacts_fts fts
JOIN artifacts a
  ON a.artifact_id = fts.artifact_id
 AND a.workspace_id = %s
WHERE artifacts_fts MATCH %s
  AND a.deleted_at IS NULL
ORDER BY bm25(artifacts_fts), a.created_seq DESC
LIMIT %s OFFSET %s
"""

LIST_ARTIFACTS_SQL = """
SELECT a.artifact_id, a.created_seq, a.created_at,
       a.last_referenced_at AS last_seen_at, a.source_tool,
       a.upstream_instance_id,
       COALESCE(a.capture_kind, CASE
           WHEN a.kind = 'derived_query' THEN 'derived_query'
           WHEN a.kind = 'derived_codegen' THEN 'derived_codegen'
           ELSE 'mcp_tool'
       END) AS capture_kind,
       COALESCE(a.capture_key, a.request_key) AS capture_key,
       CASE WHEN a.error_summary IS NULL
            THEN 'ok' ELSE 'error'
       END AS status,
       a.payload_total_bytes, a.error_summary,
       a.map_kind, a.map_status,
       a.chain_seq, a.kind
FROM artifacts a
WHERE a.workspace_id = %s
  AND (%s = 1 OR a.deleted_at IS NULL)
  AND (%s IS NULL OR a.kind = %s)
ORDER BY a.created_seq DESC
LIMIT %s OFFSET %s
"""

LIST_DERIVED_SQL = """
SELECT a.artifact_id, a.parent_artifact_id, a.kind, a.derivation,
       a.created_seq, a.created_at, a.map_status
FROM artifact_lineage_edges le
JOIN artifacts a
  ON a.workspace_id = le.workspace_id
 AND a.artifact_id = le.child_artifact_id
WHERE le.workspace_id = %s
  AND le.parent_artifact_id = %s
  AND a.deleted_at IS NULL
  AND (%s IS NULL OR a.kind = %s)
ORDER BY a.created_seq DESC
LIMIT %s OFFSET %s
"""


def search_artifacts_fts_params(
    *, query: str, limit: int, offset: int = 0
) -> tuple[Any, ...]:
    """Build positional params for SEARCH_ARTIFACTS_FTS_SQL."""
    return (WORKSPACE_ID, query, limit, offset)


def list_artifacts_params(
    *,
    include_deleted: bool,
    kind: str | None,
    limit: int,
    offset: int = 0,
) -> tuple[Any, ...]:
    """Build positional params for LIST_ARTIFACTS_SQL."""
    return (
        WORKSPACE_ID,
        1 if include_deleted else 0,
        kind,
        kind,
        limit,
        offset,
    )


def list_derived_params(
    *,
    parent_artifact_id: str,
    kind: str | None,
    limit: int,
    offset: int = 0,
) -> tuple[Any, ...]:
    """Build positional params for LIST_DERIVED_SQL."""
    return (
        WORKSPACE_ID,
        parent_artifact_id,
        kind,
        kind,
        limit,
        offset,
    )
