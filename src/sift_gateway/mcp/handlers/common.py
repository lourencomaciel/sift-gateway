"""Shared utilities and constants used across handler modules."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from sift_gateway.core.retrieval_helpers import extract_json_target
from sift_gateway.core.rows import row_to_dict, rows_to_dicts

__all__ = [
    "ARTIFACT_META_COLUMNS",
    "ENVELOPE_COLUMNS",
    "FETCH_ARTIFACT_META_SQL",
    "ROOT_COLUMNS",
    "SAMPLE_COLUMNS",
    "VISIBLE_ARTIFACT_SQL",
    "extract_json_target",
    "row_to_dict",
    "rows_to_dicts",
    "touch_retrieval_artifacts",
]

# ---------------------------------------------------------------------------
# Row mapping helpers
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Shared SQL
# ---------------------------------------------------------------------------

VISIBLE_ARTIFACT_SQL = """
SELECT 1
FROM artifacts
WHERE workspace_id = %s
  AND artifact_id = %s
LIMIT 1
"""

FETCH_ARTIFACT_META_SQL = """
SELECT artifact_id, map_kind, map_status, index_status,
       deleted_at, generation, map_budget_fingerprint
FROM artifacts
WHERE workspace_id = %s AND artifact_id = %s
"""

# ---------------------------------------------------------------------------
# Shared column lists
# ---------------------------------------------------------------------------

ARTIFACT_META_COLUMNS = [
    "artifact_id",
    "map_kind",
    "map_status",
    "index_status",
    "deleted_at",
    "generation",
    "map_budget_fingerprint",
]

ROOT_COLUMNS = [
    "root_key",
    "root_path",
    "count_estimate",
    "inventory_coverage",
    "root_summary",
    "root_score",
    "root_shape",
    "fields_top",
    "sample_indices",
]

SAMPLE_COLUMNS = ["sample_index", "record", "record_bytes", "record_hash"]

ENVELOPE_COLUMNS = [
    "artifact_id",
    "payload_hash_full",
    "deleted_at",
    "map_kind",
    "map_status",
    "generation",
    "mapped_part_index",
    "map_budget_fingerprint",
    "envelope",
    "envelope_canonical_encoding",
    "payload_fs_path",
    "contains_binary_refs",
]


def touch_retrieval_artifacts(
    ctx: Any,
    connection: Any,
    *,
    session_id: str,
    artifact_ids: Sequence[str],
) -> None:
    """Touch retrieval timestamp for artifact ids and commit when needed."""
    touched = False
    for artifact_id in artifact_ids:
        touched = (
            ctx._safe_touch_for_retrieval(
                connection,
                session_id=session_id,
                artifact_id=artifact_id,
            )
            or touched
        )
    if touched:
        commit = getattr(connection, "commit", None)
        if callable(commit):
            commit()
