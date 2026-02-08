"""Shared utilities and constants used across handler modules."""

from __future__ import annotations

from typing import Any, Mapping

from mcp_artifact_gateway.constants import WORKSPACE_ID

# ---------------------------------------------------------------------------
# Row mapping helpers
# ---------------------------------------------------------------------------


def row_to_dict(
    row: tuple[object, ...] | Mapping[str, Any] | None,
    columns: list[str],
) -> dict[str, Any] | None:
    if row is None:
        return None
    if isinstance(row, Mapping):
        return dict(row)
    return {
        column: row[index] if index < len(row) else None
        for index, column in enumerate(columns)
    }


def rows_to_dicts(
    rows: list[tuple[object, ...] | Mapping[str, Any]],
    columns: list[str],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        mapped = row_to_dict(row, columns)
        if mapped is not None:
            out.append(mapped)
    return out


# ---------------------------------------------------------------------------
# Shared SQL
# ---------------------------------------------------------------------------

VISIBLE_ARTIFACT_SQL = """
SELECT 1
FROM artifact_refs
WHERE workspace_id = %s
  AND session_id = %s
  AND artifact_id = %s
LIMIT 1
"""

FETCH_ARTIFACT_META_SQL = """
SELECT artifact_id, map_kind, map_status, index_status, deleted_at, generation, map_budget_fingerprint
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
