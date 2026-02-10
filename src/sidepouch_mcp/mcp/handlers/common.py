"""Shared utilities and constants used across handler modules."""

from __future__ import annotations

import logging
from typing import Any, Mapping

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Row mapping helpers
# ---------------------------------------------------------------------------


def row_to_dict(
    row: tuple[object, ...] | Mapping[str, Any] | None,
    columns: list[str],
) -> dict[str, Any] | None:
    """Convert a database row to a column-keyed dictionary.

    Args:
        row: A tuple, mapping, or ``None`` from a database fetch.
        columns: Column names corresponding to tuple positions.

    Returns:
        A dict mapping column names to values, or ``None`` when
        *row* is ``None``.
    """
    if row is None:
        return None
    if isinstance(row, Mapping):
        return dict(row)
    if len(row) < len(columns):
        _logger.warning(
            "row has %d values but %d columns expected;"
            " missing columns will be None",
            len(row),
            len(columns),
        )
    return {
        column: row[index] if index < len(row) else None
        for index, column in enumerate(columns)
    }


def rows_to_dicts(
    rows: list[tuple[object, ...] | Mapping[str, Any]],
    columns: list[str],
) -> list[dict[str, Any]]:
    """Convert a list of database rows to column-keyed dicts.

    Args:
        rows: Sequence of tuples or mappings from a fetchall call.
        columns: Column names corresponding to tuple positions.

    Returns:
        List of dicts, one per non-``None`` row.
    """
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
    "envelope_canonical_bytes",
    "envelope_canonical_bytes_len",
    "contains_binary_refs",
]


# ---------------------------------------------------------------------------
# Envelope helpers
# ---------------------------------------------------------------------------


def extract_json_target(
    envelope: dict[str, Any], mapped_part_index: int | None
) -> Any:
    """Extract the JSON value that root_paths are relative to.

    Mapping creates root_paths relative to the JSON content part value (e.g.
    ``{"users": [...]}``), not the full envelope wrapper.  This helper extracts
    that target so callers can evaluate root_path JSONPath expressions against
    the correct object.

    Returns the full *envelope* when *mapped_part_index* is ``None`` or doesn't
    point at a valid JSON content part.
    """
    if not isinstance(mapped_part_index, int):
        return envelope
    content = envelope.get("content", [])
    if 0 <= mapped_part_index < len(content):
        part = content[mapped_part_index]
        if (
            isinstance(part, dict)
            and part.get("type") == "json"
            and "value" in part
        ):
            return part["value"]
    return envelope
