"""Lineage resolution and merge helpers for query scope."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping

from sift_mcp.constants import WORKSPACE_ID


def _row_to_dict(
    row: tuple[object, ...] | Mapping[str, Any] | None,
    columns: list[str],
) -> dict[str, Any] | None:
    """Map a DB row to a dict without importing handler modules."""
    if row is None:
        return None
    if isinstance(row, Mapping):
        return dict(row)
    return {
        column: row[index] if index < len(row) else None
        for index, column in enumerate(columns)
    }


def _rows_to_dicts(
    rows: list[tuple[object, ...] | Mapping[str, Any]],
    columns: list[str],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        mapped = _row_to_dict(row, columns)
        if mapped is not None:
            out.append(mapped)
    return out


RELATED_ARTIFACT_COLUMNS = [
    "artifact_id",
    "parent_artifact_id",
    "chain_seq",
    "created_seq",
    "generation",
    "map_kind",
    "map_status",
]


RESOLVE_RELATED_ARTIFACTS_SQL = """
WITH RECURSIVE
visible AS (
    SELECT a.artifact_id, a.parent_artifact_id, a.chain_seq,
           a.created_seq, a.generation, a.map_kind, a.map_status
    FROM artifacts a
    JOIN artifact_refs ar
      ON ar.workspace_id = a.workspace_id
     AND ar.artifact_id = a.artifact_id
    WHERE a.workspace_id = %s
      AND ar.session_id = %s
      AND a.deleted_at IS NULL
),
edges AS (
    SELECT v.artifact_id AS src, v.parent_artifact_id AS dst
    FROM visible v
    WHERE v.parent_artifact_id IS NOT NULL
    UNION
    SELECT v.parent_artifact_id AS src, v.artifact_id AS dst
    FROM visible v
    WHERE v.parent_artifact_id IS NOT NULL
),
related(artifact_id) AS (
    SELECT v.artifact_id
    FROM visible v
    WHERE v.artifact_id = %s
    UNION
    SELECT e.dst
    FROM related r
    JOIN edges e
      ON e.src = r.artifact_id
)
SELECT v.artifact_id, v.parent_artifact_id, v.chain_seq,
       v.created_seq, v.generation, v.map_kind, v.map_status
FROM visible v
JOIN related r
  ON r.artifact_id = v.artifact_id
ORDER BY v.chain_seq ASC NULLS FIRST, v.created_seq ASC, v.artifact_id ASC
"""


def resolve_related_artifacts(
    connection: Any,
    *,
    session_id: str,
    anchor_artifact_id: str,
) -> list[dict[str, Any]]:
    """Resolve the full connected lineage component for an anchor.

    Args:
        connection: Active database connection.
        session_id: Session requesting visibility.
        anchor_artifact_id: Anchor artifact for lineage traversal.

    Returns:
        Ordered related artifact rows visible to the session.
    """
    rows = connection.execute(
        RESOLVE_RELATED_ARTIFACTS_SQL,
        (
            WORKSPACE_ID,
            session_id,
            anchor_artifact_id,
        ),
    ).fetchall()
    return _rows_to_dicts(rows, RELATED_ARTIFACT_COLUMNS)


def compute_related_set_hash(artifacts: list[dict[str, Any]]) -> str:
    """Compute a deterministic hash for related artifact freshness.

    Args:
        artifacts: Related artifact rows with ``artifact_id`` and
            ``generation`` fields.

    Returns:
        Stable SHA-256 hex digest over sorted ``id:generation`` tuples.
    """
    tokens: list[str] = []
    for artifact in artifacts:
        artifact_id = artifact.get("artifact_id")
        generation = artifact.get("generation")
        if not isinstance(artifact_id, str):
            continue
        generation_token = str(generation) if isinstance(generation, int) else ""
        tokens.append(f"{artifact_id}:{generation_token}")
    payload = json.dumps(sorted(tokens), separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _canonicalize_fields_top(fields_top: Any) -> Any:
    """Canonicalize fields_top metadata for stable signature hashing."""
    if isinstance(fields_top, dict):
        out: dict[str, Any] = {}
        for key in sorted(fields_top.keys(), key=str):
            value = fields_top[key]
            if isinstance(value, dict):
                # fields_top shape is typically field -> {type: count}.
                # Keep only sorted type keys for compatibility checks.
                out[str(key)] = sorted(str(type_key) for type_key in value.keys())
            else:
                out[str(key)] = _canonicalize_fields_top(value)
        return out
    if isinstance(fields_top, list):
        canonical_items = [_canonicalize_fields_top(value) for value in fields_top]
        return sorted(
            canonical_items,
            key=lambda item: json.dumps(
                item,
                sort_keys=True,
                separators=(",", ":"),
            ),
        )
    return fields_top


def compute_root_signature(
    *,
    root_path: str,
    root_shape: Any,
    fields_top: Any,
    map_kind: Any,
) -> str:
    """Compute a deterministic compatibility signature for a root."""
    payload = {
        "root_path": root_path,
        "root_shape": root_shape if isinstance(root_shape, str) else None,
        "fields_top": (
            _canonicalize_fields_top(fields_top)
            if isinstance(fields_top, (dict, list))
            else None
        ),
        "map_kind": map_kind if isinstance(map_kind, str) else None,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def build_lineage_root_catalog(
    entries: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Aggregate root metadata across related artifacts.

    Args:
        entries: Rows containing ``artifact_id``, ``root_path``,
            ``root_shape``, ``fields_top``, ``count_estimate``,
            and ``map_kind``.

    Returns:
        Root catalog grouped by root path with compatibility metadata.
    """
    grouped: dict[str, dict[str, Any]] = {}
    for entry in entries:
        root_path = entry.get("root_path")
        artifact_id = entry.get("artifact_id")
        if not isinstance(root_path, str) or not isinstance(artifact_id, str):
            continue
        signature = compute_root_signature(
            root_path=root_path,
            root_shape=entry.get("root_shape"),
            fields_top=entry.get("fields_top"),
            map_kind=entry.get("map_kind"),
        )
        row = grouped.setdefault(
            root_path,
            {
                "root_path": root_path,
                "artifact_ids": set(),
                "signature_groups": {},
                "count_estimate_total": 0,
                "has_count": False,
            },
        )
        row["artifact_ids"].add(artifact_id)
        signature_groups = row["signature_groups"]
        group = signature_groups.setdefault(
            signature,
            {
                "signature": signature,
                "artifact_ids": [],
                "root_shape": entry.get("root_shape"),
                "fields_top": entry.get("fields_top"),
                "map_kind": entry.get("map_kind"),
            },
        )
        group["artifact_ids"].append(artifact_id)
        count_estimate = entry.get("count_estimate")
        if isinstance(count_estimate, int):
            row["count_estimate_total"] += count_estimate
            row["has_count"] = True

    roots: list[dict[str, Any]] = []
    for root_path in sorted(grouped.keys()):
        row = grouped[root_path]
        signature_groups_raw = row["signature_groups"]
        signature_groups_list: list[dict[str, Any]] = []
        for signature in sorted(signature_groups_raw.keys()):
            group = signature_groups_raw[signature]
            artifact_ids = sorted(
                {aid for aid in group["artifact_ids"] if isinstance(aid, str)}
            )
            signature_groups_list.append(
                {
                    "signature": group["signature"],
                    "artifact_ids": artifact_ids,
                    "root_shape": group["root_shape"],
                    "fields_top": group["fields_top"],
                    "map_kind": group["map_kind"],
                }
            )

        compatible = len(signature_groups_list) == 1
        representative = (
            signature_groups_list[0] if signature_groups_list else {}
        )
        roots.append(
            {
                "root_path": row["root_path"],
                "artifact_count": len(row["artifact_ids"]),
                "artifact_ids": sorted(row["artifact_ids"]),
                "compatible_for_select": compatible,
                "root_shape": representative.get("root_shape")
                if compatible
                else "mixed",
                "fields_top": representative.get("fields_top")
                if compatible
                else None,
                "map_kind": representative.get("map_kind")
                if compatible
                else "mixed",
                "count_estimate": row["count_estimate_total"]
                if row["has_count"]
                else None,
                "signature_groups": signature_groups_list,
            }
        )
    return roots
