"""Describe/schema helpers for mirrored tool responses."""

from __future__ import annotations

from typing import Any

from sift_gateway.constants import WORKSPACE_ID
from sift_gateway.mcp.handlers.common import row_to_dict, rows_to_dicts
from sift_gateway.mcp.handlers.schema_payload import build_schema_payload
from sift_gateway.obs.logging import get_logger
from sift_gateway.tools.artifact_describe import (
    FETCH_DESCRIBE_SQL,
    FETCH_SCHEMA_ROOTS_SQL,
    build_describe_response,
)
from sift_gateway.tools.artifact_schema import FETCH_SCHEMA_FIELDS_SQL

_DESCRIBE_COLUMNS = [
    "artifact_id",
    "map_kind",
    "map_status",
    "mapper_version",
    "map_budget_fingerprint",
    "map_backend_id",
    "prng_version",
    "mapped_part_index",
    "deleted_at",
    "generation",
]

_SCHEMA_ROOT_COLUMNS = [
    "root_key",
    "root_path",
    "schema_version",
    "schema_hash",
    "mode",
    "completeness",
    "observed_records",
    "dataset_hash",
    "traversal_contract_version",
    "map_budget_fingerprint",
]

_SCHEMA_FIELD_COLUMNS = [
    "field_path",
    "types",
    "nullable",
    "required",
    "observed_count",
    "example_value",
    "distinct_values",
    "cardinality",
]


def _is_descendant_root_path(
    parent_root_path: str,
    candidate_root_path: str,
) -> bool:
    """Return True when candidate is a strict descendant of parent."""
    if parent_root_path == candidate_root_path:
        return False
    if parent_root_path == "$":
        return candidate_root_path.startswith(("$.", "$["))
    if not candidate_root_path.startswith(parent_root_path):
        return False
    suffix = candidate_root_path[
        len(parent_root_path) : len(parent_root_path) + 1
    ]
    return suffix in {".", "["}


def _select_leaf_schema_roots(
    schemas: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Keep leaf roots only; drop exact duplicates and parent roots."""
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for schema in schemas:
        root_path = schema.get("root_path")
        schema_hash = schema.get("schema_hash")
        determinism = schema.get("determinism")
        dataset_hash = (
            determinism.get("dataset_hash")
            if isinstance(determinism, dict)
            else None
        )
        key = (
            str(root_path) if isinstance(root_path, str) else "",
            str(schema_hash) if isinstance(schema_hash, str) else "",
            str(dataset_hash) if isinstance(dataset_hash, str) else "",
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(schema)
    root_paths = [
        schema.get("root_path")
        if isinstance(schema.get("root_path"), str)
        else None
        for schema in deduped
    ]
    leaves: list[dict[str, Any]] = []
    for index, schema in enumerate(deduped):
        root_path = root_paths[index]
        if not isinstance(root_path, str):
            leaves.append(schema)
            continue
        has_child = any(
            isinstance(candidate, str)
            and _is_descendant_root_path(root_path, candidate)
            for i, candidate in enumerate(root_paths)
            if i != index
        )
        if has_child:
            continue
        leaves.append(schema)
    return leaves


def _fetch_inline_describe(
    connection: Any,
    artifact_id: str,
) -> dict[str, Any]:
    """Fetch schema-first describe data.

    Queries artifact and schema tables on the already-open
    *connection* and returns the full describe dict. Falls back
    to a minimal describe on any error so callers always get a result.

    Args:
        connection: Active database connection.
        artifact_id: The artifact to describe.

    Returns:
        Describe payload dict.
    """
    try:
        artifact_row = row_to_dict(
            connection.execute(
                FETCH_DESCRIBE_SQL,
                (WORKSPACE_ID, artifact_id),
            ).fetchone(),
            _DESCRIBE_COLUMNS,
        )
        if artifact_row is None:
            artifact_row = {
                "artifact_id": artifact_id,
                "map_kind": "none",
                "map_status": "pending",
            }
        schema_roots = rows_to_dicts(
            connection.execute(
                FETCH_SCHEMA_ROOTS_SQL,
                (WORKSPACE_ID, artifact_id),
            ).fetchall(),
            _SCHEMA_ROOT_COLUMNS,
        )
        schemas: list[dict[str, Any]] = []
        for schema_root in schema_roots:
            root_key = schema_root.get("root_key")
            if not isinstance(root_key, str):
                continue
            field_rows = rows_to_dicts(
                connection.execute(
                    FETCH_SCHEMA_FIELDS_SQL,
                    (WORKSPACE_ID, artifact_id, root_key),
                ).fetchall(),
                _SCHEMA_FIELD_COLUMNS,
            )
            schemas.append(
                build_schema_payload(
                    schema_root=schema_root,
                    field_rows=field_rows,
                    include_null_example_value=True,
                )
            )
        describe = build_describe_response(
            artifact_row,
            [],
            schemas=_select_leaf_schema_roots(schemas),
        )
    except Exception:
        get_logger(component="mcp.handlers").warning(
            "inline describe fetch failed (best-effort)",
            exc_info=True,
        )
        describe = build_describe_response(
            {
                "artifact_id": artifact_id,
                "map_kind": "none",
                "map_status": "pending",
            },
            [],
        )
    return describe


def _minimal_describe(
    artifact_id: str,
) -> dict[str, Any]:
    """Build a minimal describe for DB-less or error paths.

    Args:
        artifact_id: The artifact identifier.

    Returns:
        Describe payload dict with empty roots.
    """
    return build_describe_response(
        {
            "artifact_id": artifact_id,
            "map_kind": "none",
            "map_status": "pending",
        },
        [],
    )


def _describe_has_ready_schema(describe: dict[str, Any]) -> bool:
    """Return True when describe payload includes ready schema data."""
    mapping = describe.get("mapping")
    if not isinstance(mapping, dict):
        return False
    if mapping.get("map_status") != "ready":
        return False
    schemas = describe.get("schemas")
    return isinstance(schemas, list) and bool(schemas)


def _schema_payload_from_describe(
    describe: dict[str, Any],
) -> list[dict[str, Any]]:
    """Extract verbose schema payload from describe data."""
    schemas = describe.get("schemas")
    return (
        [item for item in schemas if isinstance(item, dict)]
        if isinstance(schemas, list)
        else []
    )


def _queryable_root_paths_from_schemas(
    schemas: list[dict[str, Any]],
) -> list[str]:
    """Return sorted unique schema root paths used as queryable roots."""
    paths: set[str] = set()
    for schema in schemas:
        root_path = schema.get("root_path")
        if isinstance(root_path, str) and root_path:
            paths.add(root_path)
    return sorted(paths)
