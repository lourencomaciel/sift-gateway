"""Shared lineage root-candidate resolution helpers."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from sift_gateway.constants import WORKSPACE_ID
from sift_gateway.core.rows import row_to_dict
from sift_gateway.envelope.responses import gateway_error
from sift_gateway.mcp.lineage import (
    compute_related_set_hash,
    compute_root_compatibility_signature,
    compute_root_signature,
    resolve_related_artifacts,
)
from sift_gateway.tools.artifact_schema import FETCH_SCHEMA_ROOT_BY_PATH_SQL
from sift_gateway.tools.artifact_select import FETCH_ROOT_SQL

_ARTIFACT_META_COLUMNS = [
    "artifact_id",
    "map_kind",
    "map_status",
    "index_status",
    "deleted_at",
    "generation",
    "map_budget_fingerprint",
]

_FETCH_ARTIFACT_META_SQL = """
SELECT artifact_id, map_kind, map_status, index_status,
       deleted_at, generation, map_budget_fingerprint
FROM artifacts
WHERE workspace_id = %s AND artifact_id = %s
"""

_SELECT_ROOT_COLUMNS = [
    "root_key",
    "root_path",
    "count_estimate",
    "root_shape",
    "fields_top",
    "sample_indices",
    "root_summary",
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

_FETCH_ARTIFACT_ROOT_PATHS_SQL = """
SELECT root_path
FROM artifact_roots
WHERE workspace_id = %s AND artifact_id = %s
ORDER BY root_path ASC
"""

CandidateRow = tuple[str, dict[str, Any], dict[str, Any], dict[str, Any]]


@dataclass(frozen=True)
class AllRelatedRootCandidates:
    """Resolved all-related lineage state for a specific root path."""

    related_rows: list[dict[str, Any]]
    related_ids: list[str]
    related_set_hash: str
    candidate_rows: list[CandidateRow]
    missing_root_artifacts: list[str]
    mixed_schema_signature_groups: list[dict[str, Any]]


@dataclass(frozen=True)
class SingleRootCandidate:
    """Resolved single-artifact candidate state for a specific root path."""

    related_ids: list[str]
    candidate_rows: list[CandidateRow]
    missing_root_artifacts: list[str]
    anchor_meta: dict[str, Any] | None


def _available_root_paths(
    connection: Any,
    *,
    artifact_id: str,
) -> list[str]:
    """Return sorted available mapped root paths for one artifact."""
    rows = connection.execute(
        _FETCH_ARTIFACT_ROOT_PATHS_SQL,
        (WORKSPACE_ID, artifact_id),
    ).fetchall()
    paths = [
        str(row[0])
        for row in rows
        if isinstance(row, (list, tuple)) and row and isinstance(row[0], str)
    ]
    return sorted(set(paths))


def _suggested_root_path(
    *,
    requested_root_path: str,
    available_root_paths: list[str],
) -> str | None:
    """Return one suggested root path when a single clear choice exists."""
    if not available_root_paths:
        return None
    if requested_root_path in available_root_paths:
        return requested_root_path
    if len(available_root_paths) == 1:
        return available_root_paths[0]
    if "$" in available_root_paths:
        return "$"
    return available_root_paths[0]


def resolve_single_root_candidate(
    connection: Any,
    *,
    anchor_artifact_id: str,
    root_path: str,
) -> SingleRootCandidate | dict[str, Any]:
    """Resolve a single-artifact candidate row for select scope=single."""
    anchor_meta = row_to_dict(
        connection.execute(
            _FETCH_ARTIFACT_META_SQL,
            (WORKSPACE_ID, anchor_artifact_id),
        ).fetchone(),
        _ARTIFACT_META_COLUMNS,
    )
    if anchor_meta is None:
        return gateway_error("NOT_FOUND", "artifact not found")
    if anchor_meta.get("deleted_at") is not None:
        return gateway_error("GONE", "artifact has been deleted")
    if anchor_meta.get("map_status") != "ready":
        return gateway_error(
            "INVALID_ARGUMENT",
            "artifact mapping is not ready",
        )

    related_ids = [anchor_artifact_id]
    root_row = row_to_dict(
        connection.execute(
            FETCH_ROOT_SQL,
            (WORKSPACE_ID, anchor_artifact_id, root_path),
        ).fetchone(),
        _SELECT_ROOT_COLUMNS,
    )
    schema_root = row_to_dict(
        connection.execute(
            FETCH_SCHEMA_ROOT_BY_PATH_SQL,
            (WORKSPACE_ID, anchor_artifact_id, root_path),
        ).fetchone(),
        _SCHEMA_ROOT_COLUMNS,
    )
    if root_row is None or schema_root is None:
        missing_root_artifacts = [anchor_artifact_id]
        available_root_paths = _available_root_paths(
            connection,
            artifact_id=anchor_artifact_id,
        )
        return gateway_error(
            "NOT_FOUND",
            "root_path not found",
            details={
                "root_path": root_path,
                "skipped_artifacts": len(missing_root_artifacts),
                "artifact_ids": missing_root_artifacts,
                "available_root_paths": available_root_paths,
                "suggested_root_path": _suggested_root_path(
                    requested_root_path=root_path,
                    available_root_paths=available_root_paths,
                ),
            },
        )
    return SingleRootCandidate(
        related_ids=related_ids,
        candidate_rows=[
            (
                anchor_artifact_id,
                anchor_meta,
                root_row,
                schema_root,
            )
        ],
        missing_root_artifacts=[],
        anchor_meta=anchor_meta,
    )


def resolve_all_related_root_candidates(
    connection: Any,
    *,
    session_id: str,
    anchor_artifact_id: str,
    root_path: str,
    max_related_artifacts: int,
    resolve_related_fn: Callable[..., list[dict[str, Any]]] = (
        resolve_related_artifacts
    ),
    compute_related_set_hash_fn: Callable[[list[dict[str, Any]]], str] = (
        compute_related_set_hash
    ),
) -> AllRelatedRootCandidates | dict[str, Any]:
    """Resolve all-related candidate roots and enforce schema compatibility."""
    related_rows = resolve_related_fn(
        connection,
        session_id=session_id,
        anchor_artifact_id=anchor_artifact_id,
    )
    if not related_rows:
        return gateway_error("NOT_FOUND", "artifact not found")
    if len(related_rows) > max_related_artifacts:
        return gateway_error(
            "RESOURCE_EXHAUSTED",
            "lineage query exceeds related artifact limit",
            details={
                "artifact_count": len(related_rows),
                "max_artifacts": max_related_artifacts,
            },
        )

    related_ids = [
        artifact_id
        for row in related_rows
        if isinstance((artifact_id := row.get("artifact_id")), str)
    ]
    if not related_ids:
        return gateway_error("NOT_FOUND", "artifact not found")

    related_set_hash = compute_related_set_hash_fn(related_rows)

    candidate_rows: list[CandidateRow] = []
    missing_root_artifacts: list[str] = []
    strict_signature_groups: dict[str, list[str]] = {}
    compatibility_signature_groups: dict[str, list[str]] = {}

    for artifact_id in related_ids:
        artifact_meta = row_to_dict(
            connection.execute(
                _FETCH_ARTIFACT_META_SQL,
                (WORKSPACE_ID, artifact_id),
            ).fetchone(),
            _ARTIFACT_META_COLUMNS,
        )
        if artifact_meta is None:
            continue
        if artifact_meta.get("deleted_at") is not None:
            continue
        if artifact_meta.get("map_status") != "ready":
            missing_root_artifacts.append(artifact_id)
            continue

        root_row = row_to_dict(
            connection.execute(
                FETCH_ROOT_SQL,
                (WORKSPACE_ID, artifact_id, root_path),
            ).fetchone(),
            _SELECT_ROOT_COLUMNS,
        )
        if root_row is None:
            missing_root_artifacts.append(artifact_id)
            continue

        schema_root = row_to_dict(
            connection.execute(
                FETCH_SCHEMA_ROOT_BY_PATH_SQL,
                (WORKSPACE_ID, artifact_id, root_path),
            ).fetchone(),
            _SCHEMA_ROOT_COLUMNS,
        )
        if schema_root is None:
            missing_root_artifacts.append(artifact_id)
            continue

        signature = compute_root_signature(
            root_path=root_path,
            schema_hash=schema_root.get("schema_hash"),
            schema_mode=schema_root.get("mode"),
            schema_completeness=schema_root.get("completeness"),
        )
        strict_signature_groups.setdefault(signature, []).append(artifact_id)
        compatibility_signature = compute_root_compatibility_signature(
            root_path=root_path,
            root_shape=root_row.get("root_shape"),
        )
        compatibility_signature_groups.setdefault(
            compatibility_signature, []
        ).append(artifact_id)
        candidate_rows.append(
            (artifact_id, artifact_meta, root_row, schema_root)
        )

    if not candidate_rows:
        details: dict[str, Any] = {}
        if missing_root_artifacts:
            available_root_paths_by_artifact: dict[str, list[str]] = {}
            merged_paths: set[str] = set()
            for artifact_id in sorted(set(missing_root_artifacts)):
                artifact_paths = _available_root_paths(
                    connection,
                    artifact_id=artifact_id,
                )
                available_root_paths_by_artifact[artifact_id] = artifact_paths
                merged_paths.update(artifact_paths)
            merged_root_paths = sorted(merged_paths)
            details = {
                "root_path": root_path,
                "skipped_artifacts": len(missing_root_artifacts),
                "artifact_ids": missing_root_artifacts,
                "available_root_paths": merged_root_paths,
                "available_root_paths_by_artifact": (
                    available_root_paths_by_artifact
                ),
                "suggested_root_path": _suggested_root_path(
                    requested_root_path=root_path,
                    available_root_paths=merged_root_paths,
                ),
            }
        return gateway_error(
            "NOT_FOUND",
            "root_path not found",
            details=details,
        )

    if len(compatibility_signature_groups) > 1:
        return gateway_error(
            "INVALID_ARGUMENT",
            "incompatible lineage schema for root_path",
            details={
                "code": "INCOMPATIBLE_LINEAGE_SCHEMA",
                "root_path": root_path,
                "hint": (
                    "Use scope=single to query only anchor artifacts, "
                    "or choose root_path/root_paths that resolve to one "
                    "compatible schema signature."
                ),
                "signature_groups": [
                    {
                        "signature": signature,
                        "artifact_ids": sorted(artifact_ids),
                    }
                    for signature, artifact_ids in sorted(
                        compatibility_signature_groups.items()
                    )
                ],
                "strict_signature_groups": [
                    {
                        "signature": signature,
                        "artifact_ids": sorted(artifact_ids),
                    }
                    for signature, artifact_ids in sorted(
                        strict_signature_groups.items()
                    )
                ],
            },
        )

    mixed_schema_signature_groups: list[dict[str, Any]] = []
    if len(strict_signature_groups) > 1:
        mixed_schema_signature_groups = [
            {
                "signature": signature,
                "artifact_ids": sorted(artifact_ids),
            }
            for signature, artifact_ids in sorted(
                strict_signature_groups.items()
            )
        ]

    return AllRelatedRootCandidates(
        related_rows=related_rows,
        related_ids=related_ids,
        related_set_hash=related_set_hash,
        candidate_rows=candidate_rows,
        missing_root_artifacts=missing_root_artifacts,
        mixed_schema_signature_groups=mixed_schema_signature_groups,
    )
