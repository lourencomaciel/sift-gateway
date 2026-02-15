"""Validate arguments and build responses for ``artifact.describe``.

Return mapping metadata and discovered root paths for an artifact,
including sample indices and coverage statistics for partially
mapped artifacts.  Exports ``validate_describe_args``,
``build_describe_response``, and fetch SQL constants.

Typical usage example::

    error = validate_describe_args(arguments)
    if error:
        return error
    response = build_describe_response(artifact_row, roots)
"""

from __future__ import annotations

from typing import Any


def validate_describe_args(arguments: dict[str, Any]) -> dict[str, Any] | None:
    """Validate ``artifact.describe`` arguments.

    Args:
        arguments: Raw tool arguments including gateway context
            and ``artifact_id``.

    Returns:
        Error dict on validation failure, ``None`` when valid.
    """
    ctx = arguments.get("_gateway_context")
    if not isinstance(ctx, dict) or not ctx.get("session_id"):
        return {
            "code": "INVALID_ARGUMENT",
            "message": "missing _gateway_context.session_id",
        }

    if not arguments.get("artifact_id"):
        return {"code": "INVALID_ARGUMENT", "message": "missing artifact_id"}

    return None


# SQL for describe
FETCH_DESCRIBE_SQL = """
SELECT a.artifact_id, a.map_kind, a.map_status, a.mapper_version,
       a.map_budget_fingerprint, a.map_backend_id, a.prng_version,
       a.mapped_part_index, a.deleted_at, a.generation
FROM artifacts a
WHERE a.workspace_id = %s AND a.artifact_id = %s
"""

FETCH_ROOTS_SQL = """
SELECT root_key, root_path, count_estimate, inventory_coverage,
       root_summary, root_score, root_shape, fields_top,
       sample_indices
FROM artifact_roots
WHERE workspace_id = %s AND artifact_id = %s
ORDER BY root_score DESC
"""

FETCH_SCHEMA_ROOTS_SQL = """
SELECT root_key, root_path, schema_version, schema_hash,
       mode, completeness, observed_records, dataset_hash,
       traversal_contract_version, map_budget_fingerprint
FROM artifact_schema_roots
WHERE workspace_id = %s AND artifact_id = %s
ORDER BY observed_records DESC, root_path ASC
"""

FETCH_SCHEMA_FIELDS_SQL = """
SELECT field_path, types, nullable, required, observed_count, example_value
FROM artifact_schema_fields
WHERE workspace_id = %s AND artifact_id = %s AND root_key = %s
ORDER BY field_path ASC
"""


def build_describe_response(
    artifact_row: dict[str, Any],
    roots: list[dict[str, Any]],
    schemas: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build the ``artifact.describe`` response dict.

    Assembles mapping metadata, traversal contract version, and
    root path information including sample coverage statistics
    for partially mapped artifacts.

    Args:
        artifact_row: Artifact database row with mapping
            metadata columns.
        roots: List of root row dicts ordered by
            ``root_score`` descending.
        schemas: Optional schema entries for this artifact.

    Returns:
        Structured response dict with ``artifact_id``,
        ``mapping``, and ``roots`` sections.
    """
    from sift_mcp.constants import TRAVERSAL_CONTRACT_VERSION

    response: dict[str, Any] = {
        "artifact_id": artifact_row["artifact_id"],
        "mapping": {
            "map_kind": artifact_row.get("map_kind", "none"),
            "map_status": artifact_row.get("map_status", "pending"),
            "mapper_version": artifact_row.get("mapper_version"),
            "map_budget_fingerprint": artifact_row.get(
                "map_budget_fingerprint"
            ),
            "map_backend_id": artifact_row.get("map_backend_id"),
            "prng_version": artifact_row.get("prng_version"),
            "traversal_contract_version": TRAVERSAL_CONTRACT_VERSION,
        },
        "roots": [],
        "schemas": list(schemas or []),
    }

    for root in roots:
        root_summary = root.get("root_summary")
        root_info: dict[str, Any] = {
            "root_key": root["root_key"],
            "root_path": root["root_path"],
            "root_shape": root.get("root_shape"),
            "count_estimate": root.get("count_estimate"),
            "fields_top": root.get("fields_top"),
        }

        sample_indices = root.get("sample_indices")
        if sample_indices is not None:
            root_info["sampled_only"] = True
            root_info["sample_indices"] = sample_indices
            sampled_record_count = (
                len(sample_indices) if isinstance(sample_indices, list) else 0
            )
            if isinstance(root_summary, dict):
                raw_count = root_summary.get("sampled_record_count")
                if isinstance(raw_count, int) and raw_count >= 0:
                    sampled_record_count = raw_count
                raw_prefix_len = root_summary.get("sampled_prefix_len")
                if isinstance(raw_prefix_len, int) and raw_prefix_len >= 0:
                    root_info["sampled_prefix_len"] = raw_prefix_len
                raw_prefix_cov = root_summary.get("prefix_coverage")
                if isinstance(raw_prefix_cov, bool):
                    root_info["prefix_coverage"] = raw_prefix_cov
                raw_stop_reason = root_summary.get("stop_reason")
                if isinstance(raw_stop_reason, str) and raw_stop_reason:
                    root_info["stop_reason"] = raw_stop_reason
                raw_skipped = root_summary.get("skipped_oversize_records")
                if isinstance(raw_skipped, int) and raw_skipped >= 0:
                    root_info["skipped_oversize_records"] = raw_skipped
            root_info["sampled_record_count"] = sampled_record_count

        response["roots"].append(root_info)

    return response
