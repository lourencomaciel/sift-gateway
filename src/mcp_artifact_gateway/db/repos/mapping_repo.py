"""Mapping repository SQL helpers."""

from __future__ import annotations

from mcp_artifact_gateway.constants import WORKSPACE_ID


UPDATE_MAP_STATUS_SQL = """
UPDATE artifacts
SET map_kind = %s,
    map_status = %s,
    mapper_version = %s,
    map_budget_fingerprint = %s,
    map_backend_id = %s,
    prng_version = %s,
    map_error = %s
WHERE workspace_id = %s
  AND artifact_id = %s
"""


def update_map_status_params(
    *,
    artifact_id: str,
    map_kind: str,
    map_status: str,
    mapper_version: str,
    map_budget_fingerprint: str | None,
    map_backend_id: str | None,
    prng_version: str | None,
    map_error: str | None,
) -> tuple[object, ...]:
    return (
        map_kind,
        map_status,
        mapper_version,
        map_budget_fingerprint,
        map_backend_id,
        prng_version,
        map_error,
        WORKSPACE_ID,
        artifact_id,
    )
