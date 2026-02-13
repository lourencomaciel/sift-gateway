"""Mapping repository SQL helpers."""

from __future__ import annotations

from sift_mcp.constants import WORKSPACE_ID

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
    """Build parameter tuple for the map status UPDATE.

    Args:
        artifact_id: Target artifact identifier.
        map_kind: Mapping kind (none, full, partial).
        map_status: New mapping status.
        mapper_version: Version of the mapper used.
        map_budget_fingerprint: Budget fingerprint hash.
        map_backend_id: Backend identifier for the mapper.
        prng_version: PRNG version for reproducibility.
        map_error: Error message if mapping failed.

    Returns:
        Positional parameter tuple for the SQL statement.
    """
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
