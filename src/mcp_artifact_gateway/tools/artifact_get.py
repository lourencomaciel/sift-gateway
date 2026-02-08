"""artifact.get tool implementation.

Retrieval targets:

- ``target="envelope"`` -- returns the raw envelope, optionally filtered by
  a JSONPath expression.  Traversal uses ``traverse_deterministic`` ordering
  (arrays ascending index, objects lexicographic key).
- ``target="mapped"`` -- returns mapped root metadata.  When the artifact
  has ``map_kind="partial"``, responses carry ``sampled_only=True`` and the
  enumerated data covers only the sampled indices (ascending order).
"""

from __future__ import annotations

from typing import Any

from mcp_artifact_gateway.constants import WORKSPACE_ID


def validate_get_args(arguments: dict[str, Any]) -> dict[str, Any] | None:
    """Validate artifact.get arguments. Returns error dict or None if valid."""
    ctx = arguments.get("_gateway_context")
    if not isinstance(ctx, dict) or not ctx.get("session_id"):
        return {
            "code": "INVALID_ARGUMENT",
            "message": "missing _gateway_context.session_id",
        }

    artifact_id = arguments.get("artifact_id")
    if not artifact_id:
        return {"code": "INVALID_ARGUMENT", "message": "missing artifact_id"}

    target = arguments.get("target", "envelope")
    if target not in ("envelope", "mapped"):
        return {"code": "INVALID_ARGUMENT", "message": f"invalid target: {target}"}

    return None


# SQL to fetch artifact for get
FETCH_ARTIFACT_SQL = """
SELECT a.artifact_id, a.payload_hash_full, a.deleted_at,
       a.map_kind, a.map_status, a.generation,
       a.mapped_part_index, a.map_budget_fingerprint,
       pb.envelope, pb.envelope_canonical_encoding,
       pb.envelope_canonical_bytes, pb.envelope_canonical_bytes_len,
       pb.contains_binary_refs
FROM artifacts a
JOIN payload_blobs pb ON pb.workspace_id = a.workspace_id
    AND pb.payload_hash_full = a.payload_hash_full
WHERE a.workspace_id = %s AND a.artifact_id = %s
"""

# SQL to touch artifact.last_referenced_at (only if not deleted)
TOUCH_ARTIFACT_SQL = """
UPDATE artifacts
SET last_referenced_at = NOW()
WHERE workspace_id = %s AND artifact_id = %s AND deleted_at IS NULL
"""


def is_sampled_only(artifact_row: dict[str, Any]) -> bool:
    """Return True if the artifact was partially mapped (sampled-only data)."""
    return str(artifact_row.get("map_kind", "none")) == "partial"


def check_get_preconditions(
    artifact_row: dict[str, Any] | None,
    target: str,
) -> dict[str, Any] | None:
    """Check preconditions for artifact.get. Returns error dict or None."""
    if artifact_row is None:
        return {"code": "NOT_FOUND", "message": "artifact not found"}

    if artifact_row.get("deleted_at") is not None:
        return {"code": "GONE", "message": "artifact has been deleted"}

    if target == "mapped":
        map_status = artifact_row.get("map_status")
        map_kind = artifact_row.get("map_kind")
        if map_status != "ready":
            return {
                "code": "INVALID_ARGUMENT",
                "message": f"map_status is {map_status}, not ready",
            }
        if map_kind not in ("full", "partial"):
            return {
                "code": "INVALID_ARGUMENT",
                "message": f"map_kind is {map_kind}",
            }

    return None
