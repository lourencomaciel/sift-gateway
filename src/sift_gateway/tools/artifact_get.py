"""Validate arguments and check preconditions for ``artifact.get``.

Support two retrieval targets: ``"envelope"`` returns the raw
envelope optionally filtered by JSONPath, and ``"mapped"`` returns
mapped root metadata.  Exports ``validate_get_args``,
``check_get_preconditions``, and fetch SQL constants.

Typical usage example::

    error = validate_get_args(arguments)
    if error:
        return error
    row = conn.execute(FETCH_ARTIFACT_SQL, params).fetchone()
    error = check_get_preconditions(row, target="envelope")
"""

from __future__ import annotations

from typing import Any

from sift_gateway.tools._validation import (
    require_artifact_id,
    require_gateway_session,
)


def validate_get_args(arguments: dict[str, Any]) -> dict[str, Any] | None:
    """Validate ``artifact.get`` arguments.

    Args:
        arguments: Raw tool arguments including gateway context,
            ``artifact_id``, and optional ``target``.

    Returns:
        Error dict on validation failure, ``None`` when valid.
    """
    session_err = require_gateway_session(arguments)
    if session_err is not None:
        return session_err

    artifact_err = require_artifact_id(arguments)
    if artifact_err is not None:
        return artifact_err

    target = arguments.get("target", "envelope")
    if target not in ("envelope", "mapped"):
        return {
            "code": "INVALID_ARGUMENT",
            "message": f"invalid target: {target}",
        }

    return None


# SQL to fetch artifact for get
FETCH_ARTIFACT_SQL = """
SELECT a.artifact_id, a.payload_hash_full, a.deleted_at,
       a.map_kind, a.map_status, a.generation,
       a.mapped_part_index, a.map_budget_fingerprint,
       pb.envelope, pb.envelope_canonical_encoding,
       pb.payload_fs_path,
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
    """Return ``True`` if the artifact uses partial mapping.

    Args:
        artifact_row: Artifact database row dict.

    Returns:
        ``True`` when ``map_kind`` equals ``"partial"``.
    """
    return str(artifact_row.get("map_kind", "none")) == "partial"


def check_get_preconditions(
    artifact_row: dict[str, Any] | None,
    target: str,
) -> dict[str, Any] | None:
    """Check preconditions for ``artifact.get`` retrieval.

    Verifies the artifact exists, is not deleted, and has the
    required mapping status when *target* is ``"mapped"``.

    Args:
        artifact_row: Artifact database row dict, or ``None``
            if the artifact was not found.
        target: Retrieval target (``"envelope"`` or
            ``"mapped"``).

    Returns:
        Error dict on precondition failure, ``None`` when all
        checks pass.
    """
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
