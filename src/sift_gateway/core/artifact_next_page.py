"""Protocol-agnostic artifact next-page execution service."""

from __future__ import annotations

import json
from typing import Any

from sift_gateway.constants import WORKSPACE_ID
from sift_gateway.core.rows import row_to_dict
from sift_gateway.core.runtime import ArtifactNextPageRuntime
from sift_gateway.envelope.content_extract import (
    first_queryable_json_from_payload,
)
from sift_gateway.envelope.responses import gateway_error
from sift_gateway.pagination.contract import (
    UPSTREAM_PARTIAL_REASON_NEXT_TOKEN_MISSING,
)
from sift_gateway.pagination.extract import (
    PaginationState,
    assess_pagination,
)
from sift_gateway.storage.payload_store import reconstruct_envelope

_PAGINATION_COLUMNS = [
    "artifact_id",
    "deleted_at",
    "payload_hash_full",
    "envelope",
    "envelope_canonical_encoding",
    "payload_fs_path",
]

_FETCH_ENVELOPE_META_SQL = """
SELECT a.artifact_id, a.deleted_at, a.payload_hash_full,
       pb.envelope, pb.envelope_canonical_encoding,
       pb.payload_fs_path
FROM artifacts a
JOIN payload_blobs pb ON pb.workspace_id = a.workspace_id
    AND pb.payload_hash_full = a.payload_hash_full
WHERE a.workspace_id = %s AND a.artifact_id = %s
"""


def _extract_pagination_state(
    envelope_raw: Any,
) -> PaginationState | None:
    """Extract pagination state from a raw envelope value."""
    if isinstance(envelope_raw, str):
        try:
            envelope_dict = json.loads(envelope_raw)
        except (json.JSONDecodeError, ValueError):
            return None
    elif isinstance(envelope_raw, dict):
        envelope_dict = envelope_raw
    else:
        return None

    meta = envelope_dict.get("meta")
    if not isinstance(meta, dict):
        return None

    pagination_data = meta.get("_gateway_pagination")
    if not isinstance(pagination_data, dict):
        return None

    try:
        return PaginationState.from_dict(pagination_data)
    except (TypeError, ValueError, KeyError):
        return None


def _diagnose_missing_pagination_state(
    envelope_dict: dict[str, Any] | None,
) -> dict[str, Any]:
    """Build diagnostics when next_page cannot find continuation state."""
    if not isinstance(envelope_dict, dict):
        return {}
    resolved = first_queryable_json_from_payload(envelope_dict)
    if resolved is None:
        return {"queryable_json_found": False}

    assessment = assess_pagination(
        json_value=resolved.value,
        pagination_config=None,
        original_args={},
        upstream_prefix="",
        tool_name="",
        page_number=0,
    )
    if assessment is None:
        return {
            "queryable_json_found": True,
            "has_more_detected": False,
            "next_params_detected": False,
            "continuable": False,
        }

    has_more_detected = (
        assessment.has_more
        or assessment.partial_reason == UPSTREAM_PARTIAL_REASON_NEXT_TOKEN_MISSING
    )
    return {
        "queryable_json_found": True,
        "has_more_detected": has_more_detected,
        "next_params_detected": bool(
            assessment.state is not None and assessment.state.next_params
        ),
        "continuable": assessment.state is not None,
        "retrieval_status": assessment.retrieval_status,
        "partial_reason": assessment.partial_reason,
        "warning": assessment.warning,
        "query_json_source": {
            "part_index": resolved.part_index,
            "part_type": resolved.part_type,
            "encoding": resolved.source_encoding,
        },
    }


def _extract_envelope_dict(
    row: dict[str, Any],
    *,
    blobs_payload_dir: Any,
) -> dict[str, Any] | None:
    """Load an envelope dict from JSONB or canonical bytes."""
    envelope_raw = row.get("envelope")
    if isinstance(envelope_raw, dict):
        return envelope_raw
    if isinstance(envelope_raw, str):
        try:
            decoded = json.loads(envelope_raw)
        except (json.JSONDecodeError, ValueError):
            return None
        if isinstance(decoded, dict):
            return decoded
        return None

    payload_fs_path = row.get("payload_fs_path")
    if not isinstance(payload_fs_path, str) or not payload_fs_path:
        return None
    try:
        return reconstruct_envelope(
            payload_fs_path=payload_fs_path,
            blobs_payload_dir=blobs_payload_dir,
            encoding=str(row.get("envelope_canonical_encoding", "none")),
            expected_hash=str(row.get("payload_hash_full", "")),
        )
    except ValueError:
        return None


async def execute_artifact_next_page(
    runtime: ArtifactNextPageRuntime,
    *,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Run artifact next_page using runtime hooks provided by an adapter."""
    raw_ctx = arguments.get("_gateway_context")
    if not isinstance(raw_ctx, dict) or not raw_ctx.get("session_id"):
        return gateway_error(
            "INVALID_ARGUMENT",
            "missing _gateway_context.session_id",
        )
    session_id = str(raw_ctx["session_id"])

    artifact_id = arguments.get("artifact_id")
    if not isinstance(artifact_id, str) or not artifact_id:
        return gateway_error(
            "INVALID_ARGUMENT",
            "missing artifact_id",
        )

    if runtime.db_pool is None:
        return runtime.not_implemented("artifact.next_page")

    with runtime.db_pool.connection() as connection:
        if not runtime.artifact_visible(
            connection,
            session_id=session_id,
            artifact_id=artifact_id,
        ):
            return gateway_error("NOT_FOUND", "artifact not found")

        row = row_to_dict(
            connection.execute(
                _FETCH_ENVELOPE_META_SQL,
                (WORKSPACE_ID, artifact_id),
            ).fetchone(),
            _PAGINATION_COLUMNS,
        )

    if row is None:
        return gateway_error("NOT_FOUND", "artifact envelope not found")
    if row.get("deleted_at") is not None:
        return gateway_error("GONE", "artifact has been deleted")

    envelope_dict = _extract_envelope_dict(
        row,
        blobs_payload_dir=runtime.blobs_payload_dir,
    )
    state = _extract_pagination_state(envelope_dict)
    if state is None:
        diagnostics = _diagnose_missing_pagination_state(envelope_dict)
        hint = (
            "This artifact does not include a continuation cursor/token. "
            "If has_more was signaled upstream, configure pagination mapping "
            "for that upstream/tool so next_params can be derived."
        )
        return gateway_error(
            "INVALID_ARGUMENT",
            "artifact has no upstream pagination state. "
            "next_page only fetches additional upstream pages.",
            details={**diagnostics, "hint": hint},
        )

    qualified_name = f"{state.upstream_prefix}.{state.tool_name}"
    mirrored = runtime.get_mirrored_tool(qualified_name)
    if mirrored is None:
        return gateway_error(
            "NOT_FOUND",
            f"upstream tool {qualified_name} not found",
        )

    next_args: dict[str, Any] = {
        **state.original_args,
        **state.next_params,
    }
    next_gateway_context = dict(raw_ctx)
    next_args["_gateway_context"] = next_gateway_context
    next_args["_gateway_parent_artifact_id"] = artifact_id
    next_args["_gateway_chain_seq"] = state.page_number + 1

    return await runtime.call_mirrored_tool(mirrored, next_args)
