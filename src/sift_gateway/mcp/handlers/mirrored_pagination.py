"""Pagination helpers for mirrored tool responses."""

from __future__ import annotations

import dataclasses
import json
from typing import Any

from sift_gateway.canon.rfc8785 import canonical_bytes, coerce_floats
from sift_gateway.constants import WORKSPACE_ID
from sift_gateway.envelope.content_extract import (
    first_queryable_json_from_envelope,
    first_queryable_json_from_payload,
)
from sift_gateway.envelope.model import Envelope
from sift_gateway.envelope.responses import gateway_error
from sift_gateway.pagination.contract import (
    PAGINATION_WARNING_INCOMPLETE_RESULT_SET,
    RETRIEVAL_STATUS_COMPLETE,
    RETRIEVAL_STATUS_PARTIAL,
    UPSTREAM_PARTIAL_REASON_NEXT_TOKEN_MISSING,
    UPSTREAM_PARTIAL_REASON_SIGNAL_INCONCLUSIVE,
    UpstreamNextKind,
    build_upstream_pagination_meta,
)
from sift_gateway.pagination.extract import (
    PaginationAssessment,
    assess_pagination,
)
from sift_gateway.pagination.path_eval import evaluate_path as _evaluate_path
from sift_gateway.response_sample import (
    build_representative_item_sample,
    resolve_item_sequence_with_path,
)
from sift_gateway.storage.payload_store import reconstruct_envelope
from sift_gateway.tools.usage_hint import schema_primary_root_path
from sift_gateway.util.hashing import sha256_hex

_FETCH_PREVIOUS_PAGE_SQL = """
SELECT artifact_id, payload_hash_full
FROM artifacts
WHERE workspace_id = ?
  AND session_id = ?
  AND source_tool = ?
  AND deleted_at IS NULL
  AND created_seq < ?
ORDER BY created_seq DESC
LIMIT 1
"""

_FETCH_SCHEMA_ROOT_SIGNATURES_SQL = """
SELECT root_path, dataset_hash, observed_records
FROM artifact_schema_roots
WHERE workspace_id = ?
  AND artifact_id = ?
"""

_FETCH_ARTIFACT_ENVELOPE_SQL = """
SELECT pb.envelope,
       pb.envelope_canonical_encoding,
       pb.payload_fs_path,
       pb.payload_hash_full
FROM artifacts a
JOIN payload_blobs pb ON pb.workspace_id = a.workspace_id
    AND pb.payload_hash_full = a.payload_hash_full
WHERE a.workspace_id = ?
  AND a.artifact_id = ?
"""

_COLLECTION_HASH_ROOT_PATHS: tuple[str, ...] = (
    "$.data",
    "$.items",
    "$.results",
    "$.records",
    "$.entries",
    "$.nodes",
    "$.result.data",
    "$.result.items",
    "$.result.results",
    "$.result.records",
    "$.result.entries",
    "$.result.nodes",
)

_PLACEHOLDER_CURSOR_VALUES = frozenset(
    {
        "cursor",
        "last_cursor",
        "next_cursor",
        "after_cursor",
        "<cursor>",
        "{cursor}",
        "your_cursor",
        "insert_cursor_here",
    }
)

_DISCOVERY_CURSOR_PARAM_CANDIDATES = (
    "after",
    "cursor",
    "next_cursor",
    "nextCursor",
    "page_token",
    "pageToken",
    "next_page_token",
    "nextPageToken",
    "continuation_token",
    "continuationToken",
    "token",
)

_PAGINATION_DUPLICATE_PAGE_WARNING_CODE = "PAGINATION_DUPLICATE_PAGE"


def _is_likely_collection_root_signature(
    *,
    root_path: str,
    observed_records: int,
) -> bool:
    """Return True for stable row-collection roots used in duplicate checks."""
    if observed_records < 2:
        return False
    lower_path = root_path.lower()
    if lower_path == "$":
        return False
    return "paging" not in lower_path


def _dataset_signature_overlap(
    *,
    current_rows: list[tuple[Any, ...]],
    previous_rows: list[tuple[Any, ...]],
) -> tuple[str, str, int] | None:
    """Return one matching dataset signature tuple across two artifacts."""
    current_signatures: set[tuple[str, str, int]] = set()
    previous_signatures: set[tuple[str, str, int]] = set()
    for row in current_rows:
        if not isinstance(row, tuple) or len(row) < 3:
            continue
        root_path, dataset_hash, observed_records = row[0], row[1], row[2]
        if not isinstance(root_path, str):
            continue
        if not isinstance(dataset_hash, str) or not dataset_hash:
            continue
        if not isinstance(observed_records, int):
            continue
        if not _is_likely_collection_root_signature(
            root_path=root_path,
            observed_records=observed_records,
        ):
            continue
        current_signatures.add((root_path, dataset_hash, observed_records))
    for row in previous_rows:
        if not isinstance(row, tuple) or len(row) < 3:
            continue
        root_path, dataset_hash, observed_records = row[0], row[1], row[2]
        if not isinstance(root_path, str):
            continue
        if not isinstance(dataset_hash, str) or not dataset_hash:
            continue
        if not isinstance(observed_records, int):
            continue
        if not _is_likely_collection_root_signature(
            root_path=root_path,
            observed_records=observed_records,
        ):
            continue
        previous_signatures.add((root_path, dataset_hash, observed_records))

    overlap = current_signatures.intersection(previous_signatures)
    if not overlap:
        return None
    # Prefer the highest-cardinality overlapping collection root.
    return sorted(
        overlap,
        key=lambda item: (item[2], len(item[0]), item[0]),
        reverse=True,
    )[0]


def _collection_hash_signature(
    records: list[Any],
) -> str | None:
    """Return deterministic dataset hash for a list-like record sequence."""
    try:
        canonical = canonical_bytes(coerce_floats(records))
    except Exception:
        return None
    return f"sha256:{sha256_hex(canonical)}"


def _payload_collection_signature(
    json_value: Any,
) -> tuple[str, str, int] | None:
    """Return best-effort collection signature from payload JSON."""
    candidates: list[tuple[str, list[Any], int]] = []
    if isinstance(json_value, list):
        candidates.append(("$", json_value, len(json_value)))
    for root_path in _COLLECTION_HASH_ROOT_PATHS:
        resolved = _evaluate_path(json_value, root_path)
        if isinstance(resolved, list):
            candidates.append((root_path, resolved, len(resolved)))
    if not candidates:
        return None
    viable = [
        (root_path, records, observed_records)
        for root_path, records, observed_records in candidates
        if observed_records >= 2
    ]
    if not viable:
        return None
    root_path, records, observed_records = sorted(
        viable,
        key=lambda item: (item[2], len(item[0]), item[0]),
        reverse=True,
    )[0]
    dataset_hash = _collection_hash_signature(records)
    if not isinstance(dataset_hash, str):
        return None
    return (root_path, dataset_hash, observed_records)


def _payload_collection_signature_from_envelope(
    envelope_payload: dict[str, Any] | None,
) -> tuple[str, str, int] | None:
    """Extract collection signature from an envelope payload dict."""
    if not isinstance(envelope_payload, dict):
        return None
    resolved = first_queryable_json_from_payload(envelope_payload)
    if resolved is None:
        return None
    return _payload_collection_signature(resolved.value)


def _load_envelope_payload_from_row(
    row: tuple[Any, ...] | None,
    *,
    blobs_payload_dir: Any,
) -> dict[str, Any] | None:
    """Load envelope payload dict from DB row, reconstructing if needed."""
    if not isinstance(row, tuple) or len(row) < 4:
        return None
    envelope_raw = row[0]
    if isinstance(envelope_raw, dict):
        return envelope_raw
    if isinstance(envelope_raw, str):
        try:
            parsed = json.loads(envelope_raw)
        except (json.JSONDecodeError, ValueError):
            parsed = None
        if isinstance(parsed, dict):
            return parsed
    encoding = row[1]
    payload_fs_path = row[2]
    expected_hash = row[3]
    if not isinstance(payload_fs_path, str) or not payload_fs_path:
        return None
    try:
        reconstructed = reconstruct_envelope(
            payload_fs_path=payload_fs_path,
            blobs_payload_dir=blobs_payload_dir,
            encoding=str(encoding if isinstance(encoding, str) else "none"),
            expected_hash=(
                str(expected_hash) if isinstance(expected_hash, str) else ""
            ),
        )
    except ValueError:
        return None
    return reconstructed if isinstance(reconstructed, dict) else None


def _is_placeholder_cursor_value(value: str) -> bool:
    """Return True when cursor value looks like a literal placeholder."""
    normalized = value.strip().lower()
    if normalized in _PLACEHOLDER_CURSOR_VALUES:
        return True
    collapsed = normalized.replace("-", "_").replace(" ", "_")
    return collapsed in _PLACEHOLDER_CURSOR_VALUES


def _validate_cursor_argument(
    *,
    forwarded_args: dict[str, Any],
    pagination_config: Any,
) -> dict[str, Any] | None:
    """Validate caller-supplied cursor argument for cursor pagination."""
    if pagination_config is None:
        return None
    if getattr(pagination_config, "strategy", None) != "cursor":
        return None
    cursor_param_name = (
        getattr(pagination_config, "cursor_param_name", None) or "after"
    )
    if cursor_param_name not in forwarded_args:
        return None
    cursor_value = forwarded_args.get(cursor_param_name)
    if isinstance(cursor_value, str) and cursor_value.strip():
        if _is_placeholder_cursor_value(cursor_value):
            return gateway_error(
                "INVALID_ARGUMENT",
                (
                    f'pagination cursor "{cursor_param_name}" appears to be '
                    "a placeholder value"
                ),
                details={
                    "cursor_param": cursor_param_name,
                    "cursor_value": cursor_value,
                    "hint": (
                        "Extract the real cursor from the previous response "
                        "pagination.next.params "
                        "before retrying."
                    ),
                },
            )
        return None
    return gateway_error(
        "INVALID_ARGUMENT",
        f'pagination cursor "{cursor_param_name}" must be a non-empty string',
        details={
            "cursor_param": cursor_param_name,
            "received_type": type(cursor_value).__name__,
        },
    )


def _detect_duplicate_page_warning(
    *,
    connection: Any,
    artifact_id: str,
    payload_hash_full: str,
    created_seq: int | None,
    session_id: str,
    source_tool: str,
    forwarded_args: dict[str, Any],
    pagination_config: Any,
    current_envelope_payload: dict[str, Any] | None = None,
    blobs_payload_dir: Any | None = None,
) -> dict[str, Any] | None:
    """Detect likely repeated first-page retrieval during cursor pagination."""
    if not isinstance(created_seq, int) or created_seq <= 0:
        return None
    cursor_param_name: str | None = None
    cursor_value: str | None = None
    if pagination_config is not None:
        if getattr(pagination_config, "strategy", None) != "cursor":
            return None
        configured_cursor_param = (
            getattr(pagination_config, "cursor_param_name", None) or "after"
        )
        candidate_value = forwarded_args.get(configured_cursor_param)
        if not (isinstance(candidate_value, str) and candidate_value.strip()):
            return None
        cursor_param_name = configured_cursor_param
        cursor_value = candidate_value
    else:
        # Discovery mode: best-effort duplicate detection when callers pass a
        # cursor-like continuation argument (for example ``after``).
        for candidate_name in _DISCOVERY_CURSOR_PARAM_CANDIDATES:
            candidate_value = forwarded_args.get(candidate_name)
            if isinstance(candidate_value, str) and candidate_value.strip():
                cursor_param_name = candidate_name
                cursor_value = candidate_value
                break
        if cursor_param_name is None or cursor_value is None:
            return None
    previous_row = connection.execute(
        _FETCH_PREVIOUS_PAGE_SQL,
        (
            WORKSPACE_ID,
            session_id,
            source_tool,
            created_seq,
        ),
    ).fetchone()
    if not isinstance(previous_row, tuple) or len(previous_row) < 2:
        return None
    previous_artifact_id = previous_row[0]
    previous_hash = previous_row[1]
    if not isinstance(previous_artifact_id, str):
        return None
    if not isinstance(previous_hash, str):
        return None
    if previous_artifact_id == artifact_id:
        return None
    if previous_hash == payload_hash_full:
        return {
            "code": "PAGINATION_DUPLICATE_PAGE",
            "message": (
                "Current page payload matches the previous page for this tool. "
                "The cursor may be invalid or ignored by the upstream."
            ),
            "cursor_param": cursor_param_name,
            "cursor_value": cursor_value,
            "previous_artifact_id": previous_artifact_id,
            "payload_hash": f"sha256:{payload_hash_full}",
            "match_method": "payload_hash",
        }

    current_rows = connection.execute(
        _FETCH_SCHEMA_ROOT_SIGNATURES_SQL,
        (
            WORKSPACE_ID,
            artifact_id,
        ),
    ).fetchall()
    previous_rows = connection.execute(
        _FETCH_SCHEMA_ROOT_SIGNATURES_SQL,
        (
            WORKSPACE_ID,
            previous_artifact_id,
        ),
    ).fetchall()
    overlap = _dataset_signature_overlap(
        current_rows=current_rows,
        previous_rows=previous_rows,
    )
    if overlap is not None:
        root_path, dataset_hash, observed_records = overlap
        return {
            "code": "PAGINATION_DUPLICATE_PAGE",
            "message": (
                "Current page appears to repeat the previous page for this "
                "tool. The cursor may be invalid or ignored by the upstream."
            ),
            "cursor_param": cursor_param_name,
            "cursor_value": cursor_value,
            "previous_artifact_id": previous_artifact_id,
            "match_method": "schema_dataset_hash",
            "duplicate_root_path": root_path,
            "duplicate_dataset_hash": dataset_hash,
            "duplicate_observed_records": observed_records,
        }

    current_signature = _payload_collection_signature_from_envelope(
        current_envelope_payload
    )
    if blobs_payload_dir is None:
        return None
    previous_envelope_row = connection.execute(
        _FETCH_ARTIFACT_ENVELOPE_SQL,
        (WORKSPACE_ID, previous_artifact_id),
    ).fetchone()
    previous_envelope_payload = _load_envelope_payload_from_row(
        previous_envelope_row,
        blobs_payload_dir=blobs_payload_dir,
    )
    previous_signature = _payload_collection_signature_from_envelope(
        previous_envelope_payload
    )
    if current_signature is None or previous_signature is None:
        return None
    current_root_path, current_dataset_hash, current_records = current_signature
    previous_root_path, previous_dataset_hash, previous_records = (
        previous_signature
    )
    if current_dataset_hash != previous_dataset_hash:
        return None
    return {
        "code": "PAGINATION_DUPLICATE_PAGE",
        "message": (
            "Current page appears to repeat the previous page for this tool. "
            "The cursor may be invalid or ignored by the upstream."
        ),
        "cursor_param": cursor_param_name,
        "cursor_value": cursor_value,
        "previous_artifact_id": previous_artifact_id,
        "match_method": "payload_collection_hash",
        "duplicate_root_path": current_root_path,
        "duplicate_previous_root_path": previous_root_path,
        "duplicate_dataset_hash": current_dataset_hash,
        "duplicate_observed_records": current_records,
        "duplicate_previous_observed_records": previous_records,
    }


def _has_more_signal_detected(assessment: PaginationAssessment) -> bool:
    """Return whether upstream signaled more pages even if not continuable."""
    if assessment.has_more:
        return True
    return (
        assessment.partial_reason == UPSTREAM_PARTIAL_REASON_NEXT_TOKEN_MISSING
    )


def _pagination_capability_payload(
    assessment: PaginationAssessment,
) -> dict[str, Any]:
    """Build a compact capability summary for pagination handling."""
    has_next_params = bool(
        assessment.state is not None and assessment.state.next_params
    )
    return {
        "has_more_signal_detected": _has_more_signal_detected(assessment),
        "continuable": assessment.state is not None,
        "next_params_detected": has_next_params,
    }


def _has_duplicate_page_warning(
    extra_warnings: list[dict[str, Any]] | None,
) -> bool:
    """Return True when warnings include duplicate-page detection."""
    if not isinstance(extra_warnings, list):
        return False
    for warning in extra_warnings:
        if not isinstance(warning, dict):
            continue
        code = warning.get("code")
        if (
            isinstance(code, str)
            and code == _PAGINATION_DUPLICATE_PAGE_WARNING_CODE
        ):
            return True
    return False


def _build_cardinality_summary(
    *,
    json_value: Any,
    sample_root_path: str | None,
    sample_root_count: int | None,
) -> dict[str, Any]:
    """Build compact payload cardinality hints for schema-ref responses."""
    summary: dict[str, Any] = {}
    if isinstance(json_value, list):
        summary["root_type"] = "array"
        summary["root_count"] = len(json_value)
    elif isinstance(json_value, dict):
        summary["root_type"] = "object"
        summary["top_level_keys"] = sorted(str(key) for key in json_value)[:20]
        array_counts = {
            str(key): len(value)
            for key, value in json_value.items()
            if isinstance(value, list)
        }
        if array_counts:
            summary["top_level_array_counts"] = array_counts
    if isinstance(sample_root_path, str) and sample_root_path:
        summary["sample_root_path"] = sample_root_path
    if isinstance(sample_root_count, int):
        summary["sample_root_count"] = sample_root_count
    return summary


def _representative_schema_ref_sample(
    *,
    payload_for_full: dict[str, Any],
    schemas: list[dict[str, Any]],
    max_jsonpath_length: int,
    max_path_segments: int,
    max_wildcard_expansion_total: int,
) -> tuple[dict[str, Any] | None, str | None, int | None]:
    """Build representative sample for mirrored schema-ref responses."""
    resolved_json = first_queryable_json_from_payload(payload_for_full)
    if resolved_json is None:
        return None, None, None
    items, resolved_root_path = resolve_item_sequence_with_path(
        resolved_json.value,
        root_path=schema_primary_root_path(schemas),
        max_jsonpath_length=max_jsonpath_length,
        max_path_segments=max_path_segments,
        max_wildcard_expansion_total=max_wildcard_expansion_total,
    )
    if items is None:
        return None, resolved_root_path, None
    return (
        build_representative_item_sample(items),
        resolved_root_path,
        len(items),
    )


def _inject_pagination_state(
    envelope: Envelope,
    upstream_config: Any,
    forwarded_args: dict[str, Any],
    upstream_prefix: str,
    page_number: int = 0,
) -> tuple[Envelope, PaginationAssessment | None]:
    """Extract pagination signals and inject state into meta.

    Inspects the first queryable JSON content part in the envelope for
    pagination indicators using the upstream's pagination config.
    When a next page is detected, creates a new envelope with
    the pagination state stored in ``meta["_gateway_pagination"]``.

    Args:
        envelope: The envelope from the upstream response.
        upstream_config: The upstream's MCP ``UpstreamConfig``.
        forwarded_args: Original tool arguments (reserved keys
            already stripped).
        upstream_prefix: Upstream namespace prefix.
        page_number: Zero-based page number of this response.

    Returns:
        A ``(envelope, assessment)`` tuple.  The envelope may
        be replaced if pagination state was injected.  The
        assessment is ``None`` when no actionable pagination
        evidence is found.
    """
    # In the MCP gateway flow, pagination strategy comes from
    # per-upstream config. Discovery remains inside assess_pagination
    # for unconfigured upstreams.
    pagination_config = upstream_config.pagination
    assessment: PaginationAssessment | None

    if envelope.status == "error":
        if pagination_config is None and page_number == 0:
            return envelope, None
        assessment = PaginationAssessment(
            state=None,
            has_more=False,
            retrieval_status=RETRIEVAL_STATUS_PARTIAL,
            partial_reason=UPSTREAM_PARTIAL_REASON_SIGNAL_INCONCLUSIVE,
            warning=PAGINATION_WARNING_INCOMPLETE_RESULT_SET,
            page_number=page_number,
        )
        return envelope, assessment

    resolved_json = first_queryable_json_from_envelope(envelope)
    json_value = resolved_json.value if resolved_json is not None else None

    if json_value is None:
        if pagination_config is None and page_number == 0:
            return envelope, None
        assessment = PaginationAssessment(
            state=None,
            has_more=False,
            retrieval_status=RETRIEVAL_STATUS_PARTIAL,
            partial_reason=UPSTREAM_PARTIAL_REASON_SIGNAL_INCONCLUSIVE,
            warning=PAGINATION_WARNING_INCOMPLETE_RESULT_SET,
            page_number=page_number,
        )
        return envelope, assessment

    assessment = assess_pagination(
        json_value=json_value,
        pagination_config=pagination_config,
        original_args=forwarded_args,
        upstream_prefix=upstream_prefix,
        tool_name=envelope.tool,
        upstream_meta=envelope.meta.get("upstream_meta"),
        page_number=page_number,
    )
    if assessment is None:
        return envelope, None
    if assessment.state is None:
        return envelope, assessment

    new_meta = {
        **envelope.meta,
        "_gateway_pagination": assessment.state.to_dict(),
    }
    return dataclasses.replace(envelope, meta=new_meta), assessment


def _pagination_response_meta(
    assessment: PaginationAssessment,
    artifact_id: str,
    *,
    extra_warnings: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build pagination metadata for a gateway tool response.

    Args:
        assessment: Canonical pagination assessment.
        artifact_id: The artifact ID for this page.
        extra_warnings: Additional structured warnings to expose
            in the pagination payload.

    Returns:
        Dict with pagination info for the LLM.
    """
    treat_duplicate_as_terminal = (
        assessment.partial_reason == UPSTREAM_PARTIAL_REASON_NEXT_TOKEN_MISSING
        and _has_duplicate_page_warning(extra_warnings)
    )
    retrieval_status = assessment.retrieval_status
    has_more = assessment.has_more
    partial_reason = assessment.partial_reason
    warning = assessment.warning
    if treat_duplicate_as_terminal:
        retrieval_status = RETRIEVAL_STATUS_COMPLETE
        has_more = False
        partial_reason = None
        # Avoid contradictory "incomplete" warning when duplicate-page
        # detection proves a non-advancing cursor loop.
        warning = None

    next_kind: UpstreamNextKind | None = (
        "tool_call" if has_more and assessment.state is not None else None
    )
    page_number = assessment.page_number
    meta = build_upstream_pagination_meta(
        artifact_id=artifact_id,
        page_number=page_number,
        retrieval_status=retrieval_status,
        has_more=has_more,
        partial_reason=partial_reason,
        warning=warning,
        next_kind=next_kind,
        next_params=(
            assessment.state.next_params
            if assessment.state is not None
            else None
        ),
        original_args=(
            assessment.state.original_args
            if assessment.state is not None
            else None
        ),
        extra_warnings=extra_warnings,
    )
    meta["capability"] = _pagination_capability_payload(assessment)
    detector = assessment.detector
    if isinstance(detector, dict) and detector:
        meta["detector"] = detector
    return meta
