"""Handle invocations of mirrored upstream tools.

Orchestrate the full lifecycle for a proxied tool call: validate
gateway context, call the upstream, persist the artifact envelope,
and trigger mapping.  Exports ``handle_mirrored_tool``.
"""

from __future__ import annotations

import dataclasses
import json
import sqlite3
from typing import TYPE_CHECKING, Any

from sift_gateway.artifacts.create import (
    CreateArtifactInput,
    persist_artifact,
)
from sift_gateway.canon.rfc8785 import canonical_bytes, coerce_floats
from sift_gateway.constants import WORKSPACE_ID
from sift_gateway.envelope.content_extract import (
    first_queryable_json_from_envelope,
    first_queryable_json_from_payload,
)
from sift_gateway.envelope.model import (
    Envelope,
)
from sift_gateway.envelope.normalize import normalize_envelope
from sift_gateway.envelope.responses import (
    gateway_error,
    gateway_tool_result,
    select_response_mode,
)
from sift_gateway.mcp.handlers.common import (
    row_to_dict,
    rows_to_dicts,
)
from sift_gateway.mcp.handlers.schema_payload import build_schema_payload
from sift_gateway.mcp.mirror import (
    MirroredTool,
    extract_gateway_context,
    strip_reserved_gateway_args,
    validate_against_schema,
)
from sift_gateway.mcp.upstream_errors import (
    classify_upstream_exception,
)
from sift_gateway.obs.logging import get_logger
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
from sift_gateway.request_identity import compute_request_identity
from sift_gateway.response_sample import (
    build_representative_item_sample,
    resolve_item_sequence_with_path,
)
from sift_gateway.storage.payload_store import reconstruct_envelope
from sift_gateway.tools.artifact_describe import (
    FETCH_DESCRIBE_SQL,
    FETCH_SCHEMA_ROOTS_SQL,
    build_describe_response,
)
from sift_gateway.tools.artifact_schema import FETCH_SCHEMA_FIELDS_SQL
from sift_gateway.tools.usage_hint import (
    build_code_query_usage,
    schema_primary_root_path,
)
from sift_gateway.util.hashing import sha256_hex

if TYPE_CHECKING:
    from sift_gateway.mcp.server import GatewayServer


_DB_CONNECTIVITY_ERRORS: tuple[type[BaseException], ...] = (
    sqlite3.OperationalError,
    sqlite3.InterfaceError,
)


@dataclasses.dataclass(frozen=True)
class _MirroredInvocation:
    """Validated mirrored-tool invocation metadata."""

    session_id: str
    parent_artifact_id: str | None
    chain_seq: int | None
    forwarded_args: dict[str, Any]


@dataclasses.dataclass(frozen=True)
class _PersistDescribeResult:
    """Outcome of artifact persistence + inline describe flow."""

    handle: Any
    describe: dict[str, Any]
    pagination_warnings: list[dict[str, Any]]


def _extract_session_id(context: dict[str, Any] | None) -> str | None:
    """Extract a non-empty session ID from the gateway context.

    Args:
        context: Gateway context dict, or ``None``.

    Returns:
        The session ID string, or ``None`` if absent or empty.
    """
    if context is None:
        return None
    session_id = context.get("session_id")
    if isinstance(session_id, str) and session_id:
        return session_id
    return None


def _json_size_bytes(payload: Any) -> int:
    """Return UTF-8 byte size of a JSON-serializable payload.

    Raises:
        ValueError: If payload cannot be represented as valid UTF-8 JSON.
    """
    try:
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        msg = "arguments must be valid UTF-8 JSON"
        raise ValueError(msg) from exc
    return len(encoded)


def _truncate_error_text(text: str, max_bytes: int) -> str:
    """Truncate text to at most ``max_bytes`` UTF-8 bytes."""
    if max_bytes <= 0:
        return ""
    raw = text.encode("utf-8", errors="replace")
    if len(raw) <= max_bytes:
        return text

    suffix = " [truncated]"
    suffix_raw = suffix.encode("utf-8")
    head_budget = max_bytes - len(suffix_raw)
    if head_budget <= 0:
        return raw[:max_bytes].decode("utf-8", errors="ignore")

    head_raw = raw[:head_budget]
    while head_raw:
        try:
            head = head_raw.decode("utf-8")
            return f"{head}{suffix}"
        except UnicodeDecodeError:
            head_raw = head_raw[:-1]
    return suffix if len(suffix_raw) <= max_bytes else ""


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

_logger = get_logger(component="mcp.handlers")

_FETCH_PREVIOUS_PAGE_SQL = """
SELECT artifact_id, payload_hash_full
FROM artifacts
WHERE workspace_id = %s
  AND session_id = %s
  AND source_tool = %s
  AND deleted_at IS NULL
  AND created_seq < %s
ORDER BY created_seq DESC
LIMIT 1
"""

_FETCH_SCHEMA_ROOT_SIGNATURES_SQL = """
SELECT root_path, dataset_hash, observed_records
FROM artifact_schema_roots
WHERE workspace_id = %s
  AND artifact_id = %s
"""

_FETCH_ARTIFACT_ENVELOPE_SQL = """
SELECT pb.envelope,
       pb.envelope_canonical_encoding,
       pb.payload_fs_path,
       pb.payload_hash_full
FROM artifacts a
JOIN payload_blobs pb ON pb.workspace_id = a.workspace_id
    AND pb.payload_hash_full = a.payload_hash_full
WHERE a.workspace_id = %s
  AND a.artifact_id = %s
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


def _has_more_signal_detected(assessment: PaginationAssessment) -> bool:
    """Return whether upstream signaled more pages even if not continuable."""
    if assessment.has_more:
        return True
    return assessment.partial_reason == UPSTREAM_PARTIAL_REASON_NEXT_TOKEN_MISSING


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
        summary["top_level_keys"] = sorted(str(key) for key in json_value)[
            :20
        ]
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
        "tool_call"
        if has_more and assessment.state is not None
        else None
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


def _preflight_mirrored_gateway(ctx: GatewayServer) -> dict[str, Any] | None:
    """Validate gateway health before accepting mirrored calls."""
    # Probe before refusing: the failure that latched db_ok=False may
    # have been transient.
    if (
        ctx.db_pool is not None
        and not ctx.db_ok
        and not ctx._probe_db_recovery()
    ):
        return gateway_error(
            "INTERNAL",
            "gateway database is unhealthy; cannot create artifact",
        )
    if not ctx.fs_ok:
        return gateway_error(
            "INTERNAL",
            "gateway filesystem is unhealthy; cannot create artifact",
        )
    return None


def _extract_invocation_context(
    arguments: dict[str, Any],
) -> tuple[tuple[str, str | None, int | None] | None, dict[str, Any] | None]:
    """Extract session/parent/chain fields from raw invocation args."""
    context = extract_gateway_context(arguments)
    session_id = _extract_session_id(context)
    if session_id is None:
        return None, gateway_error(
            "INVALID_ARGUMENT",
            "missing _gateway_context.session_id",
        )

    parent_artifact_id = arguments.get("_gateway_parent_artifact_id")
    if parent_artifact_id is not None and not isinstance(
        parent_artifact_id, str
    ):
        return None, gateway_error(
            "INVALID_ARGUMENT",
            "_gateway_parent_artifact_id must be a string when provided",
        )

    chain_seq = arguments.get("_gateway_chain_seq")
    if chain_seq is not None and (
        not isinstance(chain_seq, int) or chain_seq < 0
    ):
        return None, gateway_error(
            "INVALID_ARGUMENT",
            "_gateway_chain_seq must be a non-negative integer when provided",
        )

    return (session_id, parent_artifact_id, chain_seq), None


def _validate_forwarded_args(
    *,
    ctx: GatewayServer,
    mirrored: MirroredTool,
    arguments: dict[str, Any],
    forwarded_args: dict[str, Any],
) -> dict[str, Any] | None:
    """Validate request size/schema/cursor constraints."""
    try:
        inbound_bytes = _json_size_bytes(arguments)
    except ValueError:
        return gateway_error(
            "INVALID_ARGUMENT",
            "arguments must be valid UTF-8 JSON",
        )

    if inbound_bytes > ctx.config.max_inbound_request_bytes:
        return gateway_error(
            "INVALID_ARGUMENT",
            "arguments exceed max_inbound_request_bytes",
            details={
                "max_inbound_request_bytes": (
                    ctx.config.max_inbound_request_bytes
                ),
                "actual_bytes": inbound_bytes,
            },
        )

    violations = validate_against_schema(
        forwarded_args,
        mirrored.upstream_tool.input_schema,
    )
    if violations:
        return gateway_error(
            "INVALID_ARGUMENT",
            "arguments failed upstream schema validation",
            details={"violations": violations},
        )

    return _validate_cursor_argument(
        forwarded_args=forwarded_args,
        pagination_config=mirrored.upstream.config.pagination,
    )


def _parse_mirrored_invocation(
    *,
    ctx: GatewayServer,
    mirrored: MirroredTool,
    arguments: dict[str, Any],
) -> tuple[_MirroredInvocation | None, dict[str, Any] | None]:
    """Build validated invocation payload used by the handler."""
    context_fields, context_error = _extract_invocation_context(arguments)
    if context_error is not None:
        return None, context_error
    assert context_fields is not None
    session_id, parent_artifact_id, chain_seq = context_fields

    forwarded_args = strip_reserved_gateway_args(arguments)
    validation_error = _validate_forwarded_args(
        ctx=ctx,
        mirrored=mirrored,
        arguments=arguments,
        forwarded_args=forwarded_args,
    )
    if validation_error is not None:
        return None, validation_error

    return (
        _MirroredInvocation(
            session_id=session_id,
            parent_artifact_id=parent_artifact_id,
            chain_seq=chain_seq,
            forwarded_args=forwarded_args,
        ),
        None,
    )


def _create_artifact_input(
    *,
    ctx: GatewayServer,
    invocation: _MirroredInvocation,
    mirrored: MirroredTool,
    identity: Any,
    envelope: Envelope,
) -> CreateArtifactInput:
    """Create artifact persistence input for the current envelope."""
    runtime_provenance = ctx._runtime_provenance()
    capture_origin = {
        "prefix": mirrored.prefix,
        "tool": mirrored.original_name,
        "upstream_instance_id": mirrored.upstream.instance_id,
        "runtime": runtime_provenance,
    }
    return CreateArtifactInput(
        session_id=invocation.session_id,
        upstream_instance_id=mirrored.upstream.instance_id,
        prefix=mirrored.prefix,
        tool_name=mirrored.original_name,
        request_key=identity.request_key,
        request_args_hash=identity.request_args_hash,
        request_args_prefix=identity.request_args_prefix,
        upstream_tool_schema_hash=mirrored.upstream_tool.schema_hash,
        envelope=envelope,
        parent_artifact_id=invocation.parent_artifact_id,
        chain_seq=invocation.chain_seq,
        capture_origin=capture_origin,
        runtime_provenance=runtime_provenance,
    )


def _resolve_upstream_args(
    *,
    ctx: GatewayServer,
    forwarded_args: dict[str, Any],
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Resolve artifact references before sending args upstream."""
    from sift_gateway.mcp.resolve_refs import (
        ResolveError,
        resolve_artifact_refs,
    )

    upstream_args = forwarded_args
    try:
        with ctx.db_pool.connection() as resolve_conn:
            resolved = resolve_artifact_refs(
                resolve_conn,
                forwarded_args,
                blobs_payload_dir=ctx.config.blobs_payload_dir,
            )
            if isinstance(resolved, ResolveError):
                return None, gateway_error(resolved.code, resolved.message)
            upstream_args = resolved
    except _DB_CONNECTIVITY_ERRORS:
        ctx.db_ok = False
        return None, gateway_error(
            "INTERNAL",
            "artifact ref resolution failed; gateway marked unhealthy",
        )
    except Exception:
        _logger.warning(
            "artifact ref resolution failed",
            exc_info=True,
        )
        return None, gateway_error(
            "INTERNAL",
            "artifact ref resolution failed",
        )
    return upstream_args, None


async def _call_upstream_with_fallback(
    *,
    ctx: GatewayServer,
    mirrored: MirroredTool,
    upstream_args: dict[str, Any],
) -> dict[str, Any]:
    """Call upstream and normalize transport/runtime failures to payloads."""
    try:
        return await ctx._call_upstream_with_metrics(
            mirrored=mirrored,
            forwarded_args=upstream_args,
        )
    except Exception as exc:
        error_code = classify_upstream_exception(exc)
        error_text = _truncate_error_text(
            str(exc), ctx.config.max_upstream_error_capture_bytes
        )
        return {
            "content": [{"type": "text", "text": error_text}],
            "structuredContent": None,
            "isError": True,
            "meta": {
                "exception_type": type(exc).__name__,
                "gateway_error_code": error_code,
                "gateway_error_detail": error_text,
            },
        }


def _envelope_from_upstream_result(
    *,
    ctx: GatewayServer,
    mirrored: MirroredTool,
    upstream_result: dict[str, Any],
) -> tuple[tuple[Envelope, list[Any]] | None, dict[str, Any] | None]:
    """Build envelope from upstream result with gateway error mapping."""
    try:
        envelope, binary_refs = ctx._envelope_from_upstream_result(
            mirrored=mirrored,
            upstream_result=upstream_result,
        )
    except ValueError as exc:
        return None, gateway_error(
            "UPSTREAM_RESPONSE_INVALID",
            str(exc),
        )
    return (envelope, binary_refs), None


def _sanitize_envelope_payload(
    *,
    ctx: GatewayServer,
    envelope: Envelope,
) -> Envelope:
    """Redact envelope payload values while preserving envelope shape.

    Raises:
        ValueError: If redaction fails or produces an invalid payload shape.
    """
    raw_payload = envelope.to_dict()
    preserved_pagination_state: dict[str, Any] | None = None
    raw_meta = raw_payload.get("meta")
    if isinstance(raw_meta, dict):
        raw_pagination_state = raw_meta.get("_gateway_pagination")
        if isinstance(raw_pagination_state, dict):
            preserved_pagination_state = dict(raw_pagination_state)

    sanitized_wrapper = ctx._sanitize_tool_result({"payload": raw_payload})
    if (
        not isinstance(sanitized_wrapper, dict)
        or sanitized_wrapper.get("type") == "gateway_error"
    ):
        raise ValueError("response redaction failed")
    payload = sanitized_wrapper.get("payload")
    if not isinstance(payload, dict):
        raise ValueError("response redaction failed")
    raw_content = payload.get("content")
    if not isinstance(raw_content, list):
        raise ValueError("response redaction failed")

    raw_error = payload.get("error")
    error = raw_error if isinstance(raw_error, dict) else None
    raw_meta = payload.get("meta")
    meta = raw_meta if isinstance(raw_meta, dict) else envelope.meta
    if preserved_pagination_state is not None:
        meta = dict(envelope.meta) if not isinstance(meta, dict) else dict(meta)
        # Keep continuation state exact for artifact(action="next_page").
        meta["_gateway_pagination"] = preserved_pagination_state
    try:
        return normalize_envelope(
            upstream_instance_id=str(
                payload.get(
                    "upstream_instance_id", envelope.upstream_instance_id
                )
            ),
            upstream_prefix=str(
                payload.get("upstream_prefix", envelope.upstream_prefix)
            ),
            tool=str(payload.get("tool", envelope.tool)),
            status=str(payload.get("status", envelope.status)),
            content=[part for part in raw_content if isinstance(part, dict)],
            error=error,
            meta=meta,
        )
    except Exception as exc:
        raise ValueError("response redaction failed") from exc


def _run_inline_describe_with_fallback(
    *,
    connection: Any,
    artifact_id: str,
) -> dict[str, Any]:
    """Run inline describe; degrade to minimal describe on error."""
    try:
        return _fetch_inline_describe(connection, artifact_id)
    except Exception as exc:
        _logger.warning(
            "inline describe failed; returning minimal describe",
            artifact_id=artifact_id,
            error_type=type(exc).__name__,
            exc_info=True,
        )
        return _minimal_describe(
            artifact_id,
        )


def _normalize_describe_payload(
    *,
    artifact_id: str,
    describe: dict[str, Any],
) -> dict[str, Any]:
    """Normalize non-ready describe payloads to best-effort output."""
    if _describe_has_ready_schema(describe):
        return describe

    map_status = describe.get("mapping", {}).get("map_status")
    schemas = describe.get("schemas")
    has_schemas = isinstance(schemas, list) and bool(schemas)
    if map_status == "ready" and not has_schemas:
        describe = _minimal_describe(
            artifact_id,
        )
        map_status = describe.get("mapping", {}).get("map_status")
        has_schemas = False

    _logger.warning(
        "schema-first inline describe not ready; returning best-effort payload",
        artifact_id=artifact_id,
        map_status=map_status,
        has_schemas=has_schemas,
    )
    return describe


def _safe_duplicate_page_warning(
    *,
    connection: Any,
    handle: Any,
    invocation: _MirroredInvocation,
    mirrored: MirroredTool,
    forwarded_args: dict[str, Any],
    current_envelope_payload: dict[str, Any],
    blobs_payload_dir: Any,
    pagination_assessment: PaginationAssessment | None,
) -> dict[str, Any] | None:
    """Return duplicate-page warning; swallow best-effort failures."""
    if pagination_assessment is None:
        return None
    try:
        return _detect_duplicate_page_warning(
            connection=connection,
            artifact_id=handle.artifact_id,
            payload_hash_full=handle.payload_hash_full,
            created_seq=handle.created_seq,
            session_id=invocation.session_id,
            source_tool=handle.source_tool,
            forwarded_args=forwarded_args,
            pagination_config=mirrored.upstream.config.pagination,
            current_envelope_payload=current_envelope_payload,
            blobs_payload_dir=blobs_payload_dir,
        )
    except Exception:
        _logger.warning(
            "duplicate-page pagination check failed",
            artifact_id=handle.artifact_id,
            exc_info=True,
        )
        return None


def _persist_and_describe(
    *,
    ctx: GatewayServer,
    invocation: _MirroredInvocation,
    mirrored: MirroredTool,
    identity: Any,
    envelope: Envelope,
    binary_refs: list[Any],
    pagination_assessment: PaginationAssessment | None,
    forwarded_args: dict[str, Any],
) -> tuple[_PersistDescribeResult | None, dict[str, Any] | None]:
    """Persist artifact, run inline mapping, and produce describe payload."""
    stage = "persist_artifact"
    try:
        with ctx.db_pool.connection() as connection:
            binary_hashes = ctx._binary_hashes_from_envelope(envelope)
            handle = persist_artifact(
                connection=connection,
                config=ctx.config,
                input_data=_create_artifact_input(
                    ctx=ctx,
                    invocation=invocation,
                    mirrored=mirrored,
                    identity=identity,
                    envelope=envelope,
                ),
                binary_hashes=binary_hashes,
                binary_refs=binary_refs or None,
            )

            stage = "run_mapping_inline"
            mapped = ctx._run_mapping_inline(
                connection,
                handle=handle,
                envelope=envelope,
            )
            if not mapped:
                return None, gateway_error(
                    "INTERNAL",
                    "mapping did not complete for artifact",
                )

            stage = "fetch_inline_describe"
            describe = _run_inline_describe_with_fallback(
                connection=connection,
                artifact_id=handle.artifact_id,
            )
            describe = _normalize_describe_payload(
                artifact_id=handle.artifact_id,
                describe=describe,
            )

            pagination_warnings: list[dict[str, Any]] = []
            duplicate_warning = _safe_duplicate_page_warning(
                connection=connection,
                handle=handle,
                invocation=invocation,
                mirrored=mirrored,
                forwarded_args=forwarded_args,
                current_envelope_payload=envelope.to_dict(),
                blobs_payload_dir=ctx.config.blobs_payload_dir,
                pagination_assessment=pagination_assessment,
            )
            if duplicate_warning is not None:
                pagination_warnings.append(duplicate_warning)

            return (
                _PersistDescribeResult(
                    handle=handle,
                    describe=describe,
                    pagination_warnings=pagination_warnings,
                ),
                None,
            )
    except _DB_CONNECTIVITY_ERRORS:
        ctx.db_ok = False
        return None, gateway_error(
            "INTERNAL",
            "artifact persistence failed; gateway marked unhealthy",
        )
    except Exception as exc:
        _logger.warning(
            "artifact persistence flow failed",
            stage=stage,
            error_type=type(exc).__name__,
            exc_info=True,
        )
        return None, gateway_error(
            "INTERNAL",
            "artifact persistence failed",
            details={
                "stage": stage,
                "error_type": type(exc).__name__,
            },
        )


async def handle_mirrored_tool(
    ctx: GatewayServer,
    mirrored: MirroredTool,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Handle a mirrored upstream tool invocation.

    Orchestrates the full lifecycle: validate context, call
    the upstream, persist the artifact envelope, and trigger
    mapping.

    Args:
        ctx: Gateway server with DB pool, blob store, config,
            and metrics.
        mirrored: The mirrored tool descriptor identifying
            the upstream and schema.
        arguments: Raw tool arguments including reserved
            ``_gateway_*`` keys.

    Returns:
        A gateway tool result dict with ``artifact_id`` and
        request metadata, or a gateway error dict on failure.
    """
    health_error = _preflight_mirrored_gateway(ctx)
    if health_error is not None:
        return health_error

    invocation, invocation_error = _parse_mirrored_invocation(
        ctx=ctx,
        mirrored=mirrored,
        arguments=arguments,
    )
    if invocation_error is not None:
        return invocation_error
    assert invocation is not None

    if ctx.db_pool is None:
        return gateway_error(
            "NOT_IMPLEMENTED",
            "schema-first responses require database persistence",
        )

    forwarded_args = invocation.forwarded_args
    identity = compute_request_identity(
        upstream_instance_id=mirrored.upstream.instance_id,
        prefix=mirrored.prefix,
        tool_name=mirrored.original_name,
        forwarded_args=forwarded_args,
    )

    upstream_args, upstream_args_error = _resolve_upstream_args(
        ctx=ctx,
        forwarded_args=forwarded_args,
    )
    if upstream_args_error is not None:
        return upstream_args_error
    assert upstream_args is not None

    upstream_result = await _call_upstream_with_fallback(
        ctx=ctx,
        mirrored=mirrored,
        upstream_args=upstream_args,
    )
    envelope_result, envelope_error = _envelope_from_upstream_result(
        ctx=ctx,
        mirrored=mirrored,
        upstream_result=upstream_result,
    )
    if envelope_error is not None:
        return envelope_error
    assert envelope_result is not None
    envelope, binary_refs = envelope_result

    page_number = invocation.chain_seq or 0
    envelope, pagination_assessment = _inject_pagination_state(
        envelope,
        mirrored.upstream.config,
        forwarded_args,
        mirrored.prefix,
        page_number=page_number,
    )
    try:
        envelope = _sanitize_envelope_payload(
            ctx=ctx,
            envelope=envelope,
        )
    except ValueError:
        return gateway_error("INTERNAL", "response redaction failed")
    persist_result, persist_error = _persist_and_describe(
        ctx=ctx,
        invocation=invocation,
        mirrored=mirrored,
        identity=identity,
        envelope=envelope,
        binary_refs=binary_refs,
        pagination_assessment=pagination_assessment,
        forwarded_args=forwarded_args,
    )
    if persist_error is not None:
        return persist_error
    assert persist_result is not None

    pagination_meta = None
    if pagination_assessment is not None:
        pagination_meta = _pagination_response_meta(
            pagination_assessment,
            persist_result.handle.artifact_id,
            extra_warnings=persist_result.pagination_warnings,
        )
    schemas = _schema_payload_from_describe(persist_result.describe)
    artifact_id = persist_result.handle.artifact_id
    lineage: dict[str, Any] = {
        "scope": "single",
        "artifact_ids": [artifact_id],
    }
    if invocation.parent_artifact_id is not None:
        lineage["parent_artifact_id"] = invocation.parent_artifact_id
    if invocation.chain_seq is not None:
        lineage["chain_seq"] = invocation.chain_seq

    payload_for_full = envelope.to_dict()
    (
        representative_sample,
        sample_root_path,
        sample_root_count,
    ) = _representative_schema_ref_sample(
        payload_for_full=payload_for_full,
        schemas=schemas,
        max_jsonpath_length=ctx.config.max_jsonpath_length,
        max_path_segments=ctx.config.max_path_segments,
        max_wildcard_expansion_total=ctx.config.max_wildcard_expansion_total,
    )
    resolved_json = first_queryable_json_from_payload(payload_for_full)
    usage_root_path = schema_primary_root_path(schemas)
    queryable_roots = _queryable_root_paths_from_schemas(schemas)
    if not queryable_roots:
        queryable_roots = [usage_root_path]

    cardinality_summary: dict[str, Any] = {}
    if resolved_json is not None:
        cardinality_summary = _build_cardinality_summary(
            json_value=resolved_json.value,
            sample_root_path=sample_root_path,
            sample_root_count=sample_root_count,
        )

    metadata: dict[str, Any] = {
        "usage": build_code_query_usage(
            interface="mcp",
            artifact_id=artifact_id,
            root_path=usage_root_path,
            configured_roots=ctx.config.code_query_allowed_import_roots,
        ),
        "queryable_roots": queryable_roots,
    }
    if cardinality_summary:
        metadata["cardinality"] = cardinality_summary
    if resolved_json is not None:
        metadata["query_json_source"] = {
            "part_index": resolved_json.part_index,
            "part_type": resolved_json.part_type,
            "encoding": resolved_json.source_encoding,
        }

    full_payload = gateway_tool_result(
        response_mode="full",
        artifact_id=artifact_id,
        payload=payload_for_full,
        lineage=lineage,
        pagination=pagination_meta,
        metadata=metadata,
    )
    schema_ref_payload = gateway_tool_result(
        response_mode="schema_ref",
        artifact_id=artifact_id,
        schemas=schemas,
        lineage=lineage,
        pagination=pagination_meta,
        metadata=metadata,
    )
    if representative_sample is not None:
        schema_ref_payload.pop("schemas", None)
        schema_ref_payload.update(representative_sample)
    has_pagination = (
        pagination_meta is not None or invocation.parent_artifact_id is not None
    )
    response_mode = select_response_mode(
        has_pagination=has_pagination,
        full_payload=full_payload,
        schema_ref_payload=schema_ref_payload,
        max_bytes=ctx.config.passthrough_max_bytes,
    )
    if response_mode == "schema_ref":
        return schema_ref_payload
    return full_payload
