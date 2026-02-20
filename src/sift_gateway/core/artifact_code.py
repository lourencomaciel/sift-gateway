"""Protocol-agnostic artifact code-query execution service."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
import hashlib
from importlib.metadata import packages_distributions
import re
import sys
import time
from typing import Any, cast

from sift_gateway.canon.rfc8785 import canonical_bytes, coerce_floats
from sift_gateway.codegen.ast_guard import allowed_import_roots
from sift_gateway.codegen.runtime import (
    CODE_RUNTIME_CONTRACT_VERSION,
    CodeRuntimeConfig,
    CodeRuntimeError,
    CodeRuntimeInfrastructureError,
    CodeRuntimeMemoryLimitError,
    CodeRuntimeTimeoutError,
    encode_json_bytes,
    execute_code_in_subprocess,
)
from sift_gateway.constants import (
    TRAVERSAL_CONTRACT_VERSION,
    WORKSPACE_ID,
)
from sift_gateway.core.artifact_describe import execute_artifact_describe
from sift_gateway.core.artifact_get import ENVELOPE_COLUMNS
from sift_gateway.core.lineage_roots import (
    resolve_all_related_root_candidates,
    resolve_single_root_candidate,
)
from sift_gateway.core.retrieval_helpers import extract_json_target
from sift_gateway.core.query_scope import resolve_scope
from sift_gateway.core.rows import row_to_dict, rows_to_dicts
from sift_gateway.core.runtime import ArtifactCodeRuntime
from sift_gateway.core.schema_payload import build_schema_payload
from sift_gateway.envelope.responses import (
    gateway_error,
    gateway_tool_result,
    select_response_mode,
)
from sift_gateway.obs.logging import LogEvents, get_logger
from sift_gateway.query.jsonpath import JsonPathError, evaluate_jsonpath
from sift_gateway.schema_compact import SCHEMA_LEGEND, compact_schema_payload
from sift_gateway.storage.payload_store import reconstruct_envelope
from sift_gateway.tools.artifact_get import FETCH_ARTIFACT_SQL
from sift_gateway.tools.artifact_schema import FETCH_SCHEMA_FIELDS_SQL
from sift_gateway.tools.artifact_select import (
    FETCH_SAMPLES_SQL,
)

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

_logger = get_logger(component="artifact.codegen")
SAMPLE_COLUMNS = ["sample_index", "record", "record_bytes", "record_hash"]
_CodeCandidateRow = tuple[str, dict[str, Any], dict[str, Any], dict[str, Any]]


def _hash_text(value: str) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def _hash_json(value: Any) -> str:
    digest = hashlib.sha256(canonical_bytes(coerce_floats(value))).hexdigest()
    return f"sha256:{digest}"


_RE_NO_MODULE = re.compile(r"No module named '([^']+)'")
_RE_IMPORT_NOT_ALLOWED = re.compile(r"import not allowed: (\S+)")
_STDLIB_ROOTS = sys.stdlib_module_names

# Well-known module-to-distribution mappings for packages where
# the import root differs from the pip distribution name.  Used
# as a fallback when runtime metadata is unavailable (i.e. the
# package is not installed).
_MODULE_TO_DIST: dict[str, str] = {
    "PIL": "pillow",
    "attr": "attrs",
    "bs4": "beautifulsoup4",
    "cv2": "opencv-python",
    "dateutil": "python-dateutil",
    "dotenv": "python-dotenv",
    "skimage": "scikit-image",
    "sklearn": "scikit-learn",
    "yaml": "pyyaml",
}


def _module_to_dist(root: str) -> str:
    """Map an import root to its pip distribution name.

    Checks installed distribution metadata first, then falls
    back to a static mapping of well-known mismatches.

    Args:
        root: Top-level import root (e.g. ``"sklearn"``).

    Returns:
        Pip distribution name (e.g. ``"scikit-learn"``).
    """
    # Runtime lookup for installed packages.
    try:
        dists = packages_distributions().get(root)
        if dists:
            return dists[0]
    except Exception:
        pass
    # Static map for well-known mismatches.
    return _MODULE_TO_DIST.get(root, root)


def _enrich_install_hint(msg: str) -> str:
    """Append an agent-actionable install hint when possible."""
    m = _RE_NO_MODULE.search(msg)
    if m:
        root = m.group(1).split(".")[0]
        dist = _module_to_dist(root)
        return f"{msg}\nRun: sift-gateway install {dist}"
    m = _RE_IMPORT_NOT_ALLOWED.search(msg)
    if m:
        root = m.group(1).split(".")[0]
        # stdlib modules are policy-blocked, not missing —
        # suggesting install would be misleading.
        if root in _STDLIB_ROOTS:
            return msg
        dist = _module_to_dist(root)
        return f"{msg}\nRun: sift-gateway install {dist}"
    return msg


def _code_error(
    message: str,
    *,
    details_code: str,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"code": details_code}
    if details:
        payload.update(details)
    return gateway_error("INVALID_ARGUMENT", message, details=payload)


def _normalize_code_artifact_ids(
    arguments: dict[str, Any],
) -> tuple[list[str], dict[str, Any] | None]:
    raw_artifact_id = arguments.get("artifact_id")
    raw_artifact_ids = arguments.get("artifact_ids")

    if raw_artifact_ids is not None:
        if raw_artifact_id is not None:
            return [], gateway_error(
                "INVALID_ARGUMENT",
                "provide either artifact_id or artifact_ids, not both",
            )
        if not isinstance(raw_artifact_ids, list):
            return [], gateway_error(
                "INVALID_ARGUMENT",
                "artifact_ids must be a list",
            )
        if not raw_artifact_ids:
            return [], gateway_error(
                "INVALID_ARGUMENT",
                "artifact_ids cannot be empty",
            )
        normalized: list[str] = []
        seen: set[str] = set()
        for artifact_id in raw_artifact_ids:
            if not isinstance(artifact_id, str) or not artifact_id.strip():
                return [], gateway_error(
                    "INVALID_ARGUMENT",
                    "artifact_ids items must be non-empty strings",
                )
            if artifact_id not in seen:
                normalized.append(artifact_id)
                seen.add(artifact_id)
        return normalized, None

    if not isinstance(raw_artifact_id, str) or not raw_artifact_id.strip():
        return [], gateway_error(
            "INVALID_ARGUMENT",
            "missing artifact_id or artifact_ids",
        )
    return [raw_artifact_id], None


def _normalize_code_root_paths(
    arguments: dict[str, Any],
    *,
    artifact_ids: list[str],
) -> tuple[dict[str, str], dict[str, Any] | None]:
    raw_root_path = arguments.get("root_path")
    raw_root_paths = arguments.get("root_paths")

    if raw_root_paths is not None:
        if raw_root_path is not None:
            return {}, gateway_error(
                "INVALID_ARGUMENT",
                "provide either root_path or root_paths, not both",
            )
        if not isinstance(raw_root_paths, Mapping):
            return {}, gateway_error(
                "INVALID_ARGUMENT",
                "root_paths must be an object keyed by artifact id",
            )
        normalized: dict[str, str] = {}
        for artifact_id in artifact_ids:
            value = raw_root_paths.get(artifact_id)
            if not isinstance(value, str) or not value.strip():
                return {}, gateway_error(
                    "INVALID_ARGUMENT",
                    f"missing root_paths.{artifact_id}",
                )
            normalized[artifact_id] = value
        return normalized, None

    if not isinstance(raw_root_path, str) or not raw_root_path.strip():
        return {}, gateway_error(
            "INVALID_ARGUMENT",
            "missing root_path or root_paths",
        )
    return dict.fromkeys(artifact_ids, raw_root_path), None


@dataclass(frozen=True)
class _ParsedCodeArgs:
    """Normalized and validated inputs for code queries."""

    session_id: str
    scope: str
    artifact_ids: list[str]
    root_paths: dict[str, str]
    code: str
    params: dict[str, Any]


@dataclass(frozen=True)
class _CodeRequest:
    """Normalized request state for code queries."""

    session_id: str
    scope: str
    requested_artifact_ids: list[str]
    requested_root_paths: dict[str, str]
    anchor_artifact_id: str
    root_path: str
    root_path_log: str
    code: str
    params: dict[str, Any]
    code_hash: str
    params_hash: str


@dataclass
class _CodeCollectionState:
    """Mutable state accumulated during code-query input collection."""

    related_ids: list[str] = field(default_factory=list)
    related_set_hash: str = ""
    related_set_hashes: dict[str, str] = field(default_factory=dict)
    warnings: list[dict[str, Any]] = field(default_factory=list)
    sampled_artifacts: set[str] = field(default_factory=set)
    schema_obj: dict[str, Any] | None = None
    schema_hash: str = ""
    schema_by_artifact: dict[str, dict[str, Any]] = field(default_factory=dict)
    schema_hashes: dict[str, str] = field(default_factory=dict)
    input_records_by_artifact: dict[str, list[dict[str, Any]]] = field(
        default_factory=dict
    )
    input_count: int = 0
    input_bytes: int = 2
    input_limit_reason: str | None = None
    input_limit_value: int | None = None
    input_serialization_error: dict[str, Any] | None = None


@dataclass(frozen=True)
class _RequestedCodeCandidates:
    """Resolved candidate set for one requested code artifact."""

    candidate_rows: list[_CodeCandidateRow]
    missing_root_artifacts: list[str]
    related_ids: list[str]
    related_set_hash: str


def _resolve_code_request(
    parsed_args: _ParsedCodeArgs,
) -> tuple[_CodeRequest | None, dict[str, Any] | None]:
    """Build normalized code-query request state and stable hashes."""
    requested_artifact_ids = parsed_args.artifact_ids
    requested_root_paths = parsed_args.root_paths
    anchor_artifact_id = requested_artifact_ids[0]
    root_path = requested_root_paths[anchor_artifact_id]
    root_path_log = (
        root_path
        if len(set(requested_root_paths.values())) == 1
        else "<multi_root_paths>"
    )
    try:
        code_hash = _hash_text(parsed_args.code)
        params_hash = _hash_json(parsed_args.params)
    except (TypeError, ValueError) as exc:
        return None, gateway_error(
            "INVALID_ARGUMENT",
            f"invalid params: {exc}",
        )
    return (
        _CodeRequest(
            session_id=parsed_args.session_id,
            scope=parsed_args.scope,
            requested_artifact_ids=requested_artifact_ids,
            requested_root_paths=requested_root_paths,
            anchor_artifact_id=anchor_artifact_id,
            root_path=root_path,
            root_path_log=root_path_log,
            code=parsed_args.code,
            params=parsed_args.params,
            code_hash=code_hash,
            params_hash=params_hash,
        ),
        None,
    )


def _new_collection_state(request: _CodeRequest) -> _CodeCollectionState:
    """Initialize input collection state for requested artifacts."""
    state = _CodeCollectionState()
    state.input_records_by_artifact = {
        artifact_id: [] for artifact_id in request.requested_artifact_ids
    }
    return state


def _append_code_input_record(
    *,
    state: _CodeCollectionState,
    requested_artifact_id: str,
    record: Any,
    locator: dict[str, Any],
    max_input_records: int,
    max_input_bytes: int,
) -> bool:
    """Append one normalized input record unless limits are exceeded."""
    enriched = _with_locator(record, locator)
    next_count = state.input_count + 1
    if next_count > max_input_records:
        state.input_limit_reason = "records"
        state.input_limit_value = next_count
        return False
    try:
        record_bytes = len(encode_json_bytes(enriched))
    except Exception as exc:
        state.input_serialization_error = gateway_error(
            "INVALID_ARGUMENT",
            f"input serialization failed: {exc}",
            details={"code": "CODE_RUNTIME_EXCEPTION"},
        )
        return False
    next_bytes = (
        state.input_bytes + record_bytes + (1 if state.input_count else 0)
    )
    if next_bytes > max_input_bytes:
        state.input_limit_reason = "bytes"
        state.input_limit_value = next_bytes
        return False
    state.input_records_by_artifact[requested_artifact_id].append(enriched)
    state.input_count = next_count
    state.input_bytes = next_bytes
    return True


def _append_missing_root_warning(
    *,
    state: _CodeCollectionState,
    requested_artifact_id: str,
    requested_artifact_count: int,
    root_path_for_requested: str,
    missing_root_artifacts: list[str],
) -> None:
    """Append MISSING_ROOT_PATH warning when candidates skipped by root_path."""
    if not missing_root_artifacts:
        return
    warning: dict[str, Any] = {
        "code": "MISSING_ROOT_PATH",
        "root_path": root_path_for_requested,
        "skipped_artifacts": len(missing_root_artifacts),
        "artifact_ids": missing_root_artifacts,
    }
    if requested_artifact_count > 1:
        warning["anchor_artifact_id"] = requested_artifact_id
    state.warnings.append(warning)


def _load_code_schema_for_requested_artifact(
    *,
    connection: Any,
    candidate_rows: list[
        tuple[str, dict[str, Any], dict[str, Any], dict[str, Any]]
    ],
    fetch_schema_fields_sql: str,
) -> tuple[dict[str, Any] | None, str, dict[str, Any] | None]:
    """Load canonical schema payload for one requested artifact."""
    schema_artifact_id, _meta, schema_root_row, schema_root = candidate_rows[0]
    root_key = schema_root_row.get("root_key")
    if not isinstance(root_key, str):
        return None, "", gateway_error("INTERNAL", "schema root_key missing")
    field_rows = rows_to_dicts(
        connection.execute(
            fetch_schema_fields_sql,
            (WORKSPACE_ID, schema_artifact_id, root_key),
        ).fetchall(),
        _SCHEMA_FIELD_COLUMNS,
    )
    requested_schema = build_schema_payload(
        schema_root=schema_root,
        field_rows=field_rows,
    )
    schema_hash_raw = requested_schema.get("schema_hash")
    schema_hash = schema_hash_raw if isinstance(schema_hash_raw, str) else ""
    return requested_schema, schema_hash, None


def _reconstruct_code_envelope(
    artifact_row: dict[str, Any],
    *,
    blobs_payload_dir: Any,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Resolve envelope payload from inline JSONB or payload file."""
    envelope_value = artifact_row.get("envelope")
    payload_fs_path = artifact_row.get("payload_fs_path")
    if isinstance(envelope_value, dict) and "content" in envelope_value:
        return envelope_value, None
    if not isinstance(payload_fs_path, str) or not payload_fs_path:
        return None, gateway_error(
            "INTERNAL",
            "missing payload file path for artifact",
        )
    try:
        envelope = reconstruct_envelope(
            payload_fs_path=payload_fs_path,
            blobs_payload_dir=blobs_payload_dir,
            encoding=str(
                artifact_row.get("envelope_canonical_encoding", "none")
            ),
            expected_hash=str(artifact_row.get("payload_hash_full", "")),
        )
    except ValueError as exc:
        return None, gateway_error(
            "INTERNAL",
            f"envelope reconstruction failed: {exc}",
        )
    return envelope, None


def _collect_sample_candidate_records(
    *,
    runtime: ArtifactCodeRuntime,
    connection: Any,
    state: _CodeCollectionState,
    requested_artifact_id: str,
    requested_artifact_count: int,
    root_path_for_requested: str,
    artifact_id: str,
    root_row: dict[str, Any],
    fetch_samples_sql: str,
    max_input_records: int,
    max_input_bytes: int,
) -> tuple[bool, dict[str, Any] | None]:
    """Collect sampled rows for one candidate artifact."""
    state.sampled_artifacts.add(artifact_id)
    sample_rows = rows_to_dicts(
        connection.execute(
            fetch_samples_sql,
            (
                WORKSPACE_ID,
                artifact_id,
                root_row["root_key"],
            ),
        ).fetchall(),
        SAMPLE_COLUMNS,
    )
    corruption = runtime.check_sample_corruption(root_row, sample_rows)
    if corruption is not None:
        return False, corruption
    for sample in sample_rows:
        locator = {
            "artifact_id": artifact_id,
            "root_path": root_path_for_requested,
            "sample_index": sample.get("sample_index"),
        }
        if requested_artifact_count > 1:
            locator["requested_artifact_id"] = requested_artifact_id
        if not _append_code_input_record(
            state=state,
            requested_artifact_id=requested_artifact_id,
            record=sample.get("record"),
            locator=locator,
            max_input_records=max_input_records,
            max_input_bytes=max_input_bytes,
        ):
            return True, None
    return False, None


def _collect_envelope_candidate_records(
    *,
    runtime: ArtifactCodeRuntime,
    connection: Any,
    state: _CodeCollectionState,
    requested_artifact_id: str,
    requested_artifact_count: int,
    root_path_for_requested: str,
    artifact_id: str,
    fetch_artifact_sql: str,
    max_input_records: int,
    max_input_bytes: int,
) -> tuple[bool, dict[str, Any] | None]:
    """Collect envelope-derived rows for one candidate artifact."""
    artifact_row = row_to_dict(
        connection.execute(
            fetch_artifact_sql,
            (WORKSPACE_ID, artifact_id),
        ).fetchone(),
        ENVELOPE_COLUMNS,
    )
    if artifact_row is None:
        return False, None
    envelope, envelope_err = _reconstruct_code_envelope(
        artifact_row,
        blobs_payload_dir=runtime.blobs_payload_dir,
    )
    if envelope_err is not None:
        return False, envelope_err
    if envelope is None:
        return False, gateway_error("INTERNAL", "missing envelope")

    json_target = extract_json_target(
        envelope, artifact_row.get("mapped_part_index")
    )
    try:
        root_values = evaluate_jsonpath(
            json_target,
            root_path_for_requested,
            max_length=runtime.max_jsonpath_length,
            max_segments=runtime.max_path_segments,
            max_wildcard_expansion_total=runtime.max_wildcard_expansion_total,
        )
    except JsonPathError as exc:
        return False, gateway_error("INVALID_ARGUMENT", str(exc))

    if len(root_values) == 1 and isinstance(root_values[0], list):
        records: list[Any] = list(root_values[0])
    else:
        records = list(root_values)
    for index, record in enumerate(records):
        locator = {
            "artifact_id": artifact_id,
            "root_path": root_path_for_requested,
            "index": index,
        }
        if requested_artifact_count > 1:
            locator["requested_artifact_id"] = requested_artifact_id
        if not _append_code_input_record(
            state=state,
            requested_artifact_id=requested_artifact_id,
            record=record,
            locator=locator,
            max_input_records=max_input_records,
            max_input_bytes=max_input_bytes,
        ):
            return True, None
    return False, None


def _collect_requested_candidate_rows(
    *,
    runtime: ArtifactCodeRuntime,
    connection: Any,
    state: _CodeCollectionState,
    request: _CodeRequest,
    requested_artifact_id: str,
    root_path_for_requested: str,
    candidate_rows: list[
        tuple[str, dict[str, Any], dict[str, Any], dict[str, Any]]
    ],
    fetch_artifact_sql: str,
    fetch_samples_sql: str,
    max_input_records: int,
    max_input_bytes: int,
) -> tuple[bool, dict[str, Any] | None]:
    """Collect records from candidate rows for one requested artifact."""
    for artifact_id, artifact_meta, root_row, _schema in candidate_rows:
        map_kind = str(artifact_meta.get("map_kind", "none"))
        sampled_only = map_kind == "partial"
        if sampled_only:
            stop_collection, collect_err = _collect_sample_candidate_records(
                runtime=runtime,
                connection=connection,
                state=state,
                requested_artifact_id=requested_artifact_id,
                requested_artifact_count=len(request.requested_artifact_ids),
                root_path_for_requested=root_path_for_requested,
                artifact_id=artifact_id,
                root_row=root_row,
                fetch_samples_sql=fetch_samples_sql,
                max_input_records=max_input_records,
                max_input_bytes=max_input_bytes,
            )
        else:
            stop_collection, collect_err = _collect_envelope_candidate_records(
                runtime=runtime,
                connection=connection,
                state=state,
                requested_artifact_id=requested_artifact_id,
                requested_artifact_count=len(request.requested_artifact_ids),
                root_path_for_requested=root_path_for_requested,
                artifact_id=artifact_id,
                fetch_artifact_sql=fetch_artifact_sql,
                max_input_records=max_input_records,
                max_input_bytes=max_input_bytes,
            )
        if collect_err is not None:
            return False, collect_err
        if stop_collection:
            return True, None
    return False, None


def _touch_code_retrieval_artifacts(
    *,
    runtime: ArtifactCodeRuntime,
    connection: Any,
    session_id: str,
    related_ids: list[str],
) -> None:
    """Best-effort retrieval touch for collected related artifacts."""
    if not related_ids:
        return
    touched = runtime.safe_touch_for_retrieval_many(
        connection,
        session_id=session_id,
        artifact_ids=related_ids,
    )
    if not touched:
        return
    commit = getattr(connection, "commit", None)
    if callable(commit):
        commit()


def _collect_code_inputs(
    *,
    runtime: ArtifactCodeRuntime,
    request: _CodeRequest,
    fetch_artifact_sql: str,
    fetch_schema_fields_sql: str,
    fetch_samples_sql: str,
) -> tuple[_CodeCollectionState | None, dict[str, Any] | None]:
    """Collect schemas, lineage hashes, and runtime input records."""
    state = _new_collection_state(request)
    max_input_records = runtime.code_query_max_input_records
    max_input_bytes = runtime.code_query_max_input_bytes
    db_pool = runtime.db_pool
    if db_pool is None:
        return None, runtime.not_implemented("artifact.code")

    with db_pool.connection() as connection:
        for artifact_id in request.requested_artifact_ids:
            if not runtime.artifact_visible(
                connection,
                session_id=request.session_id,
                artifact_id=artifact_id,
            ):
                return None, gateway_error(
                    "NOT_FOUND", f"artifact not found: {artifact_id}"
                )

        all_related_ids: set[str] = set()
        for requested_artifact_id in request.requested_artifact_ids:
            stop_collection, collect_err = (
                _collect_code_inputs_for_requested_artifact(
                    runtime=runtime,
                    connection=connection,
                    state=state,
                    request=request,
                    requested_artifact_id=requested_artifact_id,
                    all_related_ids=all_related_ids,
                    fetch_artifact_sql=fetch_artifact_sql,
                    fetch_schema_fields_sql=fetch_schema_fields_sql,
                    fetch_samples_sql=fetch_samples_sql,
                    max_input_records=max_input_records,
                    max_input_bytes=max_input_bytes,
                )
            )
            if collect_err is not None:
                return None, collect_err
            if stop_collection:
                break

        state.related_ids = sorted(all_related_ids)
        _touch_code_retrieval_artifacts(
            runtime=runtime,
            connection=connection,
            session_id=request.session_id,
            related_ids=state.related_ids,
        )

    return state, None


def _collect_code_inputs_for_requested_artifact(
    *,
    runtime: ArtifactCodeRuntime,
    connection: Any,
    state: _CodeCollectionState,
    request: _CodeRequest,
    requested_artifact_id: str,
    all_related_ids: set[str],
    fetch_artifact_sql: str,
    fetch_schema_fields_sql: str,
    fetch_samples_sql: str,
    max_input_records: int,
    max_input_bytes: int,
) -> tuple[bool, dict[str, Any] | None]:
    """Collect schema and records for one requested artifact anchor."""
    root_path_for_requested = request.requested_root_paths[
        requested_artifact_id
    ]
    resolved_candidates, resolve_err = _resolve_requested_code_candidates(
        runtime=runtime,
        connection=connection,
        request=request,
        requested_artifact_id=requested_artifact_id,
        root_path_for_requested=root_path_for_requested,
    )
    if resolve_err is not None:
        return False, resolve_err
    resolved_candidates = cast(_RequestedCodeCandidates, resolved_candidates)

    all_related_ids.update(resolved_candidates.related_ids)
    _record_requested_code_lineage_state(
        state=state,
        request=request,
        requested_artifact_id=requested_artifact_id,
        root_path_for_requested=root_path_for_requested,
        resolved_candidates=resolved_candidates,
    )

    requested_schema, schema_hash, schema_err = (
        _load_code_schema_for_requested_artifact(
            connection=connection,
            candidate_rows=resolved_candidates.candidate_rows,
            fetch_schema_fields_sql=fetch_schema_fields_sql,
        )
    )
    if schema_err is not None:
        return False, schema_err
    if requested_schema is None:
        return False, gateway_error("INTERNAL", "schema resolution failed")
    _store_requested_code_schema(
        state=state,
        request=request,
        requested_artifact_id=requested_artifact_id,
        requested_schema=requested_schema,
        schema_hash=schema_hash,
    )

    stop_collection, collect_err = _collect_requested_candidate_rows(
        runtime=runtime,
        connection=connection,
        state=state,
        request=request,
        requested_artifact_id=requested_artifact_id,
        root_path_for_requested=root_path_for_requested,
        candidate_rows=resolved_candidates.candidate_rows,
        fetch_artifact_sql=fetch_artifact_sql,
        fetch_samples_sql=fetch_samples_sql,
        max_input_records=max_input_records,
        max_input_bytes=max_input_bytes,
    )
    if collect_err is not None:
        return False, collect_err
    if stop_collection:
        return True, None
    if state.input_limit_reason or state.input_serialization_error:
        return True, None
    return False, None


def _single_scope_related_set_hash(
    *,
    runtime: ArtifactCodeRuntime,
    requested_artifact_id: str,
    anchor_meta: dict[str, Any] | None,
) -> str:
    """Build related-set hash for scope=single code queries."""
    generation = None
    if isinstance(anchor_meta, dict):
        raw_generation = anchor_meta.get("generation")
        if isinstance(raw_generation, int):
            generation = raw_generation
    return runtime.compute_related_set_hash(
        [{"artifact_id": requested_artifact_id, "generation": generation}]
    )


def _resolve_requested_code_candidates(
    *,
    runtime: ArtifactCodeRuntime,
    connection: Any,
    request: _CodeRequest,
    requested_artifact_id: str,
    root_path_for_requested: str,
) -> tuple[_RequestedCodeCandidates | None, dict[str, Any] | None]:
    """Resolve candidate rows and related-set metadata for one request."""
    if request.scope == "single":
        resolved_single = resolve_single_root_candidate(
            connection,
            anchor_artifact_id=requested_artifact_id,
            root_path=root_path_for_requested,
        )
        if isinstance(resolved_single, dict):
            return None, resolved_single
        return (
            _RequestedCodeCandidates(
                candidate_rows=resolved_single.candidate_rows,
                missing_root_artifacts=resolved_single.missing_root_artifacts,
                related_ids=resolved_single.related_ids,
                related_set_hash=_single_scope_related_set_hash(
                    runtime=runtime,
                    requested_artifact_id=requested_artifact_id,
                    anchor_meta=resolved_single.anchor_meta,
                ),
            ),
            None,
        )

    resolved_candidates = resolve_all_related_root_candidates(
        connection,
        session_id=request.session_id,
        anchor_artifact_id=requested_artifact_id,
        root_path=root_path_for_requested,
        max_related_artifacts=runtime.related_query_max_artifacts,
        resolve_related_fn=runtime.resolve_related_artifacts,
        compute_related_set_hash_fn=runtime.compute_related_set_hash,
    )
    if isinstance(resolved_candidates, dict):
        return None, resolved_candidates
    return (
        _RequestedCodeCandidates(
            candidate_rows=resolved_candidates.candidate_rows,
            missing_root_artifacts=resolved_candidates.missing_root_artifacts,
            related_ids=resolved_candidates.related_ids,
            related_set_hash=resolved_candidates.related_set_hash,
        ),
        None,
    )


def _record_requested_code_lineage_state(
    *,
    state: _CodeCollectionState,
    request: _CodeRequest,
    requested_artifact_id: str,
    root_path_for_requested: str,
    resolved_candidates: _RequestedCodeCandidates,
) -> None:
    """Store related-set metadata and missing-root warnings for one request."""
    state.related_set_hashes[requested_artifact_id] = (
        resolved_candidates.related_set_hash
    )
    if requested_artifact_id == request.anchor_artifact_id:
        state.related_set_hash = resolved_candidates.related_set_hash
    _append_missing_root_warning(
        state=state,
        requested_artifact_id=requested_artifact_id,
        requested_artifact_count=len(request.requested_artifact_ids),
        root_path_for_requested=root_path_for_requested,
        missing_root_artifacts=resolved_candidates.missing_root_artifacts,
    )


def _store_requested_code_schema(
    *,
    state: _CodeCollectionState,
    request: _CodeRequest,
    requested_artifact_id: str,
    requested_schema: dict[str, Any],
    schema_hash: str,
) -> None:
    """Persist resolved schema payload and hashes for one request."""
    state.schema_by_artifact[requested_artifact_id] = requested_schema
    if schema_hash:
        state.schema_hashes[requested_artifact_id] = schema_hash
    if requested_artifact_id == request.anchor_artifact_id:
        state.schema_obj = requested_schema
        state.schema_hash = schema_hash


def _append_sampled_warning(state: _CodeCollectionState) -> None:
    """Append sampled mapping warning when sampled artifacts were used."""
    if not state.sampled_artifacts:
        return
    state.warnings.append(
        {
            "code": "SAMPLED_MAPPING_USED",
            "sampled_only": True,
            "artifact_ids": sorted(state.sampled_artifacts),
        }
    )


def _append_overlapping_dataset_warning(
    *,
    state: _CodeCollectionState,
    request: _CodeRequest,
) -> None:
    """Warn when requested artifacts resolve to identical dataset hashes."""
    if len(request.requested_artifact_ids) <= 1:
        return
    grouped: dict[str, list[str]] = {}
    for artifact_id in request.requested_artifact_ids:
        schema = state.schema_by_artifact.get(artifact_id)
        if not isinstance(schema, dict):
            continue
        determinism = schema.get("determinism")
        if not isinstance(determinism, dict):
            continue
        dataset_hash = determinism.get("dataset_hash")
        if not isinstance(dataset_hash, str) or not dataset_hash:
            continue
        grouped.setdefault(dataset_hash, []).append(artifact_id)

    overlaps = [
        {
            "dataset_hash": dataset_hash,
            "artifact_ids": artifact_ids,
        }
        for dataset_hash, artifact_ids in grouped.items()
        if len(artifact_ids) > 1
    ]
    if not overlaps:
        return
    state.warnings.append(
        {
            "code": "OVERLAPPING_INPUT_DATASETS",
            "message": (
                "Requested artifacts share dataset_hash values and may "
                "represent duplicate pages."
            ),
            "overlaps": overlaps,
        }
    )


def _code_input_limit_error(
    *,
    runtime: ArtifactCodeRuntime,
    state: _CodeCollectionState,
    max_input_records: int,
    max_input_bytes: int,
) -> dict[str, Any] | None:
    """Return user-facing error when input collection exceeded limits."""
    if state.input_serialization_error is not None:
        return state.input_serialization_error
    if state.input_limit_reason == "records":
        exceeded_count = (
            state.input_limit_value
            if isinstance(state.input_limit_value, int)
            else state.input_count
        )
        _logger.info(
            LogEvents.CODEGEN_REJECTED,
            reason="input_records_exceeded",
            input_records=exceeded_count,
            max_input_records=max_input_records,
        )
        runtime.increment_metric("codegen_failure")
        return _code_error(
            "code query input exceeds max_input_records",
            details_code="CODE_INPUT_TOO_LARGE",
            details={
                "input_records": exceeded_count,
                "max_input_records": max_input_records,
            },
        )
    if state.input_limit_reason == "bytes":
        exceeded_bytes = (
            state.input_limit_value
            if isinstance(state.input_limit_value, int)
            else state.input_bytes
        )
        _logger.info(
            LogEvents.CODEGEN_REJECTED,
            reason="input_bytes_exceeded",
            input_bytes=exceeded_bytes,
            max_input_bytes=max_input_bytes,
        )
        runtime.increment_metric("codegen_failure")
        return _code_error(
            "code query input exceeds max_input_bytes",
            details_code="CODE_INPUT_TOO_LARGE",
            details={
                "input_bytes": exceeded_bytes,
                "max_input_bytes": max_input_bytes,
            },
        )
    return None


def _build_runtime_args(
    *,
    request: _CodeRequest,
    state: _CodeCollectionState,
    runtime_cfg: CodeRuntimeConfig,
    runtime_import_roots: list[str],
) -> dict[str, Any]:
    """Build subprocess runtime invocation args."""
    runtime_args: dict[str, Any] = {
        "code": request.code,
        "params": request.params,
        "runtime": runtime_cfg,
        "allowed_import_roots": runtime_import_roots,
    }
    if len(request.requested_artifact_ids) == 1:
        only_artifact_id = request.requested_artifact_ids[0]
        runtime_args["data"] = state.input_records_by_artifact.get(
            only_artifact_id, []
        )
        runtime_args["schema"] = state.schema_by_artifact.get(
            only_artifact_id, state.schema_obj
        )
        return runtime_args
    runtime_args["artifacts"] = state.input_records_by_artifact
    runtime_args["schemas"] = state.schema_by_artifact
    return runtime_args


def _execute_code_runtime(
    *,
    runtime: ArtifactCodeRuntime,
    request: _CodeRequest,
    state: _CodeCollectionState,
    runtime_cfg: CodeRuntimeConfig,
    runtime_import_roots: list[str],
) -> tuple[Any | None, dict[str, Any] | None]:
    """Execute generated code in subprocess and map runtime failures."""
    started_at = time.monotonic()
    try:
        runtime_args = _build_runtime_args(
            request=request,
            state=state,
            runtime_cfg=runtime_cfg,
            runtime_import_roots=runtime_import_roots,
        )
        return execute_code_in_subprocess(**runtime_args), None
    except CodeRuntimeTimeoutError as exc:
        runtime.increment_metric("codegen_timeout")
        runtime.increment_metric("codegen_failure")
        _logger.warning(
            LogEvents.CODEGEN_TIMEOUT,
            artifact_id=request.anchor_artifact_id,
            root_path=request.root_path_log,
            message=str(exc),
        )
        return None, _code_error(
            str(exc),
            details_code="CODE_RUNTIME_TIMEOUT",
        )
    except CodeRuntimeMemoryLimitError as exc:
        runtime.increment_metric("codegen_failure")
        _logger.warning(
            LogEvents.CODEGEN_FAILED,
            artifact_id=request.anchor_artifact_id,
            root_path=request.root_path_log,
            code=exc.code,
            message=str(exc),
        )
        return None, _code_error(
            str(exc),
            details_code=exc.code,
            details={"traceback": exc.traceback} if exc.traceback else None,
        )
    except CodeRuntimeInfrastructureError as exc:
        runtime.increment_metric("codegen_failure")
        _logger.warning(
            LogEvents.CODEGEN_FAILED,
            artifact_id=request.anchor_artifact_id,
            root_path=request.root_path_log,
            code=exc.code,
            message=str(exc),
        )
        return None, gateway_error("INTERNAL", str(exc))
    except CodeRuntimeError as exc:
        runtime.increment_metric("codegen_failure")
        event = (
            LogEvents.CODEGEN_REJECTED
            if exc.code
            in {
                "CODE_ENTRYPOINT_MISSING",
                "CODE_IMPORT_NOT_ALLOWED",
                "CODE_AST_REJECTED",
            }
            else LogEvents.CODEGEN_FAILED
        )
        _logger.info(
            event,
            artifact_id=request.anchor_artifact_id,
            root_path=request.root_path_log,
            code=exc.code,
            message=str(exc),
        )
        return None, _code_error(
            _enrich_install_hint(str(exc)),
            details_code=exc.code,
            details={"traceback": exc.traceback} if exc.traceback else None,
        )
    finally:
        elapsed_ms = (time.monotonic() - started_at) * 1000.0
        runtime.observe_metric("codegen_latency", elapsed_ms)


def _validate_code_output_size(
    *,
    runtime: ArtifactCodeRuntime,
    normalized_items: list[Any],
) -> tuple[int | None, dict[str, Any] | None]:
    """Validate output serialization and return serialized byte count."""
    try:
        used_bytes = len(encode_json_bytes(normalized_items))
    except Exception as exc:
        runtime.increment_metric("codegen_failure")
        return None, _code_error(
            f"output serialization failed: {exc}",
            details_code="CODE_RUNTIME_EXCEPTION",
        )
    return used_bytes, None


def _build_code_determinism(
    *,
    request: _CodeRequest,
    state: _CodeCollectionState,
) -> dict[str, Any]:
    """Build determinism metadata for code-query responses."""
    determinism: dict[str, Any] = {
        "code_hash": request.code_hash,
        "params_hash": request.params_hash,
        "traversal_contract_version": TRAVERSAL_CONTRACT_VERSION,
        "runtime_contract_version": CODE_RUNTIME_CONTRACT_VERSION,
    }
    if len(request.requested_root_paths) <= 1:
        determinism["root_path"] = request.root_path
    else:
        determinism["root_paths"] = request.requested_root_paths
    if len(state.schema_hashes) <= 1:
        determinism["schema_hash"] = state.schema_hash
    else:
        determinism["schema_hashes"] = state.schema_hashes
    if len(state.related_set_hashes) <= 1:
        determinism["related_set_hash"] = state.related_set_hash
    else:
        determinism["related_set_hashes"] = state.related_set_hashes
    return determinism


def _build_code_lineage(
    *,
    request: _CodeRequest,
    state: _CodeCollectionState,
) -> dict[str, Any]:
    """Build lineage metadata for code-query responses."""
    if len(request.requested_artifact_ids) == 1:
        return {
            "scope": request.scope,
            "anchor_artifact_id": request.anchor_artifact_id,
            "artifact_count": len(state.related_ids),
            "artifact_ids": state.related_ids,
            "related_set_hash": state.related_set_hash,
        }
    return {
        "scope": request.scope,
        "anchor_artifact_ids": request.requested_artifact_ids,
        "root_paths": request.requested_root_paths,
        "artifact_count": len(state.related_ids),
        "artifact_ids": state.related_ids,
        "related_set_hashes": state.related_set_hashes,
    }


def _build_code_response(
    runtime: ArtifactCodeRuntime,
    *,
    request: _CodeRequest,
    state: _CodeCollectionState,
    runtime_result: Any,
    normalized_items: list[Any],
    derived_artifact_id: str,
    used_bytes: int,
) -> dict[str, Any]:
    """Build contract-v1 code response with full/schema_ref mode policy."""
    lineage = _build_code_lineage(request=request, state=state)
    metadata: dict[str, Any] = {
        "stats": {
            "bytes_out": used_bytes,
            "input_records": state.input_count,
            "input_bytes": state.input_bytes,
            "output_records": len(normalized_items),
        },
        "determinism": _build_code_determinism(request=request, state=state),
        "scope": request.scope,
    }
    if state.warnings:
        metadata["warnings"] = state.warnings

    describe: dict[str, Any] | None = None
    try:
        describe_result = execute_artifact_describe(
            runtime,
            arguments={
                "_gateway_context": {"session_id": request.session_id},
                "artifact_id": derived_artifact_id,
                "scope": "single",
            },
        )
    except Exception as exc:
        _logger.warning(
            "code response describe failed; continuing without schema metadata",
            artifact_id=derived_artifact_id,
            error_type=type(exc).__name__,
            exc_info=True,
        )
    else:
        if isinstance(describe_result, dict):
            describe = describe_result
    schemas_compact: list[dict[str, Any]] = []
    schema_legend: dict[str, Any] | None = None
    if describe is not None:
        raw_schemas = describe.get("schemas")
        if isinstance(raw_schemas, list):
            schemas_full = [
                schema for schema in raw_schemas if isinstance(schema, dict)
            ]
            schemas_compact = compact_schema_payload(schemas_full)
        if schemas_compact:
            schema_legend = SCHEMA_LEGEND

    full_payload = gateway_tool_result(
        response_mode="full",
        artifact_id=derived_artifact_id,
        payload=runtime_result,
        lineage=lineage,
        metadata=metadata,
    )
    full_payload["items"] = normalized_items
    full_payload["total_matched"] = len(normalized_items)
    full_payload["truncated"] = False
    full_payload["stats"] = metadata["stats"]
    full_payload["determinism"] = metadata["determinism"]
    full_payload["scope"] = request.scope
    if state.sampled_artifacts:
        full_payload["sampled_only"] = True
    if state.warnings:
        full_payload["warnings"] = state.warnings

    schema_ref_payload = gateway_tool_result(
        response_mode="schema_ref",
        artifact_id=derived_artifact_id,
        schemas_compact=schemas_compact,
        schema_legend=schema_legend or SCHEMA_LEGEND,
        lineage=lineage,
        metadata=metadata,
    )
    schema_ref_payload["total_matched"] = len(normalized_items)
    schema_ref_payload["truncated"] = False
    schema_ref_payload["stats"] = metadata["stats"]
    schema_ref_payload["determinism"] = metadata["determinism"]
    schema_ref_payload["scope"] = request.scope
    if state.sampled_artifacts:
        schema_ref_payload["sampled_only"] = True
    if state.warnings:
        schema_ref_payload["warnings"] = state.warnings
    response_mode = select_response_mode(
        has_pagination=False,
        full_payload=full_payload,
        schema_ref_payload=schema_ref_payload,
        max_bytes=(
            runtime.max_bytes_out
            if isinstance(runtime.max_bytes_out, int)
            and runtime.max_bytes_out > 0
            else 5_000_000
        ),
    )
    if response_mode == "schema_ref":
        return schema_ref_payload
    return full_payload


def _parse_code_args(
    arguments: dict[str, Any],
) -> tuple[_ParsedCodeArgs | None, dict[str, Any] | None]:
    """Validate and normalize user-provided code-query arguments."""
    ctx = arguments.get("_gateway_context")
    if not isinstance(ctx, dict) or not ctx.get("session_id"):
        return None, gateway_error(
            "INVALID_ARGUMENT", "missing _gateway_context.session_id"
        )
    scope, scope_err = resolve_scope(raw_scope=arguments.get("scope"))
    if scope_err is not None:
        return None, scope_err

    artifact_ids, artifact_ids_err = _normalize_code_artifact_ids(arguments)
    if artifact_ids_err is not None:
        return None, artifact_ids_err
    root_paths, root_paths_err = _normalize_code_root_paths(
        arguments,
        artifact_ids=artifact_ids,
    )
    if root_paths_err is not None:
        return None, root_paths_err

    code = arguments.get("code")
    if not isinstance(code, str) or not code.strip():
        return None, gateway_error("INVALID_ARGUMENT", "missing code")

    params = arguments.get("params")
    if params is not None and not isinstance(params, Mapping):
        return None, gateway_error(
            "INVALID_ARGUMENT", "params must be an object"
        )
    normalized_params: dict[str, Any] = (
        dict(params) if isinstance(params, Mapping) else {}
    )
    return (
        _ParsedCodeArgs(
            session_id=str(ctx["session_id"]),
            scope=scope,
            artifact_ids=artifact_ids,
            root_paths=root_paths,
            code=code,
            params=normalized_params,
        ),
        None,
    )


def _with_locator(record: Any, locator: dict[str, Any]) -> dict[str, Any]:
    if isinstance(record, dict):
        enriched = dict(record)
        enriched["_locator"] = locator
        return enriched
    return {
        "_locator": locator,
        "value": record,
    }


def _prepare_code_request_state(
    *,
    runtime: ArtifactCodeRuntime,
    arguments: dict[str, Any],
) -> tuple[_CodeRequest | None, _CodeCollectionState | None, dict[str, Any] | None]:
    """Parse inputs and collect query state required for code execution."""
    parsed_args, args_err = _parse_code_args(arguments)
    if args_err is not None:
        return None, None, args_err
    parsed_args = cast(_ParsedCodeArgs, parsed_args)
    if runtime.db_pool is None:
        return None, None, runtime.not_implemented("artifact.code")

    request, request_err = _resolve_code_request(parsed_args)
    if request_err is not None:
        return None, None, request_err
    request = cast(_CodeRequest, request)

    state, collect_err = _collect_code_inputs(
        runtime=runtime,
        request=request,
        fetch_artifact_sql=FETCH_ARTIFACT_SQL,
        fetch_schema_fields_sql=FETCH_SCHEMA_FIELDS_SQL,
        fetch_samples_sql=FETCH_SAMPLES_SQL,
    )
    if collect_err is not None:
        return None, None, collect_err
    state = cast(_CodeCollectionState, state)
    if state.schema_obj is None:
        return None, None, gateway_error("INTERNAL", "schema resolution failed")
    return request, state, None


def _normalize_runtime_items(runtime_result: Any) -> list[Any]:
    """Normalize runtime outputs to list payload expected by responses."""
    if isinstance(runtime_result, list):
        return list(runtime_result)
    return [runtime_result]


def execute_artifact_code(
    runtime: ArtifactCodeRuntime,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Execute deterministic generated Python over mapped root datasets."""
    if not runtime.code_query_enabled:
        return gateway_error("NOT_IMPLEMENTED", "query_kind=code is disabled")

    request, state, prepare_err = _prepare_code_request_state(
        runtime=runtime,
        arguments=arguments,
    )
    if prepare_err is not None:
        return prepare_err
    request = cast(_CodeRequest, request)
    state = cast(_CodeCollectionState, state)

    _append_overlapping_dataset_warning(state=state, request=request)
    _append_sampled_warning(state)
    max_input_records = runtime.code_query_max_input_records
    max_input_bytes = runtime.code_query_max_input_bytes
    input_limit_err = _code_input_limit_error(
        runtime=runtime,
        state=state,
        max_input_records=max_input_records,
        max_input_bytes=max_input_bytes,
    )
    if input_limit_err is not None:
        return input_limit_err

    runtime_cfg = CodeRuntimeConfig(
        timeout_seconds=runtime.code_query_timeout_seconds,
        max_memory_mb=runtime.code_query_max_memory_mb,
    )
    runtime_import_roots = sorted(
        allowed_import_roots(
            configured_roots=runtime.code_query_allowed_import_roots,
        )
    )

    runtime.increment_metric("codegen_executions")
    runtime.increment_metric("codegen_input_records", state.input_count)
    _logger.info(
        LogEvents.CODEGEN_STARTED,
        artifact_id=request.anchor_artifact_id,
        root_path=request.root_path_log,
        input_records=state.input_count,
        input_bytes=state.input_bytes,
    )

    runtime_result, runtime_err = _execute_code_runtime(
        runtime=runtime,
        request=request,
        state=state,
        runtime_cfg=runtime_cfg,
        runtime_import_roots=runtime_import_roots,
    )
    if runtime_err is not None:
        return runtime_err

    normalized_items = _normalize_runtime_items(runtime_result)
    total_matched = len(normalized_items)
    runtime.increment_metric("codegen_output_records", total_matched)
    used_bytes, output_err = _validate_code_output_size(
        runtime=runtime,
        normalized_items=normalized_items,
    )
    if output_err is not None:
        return output_err
    runtime.increment_metric("codegen_success")

    derived_artifact_id, persist_err = runtime.persist_code_derived(
        parent_artifact_ids=request.requested_artifact_ids,
        requested_root_paths=request.requested_root_paths,
        root_path=request.root_path,
        code_hash=request.code_hash,
        params_hash=request.params_hash,
        result_items=normalized_items,
    )
    if persist_err is not None:
        return persist_err
    if not isinstance(derived_artifact_id, str) or not derived_artifact_id:
        return gateway_error(
            "DERIVED_PERSISTENCE_FAILED",
            "derived artifact persistence returned invalid artifact_id",
            details={
                "stage": "persist_code_derived",
                "artifact_id": derived_artifact_id,
            },
        )

    response = _build_code_response(
        runtime,
        request=request,
        state=state,
        runtime_result=runtime_result,
        normalized_items=normalized_items,
        derived_artifact_id=derived_artifact_id,
        used_bytes=cast(int, used_bytes),
    )

    _logger.info(
        LogEvents.CODEGEN_COMPLETED,
        artifact_id=request.anchor_artifact_id,
        root_path=request.root_path_log,
        input_records=state.input_count,
        output_records=total_matched,
        truncated=False,
    )
    return response


__all__ = [
    "_enrich_install_hint",
    "_module_to_dist",
    "execute_artifact_code",
]
