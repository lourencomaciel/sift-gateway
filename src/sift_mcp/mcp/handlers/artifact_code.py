"""Handler for ``artifact(action="query", query_kind="code")``."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
import hashlib
from importlib.metadata import packages_distributions
import re
import sys
import time
from typing import TYPE_CHECKING, Any, cast

from sift_mcp.canon.rfc8785 import canonical_bytes, coerce_floats
from sift_mcp.codegen.ast_guard import allowed_import_roots
from sift_mcp.codegen.runtime import (
    CODE_RUNTIME_CONTRACT_VERSION,
    CodeRuntimeConfig,
    CodeRuntimeError,
    CodeRuntimeInfrastructureError,
    CodeRuntimeMemoryLimitError,
    CodeRuntimeTimeoutError,
    encode_json_bytes,
    execute_code_in_subprocess,
)
from sift_mcp.constants import TRAVERSAL_CONTRACT_VERSION, WORKSPACE_ID
from sift_mcp.envelope.responses import gateway_error
from sift_mcp.mcp.handlers.common import (
    ENVELOPE_COLUMNS,
    SAMPLE_COLUMNS,
    extract_json_target,
    row_to_dict,
    rows_to_dicts,
)
from sift_mcp.mcp.handlers.lineage_roots import (
    resolve_all_related_root_candidates,
)
from sift_mcp.mcp.handlers.schema_payload import build_schema_payload
from sift_mcp.mcp.lineage import (
    compute_related_set_hash,
    resolve_related_artifacts,
)
from sift_mcp.obs.logging import LogEvents, get_logger
from sift_mcp.query.jsonpath import JsonPathError, evaluate_jsonpath
from sift_mcp.storage.payload_store import reconstruct_envelope

if TYPE_CHECKING:
    from sift_mcp.mcp.server import GatewayServer

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


def _hash_text(value: str) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def _hash_json(value: Any) -> str:
    digest = hashlib.sha256(
        canonical_bytes(coerce_floats(value))
    ).hexdigest()
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
        return f"{msg}\nRun: sift-mcp install {dist}"
    m = _RE_IMPORT_NOT_ALLOWED.search(msg)
    if m:
        root = m.group(1).split(".")[0]
        # stdlib modules are policy-blocked, not missing —
        # suggesting install would be misleading.
        if root in _STDLIB_ROOTS:
            return msg
        dist = _module_to_dist(root)
        return f"{msg}\nRun: sift-mcp install {dist}"
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
    artifact_ids: list[str]
    root_paths: dict[str, str]
    code: str
    params: dict[str, Any]


@dataclass(frozen=True)
class _CodeRequest:
    """Normalized request state for code queries."""

    session_id: str
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
    schema_by_artifact: dict[str, dict[str, Any]] = field(
        default_factory=dict
    )
    schema_hashes: dict[str, str] = field(default_factory=dict)
    input_records_by_artifact: dict[str, list[dict[str, Any]]] = field(
        default_factory=dict
    )
    input_count: int = 0
    input_bytes: int = 2
    input_limit_reason: str | None = None
    input_limit_value: int | None = None
    input_serialization_error: dict[str, Any] | None = None


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
    next_bytes = state.input_bytes + record_bytes + (
        1 if state.input_count else 0
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
    candidate_rows: list[tuple[str, dict[str, Any], dict[str, Any], dict[str, Any]]],
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
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Resolve envelope payload from inline JSONB or canonical bytes."""
    envelope_value = artifact_row.get("envelope")
    canonical_bytes_raw = artifact_row.get("envelope_canonical_bytes")
    if isinstance(envelope_value, dict) and "content" in envelope_value:
        return envelope_value, None
    if canonical_bytes_raw is None:
        return None, gateway_error(
            "INTERNAL",
            "missing canonical bytes for artifact",
        )
    try:
        envelope = reconstruct_envelope(
            compressed_bytes=bytes(canonical_bytes_raw),
            encoding=str(artifact_row.get("envelope_canonical_encoding", "none")),
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
    ctx: GatewayServer,
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
    corruption = ctx._check_sample_corruption(root_row, sample_rows)
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
    ctx: GatewayServer,
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
    envelope, envelope_err = _reconstruct_code_envelope(artifact_row)
    if envelope_err is not None:
        return False, envelope_err
    if envelope is None:
        return False, gateway_error("INTERNAL", "missing envelope")

    json_target = extract_json_target(envelope, artifact_row.get("mapped_part_index"))
    try:
        root_values = evaluate_jsonpath(
            json_target,
            root_path_for_requested,
            max_length=ctx.config.max_jsonpath_length,
            max_segments=ctx.config.max_path_segments,
            max_wildcard_expansion_total=ctx.config.max_wildcard_expansion_total,
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
    ctx: GatewayServer,
    connection: Any,
    state: _CodeCollectionState,
    request: _CodeRequest,
    requested_artifact_id: str,
    root_path_for_requested: str,
    candidate_rows: list[tuple[str, dict[str, Any], dict[str, Any], dict[str, Any]]],
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
                ctx=ctx,
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
                ctx=ctx,
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
    ctx: GatewayServer,
    connection: Any,
    session_id: str,
    related_ids: list[str],
) -> None:
    """Best-effort retrieval touch for collected related artifacts."""
    if not related_ids:
        return
    touched = ctx._safe_touch_for_retrieval_many(
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
    ctx: GatewayServer,
    request: _CodeRequest,
    fetch_artifact_sql: str,
    fetch_schema_fields_sql: str,
    fetch_samples_sql: str,
) -> tuple[_CodeCollectionState | None, dict[str, Any] | None]:
    """Collect schemas, lineage hashes, and runtime input records."""
    state = _new_collection_state(request)
    max_input_records = ctx.config.code_query_max_input_records
    max_input_bytes = ctx.config.code_query_max_input_bytes

    with ctx.db_pool.connection() as connection:
        for artifact_id in request.requested_artifact_ids:
            if not ctx._artifact_visible(
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
                    ctx=ctx,
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
            ctx=ctx,
            connection=connection,
            session_id=request.session_id,
            related_ids=state.related_ids,
        )

    return state, None


def _collect_code_inputs_for_requested_artifact(
    *,
    ctx: GatewayServer,
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
    root_path_for_requested = request.requested_root_paths[requested_artifact_id]
    resolved_candidates = resolve_all_related_root_candidates(
        connection,
        session_id=request.session_id,
        anchor_artifact_id=requested_artifact_id,
        root_path=root_path_for_requested,
        max_related_artifacts=ctx.config.related_query_max_artifacts,
        resolve_related_fn=resolve_related_artifacts,
        compute_related_set_hash_fn=compute_related_set_hash,
    )
    if isinstance(resolved_candidates, dict):
        return False, resolved_candidates

    all_related_ids.update(resolved_candidates.related_ids)
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

    requested_schema, schema_hash, schema_err = _load_code_schema_for_requested_artifact(
        connection=connection,
        candidate_rows=resolved_candidates.candidate_rows,
        fetch_schema_fields_sql=fetch_schema_fields_sql,
    )
    if schema_err is not None:
        return False, schema_err
    if requested_schema is None:
        return False, gateway_error("INTERNAL", "schema resolution failed")
    state.schema_by_artifact[requested_artifact_id] = requested_schema
    if schema_hash:
        state.schema_hashes[requested_artifact_id] = schema_hash
    if requested_artifact_id == request.anchor_artifact_id:
        state.schema_obj = requested_schema
        state.schema_hash = schema_hash

    stop_collection, collect_err = _collect_requested_candidate_rows(
        ctx=ctx,
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


def _code_input_limit_error(
    *,
    ctx: GatewayServer,
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
        ctx._increment_metric("codegen_failure")
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
        ctx._increment_metric("codegen_failure")
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
    ctx: GatewayServer,
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
        ctx._increment_metric("codegen_timeout")
        ctx._increment_metric("codegen_failure")
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
        ctx._increment_metric("codegen_failure")
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
        ctx._increment_metric("codegen_failure")
        _logger.warning(
            LogEvents.CODEGEN_FAILED,
            artifact_id=request.anchor_artifact_id,
            root_path=request.root_path_log,
            code=exc.code,
            message=str(exc),
        )
        return None, gateway_error("INTERNAL", str(exc))
    except CodeRuntimeError as exc:
        ctx._increment_metric("codegen_failure")
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
        ctx._observe_metric("codegen_latency", elapsed_ms)


def _validate_code_output_size(
    *,
    ctx: GatewayServer,
    normalized_items: list[Any],
) -> tuple[int | None, dict[str, Any] | None]:
    """Validate runtime output serialization size against max_bytes_out."""
    try:
        used_bytes = len(encode_json_bytes(normalized_items))
    except Exception as exc:
        ctx._increment_metric("codegen_failure")
        return None, _code_error(
            f"output serialization failed: {exc}",
            details_code="CODE_RUNTIME_EXCEPTION",
        )
    if used_bytes > ctx.config.max_bytes_out:
        ctx._increment_metric("codegen_failure")
        return None, gateway_error(
            "RESPONSE_TOO_LARGE",
            (
                "Code query results "
                f"({used_bytes} bytes) exceed max_bytes_out "
                f"({ctx.config.max_bytes_out} bytes). "
                "Aggregate data in your run() function to reduce output size."
            ),
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
            "scope": "all_related",
            "anchor_artifact_id": request.anchor_artifact_id,
            "artifact_count": len(state.related_ids),
            "artifact_ids": state.related_ids,
            "related_set_hash": state.related_set_hash,
        }
    return {
        "scope": "all_related",
        "anchor_artifact_ids": request.requested_artifact_ids,
        "root_paths": request.requested_root_paths,
        "artifact_count": len(state.related_ids),
        "artifact_ids": state.related_ids,
        "related_set_hashes": state.related_set_hashes,
    }


def _build_code_response(
    *,
    build_select_result: Any,
    normalized_items: list[Any],
    total_matched: int,
    used_bytes: int,
    request: _CodeRequest,
    state: _CodeCollectionState,
    determinism: dict[str, Any],
) -> dict[str, Any]:
    """Build final code-query tool response payload."""
    response = build_select_result(
        items=normalized_items,
        truncated=False,
        cursor=None,
        total_matched=total_matched,
        sampled_only=bool(state.sampled_artifacts),
        omitted=None,
        stats={
            "bytes_out": used_bytes,
            "input_records": state.input_count,
            "input_bytes": state.input_bytes,
            "output_records": total_matched,
        },
        determinism=determinism,
    )
    response["scope"] = "all_related"
    response["lineage"] = _build_code_lineage(request=request, state=state)
    if state.warnings:
        response["warnings"] = state.warnings
    return cast(dict[str, Any], response)


def _parse_code_args(
    arguments: dict[str, Any],
) -> tuple[_ParsedCodeArgs | None, dict[str, Any] | None]:
    """Validate and normalize user-provided code-query arguments."""
    ctx = arguments.get("_gateway_context")
    if not isinstance(ctx, dict) or not ctx.get("session_id"):
        return None, gateway_error(
            "INVALID_ARGUMENT", "missing _gateway_context.session_id"
        )

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


async def handle_artifact_code(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Execute deterministic generated Python over a mapped root dataset."""
    from sift_mcp.tools.artifact_get import FETCH_ARTIFACT_SQL
    from sift_mcp.tools.artifact_schema import (
        FETCH_SCHEMA_FIELDS_SQL,
    )
    from sift_mcp.tools.artifact_select import (
        FETCH_SAMPLES_SQL,
        build_select_result,
    )

    if not ctx.config.code_query_enabled:
        return gateway_error("NOT_IMPLEMENTED", "query_kind=code is disabled")

    parsed_args, err = _parse_code_args(arguments)
    if err is not None:
        return err
    parsed_args = cast(_ParsedCodeArgs, parsed_args)
    if ctx.db_pool is None:
        return ctx._not_implemented("artifact.code")

    request, request_err = _resolve_code_request(parsed_args)
    if request_err is not None:
        return request_err
    request = cast(_CodeRequest, request)

    state, collect_err = _collect_code_inputs(
        ctx=ctx,
        request=request,
        fetch_artifact_sql=FETCH_ARTIFACT_SQL,
        fetch_schema_fields_sql=FETCH_SCHEMA_FIELDS_SQL,
        fetch_samples_sql=FETCH_SAMPLES_SQL,
    )
    if collect_err is not None:
        return collect_err
    state = cast(_CodeCollectionState, state)

    if state.schema_obj is None:
        return gateway_error("INTERNAL", "schema resolution failed")

    _append_sampled_warning(state)
    max_input_records = ctx.config.code_query_max_input_records
    max_input_bytes = ctx.config.code_query_max_input_bytes
    input_limit_err = _code_input_limit_error(
        ctx=ctx,
        state=state,
        max_input_records=max_input_records,
        max_input_bytes=max_input_bytes,
    )
    if input_limit_err is not None:
        return input_limit_err

    runtime_cfg = CodeRuntimeConfig(
        timeout_seconds=ctx.config.code_query_timeout_seconds,
        max_memory_mb=ctx.config.code_query_max_memory_mb,
    )
    runtime_import_roots = sorted(
        allowed_import_roots(
            configured_roots=ctx.config.code_query_allowed_import_roots,
        )
    )

    ctx._increment_metric("codegen_executions")
    ctx._increment_metric("codegen_input_records", state.input_count)
    _logger.info(
        LogEvents.CODEGEN_STARTED,
        artifact_id=request.anchor_artifact_id,
        root_path=request.root_path_log,
        input_records=state.input_count,
        input_bytes=state.input_bytes,
    )

    runtime_result, runtime_err = _execute_code_runtime(
        ctx=ctx,
        request=request,
        state=state,
        runtime_cfg=runtime_cfg,
        runtime_import_roots=runtime_import_roots,
    )
    if runtime_err is not None:
        return runtime_err

    normalized_items = (
        list(runtime_result) if isinstance(runtime_result, list) else [runtime_result]
    )
    total_matched = len(normalized_items)
    ctx._increment_metric("codegen_output_records", total_matched)
    used_bytes, output_err = _validate_code_output_size(
        ctx=ctx,
        normalized_items=normalized_items,
    )
    if output_err is not None:
        return output_err
    ctx._increment_metric("codegen_success")

    response = _build_code_response(
        build_select_result=build_select_result,
        normalized_items=normalized_items,
        total_matched=total_matched,
        used_bytes=cast(int, used_bytes),
        request=request,
        state=state,
        determinism=_build_code_determinism(request=request, state=state),
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
