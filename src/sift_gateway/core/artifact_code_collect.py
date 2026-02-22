"""Input collection helpers for artifact code-query execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from sift_gateway.codegen.runtime import encode_json_bytes
from sift_gateway.constants import WORKSPACE_ID
from sift_gateway.core.artifact_get import ENVELOPE_COLUMNS
from sift_gateway.core.lineage_roots import (
    resolve_all_related_root_candidates,
    resolve_single_root_candidate,
)
from sift_gateway.core.retrieval_helpers import extract_json_target
from sift_gateway.core.rows import row_to_dict, rows_to_dicts
from sift_gateway.core.runtime import ArtifactCodeRuntime
from sift_gateway.core.schema_payload import build_schema_payload
from sift_gateway.envelope.responses import gateway_error
from sift_gateway.query.jsonpath import JsonPathError, evaluate_jsonpath
from sift_gateway.storage.payload_store import reconstruct_envelope

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
SAMPLE_COLUMNS = ["sample_index", "record", "record_bytes", "record_hash"]
_CodeCandidateRow = tuple[str, dict[str, Any], dict[str, Any], dict[str, Any]]


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


def _new_collection_state(request: Any) -> _CodeCollectionState:
    """Initialize input collection state for requested artifacts."""
    state = _CodeCollectionState()
    state.input_records_by_artifact = {
        artifact_id: [] for artifact_id in request.requested_artifact_ids
    }
    return state


def _with_locator(record: Any, locator: dict[str, Any]) -> dict[str, Any]:
    if isinstance(record, dict):
        enriched = dict(record)
        enriched["_locator"] = locator
        return enriched
    return {
        "_locator": {**locator, "_scalar": True},
        "value": record,
    }


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
    candidate_rows: list[_CodeCandidateRow],
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
            (WORKSPACE_ID, artifact_id, root_row["root_key"]),
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
        connection.execute(fetch_artifact_sql, (WORKSPACE_ID, artifact_id)).fetchone(),
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

    json_target = extract_json_target(envelope, artifact_row.get("mapped_part_index"))
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
    request: Any,
    requested_artifact_id: str,
    root_path_for_requested: str,
    candidate_rows: list[_CodeCandidateRow],
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
    request: Any,
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
            stop_collection, collect_err = _collect_code_inputs_for_requested_artifact(
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
    request: Any,
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
    resolved_candidates, resolve_err = _resolve_requested_code_candidates(
        runtime=runtime,
        connection=connection,
        request=request,
        requested_artifact_id=requested_artifact_id,
        root_path_for_requested=root_path_for_requested,
    )
    if resolve_err is not None:
        return False, resolve_err
    if resolved_candidates is None:
        return False, gateway_error("INTERNAL", "candidate resolution failed")

    all_related_ids.update(resolved_candidates.related_ids)
    _record_requested_code_lineage_state(
        state=state,
        request=request,
        requested_artifact_id=requested_artifact_id,
        root_path_for_requested=root_path_for_requested,
        resolved_candidates=resolved_candidates,
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
    request: Any,
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
    request: Any,
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
    request: Any,
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
    request: Any,
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

