"""Handler for ``artifact(action="query", query_kind="code")``."""

from __future__ import annotations

from decimal import Decimal
import hashlib
import time
from typing import TYPE_CHECKING, Any, Mapping

from sift_mcp.canon.rfc8785 import canonical_bytes
from sift_mcp.codegen.ast_guard import allowed_import_roots
from sift_mcp.codegen.runtime import (
    CODE_RUNTIME_CONTRACT_VERSION,
    CodeRuntimeConfig,
    CodeRuntimeError,
    CodeRuntimeInfrastructureError,
    CodeRuntimeMemoryLimit,
    CodeRuntimeTimeout,
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


def _coerce_floats_to_decimal(value: Any) -> Any:
    """Recursively coerce float values for canonical hashing."""
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, dict):
        return {
            str(key): _coerce_floats_to_decimal(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_coerce_floats_to_decimal(item) for item in value]
    return value


def _hash_json(value: Any) -> str:
    digest = hashlib.sha256(
        canonical_bytes(_coerce_floats_to_decimal(value))
    ).hexdigest()
    return f"sha256:{digest}"


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
    return {artifact_id: raw_root_path for artifact_id in artifact_ids}, None


def _validate_code_args(arguments: dict[str, Any]) -> dict[str, Any] | None:
    ctx = arguments.get("_gateway_context")
    if not isinstance(ctx, dict) or not ctx.get("session_id"):
        return gateway_error(
            "INVALID_ARGUMENT",
            "missing _gateway_context.session_id",
        )

    _, artifact_ids_err = _normalize_code_artifact_ids(arguments)
    if artifact_ids_err is not None:
        return artifact_ids_err
    artifact_ids, _ = _normalize_code_artifact_ids(arguments)
    _, root_paths_err = _normalize_code_root_paths(
        arguments,
        artifact_ids=artifact_ids,
    )
    if root_paths_err is not None:
        return root_paths_err

    code = arguments.get("code")
    if not isinstance(code, str) or not code.strip():
        return gateway_error("INVALID_ARGUMENT", "missing code")

    params = arguments.get("params")
    if params is not None and not isinstance(params, Mapping):
        return gateway_error("INVALID_ARGUMENT", "params must be an object")

    return None


def _build_schema(
    *,
    schema_root: dict[str, Any],
    field_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    fields: list[dict[str, Any]] = []
    for field in field_rows:
        raw_types = field.get("types")
        types = [str(item) for item in raw_types] if isinstance(raw_types, list) else []
        observed_count_raw = field.get("observed_count")
        observed_count = (
            int(observed_count_raw) if isinstance(observed_count_raw, int) else 0
        )
        entry: dict[str, Any] = {
            "path": field.get("field_path"),
            "types": types,
            "nullable": bool(field.get("nullable")),
            "required": bool(field.get("required")),
            "observed_count": observed_count,
        }
        example_value = field.get("example_value")
        if isinstance(example_value, str):
            entry["example_value"] = example_value
        distinct_values = field.get("distinct_values")
        if isinstance(distinct_values, list):
            entry["distinct_values"] = list(distinct_values)
        cardinality = field.get("cardinality")
        if isinstance(cardinality, int):
            entry["cardinality"] = cardinality
        fields.append(entry)

    observed_records_raw = schema_root.get("observed_records")
    observed_records = (
        int(observed_records_raw) if isinstance(observed_records_raw, int) else 0
    )
    return {
        "version": schema_root.get("schema_version"),
        "schema_hash": schema_root.get("schema_hash"),
        "root_path": schema_root.get("root_path"),
        "mode": schema_root.get("mode"),
        "coverage": {
            "completeness": schema_root.get("completeness"),
            "observed_records": observed_records,
        },
        "fields": fields,
        "determinism": {
            "dataset_hash": schema_root.get("dataset_hash"),
            "traversal_contract_version": schema_root.get(
                "traversal_contract_version"
            ),
            "map_budget_fingerprint": schema_root.get(
                "map_budget_fingerprint"
            ),
        },
    }


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

    err = _validate_code_args(arguments)
    if err is not None:
        return err
    if ctx.db_pool is None:
        return ctx._not_implemented("artifact.code")

    raw_ctx = arguments.get("_gateway_context")
    session_id = str(raw_ctx["session_id"]) if isinstance(raw_ctx, dict) else ""
    requested_artifact_ids, artifact_ids_err = _normalize_code_artifact_ids(
        arguments
    )
    if artifact_ids_err is not None:
        return artifact_ids_err
    requested_root_paths, root_paths_err = _normalize_code_root_paths(
        arguments,
        artifact_ids=requested_artifact_ids,
    )
    if root_paths_err is not None:
        return root_paths_err
    anchor_artifact_id = requested_artifact_ids[0]
    root_path = requested_root_paths[anchor_artifact_id]
    root_path_log = (
        root_path
        if len(set(requested_root_paths.values())) == 1
        else "<multi_root_paths>"
    )
    code = str(arguments["code"])
    params_raw = arguments.get("params")
    params: dict[str, Any] = (
        dict(params_raw) if isinstance(params_raw, Mapping) else {}
    )

    try:
        code_hash = _hash_text(code)
        params_hash = _hash_json(params)
    except (TypeError, ValueError) as exc:
        return gateway_error(
            "INVALID_ARGUMENT",
            f"invalid params: {exc}",
        )

    related_ids: list[str] = []
    related_set_hash = ""
    related_set_hashes: dict[str, str] = {}
    warnings: list[dict[str, Any]] = []
    sampled_artifacts: set[str] = set()
    schema_obj: dict[str, Any] | None = None
    schema_hash = ""
    schema_by_artifact: dict[str, dict[str, Any]] = {}
    schema_hashes: dict[str, str] = {}

    input_records_by_artifact: dict[str, list[dict[str, Any]]] = {
        artifact_id: [] for artifact_id in requested_artifact_ids
    }
    input_count = 0
    # Track serialized list bytes incrementally: [] => 2 bytes.
    input_bytes = 2
    input_limit_reason: str | None = None
    input_limit_value: int | None = None
    input_serialization_error: dict[str, Any] | None = None
    max_input_records = ctx.config.code_query_max_input_records
    max_input_bytes = ctx.config.code_query_max_input_bytes

    def _append_input_record(
        *,
        requested_artifact_id: str,
        record: Any,
        locator: dict[str, Any],
    ) -> bool:
        nonlocal input_bytes, input_count
        nonlocal input_limit_reason, input_limit_value
        nonlocal input_serialization_error
        enriched = _with_locator(record, locator)
        next_count = input_count + 1
        if next_count > max_input_records:
            input_limit_reason = "records"
            input_limit_value = next_count
            return False
        try:
            record_bytes = len(encode_json_bytes(enriched))
        except Exception as exc:
            input_serialization_error = gateway_error(
                "INVALID_ARGUMENT",
                f"input serialization failed: {exc}",
                details={"code": "CODE_RUNTIME_EXCEPTION"},
            )
            return False
        next_bytes = input_bytes + record_bytes + (1 if input_count else 0)
        if next_bytes > max_input_bytes:
            input_limit_reason = "bytes"
            input_limit_value = next_bytes
            return False
        input_records_by_artifact[requested_artifact_id].append(enriched)
        input_count = next_count
        input_bytes = next_bytes
        return True

    with ctx.db_pool.connection() as connection:
        for artifact_id in requested_artifact_ids:
            if not ctx._artifact_visible(
                connection,
                session_id=session_id,
                artifact_id=artifact_id,
            ):
                return gateway_error(
                    "NOT_FOUND", f"artifact not found: {artifact_id}"
                )

        all_related_ids: set[str] = set()
        for requested_artifact_id in requested_artifact_ids:
            root_path_for_requested = requested_root_paths[requested_artifact_id]
            resolved_candidates = resolve_all_related_root_candidates(
                connection,
                session_id=session_id,
                anchor_artifact_id=requested_artifact_id,
                root_path=root_path_for_requested,
                max_related_artifacts=ctx.config.related_query_max_artifacts,
                resolve_related_fn=resolve_related_artifacts,
                compute_related_set_hash_fn=compute_related_set_hash,
            )
            if isinstance(resolved_candidates, dict):
                return resolved_candidates

            all_related_ids.update(resolved_candidates.related_ids)
            related_set_hashes[requested_artifact_id] = (
                resolved_candidates.related_set_hash
            )
            if requested_artifact_id == anchor_artifact_id:
                related_set_hash = resolved_candidates.related_set_hash

            missing_root_artifacts = resolved_candidates.missing_root_artifacts
            if missing_root_artifacts:
                warning: dict[str, Any] = {
                    "code": "MISSING_ROOT_PATH",
                    "root_path": root_path_for_requested,
                    "skipped_artifacts": len(missing_root_artifacts),
                    "artifact_ids": missing_root_artifacts,
                }
                if len(requested_artifact_ids) > 1:
                    warning["anchor_artifact_id"] = requested_artifact_id
                warnings.append(warning)

            candidate_rows = resolved_candidates.candidate_rows

            # Bind one schema object per requested artifact.
            schema_artifact_id, _meta, schema_root_row, schema_root = (
                candidate_rows[0]
            )
            root_key = schema_root_row.get("root_key")
            if not isinstance(root_key, str):
                return gateway_error("INTERNAL", "schema root_key missing")
            field_rows = rows_to_dicts(
                connection.execute(
                    FETCH_SCHEMA_FIELDS_SQL,
                    (WORKSPACE_ID, schema_artifact_id, root_key),
                ).fetchall(),
                _SCHEMA_FIELD_COLUMNS,
            )
            requested_schema = _build_schema(
                schema_root=schema_root, field_rows=field_rows
            )
            schema_by_artifact[requested_artifact_id] = requested_schema
            schema_hash_raw = requested_schema.get("schema_hash")
            if isinstance(schema_hash_raw, str):
                schema_hashes[requested_artifact_id] = schema_hash_raw
            if requested_artifact_id == anchor_artifact_id:
                schema_obj = requested_schema
                schema_hash = (
                    schema_hash_raw if isinstance(schema_hash_raw, str) else ""
                )

            stop_collection = False
            for artifact_id, artifact_meta, root_row, _schema in candidate_rows:
                if stop_collection:
                    break
                map_kind = str(artifact_meta.get("map_kind", "none"))
                sampled_only = map_kind == "partial"

                if sampled_only:
                    sampled_artifacts.add(artifact_id)
                    sample_rows = rows_to_dicts(
                        connection.execute(
                            FETCH_SAMPLES_SQL,
                            (
                                WORKSPACE_ID,
                                artifact_id,
                                root_row["root_key"],
                            ),
                        ).fetchall(),
                        SAMPLE_COLUMNS,
                    )
                    corruption = ctx._check_sample_corruption(
                        root_row, sample_rows
                    )
                    if corruption is not None:
                        return corruption

                    for sample in sample_rows:
                        record = sample.get("record")
                        locator = {
                            "artifact_id": artifact_id,
                            "root_path": root_path_for_requested,
                            "sample_index": sample.get("sample_index"),
                        }
                        if len(requested_artifact_ids) > 1:
                            locator["requested_artifact_id"] = (
                                requested_artifact_id
                            )
                        if not _append_input_record(
                            requested_artifact_id=requested_artifact_id,
                            record=record,
                            locator=locator,
                        ):
                            stop_collection = True
                            break
                    continue

                artifact_row = row_to_dict(
                    connection.execute(
                        FETCH_ARTIFACT_SQL,
                        (WORKSPACE_ID, artifact_id),
                    ).fetchone(),
                    ENVELOPE_COLUMNS,
                )
                if artifact_row is None:
                    continue

                envelope_value = artifact_row.get("envelope")
                canonical_bytes_raw = artifact_row.get("envelope_canonical_bytes")
                if isinstance(envelope_value, dict) and "content" in envelope_value:
                    envelope = envelope_value
                elif canonical_bytes_raw is None:
                    return gateway_error(
                        "INTERNAL",
                        "missing canonical bytes for artifact",
                    )
                else:
                    try:
                        envelope = reconstruct_envelope(
                            compressed_bytes=bytes(canonical_bytes_raw),
                            encoding=str(
                                artifact_row.get(
                                    "envelope_canonical_encoding", "none"
                                )
                            ),
                            expected_hash=str(
                                artifact_row.get("payload_hash_full", "")
                            ),
                        )
                    except ValueError as exc:
                        return gateway_error(
                            "INTERNAL",
                            f"envelope reconstruction failed: {exc}",
                        )

                json_target = extract_json_target(
                    envelope, artifact_row.get("mapped_part_index")
                )
                try:
                    root_values = evaluate_jsonpath(
                        json_target,
                        root_path_for_requested,
                        max_length=ctx.config.max_jsonpath_length,
                        max_segments=ctx.config.max_path_segments,
                        max_wildcard_expansion_total=ctx.config.max_wildcard_expansion_total,
                    )
                except JsonPathError as exc:
                    return gateway_error("INVALID_ARGUMENT", str(exc))

                records: list[Any]
                if len(root_values) == 1 and isinstance(root_values[0], list):
                    records = list(root_values[0])
                else:
                    records = list(root_values)

                for index, record in enumerate(records):
                    locator = {
                        "artifact_id": artifact_id,
                        "root_path": root_path_for_requested,
                        "index": index,
                    }
                    if len(requested_artifact_ids) > 1:
                        locator["requested_artifact_id"] = requested_artifact_id
                    if not _append_input_record(
                        requested_artifact_id=requested_artifact_id,
                        record=record,
                        locator=locator,
                    ):
                        stop_collection = True
                        break

            if input_limit_reason is not None or input_serialization_error is not None:
                break

        related_ids = sorted(all_related_ids)
        if related_ids:
            ctx._safe_touch_for_retrieval_many(
                connection,
                session_id=session_id,
                artifact_ids=related_ids,
            )
        commit = getattr(connection, "commit", None)
        if callable(commit):
            commit()

    if schema_obj is None:
        return gateway_error("INTERNAL", "schema resolution failed")

    if sampled_artifacts:
        warnings.append(
            {
                "code": "SAMPLED_MAPPING_USED",
                "sampled_only": True,
                "artifact_ids": sorted(sampled_artifacts),
            }
        )

    if input_serialization_error is not None:
        return input_serialization_error

    if input_limit_reason == "records":
        exceeded_count = (
            input_limit_value
            if isinstance(input_limit_value, int)
            else input_count
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

    if input_limit_reason == "bytes":
        exceeded_bytes = (
            input_limit_value
            if isinstance(input_limit_value, int)
            else input_bytes
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

    runtime_cfg = CodeRuntimeConfig(
        timeout_seconds=ctx.config.code_query_timeout_seconds,
        max_memory_mb=ctx.config.code_query_max_memory_mb,
    )
    runtime_import_roots = sorted(
        allowed_import_roots(
            allow_analytics_imports=ctx.config.code_query_allow_analytics_imports,
            configured_roots=ctx.config.code_query_allowed_import_roots,
        )
    )

    ctx._increment_metric("codegen_executions")
    ctx._increment_metric("codegen_input_records", input_count)
    _logger.info(
        LogEvents.CODEGEN_STARTED,
        artifact_id=anchor_artifact_id,
        root_path=root_path_log,
        input_records=input_count,
        input_bytes=input_bytes,
    )

    started_at = time.monotonic()
    runtime_result: Any
    try:
        runtime_args: dict[str, Any] = {
            "code": code,
            "params": params,
            "runtime": runtime_cfg,
            "allowed_import_roots": runtime_import_roots,
        }
        if len(requested_artifact_ids) == 1:
            only_artifact_id = requested_artifact_ids[0]
            runtime_args["data"] = input_records_by_artifact.get(
                only_artifact_id, []
            )
            runtime_args["schema"] = schema_by_artifact.get(
                only_artifact_id, schema_obj
            )
        else:
            runtime_args["artifacts"] = input_records_by_artifact
            runtime_args["schemas"] = schema_by_artifact
        runtime_result = execute_code_in_subprocess(**runtime_args)
    except CodeRuntimeTimeout as exc:
        ctx._increment_metric("codegen_timeout")
        ctx._increment_metric("codegen_failure")
        _logger.warning(
            LogEvents.CODEGEN_TIMEOUT,
            artifact_id=anchor_artifact_id,
            root_path=root_path_log,
            message=str(exc),
        )
        return _code_error(
            str(exc),
            details_code="CODE_RUNTIME_TIMEOUT",
        )
    except CodeRuntimeMemoryLimit as exc:
        ctx._increment_metric("codegen_failure")
        _logger.warning(
            LogEvents.CODEGEN_FAILED,
            artifact_id=anchor_artifact_id,
            root_path=root_path_log,
            code=exc.code,
            message=str(exc),
        )
        return _code_error(
            str(exc),
            details_code=exc.code,
            details={
                "traceback": exc.traceback,
            }
            if exc.traceback
            else None,
        )
    except CodeRuntimeInfrastructureError as exc:
        ctx._increment_metric("codegen_failure")
        _logger.warning(
            LogEvents.CODEGEN_FAILED,
            artifact_id=anchor_artifact_id,
            root_path=root_path_log,
            code=exc.code,
            message=str(exc),
        )
        return gateway_error("INTERNAL", str(exc))
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
            artifact_id=anchor_artifact_id,
            root_path=root_path_log,
            code=exc.code,
            message=str(exc),
        )
        return _code_error(
            str(exc),
            details_code=exc.code,
            details={
                "traceback": exc.traceback,
            }
            if exc.traceback
            else None,
        )
    finally:
        elapsed_ms = (time.monotonic() - started_at) * 1000.0
        ctx._observe_metric("codegen_latency", elapsed_ms)

    normalized_items: list[Any]
    if isinstance(runtime_result, list):
        normalized_items = list(runtime_result)
    else:
        normalized_items = [runtime_result]

    total_matched = len(normalized_items)
    ctx._increment_metric("codegen_output_records", total_matched)
    try:
        used_bytes = len(encode_json_bytes(normalized_items))
    except Exception as exc:
        ctx._increment_metric("codegen_failure")
        return _code_error(
            f"output serialization failed: {exc}",
            details_code="CODE_RUNTIME_EXCEPTION",
        )
    if used_bytes > ctx.config.max_bytes_out:
        ctx._increment_metric("codegen_failure")
        return gateway_error(
            "RESPONSE_TOO_LARGE",
            (
                "Code query results "
                f"({used_bytes} bytes) exceed max_bytes_out "
                f"({ctx.config.max_bytes_out} bytes). "
                "Aggregate data in your run() function to reduce output size."
            ),
        )
    ctx._increment_metric("codegen_success")

    determinism: dict[str, Any] = {
        "code_hash": code_hash,
        "params_hash": params_hash,
        "traversal_contract_version": TRAVERSAL_CONTRACT_VERSION,
        "runtime_contract_version": CODE_RUNTIME_CONTRACT_VERSION,
    }
    if len(requested_root_paths) <= 1:
        determinism["root_path"] = root_path
    else:
        determinism["root_paths"] = requested_root_paths
    if len(schema_hashes) <= 1:
        determinism["schema_hash"] = schema_hash
    else:
        determinism["schema_hashes"] = schema_hashes
    if len(related_set_hashes) <= 1:
        determinism["related_set_hash"] = related_set_hash
    else:
        determinism["related_set_hashes"] = related_set_hashes

    response = build_select_result(
        items=normalized_items,
        truncated=False,
        cursor=None,
        total_matched=total_matched,
        sampled_only=bool(sampled_artifacts),
        omitted=None,
        stats={
            "bytes_out": used_bytes,
            "input_records": input_count,
            "input_bytes": input_bytes,
            "output_records": total_matched,
        },
        determinism=determinism,
    )

    if len(requested_artifact_ids) == 1:
        lineage: dict[str, Any] = {
            "scope": "all_related",
            "anchor_artifact_id": anchor_artifact_id,
            "artifact_count": len(related_ids),
            "artifact_ids": related_ids,
            "related_set_hash": related_set_hash,
        }
    else:
        lineage = {
            "scope": "all_related",
            "anchor_artifact_ids": requested_artifact_ids,
            "root_paths": requested_root_paths,
            "artifact_count": len(related_ids),
            "artifact_ids": related_ids,
            "related_set_hashes": related_set_hashes,
        }
    response["scope"] = "all_related"
    response["lineage"] = lineage
    if warnings:
        response["warnings"] = warnings

    _logger.info(
        LogEvents.CODEGEN_COMPLETED,
        artifact_id=anchor_artifact_id,
        root_path=root_path_log,
        input_records=input_count,
        output_records=total_matched,
        truncated=False,
    )
    return response
