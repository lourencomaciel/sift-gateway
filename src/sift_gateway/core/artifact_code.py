"""Protocol-agnostic artifact code-query execution service."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from importlib.metadata import packages_distributions
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
from sift_gateway.codegen.validate import validate_code_for_execution
from sift_gateway.constants import (
    TRAVERSAL_CONTRACT_VERSION,
)
from sift_gateway.core.artifact_code_internal import (
    _append_overlapping_dataset_warning,
    _append_sampled_warning,
    _CodeCollectionState,
    _collect_code_inputs,
    _helper_enrich_entrypoint_hint,
    _helper_enrich_install_hint,
    _helper_module_to_dist,
    _parse_code_args,
    _ParsedCodeArgs,
)
from sift_gateway.core.artifact_describe import execute_artifact_describe
from sift_gateway.core.runtime import ArtifactCodeRuntime
from sift_gateway.envelope.responses import (
    gateway_error,
    gateway_tool_result,
    select_response_mode,
)
from sift_gateway.obs.logging import LogEvents, get_logger
from sift_gateway.response_sample import build_representative_item_sample
from sift_gateway.tools.artifact_get import FETCH_ARTIFACT_SQL
from sift_gateway.tools.artifact_schema import FETCH_SCHEMA_FIELDS_SQL
from sift_gateway.tools.artifact_select import (
    FETCH_SAMPLES_SQL,
)

_logger = get_logger(component="artifact.codegen")


def _hash_text(value: str) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def _hash_json(value: Any) -> str:
    digest = hashlib.sha256(canonical_bytes(coerce_floats(value))).hexdigest()
    return f"sha256:{digest}"


_STDLIB_ROOTS = sys.stdlib_module_names


def _module_to_dist(root: str) -> str:
    """Map an import root to its pip distribution name."""
    return _helper_module_to_dist(
        root,
        packages_distributions_fn=packages_distributions,
    )


def _enrich_install_hint(msg: str) -> str:
    """Append an agent-actionable install hint when possible."""
    return _helper_enrich_install_hint(
        msg,
        packages_distributions_fn=packages_distributions,
        stdlib_roots=_STDLIB_ROOTS,
    )


def _enrich_entrypoint_hint(
    msg: str,
    *,
    details_code: str | None,
    multi_artifact: bool,
) -> str:
    """Append an entrypoint-shape hint for missing run(...) errors."""
    return _helper_enrich_entrypoint_hint(
        msg,
        details_code=details_code,
        multi_artifact=multi_artifact,
    )


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
        runtime_error_message = _enrich_entrypoint_hint(
            str(exc),
            details_code=exc.code,
            multi_artifact=len(request.requested_artifact_ids) > 1,
        )
        return None, _code_error(
            _enrich_install_hint(runtime_error_message),
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

    representative_sample = build_representative_item_sample(normalized_items)
    schemas: list[dict[str, Any]] = []
    if representative_sample is None:
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
                (
                    "code response describe failed; continuing without schema "
                    "metadata"
                ),
                artifact_id=derived_artifact_id,
                error_type=type(exc).__name__,
                exc_info=True,
            )
        else:
            if isinstance(describe_result, dict):
                describe = describe_result
        if describe is not None:
            raw_schemas = describe.get("schemas")
            if isinstance(raw_schemas, list):
                schemas = [
                    schema for schema in raw_schemas if isinstance(schema, dict)
                ]

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
        schemas=schemas,
        lineage=lineage,
        metadata=metadata,
    )
    if representative_sample is not None:
        schema_ref_payload.pop("schemas", None)
        schema_ref_payload.update(representative_sample)
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


def _prepare_code_request_state(
    *,
    runtime: ArtifactCodeRuntime,
    arguments: dict[str, Any],
) -> tuple[
    _CodeRequest | None, _CodeCollectionState | None, dict[str, Any] | None
]:
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

    # Pre-validate code AST before launching subprocess.
    # NOTE: The subprocess also validates via validate_code_ast; this
    # intentionally duplicates that check so invalid code is rejected
    # without the overhead of spawning a child process.
    validation = validate_code_for_execution(
        request.code,
        allowed_import_roots_override=runtime.code_query_allowed_import_roots,
    )
    if not validation.valid:
        runtime.increment_metric("codegen_failure")
        error_message = validation.error_message or "code validation failed"
        if validation.error_code == "CODE_ENTRYPOINT_MISSING":
            error_message = _enrich_entrypoint_hint(
                error_message,
                details_code=validation.error_code,
                multi_artifact=(len(request.requested_artifact_ids) > 1),
            )
        if validation.error_code == "CODE_IMPORT_NOT_ALLOWED":
            error_message = _enrich_install_hint(error_message)
        _logger.info(
            LogEvents.CODEGEN_REJECTED,
            artifact_id=request.anchor_artifact_id,
            root_path=request.root_path_log,
            code=validation.error_code,
            message=error_message,
        )
        return _code_error(
            error_message,
            details_code=(validation.error_code or "CODE_AST_REJECTED"),
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
    "_enrich_entrypoint_hint",
    "_enrich_install_hint",
    "_module_to_dist",
    "execute_artifact_code",
]
