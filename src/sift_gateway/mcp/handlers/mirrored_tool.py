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
from sift_gateway.envelope.content_extract import (
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
from sift_gateway.mcp.async_db import run_sync_db
from sift_gateway.mcp.handlers.mirrored_describe import (
    _describe_has_ready_schema,
    _fetch_inline_describe,
    _minimal_describe,
    _queryable_root_paths_from_schemas,
    _schema_payload_from_describe,
)
from sift_gateway.mcp.handlers.mirrored_pagination import (
    _build_cardinality_summary,
    _detect_duplicate_page_warning,
    _inject_pagination_state,
    _pagination_response_meta,
    _representative_schema_ref_sample,
    _validate_cursor_argument,
)
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
from sift_gateway.pagination.extract import (
    PaginationAssessment,
)
from sift_gateway.request_identity import compute_request_identity
from sift_gateway.tools.usage_hint import (
    build_code_query_usage,
    schema_primary_root_path,
)

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


_logger = get_logger(component="mcp.handlers")


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

    upstream_args, upstream_args_error = await run_sync_db(
        _resolve_upstream_args,
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
    persist_result, persist_error = await run_sync_db(
        _persist_and_describe,
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
