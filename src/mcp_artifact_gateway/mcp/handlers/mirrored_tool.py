"""Mirrored upstream tool handler."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from mcp_artifact_gateway.artifacts.create import (
    CreateArtifactInput,
    persist_artifact,
)
from mcp_artifact_gateway.cache.reuse import (
    ReuseResult,
    acquire_advisory_lock,
)
from mcp_artifact_gateway.constants import RESPONSE_TYPE_RESULT
from mcp_artifact_gateway.envelope.responses import gateway_error, gateway_tool_result
from mcp_artifact_gateway.mcp.mirror import (
    MirroredTool,
    extract_gateway_context,
    strip_reserved_gateway_args,
    validate_against_schema,
)
from mcp_artifact_gateway.mcp.upstream import call_upstream_tool
from mcp_artifact_gateway.request_identity import compute_request_identity

if TYPE_CHECKING:
    from mcp_artifact_gateway.mcp.server import GatewayServer


def _extract_session_id(context: dict[str, Any] | None) -> str | None:
    if context is None:
        return None
    session_id = context.get("session_id")
    if isinstance(session_id, str) and session_id:
        return session_id
    return None


def _lookup_cache_mode(context: dict[str, Any] | None) -> str | None:
    if context is None:
        return "allow"
    raw = context.get("cache_mode", "allow")
    if raw in {"allow", "fresh"}:
        return str(raw)
    return None


async def handle_mirrored_tool(
    ctx: GatewayServer,
    mirrored: MirroredTool,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    context = extract_gateway_context(arguments)
    session_id = _extract_session_id(context)
    if session_id is None:
        return gateway_error(
            "INVALID_ARGUMENT",
            "missing _gateway_context.session_id",
        )

    cache_mode = _lookup_cache_mode(context)
    if cache_mode is None:
        return gateway_error(
            "INVALID_ARGUMENT",
            "invalid _gateway_context.cache_mode; expected allow|fresh",
        )
    parent_artifact_id = arguments.get("_gateway_parent_artifact_id")
    if parent_artifact_id is not None and not isinstance(parent_artifact_id, str):
        return gateway_error(
            "INVALID_ARGUMENT",
            "_gateway_parent_artifact_id must be a string when provided",
        )
    chain_seq = arguments.get("_gateway_chain_seq")
    if chain_seq is not None and (not isinstance(chain_seq, int) or chain_seq < 0):
        return gateway_error(
            "INVALID_ARGUMENT",
            "_gateway_chain_seq must be a non-negative integer when provided",
        )

    forwarded_args = strip_reserved_gateway_args(arguments)
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

    identity = compute_request_identity(
        upstream_instance_id=mirrored.upstream.instance_id,
        prefix=mirrored.prefix,
        tool_name=mirrored.original_name,
        forwarded_args=forwarded_args,
    )

    def _create_input(envelope):  # type: ignore[no-untyped-def]
        return CreateArtifactInput(
            session_id=session_id,
            upstream_instance_id=mirrored.upstream.instance_id,
            prefix=mirrored.prefix,
            tool_name=mirrored.original_name,
            request_key=identity.request_key,
            request_args_hash=identity.request_args_hash,
            request_args_prefix=identity.request_args_prefix,
            upstream_tool_schema_hash=mirrored.upstream_tool.schema_hash,
            envelope=envelope,
            parent_artifact_id=parent_artifact_id,
            chain_seq=chain_seq,
            cache_mode=cache_mode,
        )

    reuse = ReuseResult(reused=False)
    if ctx.db_pool is None:
        try:
            upstream_result = await ctx._call_upstream_with_metrics(
                mirrored=mirrored,
                forwarded_args=forwarded_args,
            )
        except Exception as exc:
            upstream_result = {
                "content": [{"type": "text", "text": str(exc)}],
                "structuredContent": None,
                "isError": True,
                "meta": {"exception_type": type(exc).__name__},
            }

        try:
            envelope = ctx._envelope_from_upstream_result(
                mirrored=mirrored,
                upstream_result=upstream_result,
            )
        except ValueError as exc:
            return gateway_error(
                "UPSTREAM_RESPONSE_INVALID",
                str(exc),
            )
        handle = ctx._build_non_persisted_handle(input_data=_create_input(envelope))
    else:
        with ctx.db_pool.connection() as connection:
            if cache_mode != "fresh":
                acquired = acquire_advisory_lock(
                    connection,
                    request_key=identity.request_key,
                    timeout_ms=ctx.config.advisory_lock_timeout_ms,
                    metrics=ctx.metrics,
                )
                if not acquired:
                    return gateway_error(
                        "RESOURCE_BUSY",
                        "advisory lock acquisition timed out",
                        details={
                            "timeout_ms": ctx.config.advisory_lock_timeout_ms,
                        },
                    )
                reuse = ctx._check_reuse_on_connection(
                    connection,
                    request_key=identity.request_key,
                    expected_schema_hash=mirrored.upstream_tool.schema_hash,
                    strict_schema_reuse=mirrored.upstream.config.strict_schema_reuse,
                )
                if reuse.reused and reuse.artifact_id is not None:
                    ctx._increment_metric("cache_hits")
                    return {
                        "type": RESPONSE_TYPE_RESULT,
                        "artifact_id": reuse.artifact_id,
                        "meta": {
                            "inline": False,
                            "cache": {
                                "reused": True,
                                "reason": reuse.reason or "request_key_match",
                                "request_key": identity.request_key,
                            },
                        },
                    }
                ctx._increment_metric("cache_misses")

            try:
                upstream_result = await ctx._call_upstream_with_metrics(
                    mirrored=mirrored,
                    forwarded_args=forwarded_args,
                )
            except Exception as exc:
                upstream_result = {
                    "content": [{"type": "text", "text": str(exc)}],
                    "structuredContent": None,
                    "isError": True,
                    "meta": {"exception_type": type(exc).__name__},
                }

            try:
                envelope = ctx._envelope_from_upstream_result(
                    mirrored=mirrored,
                    upstream_result=upstream_result,
                )
            except ValueError as exc:
                return gateway_error(
                    "UPSTREAM_RESPONSE_INVALID",
                    str(exc),
                )
            binary_hashes = ctx._binary_hashes_from_envelope(envelope)
            handle = persist_artifact(
                connection=connection,
                config=ctx.config,
                input_data=_create_input(envelope),
                binary_hashes=binary_hashes,
            )
            ctx._trigger_mapping_for_artifact(
                connection,
                handle=handle,
                envelope=envelope,
            )

    return gateway_tool_result(
        artifact_id=handle.artifact_id,
        envelope=envelope,
        payload_json_bytes=handle.payload_json_bytes,
        payload_total_bytes=handle.payload_total_bytes,
        contains_binary_refs=handle.contains_binary_refs,
        inline_allowed=mirrored.upstream.config.inline_allowed,
        max_json_bytes=ctx.config.inline_envelope_max_json_bytes,
        max_total_bytes=ctx.config.inline_envelope_max_total_bytes,
        cache_meta={
            "reused": False,
            "reason": reuse.reason,
            "request_key": identity.request_key,
        },
    )
