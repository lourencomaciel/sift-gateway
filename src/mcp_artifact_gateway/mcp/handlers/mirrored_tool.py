"""Mirrored upstream tool handler."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import psycopg

from mcp_artifact_gateway.artifacts.create import (
    CreateArtifactInput,
    compute_payload_sizes,
    persist_artifact,
)
from mcp_artifact_gateway.cache.reuse import (
    ReuseResult,
    acquire_advisory_lock_async,
)
from mcp_artifact_gateway.constants import RESPONSE_TYPE_RESULT
from mcp_artifact_gateway.envelope.model import Envelope
from mcp_artifact_gateway.envelope.responses import can_passthrough, gateway_error, gateway_tool_result
from mcp_artifact_gateway.mcp.mirror import (
    MirroredTool,
    extract_gateway_context,
    strip_reserved_gateway_args,
    validate_against_schema,
)
from mcp_artifact_gateway.jobs.quota import QuotaBreaches, enforce_quota
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


async def _persist_async(
    ctx: GatewayServer,
    input_data: CreateArtifactInput,
    envelope: Envelope,
) -> None:
    """Best-effort async artifact persistence (no mapping)."""
    try:
        if ctx.db_pool is None:
            return
        with ctx.db_pool.connection() as connection:
            binary_hashes = ctx._binary_hashes_from_envelope(envelope)
            persist_artifact(
                connection=connection,
                config=ctx.config,
                input_data=input_data,
                binary_hashes=binary_hashes,
            )
    except Exception:
        pass  # best-effort; fire-and-forget


async def handle_mirrored_tool(
    ctx: GatewayServer,
    mirrored: MirroredTool,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    # Pre-flight health gate: refuse artifact creation when gateway is unhealthy.
    # Probe before refusing — the failure that latched db_ok=False may have
    # been transient (e.g. PoolTimeout).
    if ctx.db_pool is not None and not ctx.db_ok:
        if not ctx._probe_db_recovery():
            return gateway_error(
                "INTERNAL",
                "gateway database is unhealthy; cannot create artifact",
            )
    if not ctx.fs_ok:
        return gateway_error(
            "INTERNAL",
            "gateway filesystem is unhealthy; cannot create artifact",
        )

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
        # Check passthrough eligibility (DB-less path)
        _, _, payload_total = compute_payload_sizes(envelope)
        passthrough_eligible = can_passthrough(
            payload_total_bytes=payload_total,
            contains_binary_refs=envelope.contains_binary_refs,
            passthrough_allowed=mirrored.upstream.config.passthrough_allowed,
            max_bytes=ctx.config.passthrough_max_bytes,
        )
        if passthrough_eligible:
            return upstream_result
        handle = ctx._build_non_persisted_handle(input_data=_create_input(envelope))
    else:
        # Phase 1: Cache check in a short-lived connection.
        # The advisory lock is transaction-scoped and released when this
        # connection closes, so there is a small window for duplicate
        # upstream calls.  This is an acceptable trade-off: pool starvation
        # from holding a connection during a 30 s upstream call is far worse
        # than an occasional redundant call (persist handles the race via
        # unique artifact IDs).
        if cache_mode != "fresh":
            try:
                with ctx.db_pool.connection() as connection:
                    acquired = await acquire_advisory_lock_async(
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
                        # TODO: Cache reuse returns the artifact_id without
                        # creating an artifact_ref for the requesting session.
                        # This means the caller receives an artifact_id they
                        # cannot access via get/search/select.  Consider
                        # inserting an artifact_ref here so the session that
                        # triggered the reuse can actually retrieve the artifact.
                        return {
                            "type": RESPONSE_TYPE_RESULT,
                            "artifact_id": reuse.artifact_id,
                            "meta": {
                                "cache": {
                                    "reused": True,
                                    "reason": reuse.reason or "request_key_match",
                                    "request_key": identity.request_key,
                                },
                            },
                        }
                    ctx._increment_metric("cache_misses")
            except (psycopg.OperationalError, psycopg.InterfaceError):
                ctx.db_ok = False
                return gateway_error(
                    "INTERNAL",
                    "cache check failed; gateway marked unhealthy",
                )
            except Exception:
                pass  # cache is best-effort; skip and proceed to upstream call

        # Phase 1.5: Quota enforcement preflight.
        # Rejecting here avoids invoking side-effecting upstream tools when
        # the workspace is already over hard quota and cannot be cleared.
        # Uses a separate connection (prune needs its own transaction).
        # Non-connectivity errors fail closed but do not mark DB unhealthy.
        quota_ok = True
        quota_breaches: QuotaBreaches | None = None
        if ctx.config.quota_enforcement_enabled:
            try:
                with ctx.db_pool.connection() as quota_conn:
                    quota_result = enforce_quota(
                        quota_conn,
                        max_binary_blob_bytes=ctx.config.max_binary_blob_bytes,
                        max_payload_total_bytes=ctx.config.max_payload_total_bytes,
                        max_total_storage_bytes=ctx.config.max_total_storage_bytes,
                        prune_batch_size=ctx.config.quota_prune_batch_size,
                        max_prune_rounds=ctx.config.quota_max_prune_rounds,
                        hard_delete_grace_seconds=ctx.config.quota_hard_delete_grace_seconds,
                        remove_fs_blobs=ctx.blob_store is not None,
                        metrics=ctx.metrics,
                    )
                    quota_breaches = quota_result.breaches_after or quota_result.breaches_before
                    if not quota_result.space_cleared:
                        quota_ok = False
            except (psycopg.OperationalError, psycopg.InterfaceError):
                ctx.db_ok = False
                return gateway_error(
                    "INTERNAL",
                    "quota check failed; gateway marked unhealthy",
                )
            except Exception:
                return gateway_error(
                    "INTERNAL",
                    "quota check failed; refusing to create artifact",
                )

        if not quota_ok:
            quota_details: dict[str, Any] = {"exceeded_caps": []}
            if quota_breaches is not None and quota_breaches.binary_blob_exceeded:
                quota_details["max_binary_blob_bytes"] = ctx.config.max_binary_blob_bytes
                quota_details["exceeded_caps"].append("max_binary_blob_bytes")
            if quota_breaches is not None and quota_breaches.payload_total_exceeded:
                quota_details["max_payload_total_bytes"] = ctx.config.max_payload_total_bytes
                quota_details["exceeded_caps"].append("max_payload_total_bytes")
            if quota_breaches is not None and quota_breaches.total_storage_exceeded:
                quota_details["max_total_storage_bytes"] = ctx.config.max_total_storage_bytes
                quota_details["exceeded_caps"].append("max_total_storage_bytes")
            if not quota_details["exceeded_caps"]:
                quota_details["max_total_storage_bytes"] = ctx.config.max_total_storage_bytes
                quota_details["exceeded_caps"].append("max_total_storage_bytes")
            return gateway_error(
                "QUOTA_EXCEEDED",
                "workspace storage quota exceeded; prune could not free enough space",
                details=quota_details,
            )

        # Phase 2: Upstream call — no DB connection held.
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

        # Phase 2.5: Passthrough check — if the result is small enough,
        # return the raw upstream result immediately and persist async.
        _, _, payload_total = compute_payload_sizes(envelope)
        passthrough_eligible = can_passthrough(
            payload_total_bytes=payload_total,
            contains_binary_refs=envelope.contains_binary_refs,
            passthrough_allowed=mirrored.upstream.config.passthrough_allowed,
            max_bytes=ctx.config.passthrough_max_bytes,
        )
        if passthrough_eligible:
            asyncio.create_task(_persist_async(ctx, _create_input(envelope), envelope))
            return upstream_result

        # Phase 3: Persist + Phase 4: Mapping in a single connection.
        # Reusing the same connection avoids a second pool checkout that
        # could silently fail (e.g. PoolTimeout under load), leaving the
        # artifact stuck at map_status='pending' indefinitely.
        try:
            with ctx.db_pool.connection() as connection:
                binary_hashes = ctx._binary_hashes_from_envelope(envelope)
                handle = persist_artifact(
                    connection=connection,
                    config=ctx.config,
                    input_data=_create_input(envelope),
                    binary_hashes=binary_hashes,
                )
                # Mapping runs after the artifact is committed.
                # Failures here are non-fatal — the artifact exists and
                # is retrievable.
                try:
                    ctx._trigger_mapping_for_artifact(
                        connection,
                        handle=handle,
                        envelope=envelope,
                    )
                except Exception:
                    pass  # mapping is best-effort; artifact already committed
        except (psycopg.OperationalError, psycopg.InterfaceError):
            ctx.db_ok = False
            return gateway_error(
                "INTERNAL",
                "artifact persistence failed; gateway marked unhealthy",
            )
        except Exception:
            return gateway_error(
                "INTERNAL",
                "artifact persistence failed",
            )

    return gateway_tool_result(
        artifact_id=handle.artifact_id,
        cache_meta={
            "reused": False,
            "reason": reuse.reason,
            "request_key": identity.request_key,
        },
    )
