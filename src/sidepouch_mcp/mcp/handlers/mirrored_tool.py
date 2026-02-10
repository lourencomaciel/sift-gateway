"""Handle invocations of mirrored upstream tools.

Orchestrate the full lifecycle for a proxied tool call: validate
gateway context, check the deduplication cache, enforce storage
quotas, call the upstream, persist the artifact envelope, and
trigger mapping.  Exports ``handle_mirrored_tool``.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

try:
    import psycopg

    _PG_OPERATIONAL_ERROR: type = psycopg.OperationalError
    _PG_INTERFACE_ERROR: type = psycopg.InterfaceError
except ImportError:
    _PG_OPERATIONAL_ERROR = type(None)  # type: ignore[assignment,misc]
    _PG_INTERFACE_ERROR = type(None)  # type: ignore[assignment,misc]

from sidepouch_mcp.artifacts.create import (
    CreateArtifactInput,
    compute_payload_sizes,
    persist_artifact,
)
from sidepouch_mcp.cache.reuse import (
    ReuseResult,
    acquire_advisory_lock_async,
    release_advisory_lock,
)
from sidepouch_mcp.constants import RESPONSE_TYPE_RESULT
from sidepouch_mcp.envelope.model import Envelope
from sidepouch_mcp.envelope.responses import (
    can_passthrough,
    gateway_error,
    gateway_tool_result,
)
from sidepouch_mcp.jobs.quota import QuotaBreaches, enforce_quota
from sidepouch_mcp.mcp.mirror import (
    MirroredTool,
    extract_gateway_context,
    strip_reserved_gateway_args,
    validate_against_schema,
)
from sidepouch_mcp.obs.logging import get_logger
from sidepouch_mcp.request_identity import compute_request_identity
from sidepouch_mcp.sessions import upsert_artifact_ref

if TYPE_CHECKING:
    from sidepouch_mcp.mcp.server import GatewayServer


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


def _lookup_cache_mode(context: dict[str, Any] | None) -> str | None:
    """Resolve the cache mode from the gateway context.

    Args:
        context: Gateway context dict, or ``None`` (defaults
            to ``"allow"``).

    Returns:
        ``"allow"`` or ``"fresh"`` when valid, ``None`` when
        the value is unrecognised.
    """
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
    """Persist an artifact asynchronously on a best-effort basis.

    Used for passthrough-eligible responses where the raw
    upstream result is returned immediately.  Failures are
    logged but do not propagate.

    Args:
        ctx: Gateway server providing DB pool and helpers.
        input_data: Pre-built artifact creation input.
        envelope: The envelope to persist.
    """
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
        get_logger(component="mcp.handlers").warning(
            "persist_artifact failed (best-effort)",
            exc_info=True,
        )


async def handle_mirrored_tool(
    ctx: GatewayServer,
    mirrored: MirroredTool,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Handle a mirrored upstream tool invocation.

    Orchestrates the full lifecycle: validate context, check
    the deduplication cache, enforce storage quotas, call the
    upstream, persist the artifact envelope, and trigger
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
        cache metadata, or a gateway error dict on failure.
    """
    # Pre-flight health gate: refuse artifact creation when
    # gateway is unhealthy. Probe before refusing -- the failure
    # that latched db_ok=False may have been transient.
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
    if parent_artifact_id is not None and not isinstance(
        parent_artifact_id, str
    ):
        return gateway_error(
            "INVALID_ARGUMENT",
            "_gateway_parent_artifact_id must be a string when provided",
        )
    chain_seq = arguments.get("_gateway_chain_seq")
    if chain_seq is not None and (
        not isinstance(chain_seq, int) or chain_seq < 0
    ):
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

    def _create_input(envelope: Envelope) -> CreateArtifactInput:
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
        handle = ctx._build_non_persisted_handle(
            input_data=_create_input(envelope)
        )
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
                                "timeout_ms": (
                                    ctx.config.advisory_lock_timeout_ms
                                ),
                            },
                        )
                    try:
                        reuse = ctx._check_reuse_on_connection(
                            connection,
                            request_key=identity.request_key,
                            expected_schema_hash=mirrored.upstream_tool.schema_hash,
                            strict_schema_reuse=mirrored.upstream.config.strict_schema_reuse,
                        )
                        if reuse.reused and reuse.artifact_id is not None:
                            ctx._increment_metric("cache_hits")
                            try:
                                upsert_artifact_ref(
                                    connection,
                                    session_id,
                                    reuse.artifact_id,
                                )
                                connection.commit()
                            except Exception:
                                get_logger(component="mcp.handlers").warning(
                                    "artifact_ref upsert on cache hit failed",
                                    exc_info=True,
                                )
                            return {
                                "type": RESPONSE_TYPE_RESULT,
                                "artifact_id": reuse.artifact_id,
                                "meta": {
                                    "cache": {
                                        "reused": True,
                                        "reason": reuse.reason
                                        or "request_key_match",
                                        "request_key": identity.request_key,
                                    },
                                },
                            }
                        ctx._increment_metric("cache_misses")
                    finally:
                        release_advisory_lock(
                            connection,
                            request_key=identity.request_key,
                        )
            except (_PG_OPERATIONAL_ERROR, _PG_INTERFACE_ERROR):
                ctx.db_ok = False
                return gateway_error(
                    "INTERNAL",
                    "cache check failed; gateway marked unhealthy",
                )
            except Exception:
                get_logger(component="mcp.handlers").warning(
                    "cache check failed (best-effort)",
                    exc_info=True,
                )

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
                    quota_breaches = (
                        quota_result.breaches_after
                        or quota_result.breaches_before
                    )
                    if not quota_result.space_cleared:
                        quota_ok = False
            except (_PG_OPERATIONAL_ERROR, _PG_INTERFACE_ERROR):
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
            if (
                quota_breaches is not None
                and quota_breaches.binary_blob_exceeded
            ):
                quota_details["max_binary_blob_bytes"] = (
                    ctx.config.max_binary_blob_bytes
                )
                quota_details["exceeded_caps"].append("max_binary_blob_bytes")
            if (
                quota_breaches is not None
                and quota_breaches.payload_total_exceeded
            ):
                quota_details["max_payload_total_bytes"] = (
                    ctx.config.max_payload_total_bytes
                )
                quota_details["exceeded_caps"].append("max_payload_total_bytes")
            if (
                quota_breaches is not None
                and quota_breaches.total_storage_exceeded
            ):
                quota_details["max_total_storage_bytes"] = (
                    ctx.config.max_total_storage_bytes
                )
                quota_details["exceeded_caps"].append("max_total_storage_bytes")
            if not quota_details["exceeded_caps"]:
                quota_details["max_total_storage_bytes"] = (
                    ctx.config.max_total_storage_bytes
                )
                quota_details["exceeded_caps"].append("max_total_storage_bytes")
            return gateway_error(
                "QUOTA_EXCEEDED",
                "workspace storage quota exceeded;"
                " prune could not free enough space",
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
            asyncio.create_task(
                _persist_async(ctx, _create_input(envelope), envelope)
            )
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
                    get_logger(component="mcp.handlers").warning(
                        "mapping failed (best-effort)",
                        exc_info=True,
                    )
        except (_PG_OPERATIONAL_ERROR, _PG_INTERFACE_ERROR):
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
