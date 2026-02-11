"""Handle invocations of mirrored upstream tools.

Orchestrate the full lifecycle for a proxied tool call: validate
gateway context, check the deduplication cache, enforce storage
quotas, call the upstream, persist the artifact envelope, and
trigger mapping.  Exports ``handle_mirrored_tool``.
"""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Any

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
from sidepouch_mcp.constants import WORKSPACE_ID
from sidepouch_mcp.envelope.model import Envelope
from sidepouch_mcp.envelope.responses import (
    can_passthrough,
    gateway_error,
    gateway_tool_result,
)
from sidepouch_mcp.jobs.quota import QuotaBreaches, enforce_quota
from sidepouch_mcp.mcp.handlers.common import (
    ROOT_COLUMNS,
    row_to_dict,
    rows_to_dicts,
)
from sidepouch_mcp.mcp.mirror import (
    MirroredTool,
    extract_gateway_context,
    strip_reserved_gateway_args,
    validate_against_schema,
)
from sidepouch_mcp.obs.logging import get_logger
from sidepouch_mcp.request_identity import compute_request_identity
from sidepouch_mcp.sessions import upsert_artifact_ref
from sidepouch_mcp.tools.artifact_describe import (
    FETCH_DESCRIBE_SQL,
    FETCH_ROOTS_SQL,
    build_describe_response,
)
from sidepouch_mcp.tools.usage_hint import build_usage_hint

if TYPE_CHECKING:
    from sidepouch_mcp.mcp.server import GatewayServer


class _NeverRaised(Exception):
    """Sentinel exception that is never raised.

    Used as the fallback when ``psycopg`` is not installed so
    that ``except`` clauses referencing Postgres-specific
    exceptions remain syntactically valid without catching
    anything.
    """


try:
    import psycopg

    _PG_OPERATIONAL_ERROR: type[BaseException] = psycopg.OperationalError
    _PG_INTERFACE_ERROR: type[BaseException] = psycopg.InterfaceError
except ImportError:
    _PG_OPERATIONAL_ERROR = _NeverRaised  # type: ignore[assignment,misc]
    _PG_INTERFACE_ERROR = _NeverRaised  # type: ignore[assignment,misc]


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


def _fetch_inline_describe(
    connection: Any,
    artifact_id: str,
) -> tuple[dict[str, Any], str]:
    """Fetch describe data and build a usage hint on a connection.

    Queries the artifact and roots tables on the already-open
    *connection* and returns the full describe dict plus a
    heuristic usage hint string.  Falls back to a minimal
    describe on any error so callers always get a result.

    Args:
        connection: Active database connection.
        artifact_id: The artifact to describe.

    Returns:
        A ``(describe_dict, usage_hint)`` tuple.
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
        roots = rows_to_dicts(
            connection.execute(
                FETCH_ROOTS_SQL,
                (WORKSPACE_ID, artifact_id),
            ).fetchall(),
            ROOT_COLUMNS,
        )
        describe = build_describe_response(artifact_row, roots)
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
    return describe, build_usage_hint(artifact_id, describe)


def _minimal_describe(
    artifact_id: str,
) -> tuple[dict[str, Any], str]:
    """Build a minimal describe for DB-less or error paths.

    Args:
        artifact_id: The artifact identifier.

    Returns:
        A ``(describe_dict, usage_hint)`` tuple with empty roots.
    """
    describe = build_describe_response(
        {
            "artifact_id": artifact_id,
            "map_kind": "none",
            "map_status": "pending",
        },
        [],
    )
    return describe, build_usage_hint(artifact_id, describe)


async def _persist_async(
    ctx: GatewayServer,
    input_data: CreateArtifactInput,
    envelope: Envelope,
    binary_refs: list[Any] | None = None,
) -> None:
    """Persist an artifact asynchronously on a best-effort basis.

    Used for passthrough-eligible responses where the raw
    upstream result is returned immediately.  Failures are
    logged but do not propagate.

    Args:
        ctx: Gateway server providing DB pool and helpers.
        input_data: Pre-built artifact creation input.
        envelope: The envelope to persist.
        binary_refs: Optional ``BinaryRef`` objects from
            oversize replacement to insert into
            ``binary_blobs``.
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
                binary_refs=binary_refs,
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
            error_text = _truncate_error_text(
                str(exc), ctx.config.max_upstream_error_capture_bytes
            )
            upstream_result = {
                "content": [{"type": "text", "text": error_text}],
                "structuredContent": None,
                "isError": True,
                "meta": {"exception_type": type(exc).__name__},
            }

        try:
            envelope, _binary_refs = ctx._envelope_from_upstream_result(
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
        desc, hint = _minimal_describe(handle.artifact_id)
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
                            desc, hint = _fetch_inline_describe(
                                connection,
                                reuse.artifact_id,
                            )
                            return gateway_tool_result(
                                artifact_id=reuse.artifact_id,
                                cache_meta={
                                    "reused": True,
                                    "reason": reuse.reason
                                    or "request_key_match",
                                    "request_key": identity.request_key,
                                },
                                describe=desc,
                                usage_hint=hint,
                            )
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
                blobs_root = (
                    ctx.blob_store.blobs_bin_dir
                    if ctx.blob_store is not None
                    else None
                )
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
                        blobs_root=blobs_root,
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
            error_text = _truncate_error_text(
                str(exc), ctx.config.max_upstream_error_capture_bytes
            )
            upstream_result = {
                "content": [{"type": "text", "text": error_text}],
                "structuredContent": None,
                "isError": True,
                "meta": {"exception_type": type(exc).__name__},
            }

        try:
            envelope, binary_refs = ctx._envelope_from_upstream_result(
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
                _persist_async(
                    ctx,
                    _create_input(envelope),
                    envelope,
                    binary_refs=binary_refs or None,
                )
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
                    binary_refs=binary_refs or None,
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
                # Phase 5: Inline describe — fetch roots on
                # the same connection (2 indexed lookups).
                desc, hint = _fetch_inline_describe(
                    connection, handle.artifact_id
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
        describe=desc,
        usage_hint=hint,
    )
