"""Handle invocations of mirrored upstream tools.

Orchestrate the full lifecycle for a proxied tool call: validate
gateway context, check the deduplication cache, enforce storage
quotas, call the upstream, persist the artifact envelope, and
trigger mapping.  Exports ``handle_mirrored_tool``.
"""

from __future__ import annotations

import asyncio
import dataclasses
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
from sidepouch_mcp.envelope.model import (
    Envelope,
    JsonContentPart,
)
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
from sidepouch_mcp.mcp.upstream_errors import (
    classify_upstream_exception,
)
from sidepouch_mcp.obs.logging import get_logger
from sidepouch_mcp.pagination.contract import (
    PAGINATION_WARNING_INCOMPLETE_RESULT_SET,
    RETRIEVAL_STATUS_PARTIAL,
    UPSTREAM_PARTIAL_REASON_MORE_PAGES_AVAILABLE,
    UPSTREAM_PARTIAL_REASON_SIGNAL_INCONCLUSIVE,
    build_upstream_pagination_meta,
)
from sidepouch_mcp.pagination.extract import (
    PaginationAssessment,
    PaginationState,
    assess_pagination,
)
from sidepouch_mcp.request_identity import compute_request_identity
from sidepouch_mcp.sessions import upsert_artifact_ref, upsert_session
from sidepouch_mcp.tools.artifact_describe import (
    FETCH_DESCRIBE_SQL,
    FETCH_ROOTS_SQL,
    build_describe_response,
)
from sidepouch_mcp.tools.usage_hint import (
    build_usage_hint,
    with_pagination_completeness_rule,
)

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
            to ``"normal"``).

    Returns:
        Normalized mode:
        ``"normal"``, ``"bypass"``, or ``"refresh"``.
        Returns ``None`` when the value is unrecognized.
    """
    aliases = {
        "allow": "normal",
        "normal": "normal",
        "fresh": "bypass",
        "bypass": "bypass",
        "refresh": "refresh",
    }
    if context is None:
        return "normal"
    raw = context.get("cache_mode", "normal")
    if isinstance(raw, str):
        return aliases.get(raw)
    return aliases.get(str(raw))


def _cache_mode_allows_reuse(cache_mode: str) -> bool:
    """Return ``True`` when cache reuse should be attempted."""
    return cache_mode == "normal"


def _cache_mode_skip_reason(cache_mode: str) -> str:
    """Return machine-readable reason when reuse is skipped."""
    if cache_mode == "refresh":
        return "cache_refresh_requested"
    if cache_mode == "bypass":
        return "cache_bypass_requested"
    return "cache_miss"


def _storage_cache_mode(cache_mode: str) -> str:
    """Map normalized cache mode to persisted artifact cache mode field."""
    if cache_mode == "normal":
        return "allow"
    return "fresh"


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

_CACHED_PAGINATION_COLUMNS = [
    "chain_seq",
    "envelope",
    "payload_hash_full",
    "envelope_canonical_encoding",
    "envelope_canonical_bytes",
]

_FETCH_CACHED_PAGINATION_SQL = """
SELECT a.chain_seq, pb.envelope, a.payload_hash_full,
       pb.envelope_canonical_encoding,
       pb.envelope_canonical_bytes
FROM artifacts a
JOIN payload_blobs pb ON pb.workspace_id = a.workspace_id
    AND pb.payload_hash_full = a.payload_hash_full
WHERE a.workspace_id = %s AND a.artifact_id = %s
"""


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


def _inject_pagination_state(
    envelope: Envelope,
    upstream_config: Any,
    forwarded_args: dict[str, Any],
    upstream_prefix: str,
    page_number: int = 0,
) -> tuple[Envelope, PaginationAssessment | None]:
    """Extract pagination signals and inject state into meta.

    Inspects the first ``JsonContentPart`` in the envelope for
    pagination indicators using the upstream's pagination config.
    When a next page is detected, creates a new envelope with
    the pagination state stored in ``meta["_gateway_pagination"]``.

    Args:
        envelope: The envelope from the upstream response.
        upstream_config: The upstream's ``UpstreamConfig``.
        forwarded_args: Original tool arguments (reserved keys
            already stripped).
        upstream_prefix: Upstream namespace prefix.
        page_number: Zero-based page number of this response.

    Returns:
        A ``(envelope, assessment)`` tuple.  The envelope may
        be replaced if pagination state was injected.  The
        assessment is ``None`` when no pagination config is
        present on the upstream.
    """
    pagination_config = upstream_config.pagination
    if pagination_config is None:
        return envelope, None

    if envelope.status == "error":
        assessment = PaginationAssessment(
            state=None,
            has_more=False,
            retrieval_status=RETRIEVAL_STATUS_PARTIAL,
            partial_reason=UPSTREAM_PARTIAL_REASON_SIGNAL_INCONCLUSIVE,
            warning=PAGINATION_WARNING_INCOMPLETE_RESULT_SET,
            page_number=page_number,
        )
        return envelope, assessment

    json_value = None
    for part in envelope.content:
        if isinstance(part, JsonContentPart):
            json_value = part.value
            break

    if json_value is None:
        assessment = PaginationAssessment(
            state=None,
            has_more=False,
            retrieval_status=RETRIEVAL_STATUS_PARTIAL,
            partial_reason=UPSTREAM_PARTIAL_REASON_SIGNAL_INCONCLUSIVE,
            warning=PAGINATION_WARNING_INCOMPLETE_RESULT_SET,
            page_number=page_number,
        )
        return envelope, assessment

    assessment = assess_pagination(
        json_value=json_value,
        pagination_config=pagination_config,
        original_args=forwarded_args,
        upstream_prefix=upstream_prefix,
        tool_name=envelope.tool,
        page_number=page_number,
    )
    if assessment.state is None:
        return envelope, assessment

    new_meta = {
        **envelope.meta,
        "_gateway_pagination": assessment.state.to_dict(),
    }
    return dataclasses.replace(envelope, meta=new_meta), assessment


def _pagination_response_meta(
    assessment: PaginationAssessment,
    artifact_id: str,
) -> dict[str, Any]:
    """Build pagination metadata for a gateway tool response.

    Args:
        assessment: Canonical pagination assessment.
        artifact_id: The artifact ID for this page.

    Returns:
        Dict with pagination info for the LLM.
    """
    has_next_page = assessment.has_more and assessment.state is not None
    page_number = assessment.page_number
    base = build_upstream_pagination_meta(
        artifact_id=artifact_id,
        page_number=page_number,
        retrieval_status=assessment.retrieval_status,
        has_more=assessment.has_more,
        partial_reason=assessment.partial_reason,
        warning=assessment.warning,
        has_next_page=has_next_page,
    )
    hint = base.get("hint")
    if isinstance(hint, str):
        base["hint"] = with_pagination_completeness_rule(hint)
    return base


def _cached_pagination_meta_for_reuse(
    connection: Any,
    *,
    artifact_id: str,
    mirrored: MirroredTool,
    forwarded_args: dict[str, Any],
) -> dict[str, Any] | None:
    """Rebuild pagination metadata for a reused cached artifact.

    Args:
        connection: Active database connection.
        artifact_id: Reused artifact ID.
        mirrored: Mirrored tool descriptor.
        forwarded_args: Forwarded upstream arguments for this request.

    Returns:
        Pagination metadata dict, or ``None`` when pagination is not
        configured or cannot be reconstructed.
    """
    pagination_config = mirrored.upstream.config.pagination
    if pagination_config is None:
        return None

    row = row_to_dict(
        connection.execute(
            _FETCH_CACHED_PAGINATION_SQL,
            (WORKSPACE_ID, artifact_id),
        ).fetchone(),
        _CACHED_PAGINATION_COLUMNS,
    )
    if row is None:
        return None

    page_number_raw = row.get("chain_seq")
    page_number = (
        page_number_raw
        if isinstance(page_number_raw, int) and page_number_raw >= 0
        else 0
    )

    def _load_envelope_dict() -> dict[str, Any] | None:
        envelope_raw = row.get("envelope")
        envelope_dict: dict[str, Any] | None = None
        if isinstance(envelope_raw, dict):
            envelope_dict = envelope_raw
        elif isinstance(envelope_raw, str):
            try:
                decoded = json.loads(envelope_raw)
            except (json.JSONDecodeError, ValueError):
                return None
            if isinstance(decoded, dict):
                envelope_dict = decoded
        if envelope_dict is not None:
            return envelope_dict

        canonical_bytes_raw = row.get("envelope_canonical_bytes")
        if canonical_bytes_raw is None:
            return None
        from sidepouch_mcp.storage.payload_store import reconstruct_envelope

        try:
            return reconstruct_envelope(
                compressed_bytes=bytes(canonical_bytes_raw),
                encoding=str(
                    row.get("envelope_canonical_encoding", "none")
                ),
                expected_hash=str(row.get("payload_hash_full", "")),
            )
        except ValueError:
            return None

    envelope_dict = _load_envelope_dict()
    if envelope_dict is None:
        return None

    meta = envelope_dict.get("meta")
    if isinstance(meta, dict):
        pagination_raw = meta.get("_gateway_pagination")
        if isinstance(pagination_raw, dict):
            try:
                stored_state = PaginationState.from_dict(pagination_raw)
            except (TypeError, ValueError, KeyError):
                stored_state = None
            if stored_state is not None:
                assessment = PaginationAssessment(
                    state=stored_state,
                    has_more=True,
                    retrieval_status=RETRIEVAL_STATUS_PARTIAL,
                    partial_reason=UPSTREAM_PARTIAL_REASON_MORE_PAGES_AVAILABLE,
                    warning=PAGINATION_WARNING_INCOMPLETE_RESULT_SET,
                    page_number=stored_state.page_number,
                )
                return _pagination_response_meta(assessment, artifact_id)

    if envelope_dict.get("status") == "error":
        assessment = PaginationAssessment(
            state=None,
            has_more=False,
            retrieval_status=RETRIEVAL_STATUS_PARTIAL,
            partial_reason=UPSTREAM_PARTIAL_REASON_SIGNAL_INCONCLUSIVE,
            warning=PAGINATION_WARNING_INCOMPLETE_RESULT_SET,
            page_number=page_number,
        )
        return _pagination_response_meta(assessment, artifact_id)

    json_value = None
    content = envelope_dict.get("content")
    if isinstance(content, list):
        for part in content:
            if (
                isinstance(part, dict)
                and part.get("type") == "json"
                and "value" in part
            ):
                json_value = part["value"]
                break
    if json_value is None:
        assessment = PaginationAssessment(
            state=None,
            has_more=False,
            retrieval_status=RETRIEVAL_STATUS_PARTIAL,
            partial_reason=UPSTREAM_PARTIAL_REASON_SIGNAL_INCONCLUSIVE,
            warning=PAGINATION_WARNING_INCOMPLETE_RESULT_SET,
            page_number=page_number,
        )
        return _pagination_response_meta(assessment, artifact_id)

    assessment = assess_pagination(
        json_value=json_value,
        pagination_config=pagination_config,
        original_args=forwarded_args,
        upstream_prefix=mirrored.prefix,
        tool_name=mirrored.original_name,
        page_number=page_number,
    )
    return _pagination_response_meta(assessment, artifact_id)


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
            "invalid _gateway_context.cache_mode; expected "
            "normal|bypass|refresh (or allow|fresh aliases)",
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
            cache_mode=_storage_cache_mode(cache_mode),
        )

    reuse = ReuseResult(reused=False)
    cache_reason = _cache_mode_skip_reason(cache_mode)
    pagination_assessment: PaginationAssessment | None = None
    if ctx.db_pool is None:
        try:
            upstream_result = await ctx._call_upstream_with_metrics(
                mirrored=mirrored,
                forwarded_args=forwarded_args,
            )
        except Exception as exc:
            error_code = classify_upstream_exception(exc)
            error_text = _truncate_error_text(
                str(exc), ctx.config.max_upstream_error_capture_bytes
            )
            upstream_result = {
                "content": [{"type": "text", "text": error_text}],
                "structuredContent": None,
                "isError": True,
                "meta": {
                    "exception_type": type(exc).__name__,
                    "gateway_error_code": error_code,
                    "gateway_error_detail": error_text,
                },
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

        page_number = chain_seq or 0
        envelope, pagination_assessment = _inject_pagination_state(
            envelope,
            mirrored.upstream.config,
            forwarded_args,
            mirrored.prefix,
            page_number=page_number,
        )

        # Check passthrough eligibility (DB-less path).
        # Only force a handle when next-page chaining is actually
        # available; without DB backing, artifact.next_page is not
        # implemented.
        _, _, payload_total = compute_payload_sizes(envelope)
        passthrough_eligible = can_passthrough(
            payload_total_bytes=payload_total,
            contains_binary_refs=envelope.contains_binary_refs,
            passthrough_allowed=mirrored.upstream.config.passthrough_allowed,
            max_bytes=ctx.config.passthrough_max_bytes,
        )
        pagination_requires_handle = (
            pagination_assessment is not None
            and pagination_assessment.state is not None
            and ctx.db_pool is not None
        )
        if passthrough_eligible and not pagination_requires_handle:
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
        if _cache_mode_allows_reuse(cache_mode):
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
                            ref_attached = False
                            try:
                                upsert_session(connection, session_id)
                                upsert_artifact_ref(
                                    connection,
                                    session_id,
                                    reuse.artifact_id,
                                )
                                connection.commit()
                                ref_attached = True
                            except (
                                _PG_OPERATIONAL_ERROR,
                                _PG_INTERFACE_ERROR,
                            ):
                                raise
                            except Exception:
                                get_logger(component="mcp.handlers").warning(
                                    "artifact_ref upsert on cache hit failed",
                                    exc_info=True,
                                )
                                cache_reason = "artifact_ref_attach_failed"
                            if ref_attached:
                                ctx._increment_metric("cache_hits")
                                cache_reason = (
                                    reuse.reason or "request_key_match"
                                )
                                desc, hint = _fetch_inline_describe(
                                    connection,
                                    reuse.artifact_id,
                                )
                                pagination_meta = None
                                try:
                                    pagination_meta = (
                                        _cached_pagination_meta_for_reuse(
                                            connection,
                                            artifact_id=reuse.artifact_id,
                                            mirrored=mirrored,
                                            forwarded_args=forwarded_args,
                                        )
                                    )
                                except Exception:
                                    get_logger(
                                        component="mcp.handlers"
                                    ).warning(
                                        "cached pagination rebuild failed "
                                        "(best-effort)",
                                        exc_info=True,
                                    )
                                return gateway_tool_result(
                                    artifact_id=reuse.artifact_id,
                                    cache_meta={
                                        "reused": True,
                                        "reason": cache_reason,
                                        "request_key": identity.request_key,
                                        "artifact_id_origin": "cache",
                                        "cache_mode": cache_mode,
                                    },
                                    describe=desc,
                                    usage_hint=hint,
                                    pagination=pagination_meta,
                                )
                            ctx._increment_metric("cache_misses")
                            reuse = ReuseResult(
                                reused=False,
                                reason=cache_reason,
                            )
                        else:
                            if reuse.reason is not None:
                                cache_reason = reuse.reason
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
        else:
            cache_reason = _cache_mode_skip_reason(cache_mode)

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
            error_code = classify_upstream_exception(exc)
            error_text = _truncate_error_text(
                str(exc), ctx.config.max_upstream_error_capture_bytes
            )
            upstream_result = {
                "content": [{"type": "text", "text": error_text}],
                "structuredContent": None,
                "isError": True,
                "meta": {
                    "exception_type": type(exc).__name__,
                    "gateway_error_code": error_code,
                    "gateway_error_detail": error_text,
                },
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

        page_number = chain_seq or 0
        envelope, pagination_assessment = _inject_pagination_state(
            envelope,
            mirrored.upstream.config,
            forwarded_args,
            mirrored.prefix,
            page_number=page_number,
        )

        # Phase 2.5: Passthrough check — if the result is small enough,
        # return the raw upstream result immediately and persist async.
        # Skip passthrough when pagination is detected — the LLM
        # needs the artifact_id to call artifact.next_page.
        _, _, payload_total = compute_payload_sizes(envelope)
        passthrough_eligible = can_passthrough(
            payload_total_bytes=payload_total,
            contains_binary_refs=envelope.contains_binary_refs,
            passthrough_allowed=mirrored.upstream.config.passthrough_allowed,
            max_bytes=ctx.config.passthrough_max_bytes,
        )
        if passthrough_eligible and pagination_assessment is None:
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

    pagination_meta = None
    pagination_assessment_for_response = pagination_assessment
    if (
        pagination_assessment_for_response is not None
        and ctx.db_pool is None
        and pagination_assessment_for_response.state is not None
    ):
        pagination_assessment_for_response = dataclasses.replace(
            pagination_assessment_for_response,
            state=None,
        )
    if pagination_assessment_for_response is not None:
        pagination_meta = _pagination_response_meta(
            pagination_assessment_for_response, handle.artifact_id
        )

    return gateway_tool_result(
        artifact_id=handle.artifact_id,
        cache_meta={
            "reused": False,
            "reason": cache_reason,
            "request_key": identity.request_key,
            "artifact_id_origin": "fresh",
            "cache_mode": cache_mode,
        },
        describe=desc,
        usage_hint=hint,
        pagination=pagination_meta,
    )
