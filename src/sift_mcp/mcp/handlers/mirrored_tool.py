"""Handle invocations of mirrored upstream tools.

Orchestrate the full lifecycle for a proxied tool call: validate
gateway context, check the deduplication cache, enforce storage
quotas, call the upstream, persist the artifact envelope, and
trigger mapping.  Exports ``handle_mirrored_tool``.
"""

from __future__ import annotations

import asyncio
import dataclasses
import importlib.util
import json
import sqlite3
import time
from typing import TYPE_CHECKING, Any

from sift_mcp.artifacts.create import (
    CreateArtifactInput,
    persist_artifact,
)
from sift_mcp.cache.reuse import (
    ReuseResult,
    acquire_advisory_lock_async,
    release_advisory_lock,
)
from sift_mcp.codegen.ast_guard import (
    ALLOWED_STDLIB_IMPORTS,
    allowed_import_roots,
)
from sift_mcp.constants import WORKSPACE_ID
from sift_mcp.envelope.model import (
    Envelope,
    JsonContentPart,
)
from sift_mcp.envelope.responses import (
    gateway_error,
    gateway_tool_result,
)
from sift_mcp.jobs.quota import QuotaBreaches, enforce_quota
from sift_mcp.mcp.handlers.common import (
    row_to_dict,
    rows_to_dicts,
)
from sift_mcp.mcp.mirror import (
    MirroredTool,
    extract_gateway_context,
    strip_reserved_gateway_args,
    validate_against_schema,
)
from sift_mcp.mcp.upstream_errors import (
    classify_upstream_exception,
)
from sift_mcp.obs.logging import LogEvents, get_logger
from sift_mcp.pagination.auto import (
    AutoPaginationResult,
    _count_json_records,
    _count_json_value_records,
    _extract_json_content,
    merge_envelopes,
    resolve_auto_paginate_limits,
)
from sift_mcp.pagination.contract import (
    PAGINATION_WARNING_INCOMPLETE_RESULT_SET,
    RETRIEVAL_STATUS_PARTIAL,
    UPSTREAM_PARTIAL_REASON_MORE_PAGES_AVAILABLE,
    UPSTREAM_PARTIAL_REASON_SIGNAL_INCONCLUSIVE,
    build_upstream_pagination_meta,
)
from sift_mcp.pagination.extract import (
    PaginationAssessment,
    PaginationState,
    assess_pagination,
)
from sift_mcp.request_identity import compute_request_identity
from sift_mcp.schema_compact import (
    SCHEMA_LEGEND,
    compact_schema_payload,
)
from sift_mcp.sessions import upsert_artifact_ref, upsert_session
from sift_mcp.tools.artifact_describe import (
    FETCH_DESCRIBE_SQL,
    FETCH_SCHEMA_FIELDS_SQL,
    FETCH_SCHEMA_ROOTS_SQL,
    build_describe_response,
)
from sift_mcp.tools.usage_hint import (
    build_usage_hint,
    with_pagination_completeness_rule,
)

if TYPE_CHECKING:
    from sift_mcp.mcp.server import GatewayServer


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
    _PG_OPERATIONAL_ERROR = _NeverRaised
    _PG_INTERFACE_ERROR = _NeverRaised

_DB_CONNECTIVITY_ERRORS: tuple[type[BaseException], ...] = (
    _PG_OPERATIONAL_ERROR,
    _PG_INTERFACE_ERROR,
    sqlite3.OperationalError,
    sqlite3.InterfaceError,
)


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


def _extract_allow_reuse(
    context: dict[str, Any] | None,
) -> bool:
    """Read the ``allow_reuse`` flag from the gateway context.

    Args:
        context: Gateway context dict, or ``None``.

    Returns:
        ``True`` when the caller opts into dedup reuse,
        ``False`` otherwise (the default).
    """
    if context is None:
        return False
    raw = context.get("allow_reuse", False)
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, str):
        return raw.lower() in ("true", "1", "yes")
    return bool(raw)


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

_SCHEMA_ROOT_COLUMNS = [
    "root_key",
    "root_path",
    "schema_version",
    "schema_hash",
    "mode",
    "completeness",
    "observed_records",
    "dataset_hash",
    "traversal_contract_version",
    "map_budget_fingerprint",
]

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

_logger = get_logger(component="mcp.handlers")

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


def _retain_primary_schema_if_unique(
    schemas: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Keep only the primary schema when coverage leader is unique.

    Primary is selected by ``coverage.observed_records``. If multiple
    schemas tie for highest observed count, returns all schemas.
    """
    if len(schemas) <= 1:
        return schemas

    def _observed(schema: dict[str, Any]) -> int:
        coverage = schema.get("coverage")
        if isinstance(coverage, dict):
            raw = coverage.get("observed_records")
            if isinstance(raw, int):
                return raw
        return 0

    scores = [_observed(schema) for schema in schemas]
    max_score = max(scores)
    leaders = [idx for idx, score in enumerate(scores) if score == max_score]
    if len(leaders) != 1:
        return schemas
    return [schemas[leaders[0]]]


def _available_code_query_packages(
    ctx: GatewayServer,
) -> list[str] | None:
    """Return available third-party package roots for code-query hints."""
    if not ctx.config.code_query_enabled:
        return None

    allowed_roots = sorted(
        allowed_import_roots(
            allow_analytics_imports=ctx.config.code_query_allow_analytics_imports,
            configured_roots=ctx.config.code_query_allowed_import_roots,
        )
    )
    package_roots = [
        root for root in allowed_roots if root not in ALLOWED_STDLIB_IMPORTS
    ]
    available_packages: list[str] = []
    for package_root in package_roots:
        try:
            spec = importlib.util.find_spec(package_root)
        except (ModuleNotFoundError, ImportError, ValueError):
            spec = None
        if spec is not None:
            available_packages.append(package_root)
    return available_packages


def _fetch_inline_describe(
    connection: Any,
    artifact_id: str,
    *,
    code_query_packages: list[str] | None = None,
) -> tuple[dict[str, Any], str]:
    """Fetch schema-first describe data and build a usage hint.

    Queries artifact and schema tables on the already-open
    *connection* and returns the full describe dict plus a
    heuristic usage hint string.  Falls back to a minimal
    describe on any error so callers always get a result.

    Args:
        connection: Active database connection.
        artifact_id: The artifact to describe.
        code_query_packages: Available third-party code-query
            packages to advertise in usage hints.

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
        schema_roots = rows_to_dicts(
            connection.execute(
                FETCH_SCHEMA_ROOTS_SQL,
                (WORKSPACE_ID, artifact_id),
            ).fetchall(),
            _SCHEMA_ROOT_COLUMNS,
        )
        schemas: list[dict[str, Any]] = []
        for schema_root in schema_roots:
            root_key = schema_root.get("root_key")
            if not isinstance(root_key, str):
                continue
            field_rows = rows_to_dicts(
                connection.execute(
                    FETCH_SCHEMA_FIELDS_SQL,
                    (WORKSPACE_ID, artifact_id, root_key),
                ).fetchall(),
                _SCHEMA_FIELD_COLUMNS,
            )
            fields: list[dict[str, Any]] = []
            for field in field_rows:
                raw_types = field.get("types")
                types = (
                    [str(item) for item in raw_types]
                    if isinstance(raw_types, list)
                    else []
                )
                observed_count_raw = field.get("observed_count")
                observed_count = (
                    int(observed_count_raw)
                    if isinstance(observed_count_raw, int)
                    else 0
                )
                fields.append(
                    {
                        "path": field.get("field_path"),
                        "types": types,
                        "nullable": bool(field.get("nullable")),
                        "required": bool(field.get("required")),
                        "observed_count": observed_count,
                        "example_value": (
                            str(field.get("example_value"))
                            if isinstance(field.get("example_value"), str)
                            else None
                        ),
                    }
                )
                distinct_values = field.get("distinct_values")
                if isinstance(distinct_values, list):
                    fields[-1]["distinct_values"] = list(distinct_values)
                cardinality = field.get("cardinality")
                if isinstance(cardinality, int):
                    fields[-1]["cardinality"] = cardinality
            observed_records_raw = schema_root.get("observed_records")
            observed_records = (
                int(observed_records_raw)
                if isinstance(observed_records_raw, int)
                else 0
            )
            schemas.append(
                {
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
            )
        describe = build_describe_response(
            artifact_row,
            [],
            schemas=_retain_primary_schema_if_unique(schemas),
        )
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
    return describe, build_usage_hint(
        artifact_id,
        describe,
        code_query_enabled=code_query_packages is not None,
        code_query_packages=code_query_packages,
    )


def _minimal_describe(
    artifact_id: str,
    *,
    code_query_packages: list[str] | None = None,
) -> tuple[dict[str, Any], str]:
    """Build a minimal describe for DB-less or error paths.

    Args:
        artifact_id: The artifact identifier.
        code_query_packages: Available third-party code-query
            packages to advertise in usage hints.

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
    return describe, build_usage_hint(
        artifact_id,
        describe,
        code_query_enabled=code_query_packages is not None,
        code_query_packages=code_query_packages,
    )


def _describe_has_ready_schema(describe: dict[str, Any]) -> bool:
    """Return True when describe payload includes ready schema data."""
    mapping = describe.get("mapping")
    if not isinstance(mapping, dict):
        return False
    if mapping.get("map_status") != "ready":
        return False
    schemas = describe.get("schemas")
    return isinstance(schemas, list) and bool(schemas)


def _schema_payload_from_describe(
    describe: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any] | None]:
    """Extract canonical schema-first payload fields from describe data."""
    mapping = describe.get("mapping")
    schemas = describe.get("schemas")
    mapping_payload: dict[str, Any] = (
        dict(mapping) if isinstance(mapping, dict) else {}
    )
    schema_payload_raw: list[dict[str, Any]] = (
        [item for item in schemas if isinstance(item, dict)]
        if isinstance(schemas, list)
        else []
    )
    schema_payload = compact_schema_payload(schema_payload_raw)
    schema_legend = SCHEMA_LEGEND if schema_payload else None
    return mapping_payload, schema_payload, schema_legend


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
        from sift_mcp.storage.payload_store import reconstruct_envelope

        try:
            return reconstruct_envelope(
                compressed_bytes=bytes(canonical_bytes_raw),
                encoding=str(row.get("envelope_canonical_encoding", "none")),
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


async def _auto_paginate_loop(
    ctx: GatewayServer,
    mirrored: MirroredTool,
    *,
    first_envelope: Envelope,
    first_assessment: PaginationAssessment,
    forwarded_args: dict[str, Any],
    max_pages: int,
    max_records: int,
    timeout: float,
) -> AutoPaginationResult:
    """Fetch additional upstream pages and merge into one envelope.

    Loops while pagination indicates more pages, respecting
    configured limits for page count, record count, and timeout.
    On upstream error or timeout, returns what was successfully
    fetched so far.

    Args:
        ctx: Gateway server instance.
        mirrored: Mirrored tool descriptor.
        first_envelope: Envelope from the initial upstream call.
        first_assessment: Pagination assessment for the first page.
        forwarded_args: Original forwarded arguments.
        max_pages: Maximum total pages to fetch.
        max_records: Maximum total records across all pages.
        timeout: Timeout in seconds for the entire loop.

    Returns:
        ``AutoPaginationResult`` with merged data.
    """
    log = get_logger(component="mcp.handlers.auto_paginate")
    started = time.monotonic()
    pages_fetched = 1
    total_records = _count_json_records(first_envelope)
    additional_json_values: list[Any] = []
    accumulated_binary_refs: list[Any] = []
    current_assessment = first_assessment

    while (
        pages_fetched < max_pages
        and total_records < max_records
        and current_assessment.has_more
        and current_assessment.state is not None
    ):
        elapsed = time.monotonic() - started
        if elapsed >= timeout:
            log.info(
                LogEvents.AUTO_PAGINATION_TIMEOUT,
                pages_fetched=pages_fetched,
                total_records=total_records,
                elapsed=elapsed,
            )
            return AutoPaginationResult(
                envelope=merge_envelopes(
                    first_envelope,
                    additional_json_values,
                    current_assessment,
                ),
                assessment=current_assessment,
                pages_fetched=pages_fetched,
                total_records=total_records,
                stopped_reason="timeout",
                binary_refs=accumulated_binary_refs,
            )

        state = current_assessment.state
        next_args = {
            **state.original_args,
            **state.next_params,
        }

        # Resolve artifact refs before sending upstream, matching
        # the main handler path (Phase 1.75).
        upstream_next_args = next_args
        if ctx.db_pool is not None:
            from sift_mcp.mcp.resolve_refs import (
                ResolveError,
                resolve_artifact_refs,
            )

            try:
                with ctx.db_pool.connection() as resolve_conn:
                    resolved = resolve_artifact_refs(resolve_conn, next_args)
                    if isinstance(resolved, ResolveError):
                        log.warning(
                            LogEvents.AUTO_PAGINATION_REF_RESOLUTION_ERROR,
                            error=resolved.message,
                        )
                        return AutoPaginationResult(
                            envelope=merge_envelopes(
                                first_envelope,
                                additional_json_values,
                                current_assessment,
                            ),
                            assessment=current_assessment,
                            pages_fetched=pages_fetched,
                            total_records=total_records,
                            stopped_reason="error",
                            binary_refs=accumulated_binary_refs,
                        )
                    upstream_next_args = resolved
            except Exception:
                log.warning(
                    LogEvents.AUTO_PAGINATION_REF_RESOLUTION_ERROR,
                    exc_info=True,
                )
                return AutoPaginationResult(
                    envelope=merge_envelopes(
                        first_envelope,
                        additional_json_values,
                        current_assessment,
                    ),
                    assessment=current_assessment,
                    pages_fetched=pages_fetched,
                    total_records=total_records,
                    stopped_reason="error",
                    binary_refs=accumulated_binary_refs,
                )

        remaining = timeout - (time.monotonic() - started)
        if remaining <= 0:
            remaining = 0.1  # Near-zero floor to avoid negative.
        try:
            upstream_result = await asyncio.wait_for(
                ctx._call_upstream_with_metrics(
                    mirrored=mirrored,
                    forwarded_args=upstream_next_args,
                ),
                timeout=remaining,
            )
        except asyncio.TimeoutError:
            log.info(
                LogEvents.AUTO_PAGINATION_UPSTREAM_TIMEOUT,
                pages_fetched=pages_fetched,
                remaining=remaining,
            )
            return AutoPaginationResult(
                envelope=merge_envelopes(
                    first_envelope,
                    additional_json_values,
                    current_assessment,
                ),
                assessment=current_assessment,
                pages_fetched=pages_fetched,
                total_records=total_records,
                stopped_reason="timeout",
                binary_refs=accumulated_binary_refs,
            )
        except Exception:
            log.warning(
                LogEvents.AUTO_PAGINATION_UPSTREAM_FAILURE,
                exc_info=True,
            )
            return AutoPaginationResult(
                envelope=merge_envelopes(
                    first_envelope,
                    additional_json_values,
                    current_assessment,
                ),
                assessment=current_assessment,
                pages_fetched=pages_fetched,
                total_records=total_records,
                stopped_reason="error",
                binary_refs=accumulated_binary_refs,
            )

        if bool(upstream_result.get("isError", False)):
            log.info(
                LogEvents.AUTO_PAGINATION_UPSTREAM_ERROR_RESULT,
                pages_fetched=pages_fetched,
            )
            return AutoPaginationResult(
                envelope=merge_envelopes(
                    first_envelope,
                    additional_json_values,
                    current_assessment,
                ),
                assessment=current_assessment,
                pages_fetched=pages_fetched,
                total_records=total_records,
                stopped_reason="error",
                binary_refs=accumulated_binary_refs,
            )

        try:
            page_envelope, page_binary_refs = (
                ctx._envelope_from_upstream_result(
                    mirrored=mirrored,
                    upstream_result=upstream_result,
                )
            )
        except ValueError:
            log.warning(
                LogEvents.AUTO_PAGINATION_ENVELOPE_NORMALIZATION_FAILED,
                exc_info=True,
            )
            return AutoPaginationResult(
                envelope=merge_envelopes(
                    first_envelope,
                    additional_json_values,
                    current_assessment,
                ),
                assessment=current_assessment,
                pages_fetched=pages_fetched,
                total_records=total_records,
                stopped_reason="error",
                binary_refs=accumulated_binary_refs,
            )
        accumulated_binary_refs.extend(page_binary_refs)

        page_number = state.page_number + 1
        page_envelope, page_assessment = _inject_pagination_state(
            page_envelope,
            mirrored.upstream.config,
            next_args,
            mirrored.prefix,
            page_number=page_number,
        )

        page_json = _extract_json_content(page_envelope)
        if page_json is None:
            log.info(
                LogEvents.AUTO_PAGINATION_BINARY_CONTENT_STOP,
                pages_fetched=pages_fetched,
            )
            return AutoPaginationResult(
                envelope=merge_envelopes(
                    first_envelope,
                    additional_json_values,
                    current_assessment,
                ),
                assessment=current_assessment,
                pages_fetched=pages_fetched,
                total_records=total_records,
                stopped_reason="binary_content",
                binary_refs=accumulated_binary_refs,
            )

        # Count full page records; max_records is a stop condition
        # for additional fetches, not a per-page trim budget.
        page_records = _count_json_value_records(page_json)

        additional_json_values.append(page_json)
        pages_fetched += 1
        total_records += page_records

        if total_records >= max_records:
            if page_assessment is not None:
                current_assessment = page_assessment
            break

        if page_assessment is not None:
            current_assessment = page_assessment
        else:
            break

    stopped_reason = "complete"
    if current_assessment.has_more and current_assessment.state is not None:
        if pages_fetched >= max_pages:
            stopped_reason = "max_pages"
        elif total_records >= max_records:
            stopped_reason = "max_records"

    merged = merge_envelopes(
        first_envelope,
        additional_json_values,
        current_assessment,
    )
    return AutoPaginationResult(
        envelope=merged,
        assessment=current_assessment,
        pages_fetched=pages_fetched,
        total_records=total_records,
        stopped_reason=stopped_reason,
        binary_refs=accumulated_binary_refs,
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

    allow_reuse = _extract_allow_reuse(context)
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
    code_query_packages = _available_code_query_packages(ctx)

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
            allow_reuse=allow_reuse,
        )

    reuse = ReuseResult(reused=False)
    cache_reason = "reuse_disabled" if not allow_reuse else "cache_miss"
    pagination_assessment: PaginationAssessment | None = None
    if ctx.db_pool is None:
        return gateway_error(
            "NOT_IMPLEMENTED",
            "schema-first responses require database persistence",
        )
    else:
        # Phase 1: Cache check in a short-lived connection.
        # The advisory lock is transaction-scoped and released when this
        # connection closes, so there is a small window for duplicate
        # upstream calls.  This is an acceptable trade-off: pool starvation
        # from holding a connection during a 30 s upstream call is far worse
        # than an occasional redundant call (persist handles the race via
        # unique artifact IDs).
        if allow_reuse:
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
                            except _DB_CONNECTIVITY_ERRORS:
                                raise
                            except Exception:
                                get_logger(component="mcp.handlers").warning(
                                    "artifact_ref upsert on cache hit failed",
                                    exc_info=True,
                                )
                                cache_reason = "artifact_ref_attach_failed"
                            if ref_attached:
                                desc, hint = _fetch_inline_describe(
                                    connection,
                                    reuse.artifact_id,
                                    code_query_packages=code_query_packages,
                                )
                                if _describe_has_ready_schema(desc):
                                    ctx._increment_metric("cache_hits")
                                    cache_reason = (
                                        reuse.reason
                                        or "request_key_match"
                                    )
                                    (
                                        mapping_payload,
                                        schema_payload,
                                        schema_legend,
                                    ) = _schema_payload_from_describe(desc)
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
                                            "allow_reuse": True,
                                        },
                                        mapping=mapping_payload,
                                        schemas=schema_payload,
                                        schema_legend=schema_legend,
                                        usage_hint=hint,
                                        pagination=pagination_meta,
                                    )
                                cache_reason = "cache_schema_unavailable"
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
            except _DB_CONNECTIVITY_ERRORS:
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
            cache_reason = "reuse_disabled"

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
            except _DB_CONNECTIVITY_ERRORS:
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

        # Phase 1.75: Resolve artifact refs in forwarded args.
        # Uses a short-lived connection; resolved args are only
        # sent upstream — identity / cache / storage use the
        # original pointer-containing forwarded_args.
        # Import is deferred to avoid a circular dependency
        # (resolve_refs → handlers.common → handlers.__init__
        #  → mirrored_tool → resolve_refs).
        from sift_mcp.mcp.resolve_refs import (
            ResolveError,
            resolve_artifact_refs,
        )

        upstream_args = forwarded_args
        try:
            with ctx.db_pool.connection() as resolve_conn:
                resolved = resolve_artifact_refs(resolve_conn, forwarded_args)
                if isinstance(resolved, ResolveError):
                    return gateway_error(resolved.code, resolved.message)
                upstream_args = resolved
        except _DB_CONNECTIVITY_ERRORS:
            ctx.db_ok = False
            return gateway_error(
                "INTERNAL",
                "artifact ref resolution failed; gateway marked unhealthy",
            )
        except Exception:
            get_logger(component="mcp.handlers").warning(
                "artifact ref resolution failed (best-effort)",
                exc_info=True,
            )

        # Phase 2: Upstream call — no DB connection held.
        try:
            upstream_result = await ctx._call_upstream_with_metrics(
                mirrored=mirrored,
                forwarded_args=upstream_args,
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

        # Phase 2.25: Auto-pagination — fetch additional upstream
        # pages and merge into a single artifact when enabled.
        if (
            pagination_assessment is not None
            and pagination_assessment.has_more
            and pagination_assessment.state is not None
        ):
            ap_limits = resolve_auto_paginate_limits(
                ctx.config, mirrored.upstream.config
            )
            if ap_limits.max_pages > 1:
                ap_result = await _auto_paginate_loop(
                    ctx,
                    mirrored,
                    first_envelope=envelope,
                    first_assessment=pagination_assessment,
                    forwarded_args=forwarded_args,
                    max_pages=ap_limits.max_pages,
                    max_records=ap_limits.max_records,
                    timeout=ap_limits.timeout,
                )
                envelope = ap_result.envelope
                pagination_assessment = ap_result.assessment
                binary_refs.extend(ap_result.binary_refs)

                # Reapply oversize guard: individual pages may
                # fit under the threshold, but the merged JSON
                # can exceed it.
                if ctx.blob_store is not None:
                    from sift_mcp.envelope.oversize import (
                        replace_oversized_json_parts,
                    )

                    merged_refs: list[Any] = []
                    envelope = replace_oversized_json_parts(
                        envelope,
                        max_json_part_parse_bytes=(
                            ctx.config.max_json_part_parse_bytes
                        ),
                        blob_store=ctx.blob_store,
                        binary_refs_out=merged_refs,
                    )
                    binary_refs.extend(merged_refs)

        # Phase 3: Persist + Phase 4: inline mapping in a single connection.
        # Reusing the same connection avoids a second pool checkout that
        # could silently fail (e.g. PoolTimeout under load).
        stage = "persist_artifact"
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
                stage = "run_mapping_inline"
                mapped = ctx._run_mapping_inline(
                    connection,
                    handle=handle,
                    envelope=envelope,
                )
                if not mapped:
                    return gateway_error(
                        "INTERNAL",
                        "mapping did not complete for artifact",
                    )
                # Phase 5: Inline describe — fetch roots on
                # the same connection (2 indexed lookups).
                stage = "fetch_inline_describe"
                try:
                    desc, hint = _fetch_inline_describe(
                        connection,
                        handle.artifact_id,
                        code_query_packages=code_query_packages,
                    )
                except Exception as exc:
                    _logger.warning(
                        "inline describe failed; returning minimal describe",
                        artifact_id=handle.artifact_id,
                        error_type=type(exc).__name__,
                        exc_info=True,
                    )
                    desc, hint = _minimal_describe(
                        handle.artifact_id,
                        code_query_packages=code_query_packages,
                    )
                if not _describe_has_ready_schema(desc):
                    map_status = desc.get("mapping", {}).get("map_status")
                    schemas = desc.get("schemas")
                    has_schemas = isinstance(schemas, list) and bool(schemas)
                    if map_status == "ready" and not has_schemas:
                        # Inconsistent state: ready mapping but no schema rows.
                        # Fall back to a minimal describe payload rather than
                        # failing the mirrored call.
                        desc, hint = _minimal_describe(
                            handle.artifact_id,
                            code_query_packages=code_query_packages,
                        )
                        map_status = desc.get("mapping", {}).get("map_status")
                        has_schemas = False
                    _logger.warning(
                        "schema-first inline describe not ready; returning best-effort payload",
                        artifact_id=handle.artifact_id,
                        map_status=map_status,
                        has_schemas=has_schemas,
                    )
        except _DB_CONNECTIVITY_ERRORS:
            ctx.db_ok = False
            return gateway_error(
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
            return gateway_error(
                "INTERNAL",
                "artifact persistence failed",
                details={
                    "stage": stage,
                    "error_type": type(exc).__name__,
                },
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
    mapping_payload, schema_payload, schema_legend = (
        _schema_payload_from_describe(desc)
    )

    return gateway_tool_result(
        artifact_id=handle.artifact_id,
        cache_meta={
            "reused": False,
            "reason": cache_reason,
            "request_key": identity.request_key,
            "artifact_id_origin": "fresh",
            "allow_reuse": allow_reuse,
        },
        mapping=mapping_payload,
        schemas=schema_payload,
        schema_legend=schema_legend,
        usage_hint=hint,
        pagination=pagination_meta,
    )
