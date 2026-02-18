"""Handle invocations of mirrored upstream tools.

Orchestrate the full lifecycle for a proxied tool call: validate
gateway context, call the upstream, persist the artifact envelope,
and trigger mapping.  Exports ``handle_mirrored_tool``.
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
from sift_mcp.mcp.handlers.common import (
    row_to_dict,
    rows_to_dicts,
)
from sift_mcp.mcp.handlers.schema_payload import build_schema_payload
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
    UPSTREAM_PARTIAL_REASON_SIGNAL_INCONCLUSIVE,
    build_upstream_pagination_meta,
)
from sift_mcp.pagination.extract import (
    PaginationAssessment,
    assess_pagination,
)
from sift_mcp.request_identity import compute_request_identity
from sift_mcp.schema_compact import (
    SCHEMA_LEGEND,
    compact_schema_payload,
)
from sift_mcp.tools.artifact_describe import (
    FETCH_DESCRIBE_SQL,
    FETCH_SCHEMA_ROOTS_SQL,
    build_describe_response,
)
from sift_mcp.tools.artifact_schema import FETCH_SCHEMA_FIELDS_SQL
from sift_mcp.tools.usage_hint import (
    build_usage_hint,
    with_pagination_completeness_rule,
)

if TYPE_CHECKING:
    from sift_mcp.mcp.server import GatewayServer


_DB_CONNECTIVITY_ERRORS: tuple[type[BaseException], ...] = (
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

_FETCH_PREVIOUS_PAGE_SQL = """
SELECT artifact_id, payload_hash_full
FROM artifacts
WHERE workspace_id = %s
  AND session_id = %s
  AND source_tool = %s
  AND deleted_at IS NULL
  AND created_seq < %s
ORDER BY created_seq DESC
LIMIT 1
"""

_PLACEHOLDER_CURSOR_VALUES = frozenset(
    {
        "cursor",
        "last_cursor",
        "next_cursor",
        "after_cursor",
        "<cursor>",
        "{cursor}",
        "your_cursor",
        "insert_cursor_here",
    }
)


def _is_placeholder_cursor_value(value: str) -> bool:
    """Return True when cursor value looks like a literal placeholder."""
    normalized = value.strip().lower()
    if normalized in _PLACEHOLDER_CURSOR_VALUES:
        return True
    collapsed = normalized.replace("-", "_").replace(" ", "_")
    return collapsed in _PLACEHOLDER_CURSOR_VALUES


def _validate_cursor_argument(
    *,
    forwarded_args: dict[str, Any],
    pagination_config: Any,
) -> dict[str, Any] | None:
    """Validate caller-supplied cursor argument for cursor pagination."""
    if pagination_config is None:
        return None
    if getattr(pagination_config, "strategy", None) != "cursor":
        return None
    cursor_param_name = getattr(
        pagination_config, "cursor_param_name", None
    ) or "after"
    if cursor_param_name not in forwarded_args:
        return None
    cursor_value = forwarded_args.get(cursor_param_name)
    if isinstance(cursor_value, str) and cursor_value.strip():
        if _is_placeholder_cursor_value(cursor_value):
            return gateway_error(
                "INVALID_ARGUMENT",
                (
                    f'pagination cursor "{cursor_param_name}" appears to be '
                    "a placeholder value"
                ),
                details={
                    "cursor_param": cursor_param_name,
                    "cursor_value": cursor_value,
                    "hint": (
                        "Extract the real cursor from the previous response "
                        "pagination.next_cursor (or pagination.next_params) "
                        "before retrying."
                    ),
                },
            )
        return None
    return gateway_error(
        "INVALID_ARGUMENT",
        f'pagination cursor "{cursor_param_name}" must be a non-empty string',
        details={
            "cursor_param": cursor_param_name,
            "received_type": type(cursor_value).__name__,
        },
    )


def _detect_duplicate_page_warning(
    *,
    connection: Any,
    artifact_id: str,
    payload_hash_full: str,
    created_seq: int | None,
    session_id: str,
    source_tool: str,
    forwarded_args: dict[str, Any],
    pagination_config: Any,
) -> dict[str, Any] | None:
    """Detect likely repeated first-page retrieval during cursor pagination."""
    if pagination_config is None:
        return None
    if getattr(pagination_config, "strategy", None) != "cursor":
        return None
    if not isinstance(created_seq, int) or created_seq <= 0:
        return None
    cursor_param_name = getattr(
        pagination_config, "cursor_param_name", None
    ) or "after"
    cursor_value = forwarded_args.get(cursor_param_name)
    if not (isinstance(cursor_value, str) and cursor_value.strip()):
        return None
    previous_row = connection.execute(
        _FETCH_PREVIOUS_PAGE_SQL,
        (
            WORKSPACE_ID,
            session_id,
            source_tool,
            created_seq,
        ),
    ).fetchone()
    if not isinstance(previous_row, tuple) or len(previous_row) < 2:
        return None
    previous_artifact_id = previous_row[0]
    previous_hash = previous_row[1]
    if not isinstance(previous_artifact_id, str):
        return None
    if not isinstance(previous_hash, str):
        return None
    if previous_artifact_id == artifact_id:
        return None
    if previous_hash != payload_hash_full:
        return None
    return {
        "code": "PAGINATION_DUPLICATE_PAGE",
        "message": (
            "Current page payload matches the previous page for this tool. "
            "The cursor may be invalid or ignored by the upstream."
        ),
        "cursor_param": cursor_param_name,
        "cursor_value": cursor_value,
        "previous_artifact_id": previous_artifact_id,
        "payload_hash": f"sha256:{payload_hash_full}",
    }


def _is_descendant_root_path(
    parent_root_path: str,
    candidate_root_path: str,
) -> bool:
    """Return True when candidate is a strict descendant of parent."""
    if parent_root_path == candidate_root_path:
        return False
    if parent_root_path == "$":
        return candidate_root_path.startswith(("$.", "$["))
    if not candidate_root_path.startswith(parent_root_path):
        return False
    suffix = candidate_root_path[len(parent_root_path) : len(parent_root_path) + 1]
    return suffix in {".", "["}


def _select_leaf_schema_roots(
    schemas: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Keep leaf roots only; drop exact duplicates and parent roots."""
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for schema in schemas:
        root_path = schema.get("root_path")
        schema_hash = schema.get("schema_hash")
        determinism = schema.get("determinism")
        dataset_hash = (
            determinism.get("dataset_hash")
            if isinstance(determinism, dict)
            else None
        )
        key = (
            str(root_path) if isinstance(root_path, str) else "",
            str(schema_hash) if isinstance(schema_hash, str) else "",
            str(dataset_hash) if isinstance(dataset_hash, str) else "",
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(schema)
    root_paths = [
        schema.get("root_path") if isinstance(schema.get("root_path"), str) else None
        for schema in deduped
    ]
    leaves: list[dict[str, Any]] = []
    for index, schema in enumerate(deduped):
        root_path = root_paths[index]
        if not isinstance(root_path, str):
            leaves.append(schema)
            continue
        has_child = any(
            isinstance(candidate, str)
            and _is_descendant_root_path(root_path, candidate)
            for i, candidate in enumerate(root_paths)
            if i != index
        )
        if has_child:
            continue
        leaves.append(schema)
    return leaves


def _available_code_query_packages(
    ctx: GatewayServer,
) -> list[str] | None:
    """Return available third-party package roots for code-query hints."""
    if not ctx.config.code_query_enabled:
        return None

    allowed_roots = sorted(
        allowed_import_roots(
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
            schemas.append(
                build_schema_payload(
                    schema_root=schema_root,
                    field_rows=field_rows,
                    include_null_example_value=True,
                )
            )
        describe = build_describe_response(
            artifact_row,
            [],
            schemas=_select_leaf_schema_roots(schemas),
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
    *,
    extra_warnings: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build pagination metadata for a gateway tool response.

    Args:
        assessment: Canonical pagination assessment.
        artifact_id: The artifact ID for this page.
        extra_warnings: Additional structured warnings to expose
            in the pagination payload.

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
        next_params=(
            assessment.state.next_params if assessment.state is not None else None
        ),
        original_args=(
            assessment.state.original_args if assessment.state is not None else None
        ),
        extra_warnings=extra_warnings,
    )
    hint = base.get("hint")
    if isinstance(hint, str):
        base["hint"] = with_pagination_completeness_rule(hint)
    return base


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
                    resolved = resolve_artifact_refs(
                        resolve_conn,
                        next_args,
                        blobs_payload_dir=ctx.config.blobs_payload_dir,
                    )
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
        except TimeoutError:
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
    # Pre-flight health gate: refuse artifact creation when
    # gateway is unhealthy. Probe before refusing -- the failure
    # that latched db_ok=False may have been transient.
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

    context = extract_gateway_context(arguments)
    session_id = _extract_session_id(context)
    if session_id is None:
        return gateway_error(
            "INVALID_ARGUMENT",
            "missing _gateway_context.session_id",
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
    cursor_arg_err = _validate_cursor_argument(
        forwarded_args=forwarded_args,
        pagination_config=mirrored.upstream.config.pagination,
    )
    if cursor_arg_err is not None:
        return cursor_arg_err

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
        )

    cache_reason = "fresh"
    pagination_assessment: PaginationAssessment | None = None
    pagination_warnings: list[dict[str, Any]] = []
    if ctx.db_pool is None:
        return gateway_error(
            "NOT_IMPLEMENTED",
            "schema-first responses require database persistence",
        )

    # Phase 1: Resolve artifact refs in forwarded args.
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
            resolved = resolve_artifact_refs(
                resolve_conn,
                forwarded_args,
                blobs_payload_dir=ctx.config.blobs_payload_dir,
            )
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
            "artifact ref resolution failed",
            exc_info=True,
        )
        return gateway_error(
            "INTERNAL",
            "artifact ref resolution failed",
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
            if pagination_assessment is not None:
                try:
                    duplicate_warning = _detect_duplicate_page_warning(
                        connection=connection,
                        artifact_id=handle.artifact_id,
                        payload_hash_full=handle.payload_hash_full,
                        created_seq=handle.created_seq,
                        session_id=session_id,
                        source_tool=handle.source_tool,
                        forwarded_args=forwarded_args,
                        pagination_config=mirrored.upstream.config.pagination,
                    )
                    if duplicate_warning is not None:
                        pagination_warnings.append(duplicate_warning)
                except Exception:
                    _logger.warning(
                        "duplicate-page pagination check failed",
                        artifact_id=handle.artifact_id,
                        exc_info=True,
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
    if pagination_assessment_for_response is not None:
        pagination_meta = _pagination_response_meta(
            pagination_assessment_for_response,
            handle.artifact_id,
            extra_warnings=pagination_warnings,
        )
    mapping_payload, schema_payload, schema_legend = (
        _schema_payload_from_describe(desc)
    )

    return gateway_tool_result(
        artifact_id=handle.artifact_id,
        cache_meta={
            "reason": cache_reason,
            "request_key": identity.request_key,
            "artifact_id_origin": "fresh",
        },
        mapping=mapping_payload,
        schemas=schema_payload,
        schema_legend=schema_legend,
        usage_hint=hint,
        pagination=pagination_meta,
    )
