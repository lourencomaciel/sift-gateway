"""MCP server setup and runtime tool registration."""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Mapping

from fastmcp import FastMCP
from fastmcp.tools.tool import Tool, ToolResult
from psycopg_pool import ConnectionPool

from mcp_artifact_gateway.artifacts.create import (
    ArtifactHandle,
    CreateArtifactInput,
    compute_payload_sizes,
    generate_artifact_id,
    persist_artifact,
    prepare_envelope_storage,
)
from mcp_artifact_gateway.cache.reuse import (
    acquire_advisory_lock,
    FIND_REUSABLE_BY_REQUEST_KEY_SQL,
    ReuseResult,
    check_reuse_candidate,
)
from mcp_artifact_gateway.config.settings import GatewayConfig
from mcp_artifact_gateway.constants import RESPONSE_TYPE_RESULT, WORKSPACE_ID
from mcp_artifact_gateway.cursor.hmac import (
    CursorExpiredError,
    CursorTokenError,
    sign_cursor_payload,
    verify_cursor_token,
)
from mcp_artifact_gateway.cursor.payload import (
    CursorStaleError,
    assert_cursor_binding,
    build_cursor_payload,
)
from mcp_artifact_gateway.cursor.sample_set_hash import (
    SampleSetHashBindingError,
    assert_sample_set_hash_binding,
    compute_sample_set_hash,
)
from mcp_artifact_gateway.cursor.secrets import CursorSecrets, load_or_create_cursor_secrets
from mcp_artifact_gateway.envelope.model import BinaryRefContentPart, Envelope
from mcp_artifact_gateway.envelope.normalize import normalize_envelope
from mcp_artifact_gateway.envelope.oversize import replace_oversized_json_parts
from mcp_artifact_gateway.envelope.responses import gateway_error, gateway_tool_result
from mcp_artifact_gateway.fs.blob_store import BlobStore
from mcp_artifact_gateway.mapping.runner import MappingInput
from mcp_artifact_gateway.mapping.worker import WorkerContext, run_mapping_worker, should_run_mapping
from mcp_artifact_gateway.mcp.mirror import (
    MirroredTool,
    build_mirrored_tools,
    extract_gateway_context,
    strip_reserved_gateway_args,
    validate_against_schema,
)
from mcp_artifact_gateway.mcp.upstream import (
    UpstreamInstance,
    call_upstream_tool,
    connect_upstreams,
)
from mcp_artifact_gateway.obs.metrics import GatewayMetrics, get_metrics
from mcp_artifact_gateway.query.jsonpath import (
    JsonPathError,
    canonicalize_jsonpath,
    evaluate_jsonpath,
)
from mcp_artifact_gateway.query.select_paths import (
    canonicalize_select_paths,
    project_select_paths,
    select_paths_hash,
)
from mcp_artifact_gateway.query.where_hash import where_hash
from mcp_artifact_gateway.query.where_dsl import WhereDslError, evaluate_where
from mcp_artifact_gateway.request_identity import compute_request_identity
from mcp_artifact_gateway.retrieval.response import apply_output_budgets
from mcp_artifact_gateway.sessions import touch_for_retrieval, touch_for_search
from mcp_artifact_gateway.storage.payload_store import reconstruct_envelope
from mcp_artifact_gateway.tools.status import (
    build_status_response_with_runtime,
    probe_db,
    probe_fs,
)

_GENERIC_ARGS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {},
    "additionalProperties": True,
}
_SUPPORTED_ENVELOPE_PARTS = {
    "json",
    "text",
    "resource_ref",
    "binary_ref",
    "image_ref",
}
_BUILTIN_TOOL_DESCRIPTIONS = {
    "gateway.status": "Gateway health and configuration snapshot.",
    "artifact.search": "Search artifacts visible to a session.",
    "artifact.get": "Load a stored artifact envelope or mapped value.",
    "artifact.select": "Project and filter artifact data with bounded traversal.",
    "artifact.describe": "Describe mapped roots and retrieval affordances.",
    "artifact.find": "Find matching records under mapped roots.",
    "artifact.chain_pages": "Return chain-ordered child artifacts.",
}
_SEARCH_COLUMNS = [
    "artifact_id",
    "created_seq",
    "created_at",
    "last_seen_at",
    "source_tool",
    "upstream_instance_id",
    "status",
    "payload_total_bytes",
    "error_summary",
    "map_kind",
    "map_status",
]
_GET_COLUMNS = [
    "artifact_id",
    "payload_hash_full",
    "deleted_at",
    "map_kind",
    "map_status",
    "generation",
    "mapped_part_index",
    "map_budget_fingerprint",
    "envelope",
    "envelope_canonical_encoding",
    "envelope_canonical_bytes",
    "envelope_canonical_bytes_len",
    "contains_binary_refs",
]
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
_ROOT_COLUMNS = [
    "root_key",
    "root_path",
    "count_estimate",
    "inventory_coverage",
    "root_summary",
    "root_score",
    "root_shape",
    "fields_top",
    "sample_indices",
]
_SELECT_ROOT_COLUMNS = [
    "root_key",
    "root_path",
    "count_estimate",
    "root_shape",
    "fields_top",
    "sample_indices",
    "root_summary",
]
_SAMPLE_COLUMNS = ["sample_index", "record", "record_bytes", "record_hash"]
_CHAIN_COLUMNS = [
    "artifact_id",
    "created_seq",
    "created_at",
    "chain_seq",
    "source_tool",
    "payload_total_bytes",
    "map_kind",
    "map_status",
]
_ARTIFACT_META_COLUMNS = [
    "artifact_id",
    "map_kind",
    "map_status",
    "index_status",
    "deleted_at",
    "generation",
    "map_budget_fingerprint",
]
_VISIBLE_ARTIFACT_SQL = """
SELECT 1
FROM artifact_refs
WHERE workspace_id = %s
  AND session_id = %s
  AND artifact_id = %s
LIMIT 1
"""
_FETCH_ARTIFACT_META_SQL = """
SELECT artifact_id, map_kind, map_status, index_status, deleted_at, generation, map_budget_fingerprint
FROM artifacts
WHERE workspace_id = %s AND artifact_id = %s
"""


class RuntimeTool(Tool):
    """Custom FastMCP tool that accepts raw argument dicts."""

    handler: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]

    async def run(self, arguments: dict[str, Any]) -> ToolResult:
        result = await self.handler(arguments)
        return ToolResult(structured_content=result)


def _lookup_cache_mode(context: dict[str, Any] | None) -> str | None:
    if context is None:
        return "allow"
    raw = context.get("cache_mode", "allow")
    if raw in {"allow", "fresh"}:
        return str(raw)
    return None


def _extract_session_id(context: dict[str, Any] | None) -> str | None:
    if context is None:
        return None
    session_id = context.get("session_id")
    if isinstance(session_id, str) and session_id:
        return session_id
    return None


def _candidate_from_row(row: tuple[object, ...] | dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    if isinstance(row, dict):
        return row
    if len(row) < 5:
        return None
    return {
        "artifact_id": row[0],
        "payload_hash_full": row[1],
        "upstream_tool_schema_hash": row[2],
        "map_status": row[3],
        "generation": row[4],
    }


def _upstream_error_message(result: dict[str, Any]) -> str:
    content = result.get("content")
    if isinstance(content, list):
        for block in content:
            if isinstance(block, dict):
                text = block.get("text")
                if isinstance(text, str) and text.strip():
                    return text
    return "upstream tool returned an error"


def _normalize_upstream_content(
    *,
    content: list[dict[str, Any]] | None,
    structured_content: Any,
) -> list[Mapping[str, Any]]:
    normalized: list[Mapping[str, Any]] = []
    if isinstance(structured_content, dict):
        normalized.append({"type": "json", "value": structured_content})

    for block in content or []:
        part_type = block.get("type")
        if part_type in _SUPPORTED_ENVELOPE_PARTS:
            normalized.append(block)
            continue
        if isinstance(block.get("text"), str):
            normalized.append({"type": "text", "text": block["text"]})
            continue
        normalized.append(
            {
                "type": "text",
                "text": json.dumps(block, sort_keys=True, ensure_ascii=False),
            }
        )
    return normalized


def _row_to_dict(
    row: tuple[object, ...] | Mapping[str, Any] | None,
    columns: list[str],
) -> dict[str, Any] | None:
    if row is None:
        return None
    if isinstance(row, Mapping):
        return dict(row)
    return {
        column: row[index] if index < len(row) else None
        for index, column in enumerate(columns)
    }


def _rows_to_dicts(
    rows: list[tuple[object, ...] | Mapping[str, Any]],
    columns: list[str],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        mapped = _row_to_dict(row, columns)
        if mapped is not None:
            out.append(mapped)
    return out


@dataclass
class GatewayServer:
    """Holds runtime state and provides executable tool handlers."""

    config: GatewayConfig
    db_pool: ConnectionPool | None = None
    blob_store: BlobStore | None = None
    upstreams: list[UpstreamInstance] = field(default_factory=list)
    fs_ok: bool = True
    db_ok: bool = True
    metrics: GatewayMetrics = field(default_factory=get_metrics)
    cursor_secrets: CursorSecrets | None = None
    upstream_errors: dict[str, str] = field(default_factory=dict)
    mirrored_tools: dict[str, MirroredTool] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.mirrored_tools and self.upstreams:
            self.mirrored_tools = build_mirrored_tools(self.upstreams)

    @staticmethod
    def _not_implemented(tool_name: str) -> dict[str, Any]:
        return gateway_error(
            "NOT_IMPLEMENTED",
            f"{tool_name} is not wired to persistence yet",
        )

    def _status_upstreams(self) -> list[dict[str, Any]]:
        payload: list[dict[str, Any]] = []
        for upstream in self.upstreams:
            payload.append(
                {
                    "prefix": upstream.prefix,
                    "instance_id": upstream.instance_id,
                    "connected": True,
                    "tool_count": len(upstream.tools),
                    "auth_fingerprint": upstream.auth_fingerprint,
                }
            )
        for prefix, error in sorted(self.upstream_errors.items()):
            payload.append(
                {
                    "prefix": prefix,
                    "connected": False,
                    "tool_count": 0,
                    "error": error,
                }
            )
        return payload

    def _bounded_limit(self, raw_limit: Any) -> int:
        if isinstance(raw_limit, int) and raw_limit > 0:
            return min(raw_limit, self.config.max_items)
        return min(50, self.config.max_items)

    def _increment_metric(self, attr: str, amount: int = 1) -> None:
        counter = getattr(self.metrics, attr, None)
        increment = getattr(counter, "increment", None)
        if callable(increment):
            increment(amount)

    def _observe_metric(self, attr: str, value: float) -> None:
        histogram = getattr(self.metrics, attr, None)
        observe = getattr(histogram, "observe", None)
        if callable(observe):
            observe(value)

    async def _call_upstream_with_metrics(
        self,
        *,
        mirrored: MirroredTool,
        forwarded_args: dict[str, Any],
    ) -> dict[str, Any]:
        self._increment_metric("upstream_calls")
        started_at = time.monotonic()
        try:
            result = await call_upstream_tool(
                mirrored.upstream,
                mirrored.original_name,
                forwarded_args,
            )
        except Exception:
            self._increment_metric("upstream_errors")
            raise
        finally:
            self._observe_metric(
                "upstream_latency",
                (time.monotonic() - started_at) * 1000.0,
            )
        if bool(result.get("isError", False)):
            self._increment_metric("upstream_errors")
        return result

    def _record_cursor_stale_reason(self, message: str) -> None:
        reason: str | None = None
        if "sample_set_hash mismatch" in message:
            reason = "sample_set_mismatch"
        elif "map_budget_fingerprint mismatch" in message:
            reason = "map_budget_mismatch"
        elif "where_canonicalization_mode mismatch" in message:
            reason = "where_mode_mismatch"
        elif "traversal_contract_version mismatch" in message:
            reason = "traversal_version_mismatch"
        elif "artifact_generation mismatch" in message:
            reason = "generation_mismatch"
        if reason is not None:
            recorder = getattr(self.metrics, "record_cursor_stale_reason", None)
            if callable(recorder):
                recorder(reason)

    def _cursor_session_artifact_id(self, session_id: str, order_by: str) -> str:
        return f"session:{session_id}:{order_by}"

    def _cursor_error(self, token_error: Exception) -> dict[str, Any]:
        if isinstance(token_error, CursorExpiredError):
            self._increment_metric("cursor_expired")
            return gateway_error("CURSOR_EXPIRED", "cursor expired")
        if isinstance(token_error, CursorStaleError):
            self._record_cursor_stale_reason(str(token_error))
            return gateway_error("CURSOR_STALE", str(token_error))
        self._increment_metric("cursor_invalid")
        return gateway_error("INVALID_ARGUMENT", "invalid cursor")

    def _get_cursor_secrets(self) -> CursorSecrets:
        if self.cursor_secrets is None:
            self.cursor_secrets = load_or_create_cursor_secrets(self.config.secrets_path)
        return self.cursor_secrets

    def _issue_cursor(
        self,
        *,
        tool: str,
        artifact_id: str,
        position_state: dict[str, Any],
        extra: dict[str, Any] | None = None,
    ) -> str:
        payload = build_cursor_payload(
            tool=tool,
            artifact_id=artifact_id,
            position_state=position_state,
            ttl_minutes=self.config.cursor_ttl_minutes,
            where_canonicalization_mode=self.config.where_canonicalization_mode.value,
            extra=extra,
        )
        return sign_cursor_payload(payload, self._get_cursor_secrets())

    def _verify_cursor(
        self,
        *,
        token: str,
        tool: str,
        artifact_id: str,
    ) -> dict[str, Any]:
        payload = self._verify_cursor_payload(
            token=token,
            tool=tool,
            artifact_id=artifact_id,
        )
        return self._cursor_position(payload)

    def _verify_cursor_payload(
        self,
        *,
        token: str,
        tool: str,
        artifact_id: str,
    ) -> dict[str, Any]:
        payload = verify_cursor_token(token, self._get_cursor_secrets())
        assert_cursor_binding(
            payload,
            expected_tool=tool,
            expected_artifact_id=artifact_id,
            expected_where_mode=self.config.where_canonicalization_mode.value,
        )
        position = payload.get("position_state")
        if not isinstance(position, dict):
            msg = "cursor missing position_state"
            raise CursorTokenError(msg)
        return payload

    @staticmethod
    def _cursor_position(payload: dict[str, Any]) -> dict[str, Any]:
        position = payload.get("position_state")
        if not isinstance(position, dict):
            msg = "cursor missing position_state"
            raise CursorTokenError(msg)
        return position

    @staticmethod
    def _assert_cursor_field(
        payload: Mapping[str, Any],
        *,
        field: str,
        expected: object,
    ) -> None:
        actual = payload.get(field)
        if actual != expected:
            msg = f"cursor {field} mismatch"
            raise CursorStaleError(msg)

    def _artifact_visible(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_id: str,
    ) -> bool:
        row = connection.execute(
            _VISIBLE_ARTIFACT_SQL,
            (WORKSPACE_ID, session_id, artifact_id),
        ).fetchone()
        return row is not None

    def _safe_touch_for_retrieval(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_id: str,
    ) -> None:
        if callable(getattr(connection, "cursor", None)):
            touch_for_retrieval(connection, session_id, artifact_id)

    def _safe_touch_for_search(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_ids: list[str],
    ) -> None:
        if callable(getattr(connection, "cursor", None)):
            touch_for_search(connection, session_id, artifact_ids)

    @staticmethod
    def _check_sample_corruption(
        root_row: dict[str, Any],
        sample_rows: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        """Return INTERNAL error if expected sample indices are missing rows."""
        expected_raw = root_row.get("sample_indices")
        if not isinstance(expected_raw, list) or not expected_raw:
            return None
        expected = set(int(i) for i in expected_raw if isinstance(i, int))
        actual = set(
            int(row["sample_index"])
            for row in sample_rows
            if isinstance(row.get("sample_index"), int)
        )
        missing = sorted(expected - actual)
        if missing:
            return gateway_error(
                "INTERNAL",
                "sample data corruption: expected sample rows missing",
                details={
                    "root_key": root_row.get("root_key"),
                    "missing_indices": missing,
                    "expected_count": len(expected),
                    "actual_count": len(actual),
                },
            )
        return None

    def _binary_hashes_from_envelope(self, envelope: Envelope) -> list[str]:
        hashes: list[str] = []
        for part in envelope.content:
            if isinstance(part, BinaryRefContentPart):
                hashes.append(part.binary_hash)
        return hashes

    def _mapping_input_for_artifact(
        self,
        *,
        artifact_id: str,
        payload_hash_full: str,
        envelope: Envelope,
    ) -> MappingInput:
        open_binary_stream = None
        if self.blob_store is not None:
            open_binary_stream = self.blob_store.open_stream
        return MappingInput(
            artifact_id=artifact_id,
            payload_hash_full=payload_hash_full,
            envelope=envelope.to_dict(),
            config=self.config,
            open_binary_stream=open_binary_stream,
        )

    def _run_mapping_inline(
        self,
        connection: Any,
        *,
        handle: ArtifactHandle,
        envelope: Envelope,
    ) -> bool:
        worker_ctx = WorkerContext(
            artifact_id=handle.artifact_id,
            generation=handle.generation,
            map_status=handle.map_status,
        )
        return run_mapping_worker(
            connection,
            worker_ctx=worker_ctx,
            mapping_input=self._mapping_input_for_artifact(
                artifact_id=handle.artifact_id,
                payload_hash_full=handle.payload_hash_full,
                envelope=envelope,
            ),
            metrics=self.metrics,
        )

    async def _run_mapping_background(
        self,
        *,
        handle: ArtifactHandle,
        envelope: Envelope,
    ) -> None:
        if self.db_pool is None:
            return

        worker_ctx = WorkerContext(
            artifact_id=handle.artifact_id,
            generation=handle.generation,
            map_status=handle.map_status,
        )
        mapping_input = self._mapping_input_for_artifact(
            artifact_id=handle.artifact_id,
            payload_hash_full=handle.payload_hash_full,
            envelope=envelope,
        )

        def _execute() -> None:
            if self.db_pool is None:
                return
            with self.db_pool.connection() as connection:
                run_mapping_worker(
                    connection,
                    worker_ctx=worker_ctx,
                    mapping_input=mapping_input,
                    metrics=self.metrics,
                )

        await asyncio.to_thread(_execute)

    @staticmethod
    def _consume_mapping_task(task: asyncio.Task[None]) -> None:
        try:
            task.result()
        except Exception:
            return

    def _schedule_background_mapping(
        self,
        *,
        handle: ArtifactHandle,
        envelope: Envelope,
    ) -> None:
        task = asyncio.create_task(
            self._run_mapping_background(handle=handle, envelope=envelope)
        )
        task.add_done_callback(self._consume_mapping_task)

    def _trigger_mapping_for_artifact(
        self,
        connection: Any,
        *,
        handle: ArtifactHandle,
        envelope: Envelope,
    ) -> None:
        if not should_run_mapping(handle.map_status):
            return
        mode = self.config.mapping_mode.value
        if mode in {"sync", "hybrid"}:
            self._run_mapping_inline(connection, handle=handle, envelope=envelope)
            return
        self._schedule_background_mapping(handle=handle, envelope=envelope)

    def register_tools(
        self,
    ) -> dict[str, Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]]:
        return {
            "gateway.status": self.handle_status,
            "artifact.search": self.handle_artifact_search,
            "artifact.get": self.handle_artifact_get,
            "artifact.select": self.handle_artifact_select,
            "artifact.describe": self.handle_artifact_describe,
            "artifact.find": self.handle_artifact_find,
            "artifact.chain_pages": self.handle_artifact_chain_pages,
        }

    def register_mirrored_tools(
        self,
    ) -> dict[str, Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]]:
        handlers: dict[str, Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]] = {}
        for qualified_name, mirrored in self.mirrored_tools.items():

            async def _handler(
                arguments: dict[str, Any],
                mirrored_tool: MirroredTool = mirrored,
            ) -> dict[str, Any]:
                return await self.handle_mirrored_tool(mirrored_tool, arguments)

            handlers[qualified_name] = _handler
        return handlers

    def build_fastmcp_app(self) -> FastMCP:
        app = FastMCP(name="mcp-artifact-gateway")

        for tool_name, handler in self.register_tools().items():
            app.add_tool(
                RuntimeTool(
                    name=tool_name,
                    description=_BUILTIN_TOOL_DESCRIPTIONS.get(tool_name, "Gateway tool"),
                    parameters=dict(_GENERIC_ARGS_SCHEMA),
                    handler=handler,
                )
            )

        mirrored_handlers = self.register_mirrored_tools()
        for tool_name, mirrored in self.mirrored_tools.items():
            app.add_tool(
                RuntimeTool(
                    name=tool_name,
                    description=mirrored.upstream_tool.description
                    or f"Mirrored upstream tool {mirrored.original_name}",
                    parameters=dict(mirrored.upstream_tool.input_schema),
                    handler=mirrored_handlers[tool_name],
                )
            )

        return app

    def _check_reuse(
        self,
        *,
        request_key: str,
        expected_schema_hash: str | None,
        strict_schema_reuse: bool,
    ) -> ReuseResult:
        if self.db_pool is None:
            return ReuseResult(reused=False)

        with self.db_pool.connection() as connection:
            return self._check_reuse_on_connection(
                connection,
                request_key=request_key,
                expected_schema_hash=expected_schema_hash,
                strict_schema_reuse=strict_schema_reuse,
            )

    def _check_reuse_on_connection(
        self,
        connection: Any,
        *,
        request_key: str,
        expected_schema_hash: str | None,
        strict_schema_reuse: bool,
    ) -> ReuseResult:
        row = connection.execute(
            FIND_REUSABLE_BY_REQUEST_KEY_SQL,
            (WORKSPACE_ID, request_key),
        ).fetchone()

        return check_reuse_candidate(
            _candidate_from_row(row),
            expected_schema_hash=expected_schema_hash,
            strict_schema_reuse=strict_schema_reuse,
        )

    def _envelope_from_upstream_result(
        self,
        *,
        mirrored: MirroredTool,
        upstream_result: dict[str, Any],
    ) -> Envelope:
        is_error = bool(upstream_result.get("isError", False))
        content = upstream_result.get("content")
        structured_content = upstream_result.get("structuredContent")
        raw_content = content if isinstance(content, list) else []
        normalized_content = _normalize_upstream_content(
            content=[block for block in raw_content if isinstance(block, dict)],
            structured_content=structured_content,
        )

        error: dict[str, Any] | None = None
        if is_error:
            error = {
                "code": "UPSTREAM_ERROR",
                "message": _upstream_error_message(upstream_result),
                "details": {"tool": mirrored.original_name},
            }

        meta: dict[str, Any] = {"warnings": []}
        upstream_meta = upstream_result.get("meta")
        if isinstance(upstream_meta, dict) and upstream_meta:
            meta["upstream_meta"] = upstream_meta

        envelope = normalize_envelope(
            upstream_instance_id=mirrored.upstream.instance_id,
            upstream_prefix=mirrored.prefix,
            tool=mirrored.original_name,
            status="error" if is_error else "ok",
            content=normalized_content,
            error=error,
            meta=meta,
        )
        if self.blob_store is None:
            return envelope
        transformed = replace_oversized_json_parts(
            envelope,
            max_json_part_parse_bytes=self.config.max_json_part_parse_bytes,
            blob_store=self.blob_store,
        )
        warnings = transformed.meta.get("warnings")
        if isinstance(warnings, list):
            oversize_count = sum(
                1
                for warning in warnings
                if isinstance(warning, dict)
                and warning.get("code") == "oversized_json_part"
            )
            if oversize_count > 0:
                self._increment_metric("oversize_json_count", oversize_count)
        return transformed

    def _build_non_persisted_handle(
        self,
        *,
        input_data: CreateArtifactInput,
    ) -> ArtifactHandle:
        payload_hash, _, _, _ = prepare_envelope_storage(input_data.envelope, self.config)
        payload_json_bytes, payload_binary_bytes_total, payload_total_bytes = compute_payload_sizes(
            input_data.envelope
        )
        return ArtifactHandle(
            artifact_id=generate_artifact_id(),
            created_seq=None,
            generation=1,
            session_id=input_data.session_id,
            source_tool=f"{input_data.prefix}.{input_data.tool_name}",
            upstream_instance_id=input_data.upstream_instance_id,
            request_key=input_data.request_key,
            payload_hash_full=payload_hash,
            payload_json_bytes=payload_json_bytes,
            payload_binary_bytes_total=payload_binary_bytes_total,
            payload_total_bytes=payload_total_bytes,
            contains_binary_refs=input_data.envelope.contains_binary_refs,
            map_kind="none",
            map_status="pending",
            index_status="off",
            status=input_data.envelope.status,
            error_summary=(
                None
                if input_data.envelope.error is None
                else f"{input_data.envelope.error.code}: {input_data.envelope.error.message}"
            ),
        )

    async def handle_mirrored_tool(
        self,
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
        if self.db_pool is None:
            try:
                upstream_result = await self._call_upstream_with_metrics(
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
                envelope = self._envelope_from_upstream_result(
                    mirrored=mirrored,
                    upstream_result=upstream_result,
                )
            except ValueError as exc:
                return gateway_error(
                    "UPSTREAM_RESPONSE_INVALID",
                    str(exc),
                )
            handle = self._build_non_persisted_handle(input_data=_create_input(envelope))
        else:
            with self.db_pool.connection() as connection:
                if cache_mode != "fresh":
                    acquired = acquire_advisory_lock(
                        connection,
                        request_key=identity.request_key,
                        timeout_ms=self.config.advisory_lock_timeout_ms,
                        metrics=self.metrics,
                    )
                    if not acquired:
                        return gateway_error(
                            "RESOURCE_BUSY",
                            "advisory lock acquisition timed out",
                            details={
                                "timeout_ms": self.config.advisory_lock_timeout_ms,
                            },
                        )
                    reuse = self._check_reuse_on_connection(
                        connection,
                        request_key=identity.request_key,
                        expected_schema_hash=mirrored.upstream_tool.schema_hash,
                        strict_schema_reuse=mirrored.upstream.config.strict_schema_reuse,
                    )
                    if reuse.reused and reuse.artifact_id is not None:
                        self._increment_metric("cache_hits")
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
                    self._increment_metric("cache_misses")

                try:
                    upstream_result = await self._call_upstream_with_metrics(
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
                    envelope = self._envelope_from_upstream_result(
                        mirrored=mirrored,
                        upstream_result=upstream_result,
                    )
                except ValueError as exc:
                    return gateway_error(
                        "UPSTREAM_RESPONSE_INVALID",
                        str(exc),
                    )
                binary_hashes = self._binary_hashes_from_envelope(envelope)
                handle = persist_artifact(
                    connection=connection,
                    config=self.config,
                    input_data=_create_input(envelope),
                    binary_hashes=binary_hashes,
                )
                self._trigger_mapping_for_artifact(
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
            max_json_bytes=self.config.inline_envelope_max_json_bytes,
            max_total_bytes=self.config.inline_envelope_max_total_bytes,
            cache_meta={
                "reused": False,
                "reason": reuse.reason,
                "request_key": identity.request_key,
            },
        )

    def _cursor_secrets_info(self) -> dict[str, Any] | None:
        """Return cursor secret metadata for the status response, or None."""
        secrets = self.cursor_secrets
        if secrets is None:
            return None
        return {
            "signing_version": secrets.signing_version,
            "active_versions": sorted(secrets.active.keys()),
        }

    async def handle_status(self, arguments: dict[str, Any]) -> dict[str, Any]:
        db_health = probe_db(self.db_pool)
        fs_health = probe_fs(self.config)
        return build_status_response_with_runtime(
            self.config,
            db_health=db_health,
            fs_health=fs_health,
            upstreams=self._status_upstreams(),
            cursor_secrets_info=self._cursor_secrets_info(),
        )

    async def handle_artifact_search(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from mcp_artifact_gateway.tools.artifact_search import (
            build_search_query,
            validate_search_args,
        )

        parsed = validate_search_args(
            arguments,
            max_limit=self.config.artifact_search_max_limit,
        )
        if "error" in parsed:
            return gateway_error(str(parsed["error"]), str(parsed["message"]))
        if self.db_pool is None:
            return self._not_implemented("artifact.search")

        session_id = str(parsed["session_id"])
        order_by = str(parsed["order_by"])
        limit = min(int(parsed["limit"]), self.config.artifact_search_max_limit)
        offset = 0
        cursor_token = parsed.get("cursor")
        if isinstance(cursor_token, str) and cursor_token:
            try:
                position = self._verify_cursor(
                    token=cursor_token,
                    tool="artifact.search",
                    artifact_id=self._cursor_session_artifact_id(session_id, order_by),
                )
            except (CursorTokenError, CursorExpiredError, CursorStaleError) as exc:
                return self._cursor_error(exc)
            raw_offset = position.get("offset", 0)
            if not isinstance(raw_offset, int) or raw_offset < 0:
                return gateway_error("INVALID_ARGUMENT", "invalid cursor offset")
            offset = raw_offset

        sql, params = build_search_query(
            session_id,
            dict(parsed["filters"]),
            order_by,
            limit,
            offset=offset,
        )

        with self.db_pool.connection() as connection:
            rows = connection.execute(sql, tuple(params)).fetchall()
            mapped_rows = _rows_to_dicts(rows, _SEARCH_COLUMNS)
            page_rows = mapped_rows[:limit]
            truncated = len(mapped_rows) > limit
            artifact_ids = [
                str(row["artifact_id"])
                for row in page_rows
                if isinstance(row.get("artifact_id"), str)
            ]
            self._safe_touch_for_search(
                connection,
                session_id=session_id,
                artifact_ids=artifact_ids,
            )
            commit = getattr(connection, "commit", None)
            if callable(commit):
                commit()

        next_cursor: str | None = None
        if truncated:
            next_cursor = self._issue_cursor(
                tool="artifact.search",
                artifact_id=self._cursor_session_artifact_id(session_id, order_by),
                position_state={"offset": offset + len(page_rows)},
            )

        return {
            "items": [
                {
                    "artifact_id": row["artifact_id"],
                    "created_seq": row["created_seq"],
                    "created_at": (
                        str(row["created_at"])
                        if row.get("created_at") is not None
                        else None
                    ),
                    "last_seen_at": (
                        str(row["last_seen_at"])
                        if row.get("last_seen_at") is not None
                        else None
                    ),
                    "source_tool": row["source_tool"],
                    "upstream_instance_id": row["upstream_instance_id"],
                    "status": row["status"],
                    "payload_total_bytes": row["payload_total_bytes"],
                    "error_summary": row["error_summary"],
                    "map_kind": row["map_kind"],
                    "map_status": row["map_status"],
                }
                for row in page_rows
            ],
            "truncated": truncated,
            "cursor": next_cursor,
            "omitted": len(mapped_rows) - len(page_rows) if truncated else 0,
        }

    async def handle_artifact_get(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from mcp_artifact_gateway.tools.artifact_describe import FETCH_ROOTS_SQL
        from mcp_artifact_gateway.tools.artifact_get import (
            FETCH_ARTIFACT_SQL,
            check_get_preconditions,
            validate_get_args,
        )

        err = validate_get_args(arguments)
        if err is not None:
            return err
        if self.db_pool is None:
            return self._not_implemented("artifact.get")

        ctx = arguments.get("_gateway_context")
        session_id = str(ctx["session_id"]) if isinstance(ctx, dict) else ""
        artifact_id = str(arguments["artifact_id"])
        target = str(arguments.get("target", "envelope"))
        jsonpath = arguments.get("jsonpath")
        if jsonpath is not None and not isinstance(jsonpath, str):
            return gateway_error(
                "INVALID_ARGUMENT",
                "jsonpath must be a string when provided",
            )
        normalized_jsonpath = "$"
        if isinstance(jsonpath, str):
            try:
                normalized_jsonpath = canonicalize_jsonpath(
                    jsonpath,
                    max_length=self.config.max_jsonpath_length,
                    max_segments=self.config.max_path_segments,
                )
            except JsonPathError as exc:
                return gateway_error(
                    "INVALID_ARGUMENT",
                    f"invalid jsonpath: {exc}",
                )

        offset = 0
        cursor_payload: dict[str, Any] | None = None
        cursor_token = arguments.get("cursor")
        if isinstance(cursor_token, str) and cursor_token:
            try:
                cursor_payload = self._verify_cursor_payload(
                    token=cursor_token,
                    tool="artifact.get",
                    artifact_id=artifact_id,
                )
                position = self._cursor_position(cursor_payload)
            except (CursorTokenError, CursorExpiredError, CursorStaleError) as exc:
                return self._cursor_error(exc)
            raw_offset = position.get("offset", 0)
            if not isinstance(raw_offset, int) or raw_offset < 0:
                return gateway_error("INVALID_ARGUMENT", "invalid cursor offset")
            offset = raw_offset

        with self.db_pool.connection() as connection:
            if not self._artifact_visible(
                connection,
                session_id=session_id,
                artifact_id=artifact_id,
            ):
                return gateway_error("NOT_FOUND", "artifact not found")

            row = _row_to_dict(
                connection.execute(
                    FETCH_ARTIFACT_SQL,
                    (WORKSPACE_ID, artifact_id),
                ).fetchone(),
                _GET_COLUMNS,
            )
            precondition = check_get_preconditions(row, target)
            if precondition is not None:
                if row is not None:
                    self._safe_touch_for_retrieval(
                        connection,
                        session_id=session_id,
                        artifact_id=artifact_id,
                    )
                    commit = getattr(connection, "commit", None)
                    if callable(commit):
                        commit()
                return precondition

            self._safe_touch_for_retrieval(
                connection,
                session_id=session_id,
                artifact_id=artifact_id,
            )
            commit = getattr(connection, "commit", None)
            if callable(commit):
                commit()

            assert row is not None
            if cursor_payload is not None:
                try:
                    self._assert_cursor_field(cursor_payload, field="target", expected=target)
                    self._assert_cursor_field(
                        cursor_payload,
                        field="normalized_jsonpath",
                        expected=normalized_jsonpath,
                    )
                    generation = row.get("generation")
                    if isinstance(generation, int):
                        self._assert_cursor_field(
                            cursor_payload,
                            field="artifact_generation",
                            expected=generation,
                        )
                except CursorStaleError as exc:
                    return self._cursor_error(exc)

            if target == "mapped":
                roots_rows = connection.execute(
                    FETCH_ROOTS_SQL,
                    (WORKSPACE_ID, artifact_id),
                ).fetchall()
                roots = _rows_to_dicts(roots_rows, _ROOT_COLUMNS)
                return {
                    "artifact_id": artifact_id,
                    "target": "mapped",
                    "mapping": {
                        "map_kind": row.get("map_kind"),
                        "map_status": row.get("map_status"),
                        "mapped_part_index": row.get("mapped_part_index"),
                        "map_budget_fingerprint": row.get("map_budget_fingerprint"),
                    },
                    "roots": roots,
                }

            envelope_value = row.get("envelope")
            if isinstance(envelope_value, dict):
                envelope = envelope_value
            else:
                envelope = reconstruct_envelope(
                    compressed_bytes=bytes(row["envelope_canonical_bytes"]),
                    encoding=str(row["envelope_canonical_encoding"]),
                    expected_hash=str(row["payload_hash_full"]),
                )

            if jsonpath is not None:
                values = evaluate_jsonpath(
                    envelope,
                    normalized_jsonpath,
                    max_length=self.config.max_jsonpath_length,
                    max_segments=self.config.max_path_segments,
                    max_wildcard_expansion_total=self.config.max_wildcard_expansion_total,
                )
            else:
                values = [envelope]

        values_page = values[offset:]
        max_items = self._bounded_limit(arguments.get("limit"))
        selected, truncated, omitted, used_bytes = apply_output_budgets(
            values_page,
            max_items=max_items,
            max_bytes_out=self.config.max_bytes_out,
        )
        next_cursor: str | None = None
        if truncated:
            extra: dict[str, Any] = {
                "target": target,
                "normalized_jsonpath": normalized_jsonpath,
            }
            generation = row.get("generation")
            if isinstance(generation, int):
                extra["artifact_generation"] = generation
            next_cursor = self._issue_cursor(
                tool="artifact.get",
                artifact_id=artifact_id,
                position_state={
                    "offset": offset + len(selected),
                },
                extra=extra,
            )

        return {
            "artifact_id": artifact_id,
            "target": "envelope",
            "items": selected,
            "truncated": truncated,
            "cursor": next_cursor,
            "omitted": omitted,
            "stats": {"bytes_out": used_bytes},
        }

    async def handle_artifact_select(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from mcp_artifact_gateway.tools.artifact_get import FETCH_ARTIFACT_SQL
        from mcp_artifact_gateway.tools.artifact_select import (
            FETCH_ROOT_SQL,
            FETCH_SAMPLES_SQL,
            build_select_result,
            validate_select_args,
        )

        err = validate_select_args(arguments)
        if err is not None:
            return err
        if self.db_pool is None:
            return self._not_implemented("artifact.select")

        ctx = arguments.get("_gateway_context")
        session_id = str(ctx["session_id"]) if isinstance(ctx, dict) else ""
        artifact_id = str(arguments["artifact_id"])
        root_path = str(arguments["root_path"])
        select_paths_raw = arguments.get("select_paths", [])
        where_expr = arguments.get("where")
        if where_expr is not None and not isinstance(where_expr, (Mapping, str)):
            return gateway_error("INVALID_ARGUMENT", "where must be an object or string")
        absolute_paths = [
            str(path)
            if str(path).startswith("$")
            else (f"${path}" if str(path).startswith("[") else f"$.{path}")
            for path in select_paths_raw
        ]
        try:
            select_paths = canonicalize_select_paths(
                absolute_paths,
                max_jsonpath_length=self.config.max_jsonpath_length,
                max_path_segments=self.config.max_path_segments,
            )
        except (ValueError, TypeError) as exc:
            return gateway_error("INVALID_ARGUMENT", f"invalid select_paths: {exc}")
        select_paths_binding_hash = select_paths_hash(
            select_paths,
            max_jsonpath_length=self.config.max_jsonpath_length,
            max_path_segments=self.config.max_path_segments,
        )
        if where_expr is None:
            where_binding_hash = "__none__"
        else:
            try:
                where_binding_hash = where_hash(
                    where_expr,
                    mode=self.config.where_canonicalization_mode.value,
                )
            except ValueError as exc:
                return gateway_error(
                    "INVALID_ARGUMENT",
                    f"invalid where expression: {exc}",
                )

        offset = 0
        cursor_payload: dict[str, Any] | None = None
        cursor_token = arguments.get("cursor")
        if isinstance(cursor_token, str) and cursor_token:
            try:
                cursor_payload = self._verify_cursor_payload(
                    token=cursor_token,
                    tool="artifact.select",
                    artifact_id=artifact_id,
                )
                position = self._cursor_position(cursor_payload)
            except (CursorTokenError, CursorExpiredError, CursorStaleError) as exc:
                return self._cursor_error(exc)
            raw_offset = position.get("offset", 0)
            if not isinstance(raw_offset, int) or raw_offset < 0:
                return gateway_error("INVALID_ARGUMENT", "invalid cursor offset")
            offset = raw_offset

        with self.db_pool.connection() as connection:
            if not self._artifact_visible(
                connection,
                session_id=session_id,
                artifact_id=artifact_id,
            ):
                return gateway_error("NOT_FOUND", "artifact not found")

            artifact_meta = _row_to_dict(
                connection.execute(
                    _FETCH_ARTIFACT_META_SQL,
                    (WORKSPACE_ID, artifact_id),
                ).fetchone(),
                _ARTIFACT_META_COLUMNS,
            )
            if artifact_meta is None:
                return gateway_error("NOT_FOUND", "artifact not found")
            if artifact_meta.get("deleted_at") is not None:
                self._safe_touch_for_retrieval(
                    connection,
                    session_id=session_id,
                    artifact_id=artifact_id,
                )
                commit = getattr(connection, "commit", None)
                if callable(commit):
                    commit()
                return gateway_error("GONE", "artifact has been deleted")
            if artifact_meta.get("map_status") != "ready":
                return gateway_error(
                    "INVALID_ARGUMENT",
                    "artifact mapping is not ready",
                )

            root_row = _row_to_dict(
                connection.execute(
                    FETCH_ROOT_SQL,
                    (WORKSPACE_ID, artifact_id, root_path),
                ).fetchone(),
                _SELECT_ROOT_COLUMNS,
            )
            if root_row is None:
                return gateway_error("NOT_FOUND", "root_path not found")

            items: list[dict[str, Any]] = []
            map_kind = str(artifact_meta.get("map_kind", "none"))
            sampled_only = map_kind == "partial"
            sample_rows: list[dict[str, Any]] = []
            map_budget_fingerprint = (
                str(artifact_meta.get("map_budget_fingerprint"))
                if isinstance(artifact_meta.get("map_budget_fingerprint"), str)
                else ""
            )

            if sampled_only:
                sample_rows = _rows_to_dicts(
                    connection.execute(
                        FETCH_SAMPLES_SQL,
                        (WORKSPACE_ID, artifact_id, root_row["root_key"]),
                    ).fetchall(),
                    _SAMPLE_COLUMNS,
                )
                corruption = self._check_sample_corruption(root_row, sample_rows)
                if corruption is not None:
                    return corruption

            if cursor_payload is not None:
                try:
                    self._assert_cursor_field(
                        cursor_payload,
                        field="root_path",
                        expected=root_path,
                    )
                    self._assert_cursor_field(
                        cursor_payload,
                        field="select_paths_hash",
                        expected=select_paths_binding_hash,
                    )
                    self._assert_cursor_field(
                        cursor_payload,
                        field="where_hash",
                        expected=where_binding_hash,
                    )
                    generation = artifact_meta.get("generation")
                    if isinstance(generation, int):
                        self._assert_cursor_field(
                            cursor_payload,
                            field="artifact_generation",
                            expected=generation,
                        )
                    if sampled_only:
                        self._assert_cursor_field(
                            cursor_payload,
                            field="map_budget_fingerprint",
                            expected=map_budget_fingerprint,
                        )
                        sample_indices = sorted(
                            int(sample_index)
                            for sample in sample_rows
                            if isinstance((sample_index := sample.get("sample_index")), int)
                        )
                        expected_sample_set_hash = compute_sample_set_hash(
                            root_path=root_path,
                            sample_indices=sample_indices,
                            map_budget_fingerprint=map_budget_fingerprint,
                        )
                        assert_sample_set_hash_binding(cursor_payload, expected_sample_set_hash)
                except (CursorStaleError, SampleSetHashBindingError) as exc:
                    if isinstance(exc, SampleSetHashBindingError):
                        return self._cursor_error(CursorStaleError(str(exc)))
                    return self._cursor_error(exc)

            if sampled_only:
                for sample in sample_rows:
                    record = sample.get("record")
                    if where_expr is not None:
                        try:
                            matches = evaluate_where(
                                record,
                                where_expr,
                                max_compute_steps=self.config.max_compute_steps,
                                max_wildcard_expansion=self.config.max_wildcards,
                            )
                        except WhereDslError as exc:
                            return gateway_error("INVALID_ARGUMENT", str(exc))
                        if not matches:
                            continue
                    projection = project_select_paths(
                        record,
                        select_paths,
                        missing_as_null=self.config.select_missing_as_null,
                        max_jsonpath_length=self.config.max_jsonpath_length,
                        max_path_segments=self.config.max_path_segments,
                        max_wildcard_expansion_total=self.config.max_wildcard_expansion_total,
                    )
                    items.append(
                        {
                            "_locator": {
                                "root_path": root_path,
                                "sample_index": sample.get("sample_index"),
                            },
                            "projection": projection,
                        }
                    )
            else:
                artifact_row = _row_to_dict(
                    connection.execute(
                        FETCH_ARTIFACT_SQL,
                        (WORKSPACE_ID, artifact_id),
                    ).fetchone(),
                    _GET_COLUMNS,
                )
                if artifact_row is None:
                    return gateway_error("NOT_FOUND", "artifact not found")
                envelope_value = artifact_row.get("envelope")
                if isinstance(envelope_value, dict):
                    envelope = envelope_value
                else:
                    envelope = reconstruct_envelope(
                        compressed_bytes=bytes(artifact_row["envelope_canonical_bytes"]),
                        encoding=str(artifact_row["envelope_canonical_encoding"]),
                        expected_hash=str(artifact_row["payload_hash_full"]),
                    )
                try:
                    root_values = evaluate_jsonpath(
                        envelope,
                        root_path,
                        max_length=self.config.max_jsonpath_length,
                        max_segments=self.config.max_path_segments,
                        max_wildcard_expansion_total=self.config.max_wildcard_expansion_total,
                    )
                except JsonPathError as exc:
                    return gateway_error("INVALID_ARGUMENT", str(exc))

                records: list[Any]
                if len(root_values) == 1 and isinstance(root_values[0], list):
                    records = list(root_values[0])
                else:
                    records = list(root_values)

                for index, record in enumerate(records):
                    if where_expr is not None:
                        try:
                            matches = evaluate_where(
                                record,
                                where_expr,
                                max_compute_steps=self.config.max_compute_steps,
                                max_wildcard_expansion=self.config.max_wildcards,
                            )
                        except WhereDslError as exc:
                            return gateway_error("INVALID_ARGUMENT", str(exc))
                        if not matches:
                            continue
                    projection = project_select_paths(
                        record,
                        select_paths,
                        missing_as_null=self.config.select_missing_as_null,
                        max_jsonpath_length=self.config.max_jsonpath_length,
                        max_path_segments=self.config.max_path_segments,
                        max_wildcard_expansion_total=self.config.max_wildcard_expansion_total,
                    )
                    items.append(
                        {
                            "_locator": {
                                "root_path": root_path,
                                "index": index,
                            },
                            "projection": projection,
                        }
                    )

            self._safe_touch_for_retrieval(
                connection,
                session_id=session_id,
                artifact_id=artifact_id,
            )
            commit = getattr(connection, "commit", None)
            if callable(commit):
                commit()

        max_items = self._bounded_limit(arguments.get("limit"))
        selected, truncated, omitted, used_bytes = apply_output_budgets(
            items[offset:],
            max_items=max_items,
            max_bytes_out=self.config.max_bytes_out,
        )
        next_cursor: str | None = None
        if truncated:
            extra: dict[str, Any] = {
                "root_path": root_path,
                "select_paths_hash": select_paths_binding_hash,
                "where_hash": where_binding_hash,
            }
            generation = artifact_meta.get("generation")
            if isinstance(generation, int):
                extra["artifact_generation"] = generation
            if sampled_only:
                sample_indices = sorted(
                    int(sample_index)
                    for sample in sample_rows
                    if isinstance((sample_index := sample.get("sample_index")), int)
                )
                sample_set_hash = compute_sample_set_hash(
                    root_path=root_path,
                    sample_indices=sample_indices,
                    map_budget_fingerprint=map_budget_fingerprint,
                )
                extra["map_budget_fingerprint"] = map_budget_fingerprint
                extra["sample_set_hash"] = sample_set_hash
            next_cursor = self._issue_cursor(
                tool="artifact.select",
                artifact_id=artifact_id,
                position_state={"offset": offset + len(selected)},
                extra=extra,
            )
        sample_indices_used = [
            int(item["_locator"]["sample_index"])
            for item in selected
            if isinstance(item.get("_locator"), dict)
            and isinstance(item["_locator"].get("sample_index"), int)
        ]
        sampled_prefix_len: int | None = None
        root_summary = root_row.get("root_summary")
        if sampled_only and isinstance(root_summary, Mapping):
            raw_sampled_prefix_len = root_summary.get("sampled_prefix_len")
            if isinstance(raw_sampled_prefix_len, int) and raw_sampled_prefix_len >= 0:
                sampled_prefix_len = raw_sampled_prefix_len
        return build_select_result(
            items=selected,
            truncated=truncated,
            cursor=next_cursor,
            sampled_only=sampled_only,
            sample_indices_used=sample_indices_used if sampled_only else None,
            sampled_prefix_len=sampled_prefix_len,
            omitted={"count": omitted, "reason": "budget"} if truncated else None,
            stats={"bytes_out": used_bytes},
        )

    async def handle_artifact_describe(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from mcp_artifact_gateway.tools.artifact_describe import (
            FETCH_DESCRIBE_SQL,
            FETCH_ROOTS_SQL,
            build_describe_response,
            validate_describe_args,
        )

        err = validate_describe_args(arguments)
        if err is not None:
            return err
        if self.db_pool is None:
            return self._not_implemented("artifact.describe")

        ctx = arguments.get("_gateway_context")
        session_id = str(ctx["session_id"]) if isinstance(ctx, dict) else ""
        artifact_id = str(arguments["artifact_id"])

        with self.db_pool.connection() as connection:
            if not self._artifact_visible(
                connection,
                session_id=session_id,
                artifact_id=artifact_id,
            ):
                return gateway_error("NOT_FOUND", "artifact not found")

            artifact_row = _row_to_dict(
                connection.execute(
                    FETCH_DESCRIBE_SQL,
                    (WORKSPACE_ID, artifact_id),
                ).fetchone(),
                _DESCRIBE_COLUMNS,
            )
            if artifact_row is None:
                return gateway_error("NOT_FOUND", "artifact not found")

            self._safe_touch_for_retrieval(
                connection,
                session_id=session_id,
                artifact_id=artifact_id,
            )
            commit = getattr(connection, "commit", None)
            if callable(commit):
                commit()

            if artifact_row.get("deleted_at") is not None:
                return gateway_error("GONE", "artifact has been deleted")

            roots = _rows_to_dicts(
                connection.execute(
                    FETCH_ROOTS_SQL,
                    (WORKSPACE_ID, artifact_id),
                ).fetchall(),
                _ROOT_COLUMNS,
            )

        return build_describe_response(artifact_row, roots)

    async def handle_artifact_find(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from mcp_artifact_gateway.tools.artifact_describe import FETCH_ROOTS_SQL
        from mcp_artifact_gateway.tools.artifact_find import (
            build_find_response,
            validate_find_args,
        )
        from mcp_artifact_gateway.tools.artifact_select import FETCH_SAMPLES_SQL

        err = validate_find_args(arguments)
        if err is not None:
            return err
        if self.db_pool is None:
            return self._not_implemented("artifact.find")

        ctx = arguments.get("_gateway_context")
        session_id = str(ctx["session_id"]) if isinstance(ctx, dict) else ""
        artifact_id = str(arguments["artifact_id"])
        root_path_filter = arguments.get("root_path")
        if root_path_filter is not None and not isinstance(root_path_filter, str):
            return gateway_error("INVALID_ARGUMENT", "root_path must be a string")
        where_expr = arguments.get("where")
        if where_expr is not None and not isinstance(where_expr, (Mapping, str)):
            return gateway_error("INVALID_ARGUMENT", "where must be an object or string")
        if where_expr is None:
            where_binding_hash = "__none__"
        else:
            try:
                where_binding_hash = where_hash(
                    where_expr,
                    mode=self.config.where_canonicalization_mode.value,
                )
            except ValueError as exc:
                return gateway_error(
                    "INVALID_ARGUMENT",
                    f"invalid where expression: {exc}",
                )
        root_path_binding = root_path_filter if isinstance(root_path_filter, str) else "__any__"

        offset = 0
        cursor_payload: dict[str, Any] | None = None
        cursor_token = arguments.get("cursor")
        if isinstance(cursor_token, str) and cursor_token:
            try:
                cursor_payload = self._verify_cursor_payload(
                    token=cursor_token,
                    tool="artifact.find",
                    artifact_id=artifact_id,
                )
                position = self._cursor_position(cursor_payload)
            except (CursorTokenError, CursorExpiredError, CursorStaleError) as exc:
                return self._cursor_error(exc)
            raw_offset = position.get("offset", 0)
            if not isinstance(raw_offset, int) or raw_offset < 0:
                return gateway_error("INVALID_ARGUMENT", "invalid cursor offset")
            offset = raw_offset

        with self.db_pool.connection() as connection:
            if not self._artifact_visible(
                connection,
                session_id=session_id,
                artifact_id=artifact_id,
            ):
                return gateway_error("NOT_FOUND", "artifact not found")

            artifact_meta = _row_to_dict(
                connection.execute(
                    _FETCH_ARTIFACT_META_SQL,
                    (WORKSPACE_ID, artifact_id),
                ).fetchone(),
                _ARTIFACT_META_COLUMNS,
            )
            if artifact_meta is None:
                return gateway_error("NOT_FOUND", "artifact not found")
            if artifact_meta.get("deleted_at") is not None:
                self._safe_touch_for_retrieval(
                    connection,
                    session_id=session_id,
                    artifact_id=artifact_id,
                )
                commit = getattr(connection, "commit", None)
                if callable(commit):
                    commit()
                return gateway_error("GONE", "artifact has been deleted")
            if artifact_meta.get("map_status") != "ready":
                return gateway_error(
                    "INVALID_ARGUMENT",
                    "artifact mapping is not ready",
                )
            map_budget_fingerprint = (
                str(artifact_meta.get("map_budget_fingerprint"))
                if isinstance(artifact_meta.get("map_budget_fingerprint"), str)
                else ""
            )
            if cursor_payload is not None:
                try:
                    self._assert_cursor_field(
                        cursor_payload,
                        field="root_path_filter",
                        expected=root_path_binding,
                    )
                    self._assert_cursor_field(
                        cursor_payload,
                        field="where_hash",
                        expected=where_binding_hash,
                    )
                    generation = artifact_meta.get("generation")
                    if isinstance(generation, int):
                        self._assert_cursor_field(
                            cursor_payload,
                            field="artifact_generation",
                            expected=generation,
                        )
                    if str(artifact_meta.get("map_kind", "none")) == "partial":
                        self._assert_cursor_field(
                            cursor_payload,
                            field="map_budget_fingerprint",
                            expected=map_budget_fingerprint,
                        )
                except CursorStaleError as exc:
                    return self._cursor_error(exc)

            roots = _rows_to_dicts(
                connection.execute(
                    FETCH_ROOTS_SQL,
                    (WORKSPACE_ID, artifact_id),
                ).fetchall(),
                _ROOT_COLUMNS,
            )
            if root_path_filter is not None:
                roots = [root for root in roots if root.get("root_path") == root_path_filter]

            items: list[dict[str, Any]] = []
            for root in roots:
                sample_rows = _rows_to_dicts(
                    connection.execute(
                        FETCH_SAMPLES_SQL,
                        (WORKSPACE_ID, artifact_id, root["root_key"]),
                    ).fetchall(),
                    _SAMPLE_COLUMNS,
                )
                corruption = self._check_sample_corruption(root, sample_rows)
                if corruption is not None:
                    return corruption
                for sample in sample_rows:
                    record = sample.get("record")
                    if where_expr is not None:
                        try:
                            matches = evaluate_where(
                                record,
                                where_expr,
                                max_compute_steps=self.config.max_compute_steps,
                                max_wildcard_expansion=self.config.max_wildcards,
                            )
                        except WhereDslError as exc:
                            return gateway_error("INVALID_ARGUMENT", str(exc))
                        if not matches:
                            continue
                    items.append(
                        {
                            "root_path": root.get("root_path"),
                            "sample_index": sample.get("sample_index"),
                            "record": record,
                            "record_hash": sample.get("record_hash"),
                        }
                    )

            self._safe_touch_for_retrieval(
                connection,
                session_id=session_id,
                artifact_id=artifact_id,
            )
            commit = getattr(connection, "commit", None)
            if callable(commit):
                commit()

            index_status = str(artifact_meta.get("index_status", "off"))

        max_items = self._bounded_limit(arguments.get("limit"))
        selected, truncated, _omitted, _used_bytes = apply_output_budgets(
            items[offset:],
            max_items=max_items,
            max_bytes_out=self.config.max_bytes_out,
        )
        next_cursor: str | None = None
        if truncated:
            extra: dict[str, Any] = {
                "root_path_filter": root_path_binding,
                "where_hash": where_binding_hash,
            }
            generation = artifact_meta.get("generation")
            if isinstance(generation, int):
                extra["artifact_generation"] = generation
            if str(artifact_meta.get("map_kind", "none")) == "partial":
                map_budget_fingerprint = (
                    str(artifact_meta.get("map_budget_fingerprint"))
                    if isinstance(artifact_meta.get("map_budget_fingerprint"), str)
                    else ""
                )
                extra["map_budget_fingerprint"] = map_budget_fingerprint
            next_cursor = self._issue_cursor(
                tool="artifact.find",
                artifact_id=artifact_id,
                position_state={"offset": offset + len(selected)},
                extra=extra,
            )
        return build_find_response(
            items=selected,
            truncated=truncated,
            cursor=next_cursor,
            sampled_only=True,
            index_status=index_status,
        )

    async def handle_artifact_chain_pages(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from mcp_artifact_gateway.tools.artifact_chain_pages import (
            FETCH_CHAIN_PAGES_SQL,
            build_chain_pages_response,
            validate_chain_pages_args,
        )

        err = validate_chain_pages_args(arguments)
        if err is not None:
            return err
        if self.db_pool is None:
            return self._not_implemented("artifact.chain_pages")

        ctx = arguments.get("_gateway_context")
        session_id = str(ctx["session_id"]) if isinstance(ctx, dict) else ""
        parent_artifact_id = str(arguments["parent_artifact_id"])

        offset = 0
        cursor_token = arguments.get("cursor")
        if isinstance(cursor_token, str) and cursor_token:
            try:
                position = self._verify_cursor(
                    token=cursor_token,
                    tool="artifact.chain_pages",
                    artifact_id=parent_artifact_id,
                )
            except (CursorTokenError, CursorExpiredError, CursorStaleError) as exc:
                return self._cursor_error(exc)
            raw_offset = position.get("offset", 0)
            if not isinstance(raw_offset, int) or raw_offset < 0:
                return gateway_error("INVALID_ARGUMENT", "invalid cursor offset")
            offset = raw_offset

        limit = self._bounded_limit(arguments.get("limit"))
        sql = f"{FETCH_CHAIN_PAGES_SQL}\nLIMIT %s OFFSET %s"

        with self.db_pool.connection() as connection:
            if not self._artifact_visible(
                connection,
                session_id=session_id,
                artifact_id=parent_artifact_id,
            ):
                return gateway_error("NOT_FOUND", "parent artifact not found")

            rows = connection.execute(
                sql,
                (WORKSPACE_ID, parent_artifact_id, limit + 1, offset),
            ).fetchall()
            mapped_rows = _rows_to_dicts(rows, _CHAIN_COLUMNS)
            page_rows = mapped_rows[:limit]
            truncated = len(mapped_rows) > limit

            touch_artifacts = [parent_artifact_id] + [
                str(row["artifact_id"])
                for row in page_rows
                if isinstance(row.get("artifact_id"), str)
            ]
            self._safe_touch_for_search(
                connection,
                session_id=session_id,
                artifact_ids=touch_artifacts,
            )
            commit = getattr(connection, "commit", None)
            if callable(commit):
                commit()

        next_cursor: str | None = None
        if truncated:
            next_cursor = self._issue_cursor(
                tool="artifact.chain_pages",
                artifact_id=parent_artifact_id,
                position_state={"offset": offset + len(page_rows)},
            )
        return build_chain_pages_response(
            page_rows,
            truncated=truncated,
            cursor=next_cursor,
        )


async def bootstrap_server(
    config: GatewayConfig,
    *,
    db_pool: ConnectionPool | None = None,
    blob_store: BlobStore | None = None,
    fs_ok: bool = True,
    db_ok: bool = True,
) -> GatewayServer:
    """Connect upstreams and return a ready-to-run server instance."""
    upstreams = await connect_upstreams(config.upstreams)
    return GatewayServer(
        config=config,
        db_pool=db_pool,
        blob_store=blob_store,
        upstreams=upstreams,
        fs_ok=fs_ok,
        db_ok=db_ok,
    )
