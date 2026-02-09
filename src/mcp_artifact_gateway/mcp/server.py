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
    prepare_envelope_storage,
)
from mcp_artifact_gateway.cache.reuse import (
    FIND_REUSABLE_BY_REQUEST_KEY_SQL,
    ReuseResult,
    check_reuse_candidate,
)
from mcp_artifact_gateway.config.settings import GatewayConfig
from mcp_artifact_gateway.constants import WORKSPACE_ID
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
from mcp_artifact_gateway.cursor.secrets import CursorSecrets, load_or_create_cursor_secrets
from mcp_artifact_gateway.envelope.model import BinaryRefContentPart, Envelope
from mcp_artifact_gateway.envelope.normalize import normalize_envelope
from mcp_artifact_gateway.envelope.oversize import replace_oversized_json_parts
from mcp_artifact_gateway.envelope.responses import gateway_error
from mcp_artifact_gateway.fs.blob_store import BlobStore
from mcp_artifact_gateway.mapping.runner import MappingInput
from mcp_artifact_gateway.mapping.worker import WorkerContext, run_mapping_worker, should_run_mapping
from mcp_artifact_gateway.mcp.mirror import (
    MirroredTool,
    build_mirrored_tools,
)
from mcp_artifact_gateway.mcp.upstream import (
    UpstreamInstance,
    call_upstream_tool,
    connect_upstreams,
)
from mcp_artifact_gateway.obs.logging import LogEvents, get_logger
from mcp_artifact_gateway.obs.metrics import GatewayMetrics, get_metrics
from mcp_artifact_gateway.sessions import touch_for_retrieval, touch_for_search

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


class RuntimeTool(Tool):
    """Custom FastMCP tool that accepts raw argument dicts."""

    handler: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]

    async def run(self, arguments: dict[str, Any]) -> ToolResult:
        result = await self.handler(arguments)
        return ToolResult(structured_content=result)


_CANDIDATE_COLUMNS = [
    "artifact_id",
    "payload_hash_full",
    "upstream_tool_schema_hash",
    "map_status",
    "generation",
]


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


@dataclass
class GatewayServer:
    """Holds runtime state and provides executable tool handlers.

    Handler logic is implemented in ``mcp.handlers.*`` modules.
    Each ``handle_*`` method delegates to the corresponding handler function,
    passing ``self`` as the context.
    """

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
    _mapping_tasks: set[asyncio.Task[None]] = field(default_factory=set)

    def __post_init__(self) -> None:
        if not self.mirrored_tools and self.upstreams:
            self.mirrored_tools = build_mirrored_tools(self.upstreams)

    # ------------------------------------------------------------------
    # Utility / infrastructure methods (used by handler modules via ctx)
    # ------------------------------------------------------------------

    def _probe_db_recovery(self) -> bool:
        """Probe DB pool and recover ``db_ok`` if the connection is healthy again.

        Called by the preflight health gate so that a transient
        ``OperationalError`` (e.g. ``PoolTimeout``) does not permanently
        disable mirrored tool calls.
        """
        if self.db_pool is None:
            return False
        try:
            with self.db_pool.connection() as conn:
                conn.execute("SELECT 1")
            self.db_ok = True
            return True
        except Exception:
            return False

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

    # -- Cursor helpers --

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

    # -- DB / visibility helpers --

    def _artifact_visible(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_id: str,
    ) -> bool:
        from mcp_artifact_gateway.mcp.handlers.common import VISIBLE_ARTIFACT_SQL

        row = connection.execute(
            VISIBLE_ARTIFACT_SQL,
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

    # -- Envelope / binary helpers --

    def _binary_hashes_from_envelope(self, envelope: Envelope) -> list[str]:
        hashes: list[str] = []
        for part in envelope.content:
            if isinstance(part, BinaryRefContentPart):
                hashes.append(part.binary_hash)
        return hashes

    # -- Mapping helpers --

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

    def _consume_mapping_task(self, task: asyncio.Task[None]) -> None:
        self._mapping_tasks.discard(task)
        try:
            task.result()
        except Exception:
            log = get_logger(component="mcp.server")
            log.error(
                LogEvents.MAPPING_FAILED,
                exc_info=task.exception(),
            )

    def _schedule_background_mapping(
        self,
        *,
        handle: ArtifactHandle,
        envelope: Envelope,
    ) -> None:
        task = asyncio.create_task(
            self._run_mapping_background(handle=handle, envelope=envelope)
        )
        self._mapping_tasks.add(task)
        task.add_done_callback(self._consume_mapping_task)

    async def drain_mapping_tasks(self, *, timeout: float = 30.0) -> int:
        """Await all pending background mapping tasks.

        Returns the number of tasks that were still pending.
        """
        pending = set(self._mapping_tasks)
        if not pending:
            return 0
        done, still_pending = await asyncio.wait(pending, timeout=timeout)
        return len(still_pending)

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

    # -- Reuse / cache helpers --

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

        from mcp_artifact_gateway.mcp.handlers.common import row_to_dict

        return check_reuse_candidate(
            row_to_dict(row, _CANDIDATE_COLUMNS),
            expected_schema_hash=expected_schema_hash,
            strict_schema_reuse=strict_schema_reuse,
        )

    # -- Envelope transformation --

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

    def _cursor_secrets_info(self) -> dict[str, Any] | None:
        """Return cursor secret metadata for the status response, or None."""
        secrets = self.cursor_secrets
        if secrets is None:
            return None
        return {
            "signing_version": secrets.signing_version,
            "active_versions": sorted(secrets.active.keys()),
        }

    # ------------------------------------------------------------------
    # Tool registration
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Handler delegation stubs
    # ------------------------------------------------------------------

    async def handle_mirrored_tool(
        self,
        mirrored: MirroredTool,
        arguments: dict[str, Any],
    ) -> dict[str, Any]:
        from mcp_artifact_gateway.mcp.handlers.mirrored_tool import (
            handle_mirrored_tool as _handle,
        )

        return await _handle(self, mirrored, arguments)

    async def handle_status(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from mcp_artifact_gateway.mcp.handlers.status import handle_status as _handle

        return await _handle(self, arguments)

    async def handle_artifact_search(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from mcp_artifact_gateway.mcp.handlers.artifact_search import (
            handle_artifact_search as _handle,
        )

        return await _handle(self, arguments)

    async def handle_artifact_get(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from mcp_artifact_gateway.mcp.handlers.artifact_get import (
            handle_artifact_get as _handle,
        )

        return await _handle(self, arguments)

    async def handle_artifact_select(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from mcp_artifact_gateway.mcp.handlers.artifact_select import (
            handle_artifact_select as _handle,
        )

        return await _handle(self, arguments)

    async def handle_artifact_describe(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from mcp_artifact_gateway.mcp.handlers.artifact_describe import (
            handle_artifact_describe as _handle,
        )

        return await _handle(self, arguments)

    async def handle_artifact_find(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from mcp_artifact_gateway.mcp.handlers.artifact_find import (
            handle_artifact_find as _handle,
        )

        return await _handle(self, arguments)

    async def handle_artifact_chain_pages(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from mcp_artifact_gateway.mcp.handlers.artifact_chain_pages import (
            handle_artifact_chain_pages as _handle,
        )

        return await _handle(self, arguments)


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
