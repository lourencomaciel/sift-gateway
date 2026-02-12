"""Configure the MCP server and register runtime tools.

Provide ``GatewayServer``, the central runtime object that holds
database, blob store, upstream connections, and metrics state.
Handler methods delegate to ``mcp.handlers.*`` modules.  Exports
``bootstrap_server`` to connect upstreams and build the server.

Typical usage example::

    server = await bootstrap_server(config, db_pool=backend)
    app = server.build_fastmcp_app()
    app.run()
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import datetime as dt
import importlib.util
import json
from pathlib import Path
import shutil
import time
from typing import Any, Awaitable, Callable, Mapping

from fastmcp import FastMCP
from fastmcp.server.dependencies import get_context
from fastmcp.tools.tool import Tool, ToolResult

from sidepouch_mcp.artifacts.create import (
    ArtifactHandle,
    CreateArtifactInput,
    compute_payload_sizes,
    generate_artifact_id,
    prepare_envelope_storage,
)
from sidepouch_mcp.cache.reuse import (
    FIND_REUSABLE_BY_REQUEST_KEY_SQL,
    ReuseResult,
    check_reuse_candidate,
)
from sidepouch_mcp.config.settings import GatewayConfig
from sidepouch_mcp.constants import WORKSPACE_ID
from sidepouch_mcp.cursor.hmac import (
    CursorExpiredError,
    CursorTokenError,
    sign_cursor_payload,
    verify_cursor_token,
)
from sidepouch_mcp.cursor.payload import (
    CursorStaleError,
    assert_cursor_binding,
    build_cursor_payload,
)
from sidepouch_mcp.cursor.secrets import (
    CursorSecrets,
    load_or_create_cursor_secrets,
)
from sidepouch_mcp.envelope.model import BinaryRefContentPart, Envelope
from sidepouch_mcp.envelope.normalize import normalize_envelope
from sidepouch_mcp.envelope.oversize import replace_oversized_json_parts
from sidepouch_mcp.envelope.responses import gateway_error
from sidepouch_mcp.fs.blob_store import BinaryRef, BlobStore
from sidepouch_mcp.mapping.runner import MappingInput
from sidepouch_mcp.mapping.worker import (
    WorkerContext,
    run_mapping_worker,
    should_run_mapping,
)
from sidepouch_mcp.mcp.mirror import (
    MirroredTool,
    build_mirrored_tools,
)
from sidepouch_mcp.mcp.upstream import (
    UpstreamInstance,
    call_upstream_tool,
    connect_upstreams,
    discover_tools,
)
from sidepouch_mcp.mcp.upstream_errors import classify_upstream_exception
from sidepouch_mcp.obs.logging import LogEvents, get_logger
from sidepouch_mcp.obs.metrics import GatewayMetrics, get_metrics
from sidepouch_mcp.sessions import touch_for_retrieval, touch_for_search
from sidepouch_mcp.tools.usage_hint import PAGINATION_COMPLETENESS_RULE

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
_BUILTIN_TOOL_DESCRIPTIONS: dict[str, str] = {
    "gateway.status": "Gateway health and configuration snapshot.",
    "artifact.search": (
        "Search artifacts visible to this session. Returns "
        "summaries; use artifact.get or artifact.select to "
        "retrieve full data."
    ),
    "artifact.get": (
        "Load a stored artifact envelope or its mapped "
        "metadata. Pass target='mapped' for mapping roots."
    ),
    "artifact.select": (
        "Project specific fields from a mapped root array. "
        "Requires artifact_id, root_path (from describe), "
        "and select_paths (relative field names)."
    ),
    "artifact.describe": (
        "Describe an artifact's mapped roots, field types, "
        "and retrieval affordances. Call this first to learn "
        "available root_path and field names before using "
        "artifact.select or artifact.find."
    ),
    "artifact.find": (
        "Find matching record locators under mapped roots "
        "using optional where filters. Returns locators "
        "(root_path, index, record_hash) not full records. "
        "Use artifact.select with select_paths to retrieve "
        "specific fields from matches."
    ),
    "artifact.chain_pages": (
        "Return chain-ordered child artifacts of a parent. "
        "Use for multi-page upstream responses."
    ),
    "artifact.next_page": (
        "Fetch the next page of a paginated upstream response. "
        "Pass the artifact_id from a result that included "
        "pagination.has_next_page=true. Returns a new artifact "
        "chained to the original. "
        f"{PAGINATION_COMPLETENESS_RULE}"
    ),
}
_BUILTIN_TOOL_SCHEMAS: dict[str, dict[str, Any]] = {
    "gateway.status": {
        "type": "object",
        "properties": {
            "probe_upstreams": {
                "type": "boolean",
                "description": (
                    "When true, run active per-upstream "
                    "tool-list probes in addition to static "
                    "startup/runtime diagnostics."
                ),
            },
        },
        "additionalProperties": True,
    },
    "artifact.search": {
        "type": "object",
        "properties": {
            "order_by": {
                "type": "string",
                "enum": ["created_seq_desc", "last_seen_desc"],
                "description": (
                    "Sort order. Default: created_seq_desc."
                ),
            },
            "limit": {
                "type": "integer",
                "description": "Max results per page (default 50).",
            },
            "cursor": {
                "type": "string",
                "description": "Opaque pagination cursor.",
            },
            "filters": {
                "type": "object",
                "description": (
                    "Optional filters: source_tool, "
                    "source_tool_prefix, status (ok|error), "
                    "request_key, has_binary_refs, etc."
                ),
                "additionalProperties": True,
            },
        },
        "additionalProperties": True,
    },
    "artifact.get": {
        "type": "object",
        "properties": {
            "artifact_id": {
                "type": "string",
                "description": "Artifact identifier to retrieve.",
            },
            "target": {
                "type": "string",
                "enum": ["envelope", "mapped"],
                "description": (
                    "Retrieval target (default: envelope)."
                ),
            },
            "jsonpath": {
                "type": "string",
                "description": (
                    "JSONPath filter (only with "
                    "target=envelope)."
                ),
            },
            "cursor": {
                "type": "string",
                "description": "Opaque pagination cursor.",
            },
            "limit": {
                "type": "integer",
                "description": "Max items per page.",
            },
        },
        "required": ["artifact_id"],
        "additionalProperties": True,
    },
    "artifact.select": {
        "type": "object",
        "properties": {
            "artifact_id": {
                "type": "string",
                "description": "Artifact to select from.",
            },
            "root_path": {
                "type": "string",
                "description": (
                    "JSONPath to root array, e.g. "
                    "'$.result.data'. Get this from "
                    "artifact.describe."
                ),
            },
            "select_paths": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "Relative field names to project, "
                    "e.g. ['ad_name', 'spend']. No $ prefix."
                ),
            },
            "where": {
                "description": (
                    "Optional WHERE-DSL filter expression."
                ),
            },
            "cursor": {
                "type": "string",
                "description": "Opaque pagination cursor.",
            },
            "limit": {
                "type": "integer",
                "description": "Max items per page.",
            },
        },
        "required": ["artifact_id", "root_path", "select_paths"],
        "additionalProperties": True,
    },
    "artifact.describe": {
        "type": "object",
        "properties": {
            "artifact_id": {
                "type": "string",
                "description": "Artifact to describe.",
            },
        },
        "required": ["artifact_id"],
        "additionalProperties": True,
    },
    "artifact.find": {
        "type": "object",
        "properties": {
            "artifact_id": {
                "type": "string",
                "description": "Artifact to search within.",
            },
            "root_path": {
                "type": "string",
                "description": (
                    "Optional root path filter, e.g. "
                    "'$.result.data'."
                ),
            },
            "where": {
                "description": (
                    "Optional WHERE-DSL filter expression."
                ),
            },
            "cursor": {
                "type": "string",
                "description": "Opaque pagination cursor.",
            },
            "limit": {
                "type": "integer",
                "description": "Max items per page.",
            },
        },
        "required": ["artifact_id"],
        "additionalProperties": True,
    },
    "artifact.chain_pages": {
        "type": "object",
        "properties": {
            "parent_artifact_id": {
                "type": "string",
                "description": "Parent artifact identifier.",
            },
            "cursor": {
                "type": "string",
                "description": "Opaque pagination cursor.",
            },
            "limit": {
                "type": "integer",
                "description": "Max items per page.",
            },
        },
        "required": ["parent_artifact_id"],
        "additionalProperties": True,
    },
    "artifact.next_page": {
        "type": "object",
        "properties": {
            "artifact_id": {
                "type": "string",
                "description": (
                    "Artifact ID from a response with "
                    "pagination.has_next_page=true."
                ),
            },
        },
        "required": ["artifact_id"],
        "additionalProperties": True,
    },
}


def _not_implemented(tool_name: str) -> dict[str, Any]:
    """Return a NOT_IMPLEMENTED gateway error for a tool.

    Args:
        tool_name: Qualified name of the unimplemented tool.

    Returns:
        Gateway error dict with code NOT_IMPLEMENTED.
    """
    return gateway_error(
        "NOT_IMPLEMENTED",
        f"{tool_name} is not wired to persistence yet",
    )


def _cursor_position(payload: dict[str, Any]) -> dict[str, Any]:
    """Extract position_state dict from cursor payload.

    Args:
        payload: Decoded cursor payload dictionary.

    Returns:
        The position_state sub-dictionary.

    Raises:
        CursorTokenError: If position_state is missing or not
            a dict.
    """
    position = payload.get("position_state")
    if not isinstance(position, dict):
        msg = "cursor missing position_state"
        raise CursorTokenError(msg)
    return position


def _assert_cursor_field(
    payload: Mapping[str, Any],
    *,
    field: str,
    expected: object,
) -> None:
    """Raise CursorStaleError if a cursor field does not match.

    Args:
        payload: Decoded cursor payload mapping.
        field: Key to look up in the payload.
        expected: Value the field must equal.

    Raises:
        CursorStaleError: If the actual value differs from
            expected.
    """
    actual = payload.get(field)
    if actual != expected:
        msg = f"cursor {field} mismatch"
        raise CursorStaleError(msg)


def _check_sample_corruption(
    root_row: dict[str, Any],
    sample_rows: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Return INTERNAL error if expected sample indices are missing rows.

    Args:
        root_row: Mapping root row containing sample_indices.
        sample_rows: Fetched sample rows to verify against.

    Returns:
        Gateway error dict if corruption detected, else None.
    """
    expected_raw = root_row.get("sample_indices")
    if not isinstance(expected_raw, list) or not expected_raw:
        return None
    expected = {int(i) for i in expected_raw if isinstance(i, int)}
    actual = {
        int(row["sample_index"])
        for row in sample_rows
        if isinstance(row.get("sample_index"), int)
    }
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


def _mcp_safe_name(qualified_name: str) -> str:
    """Convert a dotted qualified name to an MCP-safe name.

    MCP clients require tool names matching the pattern
    ``^[a-zA-Z0-9_-]{1,64}$``.  Dots are replaced with
    underscores.

    Args:
        qualified_name: Internal qualified tool name
            (e.g. ``gateway.status``).

    Returns:
        MCP-safe tool name (e.g. ``gateway_status``).
    """
    return qualified_name.replace(".", "_")


def _command_resolvable(command: str | None) -> bool:
    """Return whether a stdio command appears resolvable on this host."""
    if not command:
        return False
    if "/" in command:
        candidate = Path(command)
        return candidate.exists() and candidate.is_file()
    return shutil.which(command) is not None


def _stdio_module_probe(args: list[str]) -> dict[str, Any] | None:
    """Return module import diagnostics for ``python -m <module>`` launches."""
    if len(args) < 2 or args[0] != "-m":
        return None
    module = args[1]
    spec = importlib.util.find_spec(module)
    return {
        "module": module,
        "importable": spec is not None,
    }


def _ensure_gateway_context(arguments: dict[str, Any]) -> dict[str, Any]:
    """Auto-inject ``_gateway_context.session_id`` from MCP transport.

    When the client omits ``_gateway_context`` or its ``session_id``,
    derives one from the FastMCP session context so that callers
    (e.g. Claude Code) need not manually supply it.

    Args:
        arguments: Raw tool arguments from the MCP client.

    Returns:
        Arguments dict with ``_gateway_context.session_id``
        guaranteed present.
    """
    ctx = arguments.get("_gateway_context")
    if isinstance(ctx, dict) and ctx.get("session_id"):
        return arguments
    try:
        mcp_ctx = get_context()
        session_id = mcp_ctx.session_id
    except RuntimeError:
        return arguments
    gw_ctx: dict[str, Any] = dict(ctx) if isinstance(ctx, dict) else {}
    gw_ctx.setdefault("session_id", session_id)
    return {**arguments, "_gateway_context": gw_ctx}


class RuntimeTool(Tool):
    """FastMCP tool subclass that accepts raw argument dicts.

    Bypasses FastMCP's Pydantic argument parsing so that gateway
    tools and mirrored upstream tools receive the raw ``dict``
    directly.

    Attributes:
        handler: Async callable that processes the raw arguments
            and returns a structured result dict.
    """

    handler: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]

    async def run(self, arguments: dict[str, Any]) -> ToolResult:
        """Execute the handler with raw arguments.

        Auto-injects ``_gateway_context.session_id`` from the
        MCP transport session when the client omits it.

        Args:
            arguments: Raw argument dict passed by the MCP
                client.

        Returns:
            ToolResult wrapping the handler's structured
            content dict.
        """
        arguments = _ensure_gateway_context(arguments)
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
    """Extract a human-readable error message from an upstream result.

    Args:
        result: Upstream tool call result dict.

    Returns:
        First non-empty text block, or a generic fallback string.
    """
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
    """Normalize upstream content blocks into envelope parts.

    Converts structured content and MCP content blocks into
    the canonical part types recognised by the envelope model.

    Args:
        content: List of MCP content blocks, or None.
        structured_content: Optional structured JSON content
            returned by the upstream tool.

    Returns:
        List of normalized content-part mappings.
    """
    normalized: list[Mapping[str, Any]] = []
    if isinstance(structured_content, (dict, list)):
        normalized.append({"type": "json", "value": structured_content})
    elif structured_content is not None:
        normalized.append(
            {
                "type": "text",
                "text": json.dumps(structured_content, ensure_ascii=False),
            }
        )

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
    """Hold runtime state and provide executable tool handlers.

    Central object wiring configuration, database pool, blob store,
    upstream connections, and metrics.  Handler logic lives in
    ``mcp.handlers.*`` modules; each ``handle_*`` method delegates
    to the corresponding handler function passing ``self``.

    Attributes:
        config: Gateway configuration.
        db_pool: Database backend (Postgres or SQLite), or None.
        blob_store: Content-addressed binary blob store.
        upstreams: Connected upstream MCP server instances.
        fs_ok: True if filesystem passed startup checks.
        db_ok: True if database passed startup checks.
        metrics: Prometheus-style gateway metrics.
        cursor_secrets: HMAC signing secrets for cursors.
        upstream_errors: Map of prefix to connection error.
        upstream_runtime: Per-upstream runtime probe/failure metadata.
        mirrored_tools: Qualified name to MirroredTool mapping.
    """

    config: GatewayConfig
    db_pool: Any = None  # DatabaseBackend | None (Postgres or SQLite)
    blob_store: BlobStore | None = None
    upstreams: list[UpstreamInstance] = field(default_factory=list)
    fs_ok: bool = True
    db_ok: bool = True
    metrics: GatewayMetrics = field(default_factory=get_metrics)
    cursor_secrets: CursorSecrets | None = None
    upstream_errors: dict[str, str] = field(default_factory=dict)
    upstream_runtime: dict[str, dict[str, Any]] = field(default_factory=dict)
    mirrored_tools: dict[str, MirroredTool] = field(default_factory=dict)
    _mapping_tasks: set[asyncio.Task[None]] = field(default_factory=set)

    def __post_init__(self) -> None:  # noqa: D105
        if not self.mirrored_tools and self.upstreams:
            self.mirrored_tools = build_mirrored_tools(self.upstreams)

    # ------------------------------------------------------------------
    # Utility / infrastructure methods (used by handler modules via ctx)
    # ------------------------------------------------------------------

    def _probe_db_recovery(self) -> bool:
        """Probe DB pool and recover db_ok if healthy.

        Called by the preflight health gate so that a transient
        OperationalError (e.g. PoolTimeout) does not permanently
        disable mirrored tool calls.

        Returns:
            True if the database connection is healthy and
            db_ok was restored, False otherwise.
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

    def _not_implemented(self, tool_name: str) -> dict[str, Any]:
        """Return a NOT_IMPLEMENTED error for a tool.

        Args:
            tool_name: Qualified name of the unimplemented tool.

        Returns:
            Gateway error dict with code NOT_IMPLEMENTED.
        """
        return _not_implemented(tool_name)

    def _record_upstream_failure(
        self,
        *,
        prefix: str,
        code: str,
        message: str,
    ) -> None:
        """Persist the latest runtime failure metadata for an upstream."""
        current = dict(self.upstream_runtime.get(prefix, {}))
        current["last_error_code"] = code
        current["last_error_message"] = message
        current["last_error_at"] = dt.datetime.now(
            dt.timezone.utc
        ).isoformat()
        self.upstream_runtime[prefix] = current

    def _record_upstream_success(self, *, prefix: str) -> None:
        """Persist the latest successful upstream-call timestamp."""
        current = dict(self.upstream_runtime.get(prefix, {}))
        current["last_success_at"] = dt.datetime.now(
            dt.timezone.utc
        ).isoformat()
        self.upstream_runtime[prefix] = current

    async def _probe_upstream_tools(
        self,
        upstream: UpstreamInstance,
    ) -> dict[str, Any]:
        """Run an active ``tools/list`` probe for one upstream."""
        try:
            tools = await asyncio.wait_for(
                discover_tools(
                    upstream.config,
                    data_dir=str(self.config.data_dir),
                ),
                timeout=5.0,
            )
        except Exception as exc:
            return {
                "ok": False,
                "error_code": classify_upstream_exception(exc),
                "error": str(exc),
            }
        return {
            "ok": True,
            "tool_count": len(tools),
        }

    async def _status_upstreams(
        self,
        *,
        probe_upstreams: bool = False,
    ) -> list[dict[str, Any]]:
        """Build upstream status entries for the status response.

        Returns:
            List of dicts describing each upstream's connection
            state, tool count, and any errors.
        """
        payload: list[dict[str, Any]] = []
        by_prefix: dict[str, dict[str, Any]] = {}
        for upstream in self.upstreams:
            entry: dict[str, Any] = {
                "prefix": upstream.prefix,
                "instance_id": upstream.instance_id,
                "connected": True,
                "tool_count": len(upstream.tools),
                "transport": upstream.config.transport,
            }
            if upstream.config.transport == "stdio":
                entry["command"] = upstream.config.command
                entry["command_resolvable"] = _command_resolvable(
                    upstream.config.command
                )
                module_probe = _stdio_module_probe(list(upstream.config.args))
                if module_probe is not None:
                    entry["module_probe"] = module_probe
            else:
                entry["url"] = upstream.config.url

            runtime = self.upstream_runtime.get(upstream.prefix)
            if runtime:
                entry["runtime"] = dict(runtime)
            if probe_upstreams:
                entry["active_probe"] = await self._probe_upstream_tools(
                    upstream
                )
            payload.append(entry)
            by_prefix[upstream.prefix] = entry

        for prefix, error in sorted(self.upstream_errors.items()):
            if prefix in by_prefix:
                by_prefix[prefix]["startup_error"] = {
                    "code": "UPSTREAM_STARTUP_FAILURE",
                    "message": error,
                }
                continue
            payload.append(
                {
                    "prefix": prefix,
                    "connected": False,
                    "tool_count": 0,
                    "transport": None,
                    "startup_error": {
                        "code": "UPSTREAM_STARTUP_FAILURE",
                        "message": error,
                    },
                }
            )
        return payload

    def _bounded_limit(self, raw_limit: Any) -> int:
        """Clamp a user-supplied limit to the configured maximum.

        Args:
            raw_limit: Limit value from the request arguments.

        Returns:
            Positive integer capped at config.max_items.
        """
        if isinstance(raw_limit, int) and raw_limit > 0:
            return min(raw_limit, self.config.max_items)
        return min(50, self.config.max_items)

    def _increment_metric(self, attr: str, amount: int = 1) -> None:
        """Increment a counter metric by the given amount.

        Args:
            attr: Attribute name on GatewayMetrics.
            amount: Increment value. Defaults to 1.
        """
        counter = getattr(self.metrics, attr, None)
        increment = getattr(counter, "inc", None)
        if callable(increment):
            increment(amount)

    def _observe_metric(self, attr: str, value: float) -> None:
        """Record an observation on a histogram metric.

        Args:
            attr: Attribute name on GatewayMetrics.
            value: Observation value (e.g. latency in ms).
        """
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
        """Call an upstream tool and record timing and error metrics.

        Args:
            mirrored: Mirrored tool descriptor.
            forwarded_args: Arguments to forward to the upstream.

        Returns:
            Raw result dict from the upstream tool call.
        """
        self._increment_metric("upstream_calls")
        started_at = time.monotonic()
        try:
            result = await call_upstream_tool(
                mirrored.upstream,
                mirrored.original_name,
                forwarded_args,
                data_dir=str(self.config.data_dir),
            )
        except Exception as exc:
            self._increment_metric("upstream_errors")
            self._record_upstream_failure(
                prefix=mirrored.prefix,
                code=classify_upstream_exception(exc),
                message=str(exc),
            )
            raise
        finally:
            self._observe_metric(
                "upstream_latency",
                (time.monotonic() - started_at) * 1000.0,
            )
        if bool(result.get("isError", False)):
            self._increment_metric("upstream_errors")
            self._record_upstream_failure(
                prefix=mirrored.prefix,
                code="UPSTREAM_TOOL_ERROR",
                message=_upstream_error_message(result),
            )
        else:
            self._record_upstream_success(prefix=mirrored.prefix)
        return result

    # -- Cursor helpers --

    def _record_cursor_stale_reason(self, message: str) -> None:
        """Log and record the stale-cursor reason from an error message.

        Args:
            message: CursorStaleError message string.
        """
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
        elif "tool mismatch" in message:
            reason = "tool_mismatch"
        elif "artifact binding mismatch" in message:
            reason = "artifact_binding_mismatch"
        elif "workspace binding mismatch" in message:
            reason = "workspace_binding_mismatch"
        elif "mapper_version mismatch" in message:
            reason = "mapper_version_mismatch"
        elif "target mismatch" in message:
            reason = "target_mismatch"
        elif "normalized_jsonpath mismatch" in message:
            reason = "jsonpath_mismatch"
        elif "select_paths_hash mismatch" in message:
            reason = "select_paths_mismatch"
        elif "where_hash mismatch" in message:
            reason = "where_hash_mismatch"
        elif "root_path_filter mismatch" in message:
            reason = "root_path_filter_mismatch"
        elif "root_path mismatch" in message:
            reason = "root_path_mismatch"
        else:
            reason = "unknown"
        log = get_logger(component="mcp.server")
        log.info(LogEvents.CURSOR_STALE, reason=reason, detail=message)
        recorder = getattr(self.metrics, "record_cursor_stale_reason", None)
        if callable(recorder):
            recorder(reason)

    def _cursor_session_artifact_id(
        self, session_id: str, order_by: str
    ) -> str:
        """Build a synthetic artifact ID for session-scoped cursors.

        Args:
            session_id: Active session identifier.
            order_by: Sort key used in the search query.

        Returns:
            Composite string used as the cursor's artifact
            binding.
        """
        return f"session:{session_id}:{order_by}"

    def _cursor_error(self, token_error: Exception) -> dict[str, Any]:
        """Convert a cursor exception into a gateway error response.

        Records the appropriate metric counter before returning.

        Args:
            token_error: Exception from cursor verification.

        Returns:
            Gateway error dict with the matching error code.
        """
        if isinstance(token_error, CursorExpiredError):
            self._increment_metric("cursor_expired")
            return gateway_error("CURSOR_EXPIRED", "cursor expired")
        if isinstance(token_error, CursorStaleError):
            self._record_cursor_stale_reason(str(token_error))
            return gateway_error("CURSOR_STALE", str(token_error))
        self._increment_metric("cursor_invalid")
        return gateway_error("INVALID_ARGUMENT", "invalid cursor")

    def _get_cursor_secrets(self) -> CursorSecrets:
        """Load or return cached HMAC signing secrets.

        Returns:
            CursorSecrets used for signing and verifying
            cursor tokens.
        """
        if self.cursor_secrets is None:
            self.cursor_secrets = load_or_create_cursor_secrets(
                self.config.secrets_path
            )
        return self.cursor_secrets

    def _issue_cursor(
        self,
        *,
        tool: str,
        artifact_id: str,
        position_state: dict[str, Any],
        extra: dict[str, Any] | None = None,
    ) -> str:
        """Build and sign a new cursor token.

        Args:
            tool: Tool name the cursor is bound to.
            artifact_id: Artifact the cursor is bound to.
            position_state: Pagination position state dict.
            extra: Optional additional payload fields.

        Returns:
            HMAC-signed cursor token string.
        """
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
        """Verify a cursor token and return its position state.

        Args:
            token: Signed cursor token string.
            tool: Expected tool binding.
            artifact_id: Expected artifact binding.

        Returns:
            Position state dict extracted from the cursor.

        Raises:
            CursorTokenError: If the token is invalid.
            CursorExpiredError: If the token has expired.
            CursorStaleError: If bindings do not match.
        """
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
        """Verify a cursor token and return the full payload.

        Args:
            token: Signed cursor token string.
            tool: Expected tool binding.
            artifact_id: Expected artifact binding.

        Returns:
            Full decoded cursor payload dict including
            position_state.

        Raises:
            CursorTokenError: If the token or payload is
                invalid.
            CursorExpiredError: If the token has expired.
            CursorStaleError: If bindings do not match.
        """
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

    def _cursor_position(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Extract position_state from a cursor payload.

        Args:
            payload: Decoded cursor payload dictionary.

        Returns:
            The position_state sub-dictionary.

        Raises:
            CursorTokenError: If position_state is missing or
                not a dict.
        """
        return _cursor_position(payload)

    def _assert_cursor_field(
        self,
        payload: Mapping[str, Any],
        *,
        field: str,
        expected: object,
    ) -> None:
        """Assert a cursor payload field matches the expected value.

        Args:
            payload: Decoded cursor payload mapping.
            field: Key to check in the payload.
            expected: Required value for the field.

        Raises:
            CursorStaleError: If the actual value differs from
                expected.
        """
        _assert_cursor_field(payload, field=field, expected=expected)

    # -- DB / visibility helpers --

    def _artifact_visible(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_id: str,
    ) -> bool:
        """Check whether an artifact is visible to a session.

        Args:
            connection: Active database connection.
            session_id: Session to check visibility for.
            artifact_id: Artifact identifier to look up.

        Returns:
            True if the artifact is visible to the session.
        """
        from sidepouch_mcp.mcp.handlers.common import (
            VISIBLE_ARTIFACT_SQL,
        )

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
        """Record a retrieval touch if the connection supports it.

        Args:
            connection: Active database connection.
            session_id: Session performing the retrieval.
            artifact_id: Artifact being retrieved.
        """
        if callable(getattr(connection, "cursor", None)):
            touch_for_retrieval(connection, session_id, artifact_id)

    def _safe_touch_for_search(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_ids: list[str],
    ) -> None:
        """Record a search touch if the connection supports it.

        Args:
            connection: Active database connection.
            session_id: Session performing the search.
            artifact_ids: Artifacts returned in the results.
        """
        if callable(getattr(connection, "cursor", None)):
            touch_for_search(connection, session_id, artifact_ids)

    def _check_sample_corruption(
        self,
        root_row: dict[str, Any],
        sample_rows: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        """Detect missing sample rows for a mapping root.

        Args:
            root_row: Mapping root row with sample_indices.
            sample_rows: Fetched sample rows to verify.

        Returns:
            Gateway INTERNAL error dict if corruption found,
            else None.
        """
        return _check_sample_corruption(root_row, sample_rows)

    # -- Envelope / binary helpers --

    def _binary_hashes_from_envelope(self, envelope: Envelope) -> list[str]:
        """Collect binary blob hashes from an envelope's content.

        Args:
            envelope: Envelope whose content parts to inspect.

        Returns:
            List of binary_hash strings from BinaryRefContentPart
            entries.
        """
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
        """Build a MappingInput from an artifact and its envelope.

        Args:
            artifact_id: Unique artifact identifier.
            payload_hash_full: Full payload content hash.
            envelope: Normalized envelope for the artifact.

        Returns:
            MappingInput ready for the mapping worker.
        """
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
        """Run the mapping worker synchronously on this connection.

        Args:
            connection: Active database connection.
            handle: Artifact handle with metadata.
            envelope: Normalized envelope to map.

        Returns:
            True if the mapping worker completed successfully.
        """
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
        """Run the mapping worker in a background thread.

        Opens a fresh database connection from the pool and
        executes the mapping worker via asyncio.to_thread.

        Args:
            handle: Artifact handle with metadata.
            envelope: Normalized envelope to map.
        """
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
        """Handle completion of a background mapping task.

        Removes the task from the pending set and logs any
        exception that occurred during execution.

        Args:
            task: Completed asyncio task to consume.
        """
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
        """Schedule a mapping worker as a background asyncio task.

        Args:
            handle: Artifact handle with metadata.
            envelope: Normalized envelope to map.
        """
        task = asyncio.create_task(
            self._run_mapping_background(handle=handle, envelope=envelope)
        )
        self._mapping_tasks.add(task)
        task.add_done_callback(self._consume_mapping_task)

    async def drain_mapping_tasks(self, *, timeout: float = 30.0) -> int:
        """Await all pending background mapping tasks.

        Args:
            timeout: Maximum seconds to wait for tasks to
                complete. Defaults to 30.

        Returns:
            Number of tasks still pending after the timeout.
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
        """Trigger mapping for an artifact based on configured mode.

        Runs inline for sync/hybrid modes or schedules a
        background task for async mode.  No-ops if the
        artifact's map_status does not require mapping.

        Args:
            connection: Active database connection.
            handle: Artifact handle with metadata.
            envelope: Normalized envelope to map.
        """
        if not should_run_mapping(handle.map_status):
            return
        mode = self.config.mapping_mode.value
        if mode in {"sync", "hybrid"}:
            self._run_mapping_inline(
                connection, handle=handle, envelope=envelope
            )
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
        """Check if an existing artifact can be reused for a request.

        Args:
            connection: Active database connection.
            request_key: Hash key identifying the request.
            expected_schema_hash: Expected upstream tool schema
                hash, or None to skip the check.
            strict_schema_reuse: If True, reject reuse when
                the schema hash differs.

        Returns:
            ReuseResult indicating whether reuse is possible.
        """
        row = connection.execute(
            FIND_REUSABLE_BY_REQUEST_KEY_SQL,
            (WORKSPACE_ID, request_key),
        ).fetchone()

        from sidepouch_mcp.mcp.handlers.common import row_to_dict

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
    ) -> tuple[Envelope, list[BinaryRef]]:
        """Convert a raw upstream result into a normalized envelope.

        Handles error extraction, content normalization, and
        oversized JSON part replacement when a blob store is
        available.

        Args:
            mirrored: Mirrored tool descriptor.
            upstream_result: Raw result dict from the upstream
                tool call.

        Returns:
            A tuple of (normalized Envelope, list of BinaryRef
            objects created by oversize replacement).
        """
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
            return envelope, []
        binary_refs: list[BinaryRef] = []
        transformed = replace_oversized_json_parts(
            envelope,
            max_json_part_parse_bytes=self.config.max_json_part_parse_bytes,
            blob_store=self.blob_store,
            binary_refs_out=binary_refs,
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
        return transformed, binary_refs

    def _build_non_persisted_handle(
        self,
        *,
        input_data: CreateArtifactInput,
    ) -> ArtifactHandle:
        """Build an ArtifactHandle without database persistence.

        Used when the database is unavailable to construct a
        handle with computed hashes and sizes for passthrough
        responses.

        Args:
            input_data: Artifact creation input with envelope
                and metadata.

        Returns:
            ArtifactHandle with computed payload hashes and
            size fields.
        """
        payload_hash, _, _, _ = prepare_envelope_storage(
            input_data.envelope, self.config
        )
        payload_json_bytes, payload_binary_bytes_total, payload_total_bytes = (
            compute_payload_sizes(input_data.envelope)
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
                else (
                    f"{input_data.envelope.error.code}"
                    f": {input_data.envelope.error.message}"
                )
            ),
        )

    def _cursor_secrets_info(self) -> dict[str, Any] | None:
        """Return cursor secret metadata for the status response.

        Returns:
            Dict with signing_version and active_versions, or
            None if cursor secrets are not loaded.
        """
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
        """Return a mapping of built-in tool names to handlers.

        Returns:
            Dict mapping qualified tool names to their async
            handler callables.
        """
        return {
            "gateway.status": self.handle_status,
            "artifact.search": self.handle_artifact_search,
            "artifact.get": self.handle_artifact_get,
            "artifact.select": self.handle_artifact_select,
            "artifact.describe": self.handle_artifact_describe,
            "artifact.find": self.handle_artifact_find,
            "artifact.chain_pages": self.handle_artifact_chain_pages,
            "artifact.next_page": self.handle_artifact_next_page,
        }

    def register_mirrored_tools(
        self,
    ) -> dict[str, Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]]:
        """Return a mapping of mirrored tool names to handlers.

        Creates a closure per mirrored tool that delegates to
        handle_mirrored_tool with the correct MirroredTool
        descriptor.

        Returns:
            Dict mapping qualified mirrored tool names to their
            async handler callables.
        """
        handlers: dict[
            str, Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]
        ] = {}
        for qualified_name, mirrored in self.mirrored_tools.items():

            async def _handler(
                arguments: dict[str, Any],
                mirrored_tool: MirroredTool = mirrored,
            ) -> dict[str, Any]:
                return await self.handle_mirrored_tool(mirrored_tool, arguments)

            handlers[qualified_name] = _handler
        return handlers

    def build_fastmcp_app(self) -> FastMCP:
        """Build a FastMCP application with all registered tools.

        Registers both built-in gateway tools and mirrored
        upstream tools as RuntimeTool instances.  Tool names
        are sanitised to satisfy MCP client naming rules.

        Returns:
            Configured FastMCP application ready to run.
        """
        app = FastMCP(name="sidepouch-mcp")

        for tool_name, handler in self.register_tools().items():
            schema = _BUILTIN_TOOL_SCHEMAS.get(
                tool_name, _GENERIC_ARGS_SCHEMA
            )
            app.add_tool(
                RuntimeTool(
                    name=_mcp_safe_name(tool_name),
                    description=_BUILTIN_TOOL_DESCRIPTIONS.get(
                        tool_name, "Gateway tool"
                    ),
                    parameters=dict(schema),
                    handler=handler,
                )
            )

        mirrored_handlers = self.register_mirrored_tools()
        for tool_name, mirrored in self.mirrored_tools.items():
            mirrored_description = (
                mirrored.upstream_tool.description
                or f"Mirrored upstream tool {mirrored.original_name}"
            )
            if not mirrored_description.endswith("."):
                mirrored_description = f"{mirrored_description}."
            mirrored_description = (
                f"{mirrored_description} {PAGINATION_COMPLETENESS_RULE}"
            )
            app.add_tool(
                RuntimeTool(
                    name=_mcp_safe_name(tool_name),
                    description=mirrored_description,
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
        """Handle a call to a mirrored upstream tool.

        Delegates to the mirrored_tool handler module.

        Args:
            mirrored: Mirrored tool descriptor.
            arguments: Raw arguments from the MCP client.

        Returns:
            Structured result dict for the tool response.
        """
        from sidepouch_mcp.mcp.handlers.mirrored_tool import (
            handle_mirrored_tool as _handle,
        )

        return await _handle(self, mirrored, arguments)

    async def handle_status(self, arguments: dict[str, Any]) -> dict[str, Any]:
        """Handle the gateway.status tool call.

        Args:
            arguments: Raw arguments from the MCP client.

        Returns:
            Status snapshot dict with health and config info.
        """
        from sidepouch_mcp.mcp.handlers.status import (
            handle_status as _handle,
        )

        return await _handle(self, arguments)

    async def handle_artifact_search(
        self, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        """Handle the artifact.search tool call.

        Args:
            arguments: Raw arguments from the MCP client.

        Returns:
            Search results dict with matching artifacts.
        """
        from sidepouch_mcp.mcp.handlers.artifact_search import (
            handle_artifact_search as _handle,
        )

        return await _handle(self, arguments)

    async def handle_artifact_get(
        self, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        """Handle the artifact.get tool call.

        Args:
            arguments: Raw arguments from the MCP client.

        Returns:
            Artifact envelope or mapped value dict.
        """
        from sidepouch_mcp.mcp.handlers.artifact_get import (
            handle_artifact_get as _handle,
        )

        return await _handle(self, arguments)

    async def handle_artifact_select(
        self, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        """Handle the artifact.select tool call.

        Args:
            arguments: Raw arguments from the MCP client.

        Returns:
            Projected and filtered artifact data dict.
        """
        from sidepouch_mcp.mcp.handlers.artifact_select import (
            handle_artifact_select as _handle,
        )

        return await _handle(self, arguments)

    async def handle_artifact_describe(
        self, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        """Handle the artifact.describe tool call.

        Args:
            arguments: Raw arguments from the MCP client.

        Returns:
            Mapped root descriptions and retrieval affordances.
        """
        from sidepouch_mcp.mcp.handlers.artifact_describe import (
            handle_artifact_describe as _handle,
        )

        return await _handle(self, arguments)

    async def handle_artifact_find(
        self, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        """Handle the artifact.find tool call.

        Args:
            arguments: Raw arguments from the MCP client.

        Returns:
            Matching records under mapped roots.
        """
        from sidepouch_mcp.mcp.handlers.artifact_find import (
            handle_artifact_find as _handle,
        )

        return await _handle(self, arguments)

    async def handle_artifact_chain_pages(
        self, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        """Handle the artifact.chain_pages tool call.

        Args:
            arguments: Raw arguments from the MCP client.

        Returns:
            Chain-ordered child artifacts dict.
        """
        from sidepouch_mcp.mcp.handlers.artifact_chain_pages import (
            handle_artifact_chain_pages as _handle,
        )

        return await _handle(self, arguments)

    async def handle_artifact_next_page(
        self, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        """Handle the artifact.next_page tool call.

        Args:
            arguments: Raw arguments from the MCP client.

        Returns:
            Next-page upstream result as a gateway tool result.
        """
        from sidepouch_mcp.mcp.handlers.artifact_next_page import (
            handle_artifact_next_page as _handle,
        )

        return await _handle(self, arguments)


async def bootstrap_server(
    config: GatewayConfig,
    *,
    db_pool: Any = None,  # DatabaseBackend | None (Postgres or SQLite)
    blob_store: BlobStore | None = None,
    fs_ok: bool = True,
    db_ok: bool = True,
) -> GatewayServer:
    """Connect upstreams and return a ready-to-run server instance.

    Args:
        config: Gateway configuration.
        db_pool: Optional pre-configured database backend
            (Postgres or SQLite).
        blob_store: Optional content-addressed blob store.
        fs_ok: Whether the filesystem passed startup checks.
        db_ok: Whether the database passed startup checks.

    Returns:
        A fully initialized GatewayServer with connected
        upstreams.
    """
    upstreams = await connect_upstreams(
        config.upstreams, data_dir=str(config.data_dir)
    )
    return GatewayServer(
        config=config,
        db_pool=db_pool,
        blob_store=blob_store,
        upstreams=upstreams,
        fs_ok=fs_ok,
        db_ok=db_ok,
    )
