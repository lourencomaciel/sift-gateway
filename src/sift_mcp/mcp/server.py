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
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
import datetime as dt
import importlib.util
import json
from pathlib import Path
import shutil
import time
from typing import Any

from fastmcp import FastMCP
from fastmcp.server.dependencies import get_context
from fastmcp.tools.tool import Tool, ToolResult

from sift_mcp.artifacts.create import (
    ArtifactHandle,
    CreateArtifactInput,
    compute_payload_sizes,
    generate_artifact_id,
    prepare_envelope_storage,
)
from sift_mcp.config.settings import GatewayConfig
from sift_mcp.constants import WORKSPACE_ID
from sift_mcp.core.capture_identity import build_capture_identity
from sift_mcp.cursor.payload import (
    CursorStaleError,
    build_cursor_payload,
)
from sift_mcp.cursor.token import (
    CursorExpiredError,
    CursorTokenError,
    decode_cursor,
    encode_cursor,
)
from sift_mcp.envelope.model import BinaryRefContentPart, Envelope
from sift_mcp.envelope.normalize import normalize_envelope
from sift_mcp.envelope.oversize import replace_oversized_json_parts
from sift_mcp.envelope.responses import gateway_error
from sift_mcp.fs.blob_store import BinaryRef, BlobStore
from sift_mcp.mapping.runner import MappingInput
from sift_mcp.mapping.worker import (
    WorkerContext,
    run_mapping_worker,
)
from sift_mcp.mcp.mirror import (
    MirroredTool,
    build_mirrored_tools,
)
from sift_mcp.mcp.upstream import (
    UpstreamInstance,
    call_upstream_tool,
    connect_upstreams,
    discover_tools,
)
from sift_mcp.mcp.upstream_errors import classify_upstream_exception
from sift_mcp.obs.logging import LogEvents, get_logger
from sift_mcp.obs.metrics import GatewayMetrics, get_metrics
from sift_mcp.security.redaction import (
    ResponseSecretRedactor,
    SecretRedactionError,
)
from sift_mcp.tools.usage_hint import PAGINATION_COMPLETENESS_RULE

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
    "artifact": (
        "Interact with stored artifacts. "
        "Actions: query and next_page. "
        "For query, pass query_kind: describe|get|select|search|code. "
        "For describe/get/select, pass artifact_id. "
        "For code, pass artifact_id (single artifact) or artifact_ids "
        "(multi-artifact). "
        "Use query_kind=select with root_path/select_paths and where. "
        "Use query_kind=code with root_path/code and optional params. "
        "Schema is returned inline in query_kind=describe and in mirrored tool responses. "
        "Use count_only=true for counts, distinct=true for unique values. "
        "Continue partial results with "
        "query + cursor (not next_page). "
        "next_page is only for fetching additional "
        "upstream pages. "
        "Filtering (where) and multi-field projection "
        "(select_paths) are query_kind=select only. "
        "Code queries return all results (no pagination), limited by "
        "max_bytes_out. "
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
    "artifact": {
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": [
                    "query",
                    "next_page",
                ],
                "description": (
                    "query: explicit retrieval/search "
                    "(use query_kind to choose behavior). "
                    "next_page: fetch next upstream page."
                ),
            },
            "query_kind": {
                "type": "string",
                "enum": ["describe", "get", "select", "search", "code"],
                "description": (
                    "Required for action=query. "
                    "describe: mapping roots summary; "
                    "get: retrieve envelope/mapped values; "
                    "select: projection/filter over records; "
                    "search: workspace artifact listing; "
                    "code: execute generated Python against a mapped root."
                ),
            },
            "scope": {
                "type": "string",
                "enum": ["all_related", "single"],
                "description": (
                    "For query_kind describe/get/select: "
                    "all_related (default) queries the full lineage "
                    "component of artifact_id; single queries only "
                    "artifact_id. Ignored for query_kind=code; "
                    "not valid for query_kind=search."
                ),
            },
            "artifact_id": {
                "type": "string",
                "description": (
                    "Anchor artifact. Required for query_kind "
                    "describe|get|select and next_page. "
                    "For query_kind=code, use artifact_id (single) "
                    "or artifact_ids (multi). Omit for query_kind=search."
                ),
            },
            "artifact_ids": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "[query_kind=code] Optional list of anchor artifacts for "
                    "multi-artifact code queries. Mutually exclusive with "
                    "artifact_id."
                ),
            },
            "target": {
                "type": "string",
                "enum": ["envelope", "mapped"],
                "description": (
                    "[query_kind=get] Retrieval target (default: envelope)."
                ),
            },
            "jsonpath": {
                "type": "string",
                "description": (
                    "[query_kind=get] JSONPath filter on envelope."
                ),
            },
            "root_path": {
                "type": "string",
                "description": (
                    "[query_kind=select|code] JSONPath to root records, "
                    "from query_kind=describe output."
                ),
            },
            "root_paths": {
                "type": "object",
                "additionalProperties": {"type": "string"},
                "description": (
                    "[query_kind=code] Optional per-artifact root paths for "
                    "multi-artifact queries. Keys are artifact IDs, values are "
                    "JSONPath strings. Mutually exclusive with root_path."
                ),
            },
            "select_paths": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "[query_kind=select] Field names to project, "
                    "e.g. ['name', 'spend']. No $ prefix."
                ),
            },
            "where": {
                "type": "object",
                "description": (
                    "[query_kind=select] Structured filter object. "
                    'Leaf: {"path": "$.field", "op": "<op>", "value": <v>}. '
                    "Ops: eq, ne, gt, gte, lt, lte, in, contains, "
                    "array_contains, exists, not_exists. "
                    'Group: {"logic": "and"|"or", "filters": [...]}. '
                    'Negate: {"not": {<filter>}}. '
                    "Paths use JSONPath ($.prefix). "
                    'Examples: {"path":"$.status","op":"eq","value":"active"}, '
                    '{"path":"$.spend","op":"gt","value":0}, '
                    '{"not":{"path":"$.name","op":"in","value":["x","y"]}}.'
                ),
            },
            "code": {
                "type": "string",
                "description": (
                    "[query_kind=code] Python source defining "
                    "run(artifacts, schemas, params) for multi-artifact "
                    "queries, or run(data, schema, params) for single-artifact "
                    "queries. "
                    "Allowed imports: math, statistics, decimal, "
                    "datetime, re, itertools, collections, functools, "
                    "operator, heapq, json, csv, io "
                    "(StringIO/BytesIO only), string, textwrap, copy, "
                    "typing, dataclasses, enum, fractions, bisect, "
                    "pprint, uuid, base64, struct, array, numbers, "
                    "cmath, random, secrets, fnmatch, difflib, html, "
                    "urllib.parse, jmespath, pandas, numpy by default; "
                    "allowlist can be overridden by config."
                ),
            },
            "params": {
                "type": "object",
                "description": (
                    "[query_kind=code] JSON object passed as the third argument "
                    "to run(..., ..., params)."
                ),
                "additionalProperties": True,
            },
            "filters": {
                "type": "object",
                "description": (
                    "[query_kind=search] source_tool, kind, status, "
                    "parent_artifact_id, etc."
                ),
                "additionalProperties": True,
            },
            "query": {
                "type": "string",
                "description": (
                    "[query_kind=search] Optional full-text search query "
                    "against source_tool, kind, field names, and sample "
                    "values."
                ),
            },
            "order_by": {
                "type": "string",
                "description": (
                    "Sort order. "
                    "query_kind=search: created_seq_desc (default), "
                    "last_seen_desc, chain_seq_asc. "
                    "query_kind=select: 'field [ASC|DESC]', e.g. "
                    "'spend DESC', 'to_number(spend) DESC', "
                    "'name ASC'."
                ),
            },
            "count_only": {
                "type": "boolean",
                "description": (
                    "[query_kind=select] Return only the count of matching "
                    "records, no items. Skips projection and "
                    "pagination."
                ),
            },
            "distinct": {
                "type": "boolean",
                "description": (
                    "[query_kind=select] Deduplicate projected records. "
                    "Returns only unique projections."
                ),
            },
            "cursor": {
                "type": "string",
                "description": "[query_kind=select] Opaque pagination cursor.",
            },
            "limit": {
                "type": "integer",
                "description": "[query_kind=select|search] Max items per page.",
            },
        },
        "required": ["action"],
        "additionalProperties": True,
    },
}

_CURSOR_STALE_REASON_PATTERNS: tuple[tuple[str, str], ...] = (
    ("sample_set_hash mismatch", "sample_set_mismatch"),
    ("related_set_hash mismatch", "related_set_mismatch"),
    ("map_budget_fingerprint mismatch", "map_budget_mismatch"),
    ("traversal_contract_version mismatch", "traversal_version_mismatch"),
    ("artifact_generation mismatch", "generation_mismatch"),
    ("tool mismatch", "tool_mismatch"),
    ("artifact binding mismatch", "artifact_binding_mismatch"),
    ("workspace binding mismatch", "workspace_binding_mismatch"),
    ("mapper_version mismatch", "mapper_version_mismatch"),
    ("target mismatch", "target_mismatch"),
    ("normalized_jsonpath mismatch", "jsonpath_mismatch"),
    ("select_paths_hash mismatch", "select_paths_mismatch"),
    ("where_hash mismatch", "where_hash_mismatch"),
    ("root_path_filter mismatch", "root_path_filter_mismatch"),
    ("root_path mismatch", "root_path_mismatch"),
    ("scope mismatch", "scope_mismatch"),
)


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


def _assert_unique_safe_tool_name(
    seen: dict[str, str],
    *,
    safe_name: str,
    qualified_name: str,
) -> None:
    """Ensure MCP-safe tool names remain collision-free.

    Args:
        seen: Mapping of MCP-safe names to original qualified names.
        safe_name: Sanitized MCP-safe name.
        qualified_name: Original qualified tool name.

    Raises:
        ValueError: If a different qualified name already mapped to
            the same safe name.
    """
    existing = seen.get(safe_name)
    if existing is not None and existing != qualified_name:
        msg = (
            "tool name collision after MCP-safe sanitization: "
            f"{existing!r} and {qualified_name!r} -> {safe_name!r}"
        )
        raise ValueError(msg)
    seen[safe_name] = qualified_name


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
    probe: dict[str, Any] = {"module": module}
    try:
        spec = importlib.util.find_spec(module)
    except ModuleNotFoundError as exc:
        probe["importable"] = False
        probe["error"] = str(exc)
        return probe
    probe["importable"] = spec is not None
    if spec is None:
        probe["error"] = "module not found"
    return probe


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
    response_sanitizer: Callable[[dict[str, Any]], dict[str, Any]] | None = None

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
        if self.response_sanitizer is not None:
            result = self.response_sanitizer(result)
        return ToolResult(structured_content=result)


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
        db_pool: Database backend (SQLite), or None.
        blob_store: Content-addressed binary blob store.
        upstreams: Connected upstream MCP server instances.
        fs_ok: True if filesystem passed startup checks.
        db_ok: True if database passed startup checks.
        metrics: Prometheus-style gateway metrics.
        upstream_errors: Map of prefix to connection error.
        upstream_runtime: Per-upstream runtime probe/failure metadata.
        mirrored_tools: Qualified name to MirroredTool mapping.
    """

    config: GatewayConfig
    db_pool: Any = None  # SqliteBackend | None
    blob_store: BlobStore | None = None
    upstreams: list[UpstreamInstance] = field(default_factory=list)
    fs_ok: bool = True
    db_ok: bool = True
    metrics: GatewayMetrics = field(default_factory=get_metrics)
    upstream_errors: dict[str, str] = field(default_factory=dict)
    upstream_runtime: dict[str, dict[str, Any]] = field(default_factory=dict)
    mirrored_tools: dict[str, MirroredTool] = field(default_factory=dict)
    response_redactor: ResponseSecretRedactor | None = None
    def __post_init__(self) -> None:  # noqa: D105
        if not self.mirrored_tools and self.upstreams:
            self.mirrored_tools = build_mirrored_tools(self.upstreams)
        if self.response_redactor is None:
            self.response_redactor = ResponseSecretRedactor(
                enabled=self.config.secret_redaction_enabled,
                fail_closed=self.config.secret_redaction_fail_closed,
                max_scan_bytes=self.config.secret_redaction_max_scan_bytes,
                replacement=self.config.secret_redaction_placeholder,
            )

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
        current["last_error_at"] = dt.datetime.now(dt.UTC).isoformat()
        self.upstream_runtime[prefix] = current

    def _record_upstream_success(self, *, prefix: str) -> None:
        """Persist the latest successful upstream-call timestamp."""
        current = dict(self.upstream_runtime.get(prefix, {}))
        current["last_success_at"] = dt.datetime.now(dt.UTC).isoformat()
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

    def _restore_protocol_response_fields(
        self,
        *,
        original: dict[str, Any],
        sanitized: dict[str, Any],
    ) -> dict[str, Any]:
        """Preserve control-plane fields that must remain protocol-stable."""
        for protocol_field in ("cursor", "next_cursor"):
            if protocol_field in original:
                sanitized[protocol_field] = original[protocol_field]
        if "pagination" in original:
            sanitized["pagination"] = original["pagination"]
        return sanitized

    def _sanitize_tool_result(
        self, result: dict[str, Any]
    ) -> dict[str, Any]:
        """Redact detected secrets from a tool result payload."""
        if self.response_redactor is None:
            return result
        try:
            redaction = self.response_redactor.redact_payload(result)
        except SecretRedactionError as exc:
            self._increment_metric("secret_redaction_failures")
            get_logger(component="mcp.server").warning(
                "tool response redaction failed",
                error_type=type(exc).__name__,
            )
            return gateway_error(
                "INTERNAL",
                "response redaction failed",
            )
        if redaction.redacted_count > 0:
            self._increment_metric(
                "secret_redaction_matches",
                redaction.redacted_count,
            )
        return self._restore_protocol_response_fields(
            original=result,
            sanitized=redaction.payload,
        )

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
        reason = "unknown"
        for pattern, pattern_reason in _CURSOR_STALE_REASON_PATTERNS:
            if pattern in message:
                reason = pattern_reason
                break
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

    def _issue_cursor(
        self,
        *,
        tool: str,
        artifact_id: str,
        position_state: dict[str, Any],
        extra: dict[str, Any] | None = None,
    ) -> str:
        """Build and encode a new cursor token.

        Args:
            tool: Tool name the cursor is bound to.
            artifact_id: Artifact the cursor is bound to.
            position_state: Pagination position state dict.
            extra: Optional additional payload fields.

        Returns:
            Encoded cursor token string.
        """
        payload = build_cursor_payload(
            tool=tool,
            artifact_id=artifact_id,
            position_state=position_state,
            ttl_minutes=self.config.cursor_ttl_minutes,
            extra=extra,
        )
        return encode_cursor(payload)

    def _verify_cursor(
        self,
        *,
        token: str,
        tool: str,
        artifact_id: str,
    ) -> dict[str, Any]:
        """Verify a cursor token and return its position state.

        Args:
            token: Cursor token string.
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
        """Decode a cursor token and check tool/artifact binding.

        Args:
            token: Encoded cursor token string.
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
        payload = decode_cursor(token)
        if payload.get("tool") != tool:
            msg = "cursor tool mismatch"
            raise CursorStaleError(msg)
        if payload.get("artifact_id") != artifact_id:
            msg = "cursor artifact binding mismatch"
            raise CursorStaleError(msg)
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
        from sift_mcp.mcp.handlers.common import (
            VISIBLE_ARTIFACT_SQL,
        )

        row = connection.execute(
            VISIBLE_ARTIFACT_SQL,
            (WORKSPACE_ID, artifact_id),
        ).fetchone()
        return row is not None

    def _safe_touch_for_retrieval(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_id: str,
        now: dt.datetime | None = None,
    ) -> bool:
        """Best-effort retrieval touch for session and artifact recency.

        Args:
            connection: Active database connection.
            session_id: Session performing the retrieval.
            artifact_id: Retrieved artifact identifier.
            now: Optional override timestamp for tests.

        Returns:
            ``True`` when touch SQL was executed, ``False`` when
            the connection does not expose ``execute``.
        """
        from sift_mcp.db.repos.sessions_repo import (
            UPSERT_SESSION_SQL,
            upsert_session_params,
        )

        execute = getattr(connection, "execute", None)
        if not callable(execute):
            return False

        execute(UPSERT_SESSION_SQL, upsert_session_params(session_id))
        if now is None:
            execute(
                """
                UPDATE artifacts
                SET last_referenced_at = NOW()
                WHERE workspace_id = %s
                  AND artifact_id = %s
                  AND deleted_at IS NULL
                """,
                (WORKSPACE_ID, artifact_id),
            )
        else:
            execute(
                """
                UPDATE artifacts
                SET last_referenced_at = %s
                WHERE workspace_id = %s
                  AND artifact_id = %s
                  AND deleted_at IS NULL
                """,
                (now, WORKSPACE_ID, artifact_id),
            )
        return True

    def _safe_touch_for_retrieval_many(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_ids: Sequence[str],
        now: dt.datetime | None = None,
    ) -> bool:
        """Best-effort retrieval touch for multiple artifacts."""
        touched = False
        seen: set[str] = set()
        for artifact_id in artifact_ids:
            if artifact_id in seen:
                continue
            seen.add(artifact_id)
            touched = (
                self._safe_touch_for_retrieval(
                    connection,
                    session_id=session_id,
                    artifact_id=artifact_id,
                    now=now,
                )
                or touched
            )
        return touched

    def _safe_touch_for_search(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_ids: Sequence[str],
        now: dt.datetime | None = None,
    ) -> bool:
        """Best-effort search touch for session activity only.

        Search should not alter artifact recency because
        ``last_referenced_at`` is used for LRU/retention and
        search ordering.
        """
        from sift_mcp.db.repos.sessions_repo import (
            UPSERT_SESSION_SQL,
            upsert_session_params,
        )

        execute = getattr(connection, "execute", None)
        if not callable(execute):
            return False
        execute(UPSERT_SESSION_SQL, upsert_session_params(session_id))
        _ = (artifact_ids, now)
        return True

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
        hashes: list[str] = [
            part.binary_hash
            for part in envelope.content
            if isinstance(part, BinaryRefContentPart)
        ]
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

    async def drain_mapping_tasks(self, *, timeout: float = 30.0) -> int:
        """No-op retained for shutdown compatibility.

        Args:
            timeout: Maximum seconds to wait for tasks to
                complete. Defaults to 30.

        Returns:
            Always ``0``.
        """
        _ = timeout
        return 0

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
        capture_identity = build_capture_identity(
            artifact_kind=input_data.kind,
            request_key=input_data.request_key,
            prefix=input_data.prefix,
            tool_name=input_data.tool_name,
            upstream_instance_id=input_data.upstream_instance_id,
            capture_kind=input_data.capture_kind,
            capture_origin=input_data.capture_origin,
            capture_key=input_data.capture_key,
        )
        return ArtifactHandle(
            artifact_id=generate_artifact_id(),
            created_seq=None,
            generation=1,
            session_id=input_data.session_id,
            source_tool=f"{input_data.prefix}.{input_data.tool_name}",
            upstream_instance_id=input_data.upstream_instance_id,
            request_key=input_data.request_key,
            capture_kind=capture_identity.capture_kind,
            capture_origin=capture_identity.capture_origin,
            capture_key=capture_identity.capture_key,
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
            "artifact": self.handle_artifact,
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
        app = FastMCP(name="sift-mcp")
        safe_name_to_qualified: dict[str, str] = {}

        for tool_name, handler in self.register_tools().items():
            schema = _BUILTIN_TOOL_SCHEMAS.get(tool_name, _GENERIC_ARGS_SCHEMA)
            safe_name = _mcp_safe_name(tool_name)
            _assert_unique_safe_tool_name(
                safe_name_to_qualified,
                safe_name=safe_name,
                qualified_name=tool_name,
            )
            app.add_tool(
                RuntimeTool(
                    name=safe_name,
                    description=_BUILTIN_TOOL_DESCRIPTIONS.get(
                        tool_name, "Gateway tool"
                    ),
                    parameters=dict(schema),
                    handler=handler,
                    response_sanitizer=self._sanitize_tool_result,
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
            safe_name = _mcp_safe_name(tool_name)
            _assert_unique_safe_tool_name(
                safe_name_to_qualified,
                safe_name=safe_name,
                qualified_name=tool_name,
            )
            app.add_tool(
                RuntimeTool(
                    name=safe_name,
                    description=mirrored_description,
                    parameters=dict(mirrored.upstream_tool.input_schema),
                    handler=mirrored_handlers[tool_name],
                    response_sanitizer=self._sanitize_tool_result,
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
        from sift_mcp.mcp.handlers.mirrored_tool import (
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
        from sift_mcp.mcp.handlers.status import (
            handle_status as _handle,
        )

        return await _handle(self, arguments)

    async def handle_artifact(
        self, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        """Handle the consolidated artifact tool call.

        Routes to the appropriate handler based on the
        ``action`` parameter.

        Args:
            arguments: Raw arguments from the MCP client,
                including required ``action`` parameter.

        Returns:
            Handler result dict or gateway error.
        """
        from sift_mcp.mcp.handlers.artifact_consolidated import (
            handle_artifact as _handle,
        )

        return await _handle(self, arguments)

async def bootstrap_server(
    config: GatewayConfig,
    *,
    db_pool: Any = None,  # SqliteBackend | None
    blob_store: BlobStore | None = None,
    fs_ok: bool = True,
    db_ok: bool = True,
) -> GatewayServer:
    """Connect upstreams and return a ready-to-run server instance.

    Args:
        config: Gateway configuration.
        db_pool: Optional pre-configured database backend
            (SQLite).
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
