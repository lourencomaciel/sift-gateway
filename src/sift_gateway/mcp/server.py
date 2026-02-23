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

from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
import datetime as dt
import time
from typing import Any

from fastmcp import FastMCP

from sift_gateway.artifacts.create import (
    ArtifactHandle,
    CreateArtifactInput,
    compute_payload_sizes,
    generate_artifact_id,
    prepare_envelope_storage,
)
from sift_gateway.config.settings import GatewayConfig
from sift_gateway.constants import WORKSPACE_ID
from sift_gateway.core.capture_identity import build_capture_identity
from sift_gateway.cursor.payload import (
    CursorStaleError,
    build_cursor_payload,
)
from sift_gateway.cursor.token import (
    CursorTokenError,
    decode_cursor,
    encode_cursor,
)
from sift_gateway.envelope.model import BinaryRefContentPart, Envelope
from sift_gateway.envelope.normalize import normalize_envelope
from sift_gateway.envelope.oversize import replace_oversized_json_parts
from sift_gateway.fs.blob_store import BinaryRef, BlobStore
from sift_gateway.mapping.runner import MappingInput
from sift_gateway.mapping.worker import (
    WorkerContext,
    run_mapping_worker,
)
from sift_gateway.mcp.mirror import (
    MirroredTool,
    build_mirrored_tools,
)
from sift_gateway.mcp.server_helpers import (
    RuntimeTool,
)
from sift_gateway.mcp.server_helpers import (
    artifact_tool_description as _artifact_tool_description,
)
from sift_gateway.mcp.server_helpers import (
    assert_cursor_field as _assert_cursor_field,
)
from sift_gateway.mcp.server_helpers import (
    assert_unique_safe_tool_name as _assert_unique_safe_tool_name,
)
from sift_gateway.mcp.server_helpers import (
    check_sample_corruption as _check_sample_corruption,
)
from sift_gateway.mcp.server_helpers import (
    cursor_position as _cursor_position,
)
from sift_gateway.mcp.server_helpers import (
    mcp_safe_name as _mcp_safe_name,
)
from sift_gateway.mcp.server_helpers import (
    normalize_upstream_content as _normalize_upstream_content,
)
from sift_gateway.mcp.server_helpers import (
    not_implemented as _not_implemented,
)
from sift_gateway.mcp.server_helpers import (
    upstream_error_message as _upstream_error_message,
)
from sift_gateway.mcp.server_runtime import (
    bounded_limit as _runtime_bounded_limit,
)
from sift_gateway.mcp.server_runtime import (
    cursor_error as _runtime_cursor_error,
)
from sift_gateway.mcp.server_runtime import (
    increment_metric as _runtime_increment_metric,
)
from sift_gateway.mcp.server_runtime import (
    observe_metric as _runtime_observe_metric,
)
from sift_gateway.mcp.server_runtime import (
    probe_db_recovery as _runtime_probe_db_recovery,
)
from sift_gateway.mcp.server_runtime import (
    probe_upstream_tools as _runtime_probe_upstream_tools,
)
from sift_gateway.mcp.server_runtime import (
    record_cursor_stale_reason as _runtime_record_cursor_stale_reason,
)
from sift_gateway.mcp.server_runtime import (
    record_upstream_failure as _runtime_record_upstream_failure,
)
from sift_gateway.mcp.server_runtime import (
    record_upstream_success as _runtime_record_upstream_success,
)
from sift_gateway.mcp.server_runtime import (
    restore_protocol_response_fields as _runtime_restore_protocol_response_fields,
)
from sift_gateway.mcp.server_runtime import (
    sanitize_tool_result as _runtime_sanitize_tool_result,
)
from sift_gateway.mcp.server_runtime import (
    status_upstreams as _runtime_status_upstreams,
)
from sift_gateway.mcp.upstream import (
    UpstreamInstance,
    call_upstream_tool,
    connect_upstreams,
)
from sift_gateway.mcp.upstream_errors import classify_upstream_exception
from sift_gateway.obs.metrics import GatewayMetrics, get_metrics
from sift_gateway.security.redaction import (
    ResponseSecretRedactor,
)
from sift_gateway.tools.usage_hint import (
    PAGINATION_COMPLETENESS_RULE,
    summarize_code_query_packages,
)

_GENERIC_ARGS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {},
    "additionalProperties": True,
}
_BUILTIN_TOOL_DESCRIPTIONS: dict[str, str] = {
    "gateway.status": "Gateway health and configuration snapshot.",
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
                    'query: execute code query (query_kind="code"). '
                    "next_page: fetch next upstream page."
                ),
            },
            "query_kind": {
                "type": "string",
                "enum": ["code"],
                "description": (
                    'Required for action=query and must be "code".'
                ),
            },
            "scope": {
                "type": "string",
                "enum": ["all_related", "single"],
                "description": (
                    "[query_kind=code] all_related (default) executes across "
                    "pagination-chain related artifacts for each anchor; "
                    "single executes against only the requested artifact(s). "
                    "Prefer single unless cross-artifact logic is required."
                ),
            },
            "artifact_id": {
                "type": "string",
                "description": (
                    "Anchor artifact. Required for action=next_page. "
                    "For query_kind=code, use artifact_id (single) or "
                    "artifact_ids (multi)."
                ),
            },
            "artifact_ids": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "[query_kind=code] Optional list of anchor artifacts for "
                    "multi-artifact queries (run(artifacts, schemas, params)). "
                    "Mutually exclusive with artifact_id."
                ),
            },
            "root_path": {
                "type": "string",
                "description": (
                    "[query_kind=code] JSONPath to root records for single "
                    "queries, or one shared path across artifact_ids."
                ),
            },
            "root_paths": {
                "type": "object",
                "additionalProperties": {"type": "string"},
                "description": (
                    "[query_kind=code] Optional per-artifact root paths for "
                    "multi-artifact queries. Keys must match artifact_ids "
                    "exactly; values are JSONPath strings. Mutually exclusive "
                    "with root_path."
                ),
            },
            "code": {
                "type": "string",
                "description": (
                    "[query_kind=code] Python source defining one of: "
                    "run(data, schema, params) for single-artifact queries "
                    "(data is list[dict]) or run(artifacts, schemas, params) "
                    "for multi-artifact queries (artifacts is "
                    "dict[artifact_id -> list[dict]]). Return compact output "
                    "(aggregates or top-N rows) to reduce schema_ref responses. "
                    "Allowed imports: math, statistics, decimal, "
                    "datetime, re, itertools, collections, functools, "
                    "operator, heapq, json, csv, io "
                    "(StringIO/BytesIO only), string, textwrap, copy, "
                    "typing, dataclasses, enum, fractions, bisect, "
                    "pprint, uuid, base64, struct, array, numbers, "
                    "cmath, random, secrets, fnmatch, difflib, html, "
                    "urllib.parse. Third-party imports depend on installed "
                    "packages and configured allowlist (see Code-query "
                    "packages in tool description); allowlist can be "
                    "overridden by config."
                ),
            },
            "params": {
                "type": "object",
                "description": (
                    "[query_kind=code] JSON object passed as the third "
                    "argument to run(..., ..., params)."
                ),
                "additionalProperties": True,
            },
            "steps": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "code": {"type": "string"},
                        "params": {
                            "type": "object",
                            "additionalProperties": True,
                        },
                    },
                    "required": ["code"],
                },
                "description": (
                    "[query_kind=code] Optional pipeline of code "
                    "steps. Each step's output becomes the next "
                    "step's input. When present, top-level 'code' "
                    "is ignored."
                ),
            },
        },
        "required": ["action"],
        "additionalProperties": True,
    },
}


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

    def __post_init__(self) -> None:
        """Initialize mirrored-tool cache and response redactor defaults."""
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
        """Probe DB pool and recover db_ok if healthy."""
        return _runtime_probe_db_recovery(self)

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
        _runtime_record_upstream_failure(
            self,
            prefix=prefix,
            code=code,
            message=message,
        )

    def _record_upstream_success(self, *, prefix: str) -> None:
        """Persist the latest successful upstream-call timestamp."""
        _runtime_record_upstream_success(self, prefix=prefix)

    async def _probe_upstream_tools(
        self,
        upstream: UpstreamInstance,
    ) -> dict[str, Any]:
        """Run an active ``tools/list`` probe for one upstream."""
        return await _runtime_probe_upstream_tools(self, upstream)

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
        return await _runtime_status_upstreams(
            self, probe_upstreams=probe_upstreams
        )

    def _bounded_limit(self, raw_limit: Any) -> int:
        """Clamp a user-supplied limit to the configured maximum.

        Args:
            raw_limit: Limit value from the request arguments.

        Returns:
            Positive integer capped at config.max_items.
        """
        return _runtime_bounded_limit(self, raw_limit)

    def _increment_metric(self, attr: str, amount: int = 1) -> None:
        """Increment a counter metric by the given amount.

        Args:
            attr: Attribute name on GatewayMetrics.
            amount: Increment value. Defaults to 1.
        """
        _runtime_increment_metric(self, attr, amount)

    def _observe_metric(self, attr: str, value: float) -> None:
        """Record an observation on a histogram metric.

        Args:
            attr: Attribute name on GatewayMetrics.
            value: Observation value (e.g. latency in ms).
        """
        _runtime_observe_metric(self, attr, value)

    def _restore_protocol_response_fields(
        self,
        *,
        original: dict[str, Any],
        sanitized: dict[str, Any],
    ) -> dict[str, Any]:
        """Preserve control-plane fields that must remain protocol-stable."""
        return _runtime_restore_protocol_response_fields(
            original=original,
            sanitized=sanitized,
        )

    def _sanitize_tool_result(self, result: dict[str, Any]) -> dict[str, Any]:
        """Redact detected secrets from a tool result payload."""
        return _runtime_sanitize_tool_result(self, result)

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
        _runtime_record_cursor_stale_reason(self, message)

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
        return _runtime_cursor_error(self, token_error)

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
        from sift_gateway.mcp.handlers.common import (
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
        from sift_gateway.db.repos.sessions_repo import (
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
        from sift_gateway.db.repos.sessions_repo import (
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
        app = FastMCP(name="sift-gateway")
        safe_name_to_qualified: dict[str, str] = {}

        for tool_name, handler in self.register_tools().items():
            schema = _BUILTIN_TOOL_SCHEMAS.get(tool_name, _GENERIC_ARGS_SCHEMA)
            safe_name = _mcp_safe_name(tool_name)
            _assert_unique_safe_tool_name(
                safe_name_to_qualified,
                safe_name=safe_name,
                qualified_name=tool_name,
            )
            description = _BUILTIN_TOOL_DESCRIPTIONS.get(
                tool_name, "Gateway tool"
            )
            if tool_name == "artifact":
                description = _artifact_tool_description(
                    code_query_package_summary=summarize_code_query_packages(
                        configured_roots=(
                            self.config.code_query_allowed_import_roots
                        ),
                    ),
                )
            app.add_tool(
                RuntimeTool(
                    name=safe_name,
                    description=description,
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
        from sift_gateway.mcp.handlers.mirrored_tool import (
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
        from sift_gateway.mcp.handlers.status import (
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
        from sift_gateway.mcp.handlers.artifact_consolidated import (
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
