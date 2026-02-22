"""Runtime helper functions for ``sift_gateway.mcp.server``."""

from __future__ import annotations

import asyncio
import datetime as dt
import time
from typing import Any

from sift_gateway.cursor.payload import CursorStaleError
from sift_gateway.cursor.token import CursorExpiredError
from sift_gateway.envelope.responses import gateway_error
from sift_gateway.mcp.server_helpers import (
    command_resolvable,
    stdio_module_probe,
    upstream_error_message,
)
from sift_gateway.mcp.upstream import (
    UpstreamInstance,
    call_upstream_tool,
    discover_tools,
)
from sift_gateway.mcp.upstream_errors import classify_upstream_exception
from sift_gateway.obs.logging import LogEvents, get_logger
from sift_gateway.security.redaction import SecretRedactionError

CURSOR_STALE_REASON_PATTERNS: tuple[tuple[str, str], ...] = (
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


def probe_db_recovery(server: Any) -> bool:
    """Probe DB pool and recover db_ok if healthy."""
    if server.db_pool is None:
        return False
    try:
        with server.db_pool.connection() as conn:
            conn.execute("SELECT 1")
        server.db_ok = True
        return True
    except Exception:
        return False


def record_upstream_failure(
    server: Any,
    *,
    prefix: str,
    code: str,
    message: str,
) -> None:
    """Persist the latest runtime failure metadata for an upstream."""
    current = dict(server.upstream_runtime.get(prefix, {}))
    current["last_error_code"] = code
    current["last_error_message"] = message
    current["last_error_at"] = dt.datetime.now(dt.UTC).isoformat()
    server.upstream_runtime[prefix] = current


def record_upstream_success(server: Any, *, prefix: str) -> None:
    """Persist the latest successful upstream-call timestamp."""
    current = dict(server.upstream_runtime.get(prefix, {}))
    current["last_success_at"] = dt.datetime.now(dt.UTC).isoformat()
    server.upstream_runtime[prefix] = current


async def probe_upstream_tools(
    server: Any,
    upstream: UpstreamInstance,
) -> dict[str, Any]:
    """Run an active ``tools/list`` probe for one upstream."""
    try:
        tools = await asyncio.wait_for(
            discover_tools(
                upstream.config,
                data_dir=str(server.config.data_dir),
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


async def status_upstreams(
    server: Any,
    *,
    probe_upstreams: bool = False,
) -> list[dict[str, Any]]:
    """Build upstream status entries for the status response."""
    payload: list[dict[str, Any]] = []
    by_prefix: dict[str, dict[str, Any]] = {}
    for upstream in server.upstreams:
        entry: dict[str, Any] = {
            "prefix": upstream.prefix,
            "instance_id": upstream.instance_id,
            "connected": True,
            "tool_count": len(upstream.tools),
            "transport": upstream.config.transport,
        }
        if upstream.config.transport == "stdio":
            entry["command"] = upstream.config.command
            entry["command_resolvable"] = command_resolvable(
                upstream.config.command
            )
            module_probe = stdio_module_probe(list(upstream.config.args))
            if module_probe is not None:
                entry["module_probe"] = module_probe
        else:
            entry["url"] = upstream.config.url

        runtime = server.upstream_runtime.get(upstream.prefix)
        if runtime:
            entry["runtime"] = dict(runtime)
        if probe_upstreams:
            entry["active_probe"] = await probe_upstream_tools(server, upstream)
        payload.append(entry)
        by_prefix[upstream.prefix] = entry

    for prefix, error in sorted(server.upstream_errors.items()):
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


def bounded_limit(server: Any, raw_limit: Any) -> int:
    """Clamp a user-supplied limit to the configured maximum."""
    max_items_raw = getattr(server.config, "max_items", 50)
    max_items = max_items_raw if isinstance(max_items_raw, int) else 50
    if isinstance(raw_limit, int) and raw_limit > 0:
        return min(raw_limit, max_items)
    return min(50, max_items)


def increment_metric(server: Any, attr: str, amount: int = 1) -> None:
    """Increment a counter metric by the given amount."""
    counter = getattr(server.metrics, attr, None)
    increment = getattr(counter, "inc", None)
    if callable(increment):
        increment(amount)


def observe_metric(server: Any, attr: str, value: float) -> None:
    """Record an observation on a histogram metric."""
    histogram = getattr(server.metrics, attr, None)
    observe = getattr(histogram, "observe", None)
    if callable(observe):
        observe(value)


def restore_protocol_response_fields(
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


def sanitize_tool_result(server: Any, result: dict[str, Any]) -> dict[str, Any]:
    """Redact detected secrets from a tool result payload."""
    if server.response_redactor is None:
        return result
    try:
        redaction = server.response_redactor.redact_payload(result)
    except SecretRedactionError as exc:
        increment_metric(server, "secret_redaction_failures")
        get_logger(component="mcp.server").warning(
            "tool response redaction failed",
            error_type=type(exc).__name__,
        )
        return gateway_error(
            "INTERNAL",
            "response redaction failed",
        )
    if redaction.redacted_count > 0:
        increment_metric(
            server,
            "secret_redaction_matches",
            redaction.redacted_count,
        )
    return restore_protocol_response_fields(
        original=result,
        sanitized=redaction.payload,
    )


async def call_upstream_with_metrics(
    server: Any,
    *,
    mirrored: Any,
    forwarded_args: dict[str, Any],
) -> dict[str, Any]:
    """Call an upstream tool and record timing and error metrics."""
    increment_metric(server, "upstream_calls")
    started_at = time.monotonic()
    try:
        result = await call_upstream_tool(
            mirrored.upstream,
            mirrored.original_name,
            forwarded_args,
            data_dir=str(server.config.data_dir),
        )
    except Exception as exc:
        increment_metric(server, "upstream_errors")
        record_upstream_failure(
            server,
            prefix=mirrored.prefix,
            code=classify_upstream_exception(exc),
            message=str(exc),
        )
        raise
    finally:
        observe_metric(
            server,
            "upstream_latency",
            (time.monotonic() - started_at) * 1000.0,
        )
    if bool(result.get("isError", False)):
        increment_metric(server, "upstream_errors")
        record_upstream_failure(
            server,
            prefix=mirrored.prefix,
            code="UPSTREAM_TOOL_ERROR",
            message=upstream_error_message(result),
        )
    else:
        record_upstream_success(server, prefix=mirrored.prefix)
    return result


def record_cursor_stale_reason(server: Any, message: str) -> None:
    """Log and record the stale-cursor reason from an error message."""
    reason = "unknown"
    for pattern, pattern_reason in CURSOR_STALE_REASON_PATTERNS:
        if pattern in message:
            reason = pattern_reason
            break
    log = get_logger(component="mcp.server")
    log.info(LogEvents.CURSOR_STALE, reason=reason, detail=message)
    recorder = getattr(server.metrics, "record_cursor_stale_reason", None)
    if callable(recorder):
        recorder(reason)


def cursor_error(server: Any, token_error: Exception) -> dict[str, Any]:
    """Convert a cursor exception into a gateway error response."""
    if isinstance(token_error, CursorExpiredError):
        increment_metric(server, "cursor_expired")
        return gateway_error("CURSOR_EXPIRED", "cursor expired")
    if isinstance(token_error, CursorStaleError):
        record_cursor_stale_reason(server, str(token_error))
        return gateway_error("CURSOR_STALE", str(token_error))
    increment_metric(server, "cursor_invalid")
    return gateway_error("INVALID_ARGUMENT", "invalid cursor")
