"""Core capture service for protocol-neutral artifact ingestion."""

from __future__ import annotations

from collections.abc import Mapping
import datetime as dt
from typing import Any, Literal

from sift_gateway.artifacts.create import (
    CreateArtifactInput,
    persist_artifact,
)
from sift_gateway.constants import (
    CAPTURE_KIND_CLI_COMMAND,
    CAPTURE_KIND_DERIVED_CODEGEN,
    CAPTURE_KIND_DERIVED_QUERY,
    CAPTURE_KIND_FILE_INGEST,
    CAPTURE_KIND_MCP_TOOL,
    CAPTURE_KIND_STDIN_PIPE,
    WORKSPACE_ID,
)
from sift_gateway.core.runtime import ArtifactCaptureRuntime
from sift_gateway.envelope.model import (
    Envelope,
    ErrorBlock,
    JsonContentPart,
)

_VALID_CAPTURE_KINDS = {
    CAPTURE_KIND_MCP_TOOL,
    CAPTURE_KIND_CLI_COMMAND,
    CAPTURE_KIND_STDIN_PIPE,
    CAPTURE_KIND_FILE_INGEST,
    CAPTURE_KIND_DERIVED_QUERY,
    CAPTURE_KIND_DERIVED_CODEGEN,
}

_FETCH_MAP_STATUS_SQL = """
SELECT map_status
FROM artifacts
WHERE workspace_id = %s
  AND artifact_id = %s
"""

_SOFT_DELETE_SQL = """
UPDATE artifacts
SET deleted_at = NOW(),
    generation = generation + 1
WHERE workspace_id = %s
  AND artifact_id = %s
  AND deleted_at IS NULL
"""

_TOOL_NAME = "artifact.capture"


def _error(
    code: str,
    message: str,
    *,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"code": code, "message": message}
    if details:
        payload["details"] = details
    return payload


def _extract_session_id(arguments: dict[str, Any]) -> str | None:
    context = arguments.get("_gateway_context")
    if not isinstance(context, Mapping):
        return None
    session_id = context.get("session_id")
    if isinstance(session_id, str) and session_id:
        return session_id
    return None


def _normalize_capture_origin(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, Mapping):
        return None
    return {str(key): value for key, value in raw.items()}


def _normalize_ttl_seconds(raw: Any) -> int | None:
    if raw is None:
        return None
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return raw if raw > 0 else None
    if isinstance(raw, str) and raw.isdigit():
        parsed = int(raw)
        return parsed if parsed > 0 else None
    return None


def _expires_at_from_ttl(ttl_seconds: int | None) -> dt.datetime | None:
    if ttl_seconds is None:
        return None
    return dt.datetime.now(dt.UTC) + dt.timedelta(seconds=ttl_seconds)


def _build_error_block(arguments: dict[str, Any]) -> ErrorBlock | None:
    status = arguments.get("status", "ok")
    if status != "error":
        return None

    raw_error = arguments.get("error")
    if isinstance(raw_error, Mapping):
        code = raw_error.get("code")
        message = raw_error.get("message")
        retryable = raw_error.get("retryable", False)
        upstream_trace_id = raw_error.get("upstream_trace_id")
        details = raw_error.get("details")
        if (
            isinstance(code, str)
            and code
            and isinstance(message, str)
            and message
        ):
            return ErrorBlock(
                code=code,
                message=message,
                retryable=bool(retryable),
                upstream_trace_id=(
                    str(upstream_trace_id)
                    if isinstance(upstream_trace_id, str)
                    else None
                ),
                details=details if isinstance(details, dict) else {},
            )
    return ErrorBlock(
        code="CAPTURE_ERROR",
        message="capture completed with error status",
    )


def execute_artifact_capture(
    runtime: ArtifactCaptureRuntime,
    *,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Persist one captured payload as an artifact."""
    if runtime.db_pool is None:
        return runtime.not_implemented(_TOOL_NAME)

    session_id = _extract_session_id(arguments)
    if session_id is None:
        return _error(
            "INVALID_ARGUMENT",
            "_gateway_context.session_id is required",
        )

    capture_kind = arguments.get("capture_kind")
    if capture_kind not in _VALID_CAPTURE_KINDS:
        return _error(
            "INVALID_ARGUMENT",
            f"invalid capture_kind: {capture_kind}",
        )

    capture_key = arguments.get("capture_key")
    request_key = arguments.get("request_key")
    if not isinstance(request_key, str) or not request_key:
        return _error("INVALID_ARGUMENT", "request_key is required")
    if not isinstance(capture_key, str) or not capture_key:
        capture_key = request_key

    capture_origin = _normalize_capture_origin(arguments.get("capture_origin"))
    if capture_origin is None:
        return _error("INVALID_ARGUMENT", "capture_origin must be an object")

    prefix = arguments.get("prefix")
    tool_name = arguments.get("tool_name")
    upstream_instance_id = arguments.get("upstream_instance_id")
    request_args_hash = arguments.get("request_args_hash")
    request_args_prefix = arguments.get("request_args_prefix")
    payload = arguments.get("payload")

    if not isinstance(prefix, str) or not prefix:
        return _error("INVALID_ARGUMENT", "prefix is required")
    if not isinstance(tool_name, str) or not tool_name:
        return _error("INVALID_ARGUMENT", "tool_name is required")
    if not isinstance(upstream_instance_id, str) or not upstream_instance_id:
        return _error("INVALID_ARGUMENT", "upstream_instance_id is required")
    if not isinstance(request_args_hash, str) or not request_args_hash:
        return _error("INVALID_ARGUMENT", "request_args_hash is required")
    if not isinstance(request_args_prefix, str) or not request_args_prefix:
        return _error("INVALID_ARGUMENT", "request_args_prefix is required")

    meta: dict[str, Any] = {}
    raw_meta = arguments.get("meta")
    if isinstance(raw_meta, Mapping):
        meta = {str(key): value for key, value in raw_meta.items()}
    tags = arguments.get("tags")
    if isinstance(tags, list):
        normalized_tags = [
            str(tag).strip()
            for tag in tags
            if isinstance(tag, str) and tag.strip()
        ]
        if normalized_tags:
            meta["tags"] = normalized_tags

    status_value = arguments.get("status", "ok")
    status: Literal["ok", "error"] = (
        "error" if status_value == "error" else "ok"
    )
    error_block = _build_error_block(arguments)

    ttl_seconds = _normalize_ttl_seconds(arguments.get("ttl_seconds"))
    expires_at = _expires_at_from_ttl(ttl_seconds)

    envelope = Envelope(
        upstream_instance_id=upstream_instance_id,
        upstream_prefix=prefix,
        tool=tool_name,
        status=status,
        content=[JsonContentPart(value=payload)],
        error=error_block,
        meta=meta,
    )

    with runtime.db_pool.connection() as connection:
        handle = persist_artifact(
            connection=connection,
            config=runtime.config,
            input_data=CreateArtifactInput(
                session_id=session_id,
                upstream_instance_id=upstream_instance_id,
                prefix=prefix,
                tool_name=tool_name,
                request_key=request_key,
                request_args_hash=request_args_hash,
                request_args_prefix=request_args_prefix,
                upstream_tool_schema_hash=None,
                envelope=envelope,
                capture_kind=capture_kind,
                capture_origin=capture_origin,
                capture_key=capture_key,
                expires_at=expires_at,
            ),
        )
        mapped = runtime.run_mapping_inline(
            connection,
            handle=handle,
            envelope=envelope,
        )
        if not mapped:
            connection.execute(
                _SOFT_DELETE_SQL,
                (WORKSPACE_ID, handle.artifact_id),
            )
            connection.commit()
            return _error(
                "CAPTURE_PERSISTENCE_FAILED",
                "capture mapping did not complete",
                details={
                    "stage": "mapping",
                    "artifact_id": handle.artifact_id,
                },
            )
        status_row = connection.execute(
            _FETCH_MAP_STATUS_SQL,
            (WORKSPACE_ID, handle.artifact_id),
        ).fetchone()
        map_status = (
            str(status_row[0])
            if status_row is not None and status_row[0] is not None
            else None
        )
        if map_status != "ready":
            connection.execute(
                _SOFT_DELETE_SQL,
                (WORKSPACE_ID, handle.artifact_id),
            )
            connection.commit()
            return _error(
                "CAPTURE_PERSISTENCE_FAILED",
                "capture mapping did not reach ready status",
                details={
                    "stage": "verify_ready",
                    "artifact_id": handle.artifact_id,
                    "map_status": map_status,
                },
            )

    return {
        "artifact_id": handle.artifact_id,
        "created_seq": handle.created_seq,
        "status": handle.status,
        "kind": handle.kind,
        "capture_kind": handle.capture_kind,
        "capture_key": handle.capture_key,
        "payload_json_bytes": handle.payload_json_bytes,
        "payload_binary_bytes_total": handle.payload_binary_bytes_total,
        "payload_total_bytes": handle.payload_total_bytes,
        "expires_at": (
            expires_at.isoformat().replace("+00:00", "Z")
            if expires_at is not None
            else None
        ),
        "reused": False,
    }


__all__ = [
    "execute_artifact_capture",
]
