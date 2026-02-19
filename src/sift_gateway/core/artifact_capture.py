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


def _normalize_parent_artifact_id(raw: Any) -> str | None:
    """Normalize optional parent artifact ID."""
    if raw is None:
        return None
    if isinstance(raw, str) and raw:
        return raw
    return None


def _normalize_chain_seq(raw: Any) -> int | None:
    """Normalize optional non-negative chain sequence value."""
    if raw is None or isinstance(raw, bool):
        return None
    if isinstance(raw, int) and raw >= 0:
        return raw
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


def _resolve_capture_keys(
    arguments: dict[str, Any],
) -> tuple[str | None, str | None, dict[str, Any] | None]:
    """Resolve request/capture keys with request_key fallback behavior."""
    request_key = arguments.get("request_key")
    if not isinstance(request_key, str) or not request_key:
        return None, None, _error("INVALID_ARGUMENT", "request_key is required")
    capture_key = arguments.get("capture_key")
    if not isinstance(capture_key, str) or not capture_key:
        capture_key = request_key
    return request_key, capture_key, None


def _require_non_empty_capture_field(
    arguments: dict[str, Any],
    *,
    key: str,
    message: str,
) -> tuple[str | None, dict[str, Any] | None]:
    """Validate required non-empty string capture argument."""
    value = arguments.get(key)
    if not isinstance(value, str) or not value:
        return None, _error("INVALID_ARGUMENT", message)
    return value, None


def _normalize_meta(arguments: dict[str, Any]) -> dict[str, Any]:
    """Normalize capture meta and optional tags into envelope meta."""
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
    return meta


def _resolve_parent_chain(
    arguments: dict[str, Any],
) -> tuple[str | None, int | None, dict[str, Any] | None]:
    """Normalize and validate optional parent/chain lineage fields."""
    parent_artifact_id = _normalize_parent_artifact_id(
        arguments.get("parent_artifact_id")
    )
    if (
        arguments.get("parent_artifact_id") is not None
        and parent_artifact_id is None
    ):
        return (
            None,
            None,
            _error(
                "INVALID_ARGUMENT",
                "parent_artifact_id must be a non-empty string when provided",
            ),
        )
    chain_seq = _normalize_chain_seq(arguments.get("chain_seq"))
    if arguments.get("chain_seq") is not None and chain_seq is None:
        return (
            None,
            None,
            _error(
                "INVALID_ARGUMENT",
                "chain_seq must be a non-negative integer when provided",
            ),
        )
    if chain_seq is not None and parent_artifact_id is None:
        return (
            None,
            None,
            _error(
                "INVALID_ARGUMENT",
                "chain_seq requires parent_artifact_id",
            ),
        )
    return parent_artifact_id, chain_seq, None


def _mapping_failure_payload(
    connection: Any,
    *,
    artifact_id: str,
    message: str,
    details: dict[str, Any],
) -> dict[str, Any]:
    """Soft-delete failed capture artifact and return persistence error."""
    connection.execute(
        _SOFT_DELETE_SQL,
        (WORKSPACE_ID, artifact_id),
    )
    connection.commit()
    return _error(
        "CAPTURE_PERSISTENCE_FAILED",
        message,
        details=details,
    )


def _resolve_session_id(
    arguments: dict[str, Any],
) -> tuple[str | None, dict[str, Any] | None]:
    """Resolve required gateway session_id from request context."""
    session_id = _extract_session_id(arguments)
    if session_id is None:
        return (
            None,
            _error(
                "INVALID_ARGUMENT",
                "_gateway_context.session_id is required",
            ),
        )
    return session_id, None


def _resolve_capture_kind(
    arguments: dict[str, Any],
) -> tuple[str | None, dict[str, Any] | None]:
    """Resolve and validate capture kind."""
    capture_kind = arguments.get("capture_kind")
    if capture_kind not in _VALID_CAPTURE_KINDS:
        return (
            None,
            _error(
                "INVALID_ARGUMENT",
                f"invalid capture_kind: {capture_kind}",
            ),
        )
    return str(capture_kind), None


def _resolve_capture_origin(
    arguments: dict[str, Any],
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Resolve and validate capture origin object."""
    capture_origin = _normalize_capture_origin(arguments.get("capture_origin"))
    if capture_origin is None:
        return None, _error("INVALID_ARGUMENT", "capture_origin must be an object")
    return capture_origin, None


def _resolve_required_capture_fields(
    arguments: dict[str, Any],
) -> tuple[dict[str, str] | None, dict[str, Any] | None]:
    """Resolve required non-empty string fields for capture persistence."""
    required: tuple[tuple[str, str], ...] = (
        ("prefix", "prefix is required"),
        ("tool_name", "tool_name is required"),
        ("upstream_instance_id", "upstream_instance_id is required"),
        ("request_args_hash", "request_args_hash is required"),
        ("request_args_prefix", "request_args_prefix is required"),
    )
    values: dict[str, str] = {}
    for key, message in required:
        value, error = _require_non_empty_capture_field(
            arguments,
            key=key,
            message=message,
        )
        if error is not None:
            return None, error
        assert value is not None
        values[key] = value
    return values, None


def _load_map_status(
    connection: Any,
    *,
    artifact_id: str,
) -> str | None:
    """Load current map_status for one artifact."""
    status_row = connection.execute(
        _FETCH_MAP_STATUS_SQL,
        (WORKSPACE_ID, artifact_id),
    ).fetchone()
    if status_row is None or status_row[0] is None:
        return None
    return str(status_row[0])


def _persist_capture_handle(
    runtime: ArtifactCaptureRuntime,
    *,
    session_id: str,
    request_key: str,
    capture_kind: str,
    capture_origin: dict[str, Any],
    capture_key: str,
    prefix: str,
    tool_name: str,
    upstream_instance_id: str,
    request_args_hash: str,
    request_args_prefix: str,
    envelope: Envelope,
    expires_at: dt.datetime | None,
    parent_artifact_id: str | None,
    chain_seq: int | None,
) -> tuple[Any | None, dict[str, Any] | None]:
    """Persist capture and verify mapping readiness."""
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
                parent_artifact_id=parent_artifact_id,
                chain_seq=chain_seq,
            ),
        )
        mapped = runtime.run_mapping_inline(
            connection,
            handle=handle,
            envelope=envelope,
        )
        if not mapped:
            return (
                None,
                _mapping_failure_payload(
                    connection,
                    artifact_id=handle.artifact_id,
                    message="capture mapping did not complete",
                    details={
                        "stage": "mapping",
                        "artifact_id": handle.artifact_id,
                    },
                ),
            )

        map_status = _load_map_status(
            connection,
            artifact_id=handle.artifact_id,
        )
        if map_status != "ready":
            return (
                None,
                _mapping_failure_payload(
                    connection,
                    artifact_id=handle.artifact_id,
                    message="capture mapping did not reach ready status",
                    details={
                        "stage": "verify_ready",
                        "artifact_id": handle.artifact_id,
                        "map_status": map_status,
                    },
                ),
            )
    return handle, None


def _capture_success_payload(
    *,
    handle: Any,
    expires_at: dt.datetime | None,
) -> dict[str, Any]:
    """Build artifact.capture success payload."""
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


def execute_artifact_capture(
    runtime: ArtifactCaptureRuntime,
    *,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Persist one captured payload as an artifact."""
    if runtime.db_pool is None:
        return runtime.not_implemented(_TOOL_NAME)

    session_id, session_error = _resolve_session_id(arguments)
    if session_error is not None:
        return session_error
    capture_kind, kind_error = _resolve_capture_kind(arguments)
    if kind_error is not None:
        return kind_error
    assert session_id is not None
    assert capture_kind is not None

    request_key, capture_key, key_error = _resolve_capture_keys(arguments)
    if key_error is not None:
        return key_error
    assert request_key is not None
    assert capture_key is not None

    capture_origin, origin_error = _resolve_capture_origin(arguments)
    if origin_error is not None:
        return origin_error
    required_fields, required_error = _resolve_required_capture_fields(arguments)
    if required_error is not None:
        return required_error
    assert capture_origin is not None
    assert required_fields is not None

    payload = arguments.get("payload")
    prefix = required_fields["prefix"]
    tool_name = required_fields["tool_name"]
    upstream_instance_id = required_fields["upstream_instance_id"]
    request_args_hash = required_fields["request_args_hash"]
    request_args_prefix = required_fields["request_args_prefix"]

    meta = _normalize_meta(arguments)

    status_value = arguments.get("status", "ok")
    status: Literal["ok", "error"] = (
        "error" if status_value == "error" else "ok"
    )
    error_block = _build_error_block(arguments)

    ttl_seconds = _normalize_ttl_seconds(arguments.get("ttl_seconds"))
    expires_at = _expires_at_from_ttl(ttl_seconds)
    parent_artifact_id, chain_seq, lineage_error = _resolve_parent_chain(
        arguments
    )
    if lineage_error is not None:
        return lineage_error

    envelope = Envelope(
        upstream_instance_id=upstream_instance_id,
        upstream_prefix=prefix,
        tool=tool_name,
        status=status,
        content=[JsonContentPart(value=payload)],
        error=error_block,
        meta=meta,
    )

    handle, persist_error = _persist_capture_handle(
        runtime,
        session_id=session_id,
        request_key=request_key,
        capture_kind=capture_kind,
        capture_origin=capture_origin,
        capture_key=capture_key,
        prefix=prefix,
        tool_name=tool_name,
        upstream_instance_id=upstream_instance_id,
        request_args_hash=request_args_hash,
        request_args_prefix=request_args_prefix,
        envelope=envelope,
        expires_at=expires_at,
        parent_artifact_id=parent_artifact_id,
        chain_seq=chain_seq,
    )
    if persist_error is not None:
        return persist_error
    assert handle is not None

    return _capture_success_payload(handle=handle, expires_at=expires_at)


__all__ = [
    "execute_artifact_capture",
]
