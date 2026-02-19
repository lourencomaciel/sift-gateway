"""Protocol-neutral capture identity helpers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from sift_mcp.constants import (
    CAPTURE_KIND_CLI_COMMAND,
    CAPTURE_KIND_DERIVED_CODEGEN,
    CAPTURE_KIND_DERIVED_QUERY,
    CAPTURE_KIND_FILE_INGEST,
    CAPTURE_KIND_MCP_TOOL,
    CAPTURE_KIND_STDIN_PIPE,
    KIND_DERIVED_CODEGEN,
    KIND_DERIVED_QUERY,
)

_CAPTURE_KINDS: frozenset[str] = frozenset(
    {
        CAPTURE_KIND_MCP_TOOL,
        CAPTURE_KIND_CLI_COMMAND,
        CAPTURE_KIND_STDIN_PIPE,
        CAPTURE_KIND_FILE_INGEST,
        CAPTURE_KIND_DERIVED_QUERY,
        CAPTURE_KIND_DERIVED_CODEGEN,
    }
)

_DEFAULT_CAPTURE_KIND_BY_ARTIFACT_KIND: dict[str, str] = {
    KIND_DERIVED_QUERY: CAPTURE_KIND_DERIVED_QUERY,
    KIND_DERIVED_CODEGEN: CAPTURE_KIND_DERIVED_CODEGEN,
}


@dataclass(frozen=True)
class CaptureIdentity:
    """Normalized protocol-neutral capture identity fields."""

    capture_kind: str
    capture_origin: dict[str, Any]
    capture_key: str


def default_capture_kind_for_artifact(kind: str) -> str:
    """Return the default capture kind for an artifact kind."""
    return _DEFAULT_CAPTURE_KIND_BY_ARTIFACT_KIND.get(kind, CAPTURE_KIND_MCP_TOOL)


def build_capture_identity(
    *,
    artifact_kind: str,
    request_key: str,
    prefix: str,
    tool_name: str,
    upstream_instance_id: str,
    capture_kind: str | None = None,
    capture_origin: Mapping[str, Any] | None = None,
    capture_key: str | None = None,
) -> CaptureIdentity:
    """Build neutral capture identity with defaults and validation."""
    resolved_kind = (
        capture_kind if isinstance(capture_kind, str) and capture_kind else default_capture_kind_for_artifact(artifact_kind)
    )
    if resolved_kind not in _CAPTURE_KINDS:
        msg = f"invalid capture_kind: {resolved_kind}"
        raise ValueError(msg)

    resolved_key = capture_key if isinstance(capture_key, str) and capture_key else request_key
    if not isinstance(resolved_key, str) or not resolved_key:
        msg = "capture_key must be a non-empty string"
        raise ValueError(msg)

    if capture_origin is None:
        resolved_origin: dict[str, Any] = {
            "prefix": prefix,
            "tool": tool_name,
            "upstream_instance_id": upstream_instance_id,
        }
    elif isinstance(capture_origin, Mapping):
        resolved_origin = dict(capture_origin)
    else:
        msg = "capture_origin must be an object"
        raise ValueError(msg)

    return CaptureIdentity(
        capture_kind=resolved_kind,
        capture_origin=resolved_origin,
        capture_key=resolved_key,
    )


__all__ = [
    "CaptureIdentity",
    "build_capture_identity",
    "default_capture_kind_for_artifact",
]
