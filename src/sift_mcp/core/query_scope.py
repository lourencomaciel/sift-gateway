"""Shared helpers for scope and cursor-position resolution."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from sift_mcp.envelope.responses import gateway_error

_QUERY_SCOPES = {"all_related", "single"}


def resolve_scope(
    *,
    raw_scope: Any,
    cursor_payload: Mapping[str, Any] | None = None,
) -> tuple[str, dict[str, Any] | None]:
    """Resolve query scope from arguments and optional cursor payload."""
    scope: str | None = None
    if raw_scope is not None:
        if not isinstance(raw_scope, str) or raw_scope not in _QUERY_SCOPES:
            return "", gateway_error(
                "INVALID_ARGUMENT",
                "scope must be one of: all_related, single",
            )
        scope = raw_scope
    if scope is None and cursor_payload is not None:
        cursor_scope = cursor_payload.get("scope")
        if cursor_scope in _QUERY_SCOPES:
            scope = str(cursor_scope)
        elif isinstance(cursor_payload.get("artifact_generation"), int):
            # Backward compatibility for pre-scope cursors.
            scope = "single"
    if scope is None:
        scope = "all_related"
    return scope, None


def resolve_cursor_offset(
    position: Mapping[str, Any],
) -> tuple[int, dict[str, Any] | None]:
    """Extract and validate an integer non-negative cursor offset."""
    raw_offset = position.get("offset", 0)
    if not isinstance(raw_offset, int) or raw_offset < 0:
        return 0, gateway_error("INVALID_ARGUMENT", "invalid cursor offset")
    return raw_offset, None

