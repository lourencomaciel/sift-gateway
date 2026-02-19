"""Shared lightweight validation helpers for artifact tool arguments."""

from __future__ import annotations

from typing import Any


def require_gateway_session(
    arguments: dict[str, Any],
) -> dict[str, Any] | None:
    """Ensure ``_gateway_context.session_id`` is present."""
    ctx = arguments.get("_gateway_context")
    if not isinstance(ctx, dict) or not ctx.get("session_id"):
        return {
            "code": "INVALID_ARGUMENT",
            "message": "missing _gateway_context.session_id",
        }
    return None


def require_artifact_id(
    arguments: dict[str, Any],
) -> dict[str, Any] | None:
    """Ensure ``artifact_id`` is present and non-empty."""
    if not arguments.get("artifact_id"):
        return {"code": "INVALID_ARGUMENT", "message": "missing artifact_id"}
    return None
