"""Build cursor payloads and enforce binding constraints.

Construct the canonical cursor payload dict with versioned
contract fields, timestamps, and position state.  Provide
``assert_cursor_binding`` to verify that a decoded cursor
still matches the expected tool, artifact, workspace, and
contract versions before use.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

from sift_mcp.constants import (
    CURSOR_VERSION,
    MAPPER_VERSION,
    TRAVERSAL_CONTRACT_VERSION,
    WORKSPACE_ID,
)


class CursorBindingError(ValueError):
    """Raised when cursor binding fields do not match the request context."""


class CursorStaleError(CursorBindingError):
    """Raised when a cursor reflects stale traversal or mapping context.

    Indicates the cursor's contract version, mapper version, or
    binding fields no longer match the current server state.
    """


_RESERVED_CURSOR_FIELDS = frozenset(
    {
        "cursor_version",
        "traversal_contract_version",
        "workspace_id",
        "artifact_id",
        "tool",
        "where_canonicalization_mode",
        "mapper_version",
        "position_state",
        "issued_at",
        "expires_at",
    }
)


def _utc_now() -> dt.datetime:
    """Return the current UTC time truncated to whole seconds.

    Returns:
        A timezone-aware UTC datetime with microsecond = 0.
    """
    return dt.datetime.now(dt.UTC).replace(microsecond=0)


def _iso_z(value: dt.datetime) -> str:
    """Format a datetime as ISO-8601 with trailing ``Z`` suffix.

    Args:
        value: Datetime to format (converted to UTC first).

    Returns:
        An ISO-8601 string ending with ``Z``.
    """
    return value.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")


def build_cursor_payload(
    *,
    tool: str,
    artifact_id: str,
    position_state: dict[str, Any],
    ttl_minutes: int,
    workspace_id: str = WORKSPACE_ID,
    where_canonicalization_mode: str = "raw_string",
    now: dt.datetime | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a versioned cursor payload dict for signing.

    Construct the canonical cursor payload with contract
    version fields, issued/expires timestamps, and position
    state.  Optional extra fields are merged after checking
    for reserved-key conflicts.

    Args:
        tool: Fully qualified tool name for cursor binding.
        artifact_id: Artifact this cursor is bound to.
        position_state: Opaque pagination state dict.
        ttl_minutes: Cursor lifetime in minutes.
        workspace_id: Workspace ID for binding verification.
        where_canonicalization_mode: Canonicalization mode
            for where-clause binding.
        now: Optional current time override for testing.
        extra: Additional non-reserved fields to include.

    Returns:
        A cursor payload dict ready for HMAC signing.

    Raises:
        ValueError: If extra contains reserved cursor fields.
    """
    issued_at = (now or _utc_now()).astimezone(dt.UTC)
    expires_at = issued_at + dt.timedelta(minutes=ttl_minutes)

    payload: dict[str, Any] = {
        "cursor_version": CURSOR_VERSION,
        "traversal_contract_version": TRAVERSAL_CONTRACT_VERSION,
        "workspace_id": workspace_id,
        "artifact_id": artifact_id,
        "tool": tool,
        "where_canonicalization_mode": where_canonicalization_mode,
        "mapper_version": MAPPER_VERSION,
        "position_state": position_state,
        "issued_at": _iso_z(issued_at),
        "expires_at": _iso_z(expires_at),
    }
    if extra:
        conflicts = sorted(
            key for key in extra if key in _RESERVED_CURSOR_FIELDS
        )
        if conflicts:
            msg = (
                f"extra contains reserved cursor fields: {', '.join(conflicts)}"
            )
            raise ValueError(msg)
        payload.update(extra)
    return payload


def assert_cursor_binding(
    payload: dict[str, Any],
    *,
    expected_tool: str,
    expected_artifact_id: str,
    expected_workspace_id: str = WORKSPACE_ID,
    expected_where_mode: str | None = None,
) -> None:
    """Verify that a decoded cursor matches the request context.

    Check tool, artifact_id, workspace_id, traversal contract
    version, mapper version, and optionally the where
    canonicalization mode.

    Args:
        payload: Decoded cursor payload dict.
        expected_tool: Tool name the cursor must be bound to.
        expected_artifact_id: Artifact the cursor must target.
        expected_workspace_id: Workspace the cursor must match.
        expected_where_mode: If set, required where
            canonicalization mode.

    Raises:
        CursorStaleError: If any binding field does not match.
    """
    if payload.get("tool") != expected_tool:
        msg = "cursor tool mismatch"
        raise CursorStaleError(msg)
    if payload.get("artifact_id") != expected_artifact_id:
        msg = "cursor artifact binding mismatch"
        raise CursorStaleError(msg)
    if payload.get("workspace_id") != expected_workspace_id:
        msg = "cursor workspace binding mismatch"
        raise CursorStaleError(msg)
    if payload.get("traversal_contract_version") != TRAVERSAL_CONTRACT_VERSION:
        msg = "cursor traversal_contract_version mismatch"
        raise CursorStaleError(msg)
    if payload.get("mapper_version") != MAPPER_VERSION:
        msg = "cursor mapper_version mismatch"
        raise CursorStaleError(msg)

    if (
        expected_where_mode is not None
        and payload.get("where_canonicalization_mode") != expected_where_mode
    ):
        msg = "cursor where_canonicalization_mode mismatch"
        raise CursorStaleError(msg)
