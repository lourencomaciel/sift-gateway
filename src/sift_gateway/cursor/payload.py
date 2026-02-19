"""Build cursor payloads with timestamps and position state.

Construct the canonical cursor payload dict with tool binding,
timestamps, and pagination position.  Handler-level binding
checks are performed inline by ``GatewayServer`` methods.
"""

from __future__ import annotations

import datetime as dt
from typing import Any


class CursorBindingError(ValueError):
    """Raised when cursor binding fields do not match."""


class CursorStaleError(CursorBindingError):
    """Raised when a cursor reflects stale query or mapping context.

    Indicates the cursor's binding fields no longer match
    the current request parameters or data state.
    """


_RESERVED_CURSOR_FIELDS = frozenset(
    {
        "artifact_id",
        "tool",
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
    now: dt.datetime | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a cursor payload dict for encoding.

    Construct the cursor payload with tool binding,
    issued/expires timestamps, and position state.
    Optional extra fields are merged after checking for
    reserved-key conflicts.

    Args:
        tool: Fully qualified tool name for cursor binding.
        artifact_id: Artifact this cursor is bound to.
        position_state: Opaque pagination state dict.
        ttl_minutes: Cursor lifetime in minutes.
        now: Optional current time override for testing.
        extra: Additional non-reserved fields to include.

    Returns:
        A cursor payload dict ready for encoding.

    Raises:
        ValueError: If extra contains reserved cursor fields.
    """
    issued_at = (now or _utc_now()).astimezone(dt.UTC)
    expires_at = issued_at + dt.timedelta(minutes=ttl_minutes)

    payload: dict[str, Any] = {
        "artifact_id": artifact_id,
        "tool": tool,
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
