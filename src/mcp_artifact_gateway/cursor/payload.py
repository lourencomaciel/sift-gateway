"""Cursor payload building and binding checks."""

from __future__ import annotations

import datetime as dt
from typing import Any

from mcp_artifact_gateway.constants import (
    CURSOR_VERSION,
    MAPPER_VERSION,
    TRAVERSAL_CONTRACT_VERSION,
    WORKSPACE_ID,
)


class CursorBindingError(ValueError):
    """Raised when cursor binding fields do not match request context."""


class CursorStaleError(CursorBindingError):
    """Raised when cursor reflects stale traversal/mapping context."""


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
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0)


def _iso_z(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")


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
    issued_at = (now or _utc_now()).astimezone(dt.timezone.utc)
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
        conflicts = sorted(key for key in extra if key in _RESERVED_CURSOR_FIELDS)
        if conflicts:
            msg = f"extra contains reserved cursor fields: {', '.join(conflicts)}"
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

    if expected_where_mode is not None and payload.get("where_canonicalization_mode") != expected_where_mode:
        msg = "cursor where_canonicalization_mode mismatch"
        raise CursorStaleError(msg)
