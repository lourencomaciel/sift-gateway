"""Bounded response shape for retrieval tools (Addendum F)."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class BoundedResponse(BaseModel):
    """Standard bounded response container for retrieval operations."""

    model_config = {"extra": "forbid"}

    items: list[Any] = Field(default_factory=list)
    truncated: bool = False
    cursor: str | None = None
    omitted: dict[str, Any] = Field(default_factory=dict)
    stats: dict[str, Any] = Field(default_factory=dict)


def make_response(
    *,
    items: list[Any],
    truncated: bool,
    cursor: str | None = None,
    omitted: dict[str, Any] | None = None,
    stats: dict[str, Any] | None = None,
) -> BoundedResponse:
    """Helper to build a :class:`BoundedResponse` with defaults."""
    return BoundedResponse(
        items=items,
        truncated=truncated,
        cursor=cursor,
        omitted=omitted or {},
        stats=stats or {},
    )
