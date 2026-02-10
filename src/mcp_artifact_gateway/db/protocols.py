"""Shared database protocols and helpers used across modules."""

from __future__ import annotations

from typing import Any, Protocol


class CursorLike(Protocol):
    """Minimal cursor protocol covering fetchone, fetchall, and rowcount."""

    rowcount: int

    def fetchone(self) -> tuple[object, ...] | None: ...

    def fetchall(self) -> list[tuple[object, ...]]: ...


class ConnectionLike(Protocol):
    """Minimal connection protocol for execute + commit."""

    def execute(self, query: str, params: tuple[object, ...] | None = None) -> CursorLike: ...

    def commit(self) -> None: ...


def safe_rollback(connection: object) -> None:
    """Defensively call rollback if the connection supports it."""
    rollback = getattr(connection, "rollback", None)
    if callable(rollback):
        rollback()


def increment_metric(metrics: Any | None, attr: str, amount: int = 1) -> None:
    """Increment a counter attribute on a metrics object, if present."""
    if metrics is None:
        return
    counter = getattr(metrics, attr, None)
    inc = getattr(counter, "inc", None)
    if callable(inc):
        inc(amount)
