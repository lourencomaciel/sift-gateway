"""Shared database protocols and helpers used across modules."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol


class CursorLike(Protocol):
    """Minimal cursor protocol covering fetchone, fetchall, and rowcount."""

    rowcount: int

    def fetchone(self) -> tuple[object, ...] | None:
        """Fetch the next row or None."""
        ...

    def fetchall(self) -> list[tuple[object, ...]]:
        """Fetch all remaining rows."""
        ...


class ConnectionLike(Protocol):
    """Minimal connection protocol for execute + commit."""

    def execute(
        self, query: str, params: tuple[object, ...] | None = None
    ) -> CursorLike:
        """Execute a SQL query and return a cursor."""
        ...

    def commit(self) -> None:
        """Commit the current transaction."""
        ...


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


def sqlite_in_clause(
    trusted_column_sql: str,
    values: Sequence[object],
) -> tuple[str, tuple[object, ...]]:
    """Build a parameterized SQLite ``IN`` predicate for trusted SQL columns."""
    if not values:
        return "0", ()
    placeholders = ", ".join("?" for _ in values)
    return f"{trusted_column_sql} IN ({placeholders})", tuple(values)
