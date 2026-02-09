"""Database backend abstraction for Postgres and SQLite."""
from __future__ import annotations

from contextlib import contextmanager
from enum import Enum
from typing import Any, Generator, Protocol, runtime_checkable


class Dialect(Enum):
    """SQL dialect identifier with key syntax differences."""

    POSTGRES = ("POSTGRES", "%s", "NOW()")
    SQLITE = ("SQLITE", "?", "datetime('now')")

    def __init__(self, dialect_name: str, param_marker: str, now_sql: str) -> None:
        self._dialect_name = dialect_name
        self._param_marker = param_marker
        self._now_sql = now_sql

    @property
    def param_marker(self) -> str:
        return self._param_marker

    @property
    def now_sql(self) -> str:
        return self._now_sql


@runtime_checkable
class DatabaseBackend(Protocol):
    """Protocol for database backends (Postgres, SQLite, etc.)."""

    @property
    def dialect(self) -> Dialect: ...

    def connection(self) -> Any:
        """Return a context manager that yields a ConnectionLike."""
        ...

    def close(self) -> None: ...


class PostgresBackend:
    """Wraps psycopg_pool.ConnectionPool behind the DatabaseBackend protocol."""

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    @property
    def dialect(self) -> Dialect:
        return Dialect.POSTGRES

    @contextmanager
    def connection(self) -> Generator[Any, None, None]:
        with self._pool.connection() as conn:
            yield conn

    def close(self) -> None:
        self._pool.close()
