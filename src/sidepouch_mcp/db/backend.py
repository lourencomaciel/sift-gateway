"""Abstract database backends for Postgres and SQLite.

Defines the ``Dialect`` enum for SQL syntax differences and the
``DatabaseBackend`` protocol that concrete backends implement.
Provides ``PostgresBackend`` (psycopg connection pool wrapper)
and ``SqliteBackend`` (single-connection WAL-mode SQLite) as
the two production implementations.
"""

from __future__ import annotations

from contextlib import contextmanager
from enum import Enum
from pathlib import Path
import sqlite3
from typing import Any, Generator, Protocol, runtime_checkable


class Dialect(Enum):
    """SQL dialect identifier with key syntax differences.

    Each member bundles the parameter placeholder style and the
    ``NOW()`` expression appropriate for that database engine.

    Attributes:
        POSTGRES: PostgreSQL dialect using ``%s`` params and
            ``NOW()`` timestamps.
        SQLITE: SQLite dialect using ``?`` params and
            ``datetime('now')`` timestamps.
    """

    POSTGRES = ("POSTGRES", "%s", "NOW()")
    SQLITE = ("SQLITE", "?", "datetime('now')")

    def __init__(
        self, dialect_name: str, param_marker: str, now_sql: str
    ) -> None:
        """Initialize dialect enum member.

        Args:
            dialect_name: Human-readable dialect name.
            param_marker: SQL parameter placeholder string.
            now_sql: SQL expression for current timestamp.
        """
        self._dialect_name = dialect_name
        self._param_marker = param_marker
        self._now_sql = now_sql

    @property
    def param_marker(self) -> str:
        """The SQL parameter placeholder (e.g. ``%s`` or ``?``)."""
        return self._param_marker

    @property
    def now_sql(self) -> str:
        """The SQL expression for the current timestamp."""
        return self._now_sql


@runtime_checkable
class DatabaseBackend(Protocol):
    """Protocol for database backends (Postgres, SQLite, etc.).

    Concrete implementations must expose a ``dialect`` property,
    a ``connection()`` context manager yielding a DB-API-like
    connection, and a ``close()`` method for resource cleanup.
    """

    @property
    def dialect(self) -> Dialect:
        """The SQL dialect for this backend."""
        ...

    def connection(self) -> Any:
        """Return a context manager that yields a ConnectionLike."""
        ...

    def close(self) -> None:
        """Release backend resources."""
        ...


class PostgresBackend:
    """Wrap a psycopg connection pool as a DatabaseBackend.

    Delegates connection lifecycle to the underlying
    ``psycopg_pool.ConnectionPool`` and always reports
    ``Dialect.POSTGRES``.
    """

    def __init__(self, pool: Any) -> None:
        """Initialize with a psycopg connection pool.

        Args:
            pool: A ``psycopg_pool.ConnectionPool`` instance.
        """
        self._pool = pool

    @property
    def dialect(self) -> Dialect:
        """The SQL dialect for this backend."""
        return Dialect.POSTGRES

    @contextmanager
    def connection(self) -> Generator[Any, None, None]:
        """Yield a connection from the pool.

        Yields:
            A psycopg connection checked out from the pool.
        """
        with self._pool.connection() as conn:
            yield conn

    def close(self) -> None:
        """Close the underlying connection pool."""
        self._pool.close()


def _register_json_types() -> None:
    """Register JSON adapter/converter for dict/list round-trips."""
    import json as _json

    sqlite3.register_adapter(
        dict,
        lambda d: _json.dumps(d, ensure_ascii=False, sort_keys=True),
    )
    sqlite3.register_adapter(
        list,
        lambda lst: _json.dumps(lst, ensure_ascii=False, sort_keys=True),
    )
    sqlite3.register_converter("JSON", _json.loads)


class SqliteBackend:
    """SQLite backend with WAL mode for the DatabaseBackend protocol.

    Uses a single persistent connection (SQLite serializes writes anyway).
    WAL mode allows concurrent readers while a write is in progress.
    JSON columns declared as ``JSON`` in the schema auto-convert between
    Python dicts/lists and TEXT via registered adapters/converters.
    """

    def __init__(
        self,
        db_path: Path,
        *,
        busy_timeout_ms: int = 5000,
    ) -> None:
        """Initialize SQLite backend and open connection.

        Args:
            db_path: Filesystem path to the SQLite database file.
            busy_timeout_ms: Milliseconds to wait for a locked
                database before raising an error.
        """
        self._db_path = db_path
        self._busy_timeout_ms = busy_timeout_ms
        self._conn: sqlite3.Connection | None = None
        self._init_connection()

    def _init_connection(self) -> None:
        """Open the SQLite connection and configure pragmas."""
        _register_json_types()
        self._conn = sqlite3.connect(
            str(self._db_path),
            detect_types=sqlite3.PARSE_DECLTYPES,
            check_same_thread=False,
        )
        self._conn.execute("PRAGMA journal_mode = WAL")
        self._conn.execute(f"PRAGMA busy_timeout = {self._busy_timeout_ms}")
        self._conn.execute("PRAGMA foreign_keys = ON")

    @property
    def dialect(self) -> Dialect:
        """The SQL dialect for this backend."""
        return Dialect.SQLITE

    @contextmanager
    def connection(self) -> Generator[sqlite3.Connection, None, None]:
        """Yield the single persistent SQLite connection.

        Yields:
            The shared ``sqlite3.Connection``.

        Raises:
            RuntimeError: If the backend has been closed.
        """
        if self._conn is None:
            msg = "SqliteBackend is closed"
            raise RuntimeError(msg)
        yield self._conn

    def close(self) -> None:
        """Close the SQLite connection and release resources."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
