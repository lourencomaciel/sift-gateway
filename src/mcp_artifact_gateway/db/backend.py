"""Database backend abstraction for Postgres and SQLite."""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from enum import Enum
from pathlib import Path
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
        self._db_path = db_path
        self._busy_timeout_ms = busy_timeout_ms
        self._conn: sqlite3.Connection | None = None
        self._init_connection()

    @staticmethod
    def _register_json_types() -> None:
        """Register JSON adapter/converter so JSON columns round-trip as dicts/lists."""
        import json as _json

        sqlite3.register_adapter(dict, lambda d: _json.dumps(d, ensure_ascii=False, sort_keys=True))
        sqlite3.register_adapter(list, lambda lst: _json.dumps(lst, ensure_ascii=False, sort_keys=True))
        sqlite3.register_converter("JSON", lambda b: _json.loads(b))

    def _init_connection(self) -> None:
        self._register_json_types()
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
        return Dialect.SQLITE

    @contextmanager
    def connection(self) -> Generator[sqlite3.Connection, None, None]:
        if self._conn is None:
            msg = "SqliteBackend is closed"
            raise RuntimeError(msg)
        yield self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
