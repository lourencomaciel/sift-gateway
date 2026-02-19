"""SQLite database backend.

Provides ``SqliteBackend`` as the production database backend.
SQL throughout the codebase uses Postgres-style syntax (``%s``
placeholders, ``NOW()``, ``= ANY()``) for historical reasons.
The ``_SqliteConnectionProxy`` transparently rewrites these at
execution time so callers do not need to know the dialect.
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
import sqlite3
from typing import Any


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


class _SqliteCursorProxy:
    """Wrap sqlite3.Cursor to support context manager and SQL adaptation."""

    def __init__(self, cursor: sqlite3.Cursor) -> None:
        self._cursor = cursor

    def __enter__(self) -> _SqliteCursorProxy:
        return self

    def __exit__(self, *exc: Any) -> None:
        self._cursor.close()

    def execute(self, sql: str, params: Any = None) -> _SqliteCursorProxy:
        """Execute SQL with Postgres-to-SQLite adaptation."""
        if params is not None and "ANY(" in sql.upper():
            sql, params = _SqliteConnectionProxy._expand_any(
                sql,
                params,
            )
        sql = _SqliteConnectionProxy._adapt(sql)
        params = _SqliteConnectionProxy._adapt_params(params)
        if params is not None:
            self._cursor.execute(sql, params)
        else:
            self._cursor.execute(sql)
        return self

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cursor, name)

    def __iter__(self) -> Any:
        return iter(self._cursor)


class _SqliteConnectionProxy:
    """Wrap a sqlite3.Connection to auto-rewrite Postgres SQL syntax.

    Transparently replaces ``%s`` with ``?``, ``NOW()`` with
    ``datetime('now')``, and strips ``FOR UPDATE SKIP LOCKED``
    so that Postgres-style SQL works on SQLite without callers
    needing to know the dialect.
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    @staticmethod
    def _expand_any(
        sql: str,
        params: tuple[object, ...] | list[object],
    ) -> tuple[str, tuple[object, ...]]:
        """Expand ``= ANY(%s)`` into ``IN (?, ?, ...)`` with flat params.

        Must be called before ``_adapt`` so that ``%s`` markers are
        still present for positional counting.

        Args:
            sql: SQL string with ``%s`` placeholders.
            params: Parameter tuple, where the ANY param is a list.

        Returns:
            Rewritten SQL and flattened parameter tuple.
        """
        import re

        match = re.search(r"=\s*ANY\(\s*%s\s*\)", sql)
        if not match:
            return sql, tuple(params)
        param_index = sql[: match.start()].count("%s")
        values = params[param_index]
        if not isinstance(values, (list, tuple)):
            return sql, tuple(params)
        placeholders = ", ".join("?" for _ in values)
        sql = sql[: match.start()] + f"IN ({placeholders})" + sql[match.end() :]
        flat: list[object] = (
            list(params[:param_index])
            + list(values)
            + list(params[param_index + 1 :])
        )
        return sql, tuple(flat)

    @staticmethod
    def _adapt(sql: str) -> str:
        import re

        sql = sql.replace("%s", "?")
        sql = re.sub(r"\bNOW\(\)", "datetime('now')", sql, flags=re.IGNORECASE)
        sql = re.sub(
            r"\s+FOR\s+UPDATE\s+SKIP\s+LOCKED",
            "",
            sql,
            flags=re.IGNORECASE,
        )
        # Strip Postgres type casts (e.g. ::text, ::text[], ::integer)
        return re.sub(r"::\w+(\[\])?", "", sql)

    @staticmethod
    def _adapt_params(params: Any) -> Any:
        """Convert Postgres-specific param types for SQLite."""
        if params is None:
            return None
        import json as _json

        adapted = []
        for p in params:
            # Unwrap Jsonb-like wrappers → JSON string
            if hasattr(p, "obj"):
                adapted.append(
                    _json.dumps(p.obj, ensure_ascii=False, sort_keys=True)
                )
            else:
                adapted.append(p)
        return tuple(adapted)

    def execute(
        self,
        sql: str,
        params: Any = None,
    ) -> _SqliteCursorProxy:
        """Execute SQL after adapting syntax for SQLite."""
        if params is not None and "ANY(" in sql.upper():
            sql, params = self._expand_any(sql, params)
        adapted = self._adapt(sql)
        params = self._adapt_params(params)
        if params is not None:
            cursor = self._conn.execute(adapted, params)
        else:
            cursor = self._conn.execute(adapted)
        return _SqliteCursorProxy(cursor)

    def cursor(self) -> _SqliteCursorProxy:
        """Return a wrapped cursor supporting context manager."""
        return _SqliteCursorProxy(self._conn.cursor())

    def commit(self) -> None:
        """Commit the current transaction."""
        self._conn.commit()

    def rollback(self) -> None:
        """Roll back the current transaction."""
        self._conn.rollback()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


class SqliteBackend:
    """SQLite backend with WAL mode.

    Uses a single persistent connection (SQLite serializes writes
    anyway).  WAL mode allows concurrent readers while a write is
    in progress.  JSON columns declared as ``JSON`` in the schema
    auto-convert between Python dicts/lists and TEXT via registered
    adapters/converters.
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

    @contextmanager
    def connection(self) -> Generator[Any, None, None]:
        """Yield a proxy around the persistent SQLite connection.

        The proxy auto-rewrites Postgres-style SQL (``%s``,
        ``NOW()``, ``FOR UPDATE SKIP LOCKED``) so that existing
        SQL strings work without modification.

        Yields:
            A ``_SqliteConnectionProxy`` wrapping the shared
            ``sqlite3.Connection``.

        Raises:
            RuntimeError: If the backend has been closed.
        """
        if self._conn is None:
            msg = "SqliteBackend is closed"
            raise RuntimeError(msg)
        yield _SqliteConnectionProxy(self._conn)

    def close(self) -> None:
        """Close the SQLite connection and release resources."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
