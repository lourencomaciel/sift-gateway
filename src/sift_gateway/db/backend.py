"""SQLite database backend.

Provides ``SqliteBackend`` as the production database backend.
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
import sqlite3
from threading import RLock


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
        self._lock = RLock()
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
        self._conn.execute("PRAGMA synchronous = NORMAL")
        self._conn.execute(f"PRAGMA busy_timeout = {self._busy_timeout_ms}")
        self._conn.execute("PRAGMA foreign_keys = ON")

    @contextmanager
    def connection(self) -> Generator[sqlite3.Connection, None, None]:
        """Yield the locked persistent SQLite connection.

        Yields:
            The shared ``sqlite3.Connection``.

        Raises:
            RuntimeError: If the backend has been closed.
        """
        if self._conn is None:
            msg = "SqliteBackend is closed"
            raise RuntimeError(msg)
        with self._lock:
            yield self._conn

    def close(self) -> None:
        """Close the SQLite connection and release resources."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
