"""Re-export database helpers without forcing Postgres dependencies.

The ``conn`` module depends on optional Postgres extras (``psycopg_pool``).
Import those symbols lazily so SQLite-only installs can import package modules
without requiring Postgres libraries.
"""

from __future__ import annotations

from typing import Any

from sidepouch_mcp.db.migrate import (
    apply_migrations,
    list_migrations,
    load_migrations,
)

__all__ = [
    "apply_migrations",
    "connect",
    "db_conn_info",
    "list_migrations",
    "load_migrations",
]


def __getattr__(name: str) -> Any:
    """Resolve Postgres connection helpers on first access."""
    if name in {"connect", "db_conn_info"}:
        from sidepouch_mcp.db.conn import connect, db_conn_info

        exports = {"connect": connect, "db_conn_info": db_conn_info}
        return exports[name]
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
