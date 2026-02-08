"""Connection pool and query helpers for psycopg3 async.

Provides a thin convenience layer over ``psycopg`` and ``psycopg_pool`` so
that the rest of the codebase never instantiates pools or manages
transactions directly.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pool lifecycle
# ---------------------------------------------------------------------------


async def create_pool(
    dsn: str,
    min_size: int = 2,
    max_size: int = 10,
) -> AsyncConnectionPool:
    """Create and return an open ``AsyncConnectionPool``.

    The pool is configured so that every connection uses ``dict_row`` by
    default.  The caller is responsible for calling ``pool.close()`` (or
    using an ``async with`` block) when shutting down.
    """
    pool = AsyncConnectionPool(
        conninfo=dsn,
        min_size=min_size,
        max_size=max_size,
        kwargs={"row_factory": dict_row, "autocommit": False},
        open=False,
    )
    await pool.open()
    logger.info(
        "Connection pool opened (min=%d, max=%d, dsn=%s)",
        min_size,
        max_size,
        dsn,
    )
    return pool


# ---------------------------------------------------------------------------
# Context managers
# ---------------------------------------------------------------------------


@asynccontextmanager
async def get_conn(
    pool: AsyncConnectionPool,
) -> AsyncIterator[psycopg.AsyncConnection[dict[str, Any]]]:
    """Yield a connection from the pool.

    The connection is returned to the pool when the context exits.  No
    implicit transaction management is performed; callers may use
    ``conn.transaction()`` explicitly when needed.
    """
    async with pool.connection() as conn:
        yield conn


@asynccontextmanager
async def transaction(
    pool: AsyncConnectionPool,
) -> AsyncIterator[psycopg.AsyncConnection[dict[str, Any]]]:
    """Yield a connection wrapped in a transaction.

    The transaction is committed when the block exits cleanly and rolled
    back on exception.
    """
    async with pool.connection() as conn:
        async with conn.transaction():
            yield conn


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------


async def fetchone(
    conn: psycopg.AsyncConnection[dict[str, Any]],
    query: str | psycopg.sql.Composed,
    params: tuple[Any, ...] | None = None,
) -> dict[str, Any] | None:
    """Execute *query* and return the first row as a dict, or ``None``."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, params)
        return await cur.fetchone()


async def fetchall(
    conn: psycopg.AsyncConnection[dict[str, Any]],
    query: str | psycopg.sql.Composed,
    params: tuple[Any, ...] | None = None,
) -> list[dict[str, Any]]:
    """Execute *query* and return all rows as a list of dicts."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, params)
        return await cur.fetchall()


async def execute(
    conn: psycopg.AsyncConnection[dict[str, Any]],
    query: str | psycopg.sql.Composed,
    params: tuple[Any, ...] | None = None,
) -> int:
    """Execute *query* and return the number of affected rows.

    Returns 0 when the command does not report a rowcount (e.g. DDL).
    """
    async with conn.cursor() as cur:
        await cur.execute(query, params)
        return cur.rowcount if cur.rowcount and cur.rowcount >= 0 else 0
