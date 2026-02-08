"""Simple SQL migration framework for MCP Artifact Gateway.

Reads migration files from the ``db/migrations/`` package directory, tracks
applied migrations in a ``schema_migrations`` table, and applies pending
ones inside a transaction.
"""

from __future__ import annotations

import logging
from importlib import resources as importlib_resources
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_MIGRATIONS_PACKAGE = "mcp_artifact_gateway.db.migrations"

_CREATE_TRACKING_TABLE = """\
CREATE TABLE IF NOT EXISTS schema_migrations (
    filename   text        NOT NULL PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);
"""

_APPLIED_QUERY = """\
SELECT filename FROM schema_migrations ORDER BY filename;
"""

_INSERT_APPLIED = """\
INSERT INTO schema_migrations (filename) VALUES (%s);
"""


def _discover_migration_files() -> list[tuple[str, str]]:
    """Return ``(filename, sql_text)`` pairs sorted by filename.

    Migration files are ``.sql`` files located in the
    ``mcp_artifact_gateway.db.migrations`` package directory.
    """
    results: list[tuple[str, str]] = []
    migration_files = importlib_resources.files(_MIGRATIONS_PACKAGE)
    for entry in sorted(migration_files.iterdir(), key=lambda e: e.name):
        if hasattr(entry, "name") and entry.name.endswith(".sql"):
            sql_text = entry.read_text(encoding="utf-8")
            results.append((entry.name, sql_text))
    return results


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def run_migrations(conn: psycopg.AsyncConnection[dict]) -> list[str]:
    """Apply all pending SQL migrations and return the list of newly applied filenames.

    Parameters
    ----------
    conn:
        An open ``psycopg.AsyncConnection``.  The caller is responsible for
        providing a connection that is *not* already inside a transaction
        block managed externally (autocommit or freshly opened).

    Returns
    -------
    list[str]
        Filenames that were applied during this invocation.
    """
    # Ensure the tracking table exists (idempotent).
    await conn.execute(_CREATE_TRACKING_TABLE)
    await conn.commit()

    # Discover what is on disk.
    all_migrations = _discover_migration_files()
    if not all_migrations:
        logger.info("No migration files found.")
        return []

    # Determine what has already been applied.
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(_APPLIED_QUERY)
        rows = await cur.fetchall()
    applied: set[str] = {row["filename"] for row in rows}

    pending = [(name, sql) for name, sql in all_migrations if name not in applied]
    if not pending:
        logger.info("All %d migrations already applied.", len(applied))
        return []

    newly_applied: list[str] = []
    for filename, sql_text in pending:
        logger.info("Applying migration: %s", filename)
        async with conn.transaction():
            await conn.execute(sql_text)
            await conn.execute(_INSERT_APPLIED, (filename,))
        newly_applied.append(filename)
        logger.info("Migration applied: %s", filename)

    return newly_applied


async def check_migrations(conn: psycopg.AsyncConnection[dict]) -> None:
    """Verify that every discovered migration file has been applied.

    Raises
    ------
    RuntimeError
        If any migration file has not been recorded in ``schema_migrations``.
    """
    # Check whether the tracking table itself exists.
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT to_regclass('public.schema_migrations') AS tbl;"
        )
        row = await cur.fetchone()
    if row is None or row["tbl"] is None:
        raise RuntimeError(
            "schema_migrations table does not exist. Run migrations first."
        )

    all_migrations = _discover_migration_files()
    if not all_migrations:
        return

    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(_APPLIED_QUERY)
        rows = await cur.fetchall()
    applied: set[str] = {row["filename"] for row in rows}

    missing = [name for name, _ in all_migrations if name not in applied]
    if missing:
        raise RuntimeError(
            f"Unapplied migrations detected: {', '.join(missing)}. "
            "Run migrations before starting the gateway."
        )
