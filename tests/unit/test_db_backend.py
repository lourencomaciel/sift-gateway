"""Tests for DatabaseBackend protocol and implementations."""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from mcp_artifact_gateway.db.backend import (
    DatabaseBackend,
    Dialect,
    PostgresBackend,
)


class TestDialect:
    def test_postgres_dialect_values(self):
        assert Dialect.POSTGRES.param_marker == "%s"
        assert Dialect.POSTGRES.now_sql == "NOW()"
        assert Dialect.POSTGRES.name == "POSTGRES"

    def test_sqlite_dialect_values(self):
        assert Dialect.SQLITE.param_marker == "?"
        assert Dialect.SQLITE.now_sql == "datetime('now')"
        assert Dialect.SQLITE.name == "SQLITE"


class TestPostgresBackend:
    def test_satisfies_protocol(self):
        pool = MagicMock()
        backend = PostgresBackend(pool=pool)
        assert isinstance(backend, DatabaseBackend)

    def test_dialect_is_postgres(self):
        pool = MagicMock()
        backend = PostgresBackend(pool=pool)
        assert backend.dialect is Dialect.POSTGRES

    def test_connection_delegates_to_pool(self):
        pool = MagicMock()
        backend = PostgresBackend(pool=pool)
        with backend.connection() as conn:
            pass
        pool.connection.assert_called_once()

    def test_close_delegates_to_pool(self):
        pool = MagicMock()
        backend = PostgresBackend(pool=pool)
        backend.close()
        pool.close.assert_called_once()


class TestPostgresBackendConnectionInterface:
    """Verify PostgresBackend.connection() context manager matches pool.connection() interface."""

    def test_connection_context_yields_same_object(self):
        mock_conn = MagicMock()
        pool = MagicMock()
        pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        backend = PostgresBackend(pool=pool)
        with backend.connection() as conn:
            assert conn is mock_conn
