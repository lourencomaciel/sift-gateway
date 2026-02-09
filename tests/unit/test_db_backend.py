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
    SqliteBackend,
)
from mcp_artifact_gateway.db.migrate import apply_migrations, load_migrations


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


class TestSqliteBackend:
    @pytest.fixture()
    def backend(self, tmp_path: Path) -> SqliteBackend:
        db = SqliteBackend(db_path=tmp_path / "test.db")
        yield db
        db.close()

    def test_satisfies_protocol(self, backend: SqliteBackend):
        assert isinstance(backend, DatabaseBackend)

    def test_dialect_is_sqlite(self, backend: SqliteBackend):
        assert backend.dialect is Dialect.SQLITE

    def test_wal_mode_enabled(self, backend: SqliteBackend):
        with backend.connection() as conn:
            mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
            assert mode == "wal"

    def test_foreign_keys_enabled(self, backend: SqliteBackend):
        with backend.connection() as conn:
            fk = conn.execute("PRAGMA foreign_keys").fetchone()[0]
            assert fk == 1

    def test_busy_timeout_set(self, tmp_path: Path):
        backend = SqliteBackend(db_path=tmp_path / "test.db", busy_timeout_ms=3000)
        try:
            with backend.connection() as conn:
                timeout = conn.execute("PRAGMA busy_timeout").fetchone()[0]
                assert timeout == 3000
        finally:
            backend.close()

    def test_execute_and_fetch(self, backend: SqliteBackend):
        with backend.connection() as conn:
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
            conn.execute("INSERT INTO t (id, name) VALUES (?, ?)", (1, "alice"))
            row = conn.execute("SELECT name FROM t WHERE id = ?", (1,)).fetchone()
            assert row[0] == "alice"

    def test_json_roundtrip_dict(self, backend: SqliteBackend):
        with backend.connection() as conn:
            conn.execute("CREATE TABLE j (id INTEGER PRIMARY KEY, data JSON)")
            original = {"key": "value", "nested": {"a": 1}}
            conn.execute("INSERT INTO j (id, data) VALUES (?, ?)", (1, original))
            row = conn.execute("SELECT data FROM j WHERE id = ?", (1,)).fetchone()
            assert row[0] == original
            assert isinstance(row[0], dict)

    def test_json_roundtrip_list(self, backend: SqliteBackend):
        with backend.connection() as conn:
            conn.execute("CREATE TABLE j2 (id INTEGER PRIMARY KEY, data JSON)")
            original = [1, "two", {"three": 3}]
            conn.execute("INSERT INTO j2 (id, data) VALUES (?, ?)", (1, original))
            row = conn.execute("SELECT data FROM j2 WHERE id = ?", (1,)).fetchone()
            assert row[0] == original
            assert isinstance(row[0], list)

    def test_json_null_roundtrip(self, backend: SqliteBackend):
        with backend.connection() as conn:
            conn.execute("CREATE TABLE j3 (id INTEGER PRIMARY KEY, data JSON)")
            conn.execute("INSERT INTO j3 (id, data) VALUES (?, ?)", (1, None))
            row = conn.execute("SELECT data FROM j3 WHERE id = ?", (1,)).fetchone()
            assert row[0] is None

    def test_close_then_connection_raises(self, tmp_path: Path):
        backend = SqliteBackend(db_path=tmp_path / "test.db")
        backend.close()
        with pytest.raises(RuntimeError, match="closed"):
            with backend.connection():
                pass

    def test_close_idempotent(self, tmp_path: Path):
        backend = SqliteBackend(db_path=tmp_path / "test.db")
        backend.close()
        backend.close()  # should not raise


class TestSqliteMigrationFiles:
    """Verify SQLite migration files are present and loadable."""

    def test_migrations_dir_exists(self):
        migrations_dir = (
            Path(__file__).resolve().parents[2]
            / "src"
            / "mcp_artifact_gateway"
            / "db"
            / "migrations_sqlite"
        )
        assert migrations_dir.is_dir()

    def test_migrations_loadable(self):
        migrations_dir = (
            Path(__file__).resolve().parents[2]
            / "src"
            / "mcp_artifact_gateway"
            / "db"
            / "migrations_sqlite"
        )
        migrations = load_migrations(migrations_dir)
        assert len(migrations) >= 2
        assert migrations[0].name == "001_init.sql"
        assert migrations[1].name == "002_indexes.sql"


class TestSqliteMigrationIntegration:
    """Apply SQLite migrations to an in-memory database and verify tables."""

    @pytest.fixture()
    def migrated_conn(self, tmp_path: Path) -> sqlite3.Connection:
        backend = SqliteBackend(db_path=tmp_path / "mig.db")
        migrations_dir = (
            Path(__file__).resolve().parents[2]
            / "src"
            / "mcp_artifact_gateway"
            / "db"
            / "migrations_sqlite"
        )
        with backend.connection() as conn:
            applied = apply_migrations(conn, migrations_dir, param_marker="?")
            assert len(applied) >= 2
            yield conn
        backend.close()

    def test_tables_created(self, migrated_conn: sqlite3.Connection):
        rows = migrated_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        table_names = {row[0] for row in rows}
        expected = {
            "schema_migrations",
            "sessions",
            "binary_blobs",
            "payload_blobs",
            "payload_hash_aliases",
            "payload_binary_refs",
            "artifacts",
            "artifact_refs",
            "artifact_roots",
            "artifact_samples",
            "_created_seq_counter",
        }
        assert expected.issubset(table_names)

    def test_migrations_idempotent(self, migrated_conn: sqlite3.Connection):
        """Re-applying migrations should be a no-op."""
        migrations_dir = (
            Path(__file__).resolve().parents[2]
            / "src"
            / "mcp_artifact_gateway"
            / "db"
            / "migrations_sqlite"
        )
        applied_again = apply_migrations(migrated_conn, migrations_dir, param_marker="?")
        assert applied_again == []

    def test_schema_migrations_recorded(self, migrated_conn: sqlite3.Connection):
        rows = migrated_conn.execute(
            "SELECT migration_name FROM schema_migrations ORDER BY migration_name"
        ).fetchall()
        names = [row[0] for row in rows]
        assert "001_init.sql" in names
        assert "002_indexes.sql" in names

    def test_created_seq_trigger(self, migrated_conn: sqlite3.Connection):
        """Verify the auto-increment trigger on artifacts works."""
        # Need sessions and payload_blobs rows first (foreign keys)
        migrated_conn.execute(
            "INSERT INTO sessions (workspace_id, session_id) VALUES (?, ?)",
            ("test", "s1"),
        )
        migrated_conn.execute(
            """INSERT INTO payload_blobs (
                workspace_id, payload_hash_full, envelope_canonical_encoding,
                envelope_canonical_bytes, envelope_canonical_bytes_len,
                canonicalizer_version, payload_json_bytes,
                payload_binary_bytes_total, payload_total_bytes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("test", "ph1", "none", b"data", 4, "v1", 4, 0, 4),
        )
        migrated_conn.execute(
            """INSERT INTO artifacts (
                workspace_id, artifact_id, session_id, source_tool,
                upstream_instance_id, request_key, payload_hash_full,
                canonicalizer_version, payload_json_bytes,
                payload_binary_bytes_total, payload_total_bytes,
                mapper_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("test", "a1", "s1", "tool", "up1", "rk1", "ph1", "v1", 4, 0, 4, "v1"),
        )
        row = migrated_conn.execute(
            "SELECT created_seq FROM artifacts WHERE artifact_id = ?", ("a1",)
        ).fetchone()
        assert row[0] > 0
