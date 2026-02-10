"""Smoke test: SQLite backend core paths."""

from __future__ import annotations

from pathlib import Path

import pytest

from sidepouch_mcp.cache.reuse import try_acquire_advisory_lock
from sidepouch_mcp.db.backend import SqliteBackend
from sidepouch_mcp.db.migrate import apply_migrations


@pytest.fixture()
def sqlite_backend(tmp_path: Path) -> SqliteBackend:
    db_path = tmp_path / "gateway.db"
    backend = SqliteBackend(db_path=db_path)
    migrations_dir = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "sidepouch_mcp"
        / "db"
        / "migrations_sqlite"
    )
    with backend.connection() as conn:
        apply_migrations(conn, migrations_dir, param_marker="?")
    yield backend
    backend.close()


class TestSqliteSmoke:
    def test_tables_created(self, sqlite_backend: SqliteBackend) -> None:
        with sqlite_backend.connection() as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
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
            assert expected.issubset(tables)

    def test_insert_and_query_session(
        self, sqlite_backend: SqliteBackend
    ) -> None:
        with sqlite_backend.connection() as conn:
            conn.execute(
                "INSERT INTO sessions (workspace_id, session_id) VALUES (?, ?)",
                ("local", "sess-1"),
            )
            conn.commit()
            row = conn.execute(
                "SELECT session_id FROM sessions WHERE workspace_id = ?",
                ("local",),
            ).fetchone()
            assert row[0] == "sess-1"

    def test_upsert_session(self, sqlite_backend: SqliteBackend) -> None:
        with sqlite_backend.connection() as conn:
            conn.execute(
                """INSERT INTO sessions (workspace_id, session_id)
                   VALUES (?, ?)
                   ON CONFLICT (workspace_id, session_id)
                   DO UPDATE SET last_seen_at = datetime('now')""",
                ("local", "sess-1"),
            )
            conn.execute(
                """INSERT INTO sessions (workspace_id, session_id)
                   VALUES (?, ?)
                   ON CONFLICT (workspace_id, session_id)
                   DO UPDATE SET last_seen_at = datetime('now')""",
                ("local", "sess-1"),
            )
            conn.commit()
            count = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
            assert count == 1

    def test_foreign_key_enforcement(
        self, sqlite_backend: SqliteBackend
    ) -> None:
        """Inserting artifact without session should fail due to FK constraint."""
        with sqlite_backend.connection() as conn:
            with pytest.raises(Exception):
                conn.execute(
                    """INSERT INTO artifacts (
                        workspace_id, artifact_id, created_seq, session_id,
                        source_tool, upstream_instance_id, request_key,
                        payload_hash_full, canonicalizer_version,
                        payload_json_bytes, payload_binary_bytes_total,
                        payload_total_bytes, mapper_version
                    ) VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        "local",
                        "art-1",
                        "nonexistent-session",
                        "tool",
                        "upstream-1",
                        "rk-1",
                        "ph-1",
                        "cv-1",
                        100,
                        0,
                        100,
                        "v1",
                    ),
                )

    def test_advisory_lock_noop_on_sqlite(
        self, sqlite_backend: SqliteBackend
    ) -> None:
        """Advisory lock should always return True on SQLite."""
        with sqlite_backend.connection() as conn:
            result = try_acquire_advisory_lock(conn, request_key="test-key")
            assert result is True

    def test_json_column_roundtrip(self, sqlite_backend: SqliteBackend) -> None:
        """JSON columns auto-convert between Python dicts and TEXT."""
        with sqlite_backend.connection() as conn:
            conn.execute(
                "INSERT INTO sessions (workspace_id, session_id) VALUES (?, ?)",
                ("local", "sess-json"),
            )
            conn.execute(
                """INSERT INTO payload_blobs (
                    workspace_id, payload_hash_full, envelope,
                    envelope_canonical_encoding, envelope_canonical_bytes,
                    envelope_canonical_bytes_len, canonicalizer_version,
                    payload_json_bytes, payload_binary_bytes_total,
                    payload_total_bytes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    "local",
                    "ph-json",
                    {"key": "value", "nested": [1, 2]},
                    "none",
                    b"data",
                    4,
                    "v1",
                    4,
                    0,
                    4,
                ),
            )
            conn.commit()
            row = conn.execute(
                "SELECT envelope FROM payload_blobs WHERE payload_hash_full = ?",
                ("ph-json",),
            ).fetchone()
            assert row[0] == {"key": "value", "nested": [1, 2]}
            assert isinstance(row[0], dict)

    def test_created_seq_auto_increment(
        self, sqlite_backend: SqliteBackend
    ) -> None:
        """Verify the trigger auto-generates created_seq for artifacts."""
        with sqlite_backend.connection() as conn:
            conn.execute(
                "INSERT INTO sessions (workspace_id, session_id) VALUES (?, ?)",
                ("local", "sess-seq"),
            )
            conn.execute(
                """INSERT INTO payload_blobs (
                    workspace_id, payload_hash_full,
                    envelope_canonical_encoding, envelope_canonical_bytes,
                    envelope_canonical_bytes_len, canonicalizer_version,
                    payload_json_bytes, payload_binary_bytes_total,
                    payload_total_bytes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                ("local", "ph-seq", "none", b"data", 4, "v1", 4, 0, 4),
            )
            for i in range(3):
                conn.execute(
                    """INSERT INTO artifacts (
                        workspace_id, artifact_id, session_id, source_tool,
                        upstream_instance_id, request_key, payload_hash_full,
                        canonicalizer_version, payload_json_bytes,
                        payload_binary_bytes_total, payload_total_bytes,
                        mapper_version
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        "local",
                        f"art-{i}",
                        "sess-seq",
                        "tool",
                        "up1",
                        f"rk-{i}",
                        "ph-seq",
                        "v1",
                        4,
                        0,
                        4,
                        "v1",
                    ),
                )
            conn.commit()
            rows = conn.execute(
                "SELECT artifact_id, created_seq FROM artifacts ORDER BY created_seq"
            ).fetchall()
            seqs = [row[1] for row in rows]
            assert len(seqs) == 3
            assert seqs == sorted(seqs)
            assert all(s > 0 for s in seqs)
            assert len(set(seqs)) == 3  # all unique
