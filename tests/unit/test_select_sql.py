"""Tests for select_sql query building."""

from __future__ import annotations

import json
import sqlite3

import pytest

from sift_gateway.query.select_sql import compile_select

# ── compile_select ───────────────────────────────────────────────


class TestCompileSelect:
    """compile_select produces correct SQL expressions."""

    def test_empty_paths_returns_record(self) -> None:
        sql, params = compile_select([])
        assert sql == "record"
        assert params == []

    def test_single_path(self) -> None:
        sql, params = compile_select(["$.name"])
        assert "json_object" in sql
        assert "json_extract(record, ?)" in sql
        assert params == ["$.name", "$.name"]

    def test_multiple_paths(self) -> None:
        sql, params = compile_select(["$.name", "$.age"])
        assert sql.count("json_extract") == 2
        assert params == ["$.name", "$.name", "$.age", "$.age"]

    def test_wildcard_path_rejected(self) -> None:
        with pytest.raises(ValueError, match="wildcard"):
            compile_select(["$.items[*].id"])

    def test_wildcard_among_valid_paths_rejected(self) -> None:
        with pytest.raises(ValueError, match="wildcard"):
            compile_select(["$.name", "$.items[*].id"])

    def test_malformed_path_rejected(self) -> None:
        with pytest.raises(ValueError, match="invalid SQL projection path"):
            compile_select(["not a path"])


# ── Integration: execute against real SQLite ─────────────────────


class TestSQLiteSelectIntegration:
    """Compiled queries run correctly against real SQLite."""

    def setup_method(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute(
            "CREATE TABLE artifact_records ("
            "  workspace_id TEXT, artifact_id TEXT,"
            "  root_path TEXT, idx INTEGER,"
            "  record JSON)"
        )
        records = [
            {"id": 1, "name": "alice", "age": 30},
            {"id": 2, "name": "bob", "age": 25},
            {"id": 3, "name": "carol", "age": 35},
        ]
        for i, rec in enumerate(records):
            self.conn.execute(
                "INSERT INTO artifact_records VALUES (?, ?, ?, ?, ?)",
                ("local", "art_1", "$", i, json.dumps(rec)),
            )

    def test_select_projection(self) -> None:
        select_expr, select_params = compile_select(["$.name", "$.age"])
        full_sql = (
            f"SELECT {select_expr} FROM artifact_records"
            " WHERE workspace_id = ? AND artifact_id = ?"
            " AND root_path = ?"
            " ORDER BY idx ASC"
        )
        rows = self.conn.execute(
            full_sql,
            (*select_params, "local", "art_1", "$"),
        ).fetchall()
        assert len(rows) == 3
        first = json.loads(rows[0][0])
        assert first == {"$.name": "alice", "$.age": 30}
