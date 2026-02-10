"""Tests for SQL dialect adaptation helpers."""

from __future__ import annotations

import json

import pytest

from mcp_artifact_gateway.db.backend import Dialect
from mcp_artifact_gateway.db.dialect import (
    adapt_params,
    expand_any_clause,
    rewrite_now,
    rewrite_param_markers,
    strip_skip_locked,
    wrap_json,
)


class TestRewriteParamMarkers:
    def test_postgres_unchanged(self):
        sql = "SELECT * FROM t WHERE id = %s AND name = %s"
        assert rewrite_param_markers(sql, Dialect.POSTGRES) == sql

    def test_sqlite_replaces_percent_s(self):
        sql = "SELECT * FROM t WHERE id = %s AND name = %s"
        assert rewrite_param_markers(sql, Dialect.SQLITE) == (
            "SELECT * FROM t WHERE id = ? AND name = ?"
        )

    def test_does_not_replace_percent_in_strings(self):
        sql = "SELECT * FROM t WHERE id = %s"
        result = rewrite_param_markers(sql, Dialect.SQLITE)
        assert result == "SELECT * FROM t WHERE id = ?"


class TestRewriteNow:
    def test_postgres_unchanged(self):
        sql = "INSERT INTO t (created_at) VALUES (NOW())"
        assert rewrite_now(sql, Dialect.POSTGRES) == sql

    def test_sqlite_replaces_now(self):
        sql = "INSERT INTO t (created_at) VALUES (NOW())"
        assert rewrite_now(sql, Dialect.SQLITE) == (
            "INSERT INTO t (created_at) VALUES (datetime('now'))"
        )

    def test_multiple_now_calls(self):
        sql = "SET created_at = NOW(), updated_at = NOW()"
        assert rewrite_now(sql, Dialect.SQLITE) == (
            "SET created_at = datetime('now'), updated_at = datetime('now')"
        )


class TestStripSkipLocked:
    def test_strips_for_update_skip_locked(self):
        sql = """
        SELECT id FROM t
        WHERE status = %s
        FOR UPDATE SKIP LOCKED
        """
        result = strip_skip_locked(sql)
        assert "FOR UPDATE SKIP LOCKED" not in result
        assert "SELECT id FROM t" in result

    def test_no_op_without_clause(self):
        sql = "SELECT id FROM t WHERE status = %s"
        assert strip_skip_locked(sql) == sql


class TestExpandAnyClause:
    def test_single_value(self):
        sql, params = expand_any_clause("WHERE id = ANY(%s)", (["abc"],), any_param_index=0)
        assert sql == "WHERE id IN (?)"
        assert params == ("abc",)

    def test_multiple_values(self):
        sql, params = expand_any_clause("WHERE id = ANY(%s)", (["a", "b", "c"],), any_param_index=0)
        assert sql == "WHERE id IN (?, ?, ?)"
        assert params == ("a", "b", "c")

    def test_preserves_other_params(self):
        sql, params = expand_any_clause(
            "WHERE ws = %s AND id = ANY(%s)",
            ("local", ["a", "b"]),
            any_param_index=1,
        )
        assert sql == "WHERE ws = ? AND id IN (?, ?)"
        assert params == ("local", "a", "b")

    def test_empty_list_raises(self):
        sql, params = expand_any_clause("WHERE id = ANY(%s)", ([],), any_param_index=0)
        assert sql == "WHERE id IN ()"
        assert params == ()

    def test_non_list_raises(self):
        with pytest.raises(TypeError, match="must be a list"):
            expand_any_clause("WHERE id = ANY(%s)", ("not_a_list",), any_param_index=0)


class TestAdaptParams:
    """adapt_params is the all-in-one helper for simple queries."""

    def test_postgres_passthrough(self):
        sql = "SELECT * FROM t WHERE id = %s AND ts > NOW()"
        out_sql, out_params = adapt_params(sql, ("%s",), Dialect.POSTGRES)
        assert out_sql == sql
        assert out_params == ("%s",)

    def test_sqlite_full_transform(self):
        sql = "SELECT * FROM t WHERE id = %s AND ts > NOW()"
        out_sql, out_params = adapt_params(sql, ("val",), Dialect.SQLITE)
        assert out_sql == "SELECT * FROM t WHERE id = ? AND ts > datetime('now')"
        assert out_params == ("val",)


class TestWrapJson:
    def test_postgres_returns_jsonb(self):
        from psycopg.types.json import Jsonb

        data = {"key": "value"}
        result = wrap_json(data, Dialect.POSTGRES)
        assert isinstance(result, Jsonb)

    def test_sqlite_returns_json_string(self):
        data = {"key": "value"}
        result = wrap_json(data, Dialect.SQLITE)
        assert isinstance(result, str)
        assert json.loads(result) == data

    def test_none_returns_none(self):
        assert wrap_json(None, Dialect.POSTGRES) is None
        assert wrap_json(None, Dialect.SQLITE) is None
