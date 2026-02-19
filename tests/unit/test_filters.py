"""Tests for structured filter compilation and hashing."""

from __future__ import annotations

import json
import sqlite3

import pytest

from sift_gateway.query.filters import (
    Filter,
    FilterGroup,
    FilterNot,
    compile_filter,
    filter_hash,
    parse_filter_dict,
)

# ── Validation ───────────────────────────────────────────────────


class TestFilterValidation:
    """Filter and FilterGroup reject invalid inputs."""

    def test_filter_rejects_unknown_op(self) -> None:
        with pytest.raises(ValueError, match="unsupported filter operator"):
            Filter(path="$.x", op="like")

    def test_filter_group_rejects_unknown_logic(self) -> None:
        with pytest.raises(ValueError, match="unsupported logic"):
            FilterGroup(logic="xor")

    def test_filter_accepts_all_valid_ops(self) -> None:
        for op in (
            "eq",
            "ne",
            "gt",
            "gte",
            "lt",
            "lte",
            "in",
            "contains",
            "array_contains",
            "exists",
            "not_exists",
        ):
            f = Filter(path="$.x", op=op, value=1)
            assert f.op == op


# ── SQL compilation ──────────────────────────────────────────────


class TestCompileComparison:
    """Comparison operators produce correct SQL."""

    @pytest.mark.parametrize(
        ("op", "sql_op"),
        [
            ("eq", "="),
            ("ne", "!="),
            ("gt", ">"),
            ("gte", ">="),
            ("lt", "<"),
            ("lte", "<="),
        ],
    )
    def test_comparison_ops(self, op: str, sql_op: str) -> None:
        sql, params = compile_filter(
            Filter(path="$.status", op=op, value="active")
        )
        assert sql == f"json_extract(record, ?) {sql_op} ?"
        assert params == ["$.status", "active"]

    def test_eq_numeric_value(self) -> None:
        _sql, params = compile_filter(Filter(path="$.count", op="eq", value=42))
        assert params == ["$.count", 42]

    def test_eq_bool_value_coerces_to_int(self) -> None:
        _, params = compile_filter(Filter(path="$.active", op="eq", value=True))
        assert params == ["$.active", 1]

    def test_eq_none_value(self) -> None:
        sql, params = compile_filter(Filter(path="$.x", op="eq", value=None))
        assert sql == "json_extract(record, ?) IS NULL"
        assert params == ["$.x"]

    def test_ne_none_value(self) -> None:
        sql, params = compile_filter(Filter(path="$.x", op="ne", value=None))
        assert sql == "json_extract(record, ?) IS NOT NULL"
        assert params == ["$.x"]


class TestCompileIn:
    """IN operator compiles to multi-placeholder SQL."""

    def test_in_list(self) -> None:
        sql, params = compile_filter(
            Filter(path="$.status", op="in", value=["a", "b", "c"])
        )
        assert sql == "json_extract(record, ?) IN (?, ?, ?)"
        assert params == ["$.status", "a", "b", "c"]

    def test_in_empty_list(self) -> None:
        sql, params = compile_filter(Filter(path="$.x", op="in", value=[]))
        assert sql == "0"
        assert params == []

    def test_in_rejects_non_list(self) -> None:
        with pytest.raises(ValueError, match="list"):
            compile_filter(Filter(path="$.x", op="in", value="not-a-list"))

    def test_in_single_element(self) -> None:
        sql, params = compile_filter(Filter(path="$.x", op="in", value=[99]))
        assert sql == "json_extract(record, ?) IN (?)"
        assert params == ["$.x", 99]


class TestCompileContains:
    """Contains and array_contains operators."""

    def test_contains_substring(self) -> None:
        sql, params = compile_filter(
            Filter(path="$.name", op="contains", value="alice")
        )
        assert "LIKE" in sql
        assert params == ["$.name", "alice"]

    def test_array_contains(self) -> None:
        sql, params = compile_filter(
            Filter(path="$.tags", op="array_contains", value="python")
        )
        assert "json_each" in sql
        assert "json_type" in sql
        assert params == ["$.tags", "$.tags", "python"]


class TestCompileExists:
    """Exists and not_exists operators."""

    def test_exists(self) -> None:
        sql, params = compile_filter(Filter(path="$.email", op="exists"))
        assert sql == "json_type(record, ?) IS NOT NULL"
        assert params == ["$.email"]

    def test_not_exists(self) -> None:
        sql, params = compile_filter(Filter(path="$.deleted", op="not_exists"))
        assert sql == "json_type(record, ?) IS NULL"
        assert params == ["$.deleted"]


# ── Filter groups ────────────────────────────────────────────────


class TestCompileFilterGroup:
    """FilterGroup AND/OR compilation."""

    def test_and_group(self) -> None:
        group = FilterGroup(
            logic="and",
            filters=[
                Filter(path="$.a", op="eq", value=1),
                Filter(path="$.b", op="gt", value=2),
            ],
        )
        sql, params = compile_filter(group)
        assert " AND " in sql
        assert params == ["$.a", 1, "$.b", 2]

    def test_or_group(self) -> None:
        group = FilterGroup(
            logic="or",
            filters=[
                Filter(path="$.x", op="eq", value="a"),
                Filter(path="$.x", op="eq", value="b"),
            ],
        )
        sql, params = compile_filter(group)
        assert " OR " in sql
        assert params == ["$.x", "a", "$.x", "b"]

    def test_empty_and_group(self) -> None:
        sql, params = compile_filter(FilterGroup(logic="and"))
        assert sql == "1"
        assert params == []

    def test_empty_or_group(self) -> None:
        sql, params = compile_filter(FilterGroup(logic="or"))
        assert sql == "0"
        assert params == []

    def test_nested_groups(self) -> None:
        nested = FilterGroup(
            logic="and",
            filters=[
                Filter(path="$.a", op="eq", value=1),
                FilterGroup(
                    logic="or",
                    filters=[
                        Filter(path="$.b", op="eq", value=2),
                        Filter(path="$.c", op="eq", value=3),
                    ],
                ),
            ],
        )
        sql, params = compile_filter(nested)
        assert " AND " in sql
        assert " OR " in sql
        assert params == ["$.a", 1, "$.b", 2, "$.c", 3]

    def test_single_child_group(self) -> None:
        group = FilterGroup(
            logic="and",
            filters=[Filter(path="$.x", op="eq", value=1)],
        )
        sql, params = compile_filter(group)
        assert sql == "(json_extract(record, ?) = ?)"
        assert params == ["$.x", 1]


# ── FilterNot compilation ────────────────────────────────────────


class TestCompileFilterNot:
    """FilterNot wraps child SQL with NOT(...)."""

    def test_not_simple_filter(self) -> None:
        sql, params = compile_filter(
            FilterNot(child=Filter(path="$.x", op="eq", value=1))
        )
        assert sql == "NOT (json_extract(record, ?) = ?)"
        assert params == ["$.x", 1]

    def test_not_in_filter(self) -> None:
        sql, params = compile_filter(
            FilterNot(
                child=Filter(
                    path="$.status",
                    op="in",
                    value=["a", "b"],
                )
            )
        )
        assert sql == "NOT (json_extract(record, ?) IN (?, ?))"
        assert params == ["$.status", "a", "b"]

    def test_not_group(self) -> None:
        sql, params = compile_filter(
            FilterNot(
                child=FilterGroup(
                    logic="and",
                    filters=[
                        Filter(path="$.a", op="eq", value=1),
                        Filter(path="$.b", op="gt", value=2),
                    ],
                )
            )
        )
        assert sql.startswith("NOT (")
        assert " AND " in sql
        assert params == ["$.a", 1, "$.b", 2]

    def test_double_not(self) -> None:
        sql, params = compile_filter(
            FilterNot(
                child=FilterNot(child=Filter(path="$.x", op="eq", value=1))
            )
        )
        assert sql == "NOT (NOT (json_extract(record, ?) = ?))"
        assert params == ["$.x", 1]

    def test_not_inside_group(self) -> None:
        group = FilterGroup(
            logic="and",
            filters=[
                Filter(path="$.a", op="eq", value=1),
                FilterNot(child=Filter(path="$.b", op="eq", value=2)),
            ],
        )
        sql, params = compile_filter(group)
        assert " AND " in sql
        assert "NOT (" in sql
        assert params == ["$.a", 1, "$.b", 2]


# ── Integration: execute against real SQLite ─────────────────────


class TestSQLiteExecution:
    """Compiled filters produce correct results in SQLite."""

    @pytest.fixture(autouse=True)
    def _setup_db(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute(
            "CREATE TABLE artifact_records ("
            "  workspace_id TEXT, artifact_id TEXT,"
            "  root_path TEXT, idx INTEGER,"
            "  record JSON)"
        )
        records = [
            {
                "id": 1,
                "name": "alice",
                "age": 30,
                "active": True,
                "tags": ["python", "sql"],
            },
            {
                "id": 2,
                "name": "bob",
                "age": 25,
                "active": False,
                "tags": ["go"],
            },
            {
                "id": 3,
                "name": "carol",
                "age": 35,
                "active": True,
                "tags": ["python", "rust"],
            },
        ]
        for i, rec in enumerate(records):
            self.conn.execute(
                "INSERT INTO artifact_records VALUES (?, ?, ?, ?, ?)",
                ("local", "art_1", "$", i, json.dumps(rec)),
            )

    def _query(
        self,
        f: Filter | FilterGroup | FilterNot,
    ) -> list[dict[str, object]]:
        sql, params = compile_filter(f)
        full_sql = (
            "SELECT record FROM artifact_records"
            " WHERE workspace_id = ? AND artifact_id = ?"
            " AND root_path = ?"
            f" AND ({sql})"
            " ORDER BY idx"
        )
        full_params = ["local", "art_1", "$", *params]
        rows = self.conn.execute(full_sql, full_params).fetchall()
        return [json.loads(row[0]) for row in rows]

    def test_eq_string(self) -> None:
        results = self._query(Filter(path="$.name", op="eq", value="bob"))
        assert len(results) == 1
        assert results[0]["id"] == 2

    def test_gt_numeric(self) -> None:
        results = self._query(Filter(path="$.age", op="gt", value=28))
        assert [r["id"] for r in results] == [1, 3]

    def test_in_list(self) -> None:
        results = self._query(
            Filter(path="$.name", op="in", value=["alice", "carol"])
        )
        assert [r["id"] for r in results] == [1, 3]

    def test_contains_substring(self) -> None:
        results = self._query(Filter(path="$.name", op="contains", value="ob"))
        assert len(results) == 1
        assert results[0]["name"] == "bob"

    def test_array_contains(self) -> None:
        results = self._query(
            Filter(path="$.tags", op="array_contains", value="python")
        )
        assert [r["id"] for r in results] == [1, 3]

    def test_exists(self) -> None:
        results = self._query(Filter(path="$.name", op="exists"))
        assert len(results) == 3

    def test_not_exists(self) -> None:
        results = self._query(Filter(path="$.email", op="not_exists"))
        assert len(results) == 3

    def test_and_group(self) -> None:
        results = self._query(
            FilterGroup(
                logic="and",
                filters=[
                    Filter(path="$.active", op="eq", value=1),
                    Filter(path="$.age", op="gte", value=35),
                ],
            )
        )
        assert [r["id"] for r in results] == [3]

    def test_or_group(self) -> None:
        results = self._query(
            FilterGroup(
                logic="or",
                filters=[
                    Filter(path="$.name", op="eq", value="alice"),
                    Filter(path="$.name", op="eq", value="bob"),
                ],
            )
        )
        assert [r["id"] for r in results] == [1, 2]

    def test_nested_and_or(self) -> None:
        results = self._query(
            FilterGroup(
                logic="and",
                filters=[
                    Filter(path="$.active", op="eq", value=1),
                    FilterGroup(
                        logic="or",
                        filters=[
                            Filter(
                                path="$.age",
                                op="lt",
                                value=31,
                            ),
                            Filter(
                                path="$.age",
                                op="gt",
                                value=34,
                            ),
                        ],
                    ),
                ],
            )
        )
        assert [r["id"] for r in results] == [1, 3]

    def test_not_eq(self) -> None:
        results = self._query(
            FilterNot(child=Filter(path="$.name", op="eq", value="bob"))
        )
        assert [r["id"] for r in results] == [1, 3]

    def test_not_in(self) -> None:
        results = self._query(
            FilterNot(
                child=Filter(
                    path="$.name",
                    op="in",
                    value=["alice", "carol"],
                )
            )
        )
        assert [r["id"] for r in results] == [2]

    def test_not_inside_and_group(self) -> None:
        results = self._query(
            FilterGroup(
                logic="and",
                filters=[
                    Filter(path="$.active", op="eq", value=1),
                    FilterNot(
                        child=Filter(
                            path="$.age",
                            op="gte",
                            value=35,
                        )
                    ),
                ],
            )
        )
        assert [r["id"] for r in results] == [1]


# ── Filter hashing ───────────────────────────────────────────────


class TestFilterHash:
    """filter_hash produces stable, canonical digests."""

    def test_hash_is_64_char_hex(self) -> None:
        h = filter_hash(Filter(path="$.x", op="eq", value=1))
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)

    def test_same_filter_same_hash(self) -> None:
        f1 = Filter(path="$.x", op="eq", value=1)
        f2 = Filter(path="$.x", op="eq", value=1)
        assert filter_hash(f1) == filter_hash(f2)

    def test_different_value_different_hash(self) -> None:
        f1 = Filter(path="$.x", op="eq", value=1)
        f2 = Filter(path="$.x", op="eq", value=2)
        assert filter_hash(f1) != filter_hash(f2)

    def test_different_op_different_hash(self) -> None:
        f1 = Filter(path="$.x", op="eq", value=1)
        f2 = Filter(path="$.x", op="ne", value=1)
        assert filter_hash(f1) != filter_hash(f2)

    def test_group_order_independent(self) -> None:
        g1 = FilterGroup(
            logic="and",
            filters=[
                Filter(path="$.a", op="eq", value=1),
                Filter(path="$.b", op="eq", value=2),
            ],
        )
        g2 = FilterGroup(
            logic="and",
            filters=[
                Filter(path="$.b", op="eq", value=2),
                Filter(path="$.a", op="eq", value=1),
            ],
        )
        assert filter_hash(g1) == filter_hash(g2)

    def test_float_values_hash_safely(self) -> None:
        f = Filter(path="$.x", op="eq", value=3.14)
        h = filter_hash(f)
        assert len(h) == 64

    def test_float_int_coercion(self) -> None:
        f1 = Filter(path="$.x", op="eq", value=5.0)
        f2 = Filter(path="$.x", op="eq", value=5)
        assert filter_hash(f1) == filter_hash(f2)

    def test_in_list_with_floats(self) -> None:
        f = Filter(path="$.x", op="in", value=[1.5, 2.0, 3])
        h = filter_hash(f)
        assert len(h) == 64

    def test_exists_omits_value(self) -> None:
        f1 = Filter(path="$.x", op="exists", value=None)
        f2 = Filter(path="$.x", op="exists", value=999)
        assert filter_hash(f1) == filter_hash(f2)

    def test_not_filter_hash(self) -> None:
        h = filter_hash(FilterNot(child=Filter(path="$.x", op="eq", value=1)))
        assert len(h) == 64

    def test_not_differs_from_plain(self) -> None:
        plain = Filter(path="$.x", op="eq", value=1)
        negated = FilterNot(child=plain)
        assert filter_hash(plain) != filter_hash(negated)

    def test_not_same_child_same_hash(self) -> None:
        n1 = FilterNot(child=Filter(path="$.x", op="eq", value=1))
        n2 = FilterNot(child=Filter(path="$.x", op="eq", value=1))
        assert filter_hash(n1) == filter_hash(n2)


# ── Dict parsing ─────────────────────────────────────────────────


class TestParseFilterDict:
    """parse_filter_dict converts raw dicts to Filter/FilterGroup."""

    def test_simple_filter(self) -> None:
        f = parse_filter_dict({"path": "$.x", "op": "eq", "value": 1})
        assert isinstance(f, Filter)
        assert f.path == "$.x"
        assert f.op == "eq"
        assert f.value == 1

    def test_filter_without_value(self) -> None:
        f = parse_filter_dict({"path": "$.x", "op": "exists"})
        assert isinstance(f, Filter)
        assert f.op == "exists"
        assert f.value is None

    def test_filter_group(self) -> None:
        g = parse_filter_dict(
            {
                "logic": "and",
                "filters": [
                    {"path": "$.a", "op": "eq", "value": 1},
                    {"path": "$.b", "op": "gt", "value": 2},
                ],
            }
        )
        assert isinstance(g, FilterGroup)
        assert g.logic == "and"
        assert len(g.filters) == 2

    def test_nested_groups(self) -> None:
        g = parse_filter_dict(
            {
                "logic": "or",
                "filters": [
                    {"path": "$.a", "op": "eq", "value": 1},
                    {
                        "logic": "and",
                        "filters": [
                            {"path": "$.b", "op": "gt", "value": 2},
                            {"path": "$.c", "op": "lt", "value": 3},
                        ],
                    },
                ],
            }
        )
        assert isinstance(g, FilterGroup)
        inner = g.filters[1]
        assert isinstance(inner, FilterGroup)
        assert inner.logic == "and"

    def test_missing_path_raises(self) -> None:
        with pytest.raises(ValueError, match="'path' and 'op'"):
            parse_filter_dict({"op": "eq", "value": 1})

    def test_missing_op_raises(self) -> None:
        with pytest.raises(ValueError, match="'path' and 'op'"):
            parse_filter_dict({"path": "$.x", "value": 1})

    def test_invalid_op_raises(self) -> None:
        with pytest.raises(ValueError, match="unsupported filter operator"):
            parse_filter_dict({"path": "$.x", "op": "like", "value": 1})

    def test_group_missing_filters_raises(self) -> None:
        with pytest.raises(ValueError, match="'filters' list"):
            parse_filter_dict({"logic": "and"})

    def test_round_trip_compiles(self) -> None:
        raw = {
            "logic": "and",
            "filters": [
                {"path": "$.name", "op": "eq", "value": "alice"},
                {"path": "$.age", "op": "gte", "value": 30},
            ],
        }
        f = parse_filter_dict(raw)
        sql, params = compile_filter(f)
        assert " AND " in sql
        assert params == ["$.name", "alice", "$.age", 30]

    def test_in_operator(self) -> None:
        f = parse_filter_dict({"path": "$.x", "op": "in", "value": [1, 2, 3]})
        sql, params = compile_filter(f)
        assert "IN" in sql
        assert params == ["$.x", 1, 2, 3]

    def test_not_filter(self) -> None:
        f = parse_filter_dict({"not": {"path": "$.x", "op": "eq", "value": 1}})
        assert isinstance(f, FilterNot)
        assert isinstance(f.child, Filter)
        assert f.child.op == "eq"

    def test_not_group(self) -> None:
        f = parse_filter_dict(
            {
                "not": {
                    "logic": "and",
                    "filters": [
                        {"path": "$.a", "op": "eq", "value": 1},
                        {"path": "$.b", "op": "gt", "value": 2},
                    ],
                }
            }
        )
        assert isinstance(f, FilterNot)
        assert isinstance(f.child, FilterGroup)

    def test_not_requires_dict_child(self) -> None:
        with pytest.raises(ValueError, match="dict child"):
            parse_filter_dict({"not": "bad"})

    def test_not_round_trip_compiles(self) -> None:
        raw = {"not": {"path": "$.x", "op": "in", "value": [1, 2]}}
        f = parse_filter_dict(raw)
        sql, params = compile_filter(f)
        assert sql == "NOT (json_extract(record, ?) IN (?, ?))"
        assert params == ["$.x", 1, 2]

    def test_nested_not(self) -> None:
        f = parse_filter_dict(
            {"not": {"not": {"path": "$.x", "op": "eq", "value": 1}}}
        )
        assert isinstance(f, FilterNot)
        assert isinstance(f.child, FilterNot)

    def test_not_inside_group(self) -> None:
        f = parse_filter_dict(
            {
                "logic": "and",
                "filters": [
                    {"path": "$.a", "op": "eq", "value": 1},
                    {"not": {"path": "$.b", "op": "eq", "value": 2}},
                ],
            }
        )
        assert isinstance(f, FilterGroup)
        assert isinstance(f.filters[1], FilterNot)

    def test_malformed_path_rejected(self) -> None:
        with pytest.raises(ValueError, match="invalid filter path"):
            parse_filter_dict({"path": "not a path", "op": "eq", "value": 1})

    def test_wildcard_path_rejected(self) -> None:
        with pytest.raises(ValueError, match="wildcard"):
            parse_filter_dict({"path": "$.items[*].id", "op": "eq", "value": 1})


# ── NULL with ordering operators ────────────────────────────────


class TestCompileNullOrdering:
    """Ordering operators reject None values."""

    @pytest.mark.parametrize("op", ["gt", "gte", "lt", "lte"])
    def test_ordering_op_rejects_none(self, op: str) -> None:
        with pytest.raises(ValueError, match="NULL value not supported"):
            compile_filter(Filter(path="$.x", op=op, value=None))


# ── array_contains on non-array values ──────────────────────────


class TestArrayContainsNonArray:
    """array_contains treats non-array fields as non-matches."""

    @pytest.fixture(autouse=True)
    def _setup_db(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute(
            "CREATE TABLE artifact_records ("
            "  workspace_id TEXT, artifact_id TEXT,"
            "  root_path TEXT, idx INTEGER,"
            "  record JSON)"
        )
        records = [
            {"id": 1, "tags": ["python", "sql"]},
            {"id": 2, "tags": "python"},
            {"id": 3, "tags": 42},
            {"id": 4, "tags": ["go", "rust"]},
        ]
        for i, rec in enumerate(records):
            self.conn.execute(
                "INSERT INTO artifact_records VALUES (?, ?, ?, ?, ?)",
                ("local", "art_1", "$", i, json.dumps(rec)),
            )

    def _query(
        self,
        f: Filter | FilterGroup | FilterNot,
    ) -> list[dict[str, object]]:
        sql, params = compile_filter(f)
        full_sql = (
            "SELECT record FROM artifact_records"
            " WHERE workspace_id = ? AND artifact_id = ?"
            " AND root_path = ?"
            f" AND ({sql})"
            " ORDER BY idx"
        )
        full_params = ["local", "art_1", "$", *params]
        rows = self.conn.execute(full_sql, full_params).fetchall()
        return [json.loads(row[0]) for row in rows]

    def test_matches_only_array_rows(self) -> None:
        results = self._query(
            Filter(
                path="$.tags",
                op="array_contains",
                value="python",
            )
        )
        assert [r["id"] for r in results] == [1]

    def test_no_error_on_scalar_string(self) -> None:
        results = self._query(
            Filter(
                path="$.tags",
                op="array_contains",
                value="missing",
            )
        )
        assert results == []

    def test_no_error_on_scalar_int(self) -> None:
        results = self._query(
            Filter(
                path="$.tags",
                op="array_contains",
                value=42,
            )
        )
        assert results == []
