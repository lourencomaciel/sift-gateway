from __future__ import annotations

from mcp_artifact_gateway.query.where_hash import where_hash


def test_where_hash_raw_string_mode() -> None:
    assert where_hash("a = 1", mode="raw_string") == where_hash("a = 1", mode="raw_string")


def test_where_hash_canonical_ast_mode_is_order_insensitive() -> None:
    a = {"op": "and", "clauses": [{"path": "$.x", "op": "eq", "value": 1}]}
    b = {"clauses": [{"value": 1, "op": "eq", "path": "$.x"}], "op": "and"}
    assert where_hash(a, mode="canonical_ast") == where_hash(b, mode="canonical_ast")


def test_where_hash_canonical_ast_sorts_commutative_clauses() -> None:
    a = {
        "op": "and",
        "clauses": [
            {"path": "$.x", "op": "eq", "value": 1},
            {"path": "$.y", "op": "eq", "value": 2},
        ],
    }
    b = {
        "op": "and",
        "clauses": [
            {"path": "$.y", "op": "eq", "value": 2},
            {"path": "$.x", "op": "eq", "value": 1},
        ],
    }
    assert where_hash(a, mode="canonical_ast") == where_hash(b, mode="canonical_ast")


def test_where_hash_rejects_unknown_mode() -> None:
    try:
        where_hash("x", mode="invalid")
    except ValueError as exc:
        assert "unsupported" in str(exc)
    else:
        raise AssertionError("expected ValueError")
