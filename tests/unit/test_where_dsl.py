from __future__ import annotations

from mcp_artifact_gateway.query.where_dsl import WhereComputeLimitExceeded, WhereDslError, evaluate_where


def test_where_dsl_eq_predicate() -> None:
    where = {"path": "$.status", "op": "eq", "value": "open"}
    assert evaluate_where({"status": "open"}, where) is True
    assert evaluate_where({"status": "closed"}, where) is False


def test_where_dsl_compound_and_not() -> None:
    where = {
        "op": "and",
        "clauses": [
            {"path": "$.n", "op": "gt", "value": 1},
            {"op": "not", "clause": {"path": "$.archived", "op": "eq", "value": True}},
        ],
    }
    assert evaluate_where({"n": 3, "archived": False}, where) is True
    assert evaluate_where({"n": 0, "archived": False}, where) is False


def test_where_dsl_compute_limit() -> None:
    where = {"op": "and", "clauses": [{"path": "$.a", "op": "exists"} for _ in range(5)]}
    try:
        evaluate_where({"a": 1}, where, max_compute_steps=2)
    except WhereComputeLimitExceeded:
        pass
    else:
        raise AssertionError("expected WhereComputeLimitExceeded")


def test_where_dsl_rejects_non_object_clause() -> None:
    where = {"op": "and", "clauses": [1]}
    try:
        evaluate_where({"a": 1}, where)
    except WhereDslError as exc:
        assert "clauses must contain objects" in str(exc)
    else:
        raise AssertionError("expected WhereDslError")


def test_where_dsl_rejects_non_comparable_values() -> None:
    where = {"path": "$.n", "op": "gt", "value": 1}
    try:
        evaluate_where({"n": "3"}, where)
    except WhereDslError as exc:
        assert "comparable values" in str(exc)
    else:
        raise AssertionError("expected WhereDslError")
