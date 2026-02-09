from __future__ import annotations

import pytest

from mcp_artifact_gateway.query.where_hash import where_hash


# ---------------------------------------------------------------------------
# raw_string mode
# ---------------------------------------------------------------------------


def test_where_hash_raw_string_mode() -> None:
    assert where_hash("a = 1", mode="raw_string") == where_hash("a = 1", mode="raw_string")


def test_where_hash_raw_string_different_strings_differ() -> None:
    assert where_hash("a = 1", mode="raw_string") != where_hash("a = 2", mode="raw_string")


def test_where_hash_raw_string_dict_input() -> None:
    """raw_string mode with dict input uses canonical_bytes."""
    d = {"path": "$.x", "op": "eq", "value": 1}
    h1 = where_hash(d, mode="raw_string")
    h2 = where_hash(d, mode="raw_string")
    assert h1 == h2
    assert isinstance(h1, str) and len(h1) == 64  # sha256 hex


def test_where_hash_raw_string_whitespace_matters() -> None:
    """raw_string mode does NOT normalize whitespace — different strings differ."""
    assert where_hash("a = 1", mode="raw_string") != where_hash("a  =  1", mode="raw_string")


# ---------------------------------------------------------------------------
# canonical_ast mode — dict input
# ---------------------------------------------------------------------------


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


def test_where_hash_canonical_ast_or_commutative() -> None:
    a = {
        "op": "or",
        "clauses": [
            {"path": "$.x", "op": "eq", "value": 1},
            {"path": "$.y", "op": "eq", "value": 2},
        ],
    }
    b = {
        "op": "or",
        "clauses": [
            {"path": "$.y", "op": "eq", "value": 2},
            {"path": "$.x", "op": "eq", "value": 1},
        ],
    }
    assert where_hash(a, mode="canonical_ast") == where_hash(b, mode="canonical_ast")


def test_where_hash_canonical_ast_nested_sorting() -> None:
    """Nested AND/OR clauses should be sorted recursively."""
    a = {
        "op": "and",
        "clauses": [
            {"op": "or", "clauses": [
                {"path": "$.b", "op": "eq", "value": 2},
                {"path": "$.a", "op": "eq", "value": 1},
            ]},
            {"path": "$.c", "op": "eq", "value": 3},
        ],
    }
    b = {
        "op": "and",
        "clauses": [
            {"path": "$.c", "op": "eq", "value": 3},
            {"op": "or", "clauses": [
                {"path": "$.a", "op": "eq", "value": 1},
                {"path": "$.b", "op": "eq", "value": 2},
            ]},
        ],
    }
    assert where_hash(a, mode="canonical_ast") == where_hash(b, mode="canonical_ast")


def test_where_hash_canonical_ast_different_predicates_differ() -> None:
    a = {"path": "$.x", "op": "eq", "value": 1}
    b = {"path": "$.x", "op": "eq", "value": 2}
    assert where_hash(a, mode="canonical_ast") != where_hash(b, mode="canonical_ast")


# ---------------------------------------------------------------------------
# canonical_ast mode — string input (parses expression)
# ---------------------------------------------------------------------------


def test_where_hash_canonical_ast_mode_parses_string_expression() -> None:
    a = "status = 'open' AND score >= 10"
    b = "score >= 10 and status = 'open'"
    assert where_hash(a, mode="canonical_ast") == where_hash(b, mode="canonical_ast")


def test_where_hash_canonical_ast_string_or_commutative() -> None:
    a = "x = 1 OR y = 2"
    b = "y = 2 OR x = 1"
    assert where_hash(a, mode="canonical_ast") == where_hash(b, mode="canonical_ast")


def test_where_hash_canonical_ast_string_whitespace_insensitive() -> None:
    """canonical_ast mode normalizes parsed expressions regardless of whitespace."""
    a = "x = 1 AND y = 2"
    b = "x=1   AND   y=2"
    assert where_hash(a, mode="canonical_ast") == where_hash(b, mode="canonical_ast")


def test_where_hash_canonical_ast_case_insensitive_keywords() -> None:
    """AND/and/And should all produce the same hash in canonical_ast mode."""
    a = "x = 1 AND y = 2"
    b = "x = 1 and y = 2"
    assert where_hash(a, mode="canonical_ast") == where_hash(b, mode="canonical_ast")


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def test_where_hash_rejects_unknown_mode() -> None:
    try:
        where_hash("x", mode="invalid")
    except ValueError as exc:
        assert "unsupported" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_where_hash_canonical_ast_rejects_non_dict_non_string() -> None:
    with pytest.raises(ValueError, match="canonical_ast mode requires"):
        where_hash(42, mode="canonical_ast")  # type: ignore[arg-type]


def test_where_hash_produces_hex_string() -> None:
    h = where_hash("x = 1", mode="raw_string")
    assert isinstance(h, str)
    assert len(h) == 64  # SHA-256 hex digest
    int(h, 16)  # Should be valid hex
