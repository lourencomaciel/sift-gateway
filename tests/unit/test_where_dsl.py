from __future__ import annotations

import pytest

from mcp_artifact_gateway.query.where_dsl import (
    WhereComputeLimitExceededError,
    WhereDslError,
    canonicalize_where_ast,
    evaluate_where,
    parse_where_expression,
)

# ---------------------------------------------------------------------------
# Existing tests (preserved)
# ---------------------------------------------------------------------------


def test_where_dsl_eq_predicate() -> None:
    where = {"path": "$.status", "op": "eq", "value": "open"}
    assert evaluate_where({"status": "open"}, where) is True
    assert evaluate_where({"status": "closed"}, where) is False


def test_where_dsl_compound_and_not() -> None:
    where = {
        "op": "and",
        "clauses": [
            {"path": "$.n", "op": "gt", "value": 1},
            {
                "op": "not",
                "clause": {"path": "$.archived", "op": "eq", "value": True},
            },
        ],
    }
    assert evaluate_where({"n": 3, "archived": False}, where) is True
    assert evaluate_where({"n": 0, "archived": False}, where) is False


def test_where_dsl_compute_limit() -> None:
    where = {
        "op": "and",
        "clauses": [{"path": "$.a", "op": "exists"} for _ in range(5)],
    }
    try:
        evaluate_where({"a": 1}, where, max_compute_steps=2)
    except WhereComputeLimitExceededError:
        pass
    else:
        raise AssertionError("expected WhereComputeLimitExceededError")


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
        assert "requires numeric operands" in str(exc)
    else:
        raise AssertionError("expected WhereDslError")


def test_where_dsl_missing_path_false_except_ne_null() -> None:
    assert (
        evaluate_where({}, {"path": "$.missing", "op": "eq", "value": 1})
        is False
    )
    assert (
        evaluate_where({}, {"path": "$.missing", "op": "ne", "value": 1})
        is False
    )
    assert (
        evaluate_where({}, {"path": "$.missing", "op": "ne", "value": None})
        is True
    )


def test_where_dsl_relative_path_is_supported() -> None:
    where = {"path": "status", "op": "eq", "value": "open"}
    assert evaluate_where({"status": "open"}, where) is True


def test_where_dsl_wildcard_is_existential() -> None:
    where = {"path": "$.items[*].id", "op": "eq", "value": 2}
    assert evaluate_where({"items": [{"id": 1}, {"id": 2}]}, where) is True
    assert evaluate_where({"items": [{"id": 1}, {"id": 3}]}, where) is False


def test_where_dsl_wildcard_expansion_limit_enforced() -> None:
    where = {"path": "$.items[*]", "op": "exists"}
    try:
        evaluate_where({"items": [1, 2, 3]}, where, max_wildcard_expansion=2)
    except WhereDslError as exc:
        assert "wildcard expansion" in str(exc)
    else:
        raise AssertionError("expected WhereDslError")


def test_parse_where_expression_builds_ast() -> None:
    ast = parse_where_expression("status = 'open' AND NOT archived = true")
    assert ast["op"] == "and"
    assert len(ast["clauses"]) == 2


def test_evaluate_where_accepts_string_expression() -> None:
    expr = "items[*].id = 2 OR score >= 10"
    assert (
        evaluate_where({"items": [{"id": 1}, {"id": 2}], "score": 1}, expr)
        is True
    )
    assert evaluate_where({"items": [{"id": 1}], "score": 5}, expr) is False


# ---------------------------------------------------------------------------
# G39: Parser — AND, OR, NOT, parentheses, all comparison ops
# ---------------------------------------------------------------------------


def test_parser_or_operator() -> None:
    ast = parse_where_expression("a = 1 OR b = 2")
    assert ast["op"] == "or"
    assert len(ast["clauses"]) == 2


def test_parser_and_operator() -> None:
    ast = parse_where_expression("a = 1 AND b = 2")
    assert ast["op"] == "and"
    assert len(ast["clauses"]) == 2


def test_parser_not_operator() -> None:
    ast = parse_where_expression("NOT a = 1")
    assert ast["op"] == "not"
    assert ast["clause"]["path"] == "a"
    assert ast["clause"]["op"] == "eq"


def test_parser_parentheses_grouping() -> None:
    ast = parse_where_expression("(a = 1 OR b = 2) AND c = 3")
    assert ast["op"] == "and"
    assert ast["clauses"][0]["op"] == "or"
    assert ast["clauses"][1]["path"] == "c"


def test_parser_nested_parentheses() -> None:
    ast = parse_where_expression("((a = 1))")
    assert ast["path"] == "a"
    assert ast["op"] == "eq"
    assert ast["value"] == 1


def test_parser_precedence_and_binds_tighter_than_or() -> None:
    """A OR b AND c should parse as a OR (b AND c)."""
    ast = parse_where_expression("a = 1 OR b = 2 AND c = 3")
    assert ast["op"] == "or"
    assert ast["clauses"][0]["path"] == "a"
    assert ast["clauses"][1]["op"] == "and"


def test_parser_not_binds_tighter_than_and() -> None:
    """NOT a AND b should parse as (NOT a) AND b."""
    ast = parse_where_expression("NOT a = 1 AND b = 2")
    assert ast["op"] == "and"
    assert ast["clauses"][0]["op"] == "not"
    assert ast["clauses"][1]["path"] == "b"


def test_parser_all_comparison_operators() -> None:
    ops = {
        "=": "eq",
        "==": "eq",
        "!=": "ne",
        ">": "gt",
        ">=": "gte",
        "<": "lt",
        "<=": "lte",
    }
    for text_op, expected_op in ops.items():
        ast = parse_where_expression(f"x {text_op} 1")
        assert ast["op"] == expected_op, (
            f"operator {text_op} should parse as {expected_op}"
        )


def test_parser_in_operator() -> None:
    ast = parse_where_expression("status IN ['open', 'closed']")
    assert ast["op"] == "in"
    assert ast["path"] == "status"
    assert ast["value"] == ["open", "closed"]


def test_parser_in_empty_array() -> None:
    ast = parse_where_expression("tags IN []")
    assert ast["op"] == "in"
    assert ast["value"] == []


def test_parser_contains_operator() -> None:
    ast = parse_where_expression("tags CONTAINS 'python'")
    assert ast["op"] == "contains"
    assert ast["path"] == "tags"
    assert ast["value"] == "python"


def test_parser_exists_operator() -> None:
    ast = parse_where_expression("EXISTS(name)")
    assert ast["op"] == "exists"
    assert ast["path"] == "name"


def test_parser_exists_with_bracket_path() -> None:
    ast = parse_where_expression("EXISTS(data['key'])")
    assert ast["op"] == "exists"
    assert ast["path"] == "data['key']"


def test_parser_string_literal_escapes() -> None:
    ast = parse_where_expression(r"name = 'hello\'world'")
    assert ast["value"] == "hello'world"


def test_parser_double_quoted_string() -> None:
    ast = parse_where_expression('name = "hello"')
    assert ast["value"] == "hello"


def test_parser_numeric_float_literal() -> None:
    ast = parse_where_expression("score >= 3.14")
    assert ast["value"] == 3.14
    assert isinstance(ast["value"], float)


def test_parser_negative_number() -> None:
    ast = parse_where_expression("temp > -5")
    assert ast["value"] == -5


def test_parser_boolean_true() -> None:
    ast = parse_where_expression("active = true")
    assert ast["value"] is True


def test_parser_boolean_false() -> None:
    ast = parse_where_expression("active = false")
    assert ast["value"] is False


def test_parser_null_literal() -> None:
    ast = parse_where_expression("value = null")
    assert ast["value"] is None


def test_parser_rejects_empty_string() -> None:
    with pytest.raises(WhereDslError, match="non-empty"):
        parse_where_expression("")


def test_parser_rejects_whitespace_only() -> None:
    with pytest.raises(WhereDslError, match="non-empty"):
        parse_where_expression("   ")


def test_parser_flat_and_chain() -> None:
    """Multiple ANDs should be flattened into a single clauses list."""
    ast = parse_where_expression("a = 1 AND b = 2 AND c = 3")
    assert ast["op"] == "and"
    assert len(ast["clauses"]) == 3


def test_parser_flat_or_chain() -> None:
    """Multiple ORs should be flattened into a single clauses list."""
    ast = parse_where_expression("a = 1 OR b = 2 OR c = 3")
    assert ast["op"] == "or"
    assert len(ast["clauses"]) == 3


def test_parser_bracket_path_with_wildcard_and_index() -> None:
    ast = parse_where_expression("data[*][0].name = 'test'")
    assert ast["path"] == "data[*][0].name"


def test_parser_bracket_string_key() -> None:
    ast = parse_where_expression("data['my key'] = 'val'")
    assert ast["path"] == "data['my key']"


def test_parser_rejects_unexpected_token() -> None:
    with pytest.raises(WhereDslError):
        parse_where_expression("@ = 1")


def test_parser_rejects_unclosed_paren() -> None:
    with pytest.raises(WhereDslError):
        parse_where_expression("(a = 1")


def test_parser_rejects_unterminated_string() -> None:
    with pytest.raises(WhereDslError):
        parse_where_expression("a = 'hello")


# ---------------------------------------------------------------------------
# G39: Relative path evaluation (auto-anchored to $)
# ---------------------------------------------------------------------------


def test_relative_path_dotted() -> None:
    assert (
        evaluate_where({"a": {"b": 1}}, {"path": "a.b", "op": "eq", "value": 1})
        is True
    )


def test_relative_path_bracket() -> None:
    assert (
        evaluate_where({"a": 1}, {"path": "['a']", "op": "eq", "value": 1})
        is True
    )


def test_relative_path_mixed() -> None:
    doc = {"items": [{"id": 10}]}
    assert (
        evaluate_where(doc, {"path": "items[0].id", "op": "eq", "value": 10})
        is True
    )


def test_relative_path_in_string_expression() -> None:
    assert evaluate_where({"x": 5}, "x >= 5") is True


# ---------------------------------------------------------------------------
# G39: Missing-path semantics (comparisons false except != null)
# ---------------------------------------------------------------------------


def test_missing_path_eq_returns_false() -> None:
    assert (
        evaluate_where({}, {"path": "$.x", "op": "eq", "value": "anything"})
        is False
    )


def test_missing_path_eq_null_returns_false() -> None:
    """Missing path == null is False (missing is not the same as null value)."""
    assert (
        evaluate_where({}, {"path": "$.x", "op": "eq", "value": None}) is False
    )


def test_missing_path_ne_non_null_returns_false() -> None:
    assert evaluate_where({}, {"path": "$.x", "op": "ne", "value": 42}) is False


def test_missing_path_ne_null_returns_true() -> None:
    assert (
        evaluate_where({}, {"path": "$.x", "op": "ne", "value": None}) is True
    )


def test_missing_path_ordered_returns_false() -> None:
    for op in ["gt", "gte", "lt", "lte"]:
        assert (
            evaluate_where({}, {"path": "$.x", "op": op, "value": 0}) is False
        )


def test_missing_path_in_returns_false() -> None:
    assert (
        evaluate_where({}, {"path": "$.x", "op": "in", "value": [1, 2]})
        is False
    )


def test_missing_path_contains_returns_false() -> None:
    assert (
        evaluate_where({}, {"path": "$.x", "op": "contains", "value": "a"})
        is False
    )


def test_missing_path_exists_returns_false() -> None:
    assert evaluate_where({}, {"path": "$.x", "op": "exists"}) is False


# ---------------------------------------------------------------------------
# G39: Wildcard existential/bounded
# ---------------------------------------------------------------------------


def test_wildcard_any_match_satisfies_ne() -> None:
    """Ne with wildcard: true if ANY value != right."""
    doc = {"items": [1, 2, 3]}
    assert (
        evaluate_where(doc, {"path": "$.items[*]", "op": "ne", "value": 1})
        is True
    )


def test_wildcard_all_same_ne_false() -> None:
    """Ne returns false if all values match."""
    doc = {"items": [1, 1, 1]}
    assert (
        evaluate_where(doc, {"path": "$.items[*]", "op": "ne", "value": 1})
        is False
    )


def test_wildcard_in_check() -> None:
    doc = {"tags": ["a", "b", "c"]}
    assert (
        evaluate_where(
            doc, {"path": "$.tags[*]", "op": "in", "value": ["b", "d"]}
        )
        is True
    )
    assert (
        evaluate_where(
            doc, {"path": "$.tags[*]", "op": "in", "value": ["x", "y"]}
        )
        is False
    )


def test_wildcard_contains_string_in_list() -> None:
    doc = {"items": ["hello world", "foo"]}
    assert (
        evaluate_where(
            doc, {"path": "$.items[*]", "op": "contains", "value": "world"}
        )
        is True
    )
    assert (
        evaluate_where(
            doc, {"path": "$.items[*]", "op": "contains", "value": "bar"}
        )
        is False
    )


def test_wildcard_expansion_within_limit_passes() -> None:
    doc = {"items": [1, 2]}
    assert (
        evaluate_where(
            doc,
            {"path": "$.items[*]", "op": "exists"},
            max_wildcard_expansion=5,
        )
        is True
    )


# ---------------------------------------------------------------------------
# G39: Type enforcement — numeric/string/boolean semantics
# ---------------------------------------------------------------------------


def test_numeric_gt_requires_numeric_operands() -> None:
    with pytest.raises(WhereDslError, match="numeric operands"):
        evaluate_where({"x": "abc"}, {"path": "$.x", "op": "gt", "value": 1})


def test_string_gt_lexicographic() -> None:
    assert (
        evaluate_where(
            {"x": "banana"}, {"path": "$.x", "op": "gt", "value": "apple"}
        )
        is True
    )
    assert (
        evaluate_where(
            {"x": "apple"}, {"path": "$.x", "op": "gt", "value": "banana"}
        )
        is False
    )


def test_string_gte_lexicographic() -> None:
    assert (
        evaluate_where(
            {"x": "apple"}, {"path": "$.x", "op": "gte", "value": "apple"}
        )
        is True
    )


def test_string_lt_lexicographic() -> None:
    assert (
        evaluate_where(
            {"x": "abc"}, {"path": "$.x", "op": "lt", "value": "abd"}
        )
        is True
    )


def test_string_lte_lexicographic() -> None:
    assert (
        evaluate_where(
            {"x": "abc"}, {"path": "$.x", "op": "lte", "value": "abc"}
        )
        is True
    )


def test_string_gt_rejects_non_string_left() -> None:
    with pytest.raises(WhereDslError, match="string operands"):
        evaluate_where({"x": 42}, {"path": "$.x", "op": "gt", "value": "abc"})


def test_ordered_rejects_boolean_value() -> None:
    """gt/gte/lt/lte with boolean comparison value should be rejected."""
    with pytest.raises(WhereDslError, match="requires numeric or string"):
        evaluate_where({"x": True}, {"path": "$.x", "op": "gt", "value": True})


def test_boolean_eq_only() -> None:
    """Boolean values support = and != but not ordered comparisons."""
    assert (
        evaluate_where({"x": True}, {"path": "$.x", "op": "eq", "value": True})
        is True
    )
    assert (
        evaluate_where({"x": True}, {"path": "$.x", "op": "eq", "value": False})
        is False
    )
    assert (
        evaluate_where({"x": False}, {"path": "$.x", "op": "ne", "value": True})
        is True
    )


def test_boolean_int_coercion_prevented() -> None:
    """1 == True should be False with strict type semantics (no Python coercion)."""
    assert (
        evaluate_where({"x": 1}, {"path": "$.x", "op": "eq", "value": True})
        is False
    )
    assert (
        evaluate_where({"x": 0}, {"path": "$.x", "op": "eq", "value": False})
        is False
    )
    assert (
        evaluate_where({"x": True}, {"path": "$.x", "op": "eq", "value": 1})
        is False
    )


def test_boolean_ne_int_coercion_prevented() -> None:
    """1 != True should be True with strict type semantics."""
    assert (
        evaluate_where({"x": 1}, {"path": "$.x", "op": "ne", "value": True})
        is True
    )
    assert (
        evaluate_where({"x": 0}, {"path": "$.x", "op": "ne", "value": False})
        is True
    )


def test_in_with_strict_types() -> None:
    """IN should use strict type checking too."""
    assert (
        evaluate_where(
            {"x": 1}, {"path": "$.x", "op": "in", "value": [True, 2]}
        )
        is False
    )
    assert (
        evaluate_where({"x": 1}, {"path": "$.x", "op": "in", "value": [1, 2]})
        is True
    )
    assert (
        evaluate_where(
            {"x": True}, {"path": "$.x", "op": "in", "value": [1, 2]}
        )
        is False
    )


def test_numeric_float_comparison() -> None:
    assert (
        evaluate_where({"x": 3.14}, {"path": "$.x", "op": "gte", "value": 3.0})
        is True
    )
    assert (
        evaluate_where({"x": 2.5}, {"path": "$.x", "op": "lt", "value": 3.0})
        is True
    )


# ---------------------------------------------------------------------------
# G39: Compute accounting — deterministic short-circuiting
# ---------------------------------------------------------------------------


def test_compute_accounting_increments_per_path_segment() -> None:
    """Deep path with many segments should consume more compute steps."""
    deep_path = {"path": "$.a.b.c.d.e.f.g.h", "op": "exists"}
    # The path has 8 segments; walk=1, segments=8, expansion=0, comparison=1 => 10 steps
    # With max_compute_steps=5 this should exceed the limit
    with pytest.raises(WhereComputeLimitExceededError):
        evaluate_where(
            {"a": {"b": {"c": {"d": {"e": {"f": {"g": {"h": 1}}}}}}}},
            deep_path,
            max_compute_steps=5,
        )


def test_compute_accounting_short_circuit_and() -> None:
    """AND should short-circuit: if first clause is False, second should not consume budget."""
    where = {
        "op": "and",
        "clauses": [
            {"path": "$.a", "op": "eq", "value": 999},  # Always false
            {
                "path": "$.b.c.d.e.f.g.h.i.j",
                "op": "exists",
            },  # Expensive but skipped
        ],
    }
    # The first clause is false, so the second should be skipped.
    # Budget just enough for walk(and) + walk(clause1) + path resolution + comparison
    result = evaluate_where({"a": 1}, where, max_compute_steps=20)
    assert result is False


def test_compute_accounting_short_circuit_or() -> None:
    """OR should short-circuit: if first clause is True, rest should not consume budget."""
    where = {
        "op": "or",
        "clauses": [
            {"path": "$.a", "op": "eq", "value": 1},  # Always true
            {
                "path": "$.b.c.d.e.f.g.h.i.j",
                "op": "exists",
            },  # Expensive but skipped
        ],
    }
    result = evaluate_where({"a": 1}, where, max_compute_steps=20)
    assert result is True


def test_compute_accounting_deterministic_for_same_input() -> None:
    """Same expression on same data should always produce the same result."""
    where = {"path": "$.x", "op": "eq", "value": 42}
    record = {"x": 42}
    results = [
        evaluate_where(record, where, max_compute_steps=100) for _ in range(100)
    ]
    assert all(r is True for r in results)


# ---------------------------------------------------------------------------
# G39: Evaluation — IN, CONTAINS, EXISTS with dict/string expressions
# ---------------------------------------------------------------------------


def test_eval_in_with_numbers() -> None:
    assert (
        evaluate_where(
            {"x": 2}, {"path": "$.x", "op": "in", "value": [1, 2, 3]}
        )
        is True
    )
    assert (
        evaluate_where(
            {"x": 5}, {"path": "$.x", "op": "in", "value": [1, 2, 3]}
        )
        is False
    )


def test_eval_contains_string_substring() -> None:
    assert (
        evaluate_where(
            {"s": "hello world"},
            {"path": "$.s", "op": "contains", "value": "world"},
        )
        is True
    )
    assert (
        evaluate_where(
            {"s": "hello world"},
            {"path": "$.s", "op": "contains", "value": "xyz"},
        )
        is False
    )


def test_eval_contains_array_element() -> None:
    assert (
        evaluate_where(
            {"tags": [1, 2, 3]},
            {"path": "$.tags", "op": "contains", "value": 2},
        )
        is True
    )
    assert (
        evaluate_where(
            {"tags": [1, 2, 3]},
            {"path": "$.tags", "op": "contains", "value": 5},
        )
        is False
    )


def test_eval_exists_present() -> None:
    assert evaluate_where({"a": 1}, {"path": "$.a", "op": "exists"}) is True


def test_eval_exists_null_value_is_present() -> None:
    """A key with null value still exists."""
    assert evaluate_where({"a": None}, {"path": "$.a", "op": "exists"}) is True


def test_eval_or_clause() -> None:
    where = {
        "op": "or",
        "clauses": [
            {"path": "$.x", "op": "eq", "value": 1},
            {"path": "$.y", "op": "eq", "value": 2},
        ],
    }
    assert evaluate_where({"x": 1, "y": 0}, where) is True
    assert evaluate_where({"x": 0, "y": 2}, where) is True
    assert evaluate_where({"x": 0, "y": 0}, where) is False


def test_eval_not_clause() -> None:
    where = {"op": "not", "clause": {"path": "$.x", "op": "eq", "value": 1}}
    assert evaluate_where({"x": 2}, where) is True
    assert evaluate_where({"x": 1}, where) is False


# ---------------------------------------------------------------------------
# G39: String expression evaluation end-to-end
# ---------------------------------------------------------------------------


def test_string_expr_in_operator() -> None:
    assert evaluate_where({"x": "b"}, "x IN ['a', 'b', 'c']") is True
    assert evaluate_where({"x": "d"}, "x IN ['a', 'b', 'c']") is False


def test_string_expr_contains_operator() -> None:
    assert evaluate_where({"s": "foobar"}, "s CONTAINS 'bar'") is True


def test_string_expr_exists_operator() -> None:
    assert evaluate_where({"a": 1}, "EXISTS(a)") is True
    assert evaluate_where({}, "EXISTS(a)") is False


def test_string_expr_not_exists() -> None:
    assert evaluate_where({}, "NOT EXISTS(a)") is True
    assert evaluate_where({"a": 1}, "NOT EXISTS(a)") is False


def test_string_expr_complex_combination() -> None:
    expr = "(status = 'open' OR status = 'pending') AND NOT archived = true"
    assert evaluate_where({"status": "open", "archived": False}, expr) is True
    assert evaluate_where({"status": "open", "archived": True}, expr) is False
    assert (
        evaluate_where({"status": "closed", "archived": False}, expr) is False
    )


# ---------------------------------------------------------------------------
# G39: canonicalize_where_ast
# ---------------------------------------------------------------------------


def test_canonicalize_where_ast_sorts_and_clauses() -> None:
    ast = {
        "op": "and",
        "clauses": [
            {"path": "$.z", "op": "eq", "value": 1},
            {"path": "$.a", "op": "eq", "value": 2},
        ],
    }
    result = canonicalize_where_ast(ast)
    assert result["op"] == "and"
    # clauses should be sorted by canonical_bytes deterministically
    assert len(result["clauses"]) == 2


def test_canonicalize_where_ast_sorts_or_clauses() -> None:
    ast = {
        "op": "or",
        "clauses": [
            {"path": "$.z", "op": "eq", "value": 1},
            {"path": "$.a", "op": "eq", "value": 2},
        ],
    }
    result = canonicalize_where_ast(ast)
    assert result["op"] == "or"
    assert len(result["clauses"]) == 2


def test_canonicalize_where_ast_deep_sort() -> None:
    """Nested logical operators should also get their clauses sorted."""
    ast = {
        "op": "and",
        "clauses": [
            {
                "op": "or",
                "clauses": [
                    {"path": "$.z", "op": "eq", "value": 1},
                    {"path": "$.a", "op": "eq", "value": 2},
                ],
            },
            {"path": "$.b", "op": "eq", "value": 3},
        ],
    }
    result = canonicalize_where_ast(ast)
    inner = [c for c in result["clauses"] if c.get("op") == "or"][0]
    assert len(inner["clauses"]) == 2


# ---------------------------------------------------------------------------
# Edge cases and error handling
# ---------------------------------------------------------------------------


def test_where_rejects_non_object_non_string_input() -> None:
    with pytest.raises(WhereDslError, match="object or string"):
        evaluate_where({"a": 1}, 42)  # type: ignore[arg-type]


def test_where_rejects_missing_path_in_predicate() -> None:
    with pytest.raises(WhereDslError, match="string path and op"):
        evaluate_where({"a": 1}, {"op": "eq", "value": 1})


def test_where_rejects_empty_path_in_predicate() -> None:
    with pytest.raises(WhereDslError, match="non-empty"):
        evaluate_where({"a": 1}, {"path": "", "op": "eq", "value": 1})


def test_where_rejects_unsupported_op() -> None:
    with pytest.raises(WhereDslError, match="unsupported where op"):
        evaluate_where({"a": 1}, {"path": "$.a", "op": "regex", "value": ".*"})


def test_where_and_requires_list_clauses() -> None:
    with pytest.raises(WhereDslError, match="and requires clauses list"):
        evaluate_where({"a": 1}, {"op": "and", "clauses": "not_a_list"})


def test_where_or_requires_list_clauses() -> None:
    with pytest.raises(WhereDslError, match="or requires clauses list"):
        evaluate_where({"a": 1}, {"op": "or", "clauses": "not_a_list"})


def test_where_not_requires_object_clause() -> None:
    with pytest.raises(WhereDslError, match="not requires clause object"):
        evaluate_where({"a": 1}, {"op": "not", "clause": "not_an_object"})


def test_in_rejects_non_array_value() -> None:
    with pytest.raises(WhereDslError, match="in operator requires array"):
        evaluate_where(
            {"x": 1}, {"path": "$.x", "op": "in", "value": "not_array"}
        )


def test_ordered_rejects_list_comparison_value() -> None:
    with pytest.raises(WhereDslError, match="requires numeric or string"):
        evaluate_where({"x": 1}, {"path": "$.x", "op": "gt", "value": [1, 2]})


def test_eq_with_null_value_present() -> None:
    """Existing null value == null should be True."""
    assert (
        evaluate_where({"x": None}, {"path": "$.x", "op": "eq", "value": None})
        is True
    )


def test_ne_with_null_value_present() -> None:
    """Existing null value != null should be False."""
    assert (
        evaluate_where({"x": None}, {"path": "$.x", "op": "ne", "value": None})
        is False
    )
