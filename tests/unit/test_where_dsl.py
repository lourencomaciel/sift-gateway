from decimal import Decimal

import pytest

from mcp_artifact_gateway.query.where_dsl import evaluate_where, parse_where


def test_simple_comparison_steps() -> None:
    node = parse_where("a = 1")
    match, steps = evaluate_where(node, {"a": 1})
    assert match is True
    assert steps == 2  # one segment + one comparison


def test_and_or_precedence() -> None:
    node = parse_where("a = 1 OR a = 2 AND b = 3")
    # AND should bind tighter than OR
    match, _ = evaluate_where(node, {"a": 2, "b": 3})
    assert match is True
    match, _ = evaluate_where(node, {"a": 2, "b": 4})
    assert match is False


def test_wildcard_exists_semantics() -> None:
    node = parse_where("items[*].v >= 3")
    record = {"items": [{"v": 1}, {"v": 3}]}
    match, _ = evaluate_where(node, record)
    assert match is True


def test_null_semantics() -> None:
    node = parse_where("missing != null")
    match, _ = evaluate_where(node, {"a": 1})
    assert match is False


def test_decimal_comparison() -> None:
    node = parse_where("a > 1.5")
    match, _ = evaluate_where(node, {"a": Decimal("2.0")})
    assert match is True
