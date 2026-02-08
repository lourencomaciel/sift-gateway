"""Minimal where-DSL evaluator."""

from __future__ import annotations

from typing import Any, Mapping

from mcp_artifact_gateway.canon.rfc8785 import canonical_bytes
from mcp_artifact_gateway.query.jsonpath import evaluate_jsonpath


class WhereDslError(ValueError):
    """Raised for invalid where DSL expressions."""


class WhereComputeLimitExceeded(WhereDslError):
    """Raised when compute steps exceed configured limit."""


def _canonicalize(value: Any) -> Any:
    if isinstance(value, dict):
        canonical = {k: _canonicalize(value[k]) for k in sorted(value)}
        op = canonical.get("op")
        clauses = canonical.get("clauses")
        if op in {"and", "or"} and isinstance(clauses, list):
            keyed = [(canonical_bytes(item), item) for item in clauses]
            canonical["clauses"] = [item for _, item in sorted(keyed, key=lambda pair: pair[0])]
        return canonical
    if isinstance(value, list):
        return [_canonicalize(item) for item in value]
    return value


def canonicalize_where_ast(where: Mapping[str, Any]) -> dict[str, Any]:
    return _canonicalize(dict(where))


def _first_at_path(record: Any, path: str) -> tuple[bool, Any]:
    values = evaluate_jsonpath(record, path)
    if not values:
        return False, None
    return True, values[0]


def _ordered_compare(left: Any, right: Any, op: str) -> bool:
    try:
        if op == "gt":
            return left > right
        if op == "gte":
            return left >= right
        if op == "lt":
            return left < right
        return left <= right
    except TypeError as exc:
        msg = f"{op} operator requires comparable values"
        raise WhereDslError(msg) from exc


def _eval_predicate(record: Any, expr: Mapping[str, Any]) -> bool:
    path = expr.get("path")
    op = expr.get("op")
    if not isinstance(path, str) or not isinstance(op, str):
        msg = "predicate requires string path and op"
        raise WhereDslError(msg)

    exists, left = _first_at_path(record, path)
    right = expr.get("value")

    if op == "exists":
        return exists
    if op == "eq":
        return exists and left == right
    if op == "ne":
        return (not exists) or left != right
    if op in {"gt", "gte", "lt", "lte"}:
        if not exists:
            return False
        return _ordered_compare(left, right, op)
    if op == "in":
        if not isinstance(right, (list, tuple, set)):
            msg = "in operator requires array/set value"
            raise WhereDslError(msg)
        return exists and left in right
    if op == "contains":
        if not exists:
            return False
        if isinstance(left, str) and isinstance(right, str):
            return right in left
        if isinstance(left, (list, tuple, set)):
            return right in left
        return False

    msg = f"unsupported where op: {op}"
    raise WhereDslError(msg)


def evaluate_where(
    record: Any,
    where: Mapping[str, Any],
    *,
    max_compute_steps: int = 1_000_000,
) -> bool:
    if not isinstance(where, Mapping):
        msg = "where expression must be an object"
        raise WhereDslError(msg)

    steps = 0

    def walk(expr: Mapping[str, Any]) -> bool:
        nonlocal steps
        steps += 1
        if steps > max_compute_steps:
            msg = "where compute step budget exceeded"
            raise WhereComputeLimitExceeded(msg)

        op = expr.get("op")
        if op == "and":
            clauses = expr.get("clauses")
            if not isinstance(clauses, list):
                msg = "and requires clauses list"
                raise WhereDslError(msg)
            if any(not isinstance(clause, Mapping) for clause in clauses):
                msg = "and clauses must contain objects"
                raise WhereDslError(msg)
            return all(walk(clause) for clause in clauses)
        if op == "or":
            clauses = expr.get("clauses")
            if not isinstance(clauses, list):
                msg = "or requires clauses list"
                raise WhereDslError(msg)
            if any(not isinstance(clause, Mapping) for clause in clauses):
                msg = "or clauses must contain objects"
                raise WhereDslError(msg)
            return any(walk(clause) for clause in clauses)
        if op == "not":
            clause = expr.get("clause")
            if not isinstance(clause, Mapping):
                msg = "not requires clause object"
                raise WhereDslError(msg)
            return not walk(clause)

        return _eval_predicate(record, expr)

    return walk(where)
